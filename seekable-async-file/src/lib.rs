pub mod common;
pub mod file;
pub mod merge;
pub mod mmap;

use crate::common::IAsyncIO;
use crate::merge::merge_overlapping_writes;
use async_trait::async_trait;
use futures::stream::iter;
use futures::stream::StreamExt;
use off64::chrono::Off64AsyncReadChrono;
use off64::chrono::Off64AsyncWriteChrono;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64AsyncWriteInt;
use off64::u64;
use off64::Off64AsyncRead;
use off64::Off64AsyncWrite;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io;
use tokio::io::AsyncSeekExt;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tokio::time::Instant;

pub async fn get_file_len_via_seek(path: &Path) -> io::Result<u64> {
  let mut file = File::open(path).await?;
  // Note that `file.metadata().len()` is 0 for device files.
  file.seek(SeekFrom::End(0)).await
}

fn dur_us(dur: Instant) -> u64 {
  dur.elapsed().as_micros().try_into().unwrap()
}

/// Data to write and the offset to write it at. This is provided to `write_at_with_delayed_sync`.
pub struct WriteRequest<D: AsRef<[u8]> + Send + 'static> {
  data: D,
  offset: u64,
}

impl<D: AsRef<[u8]> + Send + 'static> WriteRequest<D> {
  pub fn new(offset: u64, data: D) -> Self {
    Self { data, offset }
  }
}

struct PendingSyncState {
  earliest_unsynced: Option<Instant>, // Only set when first pending_sync_fut_states is created; otherwise, metrics are misleading as we'd count time when no one is waiting for a sync as delayed sync time.
  latest_unsynced: Option<Instant>,
  pending_sync_fut_states: Vec<SignalFutureController>,
}

/// Metrics populated by a `SeekableAsyncFile`. There should be exactly one per `SeekableAsyncFile`; don't share between multiple `SeekableAsyncFile` values.
///
/// To initalise, use `SeekableAsyncFileMetrics::default()`. The values can be accessed via the thread-safe getter methods.
#[derive(Default, Debug)]
pub struct SeekableAsyncFileMetrics {
  sync_background_loops_counter: AtomicU64,
  sync_counter: AtomicU64,
  sync_delayed_counter: AtomicU64,
  sync_longest_delay_us_counter: AtomicU64,
  sync_shortest_delay_us_counter: AtomicU64,
  sync_us_counter: AtomicU64,
  write_bytes_counter: AtomicU64,
  write_counter: AtomicU64,
  write_us_counter: AtomicU64,
}

impl SeekableAsyncFileMetrics {
  /// Total number of delayed sync background loop iterations.
  pub fn sync_background_loops_counter(&self) -> u64 {
    self.sync_background_loops_counter.load(Ordering::Relaxed)
  }

  /// Total number of fsync and fdatasync syscalls.
  pub fn sync_counter(&self) -> u64 {
    self.sync_counter.load(Ordering::Relaxed)
  }

  /// Total number of requested syncs that were delayed until a later time.
  pub fn sync_delayed_counter(&self) -> u64 {
    self.sync_delayed_counter.load(Ordering::Relaxed)
  }

  /// Total number of microseconds spent waiting for a sync by one or more delayed syncs.
  pub fn sync_longest_delay_us_counter(&self) -> u64 {
    self.sync_longest_delay_us_counter.load(Ordering::Relaxed)
  }

  /// Total number of microseconds spent waiting after a final delayed sync before the actual sync.
  pub fn sync_shortest_delay_us_counter(&self) -> u64 {
    self.sync_shortest_delay_us_counter.load(Ordering::Relaxed)
  }

  /// Total number of microseconds spent in fsync and fdatasync syscalls.
  pub fn sync_us_counter(&self) -> u64 {
    self.sync_us_counter.load(Ordering::Relaxed)
  }

  /// Total number of bytes written.
  pub fn write_bytes_counter(&self) -> u64 {
    self.write_bytes_counter.load(Ordering::Relaxed)
  }

  /// Total number of write syscalls.
  pub fn write_counter(&self) -> u64 {
    self.write_counter.load(Ordering::Relaxed)
  }

  /// Total number of microseconds spent in write syscalls.
  pub fn write_us_counter(&self) -> u64 {
    self.write_us_counter.load(Ordering::Relaxed)
  }
}

/// A `File`-like value that can perform async `read_at` and `write_at` for I/O at specific offsets without mutating any state (i.e. is thread safe). Metrics are collected, and syncs can be delayed for write batching opportunities as a performance optimisation.
#[derive(Clone)]
pub struct SeekableAsyncFile {
  io: Arc<dyn IAsyncIO>,
  sync_delay_us: u64,
  metrics: Arc<SeekableAsyncFileMetrics>,
  pending_sync_state: Arc<Mutex<PendingSyncState>>,
}

impl SeekableAsyncFile {
  pub async fn open(
    io: Arc<dyn IAsyncIO>,
    metrics: Arc<SeekableAsyncFileMetrics>,
    sync_delay: Duration,
  ) -> Self {
    SeekableAsyncFile {
      io,
      sync_delay_us: sync_delay.as_micros().try_into().unwrap(),
      metrics,
      pending_sync_state: Arc::new(Mutex::new(PendingSyncState {
        earliest_unsynced: None,
        latest_unsynced: None,
        pending_sync_fut_states: Vec::new(),
      })),
    }
  }

  fn bump_write_metrics(&self, len: u64, call_us: u64) {
    self
      .metrics
      .write_bytes_counter
      .fetch_add(len, Ordering::Relaxed);
    self.metrics.write_counter.fetch_add(1, Ordering::Relaxed);
    self
      .metrics
      .write_us_counter
      .fetch_add(call_us, Ordering::Relaxed);
  }

  pub async fn read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    self.io.read_at(offset, len).await
  }

  pub async fn write_at<D: AsRef<[u8]> + Send + 'static>(&self, offset: u64, data: D) {
    let len = data.as_ref().len();
    let started = Instant::now();
    self.io.write_at(offset, data.as_ref()).await;
    let call_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self.bump_write_metrics(len.try_into().unwrap(), call_us);
  }

  // Later writes win where there are overlaps.
  pub async fn write_at_with_delayed_sync<D: AsRef<[u8]> + Send + 'static>(
    &self,
    writes: impl IntoIterator<Item = WriteRequest<D>>,
  ) {
    // Count original writes and merge overlapping intervals.
    let writes_vec: Vec<_> = writes.into_iter().collect();
    let count = u64!(writes_vec.len());
    let intervals = merge_overlapping_writes(writes_vec);

    // Execute all non-overlapping writes concurrently.
    iter(intervals)
      .for_each_concurrent(None, async |(offset, (_, data))| {
        self.write_at(offset, data).await;
      })
      .await;

    let (fut, fut_ctl) = SignalFuture::new();

    {
      let mut state = self.pending_sync_state.lock().await;
      let now = Instant::now();
      state.earliest_unsynced.get_or_insert(now);
      state.latest_unsynced = Some(now);
      state.pending_sync_fut_states.push(fut_ctl.clone());
    };

    self
      .metrics
      .sync_delayed_counter
      .fetch_add(count, Ordering::Relaxed);

    fut.await;
  }

  pub async fn start_delayed_data_sync_background_loop(&self) {
    // Store these outside and reuse them to avoid reallocations on each loop.
    let mut futures_to_wake = Vec::new();
    loop {
      sleep(std::time::Duration::from_micros(self.sync_delay_us)).await;

      struct SyncNow {
        longest_delay_us: u64,
        shortest_delay_us: u64,
      }

      let sync_now = {
        let mut state = self.pending_sync_state.lock().await;

        if !state.pending_sync_fut_states.is_empty() {
          let longest_delay_us = dur_us(state.earliest_unsynced.unwrap());
          let shortest_delay_us = dur_us(state.latest_unsynced.unwrap());

          state.earliest_unsynced = None;
          state.latest_unsynced = None;

          futures_to_wake.extend(state.pending_sync_fut_states.drain(..));

          Some(SyncNow {
            longest_delay_us,
            shortest_delay_us,
          })
        } else {
          None
        }
      };

      if let Some(SyncNow {
        longest_delay_us,
        shortest_delay_us,
      }) = sync_now
      {
        // OPTIMISATION: Don't perform these atomic operations while unnecessarily holding up the lock.
        self
          .metrics
          .sync_longest_delay_us_counter
          .fetch_add(longest_delay_us, Ordering::Relaxed);
        self
          .metrics
          .sync_shortest_delay_us_counter
          .fetch_add(shortest_delay_us, Ordering::Relaxed);

        self.sync_data().await;

        for ft in futures_to_wake.drain(..) {
          ft.signal();
        }
      };

      self
        .metrics
        .sync_background_loops_counter
        .fetch_add(1, Ordering::Relaxed);
    }
  }

  pub async fn sync_data(&self) {
    let started = Instant::now();
    self.io.sync_data().await;
    let sync_us: u64 = started.elapsed().as_micros().try_into().unwrap();
    self.metrics.sync_counter.fetch_add(1, Ordering::Relaxed);
    self
      .metrics
      .sync_us_counter
      .fetch_add(sync_us, Ordering::Relaxed);
  }
}

#[async_trait]
impl<'a> Off64AsyncRead<'a, Vec<u8>> for SeekableAsyncFile {
  async fn read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    SeekableAsyncFile::read_at(self, offset, len).await
  }
}
impl<'a> Off64AsyncReadChrono<'a, Vec<u8>> for SeekableAsyncFile {}
impl<'a> Off64AsyncReadInt<'a, Vec<u8>> for SeekableAsyncFile {}

#[async_trait]
impl Off64AsyncWrite for SeekableAsyncFile {
  async fn write_at(&self, offset: u64, value: &[u8]) {
    SeekableAsyncFile::write_at(self, offset, value.to_vec()).await
  }
}
impl Off64AsyncWriteChrono for SeekableAsyncFile {}
impl Off64AsyncWriteInt for SeekableAsyncFile {}
