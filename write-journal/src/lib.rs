pub mod merge;

use crate::merge::merge_overlapping_writes;
use dashmap::DashMap;
use futures::stream::iter;
use futures::StreamExt;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteMutInt;
use off64::usz;
use off64::Off64Read;
use off64::Off64WriteMut;
use signal_future::SignalFuture;
use signal_future::SignalFutureController;
use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::spawn_blocking;
use tracing::info;
use tracing::warn;

const OFFSETOF_HASH: u64 = 0;
const OFFSETOF_LEN: u64 = OFFSETOF_HASH + 32;
const OFFSETOF_ENTRIES: u64 = OFFSETOF_LEN + 4;

// Public so `Transaction` can be used elsewhere (not just WriteJournal) e.g. mock journals.
#[derive(Debug)]
pub struct TransactionWrite {
  pub offset: u64,
  pub data: Vec<u8>,
  pub is_overlay: bool,
}

#[derive(Debug)]
pub struct Transaction {
  serial_no: u64,
  writes: Vec<TransactionWrite>,
  overlay: Arc<DashMap<u64, OverlayEntry>>,
}

impl Transaction {
  fn serialised_byte_len(&self) -> u64 {
    u64::try_from(
      self
        .writes
        .iter()
        .map(|w| 8 + 4 + w.data.len())
        .sum::<usize>(),
    )
    .unwrap()
  }

  // Public so `Transaction` can be used elsewhere (not just WriteJournal) e.g. mock journals.
  pub fn new(serial_no: u64, overlay: Arc<DashMap<u64, OverlayEntry>>) -> Self {
    Self {
      serial_no,
      writes: Vec::new(),
      overlay,
    }
  }

  pub fn serial_no(&self) -> u64 {
    self.serial_no
  }

  pub fn into_writes(self) -> Vec<TransactionWrite> {
    self.writes
  }

  // Use generic so callers don't even need to `.into()` from Vec, array, etc.
  pub fn write(&mut self, offset: u64, data: Vec<u8>) -> &mut Self {
    self.writes.push(TransactionWrite {
      offset,
      data,
      is_overlay: false,
    });
    self
  }

  /// WARNING: Use this function with caution, it's up to the caller to avoid the potential issues with misuse, including logic incorrectness, cache incoherency, and memory leaking. Carefully read notes/Overlay.md before using the overlay.
  // Use generic so callers don't even need to `.into()` from Vec, array, etc.
  pub fn write_with_overlay(&mut self, offset: u64, data: Vec<u8>) -> &mut Self {
    self.overlay.insert(offset, OverlayEntry {
      data: data.clone(),
      serial_no: self.serial_no,
    });
    self.writes.push(TransactionWrite {
      offset,
      data,
      is_overlay: true,
    });
    self
  }
}

// We cannot evict an overlay entry after a commit loop iteration if the data at the offset has since been updated again using the overlay while the commit loop was happening. This is why we need to track `serial_no`. This mechanism requires slower one-by-one deletes by the commit loop, but allows much faster parallel overlay reads with a DashMap. The alternative would be a RwLock over two maps, one for each generation, swapping them around after each loop iteration.
// Note that it's not necessary to ever evict for correctness (assuming the overlay is used correctly); eviction is done to avoid memory leaking.
// Public so `OverlayEntry` can be used elsewhere (not just WriteJournal) e.g. mock journals.
#[derive(Debug)]
pub struct OverlayEntry {
  pub data: Vec<u8>,
  pub serial_no: u64,
}

struct DsyncFile(Arc<std::fs::File>);

impl DsyncFile {
  pub async fn open(path: &Path) -> Self {
    Self(
      OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DSYNC)
        .open(path)
        .await
        .unwrap()
        .into_std()
        .await
        .into(),
    )
  }

  pub async fn read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    let file = self.0.clone();
    spawn_blocking(move || {
      let mut buf = vec![0u8; usz!(len)];
      file.read_exact_at(&mut buf, offset).unwrap();
      buf
    })
    .await
    .unwrap()
  }

  pub async fn write_at(&self, offset: u64, data: Vec<u8>) {
    let file = self.0.clone();
    spawn_blocking(move || {
      file.write_all_at(&data, offset).unwrap();
    })
    .await
    .unwrap()
  }
}

pub struct WriteJournal {
  device: DsyncFile,
  offset: u64,
  capacity: u64,
  sender: UnboundedSender<(Transaction, SignalFutureController)>,
  next_txn_serial_no: AtomicU64,
  overlay: Arc<DashMap<u64, OverlayEntry>>,
}

impl WriteJournal {
  pub async fn open(path: &Path, offset: u64, capacity: u64) -> Arc<Self> {
    assert!(capacity > OFFSETOF_ENTRIES && capacity <= u32::MAX.into());
    let (tx, rx) = unbounded_channel();
    let journal = Arc::new(Self {
      // We use DSYNC for more efficient fsync. All our writes must be immediately fsync'd — we don't use kernel buffer as intermediate storage, nor split writes — but fsync isn't granular (and sync_file_range is not portable). Delaying sync is even more pointless.
      device: DsyncFile::open(path).await,
      offset,
      capacity,
      sender: tx,
      next_txn_serial_no: AtomicU64::new(0),
      overlay: Default::default(),
    });
    tokio::task::spawn({
      let journal = journal.clone();
      async move { journal.start_commit_background_loop(rx).await }
    });
    journal
  }

  pub fn generate_blank_state(&self) -> Vec<u8> {
    let mut raw = vec![0u8; usz!(OFFSETOF_ENTRIES)];
    raw.write_u32_be_at(OFFSETOF_LEN, 0u32);
    let hash = blake3::hash(&raw[usz!(OFFSETOF_LEN)..]);
    raw.write_at(OFFSETOF_HASH, hash.as_bytes());
    raw
  }

  pub async fn format_device(&self) {
    self
      .device
      .write_at(self.offset, self.generate_blank_state())
      .await;
  }

  pub async fn recover(&self) {
    let mut raw = self
      .device
      .read_at(self.offset, OFFSETOF_ENTRIES)
      .await
      .to_vec();
    let len: u64 = raw.read_u32_be_at(OFFSETOF_LEN).into();
    if len > self.capacity - OFFSETOF_ENTRIES {
      warn!("journal is corrupt, has invalid length, skipping recovery");
      return;
    };
    raw.extend_from_slice(
      &mut self
        .device
        .read_at(self.offset + OFFSETOF_ENTRIES, len)
        .await,
    );
    let expected_hash = blake3::hash(&raw[usz!(OFFSETOF_LEN)..]);
    let recorded_hash = &raw[..usz!(OFFSETOF_LEN)];
    if expected_hash.as_bytes() != recorded_hash {
      warn!("journal is corrupt, has invalid hash, skipping recovery");
      return;
    };
    if len == 0 {
      info!("journal is empty, no recovery necessary");
      return;
    };
    let mut recovered_bytes_total = 0;
    let mut journal_offset = OFFSETOF_ENTRIES;
    while journal_offset < len {
      let offset = raw.read_u64_be_at(journal_offset);
      journal_offset += 8;
      let data_len = raw.read_u32_be_at(journal_offset);
      journal_offset += 4;
      let data = raw.read_at(journal_offset, data_len.into()).to_vec();
      journal_offset += u64::from(data_len);
      self.device.write_at(offset, data).await;
      recovered_bytes_total += data_len;
    }

    // WARNING: Make sure to sync writes BEFORE erasing journal.
    self
      .device
      .write_at(self.offset, self.generate_blank_state())
      .await;
    info!(
      recovered_entries = len,
      recovered_bytes = recovered_bytes_total,
      "journal has been recovered"
    );
  }

  pub fn begin_transaction(&self) -> Transaction {
    let serial_no = self
      .next_txn_serial_no
      .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Transaction::new(serial_no, self.overlay.clone())
  }

  pub async fn commit_transaction(&self, txn: Transaction) {
    let (fut, fut_ctl) = SignalFuture::new();
    self.sender.send((txn, fut_ctl)).unwrap();
    fut.await;
  }

  /// WARNING: Use this function with caution, it's up to the caller to avoid the potential issues with misuse, including logic incorrectness, cache incoherency, and memory leaking. Carefully read notes/Overlay.md before using the overlay.
  pub async fn read_with_overlay(&self, offset: u64, len: u64) -> Vec<u8> {
    if let Some(e) = self.overlay.get(&offset) {
      let overlay_len = e.value().data.len();
      assert_eq!(
        overlay_len,
        usz!(len),
        "overlay data at {offset} has length {overlay_len} but requested length {len}"
      );
      e.value().data.clone()
    } else {
      self.device.read_at(offset, len).await
    }
  }

  /// WARNING: Use this function with caution, it's up to the caller to avoid the potential issues with misuse, including logic incorrectness, cache incoherency, and memory leaking. Carefully read notes/Overlay.md before using the overlay.
  /// WARNING: It's unlikely you want to to use this function, as it will cause cache coherency problems as this only removes the overlay entry, so stale device data will be read instead. You most likely want to write blank/default data instead. However, this is available if you know what you're doing and have a need.
  pub async fn clear_from_overlay(&self, offset: u64) {
    self.overlay.remove(&offset);
  }

  async fn start_commit_background_loop(
    &self,
    mut rx: UnboundedReceiver<(Transaction, SignalFutureController)>,
  ) {
    let mut next_serial = 0;
    let mut pending = HashMap::new();
    // There's no point to sleeping. We can only serially do I/O. So in the time we wait, we could have just done some I/O.
    // Also, tokio::sleep granularity is 1 ms. Super high latency.
    while let Some((txn, fut_ctl)) = rx.recv().await {
      pending.insert(txn.serial_no, (txn, fut_ctl));

      let mut len = 0;
      let mut raw = vec![0u8; usz!(OFFSETOF_ENTRIES)];
      let mut writes = Vec::new();
      let mut overlays_to_delete = Vec::new();
      let mut fut_ctls = Vec::new();
      // We must `remove` to take ownership of the write data and avoid copying. But this means we need to reinsert into the map if we cannot process a transaction in this iteration.
      while let Some((serial_no, (txn, fut_ctl))) = pending.remove_entry(&next_serial) {
        let entry_len = txn.serialised_byte_len();
        if len + entry_len > self.capacity - OFFSETOF_ENTRIES {
          // Out of space, wait until next iteration.
          // TODO THIS MUST PANIC IF THE FIRST, OTHERWISE WE'LL LOOP FOREVER.
          let None = pending.insert(serial_no, (txn, fut_ctl)) else {
            unreachable!();
          };
          break;
        };
        next_serial += 1;
        for w in txn.writes {
          let data_len: u32 = w.data.len().try_into().unwrap();
          raw.extend_from_slice(&w.offset.to_be_bytes());
          raw.extend_from_slice(&data_len.to_be_bytes());
          raw.extend_from_slice(&w.data);
          writes.push((w.offset, w.data));
          if w.is_overlay {
            overlays_to_delete.push((w.offset, serial_no));
          };
        }
        len += entry_len;
        fut_ctls.push(fut_ctl);
      }
      if fut_ctls.is_empty() {
        // Assert these are empty as each iteration expects to start with cleared reused Vec values.
        assert!(overlays_to_delete.is_empty());
        assert!(writes.is_empty());
        continue;
      };
      raw.write_u32_be_at(OFFSETOF_LEN, u32::try_from(len).unwrap());
      let hash = blake3::hash(&raw[usz!(OFFSETOF_LEN)..]);
      raw.write_at(OFFSETOF_HASH, hash.as_bytes());
      self.device.write_at(self.offset, raw).await;

      // Keep semantic order without slowing down to serial writes.
      let writes_ordered = merge_overlapping_writes(writes);
      iter(writes_ordered)
        .for_each_concurrent(None, async |(offset, data)| {
          self.device.write_at(offset, data).await;
        })
        .await;

      for fut_ctl in fut_ctls.drain(..) {
        fut_ctl.signal(());
      }

      for (offset, serial_no) in overlays_to_delete.drain(..) {
        self
          .overlay
          .remove_if(&offset, |_, e| e.serial_no <= serial_no);
      }

      self
        .device
        .write_at(self.offset, self.generate_blank_state())
        .await;
    }
  }
}
