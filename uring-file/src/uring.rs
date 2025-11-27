//! Core io_uring wrapper providing async file operations.
//!
//! This module implements a thread-safe, async-compatible wrapper around Linux's io_uring interface. It uses a dedicated submission thread and completion thread to manage the ring, allowing multiple async tasks to share a single ring instance.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
//! │ Async Tasks │────▶│ Submission Thread │────▶│    io_uring     │
//! │  (callers)  │     │  (batches SQEs)   │     │ (kernel space)  │
//! └─────────────┘     └──────────────────┘     └────────┬────────┘
//!        ▲                                              │
//!        │            ┌──────────────────┐              │
//!        └────────────│ Completion Thread │◀────────────┘
//!                     │  (polls CQEs)     │
//!                     └──────────────────┘
//! ```
//!
//! # Thread Safety
//!
//! The `Uring` struct is `Clone + Send + Sync`. Cloning is cheap (just clones the internal channel sender). All operations are thread-safe.

use crate::metadata::Metadata;
use dashmap::DashMap;
use io_uring::IoUring;
use io_uring::cqueue::Entry as CEntry;
use io_uring::opcode;
use io_uring::squeue::Entry as SEntry;
use io_uring::types;
use std::collections::VecDeque;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::thread;
use tokio::sync::oneshot;

// ============================================================================
// File Target Types
// ============================================================================

/// Maximum number of files that can be registered with a single Uring instance.
const MAX_REGISTERED_FILES: u32 = 4096;

/// Internal representation of a file target - either a raw fd or a registered file index.
#[derive(Clone, Copy)]
#[doc(hidden)]
pub enum Target {
  Fd(RawFd),
  Fixed { index: u32, raw_fd: RawFd },
}

/// Trait for types that can be used as file targets in io_uring operations.
///
/// This is implemented for all types that implement `AsRawFd` (using unregistered fds) and for `RegisteredFile` (using registered file indices for better performance).
pub trait UringTarget {
  #[doc(hidden)]
  fn as_target(&self, uring_identity: &Arc<()>) -> Target;
}

impl<T: AsRawFd> UringTarget for T {
  fn as_target(&self, _uring_identity: &Arc<()>) -> Target {
    Target::Fd(self.as_raw_fd())
  }
}

/// A file registered with a specific `Uring` instance for optimized I/O.
///
/// Registered files avoid the overhead of fd lookup on each operation. Create one via [`Uring::register`].
///
/// # Performance
///
/// Using registered files can significantly reduce per-operation overhead, especially for high-frequency I/O patterns. The kernel maintains a pre-validated reference to the file, avoiding repeated fd table lookups.
///
/// # Kernel Requirements
///
/// Requires Linux 5.12+ for sparse file registration.
pub struct RegisteredFile {
  index: u32,
  raw_fd: RawFd,
  uring_identity: Arc<()>,
}

impl UringTarget for RegisteredFile {
  fn as_target(&self, uring_identity: &Arc<()>) -> Target {
    assert!(
      Arc::ptr_eq(&self.uring_identity, uring_identity),
      "RegisteredFile used with wrong Uring instance"
    );
    Target::Fixed {
      index: self.index,
      raw_fd: self.raw_fd,
    }
  }
}

// ============================================================================
// Request Types
// ============================================================================

struct ReadRequest {
  target: Target,
  out_buf: Vec<u8>,
  offset: u64,
  len: u32,
}

struct WriteRequest {
  target: Target,
  offset: u64,
  data: Vec<u8>,
}

struct SyncRequest {
  target: Target,
  datasync: bool,
}

struct StatxRequest {
  target: Target,
  /// Caller-allocated buffer for statx result. We use libc::statx for the actual storage, cast to types::statx* for the opcode.
  statx_buf: Box<MaybeUninit<libc::statx>>,
}

struct FallocateRequest {
  target: Target,
  offset: u64,
  len: u64,
  mode: i32,
}

struct FadviseRequest {
  target: Target,
  offset: u64,
  len: u32,
  advice: i32,
}

struct FtruncateRequest {
  target: Target,
  len: u64,
}

// ============================================================================
// Response Types
// ============================================================================

/// Result of a read operation: the buffer and actual bytes read.
pub struct ReadResult {
  /// The buffer containing the data read.
  pub buf: Vec<u8>,
  /// Number of bytes actually read (may be less than buffer size at EOF).
  pub bytes_read: usize,
}

/// Result of a write operation: the buffer and actual bytes written.
pub struct WriteResult {
  /// The original buffer (returned for reuse).
  pub buf: Vec<u8>,
  /// Number of bytes actually written (may be less than buffer size for non-regular files).
  pub bytes_written: usize,
}

// ============================================================================
// Request Enum
// ============================================================================

enum Request {
  Read {
    req: ReadRequest,
    res: oneshot::Sender<io::Result<ReadResult>>,
  },
  Write {
    req: WriteRequest,
    res: oneshot::Sender<io::Result<WriteResult>>,
  },
  Sync {
    req: SyncRequest,
    res: oneshot::Sender<io::Result<()>>,
  },
  Statx {
    req: StatxRequest,
    res: oneshot::Sender<io::Result<Metadata>>,
  },
  Fallocate {
    req: FallocateRequest,
    res: oneshot::Sender<io::Result<()>>,
  },
  Fadvise {
    req: FadviseRequest,
    res: oneshot::Sender<io::Result<()>>,
  },
  Ftruncate {
    req: FtruncateRequest,
    res: oneshot::Sender<io::Result<()>>,
  },
}

// ============================================================================
// Uring Configuration
// ============================================================================

/// Configuration options for io_uring initialization.
///
/// These are advanced options that affect io_uring behavior. Most users should use `UringCfg::default()`. Incorrect configuration may cause `EINVAL` errors or degraded performance.
///
/// # Kernel Requirements
///
/// Some options require specific kernel versions or capabilities:
/// - `coop_taskrun`: Linux 5.19+
/// - `defer_taskrun`: Linux 6.1+
/// - `sqpoll`: Requires `CAP_SYS_NICE` capability
/// - `iopoll`: Only works with O_DIRECT files on supported filesystems
#[derive(Clone, Default, Debug)]
pub struct UringCfg {
  /// Enable cooperative task running (Linux 5.19+). When enabled, the kernel will only process completions when the application explicitly asks for them, reducing overhead.
  pub coop_taskrun: bool,

  /// Enable deferred task running (Linux 6.1+). Similar to `coop_taskrun` but with additional deferral. Requires `coop_taskrun` to also be set.
  pub defer_taskrun: bool,

  /// Enable I/O polling mode. When enabled, the kernel will poll for completions instead of using interrupts. Only works with `O_DIRECT` files on supported filesystems. Can provide lower latency but uses more CPU.
  pub iopoll: bool,

  /// Enable submission queue polling with the given idle timeout in milliseconds. When enabled, a kernel thread will poll the submission queue, eliminating the need for system calls to submit I/O. The thread will go to sleep after being idle for the specified duration. **Requires `CAP_SYS_NICE` capability.**
  pub sqpoll: Option<u32>,
}

// ============================================================================
// Uring Core
// ============================================================================

/// Handle to a shared io_uring instance.
///
/// This is the main entry point for performing async I/O operations via io_uring. It is cheap to clone (just clones an `Arc` internally) and safe to share across threads and async tasks.
///
/// # Example
///
/// ```ignore
/// use uring_file::uring::{Uring, UringCfg};
/// use std::fs::File;
///
/// #[tokio::main]
/// async fn main() -> std::io::Result<()> {
///     let uring = Uring::new(UringCfg::default())?;
///     let file = File::open("test.txt")?;
///     
///     // Option 1: Use file directly (unregistered)
///     let result = uring.read_at(&file, 0, 1024).await?;
///     
///     // Option 2: Register file for better performance
///     let registered = uring.register(&file)?;
///     let result = uring.read_at(&registered, 0, 1024).await?;
///     
///     println!("Read {} bytes", result.bytes_read);
///     Ok(())
/// }
/// ```
///
/// # Architecture Notes
///
/// Internally, `Uring` spawns two background threads:
/// 1. **Submission thread**: Receives requests via a channel, batches them, and submits them to the kernel.
/// 2. **Completion thread**: Polls the completion queue and dispatches results back to waiting async tasks.
///
/// This design allows for efficient batching of submissions while maintaining a simple async API.
// Sources:
// - Example: https://github1s.com/tokio-rs/io-uring/blob/HEAD/examples/tcp_echo.rs
// - liburing docs: https://unixism.net/loti/ref-liburing/completion.html
// - Quick high-level overview: https://man.archlinux.org/man/io_uring.7.en
// - io_uring walkthrough: https://unixism.net/2020/04/io-uring-by-example-part-1-introduction/
// - Multithreading:
//   - https://github.com/axboe/liburing/issues/109#issuecomment-1114213402
//   - https://github.com/axboe/liburing/issues/109#issuecomment-1166378978
//   - https://github.com/axboe/liburing/issues/109#issuecomment-614911522
//   - https://github.com/axboe/liburing/issues/125
//   - https://github.com/axboe/liburing/issues/127
//   - https://github.com/axboe/liburing/issues/129
//   - https://github.com/axboe/liburing/issues/571#issuecomment-1106480309
// - Kernel poller: https://unixism.net/loti/tutorial/sq_poll.html
#[derive(Clone)]
pub struct Uring {
  // We don't use std::sync::mpsc::Sender as it is not Sync, so it's really complicated to use from any async function.
  sender: crossbeam_channel::Sender<Request>,
  ring: Arc<IoUring<SEntry, CEntry>>,
  next_file_slot: Arc<AtomicU32>,
  identity: Arc<()>,
}

/// Helper to build a submission entry for either Fd or Fixed target.
macro_rules! build_op {
  ($target:expr, | $fd:ident | $op:expr) => {
    match $target {
      Target::Fd(raw) => {
        let $fd = types::Fd(raw);
        $op
      }
      Target::Fixed { index, .. } => {
        let $fd = types::Fixed(index);
        $op
      }
    }
  };
}

/// Helper to build a submission entry that only supports Fd (not Fixed).
macro_rules! build_op_fd_only {
  ($target:expr, | $fd:ident | $op:expr) => {
    match $target {
      Target::Fd(raw) => {
        let $fd = types::Fd(raw);
        $op
      }
      Target::Fixed { raw_fd, .. } => {
        let $fd = types::Fd(raw_fd);
        $op
      }
    }
  };
}

/// Process a completion entry and dispatch the result.
fn handle_completion(req: Request, result: i32) {
  let result: io::Result<i32> = if result < 0 {
    Err(io::Error::from_raw_os_error(-result))
  } else {
    Ok(result)
  };

  match req {
    Request::Read { req, res } => {
      let outcome = result.map(|n| ReadResult {
        buf: req.out_buf,
        bytes_read: n as usize,
      });
      let _ = res.send(outcome);
    }
    Request::Write { req, res } => {
      let outcome = result.map(|n| WriteResult {
        buf: req.data,
        bytes_written: n as usize,
      });
      let _ = res.send(outcome);
    }
    Request::Sync { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Request::Statx { req, res } => {
      let outcome = result.map(|_| {
        // SAFETY: The kernel has written the statx structure to this buffer
        let statx = unsafe { (*req.statx_buf).assume_init() };
        Metadata(statx)
      });
      let _ = res.send(outcome);
    }
    Request::Fallocate { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Request::Fadvise { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Request::Ftruncate { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
  }
}

impl Uring {
  /// Create a new io_uring instance with the given configuration.
  ///
  /// This spawns two background threads for submission and completion handling. The threads will automatically stop when all `Uring` handles are dropped.
  ///
  /// # Errors
  ///
  /// Returns an error if the io_uring cannot be created (e.g., kernel too old, resource limits exceeded, or insufficient permissions).
  pub fn new(cfg: UringCfg) -> io::Result<Self> {
    let (sender, receiver) = crossbeam_channel::unbounded::<Request>();
    let pending: Arc<DashMap<u64, Request>> = Default::default();

    // ASSUMPTION: Ring size of 128Mi entries. This is very large and will be clamped by the kernel to the maximum supported size. We intentionally request a large value to get the maximum available, as we batch many operations. The kernel will clamp this via IORING_SETUP_CLAMP.
    const RING_SIZE: u32 = 134217728;

    let ring = {
      let mut builder = IoUring::<SEntry, CEntry>::builder();
      // ASSUMPTION: We use IORING_SETUP_CLAMP to let the kernel reduce the ring size if our requested size exceeds system limits. This is safer than failing outright.
      builder.setup_clamp();
      if cfg.coop_taskrun {
        builder.setup_coop_taskrun();
      };
      if cfg.defer_taskrun {
        builder.setup_defer_taskrun();
      };
      if cfg.iopoll {
        builder.setup_iopoll();
      }
      if let Some(sqpoll) = cfg.sqpoll {
        builder.setup_sqpoll(sqpoll);
      };
      builder.build(RING_SIZE)?
    };

    // Pre-allocate sparse file table for registration (Linux 5.12+). If this fails, file registration won't work but unregistered fds will still function.
    let _ = ring.submitter().register_files_sparse(MAX_REGISTERED_FILES);

    let ring = Arc::new(ring);

    // Submission thread.
    thread::spawn({
      let pending = pending.clone();
      let ring = ring.clone();
      // This is outside the loop to avoid reallocation each time.
      let mut msgbuf = VecDeque::new();
      move || {
        // SAFETY: We ensure that the submission queue is only accessed from this single thread. The completion queue is accessed from a separate thread.
        let mut submission = unsafe { ring.submission_shared() };
        let mut next_id = 0u64;

        // If this loop exits, it means we've dropped all `Uring` handles and can safely stop.
        while let Ok(init_msg) = receiver.recv() {
          // Process multiple messages at once to avoid too many io_uring submits.
          msgbuf.push_back(init_msg);
          while let Ok(msg) = receiver.try_recv() {
            msgbuf.push_back(msg);
          }

          // How the io_uring submission queue works:
          // - The buffer is shared between the kernel and userspace.
          // - There are atomic head and tail indices that allow them to be shared mutably between kernel and userspace safely.
          // - The Rust library we're using abstracts over this by caching the head and tail as local values. Once we've made our inserts, we update the atomic tail and then tell the kernel to consume some of the queue. When we update the atomic tail, we also check the atomic head and update our local cached value; some entries may have been consumed by the kernel in some other thread since we last checked and we may actually have more free space than we thought.
          while let Some(msg) = msgbuf.pop_front() {
            let id = next_id;
            next_id = next_id.wrapping_add(1);

            let submission_entry = match &msg {
              Request::Read { req, .. } => {
                // SAFETY: We're using `as_ptr()` to get a pointer to the buffer. The buffer is owned by the Request and will live until the completion is processed.
                let ptr = req.out_buf.as_ptr() as *mut u8;
                build_op!(req.target, |fd| opcode::Read::new(fd, ptr, req.len)
                  .offset(req.offset)
                  .build()
                  .user_data(id))
              }
              Request::Write { req, .. } => {
                let ptr = req.data.as_ptr();
                let len: u32 = req.data.len().try_into().unwrap();
                build_op!(req.target, |fd| opcode::Write::new(fd, ptr, len)
                  .offset(req.offset)
                  .build()
                  .user_data(id))
              }
              Request::Sync { req, .. } => {
                build_op!(req.target, |fd| {
                  let mut fsync = opcode::Fsync::new(fd);
                  if req.datasync {
                    fsync = fsync.flags(types::FsyncFlags::DATASYNC);
                  }
                  fsync.build().user_data(id)
                })
              }
              Request::Statx { req, .. } => {
                const STATX_BASIC_STATS: u32 = 0x000007ff; // STATX_BASIC_STATS requests the basic stat fields
                const AT_EMPTY_PATH: i32 = 0x1000; // AT_EMPTY_PATH means interpret dirfd as the file itself
                static EMPTY_PATH: &std::ffi::CStr = c""; // Empty path - statx with AT_EMPTY_PATH uses the fd directly

                // Cast libc::statx* to types::statx* - the opcode uses an opaque type but the kernel writes the actual statx struct
                let statx_ptr = req.statx_buf.as_ptr() as *mut types::statx;

                // Note: Statx doesn't support Fixed in the io-uring crate, so we fall back to raw fd
                build_op_fd_only!(req.target, |fd| opcode::Statx::new(
                  fd,
                  EMPTY_PATH.as_ptr(),
                  statx_ptr
                )
                .flags(AT_EMPTY_PATH)
                .mask(STATX_BASIC_STATS)
                .build()
                .user_data(id))
              }
              Request::Fallocate { req, .. } => {
                build_op!(req.target, |fd| opcode::Fallocate::new(fd, req.len)
                  .offset(req.offset)
                  .mode(req.mode)
                  .build()
                  .user_data(id))
              }
              Request::Fadvise { req, .. } => {
                build_op!(req.target, |fd| opcode::Fadvise::new(
                  fd,
                  req.len as i64,
                  req.advice
                )
                .offset(req.offset)
                .build()
                .user_data(id))
              }
              Request::Ftruncate { req, .. } => {
                build_op!(req.target, |fd| opcode::Ftruncate::new(fd, req.len)
                  .build()
                  .user_data(id))
              }
            };

            // Insert before submitting so the completion handler can find it.
            pending.insert(id, msg);

            if submission.is_full() {
              submission.sync();
              // ASSUMPTION: We unwrap here because ring submission failure is catastrophic and unrecoverable. The ring is in an inconsistent state if this fails.
              ring.submit_and_wait(1).unwrap();
            }

            // SAFETY: The submission entry references memory that will remain valid until the completion is processed (owned by Request).
            unsafe {
              // This call only has one error: queue is full. It should never happen because we just checked that it's not.
              submission.push(&submission_entry).unwrap();
            };
          }

          submission.sync();
          // This is still necessary even with sqpoll, as our kernel thread may have gone to sleep.
          ring.submit().unwrap();
        }
      }
    });

    // Completion thread.
    thread::spawn({
      let pending = pending.clone();
      let ring = ring.clone();
      move || {
        // SAFETY: We ensure that the completion queue is only accessed from this single thread. The submission queue is accessed from a separate thread.
        let mut completion = unsafe { ring.completion_shared() };

        // TODO: Stop this loop if all `Uring` handles have been dropped and there are no pending requests. Currently this thread runs forever.
        loop {
          let Some(e) = completion.next() else {
            // No completions available, wait for one.
            ring.submit_and_wait(1).unwrap();
            completion.sync();
            continue;
          };

          let id = e.user_data();
          let (_, req) = pending
            .remove(&id)
            .expect("completion for unknown request id");
          handle_completion(req, e.result());
        }
      }
    });

    Ok(Self {
      sender,
      ring,
      next_file_slot: Arc::new(AtomicU32::new(0)),
      identity: Arc::new(()),
    })
  }

  /// Register a file for optimized I/O operations.
  ///
  /// Registered files use kernel-side file references, avoiding fd table lookups on each operation. This can significantly improve performance for high-frequency I/O.
  ///
  /// # Errors
  ///
  /// Returns an error if the maximum number of registered files has been reached, or if file registration fails (e.g., kernel too old).
  ///
  /// # Kernel Requirements
  ///
  /// Requires Linux 5.12+ for sparse file registration.
  pub fn register(&self, file: &impl AsRawFd) -> io::Result<RegisteredFile> {
    let raw_fd = file.as_raw_fd();
    let slot = self.next_file_slot.fetch_add(1, Ordering::SeqCst);
    if slot >= MAX_REGISTERED_FILES {
      return Err(io::Error::new(
        io::ErrorKind::Other,
        "maximum registered files exceeded",
      ));
    }

    self
      .ring
      .submitter()
      .register_files_update(slot, &[raw_fd])?;

    Ok(RegisteredFile {
      index: slot,
      raw_fd,
      uring_identity: self.identity.clone(),
    })
  }

  /// Send a request to the submission thread.
  fn send(&self, req: Request) {
    self.sender.send(req).expect("uring submission thread dead");
  }

  /// Read from a file at the specified offset. Returns the buffer and the number of bytes actually read. The number of bytes read may be less than requested if the file is smaller or if EOF is reached.
  pub async fn read_at(
    &self,
    file: &impl UringTarget,
    offset: u64,
    len: u64,
  ) -> io::Result<ReadResult> {
    let target = file.as_target(&self.identity);
    let out_buf = vec![0u8; len.try_into().unwrap()];
    let (tx, rx) = oneshot::channel();
    self.send(Request::Read {
      req: ReadRequest {
        target,
        out_buf,
        offset,
        len: len.try_into().unwrap(),
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Write to a file at the specified offset. Returns the original buffer and number of bytes written. For regular files, `bytes_written` will typically equal `data.len()`. For pipes, sockets, etc., short writes are possible and normal.
  pub async fn write_at(
    &self,
    file: &impl UringTarget,
    offset: u64,
    data: Vec<u8>,
  ) -> io::Result<WriteResult> {
    let target = file.as_target(&self.identity);
    let (tx, rx) = oneshot::channel();
    self.send(Request::Write {
      req: WriteRequest {
        target,
        offset,
        data,
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Synchronize file data and metadata to disk (fsync). This ensures that all data and metadata modifications are flushed to the underlying storage device. Even when using direct I/O, this is necessary to ensure the device itself has flushed any internal caches.
  ///
  /// **Note on ordering**: io_uring does not guarantee ordering between operations. If you need to ensure writes complete before fsync, you should await the write first, then call fsync.
  pub async fn sync(&self, file: &impl UringTarget) -> io::Result<()> {
    let target = file.as_target(&self.identity);
    let (tx, rx) = oneshot::channel();
    self.send(Request::Sync {
      req: SyncRequest {
        target,
        datasync: false,
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Synchronize file data to disk (fdatasync). Like [`sync`](Self::sync), but only flushes data, not metadata (unless the metadata is required to retrieve the data). This can be faster than a full fsync.
  pub async fn datasync(&self, file: &impl UringTarget) -> io::Result<()> {
    let target = file.as_target(&self.identity);
    let (tx, rx) = oneshot::channel();
    self.send(Request::Sync {
      req: SyncRequest {
        target,
        datasync: true,
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Get file status information (statx). This is the io_uring equivalent of `fstat`/`statx`. It returns metadata about the file including size, permissions, timestamps, etc. Requires Linux 5.6+. On older kernels, this will fail with `EINVAL`.
  pub async fn statx(&self, file: &impl UringTarget) -> io::Result<Metadata> {
    let target = file.as_target(&self.identity);
    // Allocate buffer for the kernel to write statx result
    let statx_buf = Box::new(MaybeUninit::<libc::statx>::uninit());
    let (tx, rx) = oneshot::channel();
    self.send(Request::Statx {
      req: StatxRequest { target, statx_buf },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Pre-allocate or deallocate space for a file (fallocate). This can be used to pre-allocate space to avoid fragmentation, punch holes in sparse files, or zero-fill regions. See [`falloc`] module for mode flags. Requires Linux 5.6+.
  pub async fn fallocate(
    &self,
    file: &impl UringTarget,
    offset: u64,
    len: u64,
    mode: i32,
  ) -> io::Result<()> {
    let target = file.as_target(&self.identity);
    let (tx, rx) = oneshot::channel();
    self.send(Request::Fallocate {
      req: FallocateRequest {
        target,
        offset,
        len,
        mode,
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Advise the kernel about expected file access patterns (fadvise). This is a hint to the kernel about how you intend to access a file region. The kernel may use this to optimize readahead, caching, etc. See [`advice`] module for advice values. Requires Linux 5.6+.
  pub async fn fadvise(
    &self,
    file: &impl UringTarget,
    offset: u64,
    len: u32,
    advice: i32,
  ) -> io::Result<()> {
    let target = file.as_target(&self.identity);
    let (tx, rx) = oneshot::channel();
    self.send(Request::Fadvise {
      req: FadviseRequest {
        target,
        offset,
        len,
        advice,
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Truncate a file to a specified length (ftruncate). If the file is larger than the specified length, the extra data is lost. If the file is smaller, it is extended and the extended part reads as zeros. Requires Linux 6.9+. On older kernels, this will fail with `EINVAL`.
  pub async fn ftruncate(&self, file: &impl UringTarget, len: u64) -> io::Result<()> {
    let target = file.as_target(&self.identity);
    let (tx, rx) = oneshot::channel();
    self.send(Request::Ftruncate {
      req: FtruncateRequest { target, len },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }
}

// ============================================================================
// Constants for fadvise
// ============================================================================

/// fadvise advice values. These are the standard POSIX fadvise constants.
pub mod advice {
  /// No special treatment (default).
  pub const NORMAL: i32 = libc::POSIX_FADV_NORMAL;
  /// Expect random access pattern.
  pub const RANDOM: i32 = libc::POSIX_FADV_RANDOM;
  /// Expect sequential access pattern.
  pub const SEQUENTIAL: i32 = libc::POSIX_FADV_SEQUENTIAL;
  /// Data will be needed soon (trigger readahead).
  pub const WILLNEED: i32 = libc::POSIX_FADV_WILLNEED;
  /// Data won't be needed soon (may be evicted from cache).
  pub const DONTNEED: i32 = libc::POSIX_FADV_DONTNEED;
  /// Data will be accessed once (don't keep in cache).
  pub const NOREUSE: i32 = libc::POSIX_FADV_NOREUSE;
}

// ============================================================================
// Constants for fallocate
// ============================================================================

/// fallocate mode flags.
pub mod falloc {
  /// Don't modify the file size.
  pub const KEEP_SIZE: i32 = libc::FALLOC_FL_KEEP_SIZE;
  /// Deallocate space (punch a hole).
  pub const PUNCH_HOLE: i32 = libc::FALLOC_FL_PUNCH_HOLE;
  /// Zero-fill a range without allocating.
  #[cfg(target_os = "linux")]
  pub const ZERO_RANGE: i32 = 0x10; // FALLOC_FL_ZERO_RANGE
  /// Collapse a range (remove without leaving a hole).
  #[cfg(target_os = "linux")]
  pub const COLLAPSE_RANGE: i32 = 0x08; // FALLOC_FL_COLLAPSE_RANGE
  /// Insert a range (shift data).
  #[cfg(target_os = "linux")]
  pub const INSERT_RANGE: i32 = 0x20; // FALLOC_FL_INSERT_RANGE
}
