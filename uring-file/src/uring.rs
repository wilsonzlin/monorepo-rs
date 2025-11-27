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
use std::ffi::CString;
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::IntoRawFd;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::thread;
use tokio::sync::oneshot;

// ============================================================================
// Constants
// ============================================================================

/// Maximum length for a single io_uring read/write operation.
///
/// io_uring uses i32 for return values, limiting single operations to ~2GB. The actual limit is 4096 bytes less than 2GB for unknown reasons.
pub const URING_LEN_MAX: u64 = 2 * 1024 * 1024 * 1024 - 4096;

/// Maximum number of files that can be registered with a single Uring instance.
const MAX_REGISTERED_FILES: u32 = 4096;

// ============================================================================
// Path Conversion
// ============================================================================

/// Convert a path to a CString for use with io_uring operations.
fn path_to_cstring(path: &Path) -> io::Result<CString> {
  CString::new(path.as_os_str().as_bytes()).map_err(|e| {
    io::Error::new(
      io::ErrorKind::InvalidInput,
      format!("path contains null byte at position {}", e.nul_position()),
    )
  })
}

// ============================================================================
// Buffer Traits
// ============================================================================

// We define custom buffer traits rather than using `Vec<u8>` or `Box<[u8]>` directly because:
//
// 1. **Aligned allocations**: O_DIRECT requires sector-aligned buffers (typically 512 or 4096 bytes).
//    `Vec<u8>` only guarantees pointer alignment, not allocation alignment. Custom allocators
//    can provide properly aligned buffers that implement these traits.
//
// 2. **Buffer pools**: High-performance applications reuse buffers to avoid allocation overhead.
//    Pool-managed buffers can implement these traits directly without conversion.
//
// 3. **Specialized memory**: GPU memory, mmap'd regions, or other exotic buffer types can
//    participate in io_uring operations by implementing these traits.
//
// 4. **Zero-copy**: Accepting generic buffers avoids the need to copy data into/out of
//    a library-owned buffer type.
//
// The traits are `unsafe` because implementors must guarantee pointer stability across moves,
// which is automatically true for heap allocations but NOT for stack arrays.

/// A buffer that can be used for io_uring write operations.
///
/// # Safety
///
/// Implementors must guarantee that:
/// - The pointer returned by `as_ptr()` remains valid and at a stable address until the I/O operation completes, even if `self` is moved.
/// - This is automatically satisfied for heap-allocated buffers (`Vec<u8>`, `Box<[u8]>`, etc.) but NOT for stack-allocated arrays.
pub unsafe trait IoBuf: Send + 'static {
  /// Returns a pointer to the buffer's data.
  fn as_ptr(&self) -> *const u8;
  /// Returns the number of initialized bytes in the buffer.
  fn len(&self) -> usize;
  /// Returns true if the buffer is empty.
  fn is_empty(&self) -> bool {
    self.len() == 0
  }
}

/// A buffer that can be used for io_uring read operations.
///
/// # Safety
///
/// Implementors must guarantee that:
/// - The pointer returned by `as_mut_ptr()` remains valid and at a stable address until the I/O operation completes, even if `self` is moved.
/// - This is automatically satisfied for heap-allocated buffers (`Vec<u8>`, `Box<[u8]>`, etc.) but NOT for stack-allocated arrays.
pub unsafe trait IoBufMut: Send + 'static {
  /// Returns a mutable pointer to the buffer's data.
  fn as_mut_ptr(&mut self) -> *mut u8;
  /// Returns the buffer's total capacity (maximum bytes that can be read into it).
  fn capacity(&self) -> usize;
}

// Implementations for Vec<u8>
unsafe impl IoBuf for Vec<u8> {
  fn as_ptr(&self) -> *const u8 {
    Vec::as_ptr(self)
  }

  fn len(&self) -> usize {
    Vec::len(self)
  }
}

unsafe impl IoBufMut for Vec<u8> {
  fn as_mut_ptr(&mut self) -> *mut u8 {
    Vec::as_mut_ptr(self)
  }

  fn capacity(&self) -> usize {
    Vec::capacity(self)
  }
}

// Implementations for Box<[u8]>
unsafe impl IoBuf for Box<[u8]> {
  fn as_ptr(&self) -> *const u8 {
    <[u8]>::as_ptr(self)
  }

  fn len(&self) -> usize {
    <[u8]>::len(self)
  }
}

unsafe impl IoBufMut for Box<[u8]> {
  fn as_mut_ptr(&mut self) -> *mut u8 {
    <[u8]>::as_mut_ptr(self)
  }

  fn capacity(&self) -> usize {
    // Box<[u8]> has fixed size, capacity == len
    <[u8]>::len(self)
  }
}

// ============================================================================
// File Target Types
// ============================================================================

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
// Internal Request Types (just pointers, caller owns the buffer)
// ============================================================================

struct ReadRequest {
  target: Target,
  buf_ptr: *mut u8,
  buf_len: u32,
  offset: u64,
}

// SAFETY: The pointer is to heap-allocated memory owned by the caller's future, which awaits completion. The pointer is only dereferenced by the kernel, not by our threads.
unsafe impl Send for ReadRequest {}
unsafe impl Sync for ReadRequest {}

struct WriteRequest {
  target: Target,
  buf_ptr: *const u8,
  buf_len: u32,
  offset: u64,
}

// SAFETY: The pointer is to heap-allocated memory owned by the caller's future, which awaits completion. The pointer is only dereferenced by the kernel, not by our threads.
unsafe impl Send for WriteRequest {}
unsafe impl Sync for WriteRequest {}

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

struct OpenAtRequest {
  /// Directory fd for relative paths, or AT_FDCWD for current directory.
  dir_fd: RawFd,
  /// Path to open. Owned to ensure validity until completion.
  path: CString,
  flags: i32,
  mode: u32,
}

struct StatxPathRequest {
  /// Directory fd for relative paths, or AT_FDCWD for current directory.
  dir_fd: RawFd,
  /// Path to stat. Owned to ensure validity until completion.
  path: CString,
  flags: i32,
  statx_buf: Box<MaybeUninit<libc::statx>>,
}

struct CloseRequest {
  fd: RawFd,
}

struct RenameAtRequest {
  old_dir_fd: RawFd,
  old_path: CString,
  new_dir_fd: RawFd,
  new_path: CString,
  flags: u32,
}

struct UnlinkAtRequest {
  dir_fd: RawFd,
  path: CString,
  flags: i32,
}

struct MkdirAtRequest {
  dir_fd: RawFd,
  path: CString,
  mode: u32,
}

struct SymlinkAtRequest {
  new_dir_fd: RawFd,
  target: CString,
  link_path: CString,
}

struct LinkAtRequest {
  old_dir_fd: RawFd,
  old_path: CString,
  new_dir_fd: RawFd,
  new_path: CString,
  flags: i32,
}

// ============================================================================
// Response Types
// ============================================================================

/// Result of a read operation: the buffer and actual bytes read.
pub struct ReadResult<B> {
  /// The buffer containing the data read.
  pub buf: B,
  /// Number of bytes actually read (may be less than buffer capacity at EOF).
  pub bytes_read: usize,
}

/// Result of a write operation: the buffer and actual bytes written.
pub struct WriteResult<B> {
  /// The original buffer (returned for reuse).
  pub buf: B,
  /// Number of bytes actually written (may be less than buffer size for non-regular files).
  pub bytes_written: usize,
}

// ============================================================================
// Request Enum
// ============================================================================

enum Message {
  Read {
    req: ReadRequest,
    res: oneshot::Sender<io::Result<usize>>,
  },
  Write {
    req: WriteRequest,
    res: oneshot::Sender<io::Result<usize>>,
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
  OpenAt {
    req: OpenAtRequest,
    res: oneshot::Sender<io::Result<OwnedFd>>,
  },
  StatxPath {
    req: StatxPathRequest,
    res: oneshot::Sender<io::Result<Metadata>>,
  },
  Close {
    req: CloseRequest,
    res: oneshot::Sender<io::Result<()>>,
  },
  RenameAt {
    req: RenameAtRequest,
    res: oneshot::Sender<io::Result<()>>,
  },
  UnlinkAt {
    req: UnlinkAtRequest,
    res: oneshot::Sender<io::Result<()>>,
  },
  MkdirAt {
    req: MkdirAtRequest,
    res: oneshot::Sender<io::Result<()>>,
  },
  SymlinkAt {
    req: SymlinkAtRequest,
    res: oneshot::Sender<io::Result<()>>,
  },
  LinkAt {
    req: LinkAtRequest,
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
///     // Read with library-allocated buffer
///     let result = uring.read_at(&file, 0, 1024).await?;
///     
///     // Read into user-provided buffer (zero-copy for custom allocators)
///     let buf = vec![0u8; 1024];
///     let result = uring.read_into(&file, 0, buf).await?;
///     
///     // Register file for reduced per-operation overhead
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
  sender: crossbeam_channel::Sender<Message>,
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
fn handle_completion(msg: Message, result: i32) {
  let result: io::Result<i32> = if result < 0 {
    Err(io::Error::from_raw_os_error(-result))
  } else {
    Ok(result)
  };

  match msg {
    Message::Read { res, .. } => {
      let _ = res.send(result.map(|n| n as usize));
    }
    Message::Write { res, .. } => {
      let _ = res.send(result.map(|n| n as usize));
    }
    Message::Sync { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Message::Statx { req, res } => {
      let outcome = result.map(|_| {
        // SAFETY: The kernel has initialized the statx buffer
        let statx = unsafe { (*req.statx_buf).assume_init() };
        Metadata(statx)
      });
      let _ = res.send(outcome);
    }
    Message::Fallocate { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Message::Fadvise { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Message::Ftruncate { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Message::OpenAt { res, .. } => {
      let outcome = result.map(|fd| {
        // SAFETY: The kernel returns a valid fd on success
        unsafe { OwnedFd::from_raw_fd(fd) }
      });
      let _ = res.send(outcome);
    }
    Message::StatxPath { req, res } => {
      let outcome = result.map(|_| {
        // SAFETY: The kernel has initialized the statx buffer
        let statx = unsafe { (*req.statx_buf).assume_init() };
        Metadata(statx)
      });
      let _ = res.send(outcome);
    }
    Message::Close { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Message::RenameAt { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Message::UnlinkAt { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Message::MkdirAt { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Message::SymlinkAt { res, .. } => {
      let _ = res.send(result.map(|_| ()));
    }
    Message::LinkAt { res, .. } => {
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
    let (sender, receiver) = crossbeam_channel::unbounded::<Message>();
    let pending: Arc<DashMap<u64, Message>> = Default::default();

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
              Message::Read { req, .. } => {
                build_op!(req.target, |fd| opcode::Read::new(
                  fd,
                  req.buf_ptr,
                  req.buf_len
                )
                .offset(req.offset)
                .build()
                .user_data(id))
              }
              Message::Write { req, .. } => {
                build_op!(req.target, |fd| opcode::Write::new(
                  fd,
                  req.buf_ptr,
                  req.buf_len
                )
                .offset(req.offset)
                .build()
                .user_data(id))
              }
              Message::Sync { req, .. } => {
                build_op!(req.target, |fd| {
                  let mut fsync = opcode::Fsync::new(fd);
                  if req.datasync {
                    fsync = fsync.flags(types::FsyncFlags::DATASYNC);
                  }
                  fsync.build().user_data(id)
                })
              }
              Message::Statx { req, .. } => {
                const STATX_BASIC_STATS: u32 = 0x000007ff; // Request all basic stat fields
                const AT_EMPTY_PATH: i32 = 0x1000; // Interpret fd as the file itself, not a directory
                static EMPTY_PATH: &std::ffi::CStr = c""; // Empty path since we use AT_EMPTY_PATH

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
              Message::Fallocate { req, .. } => {
                build_op!(req.target, |fd| opcode::Fallocate::new(fd, req.len)
                  .offset(req.offset)
                  .mode(req.mode)
                  .build()
                  .user_data(id))
              }
              Message::Fadvise { req, .. } => {
                build_op!(req.target, |fd| opcode::Fadvise::new(
                  fd,
                  req.len as i64,
                  req.advice
                )
                .offset(req.offset)
                .build()
                .user_data(id))
              }
              Message::Ftruncate { req, .. } => {
                build_op!(req.target, |fd| opcode::Ftruncate::new(fd, req.len)
                  .build()
                  .user_data(id))
              }
              Message::OpenAt { req, .. } => {
                opcode::OpenAt::new(types::Fd(req.dir_fd), req.path.as_ptr())
                  .flags(req.flags)
                  .mode(req.mode)
                  .build()
                  .user_data(id)
              }
              Message::StatxPath { req, .. } => {
                const STATX_BASIC_STATS: u32 = 0x000007ff;
                let statx_ptr = req.statx_buf.as_ptr() as *mut types::statx;

                opcode::Statx::new(types::Fd(req.dir_fd), req.path.as_ptr(), statx_ptr)
                  .flags(req.flags)
                  .mask(STATX_BASIC_STATS)
                  .build()
                  .user_data(id)
              }
              Message::Close { req, .. } => {
                opcode::Close::new(types::Fd(req.fd)).build().user_data(id)
              }
              Message::RenameAt { req, .. } => {
                opcode::RenameAt::new(
                  types::Fd(req.old_dir_fd),
                  req.old_path.as_ptr(),
                  types::Fd(req.new_dir_fd),
                  req.new_path.as_ptr(),
                )
                .flags(req.flags)
                .build()
                .user_data(id)
              }
              Message::UnlinkAt { req, .. } => {
                opcode::UnlinkAt::new(types::Fd(req.dir_fd), req.path.as_ptr())
                  .flags(req.flags)
                  .build()
                  .user_data(id)
              }
              Message::MkdirAt { req, .. } => {
                opcode::MkDirAt::new(types::Fd(req.dir_fd), req.path.as_ptr())
                  .mode(req.mode)
                  .build()
                  .user_data(id)
              }
              Message::SymlinkAt { req, .. } => {
                opcode::SymlinkAt::new(
                  types::Fd(req.new_dir_fd),
                  req.target.as_ptr(),
                  req.link_path.as_ptr(),
                )
                .build()
                .user_data(id)
              }
              Message::LinkAt { req, .. } => {
                opcode::LinkAt::new(
                  types::Fd(req.old_dir_fd),
                  req.old_path.as_ptr(),
                  types::Fd(req.new_dir_fd),
                  req.new_path.as_ptr(),
                )
                .flags(req.flags)
                .build()
                .user_data(id)
              }
            };

            // Insert before submitting so the completion handler can find it.
            pending.insert(id, msg);

            if submission.is_full() {
              submission.sync();
              ring.submit_and_wait(1).unwrap();
            }

            // SAFETY: The submission entry references memory owned by the caller's future, which is awaiting completion.
            unsafe {
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

  /// Send a message to the submission thread.
  fn send(&self, msg: Message) {
    self.sender.send(msg).expect("uring submission thread dead");
  }

  /// Read into a user-provided buffer. This is the primitive read operation that accepts any buffer type implementing [`IoBufMut`].
  ///
  /// The buffer is returned along with the number of bytes read. This allows buffer reuse and supports custom allocators (e.g., aligned buffers for O_DIRECT).
  pub async fn read_into<B: IoBufMut>(
    &self,
    file: &impl UringTarget,
    offset: u64,
    mut buf: B,
  ) -> io::Result<ReadResult<B>> {
    let target = file.as_target(&self.identity);
    let ptr = buf.as_mut_ptr();
    let cap = buf.capacity();
    let (tx, rx) = oneshot::channel();
    self.send(Message::Read {
      req: ReadRequest {
        target,
        buf_ptr: ptr,
        buf_len: cap.try_into().unwrap(),
        offset,
      },
      res: tx,
    });
    let bytes_read = rx.await.expect("uring completion channel dropped")?;
    Ok(ReadResult { buf, bytes_read })
  }

  /// Read from a file at the specified offset, allocating a buffer internally.
  ///
  /// This is a convenience wrapper around [`read_into`](Self::read_into) that allocates a `Vec<u8>`. For zero-copy or custom allocators, use `read_into` directly.
  pub async fn read_at(
    &self,
    file: &impl UringTarget,
    offset: u64,
    len: u64,
  ) -> io::Result<ReadResult<Vec<u8>>> {
    let buf = vec![0u8; len.try_into().unwrap()];
    self.read_into(file, offset, buf).await
  }

  /// Write a buffer to a file at the specified offset. Accepts any buffer type implementing [`IoBuf`].
  ///
  /// The buffer is returned along with the number of bytes written. This allows buffer reuse and supports custom allocators.
  pub async fn write_at<B: IoBuf>(
    &self,
    file: &impl UringTarget,
    offset: u64,
    buf: B,
  ) -> io::Result<WriteResult<B>> {
    let target = file.as_target(&self.identity);
    let ptr = buf.as_ptr();
    let len = buf.len();
    let (tx, rx) = oneshot::channel();
    self.send(Message::Write {
      req: WriteRequest {
        target,
        buf_ptr: ptr,
        buf_len: len.try_into().unwrap(),
        offset,
      },
      res: tx,
    });
    let bytes_written = rx.await.expect("uring completion channel dropped")?;
    Ok(WriteResult { buf, bytes_written })
  }

  /// Synchronize file data and metadata to disk (fsync). This ensures that all data and metadata modifications are flushed to the underlying storage device. Even when using direct I/O, this is necessary to ensure the device itself has flushed any internal caches.
  ///
  /// **Note on ordering**: io_uring does not guarantee ordering between operations. If you need to ensure writes complete before fsync, you should await the write first, then call fsync.
  pub async fn sync(&self, file: &impl UringTarget) -> io::Result<()> {
    let target = file.as_target(&self.identity);
    let (tx, rx) = oneshot::channel();
    self.send(Message::Sync {
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
    self.send(Message::Sync {
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
    let statx_buf = Box::new(MaybeUninit::<libc::statx>::uninit());
    let (tx, rx) = oneshot::channel();
    self.send(Message::Statx {
      req: StatxRequest { target, statx_buf },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Pre-allocate or deallocate space for a file (fallocate). This can be used to pre-allocate space to avoid fragmentation, punch holes in sparse files, or zero-fill regions. Use `libc::FALLOC_FL_*` constants for mode flags. Requires Linux 5.6+.
  pub async fn fallocate(
    &self,
    file: &impl UringTarget,
    offset: u64,
    len: u64,
    mode: i32,
  ) -> io::Result<()> {
    let target = file.as_target(&self.identity);
    let (tx, rx) = oneshot::channel();
    self.send(Message::Fallocate {
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

  /// Advise the kernel about expected file access patterns (fadvise). This is a hint to the kernel about how you intend to access a file region. The kernel may use this to optimize readahead, caching, etc. Use `libc::POSIX_FADV_*` constants for advice values. Requires Linux 5.6+.
  pub async fn fadvise(
    &self,
    file: &impl UringTarget,
    offset: u64,
    len: u32,
    advice: i32,
  ) -> io::Result<()> {
    let target = file.as_target(&self.identity);
    let (tx, rx) = oneshot::channel();
    self.send(Message::Fadvise {
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
    self.send(Message::Ftruncate {
      req: FtruncateRequest { target, len },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Open a file asynchronously. This is the io_uring equivalent of `open(2)`/`openat(2)`. Requires Linux 5.6+.
  ///
  /// # Arguments
  ///
  /// * `path` - Path to open (any `CStr`-like type: `&CStr`, `CString`, `c"literal"`).
  /// * `flags` - Open flags from `libc` (e.g., `libc::O_RDONLY`, `libc::O_RDWR | libc::O_CREAT`).
  /// * `mode` - File mode for creation (only used with `O_CREAT`).
  ///
  /// # Returns
  ///
  /// Returns an `OwnedFd` on success. The fd is automatically closed when dropped.
  pub async fn open(&self, path: impl AsRef<Path>, flags: i32, mode: u32) -> io::Result<OwnedFd> {
    self.open_at(libc::AT_FDCWD, path, flags, mode).await
  }

  /// Open a file relative to a directory fd. This is useful for safe path traversal and avoiding TOCTOU races. Requires Linux 5.6+.
  ///
  /// # Arguments
  ///
  /// * `dir_fd` - Directory fd for relative paths, or `libc::AT_FDCWD` for current directory.
  /// * `path` - Path to open relative to `dir_fd`.
  /// * `flags` - Open flags.
  /// * `mode` - File mode for creation.
  pub async fn open_at(
    &self,
    dir_fd: RawFd,
    path: impl AsRef<Path>,
    flags: i32,
    mode: u32,
  ) -> io::Result<OwnedFd> {
    let path = path_to_cstring(path.as_ref())?;
    let (tx, rx) = oneshot::channel();
    self.send(Message::OpenAt {
      req: OpenAtRequest {
        dir_fd,
        path,
        flags,
        mode,
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Get file metadata by path without opening the file. This is the io_uring equivalent of `stat(2)`/`statx(2)`. Requires Linux 5.6+.
  ///
  /// Unlike `statx()` which operates on an open fd, this method stats the path directly.
  pub async fn statx_path(&self, path: impl AsRef<Path>) -> io::Result<Metadata> {
    self.statx_at(libc::AT_FDCWD, path, 0).await
  }

  /// Get file metadata relative to a directory fd. This allows safe path traversal and additional flags for controlling symlink behavior. Requires Linux 5.6+.
  ///
  /// # Arguments
  ///
  /// * `dir_fd` - Directory fd for relative paths, or `libc::AT_FDCWD` for current directory.
  /// * `path` - Path to stat relative to `dir_fd`.
  /// * `flags` - Flags from `libc` (e.g., `libc::AT_SYMLINK_NOFOLLOW` to not follow symlinks).
  pub async fn statx_at(
    &self,
    dir_fd: RawFd,
    path: impl AsRef<Path>,
    flags: i32,
  ) -> io::Result<Metadata> {
    let path = path_to_cstring(path.as_ref())?;
    let statx_buf = Box::new(MaybeUninit::<libc::statx>::uninit());
    let (tx, rx) = oneshot::channel();
    self.send(Message::StatxPath {
      req: StatxPathRequest {
        dir_fd,
        path,
        flags,
        statx_buf,
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Close a file descriptor asynchronously. This is the io_uring equivalent of `close(2)`. Requires Linux 5.6+.
  ///
  /// Takes ownership of the fd to prevent the automatic synchronous close on drop. This is useful when you want to:
  /// - Handle close errors (which are silently ignored by `OwnedFd::drop`)
  /// - Batch close operations with other io_uring operations
  /// - Avoid blocking the async runtime on close
  pub async fn close(&self, fd: impl IntoRawFd) -> io::Result<()> {
    let raw_fd = fd.into_raw_fd();
    let (tx, rx) = oneshot::channel();
    self.send(Message::Close {
      req: CloseRequest { fd: raw_fd },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Rename a file asynchronously. This is the io_uring equivalent of `rename(2)`. Requires Linux 5.11+.
  pub async fn rename(
    &self,
    old_path: impl AsRef<Path>,
    new_path: impl AsRef<Path>,
  ) -> io::Result<()> {
    self
      .rename_at(libc::AT_FDCWD, old_path, libc::AT_FDCWD, new_path, 0)
      .await
  }

  /// Rename a file relative to directory fds. This is the io_uring equivalent of `renameat2(2)`. Requires Linux 5.11+.
  ///
  /// Flags can include `libc::RENAME_NOREPLACE`, `libc::RENAME_EXCHANGE`, etc.
  pub async fn rename_at(
    &self,
    old_dir_fd: RawFd,
    old_path: impl AsRef<Path>,
    new_dir_fd: RawFd,
    new_path: impl AsRef<Path>,
    flags: u32,
  ) -> io::Result<()> {
    let old_path = path_to_cstring(old_path.as_ref())?;
    let new_path = path_to_cstring(new_path.as_ref())?;
    let (tx, rx) = oneshot::channel();
    self.send(Message::RenameAt {
      req: RenameAtRequest {
        old_dir_fd,
        old_path,
        new_dir_fd,
        new_path,
        flags,
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Delete a file or empty directory. This is the io_uring equivalent of `unlink(2)`. Requires Linux 5.11+.
  pub async fn unlink(&self, path: impl AsRef<Path>) -> io::Result<()> {
    self.unlink_at(libc::AT_FDCWD, path, 0).await
  }

  /// Delete a directory. This is the io_uring equivalent of `rmdir(2)`. Requires Linux 5.11+.
  pub async fn rmdir(&self, path: impl AsRef<Path>) -> io::Result<()> {
    self.unlink_at(libc::AT_FDCWD, path, libc::AT_REMOVEDIR).await
  }

  /// Delete a file or directory relative to a directory fd. This is the io_uring equivalent of `unlinkat(2)`. Requires Linux 5.11+.
  ///
  /// Use `libc::AT_REMOVEDIR` flag to remove directories.
  pub async fn unlink_at(
    &self,
    dir_fd: RawFd,
    path: impl AsRef<Path>,
    flags: i32,
  ) -> io::Result<()> {
    let path = path_to_cstring(path.as_ref())?;
    let (tx, rx) = oneshot::channel();
    self.send(Message::UnlinkAt {
      req: UnlinkAtRequest { dir_fd, path, flags },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Create a directory. This is the io_uring equivalent of `mkdir(2)`. Requires Linux 5.15+.
  pub async fn mkdir(&self, path: impl AsRef<Path>, mode: u32) -> io::Result<()> {
    self.mkdir_at(libc::AT_FDCWD, path, mode).await
  }

  /// Create a directory relative to a directory fd. This is the io_uring equivalent of `mkdirat(2)`. Requires Linux 5.15+.
  pub async fn mkdir_at(
    &self,
    dir_fd: RawFd,
    path: impl AsRef<Path>,
    mode: u32,
  ) -> io::Result<()> {
    let path = path_to_cstring(path.as_ref())?;
    let (tx, rx) = oneshot::channel();
    self.send(Message::MkdirAt {
      req: MkdirAtRequest { dir_fd, path, mode },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Create a symbolic link. This is the io_uring equivalent of `symlink(2)`. Requires Linux 5.11+.
  pub async fn symlink(
    &self,
    target: impl AsRef<Path>,
    link_path: impl AsRef<Path>,
  ) -> io::Result<()> {
    self.symlink_at(target, libc::AT_FDCWD, link_path).await
  }

  /// Create a symbolic link relative to a directory fd. This is the io_uring equivalent of `symlinkat(2)`. Requires Linux 5.11+.
  pub async fn symlink_at(
    &self,
    target: impl AsRef<Path>,
    new_dir_fd: RawFd,
    link_path: impl AsRef<Path>,
  ) -> io::Result<()> {
    let target = path_to_cstring(target.as_ref())?;
    let link_path = path_to_cstring(link_path.as_ref())?;
    let (tx, rx) = oneshot::channel();
    self.send(Message::SymlinkAt {
      req: SymlinkAtRequest {
        new_dir_fd,
        target,
        link_path,
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }

  /// Create a hard link. This is the io_uring equivalent of `link(2)`. Requires Linux 5.11+.
  pub async fn hard_link(
    &self,
    original: impl AsRef<Path>,
    link: impl AsRef<Path>,
  ) -> io::Result<()> {
    self
      .hard_link_at(libc::AT_FDCWD, original, libc::AT_FDCWD, link, 0)
      .await
  }

  /// Create a hard link relative to directory fds. This is the io_uring equivalent of `linkat(2)`. Requires Linux 5.11+.
  pub async fn hard_link_at(
    &self,
    old_dir_fd: RawFd,
    original: impl AsRef<Path>,
    new_dir_fd: RawFd,
    link: impl AsRef<Path>,
    flags: i32,
  ) -> io::Result<()> {
    let old_path = path_to_cstring(original.as_ref())?;
    let new_path = path_to_cstring(link.as_ref())?;
    let (tx, rx) = oneshot::channel();
    self.send(Message::LinkAt {
      req: LinkAtRequest {
        old_dir_fd,
        old_path,
        new_dir_fd,
        new_path,
        flags,
      },
      res: tx,
    });
    rx.await.expect("uring completion channel dropped")
  }
}
