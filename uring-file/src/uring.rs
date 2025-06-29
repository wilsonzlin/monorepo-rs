use dashmap::DashMap;
use io_uring::IoUring;
use io_uring::cqueue::Entry as CEntry;
use io_uring::opcode;
use io_uring::squeue::Entry as SEntry;
use io_uring::types;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::thread;
use tokio::sync::oneshot;

fn assert_result_is_ok(req: &Request, res: i32) -> u32 {
  if res < 0 {
    panic!(
      "{:?} failed with {:?}",
      req,
      io::Error::from_raw_os_error(-res)
    );
  };
  res.try_into().unwrap()
}

struct ReadRequest {
  fd: RawFd,
  out_buf: Vec<u8>,
  offset: u64,
  len: u32,
}

struct WriteRequest {
  fd: RawFd,
  offset: u64,
  data: Vec<u8>,
}

struct SyncRequest {
  fd: RawFd,
}

enum Request {
  Read {
    req: ReadRequest,
    res: oneshot::Sender<Vec<u8>>,
  },
  Write {
    req: WriteRequest,
    res: oneshot::Sender<Vec<u8>>,
  },
  Sync {
    req: SyncRequest,
    res: oneshot::Sender<()>,
  },
}

impl Debug for Request {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Read { req, .. } => write!(f, "read {} len {}", req.offset, req.len),
      Self::Write { req, .. } => write!(f, "write {} len {}", req.offset, req.data.len()),
      Self::Sync { .. } => write!(f, "sync"),
    }
  }
}

// For now, we just use one ring, with one submitter on one thread and one receiver on another thread. While there are possibly faster ways, like one ring per thread and using one thread for both submitting and receiving (to avoid any locking), the actual I/O should be the bottleneck so we can just stick with this for now.
/// This can be cheaply cloned.
#[derive(Clone)]
pub struct Uring {
  // We don't use std::sync::mpsc::Sender as it is not Sync, so it's really complicated to use from any async function.
  sender: crossbeam_channel::Sender<Request>,
}

/// For advanced users only. Some of these may cause EINVAL, or worsen performance.
#[derive(Clone, Default, Debug)]
pub struct UringCfg {
  pub coop_taskrun: bool,
  pub defer_taskrun: bool,
  pub iopoll: bool,
  /// This requires CAP_SYS_NICE.
  pub sqpoll: Option<u32>,
}

impl Uring {
  pub fn new(cfg: UringCfg) -> Self {
    let (sender, receiver) = crossbeam_channel::unbounded::<Request>();
    let pending: Arc<DashMap<u64, Request>> = Default::default();
    let ring = {
      let mut builder = IoUring::<SEntry, CEntry>::builder();
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
      builder.build(134217728).unwrap()
    };
    let ring = Arc::new(ring);
    // Submission thread.
    thread::spawn({
      let pending = pending.clone();
      let ring = ring.clone();
      // This is outside the loop to avoid reallocation each time.
      let mut msgbuf = VecDeque::new();
      move || {
        let mut submission = unsafe { ring.submission_shared() };
        let mut next_id = 0;
        // If this loop exits, it means we've dropped the `UringBackingStore` and can safely stop.
        while let Ok(init_msg) = receiver.recv() {
          // Process multiple messages at once to avoid too many io_uring submits.
          msgbuf.push_back(init_msg);
          while let Ok(msg) = receiver.try_recv() {
            msgbuf.push_back(msg);
          }
          // How the io_uring submission queue work:
          // - The buffer is shared between the kernel and userspace.
          // - There are atomic head and tail indices that allow them to be shared mutably between kernel and userspace safely.
          // - The Rust library we're using abstracts over this by caching the head and tail as local values. Once we've made our inserts, we update the atomic tail and then tell the kernel to consume some of the queue. When we update the atomic tail, we also check the atomic head and update our local cached value; some entries may have been consumed by the kernel in some other thread since we last checked and we may actually have more free space than we thought.
          while let Some(msg) = msgbuf.pop_front() {
            let id = next_id;
            next_id += 1;
            let submission_entry = match &msg {
              Request::Read { req, .. } => {
                // Using `as_mut_ptr` would require a mutable borrow.
                let ptr = req.out_buf.as_ptr() as *mut u8;
                opcode::Read::new(types::Fd(req.fd), ptr, req.len)
                  .offset(req.offset)
                  .build()
                  .user_data(id)
              }
              Request::Write { req, .. } => {
                let ptr = req.data.as_ptr();
                let len: u32 = req.data.len().try_into().unwrap();
                opcode::Write::new(types::Fd(req.fd), ptr, len)
                  .offset(req.offset)
                  .build()
                  .user_data(id)
              }
              Request::Sync { req, .. } => {
                opcode::Fsync::new(types::Fd(req.fd)).build().user_data(id)
              }
            };
            // Insert before submitting.
            pending.insert(id, msg);
            if submission.is_full() {
              submission.sync();
              ring.submit_and_wait(1).unwrap();
            }
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
        let mut completion = unsafe { ring.completion_shared() };
        // TODO Stop this loop if `UringBackingStore` has been dropped.
        loop {
          let Some(e) = completion.next() else {
            ring.submit_and_wait(1).unwrap();
            completion.sync();
            continue;
          };
          let id = e.user_data();
          let req = pending.remove(&id).unwrap().1;
          let rv = assert_result_is_ok(&req, e.result());
          match req {
            Request::Read { req, res } => {
              // We may have read fewer bytes.
              assert_eq!(usize::try_from(rv).unwrap(), req.out_buf.len());
              res.send(req.out_buf).unwrap();
            }
            Request::Write { req, res } => {
              // Assert that all requested bytes to write were written.
              assert_eq!(rv, u32::try_from(req.data.len()).unwrap());
              res.send(req.data).unwrap();
            }
            Request::Sync { res, .. } => {
              res.send(()).unwrap();
            }
          }
        }
      }
    });

    Self { sender }
  }
}

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
impl Uring {
  pub async fn read_at(&self, file: &impl AsRawFd, offset: u64, len: u64) -> Vec<u8> {
    let out_buf = vec![0u8; len.try_into().unwrap()];
    let (tx, rx) = oneshot::channel();
    self
      .sender
      .send(Request::Read {
        req: ReadRequest {
          fd: file.as_raw_fd(),
          out_buf,
          offset,
          len: len.try_into().unwrap(),
        },
        res: tx,
      })
      .unwrap();
    rx.await.unwrap()
  }

  /// Returns the original `data` so that it can be reused, if desired.
  pub async fn write_at(&self, file: &impl AsRawFd, offset: u64, data: Vec<u8>) -> Vec<u8> {
    let (tx, rx) = oneshot::channel();
    self
      .sender
      .send(Request::Write {
        req: WriteRequest {
          fd: file.as_raw_fd(),
          offset,
          data,
        },
        res: tx,
      })
      .unwrap();
    rx.await.unwrap()
  }

  /// Even when using direct I/O, `fsync` is still necessary, as it ensures the device itself has flushed any internal caches.
  pub async fn sync(&self, file: &impl AsRawFd) {
    let (tx, rx) = oneshot::channel();
    self
      .sender
      .send(Request::Sync {
        req: SyncRequest {
          fd: file.as_raw_fd(),
        },
        res: tx,
      })
      .unwrap();
    rx.await.unwrap();
  }
}
