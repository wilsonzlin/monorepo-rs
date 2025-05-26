use futures::Stream;
use std::io;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use tokio::runtime::Handle;

const DEFAULT_BUFFER_SIZE: usize = 1024 * 16;

struct State<R: Read + Send + Sync + 'static> {
  readable: R,
  res: Option<Result<Option<Vec<u8>>, io::Error>>,
}

pub struct SyncReadStream<R: Read + Send + Sync + 'static> {
  // An `Arc<Mutex<>>` is probably unnecessary, but it's fully uncontended and rarely cloned (both assuming `poll_next` is never called until `waker.wake()`), and easy to work with.
  state: Arc<Mutex<State<R>>>,
  tokio: Handle,
  buffer_size: usize,
}

impl<R: Read + Send + Sync + 'static> SyncReadStream<R> {
  /// This must be called from within a Tokio runtime context, or else it will panic.
  pub fn with_tokio_handle_and_buffer_size(tokio: Handle, readable: R, buffer_size: usize) -> Self {
    Self {
      tokio,
      buffer_size,
      state: Arc::new(Mutex::new(State {
        readable,
        res: None,
      })),
    }
  }

  /// This must be called from within a Tokio runtime context, or else it will panic.
  pub fn with_tokio_handle(tokio: Handle, readable: R) -> Self {
    Self::with_tokio_handle_and_buffer_size(tokio, readable, DEFAULT_BUFFER_SIZE)
  }

  /// This must be called from within a Tokio runtime context, or else it will panic.
  pub fn with_buffer_size(readable: R, buffer_size: usize) -> Self {
    Self::with_tokio_handle_and_buffer_size(Handle::current(), readable, buffer_size)
  }
}

impl<R: Read + Send + Sync> Stream for SyncReadStream<R> {
  type Item = Result<Vec<u8>, io::Error>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
    let buffer_size = self.buffer_size;
    let mut state = self.state.lock().unwrap();
    if let Some(res) = state.res.take() {
      return Poll::Ready(res.transpose());
    };
    let waker = cx.waker().clone();
    drop(state);
    let state = Arc::clone(&self.state);
    self.tokio.spawn_blocking(move || {
      let mut state = state.lock().unwrap();
      let mut buf = vec![0u8; buffer_size];
      state.res = Some(match state.readable.read(&mut buf) {
        Ok(n) if n == 0 => Ok(None),
        Ok(n) => {
          buf.truncate(n);
          Ok(Some(buf))
        }
        Err(err) => Err(err),
      });
      waker.wake();
    });
    Poll::Pending
  }
}

impl<R: Read + Send + Sync + 'static> From<R> for SyncReadStream<R> {
  /// This must be called from within a Tokio runtime context, or else it will panic.
  fn from(value: R) -> Self {
    Self::with_buffer_size(value, DEFAULT_BUFFER_SIZE)
  }
}
