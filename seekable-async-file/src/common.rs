use async_trait::async_trait;
use off64::chrono::Off64AsyncReadChrono;
use off64::chrono::Off64AsyncWriteChrono;
use off64::chrono::Off64ReadChrono;
use off64::chrono::Off64WriteChrono;
use off64::int::Off64AsyncReadInt;
use off64::int::Off64AsyncWriteInt;
use off64::int::Off64ReadInt;
use off64::int::Off64WriteInt;
use off64::Off64AsyncRead;
use off64::Off64AsyncWrite;
use off64::Off64Read;
use off64::Off64Write;
use std::sync::Arc;
use tokio::task::spawn_blocking;

// It's assumed that a type that implements this can become async optimally by wrapping in Arc and using spawn_blocking.
// You can use SyncIOAsyncAdapter to wrap a SyncIO in an IAsyncIO.
// This also allows you to continue using the underlying concrete SyncIO (e.g. mmap raw pointers) or trait methods (e.g. `read_at_sync`) as you can keep an Arc clone.
pub trait ISyncIO: Send + Sync {
  fn read_at_sync(&self, offset: u64, len: u64) -> Vec<u8>;
  fn write_at_sync(&self, offset: u64, data: &[u8]) -> ();
  fn sync_data_sync(&self) -> ();
}

impl<'a> Off64Read<'a, Vec<u8>> for dyn ISyncIO {
  fn read_at(&'a self, offset: u64, len: u64) -> Vec<u8> {
    self.read_at_sync(offset, len)
  }
}
impl<'a> Off64ReadChrono<'a, Vec<u8>> for dyn ISyncIO {}
impl<'a> Off64ReadInt<'a, Vec<u8>> for dyn ISyncIO {}

impl Off64Write for dyn ISyncIO {
  fn write_at(&self, offset: u64, value: &[u8]) -> () {
    self.write_at_sync(offset, value)
  }
}
impl Off64WriteChrono for dyn ISyncIO {}
impl Off64WriteInt for dyn ISyncIO {}

#[async_trait]
pub trait IAsyncIO: for<'a> Off64AsyncRead<'a, Vec<u8>> + Off64AsyncWrite + Send + Sync {
  async fn sync_data(&self) -> ();
}

impl<'a> Off64AsyncReadChrono<'a, Vec<u8>> for dyn IAsyncIO {}
impl<'a> Off64AsyncReadInt<'a, Vec<u8>> for dyn IAsyncIO {}

impl Off64AsyncWriteChrono for dyn IAsyncIO {}
impl Off64AsyncWriteInt for dyn IAsyncIO {}

pub struct SyncIOAsyncAdapter(Arc<dyn ISyncIO>);

impl SyncIOAsyncAdapter {
  pub fn new<I: ISyncIO + 'static>(sync_io: Arc<I>) -> Self {
    Self(sync_io)
  }
}

#[async_trait]
impl<'a> Off64AsyncRead<'a, Vec<u8>> for SyncIOAsyncAdapter {
  async fn read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    let io = self.0.clone();
    spawn_blocking(move || io.read_at_sync(offset, len))
      .await
      .unwrap()
  }
}

#[async_trait]
impl Off64AsyncWrite for SyncIOAsyncAdapter {
  async fn write_at(&self, offset: u64, value: &[u8]) -> () {
    let io = self.0.clone();
    let value = value.to_vec();
    spawn_blocking(move || io.write_at_sync(offset, &value))
      .await
      .unwrap()
  }
}

#[async_trait]
impl IAsyncIO for SyncIOAsyncAdapter {
  async fn sync_data(&self) -> () {
    let io = self.0.clone();
    spawn_blocking(move || io.sync_data_sync()).await.unwrap()
  }
}
