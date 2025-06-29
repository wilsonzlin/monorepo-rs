#![cfg(target_os = "linux")]
#![allow(async_fn_in_trait)]

use crate::uring::Uring;
use crate::uring::UringCfg;
use once_cell::sync::Lazy;
use std::fs::File as StdFile;
use tokio::fs::File as TokioFile;

pub mod uring;

static DEFAULT_URING: Lazy<Uring> = Lazy::new(|| Uring::new(UringCfg::default()));

pub trait UringFile {
  async fn ur_read_at(&self, offset: u64, len: u64) -> Vec<u8>;
  async fn ur_write_at(&self, offset: u64, data: Vec<u8>) -> Vec<u8>;
  async fn ur_sync(&self);
}

impl UringFile for StdFile {
  async fn ur_read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    DEFAULT_URING.read_at(self, offset, len).await
  }

  async fn ur_write_at(&self, offset: u64, data: Vec<u8>) -> Vec<u8> {
    DEFAULT_URING.write_at(self, offset, data).await
  }

  async fn ur_sync(&self) {
    DEFAULT_URING.sync(self).await
  }
}

impl UringFile for TokioFile {
  async fn ur_read_at(&self, offset: u64, len: u64) -> Vec<u8> {
    DEFAULT_URING.read_at(self, offset, len).await
  }

  async fn ur_write_at(&self, offset: u64, data: Vec<u8>) -> Vec<u8> {
    DEFAULT_URING.write_at(self, offset, data).await
  }

  async fn ur_sync(&self) {
    DEFAULT_URING.sync(self).await
  }
}
