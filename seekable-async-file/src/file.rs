use crate::common::ISyncIO;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use tokio::fs::OpenOptions;

/// A `File`-like value that can perform async `read_at` and `write_at` for I/O at specific offsets without mutating any state (i.e. is thread safe). Metrics are collected, and syncs can be delayed for write batching opportunities as a performance optimisation.
pub struct FileIO {
  // Tokio has still not implemented read_at and write_at: https://github.com/tokio-rs/tokio/issues/1529. We need these to be able to share a file descriptor across threads (e.g. use from within async function).
  // Apparently spawn_blocking is how Tokio does all file operations (as not all platforms have native async I/O), so our use is not worse but not optimised for async I/O either.
  fd: std::fs::File,
}

impl FileIO {
  /// Open a file descriptor in read and write modes, creating it if it doesn't exist. If it already exists, the contents won't be truncated.
  ///
  /// If the mmap feature is being used, to save a `stat` call, the size must be provided. This also allows opening non-standard files which may have a size of zero (e.g. block devices). A different size value also allows only using a portion of the beginning of the file.
  ///
  /// The `io_direct` and `io_dsync` parameters set the `O_DIRECT` and `O_DSYNC` flags, respectively. Unless you need those flags, provide `false`.
  ///
  /// Make sure to execute `start_delayed_data_sync_background_loop` in the background after this call.
  pub async fn open(path: &Path, flags: i32) -> Self {
    let async_fd = OpenOptions::new()
      .read(true)
      .write(true)
      .custom_flags(flags)
      .open(path)
      .await
      .unwrap();

    let fd = async_fd.into_std().await;

    FileIO { fd }
  }
}

impl ISyncIO for FileIO {
  fn read_at_sync(&self, offset: u64, len: u64) -> Vec<u8> {
    let mut buf = vec![0u8; len.try_into().unwrap()];
    self.fd.read_exact_at(&mut buf, offset).unwrap();
    buf
  }

  fn write_at_sync(&self, offset: u64, data: &[u8]) -> () {
    self.fd.write_all_at(data, offset).unwrap();
  }

  fn sync_data_sync(&self) -> () {
    self.fd.sync_data().unwrap();
  }
}
