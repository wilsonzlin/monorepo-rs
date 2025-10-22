use crate::common::ISyncIO;
use off64::usz;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::OpenOptions;

/// A `File`-like value that can perform async `read_at` and `write_at` for I/O at specific offsets without mutating any state (i.e. is thread safe). Metrics are collected, and syncs can be delayed for write batching opportunities as a performance optimisation.
pub struct MMapIO {
  mmap: Arc<memmap2::MmapRaw>,
  mmap_len: usize,
}

impl MMapIO {
  /// Open a file descriptor in read and write modes, creating it if it doesn't exist. If it already exists, the contents won't be truncated.
  ///
  /// If the mmap feature is being used, to save a `stat` call, the size must be provided. This also allows opening non-standard files which may have a size of zero (e.g. block devices). A different size value also allows only using a portion of the beginning of the file.
  ///
  /// The `io_direct` and `io_dsync` parameters set the `O_DIRECT` and `O_DSYNC` flags, respectively. Unless you need those flags, provide `false`.
  ///
  /// Make sure to execute `start_delayed_data_sync_background_loop` in the background after this call.
  pub async fn open(path: &Path, size: u64, flags: i32) -> Self {
    let async_fd = OpenOptions::new()
      .read(true)
      .write(true)
      .custom_flags(flags)
      .open(path)
      .await
      .unwrap();

    let fd = async_fd.into_std().await;

    Self {
      mmap: Arc::new(memmap2::MmapRaw::map_raw(&fd).unwrap()),
      mmap_len: usz!(size),
    }
  }

  pub unsafe fn get_mmap_raw_ptr(&self, offset: u64) -> *const u8 {
    self.mmap.as_ptr().add(usz!(offset))
  }

  pub unsafe fn get_mmap_raw_mut_ptr(&self, offset: u64) -> *mut u8 {
    self.mmap.as_mut_ptr().add(usz!(offset))
  }
}

impl ISyncIO for MMapIO {
  fn read_at_sync(&self, offset: u64, len: u64) -> Vec<u8> {
    let offset = usz!(offset);
    let len = usz!(len);
    let memory = unsafe { std::slice::from_raw_parts(self.mmap.as_ptr(), self.mmap_len) };
    memory[offset..offset + len].to_vec()
  }

  fn write_at_sync(&self, offset: u64, data: &[u8]) -> () {
    let offset = usz!(offset);
    let len = data.as_ref().len();

    let memory = unsafe { std::slice::from_raw_parts_mut(self.mmap.as_mut_ptr(), self.mmap_len) };
    memory[offset..offset + len].copy_from_slice(data.as_ref());
  }

  fn sync_data_sync(&self) -> () {
    self.mmap.flush().unwrap();
  }
}
