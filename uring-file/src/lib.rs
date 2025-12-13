//! Async file I/O via Linux io_uring.
//!
//! This crate provides a high-level, async-compatible interface to Linux's io_uring for file operations. It is designed for high-performance scenarios where traditional threaded I/O or epoll-based async I/O is a bottleneck.
//!
//! # Features
//!
//! - **Async/await compatible**: Works with tokio and other async runtimes
//! - **Zero-copy where possible**: Buffers are passed through, not copied
//! - **Batched submissions**: Multiple I/O operations are batched for efficiency
//! - **Thread-safe**: `Uring` handles can be shared across threads and tasks
//!
//! # Quick Start
//!
//! ```ignore
//! use uring_file::{UringFile, uring::{Uring, UringCfg}};
//! use std::fs::File;
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     // Option 1: Use the UringFile trait with the global ring
//!     let file = File::open("data.bin")?;
//!     let result = file.ur_read_at(0, 4096).await?;
//!     println!("Read {} bytes", result.bytes_read);
//!
//!     // Option 2: Create your own ring for more control
//!     let uring = Uring::new(UringCfg::default())?;
//!     let result = uring.read_at(&file, 0, 4096).await?;
//!     println!("Read {} bytes", result.bytes_read);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Architecture
//!
//! The crate uses a shared io_uring instance with dedicated threads:
//!
//! ```text
//! ┌─────────────┐     ┌───────────────────┐     ┌─────────────┐
//! │ Async Tasks │────▶│ Submission Thread │────▶│  io_uring   │
//! └─────────────┘     └───────────────────┘     └──────┬──────┘
//!        ▲                                             │
//!        │            ┌───────────────────┐            │
//!        └────────────│ Completion Thread │◀───────────┘
//!                     └───────────────────┘
//! ```
//!
//! # Kernel Requirements
//!
//! - **Minimum**: Linux 5.1 (basic io_uring support)
//! - **Recommended**: Linux 5.6+ (for statx, fallocate, fadvise)
//! - **ftruncate**: Linux 6.9+
//!
//! # Platform Support
//!
//! This crate only compiles on Linux. On other platforms, it will fail at compile time.
//!
//! # Troubleshooting
//!
//! `ENOMEM` on init usually means memory fragmentation, not insufficient RAM. Ring buffers
//! need physically contiguous memory (1 MiB for default 16384 entries). On fragmented
//! systems, reduce [`uring::UringCfg::ring_size`] to 8192 (needs 512 KiB). See README.

#![cfg(target_os = "linux")]
#![allow(async_fn_in_trait)]

pub mod fs;
pub mod metadata;
pub mod uring;

use metadata::Metadata;
use once_cell::sync::Lazy;
use std::fs::File as StdFile;
use std::io;
use std::os::fd::AsRawFd;
use tokio::fs::File as TokioFile;
use uring::ReadResult;
use uring::Uring;
use uring::UringCfg;
use uring::WriteResult;

// Re-export for convenience
pub use uring::DEFAULT_RING_SIZE;

// ============================================================================
// Global Default Ring
// ============================================================================

/// Global default io_uring instance. Lazily initialized on first use with default configuration. For production use with specific requirements, consider creating your own `Uring` instance with custom configuration.
static DEFAULT_URING: Lazy<Uring> =
  Lazy::new(|| Uring::new(UringCfg::default()).expect("failed to initialize io_uring"));

/// Get a reference to the global default io_uring instance.
pub fn default_uring() -> &'static Uring {
  &DEFAULT_URING
}

// ============================================================================
// UringFile Trait
// ============================================================================

/// Extension trait for performing io_uring operations on file types.
///
/// This trait is implemented for `std::fs::File` and `tokio::fs::File`, allowing you to call io_uring operations directly on file handles. All operations use the global default io_uring instance. For more control, use the `Uring` struct directly.
pub trait UringFile: AsRawFd {
  /// Read from the file at the specified offset. Returns the buffer and actual bytes read. The buffer may contain fewer bytes than requested if EOF is reached.
  async fn ur_read_at(&self, offset: u64, len: u64) -> io::Result<ReadResult<Vec<u8>>>;

  /// Read exactly `len` bytes from the file at the specified offset. Returns an error if fewer bytes are available.
  async fn ur_read_exact_at(&self, offset: u64, len: u64) -> io::Result<Vec<u8>>;

  /// Write to the file at the specified offset. Returns the original buffer and bytes written.
  async fn ur_write_at(&self, offset: u64, data: Vec<u8>) -> io::Result<WriteResult<Vec<u8>>>;

  /// Write all bytes to the file at the specified offset. Loops on short writes until complete.
  async fn ur_write_all_at(&self, offset: u64, data: &[u8]) -> io::Result<()>;

  /// Synchronize file data and metadata to disk (fsync).
  async fn ur_sync(&self) -> io::Result<()>;

  /// Synchronize file data to disk (fdatasync). Faster than fsync as it doesn't sync metadata unless required.
  async fn ur_datasync(&self) -> io::Result<()>;

  /// Get file metadata via statx. Requires Linux 5.6+.
  async fn ur_statx(&self) -> io::Result<Metadata>;

  /// Pre-allocate or manipulate file space. Requires Linux 5.6+.
  async fn ur_fallocate(&self, offset: u64, len: u64, mode: i32) -> io::Result<()>;

  /// Advise the kernel about file access patterns. Requires Linux 5.6+.
  async fn ur_fadvise(&self, offset: u64, len: u64, advice: i32) -> io::Result<()>;

  /// Truncate the file to the specified length. Requires Linux 6.9+.
  async fn ur_ftruncate(&self, len: u64) -> io::Result<()>;
}

impl UringFile for StdFile {
  async fn ur_read_at(&self, offset: u64, len: u64) -> io::Result<ReadResult<Vec<u8>>> {
    DEFAULT_URING.read_at(self, offset, len).await
  }

  async fn ur_read_exact_at(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
    DEFAULT_URING.read_exact_at(self, offset, len).await
  }

  async fn ur_write_at(&self, offset: u64, data: Vec<u8>) -> io::Result<WriteResult<Vec<u8>>> {
    DEFAULT_URING.write_at(self, offset, data).await
  }

  async fn ur_write_all_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
    DEFAULT_URING.write_all_at(self, offset, data).await
  }

  async fn ur_sync(&self) -> io::Result<()> {
    DEFAULT_URING.sync(self).await
  }

  async fn ur_datasync(&self) -> io::Result<()> {
    DEFAULT_URING.datasync(self).await
  }

  async fn ur_statx(&self) -> io::Result<Metadata> {
    DEFAULT_URING.statx(self).await
  }

  async fn ur_fallocate(&self, offset: u64, len: u64, mode: i32) -> io::Result<()> {
    DEFAULT_URING.fallocate(self, offset, len, mode).await
  }

  async fn ur_fadvise(&self, offset: u64, len: u64, advice: i32) -> io::Result<()> {
    DEFAULT_URING.fadvise(self, offset, len, advice).await
  }

  async fn ur_ftruncate(&self, len: u64) -> io::Result<()> {
    DEFAULT_URING.ftruncate(self, len).await
  }
}

impl UringFile for TokioFile {
  async fn ur_read_at(&self, offset: u64, len: u64) -> io::Result<ReadResult<Vec<u8>>> {
    DEFAULT_URING.read_at(self, offset, len).await
  }

  async fn ur_read_exact_at(&self, offset: u64, len: u64) -> io::Result<Vec<u8>> {
    DEFAULT_URING.read_exact_at(self, offset, len).await
  }

  async fn ur_write_at(&self, offset: u64, data: Vec<u8>) -> io::Result<WriteResult<Vec<u8>>> {
    DEFAULT_URING.write_at(self, offset, data).await
  }

  async fn ur_write_all_at(&self, offset: u64, data: &[u8]) -> io::Result<()> {
    DEFAULT_URING.write_all_at(self, offset, data).await
  }

  async fn ur_sync(&self) -> io::Result<()> {
    DEFAULT_URING.sync(self).await
  }

  async fn ur_datasync(&self) -> io::Result<()> {
    DEFAULT_URING.datasync(self).await
  }

  async fn ur_statx(&self) -> io::Result<Metadata> {
    DEFAULT_URING.statx(self).await
  }

  async fn ur_fallocate(&self, offset: u64, len: u64, mode: i32) -> io::Result<()> {
    DEFAULT_URING.fallocate(self, offset, len, mode).await
  }

  async fn ur_fadvise(&self, offset: u64, len: u64, advice: i32) -> io::Result<()> {
    DEFAULT_URING.fadvise(self, offset, len, advice).await
  }

  async fn ur_ftruncate(&self, len: u64) -> io::Result<()> {
    DEFAULT_URING.ftruncate(self, len).await
  }
}
