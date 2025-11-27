//! High-level filesystem operations using io_uring.
//!
//! This module provides convenience functions that mirror `std::fs`, but use io_uring for async I/O.
//! All functions use the global default ring.
//!
//! # Example
//!
//! ```ignore
//! use uring_file::fs;
//!
//! #[tokio::main]
//! async fn main() -> std::io::Result<()> {
//!     // Read entire file
//!     let contents = fs::read_to_string("/etc/hostname").await?;
//!     println!("Hostname: {}", contents.trim());
//!
//!     // Write to file
//!     fs::write("/tmp/hello.txt", b"Hello, world!").await?;
//!
//!     // Create directory tree
//!     fs::create_dir_all("/tmp/a/b/c").await?;
//!
//!     // Remove directory tree
//!     fs::remove_dir_all("/tmp/a").await?;
//!
//!     Ok(())
//! }
//! ```

use crate::default_uring;
use crate::metadata::Metadata;
use std::io;
use std::path::Path;
use tokio::fs::File;

/// Open a file in read-only mode.
///
/// Returns a `tokio::fs::File` which can be used with the `UringFile` trait or tokio's async methods.
pub async fn open(path: impl AsRef<Path>) -> io::Result<File> {
  let fd = default_uring().open(path, libc::O_RDONLY, 0).await?;
  Ok(File::from_std(std::fs::File::from(fd)))
}

/// Create a file for writing, truncating if it exists.
///
/// Returns a `tokio::fs::File` which can be used with the `UringFile` trait or tokio's async methods.
pub async fn create(path: impl AsRef<Path>) -> io::Result<File> {
  let fd = default_uring()
    .open(path, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC, 0o644)
    .await?;
  Ok(File::from_std(std::fs::File::from(fd)))
}

/// Read the entire contents of a file into a byte vector.
///
/// This is a convenience function for reading entire files. For large files, consider using `Uring::read_at` with explicit offsets and chunking.
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
  let uring = default_uring();
  let fd = uring.open(path.as_ref(), libc::O_RDONLY, 0).await?;
  let meta = uring.statx(&fd).await?;
  let size = meta.len();

  if size == 0 {
    return Ok(Vec::new());
  }

  let result = uring.read_at(&fd, 0, size).await?;
  Ok(result.buf)
}

/// Read the entire contents of a file into a string.
///
/// Returns an error if the file contents are not valid UTF-8.
pub async fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
  let bytes = read(path).await?;
  String::from_utf8(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Write data to a file, creating it if it doesn't exist, truncating it if it does.
pub async fn write(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
  let uring = default_uring();
  let fd = uring
    .open(
      path.as_ref(),
      libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
      0o644,
    )
    .await?;
  let data = contents.as_ref().to_vec();
  uring.write_at(&fd, 0, data).await?;
  Ok(())
}

/// Get metadata for a file or directory.
pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
  default_uring().statx_path(path).await
}

/// Create a directory.
///
/// Returns an error if the directory already exists or the parent doesn't exist.
pub async fn create_dir(path: impl AsRef<Path>) -> io::Result<()> {
  default_uring().mkdir(path, 0o755).await
}

/// Create a directory and all parent directories.
///
/// Does nothing if the directory already exists.
pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
  let path = path.as_ref();

  // If it already exists, we're done
  if metadata(path).await.is_ok() {
    return Ok(());
  }

  // Collect ancestors that need to be created
  let mut to_create = Vec::new();
  let mut current = Some(path);

  while let Some(p) = current {
    if metadata(p).await.is_ok() {
      break;
    }
    to_create.push(p);
    current = p.parent();
  }

  // Create from root to leaf
  for p in to_create.into_iter().rev() {
    match default_uring().mkdir(p, 0o755).await {
      Ok(()) => {}
      Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
      Err(e) => return Err(e),
    }
  }

  Ok(())
}

/// Remove a file.
pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
  default_uring().unlink(path).await
}

/// Remove an empty directory.
pub async fn remove_dir(path: impl AsRef<Path>) -> io::Result<()> {
  default_uring().rmdir(path).await
}

/// Remove a directory and all its contents recursively.
///
/// Uses tokio for directory listing (io_uring doesn't support readdir), then io_uring for removal.
pub async fn remove_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
  let path = path.as_ref();

  // io_uring doesn't have readdir, use tokio's async version
  let mut read_dir = tokio::fs::read_dir(path).await?;
  while let Some(entry) = read_dir.next_entry().await? {
    let entry_path = entry.path();
    let file_type = entry.file_type().await?;
    if file_type.is_dir() {
      Box::pin(remove_dir_all(&entry_path)).await?;
    } else {
      remove_file(&entry_path).await?;
    }
  }

  remove_dir(path).await
}

/// Rename a file or directory.
pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
  default_uring().rename(from, to).await
}

/// Copy the contents of one file to another.
///
/// Creates the destination file if it doesn't exist, truncates it if it does.
pub async fn copy(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<u64> {
  let contents = read(from).await?;
  let len = contents.len() as u64;
  write(to, contents).await?;
  Ok(len)
}

/// Check if a path exists.
pub async fn exists(path: impl AsRef<Path>) -> bool {
  metadata(path).await.is_ok()
}

/// Create a symbolic link.
///
/// `target` is what the symlink points to, `link` is the path of the new symlink.
pub async fn symlink(target: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
  default_uring().symlink(target, link).await
}

/// Create a hard link.
///
/// Creates a new hard link `link` pointing to the same inode as `original`.
pub async fn hard_link(original: impl AsRef<Path>, link: impl AsRef<Path>) -> io::Result<()> {
  default_uring().hard_link(original, link).await
}

/// Truncate a file to the specified length.
///
/// If the file is larger, it will be truncated. If smaller, it will be extended with zeros.
pub async fn truncate(path: impl AsRef<Path>, len: u64) -> io::Result<()> {
  let uring = default_uring();
  let fd = uring.open(path.as_ref(), libc::O_WRONLY, 0).await?;
  uring.ftruncate(&fd, len).await
}

/// Append data to a file, creating it if it doesn't exist.
pub async fn append(path: impl AsRef<Path>, contents: impl AsRef<[u8]>) -> io::Result<()> {
  let uring = default_uring();
  let fd = uring
    .open(
      path.as_ref(),
      libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND,
      0o644,
    )
    .await?;
  let data = contents.as_ref().to_vec();
  // With O_APPEND, the offset is ignored - kernel always writes at end
  uring.write_at(&fd, 0, data).await?;
  Ok(())
}
