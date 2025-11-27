//! High-level filesystem operations using io_uring.
//!
//! This module provides convenience functions that mirror `std::fs`, but use io_uring for async I/O.
//! All functions use the global default ring.
//!
//! # Example
//!
//! ```ignore
//! use uring_file::fs::{self, OpenOptions};
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
//!     // Open with options
//!     let file = OpenOptions::new()
//!         .read(true)
//!         .write(true)
//!         .create(true)
//!         .open("/tmp/data.bin")
//!         .await?;
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
use crate::uring::URING_LEN_MAX;
use std::cmp::min;
use std::io;
use std::path::Path;
use tokio::fs::File;

/// Options for opening files, mirroring `std::fs::OpenOptions`.
///
/// # Example
///
/// ```ignore
/// use uring_file::fs::OpenOptions;
///
/// // Open for reading
/// let file = OpenOptions::new().read(true).open("foo.txt").await?;
///
/// // Create for writing
/// let file = OpenOptions::new()
///     .write(true)
///     .create(true)
///     .truncate(true)
///     .open("bar.txt")
///     .await?;
///
/// // Append to existing
/// let file = OpenOptions::new()
///     .append(true)
///     .open("log.txt")
///     .await?;
///
/// // Create new (fails if exists)
/// let file = OpenOptions::new()
///     .write(true)
///     .create_new(true)
///     .open("new.txt")
///     .await?;
/// ```
#[derive(Clone, Debug, Default)]
pub struct OpenOptions {
  read: bool,
  write: bool,
  append: bool,
  truncate: bool,
  create: bool,
  create_new: bool,
  mode: Option<u32>,
  custom_flags: i32,
}

impl OpenOptions {
  /// Creates a blank new set of options.
  ///
  /// All options are initially set to `false`.
  pub fn new() -> Self {
    Self::default()
  }

  /// Sets read access.
  pub fn read(&mut self, read: bool) -> &mut Self {
    self.read = read;
    self
  }

  /// Sets write access.
  pub fn write(&mut self, write: bool) -> &mut Self {
    self.write = write;
    self
  }

  /// Sets append mode.
  ///
  /// Writes will append to the file instead of overwriting.
  /// Implies `write(true)`.
  pub fn append(&mut self, append: bool) -> &mut Self {
    self.append = append;
    self
  }

  /// Sets truncate mode.
  ///
  /// If the file exists, it will be truncated to zero length.
  /// Requires `write(true)`.
  pub fn truncate(&mut self, truncate: bool) -> &mut Self {
    self.truncate = truncate;
    self
  }

  /// Sets create mode.
  ///
  /// Creates the file if it doesn't exist. Requires `write(true)` or `append(true)`.
  pub fn create(&mut self, create: bool) -> &mut Self {
    self.create = create;
    self
  }

  /// Sets create-new mode.
  ///
  /// Creates a new file, failing if it already exists.
  /// Implies `create(true)` and requires `write(true)`.
  pub fn create_new(&mut self, create_new: bool) -> &mut Self {
    self.create_new = create_new;
    self
  }

  /// Sets the file mode (permissions) for newly created files.
  ///
  /// Default is `0o644`.
  pub fn mode(&mut self, mode: u32) -> &mut Self {
    self.mode = Some(mode);
    self
  }

  /// Sets custom flags to pass to the underlying `open` syscall.
  ///
  /// This allows flags like `O_DIRECT`, `O_SYNC`, `O_NOFOLLOW`, `O_CLOEXEC`, etc.
  /// The flags are OR'd with the flags derived from other options.
  ///
  /// # Example
  ///
  /// ```ignore
  /// let file = OpenOptions::new()
  ///     .read(true)
  ///     .write(true)
  ///     .create(true)
  ///     .custom_flags(libc::O_DIRECT | libc::O_SYNC)
  ///     .open("data.bin")
  ///     .await?;
  /// ```
  pub fn custom_flags(&mut self, flags: i32) -> &mut Self {
    self.custom_flags = flags;
    self
  }

  /// Opens a file with the configured options.
  ///
  /// Returns a `tokio::fs::File` for async operations.
  pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
    let flags = self.to_flags()?;
    let mode = self.mode.unwrap_or(0o644);
    let fd = default_uring().open(path, flags, mode).await?;
    Ok(File::from_std(std::fs::File::from(fd)))
  }

  fn to_flags(&self) -> io::Result<i32> {
    // Validate options (matches std::fs::OpenOptions behavior)
    if self.append && self.truncate {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "cannot combine append and truncate",
      ));
    }

    // Build flags
    let mut flags = self.custom_flags;

    // Access mode: determine O_RDONLY, O_WRONLY, or O_RDWR
    let has_write = self.write || self.append;
    if self.read && has_write {
      flags |= libc::O_RDWR;
    } else if self.read {
      flags |= libc::O_RDONLY;
    } else if has_write {
      flags |= libc::O_WRONLY;
    } else {
      // Match std behavior: default to read-only if nothing specified
      flags |= libc::O_RDONLY;
    }

    if self.append {
      flags |= libc::O_APPEND;
    }

    if self.truncate {
      flags |= libc::O_TRUNC;
    }

    if self.create_new {
      flags |= libc::O_CREAT | libc::O_EXCL;
    } else if self.create {
      flags |= libc::O_CREAT;
    }

    Ok(flags)
  }
}

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
/// Handles files larger than io_uring's single-operation limit (~2GB) by chunking automatically.
pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
  let uring = default_uring();
  let fd = uring.open(path.as_ref(), libc::O_RDONLY, 0).await?;
  let meta = uring.statx(&fd).await?;
  let size = meta.len();

  if size == 0 {
    return Ok(Vec::new());
  }

  // For small files, read in one operation
  if size <= URING_LEN_MAX {
    return uring.read_exact_at(&fd, 0, size).await;
  }

  // For large files, read in chunks
  let mut buf = Vec::with_capacity(size as usize);
  let mut offset = 0u64;
  while offset < size {
    let chunk_size = min(URING_LEN_MAX, size - offset);
    let chunk = uring.read_exact_at(&fd, offset, chunk_size).await?;
    buf.extend_from_slice(&chunk);
    offset += chunk_size;
  }
  Ok(buf)
}

/// Read the entire contents of a file into a string.
///
/// Returns an error if the file contents are not valid UTF-8.
///
/// Unlike `std::fs::read_to_string`, this does not handle `io::ErrorKind::Interrupted` because io_uring operations are not interrupted by signals — the kernel completes them asynchronously.
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
  uring.write_all_at(&fd, 0, contents.as_ref()).await
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
  // With O_APPEND, the offset is ignored - kernel always writes at end
  uring.write_all_at(&fd, 0, contents.as_ref()).await
}
