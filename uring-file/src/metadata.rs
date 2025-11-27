//! File metadata types compatible with `std::fs::Metadata`.

use std::fmt;
use std::io;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// File metadata returned by statx. Provides an interface compatible with [`std::fs::Metadata`] and [`std::os::unix::fs::MetadataExt`].
pub struct Metadata(pub(crate) libc::statx);

impl Metadata {
  // ===========================================================================
  // std::fs::Metadata interface
  // ===========================================================================

  /// Returns the file type for this metadata.
  pub fn file_type(&self) -> FileType {
    FileType(self.0.stx_mode)
  }

  /// Returns `true` if this metadata is for a directory.
  pub fn is_dir(&self) -> bool {
    self.file_type().is_dir()
  }

  /// Returns `true` if this metadata is for a regular file.
  pub fn is_file(&self) -> bool {
    self.file_type().is_file()
  }

  /// Returns `true` if this metadata is for a symbolic link.
  pub fn is_symlink(&self) -> bool {
    self.file_type().is_symlink()
  }

  /// Returns the size of the file, in bytes.
  pub fn len(&self) -> u64 {
    self.0.stx_size
  }

  /// Returns `true` if the file size is 0 bytes.
  pub fn is_empty(&self) -> bool {
    self.0.stx_size == 0
  }

  /// Returns the permissions of the file.
  pub fn permissions(&self) -> Permissions {
    Permissions(self.0.stx_mode as u32 & 0o7777)
  }

  /// Returns the last modification time.
  pub fn modified(&self) -> io::Result<SystemTime> {
    Ok(system_time_from_unix(
      self.0.stx_mtime.tv_sec,
      self.0.stx_mtime.tv_nsec,
    ))
  }

  /// Returns the last access time.
  pub fn accessed(&self) -> io::Result<SystemTime> {
    Ok(system_time_from_unix(
      self.0.stx_atime.tv_sec,
      self.0.stx_atime.tv_nsec,
    ))
  }

  /// Returns the creation time (if supported by filesystem).
  pub fn created(&self) -> io::Result<SystemTime> {
    Ok(system_time_from_unix(
      self.0.stx_btime.tv_sec,
      self.0.stx_btime.tv_nsec,
    ))
  }

  // ===========================================================================
  // std::os::unix::fs::MetadataExt interface
  // ===========================================================================

  pub fn dev(&self) -> u64 {
    libc::makedev(self.0.stx_dev_major, self.0.stx_dev_minor) as u64
  }

  pub fn ino(&self) -> u64 {
    self.0.stx_ino
  }

  pub fn mode(&self) -> u32 {
    self.0.stx_mode as u32
  }

  pub fn nlink(&self) -> u64 {
    self.0.stx_nlink as u64
  }

  pub fn uid(&self) -> u32 {
    self.0.stx_uid
  }

  pub fn gid(&self) -> u32 {
    self.0.stx_gid
  }

  pub fn rdev(&self) -> u64 {
    libc::makedev(self.0.stx_rdev_major, self.0.stx_rdev_minor) as u64
  }

  pub fn size(&self) -> u64 {
    self.0.stx_size
  }

  pub fn atime(&self) -> i64 {
    self.0.stx_atime.tv_sec
  }

  pub fn atime_nsec(&self) -> i64 {
    self.0.stx_atime.tv_nsec as i64
  }

  pub fn mtime(&self) -> i64 {
    self.0.stx_mtime.tv_sec
  }

  pub fn mtime_nsec(&self) -> i64 {
    self.0.stx_mtime.tv_nsec as i64
  }

  pub fn ctime(&self) -> i64 {
    self.0.stx_ctime.tv_sec
  }

  pub fn ctime_nsec(&self) -> i64 {
    self.0.stx_ctime.tv_nsec as i64
  }

  pub fn blksize(&self) -> u64 {
    self.0.stx_blksize as u64
  }

  pub fn blocks(&self) -> u64 {
    self.0.stx_blocks
  }
}

impl fmt::Debug for Metadata {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Metadata")
      .field("file_type", &self.file_type())
      .field("permissions", &self.permissions())
      .field("len", &self.len())
      .field("uid", &self.uid())
      .field("gid", &self.gid())
      .field("ino", &self.ino())
      .finish_non_exhaustive()
  }
}

// =============================================================================
// FileType
// =============================================================================

/// Representation of file types. Equivalent to [`std::fs::FileType`].
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileType(u16);

impl FileType {
  pub fn is_dir(&self) -> bool {
    (self.0 & libc::S_IFMT as u16) == libc::S_IFDIR as u16
  }

  pub fn is_file(&self) -> bool {
    (self.0 & libc::S_IFMT as u16) == libc::S_IFREG as u16
  }

  pub fn is_symlink(&self) -> bool {
    (self.0 & libc::S_IFMT as u16) == libc::S_IFLNK as u16
  }

  pub fn is_block_device(&self) -> bool {
    (self.0 & libc::S_IFMT as u16) == libc::S_IFBLK as u16
  }

  pub fn is_char_device(&self) -> bool {
    (self.0 & libc::S_IFMT as u16) == libc::S_IFCHR as u16
  }

  pub fn is_fifo(&self) -> bool {
    (self.0 & libc::S_IFMT as u16) == libc::S_IFIFO as u16
  }

  pub fn is_socket(&self) -> bool {
    (self.0 & libc::S_IFMT as u16) == libc::S_IFSOCK as u16
  }
}

impl fmt::Debug for FileType {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let kind = if self.is_file() {
      "file"
    } else if self.is_dir() {
      "directory"
    } else if self.is_symlink() {
      "symlink"
    } else if self.is_block_device() {
      "block_device"
    } else if self.is_char_device() {
      "char_device"
    } else if self.is_fifo() {
      "fifo"
    } else if self.is_socket() {
      "socket"
    } else {
      "unknown"
    };
    write!(f, "FileType({kind})")
  }
}

// =============================================================================
// Permissions
// =============================================================================

/// Representation of file permissions. Equivalent to [`std::fs::Permissions`].
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Permissions(u32);

impl Permissions {
  pub fn readonly(&self) -> bool {
    (self.0 & 0o200) == 0
  }

  pub fn mode(&self) -> u32 {
    self.0
  }

  pub fn from_mode(mode: u32) -> Self {
    Self(mode & 0o7777)
  }

  pub fn set_readonly(&mut self, readonly: bool) {
    if readonly {
      self.0 &= !0o222;
    } else {
      self.0 |= 0o200;
    }
  }
}

impl fmt::Debug for Permissions {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "Permissions({:04o})", self.0)
  }
}

// =============================================================================
// Helpers
// =============================================================================

fn system_time_from_unix(sec: i64, nsec: u32) -> SystemTime {
  if sec >= 0 {
    UNIX_EPOCH + Duration::new(sec as u64, nsec)
  } else {
    UNIX_EPOCH - Duration::new((-sec) as u64, nsec)
  }
}
