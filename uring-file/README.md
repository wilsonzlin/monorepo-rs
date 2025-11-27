# uring-file

Async file I/O for Linux using [io_uring](https://kernel.dk/io_uring.pdf).

io_uring is Linux's modern async I/O interface. It's fast and efficient, but the raw API requires managing submission queues, completion queues, memory lifetimes, and substantial unsafe code. This crate builds on [io-uring](https://crates.io/crates/io-uring) to provide simple async/await file operations at three levels of abstraction.

## Three ways to use it

**1. High-level functions** — Similar to `std::fs`, but async. Suitable for scripts, tools, and straightforward file operations.

```rust
let contents = uring_file::fs::read_to_string("config.toml").await?;
```

**2. The default ring** — The `UringFile` trait works on any file handle. Useful when you have files opened elsewhere and want io_uring performance without managing a ring.

```rust
use uring_file::UringFile;

let file = std::fs::File::open("data.bin")?;
let result = file.ur_read_at(0, 4096).await?;
```

**3. Custom rings** — Full control over ring configuration. Configure queue size, enable kernel polling, register files for faster repeated access.

```rust
use uring_file::uring::{Uring, UringCfg};

let ring = Uring::new(UringCfg {
    entries: 256,
    kernel_poll: true,
    ..Default::default()
})?;
```

All three use the same underlying io_uring implementation. `Uring` is `Clone + Send + Sync`, and path arguments accept any type implementing `AsRef<Path>`.

## Requirements

- Linux 5.6+ (5.11+ for full feature set)
- tokio runtime

```toml
[dependencies]
uring-file = "0.4"
tokio = { version = "1", features = ["rt", "macros"] }
```

## Example

```rust
use uring_file::fs;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    fs::write("/tmp/hello.txt", "Hello!").await?;
    let text = fs::read_to_string("/tmp/hello.txt").await?;
    
    fs::create_dir_all("/tmp/a/b/c").await?;
    fs::remove_dir_all("/tmp/a").await?;
    
    Ok(())
}
```

## API reference

See [docs.rs](https://docs.rs/uring-file) for full documentation.

### `uring_file::fs` — high-level convenience

```rust
// Reading
fs::read(path).await?;                    // -> Vec<u8>
fs::read_to_string(path).await?;          // -> String

// Writing
fs::write(path, data).await?;             // create or truncate
fs::append(path, data).await?;            // create or append
fs::copy(src, dst).await?;                // -> bytes copied

// Opening files (returns tokio::fs::File)
let file = fs::open(path).await?;         // read-only
let file = fs::create(path).await?;       // write, create/truncate

// Directories
fs::create_dir(path).await?;
fs::create_dir_all(path).await?;
fs::remove_dir(path).await?;
fs::remove_dir_all(path).await?;

// Files and links
fs::remove_file(path).await?;
fs::rename(from, to).await?;
fs::symlink(target, link).await?;
fs::hard_link(original, link).await?;
fs::truncate(path, len).await?;

// Metadata
fs::metadata(path).await?;                // -> Metadata
fs::exists(path).await;                   // -> bool
```

### `UringFile` trait — for existing file handles

Works with `std::fs::File` and `tokio::fs::File`:

```rust
use uring_file::UringFile;

let file = std::fs::File::open("data.bin")?;

// Positioned I/O
let result = file.ur_read_at(offset, len).await?;
let result = file.ur_write_at(offset, data).await?;

// Durability
file.ur_sync().await?;                    // fsync
file.ur_datasync().await?;                // fdatasync

// Metadata and space management
file.ur_statx().await?;
file.ur_fallocate(offset, len, mode).await?;
file.ur_fadvise(offset, len, advice).await?;
file.ur_ftruncate(len).await?;
```

### `Uring` — full control

```rust
use uring_file::uring::{Uring, UringCfg};

let ring = Uring::new(UringCfg {
    entries: 256,           // submission queue size
    kernel_poll: true,      // SQPOLL for lower latency
    ..Default::default()
})?;

// File operations
let fd = ring.open("/tmp/file", libc::O_RDWR | libc::O_CREAT, 0o644).await?;
ring.write_at(&fd, 0, b"hello".to_vec()).await?;
ring.read_at(&fd, 0, 5).await?;
ring.close(fd).await?;

// Path operations
ring.mkdir("/tmp/dir", 0o755).await?;
ring.rename("/tmp/a", "/tmp/b").await?;
ring.unlink("/tmp/file").await?;

// Registered files for faster repeated access
let registered = ring.register(&file)?;
ring.read_at(&registered, 0, 1024).await?;
```

## Kernel version requirements

| Feature | Minimum kernel |
|---------|---------------|
| Basic read/write/sync | 5.1 |
| open, statx, fallocate, fadvise | 5.6 |
| close, rename, unlink, mkdir, symlink, link | 5.11 |
| ftruncate | 6.9 |

## Limitations

- **No readdir in io_uring** — `remove_dir_all` uses tokio for directory listing, then io_uring for deletions. This is a kernel limitation.

- **~2GB per operation** — Single read/write operations are limited to approximately 2GB (`uring::URING_LEN_MAX`). Chunk larger transfers.

## Architecture

```
┌─────────────┐     ┌───────────────────┐     ┌─────────────┐
│ Async tasks │────▶│ Submission thread │────▶│  io_uring   │
└─────────────┘     └───────────────────┘     └──────┬──────┘
       ▲                                             │
       │            ┌───────────────────┐            │
       └────────────│ Completion thread │◀───────────┘
                    └───────────────────┘
```

Async tasks send requests to a submission thread that batches them into the io_uring submission queue. A completion thread polls for results and wakes the appropriate futures.

## License

MIT OR Apache-2.0
