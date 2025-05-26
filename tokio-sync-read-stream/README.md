# tokio-sync-read-stream

Transforms a [std::io::Read](https://doc.rust-lang.org/std/fs/struct.File.html) into a fallable [futures::stream::Stream](https://docs.rs/futures/latest/futures/stream/trait.Stream.html) that yields `Result<Vec<u8>, std::io::Error>`.

Under the hood, it reads from the file in chunks of up to `buffer_size` on a Tokio blocking thread using [spawn_blocking](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html). A [Handle](https://docs.rs/tokio/latest/tokio/runtime/struct.Handle.html) to the Tokio runtime must be provided.

## Usage

Add the library as a dependency:

```bash
cargo add tokio-sync-read-stream
```

Sample Rust code:

```rust
use std::io::File;
use tokio_sync_read_stream::SyncReadStream;

#[tokio::main]
async fn main() {
  let file = File::open("data.bin").unwrap();
  let stream: SyncReadStream<File> = file.into();
}
```
