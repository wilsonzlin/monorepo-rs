# arbitrary-lock

Acquire a lock on an arbitrary key, like a string or integer.

## Usage

Add the dependency to your project:

```bash
cargo add arbitrary-lock
```

Use the struct:

```rust
// This can be cheaply cloned.
// The key must be Hash + Eq + Clone.
// Provide your preferred lock type as the second generic argument. It must implement Default.
let locker = ArbitraryLock::<String, tokio::sync::Mutex<()>>::new();
{
  let _l = locker.get("user:2185724".to_string()).lock().await;
  do_something_in_database().await;
};
```
