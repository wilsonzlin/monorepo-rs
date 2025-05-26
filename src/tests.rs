use crate::ArbitraryLock;
use std::sync::Mutex;
use std::thread::sleep;
use std::thread::spawn;
use std::time::Duration;

#[test]
fn test_locking() {
  let locker = ArbitraryLock::<&'static str, Mutex<()>>::new();
  let t1 = spawn({
    let locker = locker.clone();
    move || {
      // T = 0.
      let l = locker.get("hello");
      let h = l.try_lock().unwrap();
      sleep(Duration::from_secs(2));
      // T = 2.
      drop(h);
      sleep(Duration::from_secs(2));
      // T = 4.
      assert!(l.try_lock().is_err());
    }
  });
  let t2 = spawn({
    let locker = locker.clone();
    move || {
      // T = 0.
      let l = locker.get("hello");
      sleep(Duration::from_secs(1));
      // T = 1.
      assert!(l.try_lock().is_err());
      sleep(Duration::from_secs(2));
      // T = 3.
      let h = l.try_lock().unwrap();
      sleep(Duration::from_secs(2));
      // T = 5.
      drop(h);
    }
  });
  let t3 = spawn({
    let locker = locker.clone();
    move || {
      sleep(Duration::from_secs(4));
      // T = 4.
      // Variation: get locker after a while.
      let l = locker.get("hello");
      assert!(l.try_lock().is_err());
      sleep(Duration::from_secs(2));
      // Variation: get locker again.
      assert!(locker.get("hello").try_lock().is_ok());
    }
  });
  t1.join().unwrap();
  t2.join().unwrap();
  t3.join().unwrap();
  assert_eq!(locker.map.len(), 0);
}
