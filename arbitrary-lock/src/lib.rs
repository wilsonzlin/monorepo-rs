#[cfg(test)]
pub mod tests;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::hash::Hash;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::Arc;

struct EntryInner<K: Clone + Eq + Hash, L: Default> {
  key: K,
  lock: L,
}

pub struct ArbitraryLockEntry<K: Clone + Eq + Hash, L: Default> {
  // We use ManuallyDrop because we need to drop this before we drop the ArbitraryLockEntry (usually fields are dropped after the containing struct). See the Drop impl for more details. Idea by https://stackoverflow.com/a/41056727/6249022.
  inner: ManuallyDrop<Arc<EntryInner<K, L>>>,
  map: ArbitraryLock<K, L>,
}

impl<K: Clone + Eq + Hash, L: Default> Drop for ArbitraryLockEntry<K, L> {
  fn drop(&mut self) {
    let key = self.inner.key.clone();
    // We must drop `inner` Arc before we attempt to drop the map's Arc.
    unsafe {
      ManuallyDrop::drop(&mut self.inner);
    };
    let Entry::Occupied(mut e) = self.map.map.entry(key) else {
      // Already returned. This is possible because some other thread may have dropped the map entry after we droppepd our `inner` Arc but before we were able to acquire a lock on the map entry.
      return;
    };
    let arc = e.get_mut().take().unwrap();
    // `try_unwrap` is the appropriate function, not `Arc::into_inner`; there is no race condition because we're holdding onto the map entry lock, and `into_inner` will drop the map's Arc even if it isn't actually the last reference which is not what we want.
    match Arc::try_unwrap(arc) {
      Ok(_) => {
        // We dropped the map's Arc, which means there is no other reference anywhere and we must remove the map entry (and not leave it as a None and leak memory).
        e.remove();
      }
      Err(arc) => {
        // We couldn't drop the map's Arc, so do not delete the map entry and restore its value to a Some containing the Arc.
        *e.get_mut() = Some(arc);
      }
    };
  }
}

impl<K: Clone + Eq + Hash, L: Default> Deref for ArbitraryLockEntry<K, L> {
  type Target = L;

  fn deref(&self) -> &Self::Target {
    &self.inner.lock
  }
}

/// This can be cheaply cloned.
/// Provide your desired lock implementation for the `L` generic parameter. Options include `tokio::sync::RwLock`, `parking_lot::Mutex`, and `std::sync::Exclusive`.
pub struct ArbitraryLock<K: Clone + Eq + Hash, L: Default> {
  map: Arc<DashMap<K, Option<Arc<EntryInner<K, L>>>, ahash::RandomState>>,
}

impl<K: Clone + Hash + Eq, L: Default> ArbitraryLock<K, L> {
  pub fn new() -> Self {
    Self {
      map: Default::default(),
    }
  }

  pub fn get(&self, k: K) -> ArbitraryLockEntry<K, L> {
    let inner = self
      .map
      .entry(k.clone())
      .or_insert_with(|| {
        Some(Arc::new(EntryInner {
          key: k,
          lock: L::default(),
        }))
      })
      .clone()
      // If the entry exists, it must be `Some`.
      .unwrap();
    ArbitraryLockEntry {
      inner: ManuallyDrop::new(inner),
      map: self.clone(),
    }
  }

  pub fn len(&self) -> usize {
    self.map.len()
  }

  pub fn is_empty(&self) -> bool {
    self.map.is_empty()
  }

  pub fn capacity(&self) -> usize {
    self.map.capacity()
  }

  pub fn shrink_to_fit(&self) {
    self.map.shrink_to_fit();
  }
}

impl<K: Clone + Hash + Eq, L: Default> Clone for ArbitraryLock<K, L> {
  fn clone(&self) -> Self {
    Self {
      map: self.map.clone(),
    }
  }
}
