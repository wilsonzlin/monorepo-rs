use crate::origins::OriginId;
use chrono::DateTime;
use chrono::Utc;
use parking_lot::Mutex;
use rand::thread_rng;
use rand::Rng;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BinaryHeap;
use std::ptr;
use std::sync::Arc;

// `Serialize` and `Clone` are for debugging.
#[derive(PartialEq, Eq, Debug, Serialize, Clone)]
pub enum TaskState {
  Available {
    // This is needed in case we need to transition a task from Available to some other state and remove it from the list.
    index_in_origin_list: usize,
  },
  Completed,
  InFlight {
    lock_release_time: DateTime<Utc>,
  },
}

#[derive(PartialEq, Eq, Clone, Copy, Hash, Serialize, Deserialize, Debug)]
pub struct TaskData {
  pub resource_uid: u64,
  // For crawl tasks, this is the ID of the last fetch, or 0 if this is the first fetch.
  // For any other task, this is the ID of the latest fetch, the one we're parsing.
  pub resource_fetch_id: u64,
}

#[derive(Debug)]
struct TaskInner {
  state: TaskState,
  origin_id: OriginId,
  data: TaskData,
}

#[derive(Clone)]
pub struct Task {
  lock: Arc<Mutex<TaskInner>>,
}

// This is a wrapper over a Task to be held in `by_lock_release_time`.
// The ordering is defined as `lock_release_time` descending, such that when in a BinaryHeap (whiich is a max-heap) the lowest `lock_release_time` will be popped first.
// The value of `lock_release_time` is cached when inserted. Once popped, the background loop will check the actual state. If it's still in flight, it'll insert the task back into the heap with the newest `lock_release_time`; otherwise, it'll be dropped from the heap.
// See `maybe_release_locked_tasks`.
struct ByLockReleaseTime {
  task: Task,
  lock_release_time: DateTime<Utc>,
}

impl PartialEq for ByLockReleaseTime {
  fn eq(&self, other: &Self) -> bool {
    self.lock_release_time == other.lock_release_time
  }
}

impl Eq for ByLockReleaseTime {}

impl PartialOrd for ByLockReleaseTime {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    self
      .lock_release_time
      .partial_cmp(&other.lock_release_time)
      .map(|o| o.reverse())
  }
}

impl Ord for ByLockReleaseTime {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.partial_cmp(other).unwrap()
  }
}

#[derive(Default)]
struct AvailableByOrigin {
  tasks: Vec<Task>,
  // Sometimes `tasks` will be empty but the origin will still be present in the list `origins_available`; see `remove_task_from_origin_available_list` for more details.
  in_origins_available: bool,
}

#[derive(Default)]
pub struct Tasks {
  // For updating and completing.
  tasks: FxHashMap<TaskData, Task>,
  // For releasing.
  by_lock_release_time: BinaryHeap<ByLockReleaseTime>,
  // For polling.
  available_by_origin: FxHashMap<OriginId, AvailableByOrigin>,
  origins_available: Vec<OriginId>,
}

impl Tasks {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn len(&self) -> usize {
    self.tasks.len()
  }

  /// For debugging.
  pub fn get_task_state(&self, t: &TaskData) -> Option<TaskState> {
    self.tasks.get(t).map(|t| t.lock.lock().state.clone())
  }

  // This only does one at a time because the caller may want an upper bound on how many to release to limit time executing while holding lock on Tasks. Returns the released task data for testing or debugging (e.g. logging).
  pub fn maybe_release_locked_task(&mut self) -> Option<TaskData> {
    let Some(t) = self.by_lock_release_time.pop() else {
      return None;
    };
    let task = t.task.lock.lock();
    match task.state {
      TaskState::Available { .. } => unreachable!(),
      TaskState::Completed => {}
      TaskState::InFlight { lock_release_time } => {
        let data = task.data;
        // Release the task so that `make_task_available` won't cause a deadlock.
        drop(task);
        if lock_release_time <= Utc::now() {
          // Release the task.
          self.make_task_available(t.task);
          return Some(data);
        } else {
          // Push the task back into the heap.
          self.by_lock_release_time.push(ByLockReleaseTime {
            task: t.task,
            lock_release_time: t.lock_release_time,
          });
          // There's no point in returning the release time to sleep until, as new tasks can get polled (with earlier release times) in the meantime.
        }
      }
    };
    None
  }

  fn update_in_flight_task_state_if_not_complete(
    &mut self,
    data: TaskData,
    new_state: TaskState,
  ) -> bool {
    let Some(task_lock) = self.tasks.get(&data) else {
      return false;
    };
    let mut task = task_lock.lock.lock();
    match task.state {
      TaskState::Available {
        index_in_origin_list,
      } => {
        // The task is marked as available, but a worker already has it and is working on it and sending us heartbeats or the completed results, so assume we crashed and lost track.
        let available = &mut self
          .available_by_origin
          .get_mut(&task.origin_id)
          .unwrap()
          .tasks;
        // We will remove this task from the list by using `swap_remove`, so the only other element in the list that will have their `index_in_origin_list` changed is the last element, so update that (unless we are the last element, in which case skip this to avoid deadlocking).
        if index_in_origin_list != available.len() - 1 {
          match &mut available.last_mut().unwrap().lock.lock().state {
            TaskState::Available {
              index_in_origin_list: oi,
            } => *oi = index_in_origin_list,
            _ => unreachable!(),
          };
        };
        // Remove ourselves from the list.
        let t = available.swap_remove(index_in_origin_list);
        // Sanity check: assert that it is us.
        assert!(ptr::eq(t.lock.data_ptr(), &*task));
        // Normally we'd now remove the origin from `origins_available` if the list is now empty, but because we don't have the index in `origins_available`, we'd have to do a linear search and remove, which is too slow; therefore, just leave the possibility of entries in `origins_available` actually referring to a blank list.

        // Update to the new state, so that the existing invalid `index_in_origin_list` is erased.
        task.state = new_state;
        true
      }
      TaskState::Completed => {
        // Task has already completed.
        false
      }
      TaskState::InFlight { .. } => {
        task.state = new_state;
        true
      }
    }
  }

  pub fn update_lock_release_time_for_task(
    &mut self,
    data: TaskData,
    new_time: DateTime<Utc>,
  ) -> bool {
    self.update_in_flight_task_state_if_not_complete(data, TaskState::InFlight {
      lock_release_time: new_time,
    })
  }

  pub fn complete_task(&mut self, data: TaskData) -> bool {
    let rv = self.update_in_flight_task_state_if_not_complete(data, TaskState::Completed);
    if rv {
      self.tasks.remove(&data).unwrap();
    };
    rv
  }

  fn make_task_available(&mut self, task_lock: Task) {
    let mut task = task_lock.lock.lock();
    let available = self.available_by_origin.entry(task.origin_id).or_default();
    let index_in_origin_list = available.tasks.len();
    if available.tasks.is_empty() && !available.in_origins_available {
      self.origins_available.push(task.origin_id);
      available.in_origins_available = true;
    };
    task.state = TaskState::Available {
      index_in_origin_list,
    };
    drop(task);
    available.tasks.push(task_lock);
  }

  pub fn push(&mut self, origin_id: OriginId, data: TaskData) {
    let task = Task {
      lock: Arc::new(Mutex::new(TaskInner {
        origin_id,
        data,
        // This is incorrect but will be immediately updated by `make_task_available`.
        state: TaskState::Completed,
      })),
    };
    self.make_task_available(task.clone());
    let None = self.tasks.insert(data, task) else {
      unreachable!();
    };
  }

  pub fn pop_random(
    &mut self,
    excluding_origin_ids: &FxHashSet<OriginId>,
    lock_until: DateTime<Utc>,
  ) -> Option<TaskData> {
    let n = self.origins_available.len();
    if n == 0 {
      return None;
    };
    // Pick a random starting position in `origins_available`.
    let idx = thread_rng().gen_range(0..n);
    let mut origin_indices_to_remove = Vec::new();
    let mut rv: Option<TaskData> = None;
    // Iterate through entire `origins_available`, starting from random position, to find a suitable non-excluded origin.
    for i in 0..n {
      let origin_idx = (idx + i) % n;
      let origin_id = &self.origins_available[origin_idx];
      if excluding_origin_ids.contains(origin_id) {
        continue;
      };
      let tasks = &mut self.available_by_origin.get_mut(origin_id).unwrap().tasks;
      // Because of `remove_task_from_origin_available_list`, some entries in `origins_available` actually refer to a blank list, so we need to skip and prune them now.
      if tasks.is_empty() {
        origin_indices_to_remove.push(origin_idx);
        continue;
      }
      // Pick a random task.
      let task_idx = thread_rng().gen_range(0..tasks.len());
      // We will remove this randomly-picked task from the list by using `swap_remove`, so the only other element in the list that will have their `index_in_origin_list` changed is the last element, so update that.
      match &mut tasks.last_mut().unwrap().lock.lock().state {
        TaskState::Available {
          index_in_origin_list: oi,
        } => *oi = task_idx,
        _ => unreachable!(),
      };
      let task_lock = tasks.swap_remove(task_idx);
      let mut task = task_lock.lock.lock();
      // Sanity check: assert that our picked task has the correct index.
      match task.state {
        TaskState::Available {
          index_in_origin_list,
        } if index_in_origin_list == task_idx => {}
        _ => unreachable!(),
      };
      // Update our picked task to the new state.
      task.state = TaskState::InFlight {
        lock_release_time: lock_until,
      };
      self.by_lock_release_time.push(ByLockReleaseTime {
        task: task_lock.clone(),
        lock_release_time: lock_until,
      });
      rv = Some(task.data);
      if tasks.is_empty() {
        origin_indices_to_remove.push(origin_idx);
      }
      break;
    }
    origin_indices_to_remove.sort_unstable();
    for idx in origin_indices_to_remove.into_iter().rev() {
      let removed_origin_id = self.origins_available.swap_remove(idx);
      self
        .available_by_origin
        .get_mut(&removed_origin_id)
        .unwrap()
        .in_origins_available = false;
    }
    rv
  }
}

#[cfg(test)]
mod tests {
  use super::TaskData;
  use super::Tasks;
  use crate::origins::OriginId;
  use chrono::TimeZone;
  use chrono::Utc;
  use num_derive::FromPrimitive;
  use num_traits::FromPrimitive;
  use rand::thread_rng;
  use rand::Rng;
  use rustc_hash::FxHashMap;
  use rustc_hash::FxHashSet;
  use strum::EnumCount;

  #[test]
  fn test_maybe_release_locked_task() {
    let set = FxHashSet::default();
    let time = Utc.timestamp_millis_opt(0).unwrap();

    let mut tasks = Tasks::new();
    let origin_id = OriginId::from_raw(42);
    // Nothing should be in flight.
    assert_eq!(tasks.maybe_release_locked_task(), None);

    // We add one available task, which is not in flight yet.
    tasks.push(origin_id, TaskData {
      resource_uid: 1,
      resource_fetch_id: 1,
    });
    assert_eq!(tasks.maybe_release_locked_task(), None);

    // We add another available task, which is not in flight yet.
    tasks.push(origin_id, TaskData {
      resource_uid: 2,
      resource_fetch_id: 2,
    });

    // We pop a task and make it already expired.
    let t = tasks.pop_random(&set, time).unwrap();
    // It's in flight but has expired, so should be released.
    assert_eq!(tasks.maybe_release_locked_task(), Some(t));
    // Nothing else should be in flight.
    assert_eq!(tasks.maybe_release_locked_task(), None);

    // We pop all (2) tasks and make them already expired.
    let t1 = tasks.pop_random(&set, time).unwrap();
    let t2 = tasks.pop_random(&set, time).unwrap();
    assert_eq!(tasks.pop_random(&set, time), None);
    // We release both tasks.
    assert!(tasks
      .maybe_release_locked_task()
      .filter(|&t| t == t1 || t == t2)
      .is_some());
    assert!(tasks
      .maybe_release_locked_task()
      .filter(|&t| t == t1 || t == t2)
      .is_some());
    assert_eq!(tasks.maybe_release_locked_task(), None);
    // Now that the tasks are released, they should be available for us to pop again.
    let t1a = tasks.pop_random(&set, time).unwrap();
    let t2a = tasks.pop_random(&set, time).unwrap();
    assert_eq!(tasks.pop_random(&set, time), None);
    assert!(t1a == t1 && t2a == t2 || t1a == t2 && t2a == t1);
  }

  /// This test does not test `maybe_release_locked_task`, as it's a bit hard given the data structures being used, so that is tested in a separate test.
  #[test]
  fn test_tasks() {
    let mut tasks = Tasks::new();

    let time = Utc.timestamp_opt(0, 0).unwrap();

    let mut next_origin_id = 1;
    let mut next_id = 0;
    let mut available = FxHashSet::<TaskData>::default();
    let mut polled = Vec::<TaskData>::new();
    let mut completed = Vec::<TaskData>::new();
    #[derive(Clone, Copy, PartialEq, Eq, Debug, FromPrimitive, Hash, EnumCount)]
    enum Action {
      CompleteAvailable,
      CompleteCompleted,
      CompleteInvalid,
      CompleteValid,
      HeartbeatAvailable,
      HeartbeatCompleted,
      HeartbeatInvalid,
      HeartbeatValid,
      Poll,
      Push,
    }
    // We collect these metrics so we can see if the test actually hit all test cases and branches with reasonable counts.
    let mut action_counts = FxHashMap::<Action, u64>::default();
    let mut empty_action_counts = FxHashMap::<Action, u64>::default();
    const ITERATIONS: u64 = 1000000;
    // After ITERATIONS, we should stop pushing to pop everything and test what happens when the queue is completely empty and is still repeatedly hit with all these requests. This should be at least ITERATIONS to allow for all entries to be popped.
    const ITERATIONS_NO_PUSH: u64 = 1000000;
    for iteration_no in 0..ITERATIONS + ITERATIONS_NO_PUSH {
      let action = Action::from_usize(thread_rng().gen_range(0..Action::COUNT)).unwrap();
      *action_counts.entry(action).or_default() += 1;
      match action {
        Action::CompleteAvailable => {
          if available.is_empty() {
            *empty_action_counts.entry(action).or_default() += 1;
          } else {
            let t = *available.iter().next().unwrap();
            available.remove(&t);
            let rv = tasks.complete_task(t);
            assert!(rv);
            completed.push(t);
          }
        }
        Action::CompleteCompleted => {
          if completed.is_empty() {
            *empty_action_counts.entry(action).or_default() += 1;
          } else {
            let c = completed[thread_rng().gen_range(0..completed.len())];
            let rv = tasks.complete_task(c);
            assert!(!rv);
          }
        }
        Action::CompleteInvalid => {
          let rv = tasks.complete_task(TaskData {
            resource_fetch_id: next_id,
            resource_uid: next_id,
          });
          assert!(!rv);
        }
        Action::CompleteValid => {
          if polled.is_empty() {
            *empty_action_counts.entry(action).or_default() += 1;
          } else {
            let idx = thread_rng().gen_range(0..polled.len());
            let t = polled.swap_remove(idx);
            let rv = tasks.complete_task(t);
            assert!(rv);
            completed.push(t);
          };
        }
        Action::HeartbeatAvailable => {
          if available.is_empty() {
            *empty_action_counts.entry(action).or_default() += 1;
          } else {
            let t = *available.iter().next().unwrap();
            available.remove(&t);
            let rv = tasks.update_lock_release_time_for_task(t, time);
            assert!(rv);
            polled.push(t);
          }
        }
        Action::HeartbeatCompleted => {
          if completed.is_empty() {
            *empty_action_counts.entry(action).or_default() += 1;
          } else {
            let c = completed[thread_rng().gen_range(0..completed.len())];
            let rv = tasks.update_lock_release_time_for_task(c, time);
            assert!(!rv);
          }
        }
        Action::HeartbeatInvalid => {
          let rv = tasks.update_lock_release_time_for_task(
            TaskData {
              resource_fetch_id: next_id,
              resource_uid: next_id,
            },
            time,
          );
          assert!(!rv);
        }
        Action::HeartbeatValid => {
          if polled.is_empty() {
            *empty_action_counts.entry(action).or_default() += 1;
          } else {
            let idx = thread_rng().gen_range(0..polled.len());
            let rv = tasks.update_lock_release_time_for_task(polled[idx], time);
            assert!(rv);
          };
        }
        Action::Poll => match tasks.pop_random(&FxHashSet::default(), time) {
          Some(t) => {
            assert!(available.remove(&t));
            polled.push(t);
          }
          None => {
            *empty_action_counts.entry(action).or_default() += 1;
            assert!(available.is_empty());
          }
        },
        Action::Push => {
          if iteration_no < ITERATIONS {
            let origin_id = thread_rng().gen_range(0..=next_origin_id);
            if origin_id == next_origin_id {
              next_origin_id += 1;
            };
            let origin_id = OriginId::from_raw(origin_id);
            let id = next_id;
            next_id += 1;
            let task = TaskData {
              resource_fetch_id: id,
              resource_uid: id,
            };
            available.insert(task);
            tasks.push(origin_id, task);
          }
        }
      };
    }
    println!("Action counts: {:?}", action_counts);
    println!("Empty action counts: {:?}", empty_action_counts);
    println!("Origins: {}", next_origin_id - 1);
    println!("Remaining available: {}", available.len());
    println!("Remaining polled: {}", polled.len());
  }
}
