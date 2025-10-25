use off64::u64;
use off64::usz;
use std::collections::BTreeMap;

/// Merges a sequence of writes that may overlap, producing a minimal set of non-overlapping writes.
/// Later writes win where there are overlaps.
///
/// Returns a BTreeMap where keys are start offsets and values are the merged data.
/// The result contains only non-overlapping intervals with the final data for each byte position.
///
/// # Complexity
/// - O(N log N) typical case for N writes with few overlaps
/// - O(N² log N) worst case if every write overlaps everything
///
/// # Example
/// ```ignore
/// let writes = vec![
///   WriteRequest::new(0, vec![1, 2, 3, 4]),
///   WriteRequest::new(2, vec![5, 6]),  // Overlaps and wins
/// ];
/// let merged = merge_overlapping_writes(writes);
/// // Result: {0 => [1, 2], 2 => [5, 6]}
/// ```
pub(crate) fn merge_overlapping_writes(
  writes: impl IntoIterator<Item = (u64, Vec<u8>)>,
) -> BTreeMap<u64, Vec<u8>> {
  let mut intervals: BTreeMap<u64, Vec<u8>> = BTreeMap::new();

  for (offset, data) in writes {
    let end = offset + u64!(data.len());

    // Find all intervals that overlap with [offset, end).
    // An interval [s, e) overlaps if s < end && e > offset.
    let overlapping_keys: Vec<u64> = intervals
      .range(..end)
      .filter(|(&start, old_data)| start + u64!(old_data.len()) > offset)
      .map(|(&start, _)| start)
      .collect();

    // Remove overlapping intervals and process them.
    let mut to_insert = Vec::new();

    for key in overlapping_keys {
      let old_data = intervals.remove(&key).unwrap();
      let old_end = key + u64!(old_data.len());

      // Preserve the part before our new write if it exists.
      if key < offset {
        let preserved_len = usz!(offset - key);
        to_insert.push((key, old_data[..preserved_len].to_vec()));
      }

      // Preserve the part after our new write if it exists.
      if old_end > end {
        let skip_len = usz!(end - key);
        to_insert.push((end, old_data[skip_len..].to_vec()));
      }
    }

    // Insert the new write.
    to_insert.push((offset, data.to_vec()));

    // Insert all intervals back.
    for (start, data) in to_insert {
      intervals.insert(start, data);
    }
  }

  intervals
}

#[cfg(test)]
mod tests {
  use super::*;

  /// Helper to verify the merged intervals match expected results
  fn assert_intervals(intervals: &BTreeMap<u64, Vec<u8>>, expected: &[(u64, Vec<u8>)]) {
    let actual: Vec<_> = intervals
      .iter()
      .map(|(&start, data)| (start, data.clone()))
      .collect();
    assert_eq!(
      actual, expected,
      "\nActual intervals:   {:?}\nExpected intervals: {:?}",
      actual, expected
    );
  }

  #[test]
  fn test_merge_no_writes() {
    let writes: Vec<(u64, Vec<u8>)> = vec![];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[]);
  }

  #[test]
  fn test_merge_single_write() {
    let writes = vec![(10, vec![1, 2, 3, 4])];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[(10, vec![1, 2, 3, 4])]);
  }

  #[test]
  fn test_merge_non_overlapping_sequential() {
    let writes = vec![(0, vec![1, 2]), (2, vec![3, 4]), (4, vec![5, 6])];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[
      (0, vec![1, 2]),
      (2, vec![3, 4]),
      (4, vec![5, 6]),
    ]);
  }

  #[test]
  fn test_merge_non_overlapping_gaps() {
    let writes = vec![(0, vec![1, 2]), (10, vec![3, 4]), (20, vec![5, 6])];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[
      (0, vec![1, 2]),
      (10, vec![3, 4]),
      (20, vec![5, 6]),
    ]);
  }

  #[test]
  fn test_merge_complete_overlap_later_wins() {
    let writes = vec![
      (0, vec![1, 2, 3, 4]),
      (0, vec![5, 6, 7, 8]), // Completely overwrites first
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[(0, vec![5, 6, 7, 8])]);
  }

  #[test]
  fn test_merge_partial_overlap_at_end() {
    let writes = vec![
      (0, vec![1, 2, 3, 4]), // [0..4)
      (2, vec![5, 6]),       // [2..4) overlaps
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[(0, vec![1, 2]), (2, vec![5, 6])]);
  }

  #[test]
  fn test_merge_partial_overlap_at_start() {
    let writes = vec![
      (2, vec![1, 2, 3, 4]), // [2..6)
      (0, vec![5, 6]),       // [0..2) adjacent, not overlapping
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[(0, vec![5, 6]), (2, vec![1, 2, 3, 4])]);
  }

  #[test]
  fn test_merge_write_inside_another() {
    let writes = vec![
      (0, vec![1, 2, 3, 4, 5, 6]), // [0..6)
      (2, vec![7, 8]),             // [2..4) inside
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[
      (0, vec![1, 2]),
      (2, vec![7, 8]),
      (4, vec![5, 6]),
    ]);
  }

  #[test]
  fn test_merge_write_spans_multiple() {
    let writes = vec![
      (0, vec![1, 2]),
      (2, vec![3, 4]),
      (4, vec![5, 6]),
      (1, vec![7, 8, 9, 10, 11]), // [1..6) spans all three
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[(0, vec![1]), (1, vec![7, 8, 9, 10, 11])]);
  }

  #[test]
  fn test_merge_multiple_overlaps_later_wins() {
    let writes = vec![
      (0, vec![1, 2, 3, 4]),
      (1, vec![5, 6]), // [1..3) overlaps
      (2, vec![7, 8]), // [2..4) overlaps previous
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[
      (0, vec![1]),
      (1, vec![5]),
      (2, vec![7, 8]),
    ]);
  }

  #[test]
  fn test_merge_identical_offsets_later_wins() {
    let writes = vec![(5, vec![1, 2, 3]), (5, vec![4, 5, 6]), (5, vec![7, 8, 9])];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[(5, vec![7, 8, 9])]);
  }

  #[test]
  fn test_merge_complex_overlapping_sequence() {
    let writes = vec![
      (0, vec![1, 2, 3, 4, 5]), // [0..5)
      (3, vec![6, 7, 8, 9]),    // [3..7) extends and overlaps
      (1, vec![10, 11]),        // [1..3) overlaps first part
      (6, vec![12, 13, 14]),    // [6..9) extends further
      (2, vec![15]),            // [2..3) single byte
    ];
    let merged = merge_overlapping_writes(writes);
    // Expected:
    // [0..1): 1
    // [1..2): 10
    // [2..3): 15
    // [3..6): 6,7,8
    // [6..9): 12,13,14
    assert_intervals(&merged, &[
      (0, vec![1]),
      (1, vec![10]),
      (2, vec![15]),
      (3, vec![6, 7, 8]),
      (6, vec![12, 13, 14]),
    ]);
  }

  #[test]
  fn test_merge_zero_length_not_supported() {
    // Zero-length writes should just be no-ops (they create a 0-length interval)
    let writes = vec![
      (5, vec![1, 2, 3]),
      (10, vec![]), // Empty write
    ];
    let merged = merge_overlapping_writes(writes);
    // The empty write creates [10, 10) which is valid but has no effect
    assert_intervals(&merged, &[(5, vec![1, 2, 3]), (10, vec![])]);
  }

  #[test]
  fn test_merge_out_of_order_offsets() {
    // Writes don't need to be in order
    let writes = vec![(20, vec![5, 6]), (0, vec![1, 2]), (10, vec![3, 4])];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[
      (0, vec![1, 2]),
      (10, vec![3, 4]),
      (20, vec![5, 6]),
    ]);
  }

  #[test]
  fn test_merge_adjacent_not_merged() {
    // Adjacent intervals should remain separate
    let writes = vec![
      (0, vec![1, 2]),
      (2, vec![3, 4]), // Starts exactly where first ends
    ];
    let merged = merge_overlapping_writes(writes);
    // These are adjacent, not overlapping, so they stay separate
    assert_intervals(&merged, &[(0, vec![1, 2]), (2, vec![3, 4])]);
  }

  #[test]
  fn test_merge_large_span_over_many_small() {
    let writes = vec![
      (0, vec![1]),
      (2, vec![2]),
      (4, vec![3]),
      (6, vec![4]),
      (8, vec![5]),
      (1, vec![10, 11, 12, 13, 14, 15, 16, 17]), // [1..9) covers most
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[
      (0, vec![1]),
      (1, vec![10, 11, 12, 13, 14, 15, 16, 17]),
    ]);
  }

  #[test]
  fn test_merge_write_extends_left() {
    let writes = vec![
      (10, vec![1, 2, 3, 4]),
      (5, vec![5, 6, 7, 8, 9, 10]), // [5..11) extends left and overlaps
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[
      (5, vec![5, 6, 7, 8, 9, 10]),
      (11, vec![2, 3, 4]),
    ]);
  }

  #[test]
  fn test_merge_write_extends_right() {
    let writes = vec![
      (5, vec![1, 2, 3, 4]),
      (7, vec![5, 6, 7, 8, 9]), // [7..12) extends right and overlaps
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[(5, vec![1, 2]), (7, vec![5, 6, 7, 8, 9])]);
  }

  #[test]
  fn test_merge_alternating_overlaps() {
    let writes = vec![
      (0, vec![1, 2, 3]),
      (2, vec![4, 5, 6]),
      (4, vec![7, 8, 9]),
      (6, vec![10, 11, 12]),
    ];
    let merged = merge_overlapping_writes(writes);
    // [0..2): 1,2
    // [2..4): 4,5
    // [4..6): 7,8
    // [6..9): 10,11,12
    assert_intervals(&merged, &[
      (0, vec![1, 2]),
      (2, vec![4, 5]),
      (4, vec![7, 8]),
      (6, vec![10, 11, 12]),
    ]);
  }

  #[test]
  fn test_merge_same_byte_overwritten_multiple_times() {
    // Multiple writes to the exact same byte
    let writes = vec![
      (5, vec![1]),
      (5, vec![2]),
      (5, vec![3]),
      (5, vec![4]),
      (5, vec![5]),
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[(5, vec![5])]);
  }

  #[test]
  fn test_merge_preserves_data_correctly() {
    // Ensure data bytes are preserved correctly when splitting
    let writes = vec![
      (0, vec![10, 20, 30, 40, 50, 60, 70, 80]), // [0..8)
      (2, vec![99, 88]),                         // [2..4) overwrites 30, 40
    ];
    let merged = merge_overlapping_writes(writes);
    assert_intervals(&merged, &[
      (0, vec![10, 20]),
      (2, vec![99, 88]),
      (4, vec![50, 60, 70, 80]),
    ]);
  }
}
