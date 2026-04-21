// Copyright 2026 The ForSt-RS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! [`VectorizedMemTable`] — hybrid Sorted Run Array + BTreeMap implementation.
//!
//! Columnar storage (key, value, sequence, op_type) mirrors the SST Arrow
//! schema. A BTreeMap sorted index enables O(log N) point lookups over
//! already-merged data, while a HashMap buffers recent unsorted writes.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};

use forst_rs_common::{ForstResult, OpType};

use super::{GetResult, MemTableConfig};

/// Index of a single row within the columnar storage arrays.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // Fields used by get() in Task 3 and batch_insert() in Task 4.
struct RowIndex {
    /// Position in the columnar arrays.
    offset: u32,
    /// Sequence number (for MVCC).
    sequence: u64,
    /// Operation type.
    op_type: OpType,
}

/// A vectorized MemTable using columnar storage + BTreeMap sorted index.
///
/// **Write path:** `put()` appends data to columnar arrays and inserts into
/// the unsorted HashMap. When the unsorted zone grows beyond the configured
/// ratio, entries are merged into the sorted BTreeMap.
///
/// **Read path:** `get()` checks the unsorted HashMap first (newer data),
/// then the sorted BTreeMap, returning the entry with the highest sequence.
pub struct VectorizedMemTable {
    // -- Columnar storage (append-only) --
    /// Key bytes, concatenated.
    key_data: Vec<u8>,
    /// End-offsets into `key_data` for each row. Row i spans
    /// `key_offsets[i]..key_offsets[i+1]`.
    key_offsets: Vec<u32>,

    /// Value bytes, concatenated.
    value_data: Vec<u8>,
    /// End-offsets into `value_data`.
    value_offsets: Vec<u32>,
    /// Tracks which rows have null (tombstone) values.
    value_nulls: Vec<bool>,

    /// Sequence numbers, one per row.
    sequences: Vec<u64>,
    /// Operation types, one per row.
    op_types: Vec<u8>,

    // -- Indexes --
    /// Sorted index: key -> list of RowIndex (multi-version, newest first after merge).
    sorted_index: BTreeMap<Vec<u8>, Vec<RowIndex>>,
    /// Number of rows covered by the sorted index.
    sorted_count: u32,

    /// Unsorted buffer: row offsets not yet merged into sorted_index.
    unsorted_entries: Vec<u32>,
    /// Unsorted lookup: key -> list of RowIndex for quick point lookups.
    unsorted_lookup: HashMap<Vec<u8>, Vec<RowIndex>>,

    // -- State --
    /// Current sequence counter (incremented on each insert).
    next_sequence: u64,
    /// Approximate memory usage in bytes.
    memory_used: usize,
    /// Whether this MemTable has been frozen (immutable).
    frozen: bool,
    /// Configuration.
    config: MemTableConfig,
}

impl VectorizedMemTable {
    /// Creates a new, empty VectorizedMemTable.
    pub fn new(config: MemTableConfig) -> Self {
        Self {
            key_data: Vec::new(),
            key_offsets: vec![0], // sentinel
            value_data: Vec::new(),
            value_offsets: vec![0], // sentinel
            value_nulls: Vec::new(),
            sequences: Vec::new(),
            op_types: Vec::new(),
            sorted_index: BTreeMap::new(),
            sorted_count: 0,
            unsorted_entries: Vec::new(),
            unsorted_lookup: HashMap::new(),
            next_sequence: 1,
            memory_used: 0,
            frozen: false,
            config,
        }
    }

    /// Creates a new MemTable with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(MemTableConfig::default())
    }

    /// Inserts a single key-value pair.
    ///
    /// `value` is `None` for delete tombstones.
    /// `op_type_byte`: 0 = Put, 1 = Delete, 2 = SingleDelete, 3 = Merge.
    ///
    /// Returns the assigned sequence number.
    pub fn put(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        op_type_byte: u8,
    ) -> ForstResult<u64> {
        if self.frozen {
            return Err(forst_rs_common::ForstError::invalid_argument(
                "cannot write to a frozen MemTable",
            ));
        }

        let seq = self.next_sequence;
        self.next_sequence += 1;

        let row_offset = self.sequences.len() as u32;

        // Append key.
        self.key_data.extend_from_slice(key);
        self.key_offsets.push(self.key_data.len() as u32);

        // Append value.
        match value {
            Some(v) => {
                self.value_data.extend_from_slice(v);
                self.value_offsets.push(self.value_data.len() as u32);
                self.value_nulls.push(false);
            }
            None => {
                self.value_offsets.push(self.value_data.len() as u32);
                self.value_nulls.push(true);
            }
        }

        // Append sequence and op_type.
        self.sequences.push(seq);
        self.op_types.push(op_type_byte);

        let op_type = OpType::from_u8(op_type_byte).unwrap_or(OpType::Put);
        let row_index = RowIndex {
            offset: row_offset,
            sequence: seq,
            op_type,
        };

        // Add to unsorted zone.
        self.unsorted_entries.push(row_offset);
        self.unsorted_lookup
            .entry(key.to_vec())
            .or_default()
            .push(row_index);

        // Update memory tracking (approximate).
        self.memory_used += key.len() + value.map_or(0, |v| v.len()) + 8 + 1 + 48;

        // Check if merge is needed.
        let merge_threshold = ((self.sorted_count as f64) * self.config.unsorted_merge_ratio)
            .max(1024.0) as usize;
        if self.unsorted_entries.len() > merge_threshold {
            self.merge_unsorted_to_sorted();
        }

        Ok(seq)
    }

    /// Returns the total number of entries (rows) in the MemTable.
    pub fn num_entries(&self) -> usize {
        self.sequences.len()
    }

    /// Returns the approximate memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        self.memory_used
    }

    /// Returns whether this MemTable has been frozen.
    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    /// Returns whether the MemTable should be flushed (memory limit reached).
    pub fn should_flush(&self) -> bool {
        self.memory_used >= self.config.max_size
    }

    /// Retrieves the key bytes for a given row offset.
    /// Used by get() (Task 3) and to_flush_batches() (Task 5).
    #[allow(dead_code)]
    fn key_at(&self, offset: u32) -> &[u8] {
        let start = self.key_offsets[offset as usize] as usize;
        let end = self.key_offsets[offset as usize + 1] as usize;
        &self.key_data[start..end]
    }

    /// Retrieves the value bytes for a given row offset. Returns `None` for tombstones.
    /// Used by get() (Task 3) and to_flush_batches() (Task 5).
    fn value_at(&self, offset: u32) -> Option<&[u8]> {
        if self.value_nulls[offset as usize] {
            return None;
        }
        let start = self.value_offsets[offset as usize] as usize;
        let end = self.value_offsets[offset as usize + 1] as usize;
        Some(&self.value_data[start..end])
    }

    /// Merges all unsorted entries into the sorted BTreeMap index.
    pub fn merge_unsorted_to_sorted(&mut self) {
        for (key, mut indices) in self.unsorted_lookup.drain() {
            let entry = self.sorted_index.entry(key).or_default();
            entry.append(&mut indices);
            // Sort by sequence descending so newest is first.
            entry.sort_by_key(|idx| Reverse(idx.sequence));
        }
        self.sorted_count += self.unsorted_entries.len() as u32;
        self.unsorted_entries.clear();
    }

    /// Point lookup: returns the latest entry for `key` with sequence <= `read_sequence`.
    ///
    /// Checks the unsorted zone first (newer data), then the sorted index.
    /// Returns the entry with the highest sequence number.
    pub fn get(&self, key: &[u8], read_sequence: u64) -> ForstResult<Option<GetResult>> {
        // Check unsorted zone first (potentially newer).
        let unsorted_result = self
            .unsorted_lookup
            .get(key)
            .and_then(|indices| self.find_latest(indices, read_sequence));

        // Check sorted index.
        let sorted_result = self
            .sorted_index
            .get(key)
            .and_then(|indices| self.find_latest(indices, read_sequence));

        // Return the one with the higher sequence.
        let result = match (unsorted_result, sorted_result) {
            (Some(u), Some(s)) => {
                if u.sequence >= s.sequence {
                    Some(u)
                } else {
                    Some(s)
                }
            }
            (Some(u), None) => Some(u),
            (None, Some(s)) => Some(s),
            (None, None) => None,
        };

        Ok(result)
    }

    /// Batch-inserts multiple entries at once.
    ///
    /// All arrays must have the same length. `values[i]` is `None` for deletes.
    /// Sequences are assigned monotonically starting from `self.next_sequence`.
    ///
    /// Returns the number of entries inserted.
    pub fn batch_insert(
        &mut self,
        keys: &[&[u8]],
        values: &[Option<&[u8]>],
        op_types: &[u8],
    ) -> ForstResult<usize> {
        if self.frozen {
            return Err(forst_rs_common::ForstError::invalid_argument(
                "cannot write to a frozen MemTable",
            ));
        }
        if keys.len() != values.len() || keys.len() != op_types.len() {
            return Err(forst_rs_common::ForstError::invalid_argument(
                "batch_insert: all arrays must have the same length",
            ));
        }

        let count = keys.len();
        let base_seq = self.next_sequence;
        self.next_sequence += count as u64;
        let base_offset = self.sequences.len() as u32;

        for i in 0..count {
            let key = keys[i];
            let value = values[i];
            let seq = base_seq + i as u64;
            let row_offset = base_offset + i as u32;

            // Append key.
            self.key_data.extend_from_slice(key);
            self.key_offsets.push(self.key_data.len() as u32);

            // Append value.
            match value {
                Some(v) => {
                    self.value_data.extend_from_slice(v);
                    self.value_offsets.push(self.value_data.len() as u32);
                    self.value_nulls.push(false);
                }
                None => {
                    self.value_offsets.push(self.value_data.len() as u32);
                    self.value_nulls.push(true);
                }
            }

            self.sequences.push(seq);
            self.op_types.push(op_types[i]);

            let op_type = OpType::from_u8(op_types[i]).unwrap_or(OpType::Put);
            let row_index = RowIndex {
                offset: row_offset,
                sequence: seq,
                op_type,
            };

            self.unsorted_entries.push(row_offset);
            self.unsorted_lookup
                .entry(key.to_vec())
                .or_default()
                .push(row_index);

            self.memory_used += key.len() + value.map_or(0, |v| v.len()) + 8 + 1 + 48;
        }

        // Check merge threshold.
        let merge_threshold = ((self.sorted_count as f64) * self.config.unsorted_merge_ratio)
            .max(1024.0) as usize;
        if self.unsorted_entries.len() > merge_threshold {
            self.merge_unsorted_to_sorted();
        }

        Ok(count)
    }

    /// Among a list of RowIndex entries, find the one with the highest
    /// sequence that is <= `read_sequence` and build a GetResult.
    fn find_latest(&self, indices: &[RowIndex], read_sequence: u64) -> Option<GetResult> {
        let mut best: Option<&RowIndex> = None;
        for idx in indices {
            if idx.sequence <= read_sequence {
                match best {
                    Some(b) if idx.sequence <= b.sequence => {}
                    _ => best = Some(idx),
                }
            }
        }
        best.map(|idx| GetResult {
            value: self.value_at(idx.offset).map(|v| v.to_vec()),
            sequence: idx.sequence,
            op_type: idx.op_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MemTableConfig {
        MemTableConfig {
            max_size: 1024 * 1024, // 1MB
            unsorted_merge_ratio: 0.25,
        }
    }

    #[test]
    fn test_new_memtable_empty() {
        let mt = VectorizedMemTable::new(test_config());
        assert_eq!(mt.num_entries(), 0);
        assert_eq!(mt.memory_usage(), 0);
        assert!(!mt.is_frozen());
    }

    #[test]
    fn test_put_single_entry() {
        let mut mt = VectorizedMemTable::new(test_config());
        let seq = mt.put(b"key1", Some(b"val1"), 0).unwrap();
        assert_eq!(seq, 1);
        assert_eq!(mt.num_entries(), 1);
    }

    #[test]
    fn test_put_multiple_entries() {
        let mut mt = VectorizedMemTable::new(test_config());
        for i in 0..100u32 {
            let key = format!("key_{:05}", i);
            let val = format!("val_{:05}", i);
            let seq = mt.put(key.as_bytes(), Some(val.as_bytes()), 0).unwrap();
            assert_eq!(seq, i as u64 + 1);
        }
        assert_eq!(mt.num_entries(), 100);
    }

    #[test]
    fn test_put_delete_tombstone() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"del_key", None, 1).unwrap(); // Delete
        assert_eq!(mt.num_entries(), 1);
        assert!(mt.value_at(0).is_none());
    }

    #[test]
    fn test_put_frozen_fails() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.frozen = true;
        let result = mt.put(b"k", Some(b"v"), 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_key_at_value_at() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"alpha", Some(b"one"), 0).unwrap();
        mt.put(b"beta", Some(b"two"), 0).unwrap();
        mt.put(b"gamma", None, 1).unwrap(); // delete

        assert_eq!(mt.key_at(0), b"alpha");
        assert_eq!(mt.key_at(1), b"beta");
        assert_eq!(mt.key_at(2), b"gamma");
        assert_eq!(mt.value_at(0), Some(b"one".as_slice()));
        assert_eq!(mt.value_at(1), Some(b"two".as_slice()));
        assert_eq!(mt.value_at(2), None);
    }

    #[test]
    fn test_merge_unsorted_to_sorted() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"c", Some(b"3"), 0).unwrap();
        mt.put(b"a", Some(b"1"), 0).unwrap();
        mt.put(b"b", Some(b"2"), 0).unwrap();

        assert_eq!(mt.unsorted_entries.len(), 3);
        assert_eq!(mt.sorted_count, 0);

        mt.merge_unsorted_to_sorted();

        assert_eq!(mt.unsorted_entries.len(), 0);
        assert_eq!(mt.sorted_count, 3);
        // Keys should be in BTreeMap
        assert!(mt.sorted_index.contains_key(b"a".as_slice()));
        assert!(mt.sorted_index.contains_key(b"b".as_slice()));
        assert!(mt.sorted_index.contains_key(b"c".as_slice()));
    }

    #[test]
    fn test_merge_multi_version_same_key() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"key", Some(b"v1"), 0).unwrap(); // seq=1
        mt.put(b"key", Some(b"v2"), 0).unwrap(); // seq=2

        mt.merge_unsorted_to_sorted();

        let indices = mt.sorted_index.get(b"key".as_slice()).unwrap();
        assert_eq!(indices.len(), 2);
        // Newest first
        assert_eq!(indices[0].sequence, 2);
        assert_eq!(indices[1].sequence, 1);
    }

    #[test]
    fn test_sequence_monotonically_increasing() {
        let mut mt = VectorizedMemTable::new(test_config());
        let s1 = mt.put(b"a", Some(b"1"), 0).unwrap();
        let s2 = mt.put(b"b", Some(b"2"), 0).unwrap();
        let s3 = mt.put(b"c", Some(b"3"), 0).unwrap();
        assert!(s1 < s2);
        assert!(s2 < s3);
    }

    #[test]
    fn test_memory_usage_increases() {
        let mut mt = VectorizedMemTable::new(test_config());
        assert_eq!(mt.memory_usage(), 0);
        mt.put(b"key", Some(b"value"), 0).unwrap();
        assert!(mt.memory_usage() > 0);
    }

    #[test]
    fn test_get_existing_key() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"hello", Some(b"world"), 0).unwrap();
        let result = mt.get(b"hello", u64::MAX).unwrap();
        assert!(result.is_some());
        let r = result.unwrap();
        assert_eq!(r.value, Some(b"world".to_vec()));
        assert_eq!(r.sequence, 1);
        assert_eq!(r.op_type, OpType::Put);
    }

    #[test]
    fn test_get_missing_key() {
        let mt = VectorizedMemTable::new(test_config());
        let result = mt.get(b"missing", u64::MAX).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_get_returns_latest_version() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"key", Some(b"v1"), 0).unwrap();
        mt.put(b"key", Some(b"v2"), 0).unwrap();
        mt.put(b"key", Some(b"v3"), 0).unwrap();

        let r = mt.get(b"key", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v3".to_vec()));
        assert_eq!(r.sequence, 3);
    }

    #[test]
    fn test_get_respects_read_sequence() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"key", Some(b"v1"), 0).unwrap();
        mt.put(b"key", Some(b"v2"), 0).unwrap();
        mt.put(b"key", Some(b"v3"), 0).unwrap();

        let r = mt.get(b"key", 2).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v2".to_vec()));
        assert_eq!(r.sequence, 2);

        let r = mt.get(b"key", 1).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v1".to_vec()));
    }

    #[test]
    fn test_get_delete_tombstone() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"key", Some(b"val"), 0).unwrap();
        mt.put(b"key", None, 1).unwrap();

        let r = mt.get(b"key", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, None);
        assert_eq!(r.op_type, OpType::Delete);
        assert_eq!(r.sequence, 2);
    }

    #[test]
    fn test_get_after_merge_to_sorted() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"a", Some(b"1"), 0).unwrap();
        mt.put(b"b", Some(b"2"), 0).unwrap();
        mt.merge_unsorted_to_sorted();

        mt.put(b"a", Some(b"updated"), 0).unwrap();

        let r = mt.get(b"a", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"updated".to_vec()));
        assert_eq!(r.sequence, 3);

        let r = mt.get(b"b", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"2".to_vec()));
    }

    #[test]
    fn test_batch_insert_basic() {
        let mut mt = VectorizedMemTable::new(test_config());
        let keys: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let values: Vec<Option<&[u8]>> = vec![Some(b"1"), Some(b"2"), Some(b"3")];
        let ops = vec![0u8, 0, 0];

        let count = mt.batch_insert(&keys, &values, &ops).unwrap();
        assert_eq!(count, 3);
        assert_eq!(mt.num_entries(), 3);

        assert_eq!(mt.get(b"a", u64::MAX).unwrap().unwrap().value, Some(b"1".to_vec()));
        assert_eq!(mt.get(b"b", u64::MAX).unwrap().unwrap().value, Some(b"2".to_vec()));
        assert_eq!(mt.get(b"c", u64::MAX).unwrap().unwrap().value, Some(b"3".to_vec()));
    }

    #[test]
    fn test_batch_insert_with_deletes() {
        let mut mt = VectorizedMemTable::new(test_config());
        let keys: Vec<&[u8]> = vec![b"x", b"y"];
        let values: Vec<Option<&[u8]>> = vec![Some(b"val"), None];
        let ops = vec![0u8, 1]; // Put, Delete

        mt.batch_insert(&keys, &values, &ops).unwrap();

        let r = mt.get(b"x", u64::MAX).unwrap().unwrap();
        assert_eq!(r.op_type, OpType::Put);

        let r = mt.get(b"y", u64::MAX).unwrap().unwrap();
        assert_eq!(r.op_type, OpType::Delete);
        assert_eq!(r.value, None);
    }

    #[test]
    fn test_batch_insert_mismatched_lengths() {
        let mut mt = VectorizedMemTable::new(test_config());
        let keys: Vec<&[u8]> = vec![b"a", b"b"];
        let values: Vec<Option<&[u8]>> = vec![Some(b"1")]; // wrong length
        let ops = vec![0u8, 0];

        let result = mt.batch_insert(&keys, &values, &ops);
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_insert_100k_then_get_all() {
        let mut mt = VectorizedMemTable::new(MemTableConfig {
            max_size: 256 * 1024 * 1024,
            unsorted_merge_ratio: 0.25,
        });

        let n = 100_000;
        let batch_size = 1000;

        for batch_start in (0..n).step_by(batch_size) {
            let batch_end = (batch_start + batch_size).min(n);
            let keys: Vec<Vec<u8>> = (batch_start..batch_end)
                .map(|i| format!("k_{:08}", i).into_bytes())
                .collect();
            let values: Vec<Vec<u8>> = (batch_start..batch_end)
                .map(|i| format!("v_{:08}", i).into_bytes())
                .collect();

            let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
            let val_refs: Vec<Option<&[u8]>> = values.iter().map(|v| Some(v.as_slice())).collect();
            let ops = vec![0u8; batch_end - batch_start];

            mt.batch_insert(&key_refs, &val_refs, &ops).unwrap();
        }

        assert_eq!(mt.num_entries(), n);

        // Verify all entries can be retrieved.
        for i in 0..n {
            let key = format!("k_{:08}", i);
            let expected = format!("v_{:08}", i);
            let r = mt.get(key.as_bytes(), u64::MAX).unwrap();
            assert!(r.is_some(), "key {} not found", key);
            assert_eq!(r.unwrap().value, Some(expected.into_bytes()), "mismatch at {}", i);
        }
    }

    #[test]
    fn test_get_1000_entries() {
        let mut mt = VectorizedMemTable::new(test_config());
        for i in 0..1000u32 {
            let key = format!("k_{:06}", i);
            let val = format!("v_{:06}", i);
            mt.put(key.as_bytes(), Some(val.as_bytes()), 0).unwrap();
        }

        for i in 0..1000u32 {
            let key = format!("k_{:06}", i);
            let expected_val = format!("v_{:06}", i);
            let r = mt.get(key.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.value, Some(expected_val.into_bytes()), "mismatch at {}", i);
        }
    }
}
