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

use super::MemTableConfig;

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
    #[allow(dead_code)]
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
}
