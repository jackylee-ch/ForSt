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
use std::sync::Arc;

use arrow::array::{BinaryBuilder, RecordBatch, UInt64Builder, UInt8Builder};
use arrow::datatypes::{DataType, Field, Schema};
use forst_rs_common::{ForstResult, OpType};

use super::{GetResult, MemTableConfig, ScanRow};

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
    /// Other values are rejected with `invalid_argument`.
    ///
    /// Returns the assigned sequence number.
    pub fn put(&mut self, key: &[u8], value: Option<&[u8]>, op_type_byte: u8) -> ForstResult<u64> {
        if self.frozen {
            return Err(forst_rs_common::ForstError::invalid_argument(
                "cannot write to a frozen MemTable",
            ));
        }

        // SECURITY: validate op_type_byte BEFORE mutating any state, so an
        // invalid byte is rejected without leaving the memtable's columns
        // partially populated. Pre-fix, `OpType::from_u8(...).unwrap_or(Put)`
        // silently accepted any unrecognized byte (4..=255) and treated it
        // as Put — silent corruption potential (Sweep R13 H by Reviewer 1).
        let op_type = OpType::from_u8(op_type_byte).ok_or_else(|| {
            forst_rs_common::ForstError::invalid_argument(format!(
                "invalid op_type byte {} (expected 0=Put, 1=Delete, 2=SingleDelete, 3=Merge)",
                op_type_byte
            ))
        })?;

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
        let merge_threshold =
            ((self.sorted_count as f64) * self.config.unsorted_merge_ratio).max(1024.0) as usize;
        if self.unsorted_entries.len() > merge_threshold {
            self.merge_unsorted_to_sorted();
        }

        Ok(seq)
    }

    /// Inserts a merge operand for the given key.
    ///
    /// This is a convenience wrapper around `put()` that sets
    /// `op_type = OpType::Merge (3)`.
    ///
    /// Returns the assigned sequence number.
    pub fn merge(&mut self, key: &[u8], operand: &[u8]) -> ForstResult<u64> {
        self.put(key, Some(operand), OpType::Merge as u8)
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

    /// Freezes this MemTable, making it immutable.
    ///
    /// Before freezing, all unsorted data is merged into the sorted index.
    /// After freezing, `put()` and `batch_insert()` will return an error.
    pub fn freeze(&mut self) {
        if !self.frozen {
            self.merge_unsorted_to_sorted();
            self.frozen = true;
        }
    }

    /// Exports the entire MemTable as sorted Arrow RecordBatches.
    ///
    /// Each batch contains up to `batch_size` rows. Rows are ordered by
    /// (key ASC, sequence DESC). For each key, all versions are included.
    ///
    /// Schema: `key(Binary), value(Binary nullable), sequence(UInt64), op_type(UInt8)`.
    ///
    /// The MemTable must be frozen before calling this method.
    pub fn to_flush_batches(&self, batch_size: usize) -> ForstResult<Vec<RecordBatch>> {
        if !self.frozen {
            return Err(forst_rs_common::ForstError::invalid_argument(
                "MemTable must be frozen before flushing",
            ));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("sequence", DataType::UInt64, false),
            Field::new("op_type", DataType::UInt8, false),
        ]));

        // Collect all rows in sorted order: key ASC, sequence DESC.
        let mut sorted_rows: Vec<(u32, u64)> = Vec::new();
        for indices in self.sorted_index.values() {
            for idx in indices {
                sorted_rows.push((idx.offset, idx.sequence));
            }
        }

        // Build batches.
        let mut batches = Vec::new();
        let mut row_idx = 0;

        while row_idx < sorted_rows.len() {
            let chunk_end = (row_idx + batch_size).min(sorted_rows.len());
            let mut key_builder = BinaryBuilder::new();
            let mut value_builder = BinaryBuilder::new();
            let mut seq_builder = UInt64Builder::new();
            let mut op_builder = UInt8Builder::new();

            for &(offset, _seq) in &sorted_rows[row_idx..chunk_end] {
                let key = self.key_at(offset);
                key_builder.append_value(key);

                match self.value_at(offset) {
                    Some(v) => value_builder.append_value(v),
                    None => value_builder.append_null(),
                }

                seq_builder.append_value(self.sequences[offset as usize]);
                op_builder.append_value(self.op_types[offset as usize]);
            }

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(key_builder.finish()),
                    Arc::new(value_builder.finish()),
                    Arc::new(seq_builder.finish()),
                    Arc::new(op_builder.finish()),
                ],
            )
            .map_err(|e| forst_rs_common::ForstError::corruption(format!("Arrow error: {}", e)))?;

            batches.push(batch);
            row_idx = chunk_end;
        }

        Ok(batches)
    }

    /// Collects all entries for keys in the `[lower, upper)` range as
    /// `(key, value, sequence, op_type)` tuples in sorted order
    /// (key ASC, sequence DESC).
    ///
    /// Only entries with `sequence <= read_sequence` are included.
    /// Passing `lower=&[]` and `upper=None` yields every visible entry.
    ///
    /// The MemTable does NOT need to be frozen; unsorted buffer entries are
    /// merged into the output stream on the fly.
    pub fn collect_range_entries(
        &self,
        lower: &[u8],
        upper: Option<&[u8]>,
        read_sequence: u64,
    ) -> Vec<ScanRow> {
        use std::collections::BTreeMap;

        // Merge sorted + unsorted into a temporary BTreeMap so callers see a
        // single consistent ordering even when writes have not been folded
        // into the sorted index yet.
        let mut combined: BTreeMap<&[u8], Vec<RowIndex>> = BTreeMap::new();
        for (k, idxs) in self.sorted_index.iter() {
            combined.insert(k.as_slice(), idxs.clone());
        }
        for (k, idxs) in self.unsorted_lookup.iter() {
            combined
                .entry(k.as_slice())
                .and_modify(|v| v.extend_from_slice(idxs))
                .or_insert_with(|| idxs.clone());
        }

        let mut out = Vec::new();
        let range: Box<dyn Iterator<Item = (&&[u8], &Vec<RowIndex>)>> = match upper {
            Some(hi) => Box::new(combined.range(lower..hi)),
            None => Box::new(combined.range(lower..)),
        };

        for (key, indices) in range {
            // Sort by sequence DESC so the freshest version is first.
            let mut sorted = indices.clone();
            sorted.sort_by_key(|idx| std::cmp::Reverse(idx.sequence));
            for idx in sorted {
                if idx.sequence > read_sequence {
                    continue;
                }
                let v = self.value_at(idx.offset).map(|s| s.to_vec());
                out.push((key.to_vec(), v, idx.sequence, idx.op_type));
            }
        }
        out
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

        // SECURITY: validate ALL op_type bytes BEFORE mutating state. If even
        // one byte is invalid, reject the whole batch atomically — pre-fix,
        // the per-row `unwrap_or(Put)` silently downgraded invalid bytes to
        // Put (Sweep R13 H by Reviewer 1). Rejecting up front also avoids
        // partially-populated columns on failure.
        for (i, &b) in op_types.iter().enumerate() {
            if OpType::from_u8(b).is_none() {
                return Err(forst_rs_common::ForstError::invalid_argument(format!(
                    "batch_insert: invalid op_type byte {} at index {} (expected 0=Put, 1=Delete, 2=SingleDelete, 3=Merge)",
                    b, i
                )));
            }
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

            // Safety: validated up-front above; can never panic here.
            let op_type =
                OpType::from_u8(op_types[i]).expect("op_type byte was validated above the loop");
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
        let merge_threshold =
            ((self.sorted_count as f64) * self.config.unsorted_merge_ratio).max(1024.0) as usize;
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

    use arrow::array::{Array, BinaryArray, UInt64Array, UInt8Array};

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

        assert_eq!(
            mt.get(b"a", u64::MAX).unwrap().unwrap().value,
            Some(b"1".to_vec())
        );
        assert_eq!(
            mt.get(b"b", u64::MAX).unwrap().unwrap().value,
            Some(b"2".to_vec())
        );
        assert_eq!(
            mt.get(b"c", u64::MAX).unwrap().unwrap().value,
            Some(b"3".to_vec())
        );
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
            assert_eq!(
                r.unwrap().value,
                Some(expected.into_bytes()),
                "mismatch at {}",
                i
            );
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
            assert_eq!(
                r.value,
                Some(expected_val.into_bytes()),
                "mismatch at {}",
                i
            );
        }
    }

    #[test]
    fn test_freeze_makes_immutable() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"k", Some(b"v"), 0).unwrap();
        mt.freeze();
        assert!(mt.is_frozen());
        assert!(mt.put(b"k2", Some(b"v2"), 0).is_err());
    }

    #[test]
    fn test_freeze_merges_unsorted() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"c", Some(b"3"), 0).unwrap();
        mt.put(b"a", Some(b"1"), 0).unwrap();
        assert!(!mt.unsorted_entries.is_empty());
        mt.freeze();
        assert!(mt.unsorted_entries.is_empty());
        assert!(mt.sorted_index.contains_key(b"a".as_slice()));
    }

    #[test]
    fn test_to_flush_batches_requires_frozen() {
        let mt = VectorizedMemTable::new(test_config());
        assert!(mt.to_flush_batches(1024).is_err());
    }

    #[test]
    fn test_to_flush_batches_sorted_output() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"charlie", Some(b"3"), 0).unwrap();
        mt.put(b"alpha", Some(b"1"), 0).unwrap();
        mt.put(b"bravo", Some(b"2"), 0).unwrap();
        mt.freeze();

        let batches = mt.to_flush_batches(1024).unwrap();
        assert_eq!(batches.len(), 1);

        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);

        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(keys.value(0), b"alpha");
        assert_eq!(keys.value(1), b"bravo");
        assert_eq!(keys.value(2), b"charlie");
    }

    #[test]
    fn test_to_flush_batches_multi_version() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"key", Some(b"v1"), 0).unwrap();
        mt.put(b"key", Some(b"v2"), 0).unwrap();
        mt.freeze();

        let batches = mt.to_flush_batches(1024).unwrap();
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);

        let seqs = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(seqs.value(0), 2);
        assert_eq!(seqs.value(1), 1);
    }

    #[test]
    fn test_to_flush_batches_respects_batch_size() {
        let mut mt = VectorizedMemTable::new(test_config());
        for i in 0..100u32 {
            let key = format!("k_{:05}", i);
            mt.put(key.as_bytes(), Some(b"v"), 0).unwrap();
        }
        mt.freeze();

        let batches = mt.to_flush_batches(30).unwrap();
        assert_eq!(batches.len(), 4);
        assert_eq!(batches[0].num_rows(), 30);
        assert_eq!(batches[3].num_rows(), 10);
    }

    #[test]
    fn test_to_flush_batches_with_tombstones() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"alive", Some(b"val"), 0).unwrap();
        mt.put(b"dead", None, 1).unwrap();
        mt.freeze();

        let batches = mt.to_flush_batches(1024).unwrap();
        let batch = &batches[0];
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let ops = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        assert!(!values.is_null(0));
        assert_eq!(values.value(0), b"val");
        assert_eq!(ops.value(0), 0);

        assert!(values.is_null(1));
        assert_eq!(ops.value(1), 1);
    }

    // --- merge() convenience method tests ---

    #[test]
    fn test_merge_stores_entry() {
        let mut mt = VectorizedMemTable::with_defaults();
        let seq = mt.merge(b"key1", b"operand1").unwrap();
        assert_eq!(seq, 1);
        assert_eq!(mt.num_entries(), 1);

        let result = mt.get(b"key1", u64::MAX).unwrap().unwrap();
        assert_eq!(result.op_type, OpType::Merge);
        assert_eq!(result.value, Some(b"operand1".to_vec()));
        assert_eq!(result.sequence, 1);
    }

    #[test]
    fn test_merge_multiple_operands_same_key() {
        let mut mt = VectorizedMemTable::with_defaults();
        mt.merge(b"key1", b"op1").unwrap();
        mt.merge(b"key1", b"op2").unwrap();
        mt.merge(b"key1", b"op3").unwrap();
        assert_eq!(mt.num_entries(), 3);

        // get() returns the latest entry (highest sequence)
        let result = mt.get(b"key1", u64::MAX).unwrap().unwrap();
        assert_eq!(result.op_type, OpType::Merge);
        assert_eq!(result.value, Some(b"op3".to_vec()));
        assert_eq!(result.sequence, 3);
    }

    #[test]
    fn test_merge_after_put() {
        let mut mt = VectorizedMemTable::with_defaults();
        mt.put(b"key1", Some(b"base"), 0).unwrap(); // Put
        mt.merge(b"key1", b"append1").unwrap(); // Merge
        mt.merge(b"key1", b"append2").unwrap(); // Merge

        // get() returns latest entry (which is a Merge operand)
        // Note: actual merge resolution happens in the Engine read path, not in MemTable
        let result = mt.get(b"key1", u64::MAX).unwrap().unwrap();
        assert_eq!(result.op_type, OpType::Merge);
        assert_eq!(result.value, Some(b"append2".to_vec()));
    }

    #[test]
    fn test_merge_read_sequence_filtering() {
        let mut mt = VectorizedMemTable::with_defaults();
        mt.put(b"key1", Some(b"base"), 0).unwrap(); // seq=1
        mt.merge(b"key1", b"op1").unwrap(); // seq=2
        mt.merge(b"key1", b"op2").unwrap(); // seq=3

        // Read at seq=1: should see the Put
        let result = mt.get(b"key1", 1).unwrap().unwrap();
        assert_eq!(result.op_type, OpType::Put);
        assert_eq!(result.value, Some(b"base".to_vec()));

        // Read at seq=2: should see first merge
        let result = mt.get(b"key1", 2).unwrap().unwrap();
        assert_eq!(result.op_type, OpType::Merge);
        assert_eq!(result.value, Some(b"op1".to_vec()));
    }

    #[test]
    fn test_merge_frozen_memtable_rejects() {
        let mut mt = VectorizedMemTable::with_defaults();
        mt.freeze();
        let err = mt.merge(b"key1", b"op1");
        assert!(err.is_err());
    }

    /// Regression test for Sweep R13 H (Reviewer 1): invalid op_type bytes
    /// (4..=255) must be rejected with `invalid_argument`, not silently
    /// downgraded to `OpType::Put` via `unwrap_or(Put)`. Pre-fix this
    /// would have inserted a row labeled Put with the wrong op_type byte
    /// stored, causing silent data corruption.
    #[test]
    fn test_put_rejects_invalid_op_type() {
        let mut mt = VectorizedMemTable::with_defaults();
        // 0..=3 are Put/Delete/SingleDelete/Merge — valid.
        // 4..=255 must be rejected.
        for invalid in [4u8, 7, 42, 99, 200, 255] {
            let err = mt.put(b"key", Some(b"value"), invalid);
            assert!(err.is_err(), "op_type byte {} should be rejected", invalid);
            let msg = format!("{}", err.unwrap_err());
            assert!(
                msg.contains("invalid op_type byte"),
                "expected 'invalid op_type byte' message; got: {}",
                msg
            );
        }
        // Sanity: state is unchanged after rejection (no rows inserted).
        // key_offsets and value_offsets start with [0] sentinel (len=1);
        // sequences starts empty.
        assert_eq!(mt.sequences.len(), 0);
        assert_eq!(mt.key_offsets.len(), 1, "no key offsets pushed");
        assert_eq!(mt.value_offsets.len(), 1, "no value offsets pushed");
    }

    /// Regression test for Sweep R13 H (Reviewer 1): batch_insert validates
    /// ALL op_type bytes up-front and rejects atomically (no partial state
    /// mutation on failure).
    #[test]
    fn test_batch_insert_rejects_invalid_op_type_atomically() {
        let mut mt = VectorizedMemTable::with_defaults();
        let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"k3"];
        let values: Vec<Option<&[u8]>> = vec![Some(b"v1"), Some(b"v2"), Some(b"v3")];
        // Middle byte is invalid — must reject the whole batch.
        let op_types: &[u8] = &[0, 99, 0];
        let err = mt.batch_insert(&keys, &values, op_types);
        assert!(err.is_err());
        let msg = format!("{}", err.unwrap_err());
        assert!(
            msg.contains("invalid op_type byte 99 at index 1"),
            "expected indexed-byte error message; got: {}",
            msg
        );
        // Atomic rejection: no rows inserted (sentinel preserved).
        assert_eq!(mt.sequences.len(), 0);
        assert_eq!(mt.key_offsets.len(), 1, "key_offsets sentinel preserved");
    }

    #[test]
    fn test_freeze_to_flush_roundtrip_100k() {
        let mut mt = VectorizedMemTable::new(MemTableConfig {
            max_size: 256 * 1024 * 1024,
            unsorted_merge_ratio: 0.25,
        });

        let n = 100_000usize;
        for i in 0..n {
            let key = format!("rk_{:08}", i);
            let val = format!("rv_{:08}", i);
            mt.put(key.as_bytes(), Some(val.as_bytes()), 0).unwrap();
        }
        mt.freeze();

        let batches = mt.to_flush_batches(10_000).unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, n);

        let mut prev_key: Option<Vec<u8>> = None;
        for batch in &batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let key = keys.value(i).to_vec();
                if let Some(ref pk) = prev_key {
                    assert!(key >= *pk, "keys not sorted");
                }
                prev_key = Some(key);
            }
        }
    }
}
