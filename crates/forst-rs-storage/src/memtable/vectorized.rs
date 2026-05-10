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

use arrow::array::{Array, BinaryArray, BinaryBuilder, RecordBatch, UInt64Builder, UInt8Builder};
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
    ///
    /// PERF (B2): keyed by `Box<[u8]>` rather than `Vec<u8>` — saves 8 bytes
    /// per entry (no `cap` field) and uses a right-sized allocation instead of
    /// the `to_vec()` over-alloc + realloc pattern. The hot path uses
    /// `get_mut`-then-`insert` rather than `entry()` so collisions on the
    /// same key (e.g. per-key aggregations) skip the key-allocation entirely.
    unsorted_lookup: HashMap<Box<[u8]>, Vec<RowIndex>>,
    /// Pool of recycled `Vec<RowIndex>` allocations released by the merge step.
    /// PERF (B2): avoids re-allocating the per-key index list on every put.
    rowindex_vec_pool: Vec<Vec<RowIndex>>,

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
    ///
    /// PERF (B2): pre-reserves column buffer capacity so the first ~64 KiB of
    /// key+value bytes can be appended without `Vec` growth-reallocation. The
    /// reservation is a one-time cost when a fresh memtable is created and a
    /// lower bound on the eventual memory footprint anyway (the configured
    /// `max_size` is many MiB).
    pub fn new(config: MemTableConfig) -> Self {
        // Initial column buffer reservation. Keep small so creating many empty
        // memtables (e.g. per-CF) doesn't blow up RSS, but large enough to
        // cover the first JMH warm-up batch.
        const INIT_CAPACITY_BYTES: usize = 64 * 1024;
        const INIT_ROWS_HINT: usize = 1024;

        let mut key_offsets = Vec::with_capacity(INIT_ROWS_HINT + 1);
        key_offsets.push(0); // sentinel
        let mut value_offsets = Vec::with_capacity(INIT_ROWS_HINT + 1);
        value_offsets.push(0); // sentinel

        Self {
            key_data: Vec::with_capacity(INIT_CAPACITY_BYTES),
            key_offsets,
            value_data: Vec::with_capacity(INIT_CAPACITY_BYTES),
            value_offsets,
            value_nulls: Vec::with_capacity(INIT_ROWS_HINT),
            sequences: Vec::with_capacity(INIT_ROWS_HINT),
            op_types: Vec::with_capacity(INIT_ROWS_HINT),
            sorted_index: BTreeMap::new(),
            sorted_count: 0,
            unsorted_entries: Vec::with_capacity(INIT_ROWS_HINT),
            unsorted_lookup: HashMap::with_capacity(INIT_ROWS_HINT),
            rowindex_vec_pool: Vec::new(),
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

    /// Inserts a single key-value pair, allocating the sequence locally.
    ///
    /// `value` is `None` for delete tombstones.
    /// `op_type_byte`: 0 = Put, 1 = Delete, 2 = SingleDelete, 3 = Merge.
    /// Other values are rejected with `invalid_argument`.
    ///
    /// Returns the assigned sequence number.
    ///
    /// PERF (D1): when the engine drives writes it should use
    /// [`Self::put_with_seq`] so the engine-level seq counter (a single
    /// `fetch_add`) is reserved OUTSIDE the memtable lock. This `put` keeps
    /// the legacy "memtable owns its sequence" contract for unit tests and
    /// standalone memtable users.
    pub fn put(&mut self, key: &[u8], value: Option<&[u8]>, op_type_byte: u8) -> ForstResult<u64> {
        let seq = self.next_sequence;
        self.put_with_seq(key, value, op_type_byte, seq)
    }

    /// Inserts a single key-value pair using a pre-allocated sequence number.
    ///
    /// PERF (D1): the engine reserves `seq` via a single
    /// `AtomicU64::fetch_add` BEFORE acquiring the per-memtable write lock,
    /// so the engine-level sequence allocation is contention-free. The old
    /// path went through `put` (memtable allocates) + `bump_sequence` (CAS
    /// loop on the engine counter) — both inside the lock. The CAS loop
    /// is now removed; the engine just hands the seq down.
    ///
    /// The memtable still bumps `next_sequence` to `max(next_sequence,
    /// seq + 1)` so that any later legacy [`Self::put`] / [`Self::merge`]
    /// call (e.g. tests, recovery, fallback paths) keeps producing strictly
    /// increasing local sequences.
    pub fn put_with_seq(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        op_type_byte: u8,
        seq: u64,
    ) -> ForstResult<u64> {
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

        // Keep the legacy `next_sequence` monotonically ahead of any
        // externally-supplied seq so a follow-up `put`/`merge` (which still
        // self-allocate) cannot collide.
        if self.next_sequence <= seq {
            self.next_sequence = seq + 1;
        }

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
        // PERF (B2): use `get_mut` THEN `insert` instead of `entry().or_default()`
        // so the `key.to_vec()`/`Box::from` cost is paid only when the key is
        // genuinely new (the `entry()` API consumes the key unconditionally).
        // For per-key aggregations this saves an allocation per repeat.
        self.unsorted_entries.push(row_offset);
        if let Some(slot) = self.unsorted_lookup.get_mut(key) {
            slot.push(row_index);
        } else {
            let mut slot = self.rowindex_vec_pool.pop().unwrap_or_default();
            slot.push(row_index);
            self.unsorted_lookup.insert(Box::from(key), slot);
        }

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
    ///
    /// PERF (B2): drained `Vec<RowIndex>` allocations are recycled into
    /// `rowindex_vec_pool` so subsequent `put()` calls re-use them instead of
    /// allocating fresh ones. The `Box<[u8]>` keys are converted into
    /// `Vec<u8>` for the sorted index — this is a one-time per-key conversion
    /// (no per-row alloc on the hot path) and `Box<[u8]> -> Vec<u8>` is a
    /// pointer/length copy without re-allocation.
    pub fn merge_unsorted_to_sorted(&mut self) {
        // Cap pool size to avoid unbounded memory retention if a workload
        // produces a huge spike of unique keys then quiesces.
        const POOL_CAP: usize = 4096;

        for (key, mut indices) in self.unsorted_lookup.drain() {
            let owned_key: Vec<u8> = key.into_vec();
            match self.sorted_index.get_mut(&owned_key) {
                Some(entry) => {
                    entry.append(&mut indices);
                    entry.sort_by_key(|idx| Reverse(idx.sequence));
                    // `indices` is now empty — return it to the pool.
                    if self.rowindex_vec_pool.len() < POOL_CAP {
                        self.rowindex_vec_pool.push(indices);
                    }
                }
                None => {
                    // Sort by sequence descending so newest is first.
                    indices.sort_by_key(|idx| Reverse(idx.sequence));
                    self.sorted_index.insert(owned_key, indices);
                }
            }
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
            // PERF (B2): `k` is `Box<[u8]>` — deref to `&[u8]` directly.
            combined
                .entry(&**k)
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
    ///
    /// PERF (D1): when the engine drives the batch it should use
    /// [`Self::batch_insert_with_base_seq`] so the engine-level seq range is
    /// reserved by a single `fetch_add(N)` BEFORE the memtable lock is
    /// acquired. This entry point keeps the legacy contract (memtable
    /// allocates the seq range from its own counter).
    pub fn batch_insert(
        &mut self,
        keys: &[&[u8]],
        values: &[Option<&[u8]>],
        op_types: &[u8],
    ) -> ForstResult<usize> {
        let base_seq = self.next_sequence;
        self.batch_insert_with_base_seq(keys, values, op_types, base_seq)
    }

    /// Batch-inserts multiple entries using a pre-allocated sequence range.
    ///
    /// Sequence numbers `base_seq, base_seq+1, …, base_seq+keys.len()-1` are
    /// assigned in order. The engine allocates the entire range with one
    /// `AtomicU64::fetch_add(N)` outside the memtable lock so concurrent
    /// writers never serialize on the engine counter.
    pub fn batch_insert_with_base_seq(
        &mut self,
        keys: &[&[u8]],
        values: &[Option<&[u8]>],
        op_types: &[u8],
        base_seq: u64,
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
        // Keep `next_sequence` monotonically ahead of any externally-supplied
        // range so legacy callers that self-allocate cannot collide.
        let end_seq = base_seq.saturating_add(count as u64);
        if self.next_sequence < end_seq {
            self.next_sequence = end_seq;
        }
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

            // PERF (B2): same `get_mut` THEN `insert` pattern as `put()` —
            // skips the `Box::from(key)` allocation when the same key appears
            // multiple times within a single batch (a common state-update
            // pattern in streaming workloads).
            self.unsorted_entries.push(row_offset);
            if let Some(slot) = self.unsorted_lookup.get_mut(key) {
                slot.push(row_index);
            } else {
                let mut slot = self.rowindex_vec_pool.pop().unwrap_or_default();
                slot.push(row_index);
                self.unsorted_lookup.insert(Box::from(key), slot);
            }

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

    /// Direct columnar batch insert from an Arrow `RecordBatch`. Delegates
    /// to [`Self::batch_put_arrow_with_base_seq`] using the memtable's own
    /// `next_sequence` counter — appropriate for tests / standalone callers.
    /// The engine should use the `_with_base_seq` variant so the engine-level
    /// sequence range is allocated by a single `fetch_add(N)` outside the
    /// memtable lock (D1 pattern, mirrors `batch_insert`).
    pub fn batch_put_arrow(&mut self, batch: &RecordBatch) -> ForstResult<usize> {
        let base_seq = self.next_sequence;
        self.batch_put_arrow_with_base_seq(batch, base_seq)
    }

    /// Direct columnar batch insert from an Arrow `RecordBatch` using a
    /// pre-allocated sequence range.
    ///
    /// **C1 (zero-copy hot path):** the FFM bench's `batchedPutArrow` workload
    /// hands the engine a `key | value | op_type` `RecordBatch` straight from
    /// Java. Pre-C1 the FFI path decoded the batch into a per-row
    /// `WriteBatch` of owned `Vec<u8>`, then `db.batch_write` re-borrowed
    /// those into `Vec<&[u8]>` and called `batch_insert` — two round-trips of
    /// alloc + copy that defeated the zero-copy promise from
    /// `2.3_memtable_design.md` §2.4.2.
    ///
    /// This path appends the batch's three columns (`key: Binary`,
    /// `value: Binary nullable`, `op_type: UInt8`) directly into the
    /// memtable's column buffers via slice-copy:
    ///
    /// - `key_data` / `value_data` get a single `extend_from_slice` of the
    ///   underlying Arrow `value_data` buffer (concatenated key bytes / value
    ///   bytes), so the per-row payload is one memcpy total.
    /// - `key_offsets` / `value_offsets` are extended with rebased offsets
    ///   from the Arrow `value_offsets` array.
    /// - `value_nulls` mirrors the Arrow null bitmap.
    /// - `sequences` is filled with `base_seq..base_seq+count`.
    /// - `op_types` is a single `extend_from_slice` of the Arrow `UInt8Array`
    ///   value buffer.
    ///
    /// `unsorted_lookup` still pays per-row hash + insert (needed for
    /// point-lookup correctness), but the key bytes come directly from the
    /// Arrow buffer — no per-row `Vec<u8>` allocation chain.
    ///
    /// Schema validation: the batch MUST be exactly
    /// `key: Binary, value: Binary nullable, op_type: UInt8`. Other shapes
    /// return `invalid_argument`.
    ///
    /// Op-type validation matches `batch_insert`: ALL bytes are validated
    /// up-front so an invalid byte rejects the whole batch atomically (no
    /// partial column mutation).
    ///
    /// Returns the number of rows inserted (= `batch.num_rows()`).
    pub fn batch_put_arrow_with_base_seq(
        &mut self,
        batch: &RecordBatch,
        base_seq: u64,
    ) -> ForstResult<usize> {
        if self.frozen {
            return Err(forst_rs_common::ForstError::invalid_argument(
                "cannot write to a frozen MemTable",
            ));
        }

        // ---- schema validation ----
        if batch.num_columns() != 3 {
            return Err(forst_rs_common::ForstError::invalid_argument(format!(
                "batch_put_arrow: expected 3 columns (key, value, op_type); got {}",
                batch.num_columns()
            )));
        }
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                forst_rs_common::ForstError::invalid_argument(
                    "batch_put_arrow: column 0 must be BinaryArray (key)",
                )
            })?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                forst_rs_common::ForstError::invalid_argument(
                    "batch_put_arrow: column 1 must be BinaryArray (value)",
                )
            })?;
        let ops = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .ok_or_else(|| {
                forst_rs_common::ForstError::invalid_argument(
                    "batch_put_arrow: column 2 must be UInt8Array (op_type)",
                )
            })?;

        let count = batch.num_rows();
        if count == 0 {
            return Ok(0);
        }
        if keys.len() != count || values.len() != count || ops.len() != count {
            return Err(forst_rs_common::ForstError::invalid_argument(
                "batch_put_arrow: column lengths disagree with batch.num_rows()",
            ));
        }

        // ---- op_type pre-validation (atomic reject) ----
        // SECURITY: same up-front validation as `batch_insert` (Sweep R13 H).
        // Walk the op_type buffer once; reject if any byte is unknown.
        let op_values: &[u8] = ops.values();
        for (i, &b) in op_values.iter().enumerate() {
            if OpType::from_u8(b).is_none() {
                return Err(forst_rs_common::ForstError::invalid_argument(format!(
                    "batch_put_arrow: invalid op_type byte {} at index {} (expected 0=Put, 1=Delete, 2=SingleDelete, 3=Merge)",
                    b, i
                )));
            }
        }

        // Per-row null-vs-op_type cross-check is NOT done here: the FFI layer
        // (which is the only intended caller) already enforces "Delete /
        // SingleDelete must have a null value" and "Put / Merge must have a
        // non-null value" before invoking this function. Repeating the check
        // here would defeat the whole point of the columnar fast path.

        // ---- bulk column extends ----
        // Engine-allocated `base_seq` may be ahead of `next_sequence` when the
        // engine reserves the range with a single `fetch_add` outside the
        // memtable lock. Mirror `batch_insert_with_base_seq`: keep
        // `next_sequence` monotonic so legacy self-allocating callers can't
        // collide with engine-driven ranges.
        let end_seq = base_seq.saturating_add(count as u64);
        if self.next_sequence < end_seq {
            self.next_sequence = end_seq;
        }
        let base_offset = self.sequences.len() as u32;

        // 1. key_data: single memcpy of the concatenated key bytes.
        //    Arrow BinaryArray stores values back-to-back in `value_data` and
        //    indexes them via `value_offsets[i]..value_offsets[i+1]`. We
        //    rebase those offsets onto our running `key_data.len()`.
        let key_value_buf: &[u8] = keys.value_data();
        let key_offsets_arr = keys.value_offsets(); // OffsetBuffer<i32>
        let key_data_base = self.key_data.len() as u32;
        // The Arrow BinaryArray slice may not start at offset zero; the first
        // element of value_offsets is the start of the slice. Anchor on it.
        let key_slice_start = key_offsets_arr[0] as usize;
        let key_slice_end = key_offsets_arr[count] as usize;
        self.key_data
            .extend_from_slice(&key_value_buf[key_slice_start..key_slice_end]);
        self.key_offsets.reserve(count);
        // Append offsets [1..=count], rebased so that offset[i+1] - offset[i]
        // gives the same byte length as the source row.
        for i in 1..=count {
            let rebased = key_data_base + (key_offsets_arr[i] as u32 - key_offsets_arr[0] as u32);
            self.key_offsets.push(rebased);
        }

        // 2. value_data + value_offsets + value_nulls.
        let val_value_buf: &[u8] = values.value_data();
        let val_offsets_arr = values.value_offsets();
        let val_data_base = self.value_data.len() as u32;
        let val_slice_start = val_offsets_arr[0] as usize;
        let val_slice_end = val_offsets_arr[count] as usize;
        self.value_data
            .extend_from_slice(&val_value_buf[val_slice_start..val_slice_end]);
        self.value_offsets.reserve(count);
        self.value_nulls.reserve(count);
        for i in 0..count {
            // Always push the rebased end-offset — value_data has been extended
            // with the FULL concatenated buffer, including the zero-length
            // slots that null rows occupy. value_at() consults value_nulls so
            // the offset for a null row is meaningless but valid.
            let rebased =
                val_data_base + (val_offsets_arr[i + 1] as u32 - val_offsets_arr[0] as u32);
            self.value_offsets.push(rebased);
            self.value_nulls.push(values.is_null(i));
        }

        // 3. sequences: monotonically increasing from base_seq.
        self.sequences.reserve(count);
        for i in 0..count {
            self.sequences.push(base_seq + i as u64);
        }

        // 4. op_types: single memcpy of the validated UInt8 buffer.
        self.op_types.extend_from_slice(op_values);

        // 5. unsorted_lookup + unsorted_entries: per-row hash + insert.
        //    Hot path uses `get_mut` so repeated keys within the batch don't
        //    re-allocate the Box<[u8]> key.
        self.unsorted_entries.reserve(count);
        for i in 0..count {
            let row_offset = base_offset + i as u32;
            let seq = base_seq + i as u64;
            let key = keys.value(i);
            // Safety: op_types validated above the loop.
            let op_type =
                OpType::from_u8(op_values[i]).expect("op_type byte was validated above the loop");
            let row_index = RowIndex {
                offset: row_offset,
                sequence: seq,
                op_type,
            };
            self.unsorted_entries.push(row_offset);
            if let Some(slot) = self.unsorted_lookup.get_mut(key) {
                slot.push(row_index);
            } else {
                let mut slot = self.rowindex_vec_pool.pop().unwrap_or_default();
                slot.push(row_index);
                self.unsorted_lookup.insert(Box::from(key), slot);
            }

            // Approximate memory accounting matching put()/batch_insert().
            let v_len = if values.is_null(i) {
                0
            } else {
                (val_offsets_arr[i + 1] - val_offsets_arr[i]) as usize
            };
            self.memory_used += key.len() + v_len + 8 + 1 + 48;
        }

        // 6. Merge threshold (same logic as batch_insert).
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

    // === B2 PERF tests: Box<[u8]> key + RowIndex Vec pool ===

    /// 1000 unique keys via batch_insert + per-key get must round-trip.
    /// Validates the new `Box<[u8]>` HashMap key + `get_mut`-then-`insert`
    /// hot path with no collisions.
    #[test]
    fn test_b2_unsorted_lookup_box_key_unique_keys_1000() {
        let mut mt = VectorizedMemTable::new(MemTableConfig {
            max_size: 64 * 1024 * 1024,
            unsorted_merge_ratio: 1024.0, // never auto-merge during this test
        });
        let keys: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("uniq_{:06}", i).into_bytes())
            .collect();
        let vals: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("val_{:06}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let val_refs: Vec<Option<&[u8]>> = vals.iter().map(|v| Some(v.as_slice())).collect();
        let ops = vec![0u8; 1000];

        mt.batch_insert(&key_refs, &val_refs, &ops).unwrap();
        // All 1000 should be in unsorted_lookup (no merge yet).
        assert_eq!(mt.unsorted_lookup.len(), 1000);
        assert_eq!(mt.sorted_count, 0);

        for i in 0..1000u32 {
            let key = format!("uniq_{:06}", i);
            let expected = format!("val_{:06}", i);
            let r = mt.get(key.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.value, Some(expected.into_bytes()), "mismatch at {}", i);
        }
    }

    /// Repeated keys exercise the `get_mut`-hits-existing-slot path AND
    /// confirm multi-version semantics still work.
    #[test]
    fn test_b2_unsorted_lookup_repeated_keys_no_realloc() {
        let mut mt = VectorizedMemTable::new(test_config());
        // 100 distinct keys, each updated 10 times — `get_mut` path on every
        // update after the first.
        for round in 0..10u32 {
            for i in 0..100u32 {
                let k = format!("rep_{:03}", i);
                let v = format!("v{:02}_r{}", i, round);
                mt.put(k.as_bytes(), Some(v.as_bytes()), 0).unwrap();
            }
        }
        // 1000 inserts but only 100 distinct keys in the lookup.
        assert_eq!(mt.unsorted_lookup.len(), 100);
        // Each lookup slot should hold all 10 versions.
        for i in 0..100u32 {
            let k = format!("rep_{:03}", i);
            let slot = mt.unsorted_lookup.get(k.as_bytes()).unwrap();
            assert_eq!(slot.len(), 10, "key {} should have 10 versions", k);
        }
        // Latest version must come back from get().
        for i in 0..100u32 {
            let k = format!("rep_{:03}", i);
            let expected = format!("v{:02}_r9", i);
            let r = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.value, Some(expected.into_bytes()));
        }
    }

    /// Merge → re-fill cycle should recycle Vec<RowIndex> allocations into
    /// the pool. Verifies the pool grows after a merge and shrinks back as
    /// new puts consume it.
    #[test]
    fn test_b2_rowindex_vec_pool_recycle_round_trip() {
        let mut mt = VectorizedMemTable::new(test_config());
        for i in 0..50u32 {
            let k = format!("k_{:03}", i);
            mt.put(k.as_bytes(), Some(b"v"), 0).unwrap();
        }
        assert_eq!(mt.unsorted_lookup.len(), 50);
        assert_eq!(mt.rowindex_vec_pool.len(), 0);

        // Force merge; all 50 keys are first-time-seen so they go into
        // sorted_index via insert (NOT append) → no Vec recycled yet.
        mt.merge_unsorted_to_sorted();
        assert_eq!(mt.unsorted_lookup.len(), 0);
        assert_eq!(mt.sorted_count, 50);
        assert_eq!(
            mt.rowindex_vec_pool.len(),
            0,
            "first merge inserts into sorted_index, no recycled vecs"
        );

        // Re-write the SAME 50 keys + then merge again → these collide with
        // existing sorted_index entries, so the unsorted Vecs are appended &
        // recycled into the pool.
        for i in 0..50u32 {
            let k = format!("k_{:03}", i);
            mt.put(k.as_bytes(), Some(b"v2"), 0).unwrap();
        }
        mt.merge_unsorted_to_sorted();
        assert_eq!(
            mt.rowindex_vec_pool.len(),
            50,
            "second merge collides on every key — all 50 vecs recycled"
        );

        // Next put should consume from the pool.
        let pool_before = mt.rowindex_vec_pool.len();
        mt.put(b"new_key", Some(b"vv"), 0).unwrap();
        assert_eq!(
            mt.rowindex_vec_pool.len(),
            pool_before - 1,
            "put on a brand-new key should pop one Vec from the pool"
        );

        // Correctness sanity after recycling.
        let r = mt.get(b"new_key", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"vv".to_vec()));
    }

    /// Multi-version semantics survive a merge cycle with the new
    /// Box<[u8]> -> Vec<u8> key conversion.
    #[test]
    fn test_b2_merge_preserves_multi_version_after_box_to_vec() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"k", Some(b"v1"), 0).unwrap(); // seq=1
        mt.put(b"k", Some(b"v2"), 0).unwrap(); // seq=2
        mt.merge_unsorted_to_sorted();
        mt.put(b"k", Some(b"v3"), 0).unwrap(); // seq=3 (post-merge)
        mt.merge_unsorted_to_sorted();

        let entries = mt.sorted_index.get(b"k".as_slice()).unwrap();
        assert_eq!(entries.len(), 3);
        // Sorted DESC by sequence.
        assert_eq!(entries[0].sequence, 3);
        assert_eq!(entries[1].sequence, 2);
        assert_eq!(entries[2].sequence, 1);

        // Read at each sequence boundary.
        assert_eq!(
            mt.get(b"k", 1).unwrap().unwrap().value,
            Some(b"v1".to_vec())
        );
        assert_eq!(
            mt.get(b"k", 2).unwrap().unwrap().value,
            Some(b"v2".to_vec())
        );
        assert_eq!(
            mt.get(b"k", 3).unwrap().unwrap().value,
            Some(b"v3".to_vec())
        );
    }

    // === C1: batch_put_arrow zero-copy direct columnar path ===

    /// Build a (key, value, op_type) RecordBatch matching the FFI schema.
    fn make_arrow_batch(
        keys: &[&[u8]],
        values: &[Option<&[u8]>],
        op_types: &[u8],
    ) -> arrow::array::RecordBatch {
        use arrow::array::{BinaryBuilder, StructArray, UInt8Builder};
        use arrow::datatypes::Field;
        let mut kb = BinaryBuilder::new();
        let mut vb = BinaryBuilder::new();
        let mut ob = UInt8Builder::new();
        for (i, k) in keys.iter().enumerate() {
            kb.append_value(k);
            match values[i] {
                Some(v) => vb.append_value(v),
                None => vb.append_null(),
            }
            ob.append_value(op_types[i]);
        }
        let s = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Binary, false)),
                Arc::new(kb.finish()) as arrow::array::ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Binary, true)),
                Arc::new(vb.finish()) as arrow::array::ArrayRef,
            ),
            (
                Arc::new(Field::new("op_type", DataType::UInt8, false)),
                Arc::new(ob.finish()) as arrow::array::ArrayRef,
            ),
        ]);
        s.into()
    }

    /// 100-row batch lands in the column buffers with the correct
    /// concatenated key/value bytes, sequences allocated in order, and the
    /// op_type column populated. Validates the column-extend slice-copy
    /// path is well-formed.
    #[test]
    fn test_batch_put_arrow_appends_columns_directly() {
        let mut mt = VectorizedMemTable::new(test_config());
        let keys: Vec<Vec<u8>> = (0..100u32)
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();
        let values: Vec<Vec<u8>> = (0..100u32)
            .map(|i| format!("v{:04}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let val_refs: Vec<Option<&[u8]>> = values.iter().map(|v| Some(v.as_slice())).collect();
        let ops = vec![0u8; 100];
        let batch = make_arrow_batch(&key_refs, &val_refs, &ops);

        let inserted = mt.batch_put_arrow(&batch).unwrap();
        assert_eq!(inserted, 100);
        assert_eq!(mt.num_entries(), 100);

        // Sequences are 1..=100 (next_sequence starts at 1).
        for i in 0..100 {
            assert_eq!(mt.sequences[i], (i as u64) + 1);
        }
        // op_types extend mirrored the input bytes.
        assert!(mt.op_types.iter().all(|&b| b == 0));
        // Column-byte extents: each row's key_at()/value_at() must round-trip
        // to the original input bytes (via the rebased offset arrays).
        for i in 0..100u32 {
            let expected_key = format!("k{:04}", i);
            let expected_val = format!("v{:04}", i);
            assert_eq!(mt.key_at(i), expected_key.as_bytes());
            assert_eq!(mt.value_at(i), Some(expected_val.as_bytes()));
            assert!(!mt.value_nulls[i as usize]);
        }
    }

    /// All 100 keys must be retrievable via get() — verifies the
    /// unsorted_lookup HashMap is populated correctly.
    #[test]
    fn test_batch_put_arrow_unsorted_lookup_correct() {
        let mut mt = VectorizedMemTable::new(test_config());
        let keys: Vec<Vec<u8>> = (0..100u32)
            .map(|i| format!("k{:04}", i).into_bytes())
            .collect();
        let values: Vec<Vec<u8>> = (0..100u32)
            .map(|i| format!("v{:04}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let val_refs: Vec<Option<&[u8]>> = values.iter().map(|v| Some(v.as_slice())).collect();
        let ops = vec![0u8; 100];
        let batch = make_arrow_batch(&key_refs, &val_refs, &ops);

        mt.batch_put_arrow(&batch).unwrap();

        for i in 0..100u32 {
            let k = format!("k{:04}", i);
            let expected = format!("v{:04}", i);
            let r = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.value, Some(expected.into_bytes()));
            assert_eq!(r.sequence, (i as u64) + 1);
            assert_eq!(r.op_type, OpType::Put);
        }
    }

    /// Mixed Put/Delete in a single batch — Delete carries a null value and
    /// must be reflected by `value_nulls[i] == true` and a tombstone read.
    #[test]
    fn test_batch_put_arrow_mixed_put_delete() {
        let mut mt = VectorizedMemTable::new(test_config());
        let keys: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let values: Vec<Option<&[u8]>> = vec![Some(b"1"), None, Some(b"3")];
        let ops: Vec<u8> = vec![0, 1, 0]; // Put, Delete, Put
        let batch = make_arrow_batch(&keys, &values, &ops);

        mt.batch_put_arrow(&batch).unwrap();
        assert_eq!(mt.num_entries(), 3);

        let r = mt.get(b"a", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"1".to_vec()));
        let r = mt.get(b"b", u64::MAX).unwrap().unwrap();
        assert_eq!(r.op_type, OpType::Delete);
        assert_eq!(r.value, None);
        let r = mt.get(b"c", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"3".to_vec()));
    }

    /// Empty batch must be a no-op — no panic, no state change, returns 0.
    #[test]
    fn test_batch_put_arrow_empty_batch_noop() {
        let mut mt = VectorizedMemTable::new(test_config());
        let batch = make_arrow_batch(&[], &[], &[]);
        let inserted = mt.batch_put_arrow(&batch).unwrap();
        assert_eq!(inserted, 0);
        assert_eq!(mt.num_entries(), 0);
        assert_eq!(mt.next_sequence, 1);
    }

    /// Reject the whole batch if any op_type byte is unknown — column buffers
    /// must remain pristine (sentinel preserved).
    #[test]
    fn test_batch_put_arrow_rejects_invalid_op_atomically() {
        let mut mt = VectorizedMemTable::new(test_config());
        let keys: Vec<&[u8]> = vec![b"k1", b"k2", b"k3"];
        let values: Vec<Option<&[u8]>> = vec![Some(b"v1"), Some(b"v2"), Some(b"v3")];
        let ops: Vec<u8> = vec![0, 99, 0]; // 99 invalid
        let batch = make_arrow_batch(&keys, &values, &ops);
        let err = mt.batch_put_arrow(&batch);
        assert!(err.is_err());
        let msg = format!("{}", err.unwrap_err());
        assert!(
            msg.contains("invalid op_type byte 99 at index 1"),
            "got: {}",
            msg
        );
        // No partial state.
        assert_eq!(mt.sequences.len(), 0);
        assert_eq!(mt.key_offsets.len(), 1);
        assert_eq!(mt.value_offsets.len(), 1);
    }

    /// Frozen memtable rejects batch_put_arrow.
    #[test]
    fn test_batch_put_arrow_rejects_when_frozen() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.freeze();
        let batch = make_arrow_batch(&[b"k"], &[Some(b"v")], &[0u8]);
        assert!(mt.batch_put_arrow(&batch).is_err());
    }

    /// Equivalence: batch_put_arrow vs batch_insert produce the same
    /// observable state for the same input.
    #[test]
    fn test_batch_put_arrow_vs_batch_insert_equivalence() {
        let n = 200u32;
        let keys: Vec<Vec<u8>> = (0..n).map(|i| format!("k_{:05}", i).into_bytes()).collect();
        let values: Vec<Vec<u8>> = (0..n).map(|i| format!("v_{:05}", i).into_bytes()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let val_refs: Vec<Option<&[u8]>> = values.iter().map(|v| Some(v.as_slice())).collect();
        let ops = vec![0u8; n as usize];

        let mut mt_arrow = VectorizedMemTable::new(MemTableConfig {
            max_size: 32 * 1024 * 1024,
            unsorted_merge_ratio: 1024.0, // suppress merge to compare raw column state
        });
        let mt_batch_cfg = mt_arrow.config.clone();
        let mut mt_legacy = VectorizedMemTable::new(mt_batch_cfg);
        let batch = make_arrow_batch(&key_refs, &val_refs, &ops);
        mt_arrow.batch_put_arrow(&batch).unwrap();
        mt_legacy.batch_insert(&key_refs, &val_refs, &ops).unwrap();

        // Same row count, same sequences, same op_types, same key/value bytes.
        assert_eq!(mt_arrow.num_entries(), mt_legacy.num_entries());
        assert_eq!(mt_arrow.sequences, mt_legacy.sequences);
        assert_eq!(mt_arrow.op_types, mt_legacy.op_types);
        for i in 0..n {
            assert_eq!(mt_arrow.key_at(i), mt_legacy.key_at(i), "key {}", i);
            assert_eq!(mt_arrow.value_at(i), mt_legacy.value_at(i), "value {}", i);
        }
        // Get-equivalence for every key.
        for i in 0..n {
            let k = &keys[i as usize];
            let r_a = mt_arrow.get(k, u64::MAX).unwrap();
            let r_l = mt_legacy.get(k, u64::MAX).unwrap();
            assert_eq!(r_a, r_l, "get mismatch at {}", i);
        }
    }

    /// Mixed put / batch_insert / get / merge sequence — end-to-end
    /// regression for B2 changes.
    #[test]
    fn test_b2_mixed_workload_e2e() {
        let mut mt = VectorizedMemTable::new(MemTableConfig {
            max_size: 32 * 1024 * 1024,
            unsorted_merge_ratio: 0.5,
        });
        // Phase 1: 200 single puts.
        for i in 0..200u32 {
            let k = format!("p_{:04}", i);
            mt.put(k.as_bytes(), Some(b"px"), 0).unwrap();
        }
        // Phase 2: a batch of 500 puts overlapping with phase 1 keys.
        let keys: Vec<Vec<u8>> = (100..600u32)
            .map(|i| format!("p_{:04}", i).into_bytes())
            .collect();
        let vals: Vec<Vec<u8>> = (100..600u32)
            .map(|i| format!("bv{:04}", i).into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let val_refs: Vec<Option<&[u8]>> = vals.iter().map(|v| Some(v.as_slice())).collect();
        let ops = vec![0u8; 500];
        mt.batch_insert(&key_refs, &val_refs, &ops).unwrap();

        // Phase 3: range-collect to validate ordering & all-versions visible.
        let entries = mt.collect_range_entries(b"", None, u64::MAX);
        // Distinct keys: p_0000..p_0599 = 600.
        let distinct: std::collections::BTreeSet<_> =
            entries.iter().map(|(k, _, _, _)| k.clone()).collect();
        assert_eq!(distinct.len(), 600);

        // Phase 4: get on overlapping keys returns batch value (newer).
        for i in 100..200u32 {
            let k = format!("p_{:04}", i);
            let expected = format!("bv{:04}", i);
            let r = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.value, Some(expected.into_bytes()), "key={}", k);
        }
        // Non-overlapping keys still return original.
        for i in 0..100u32 {
            let k = format!("p_{:04}", i);
            let r = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.value, Some(b"px".to_vec()));
        }

        // Phase 5: freeze + flush.
        mt.freeze();
        let batches = mt.to_flush_batches(256).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 200 + 500 = 700 raw rows.
        assert_eq!(total_rows, 700);
    }
}
