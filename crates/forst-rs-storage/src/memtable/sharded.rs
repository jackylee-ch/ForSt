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

//! `ShardedMemTable` — N-way sharded write path over [`VectorizedMemTable`].
//!
//! E1 (sharded memtable) — the engine's pre-E1 hot path serialised every
//! writer on a single per-CF `RwLock<VectorizedMemTable>` even after D1 moved
//! sequence allocation outside `write_mutex`. The memtable column-extend +
//! `unsorted_lookup` insert + merge-threshold check still ran under that
//! single write lock, capping concurrent throughput at the lock-acquire
//! cadence.
//!
//! `ShardedMemTable` partitions the keyspace by FNV-1a hash into N
//! independent `RwLock<VectorizedMemTable>` shards. Concurrent writers
//! hashing to different shards never block each other; the engine-level
//! `write_mutex` is no longer required for the put hot path (it is retained
//! only for the memtable-switch decision in `db.rs`).
//!
//! ## Sequence numbers
//!
//! Sequence numbers come from a single shared `AtomicU64` (the
//! engine-level counter) so global monotonicity holds across shards. For
//! `batch_*` calls the engine reserves a contiguous range with one
//! `fetch_add(N)` and passes `base_seq` down; this struct sub-allocates seqs
//! per row in `base_seq + i` order regardless of which shard the row lands
//! on. (The shard's `next_sequence` is still maintained for legacy
//! self-allocating callers, but is not used on the engine hot path.)
//!
//! ## Read / scan / flush
//!
//! Reads hash to one shard. Scans visit every shard and merge results. Flush
//! merges every shard into a single sorted output stream so the downstream
//! `SstWriterImpl` never sees a key out of order.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::RwLock;

use arrow::array::{Array, BinaryArray, BinaryBuilder, RecordBatch, UInt64Builder, UInt8Builder};
use arrow::datatypes::{DataType, Field, Schema};
use forst_rs_common::{ForstError, ForstResult, OpType};

use super::vectorized::VectorizedMemTable;
use super::{GetResult, MemTableConfig, ScanRow};

/// Default number of shards. Must be a power of two so the shard index can
/// be derived by a single AND mask. 16 keeps the per-shard `RwLock`
/// vector small enough that scanning every shard for reads/flushes is cheap,
/// while admitting up to 16-way parallelism for writes (≥ the typical
/// `Runtime.availableProcessors()` of the JMH bench host). Override with
/// [`ShardedMemTable::new`] if a workload needs different parallelism.
pub const DEFAULT_SHARD_COUNT: usize = 16;

/// Maximum allowed shard count. Each shard owns a `VectorizedMemTable` with
/// a non-trivial constant-cost initialisation, so creating thousands of
/// shards per CF would blow up RSS and starve the read merge step. 256 is a
/// sane upper bound that still allows experimenting with very wide
/// parallelism on large multi-socket hosts.
pub const MAX_SHARD_COUNT: usize = 256;

/// A sharded MemTable: N independent [`VectorizedMemTable`]s each protected
/// by its own [`RwLock`]. See module docs for the design rationale.
pub struct ShardedMemTable {
    shards: Vec<RwLock<VectorizedMemTable>>,
    /// `shards.len()` cached for hot-path indexing.
    shard_count: usize,
    /// `shard_count - 1` precomputed for the AND-mask shard lookup; only
    /// valid when `shard_count` is a power of two.
    shard_mask: usize,
    /// Whether `shard_count` is a power of two. When `true`, `shard_for_key`
    /// uses the cheap AND mask; otherwise it falls back to modulus.
    is_pow2: bool,
}

impl ShardedMemTable {
    /// Constructs a sharded memtable with `shard_count` shards, each
    /// initialised with `config`.
    ///
    /// `shard_count` is clamped to `[1, MAX_SHARD_COUNT]`. `shard_count = 0`
    /// is treated as "use [`DEFAULT_SHARD_COUNT`]".
    pub fn new(shard_count: usize, config: MemTableConfig) -> Self {
        let n = if shard_count == 0 {
            DEFAULT_SHARD_COUNT
        } else {
            shard_count.min(MAX_SHARD_COUNT)
        };
        let mut shards = Vec::with_capacity(n);
        for _ in 0..n {
            shards.push(RwLock::new(VectorizedMemTable::new(config.clone())));
        }
        let is_pow2 = n.is_power_of_two();
        let shard_mask = if is_pow2 { n - 1 } else { 0 };
        Self {
            shards,
            shard_count: n,
            shard_mask,
            is_pow2,
        }
    }

    /// Convenience constructor with the default shard count + default
    /// memtable config — matches [`VectorizedMemTable::with_defaults`].
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_SHARD_COUNT, MemTableConfig::default())
    }

    /// Returns the number of shards.
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Hashes `key` and returns the owning shard index using FNV-1a (cheap,
    /// good distribution for short keys, no extra dependency).
    #[inline]
    pub fn shard_for_key(&self, key: &[u8]) -> usize {
        // FNV-1a 64-bit. ~3 ns per typical state-key on modern CPUs.
        const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
        let mut h: u64 = FNV_OFFSET;
        for &b in key {
            h ^= b as u64;
            h = h.wrapping_mul(FNV_PRIME);
        }
        if self.is_pow2 {
            (h as usize) & self.shard_mask
        } else {
            (h as usize) % self.shard_count
        }
    }

    // ------------------------------------------------------------------
    // Write path
    // ------------------------------------------------------------------

    /// Single-key put using a pre-allocated engine-level `seq`. Hashes `key`
    /// to one shard and acquires only that shard's write lock — concurrent
    /// writers on different shards never block each other.
    pub fn put_with_seq(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
        op_type_byte: u8,
        seq: u64,
    ) -> ForstResult<u64> {
        let idx = self.shard_for_key(key);
        let mut shard = self.shards[idx].write().expect("lock poisoned");
        shard.put_with_seq(key, value, op_type_byte, seq)
    }

    /// Convenience wrapper around `put_with_seq` that lets the SHARD
    /// allocate the seq locally — matches the legacy
    /// [`VectorizedMemTable::put`] contract for tests / standalone callers.
    /// **Engine code MUST use [`Self::put_with_seq`].**
    pub fn put(&self, key: &[u8], value: Option<&[u8]>, op_type_byte: u8) -> ForstResult<u64> {
        let idx = self.shard_for_key(key);
        let mut shard = self.shards[idx].write().expect("lock poisoned");
        shard.put(key, value, op_type_byte)
    }

    /// Convenience: `put` with `op_type = Merge`. Same caveats as
    /// [`VectorizedMemTable::merge`].
    pub fn merge(&self, key: &[u8], operand: &[u8]) -> ForstResult<u64> {
        self.put(key, Some(operand), OpType::Merge as u8)
    }

    /// Batch insert with engine-allocated `base_seq`. Rows are partitioned
    /// by their key's shard, and each per-shard sub-batch is dispatched
    /// under that shard's write lock with the appropriate per-row seqs from
    /// the global range.
    ///
    /// Op-type pre-validation: ALL bytes are validated up-front so an
    /// invalid byte rejects the whole batch atomically (matches the
    /// per-shard `batch_insert_with_base_seq` contract).
    pub fn batch_insert_with_base_seq(
        &self,
        keys: &[&[u8]],
        values: &[Option<&[u8]>],
        op_types: &[u8],
        base_seq: u64,
    ) -> ForstResult<usize> {
        if keys.len() != values.len() || keys.len() != op_types.len() {
            return Err(ForstError::invalid_argument(
                "batch_insert: all arrays must have the same length",
            ));
        }
        for (i, &b) in op_types.iter().enumerate() {
            if OpType::from_u8(b).is_none() {
                return Err(ForstError::invalid_argument(format!(
                    "batch_insert: invalid op_type byte {} at index {} (expected 0=Delete, 1=Put, 2=Merge, 7=SingleDelete)",
                    b, i
                )));
            }
        }

        let count = keys.len();
        if count == 0 {
            return Ok(0);
        }

        // Partition row indices by shard. Each per-shard slot holds the row
        // indices in ASCENDING order so the seq assignment below preserves
        // the original `base_seq + i` mapping (each row keeps its own seq;
        // the seq is plumbed into the per-shard batch via the `seqs` vec).
        let n = self.shard_count;
        let mut buckets: Vec<Vec<usize>> = (0..n).map(|_| Vec::new()).collect();
        for (i, key) in keys.iter().enumerate().take(count) {
            let s = self.shard_for_key(key);
            buckets[s].push(i);
        }

        // Dispatch each non-empty shard. We acquire shard locks one at a
        // time (no global lock); since each shard's row list is independent,
        // there is no cross-shard ordering hazard.
        for (shard_idx, indices) in buckets.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let sub_keys: Vec<&[u8]> = indices.iter().map(|&i| keys[i]).collect();
            let sub_values: Vec<Option<&[u8]>> = indices.iter().map(|&i| values[i]).collect();
            let sub_ops: Vec<u8> = indices.iter().map(|&i| op_types[i]).collect();
            let sub_seqs: Vec<u64> = indices.iter().map(|&i| base_seq + i as u64).collect();

            let mut shard = self.shards[shard_idx].write().expect("lock poisoned");
            shard.batch_insert_with_explicit_seqs(&sub_keys, &sub_values, &sub_ops, &sub_seqs)?;
        }
        Ok(count)
    }

    /// Convenience: batch insert using shard-local seq allocation (for
    /// tests and standalone callers). Engine code MUST use
    /// [`Self::batch_insert_with_base_seq`] so the global engine seq
    /// counter remains the single source of truth.
    pub fn batch_insert(
        &self,
        keys: &[&[u8]],
        values: &[Option<&[u8]>],
        op_types: &[u8],
    ) -> ForstResult<usize> {
        // For test convenience, allocate from shard 0's `next_sequence` —
        // this preserves the "one batch is contiguous" invariant for
        // single-threaded test scenarios. Production callers always go
        // through `_with_base_seq`.
        let base_seq = self.shards[0]
            .read()
            .expect("lock poisoned")
            .peek_next_sequence();
        self.batch_insert_with_base_seq(keys, values, op_types, base_seq)
    }

    /// Direct columnar batch put from an Arrow `RecordBatch` using a
    /// pre-allocated `base_seq`. Each row is hashed by its `key` cell to
    /// determine its shard; per-shard sub-batches are then built (via Arrow
    /// `take`) and dispatched.
    ///
    /// The schema MUST be exactly
    /// `key: Binary, value: Binary nullable, op_type: UInt8` — same
    /// contract as [`VectorizedMemTable::batch_put_arrow_with_base_seq`].
    ///
    /// **Hot path:** when all rows hash to the same shard (e.g. workloads
    /// that pre-shard at the application layer), the whole batch is
    /// forwarded to that one shard with zero re-batching — matching the
    /// pre-E1 single-memtable cost.
    pub fn batch_put_arrow_with_base_seq(
        &self,
        batch: &RecordBatch,
        base_seq: u64,
    ) -> ForstResult<usize> {
        let count = batch.num_rows();
        if count == 0 {
            return Ok(0);
        }

        // ---- schema validation (mirrors VectorizedMemTable) ----
        if batch.num_columns() != 3 {
            return Err(ForstError::invalid_argument(format!(
                "batch_put_arrow: expected 3 columns (key, value, op_type); got {}",
                batch.num_columns()
            )));
        }
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                ForstError::invalid_argument("batch_put_arrow: column 0 must be BinaryArray (key)")
            })?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                ForstError::invalid_argument(
                    "batch_put_arrow: column 1 must be BinaryArray (value)",
                )
            })?;
        let ops = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .ok_or_else(|| {
                ForstError::invalid_argument(
                    "batch_put_arrow: column 2 must be UInt8Array (op_type)",
                )
            })?;
        if keys.len() != count || values.len() != count || ops.len() != count {
            return Err(ForstError::invalid_argument(
                "batch_put_arrow: column lengths disagree with batch.num_rows()",
            ));
        }
        // Op-type pre-validation (atomic reject across shards).
        let op_values: &[u8] = ops.values();
        for (i, &b) in op_values.iter().enumerate() {
            if OpType::from_u8(b).is_none() {
                return Err(ForstError::invalid_argument(format!(
                    "batch_put_arrow: invalid op_type byte {} at index {} (expected 0=Delete, 1=Put, 2=Merge, 7=SingleDelete)",
                    b, i
                )));
            }
        }

        // Partition row indices by shard.
        let n = self.shard_count;
        let mut buckets: Vec<Vec<usize>> = (0..n).map(|_| Vec::new()).collect();
        for i in 0..count {
            let s = self.shard_for_key(keys.value(i));
            buckets[s].push(i);
        }

        // Fast path: every row hashes to the same shard — forward the batch
        // unchanged (no re-batching).
        let mut nonempty = buckets.iter().enumerate().filter(|(_, b)| !b.is_empty());
        if let Some((only_shard, only_indices)) = nonempty.next() {
            if nonempty.next().is_none() && only_indices.len() == count {
                let mut shard = self.shards[only_shard].write().expect("lock poisoned");
                return shard.batch_put_arrow_with_base_seq(batch, base_seq);
            }
        }

        // Slow path: re-batch per shard. We materialise per-shard
        // (key, value, op, seq) Vecs and dispatch via the explicit-seqs
        // batch_insert API. Per-row seq is `base_seq + original_index` so
        // the global ordering matches the input row order.
        for (shard_idx, indices) in buckets.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            // Pre-extract slices to avoid double-borrowing the Arrow array.
            let sub_keys: Vec<&[u8]> = indices.iter().map(|&i| keys.value(i)).collect();
            let sub_values: Vec<Option<&[u8]>> = indices
                .iter()
                .map(|&i| {
                    if values.is_null(i) {
                        None
                    } else {
                        Some(values.value(i))
                    }
                })
                .collect();
            let sub_ops: Vec<u8> = indices.iter().map(|&i| op_values[i]).collect();
            let sub_seqs: Vec<u64> = indices.iter().map(|&i| base_seq + i as u64).collect();

            let mut shard = self.shards[shard_idx].write().expect("lock poisoned");
            shard.batch_insert_with_explicit_seqs(&sub_keys, &sub_values, &sub_ops, &sub_seqs)?;
        }
        Ok(count)
    }

    /// Convenience: arrow batch put using the first shard's local
    /// `next_sequence` as `base_seq`. Tests/standalone only.
    pub fn batch_put_arrow(&self, batch: &RecordBatch) -> ForstResult<usize> {
        let base_seq = self.shards[0]
            .read()
            .expect("lock poisoned")
            .peek_next_sequence();
        self.batch_put_arrow_with_base_seq(batch, base_seq)
    }

    // ------------------------------------------------------------------
    // Read path
    // ------------------------------------------------------------------

    /// Point lookup: hashes `key` to one shard and forwards.
    pub fn get(&self, key: &[u8], read_sequence: u64) -> ForstResult<Option<GetResult>> {
        let idx = self.shard_for_key(key);
        let shard = self.shards[idx].read().expect("lock poisoned");
        shard.get(key, read_sequence)
    }

    /// Zero-copy point lookup: returns a raw pointer + length to the inline
    /// value without allocating. Returns `None` if the key is not found, is
    /// a tombstone, or the value is not inlined (exceeds INLINE_THRESHOLD).
    ///
    /// # Safety
    /// The returned pointer is valid as long as no write to the same key
    /// occurs and the memtable is not dropped. In Flink's single-threaded
    /// per-slot model, both hold during a single record processing cycle.
    pub fn get_pinned_ptr(&self, key: &[u8]) -> Option<(*const u8, usize)> {
        let idx = self.shard_for_key(key);
        let shard = self.shards[idx].read().expect("lock poisoned");
        shard.get_pinned_ptr(key)
    }

    /// Range scan: visits every shard and merges results into a single
    /// `Vec<ScanRow>` sorted by (key ASC, sequence DESC). Each shard's
    /// own `collect_range_entries` already merges its sorted + unsorted
    /// zones; we only need to merge across shards.
    ///
    /// For most workloads N is small (≤ 32) so a BTreeMap-based merge is
    /// cheaper than a heap-based k-way merge.
    pub fn collect_range_entries(
        &self,
        lower: &[u8],
        upper: Option<&[u8]>,
        read_sequence: u64,
    ) -> Vec<ScanRow> {
        // Group rows by key across shards, then sort each key's versions
        // by sequence DESC (matches `VectorizedMemTable::collect_range_entries`
        // contract).
        type ScanVersion = (u64, Option<Vec<u8>>, OpType);
        let mut combined: BTreeMap<Vec<u8>, Vec<ScanVersion>> = BTreeMap::new();
        for shard in &self.shards {
            let guard = shard.read().expect("lock poisoned");
            for (k, v, seq, op) in guard.collect_range_entries(lower, upper, read_sequence) {
                combined.entry(k).or_default().push((seq, v, op));
            }
        }
        let mut out: Vec<ScanRow> = Vec::new();
        for (k, mut versions) in combined {
            versions.sort_by_key(|(seq, _, _)| Reverse(*seq));
            for (seq, v, op) in versions {
                out.push((k.clone(), v, seq, op));
            }
        }
        out
    }

    // ------------------------------------------------------------------
    // Lifecycle / flush helpers
    // ------------------------------------------------------------------

    /// Returns `true` when EVERY shard is frozen. The engine freezes shards
    /// atomically via [`Self::freeze`], so this is effectively "have we
    /// frozen this memtable yet?".
    pub fn is_frozen(&self) -> bool {
        self.shards
            .iter()
            .all(|s| s.read().expect("lock poisoned").is_frozen())
    }

    /// Freezes every shard (under each shard's write lock). Subsequent
    /// `put`/`batch_*` calls will return an error from the underlying
    /// shard. Idempotent.
    pub fn freeze(&self) {
        for shard in &self.shards {
            shard.write().expect("lock poisoned").freeze();
        }
    }

    /// Sum of `num_entries()` across all shards.
    pub fn num_entries(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.read().expect("lock poisoned").num_entries())
            .sum()
    }

    /// Sum of `memory_usage()` across all shards.
    pub fn memory_usage(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.read().expect("lock poisoned").memory_usage())
            .sum()
    }

    /// Reports whether ANY shard has tripped its `should_flush` threshold.
    /// The engine compares total `memory_usage()` against the configured
    /// write-buffer cap, but this hook lets callers check per-shard
    /// behaviour for diagnostics.
    pub fn should_flush(&self) -> bool {
        self.shards
            .iter()
            .any(|s| s.read().expect("lock poisoned").should_flush())
    }

    /// Produces sorted Arrow `RecordBatch`es spanning every shard.
    ///
    /// Per-shard `to_flush_batches` already sorts within a shard. We collect
    /// all rows, k-way merge across shards, and slice into chunks of
    /// `batch_size` rows. The output is sorted by (key ASC, sequence DESC)
    /// which is exactly what `SstWriterImpl` expects.
    ///
    /// Each shard MUST be frozen (matching the per-shard contract).
    pub fn to_flush_batches(&self, batch_size: usize) -> ForstResult<Vec<RecordBatch>> {
        if !self.is_frozen() {
            return Err(ForstError::invalid_argument(
                "MemTable must be frozen before flushing",
            ));
        }

        // Collect (key, value, seq, op) from every shard.
        type FlushRow = (Vec<u8>, Option<Vec<u8>>, u64, u8);
        let mut all_rows: Vec<FlushRow> = Vec::new();
        for shard in &self.shards {
            let guard = shard.read().expect("lock poisoned");
            // Re-use `collect_range_entries(&[], None, u64::MAX)` so we
            // don't need to add new public APIs to VectorizedMemTable.
            // Walk every visible row.
            let rows = guard.collect_range_entries(&[], None, u64::MAX);
            all_rows.reserve(rows.len());
            for (k, v, seq, op) in rows {
                all_rows.push((k, v, seq, op as u8));
            }
        }

        // Sort by (key ASC, sequence DESC). This matches both the
        // VectorizedMemTable::to_flush_batches contract and SstWriterImpl's
        // input invariant.
        all_rows.sort_by(|a, b| match a.0.cmp(&b.0) {
            std::cmp::Ordering::Equal => b.2.cmp(&a.2),
            other => other,
        });

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("sequence", DataType::UInt64, false),
            Field::new("op_type", DataType::UInt8, false),
        ]));

        let mut batches = Vec::new();
        let total = all_rows.len();
        let mut row_idx = 0;
        while row_idx < total {
            let chunk_end = (row_idx + batch_size).min(total);
            let mut key_builder = BinaryBuilder::new();
            let mut value_builder = BinaryBuilder::new();
            let mut seq_builder = UInt64Builder::new();
            let mut op_builder = UInt8Builder::new();

            for row in &all_rows[row_idx..chunk_end] {
                key_builder.append_value(&row.0);
                match &row.1 {
                    Some(v) => value_builder.append_value(v),
                    None => value_builder.append_null(),
                }
                seq_builder.append_value(row.2);
                op_builder.append_value(row.3);
            }
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    std::sync::Arc::new(key_builder.finish()),
                    std::sync::Arc::new(value_builder.finish()),
                    std::sync::Arc::new(seq_builder.finish()),
                    std::sync::Arc::new(op_builder.finish()),
                ],
            )
            .map_err(|e| ForstError::corruption(format!("Arrow error: {}", e)))?;
            batches.push(batch);
            row_idx = chunk_end;
        }
        Ok(batches)
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    fn cfg() -> MemTableConfig {
        MemTableConfig {
            max_size: 1024 * 1024,
            unsorted_merge_ratio: 0.25,
        }
    }

    #[test]
    fn test_default_shard_count_is_pow2() {
        let mt = ShardedMemTable::with_defaults();
        assert_eq!(mt.shard_count(), DEFAULT_SHARD_COUNT);
        assert!(DEFAULT_SHARD_COUNT.is_power_of_two());
    }

    #[test]
    fn test_zero_shard_count_falls_back_to_default() {
        let mt = ShardedMemTable::new(0, cfg());
        assert_eq!(mt.shard_count(), DEFAULT_SHARD_COUNT);
    }

    #[test]
    fn test_shard_count_clamped_to_max() {
        let mt = ShardedMemTable::new(MAX_SHARD_COUNT * 8, cfg());
        assert_eq!(mt.shard_count(), MAX_SHARD_COUNT);
    }

    #[test]
    fn test_non_pow2_shard_count_uses_modulus() {
        let mt = ShardedMemTable::new(7, cfg());
        assert_eq!(mt.shard_count(), 7);
        // Sanity: every key lands in [0, 7).
        for i in 0..200u32 {
            let k = format!("k{:05}", i);
            let s = mt.shard_for_key(k.as_bytes());
            assert!(s < 7);
        }
    }

    #[test]
    fn test_put_and_get_single_shard() {
        let mt = ShardedMemTable::new(8, cfg());
        let _ = mt.put_with_seq(b"alpha", Some(b"one"), 1, 1).unwrap();
        let r = mt.get(b"alpha", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"one".to_vec()));
        assert_eq!(r.sequence, 1);
    }

    #[test]
    fn test_put_routes_to_correct_shard_and_get_finds_it() {
        let mt = ShardedMemTable::new(16, cfg());
        // Write 50 keys; verify each is found by `get`.
        for i in 0..50u32 {
            let k = format!("k{:05}", i);
            let v = format!("v{:05}", i);
            mt.put_with_seq(k.as_bytes(), Some(v.as_bytes()), 1, i as u64 + 1)
                .unwrap();
        }
        for i in 0..50u32 {
            let k = format!("k{:05}", i);
            let v = format!("v{:05}", i);
            let r = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.value, Some(v.as_bytes().to_vec()));
        }
    }

    #[test]
    fn test_sharded_concurrent_writers_correctness() {
        // 16 threads × 1000 puts each across distinct key prefixes; verify
        // all 16 000 keys are readable afterwards. Sequence numbers come
        // from a shared atomic so global monotonicity holds.
        let mt = Arc::new(ShardedMemTable::new(16, cfg()));
        let seq = Arc::new(AtomicU64::new(0));
        let n_threads = 16usize;
        let n_per = 1000usize;
        let mut handles = Vec::new();
        for t in 0..n_threads {
            let mt = mt.clone();
            let seq = seq.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..n_per {
                    let k = format!("t{:02}-k{:05}", t, i);
                    let v = format!("t{:02}-v{:05}", t, i);
                    let s = seq.fetch_add(1, Ordering::Relaxed) + 1;
                    mt.put_with_seq(k.as_bytes(), Some(v.as_bytes()), 1, s)
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(mt.num_entries(), n_threads * n_per);
        for t in 0..n_threads {
            for i in 0..n_per {
                let k = format!("t{:02}-k{:05}", t, i);
                let v = format!("t{:02}-v{:05}", t, i);
                let r = mt
                    .get(k.as_bytes(), u64::MAX)
                    .unwrap()
                    .unwrap_or_else(|| panic!("missing key {}", k));
                assert_eq!(r.value, Some(v.as_bytes().to_vec()));
            }
        }
    }

    #[test]
    fn test_sharded_seq_uniqueness() {
        // Concurrent writers reserve seqs from a shared atomic; verify
        // every (key,value,seq) tuple is unique by collecting a flush
        // batch and walking the seq column.
        let mt = Arc::new(ShardedMemTable::new(16, cfg()));
        let seq = Arc::new(AtomicU64::new(0));
        let n_threads = 8usize;
        let n_per = 500usize;
        let mut handles = Vec::new();
        for t in 0..n_threads {
            let mt = mt.clone();
            let seq = seq.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..n_per {
                    let k = format!("t{}-k{}", t, i);
                    let s = seq.fetch_add(1, Ordering::Relaxed) + 1;
                    mt.put_with_seq(k.as_bytes(), Some(b"v"), 1, s).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let total = mt.num_entries();
        assert_eq!(total, n_threads * n_per);

        mt.freeze();
        let batches = mt.to_flush_batches(8192).unwrap();
        let mut all_seqs: Vec<u64> = Vec::with_capacity(total);
        for b in &batches {
            let seqs = b
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            for i in 0..seqs.len() {
                all_seqs.push(seqs.value(i));
            }
        }
        all_seqs.sort_unstable();
        for w in all_seqs.windows(2) {
            assert_ne!(w[0], w[1], "duplicate seq detected: {}", w[0]);
        }
    }

    #[test]
    fn test_sharded_get_returns_correct_value() {
        let mt = ShardedMemTable::new(8, cfg());
        mt.put_with_seq(b"k1", Some(b"v1"), 1, 1).unwrap(); // Put (OpType::Put = 1, RocksDB byte-compat)
        mt.put_with_seq(b"k2", Some(b"v2"), 1, 2).unwrap(); // Put (OpType::Put = 1, RocksDB byte-compat)
        mt.put_with_seq(b"k3", None, 0, 3).unwrap(); // delete tombstone (OpType::Delete = 0, RocksDB byte-compat)
        assert_eq!(
            mt.get(b"k1", u64::MAX).unwrap().unwrap().value,
            Some(b"v1".to_vec())
        );
        assert_eq!(
            mt.get(b"k2", u64::MAX).unwrap().unwrap().value,
            Some(b"v2".to_vec())
        );
        let r = mt.get(b"k3", u64::MAX).unwrap().unwrap();
        assert!(r.value.is_none());
        assert_eq!(r.op_type, OpType::Delete);
    }

    #[test]
    fn test_sharded_to_flush_batches_total_count() {
        let mt = ShardedMemTable::new(8, cfg());
        let n = 1000;
        for i in 0..n {
            let k = format!("k{:05}", i);
            mt.put_with_seq(k.as_bytes(), Some(b"v"), 1, i as u64 + 1)
                .unwrap();
        }
        mt.freeze();
        let batches = mt.to_flush_batches(8192).unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, n);
    }

    #[test]
    fn test_sharded_memory_used_sums_across_shards() {
        let mt = ShardedMemTable::new(4, cfg());
        for i in 0..100 {
            let k = format!("k{:05}", i);
            mt.put_with_seq(k.as_bytes(), Some(b"value"), 1, i as u64 + 1)
                .unwrap();
        }
        let total = mt.memory_usage();
        let manual: usize = mt
            .shards
            .iter()
            .map(|s| s.read().unwrap().memory_usage())
            .sum();
        assert_eq!(total, manual);
        assert!(total > 0);
    }

    #[test]
    fn test_batch_insert_with_base_seq_dispatches_per_shard() {
        let mt = ShardedMemTable::new(8, cfg());
        // Use distinct keys to spread across shards.
        let owned: Vec<String> = (0..50u32).map(|i| format!("k{:05}", i)).collect();
        let kr: Vec<&[u8]> = owned.iter().map(|s| s.as_bytes()).collect();
        let vals: Vec<Option<&[u8]>> = (0..50u32).map(|_| Some(b"v" as &[u8])).collect();
        let ops = vec![0u8; 50];
        mt.batch_insert_with_base_seq(&kr, &vals, &ops, 1).unwrap();
        for (i, k) in owned.iter().enumerate() {
            let r = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.sequence, 1 + i as u64);
        }
    }

    #[test]
    fn test_batch_insert_op_type_validation_atomic() {
        let mt = ShardedMemTable::new(4, cfg());
        let keys: Vec<&[u8]> = vec![b"a", b"b", b"c"];
        let vals: Vec<Option<&[u8]>> = vec![Some(b"1"), Some(b"2"), Some(b"3")];
        let ops = vec![0u8, 1, 99]; // 99 is invalid
        let result = mt.batch_insert_with_base_seq(&keys, &vals, &ops, 1);
        assert!(result.is_err());
        assert_eq!(mt.num_entries(), 0, "no rows should have been inserted");
    }

    #[test]
    fn test_freeze_blocks_subsequent_writes() {
        let mt = ShardedMemTable::new(4, cfg());
        mt.put_with_seq(b"k", Some(b"v"), 1, 1).unwrap();
        mt.freeze();
        assert!(mt.is_frozen());
        let result = mt.put_with_seq(b"k2", Some(b"v2"), 1, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_collect_range_entries_merges_across_shards() {
        let mt = ShardedMemTable::new(8, cfg());
        // Scatter 100 keys across shards, then scan; output must be sorted.
        for i in 0..100u32 {
            let k = format!("k{:05}", i);
            mt.put_with_seq(k.as_bytes(), Some(k.as_bytes()), 1, i as u64 + 1)
                .unwrap();
        }
        let rows = mt.collect_range_entries(&[], None, u64::MAX);
        assert_eq!(rows.len(), 100);
        for w in rows.windows(2) {
            assert!(w[0].0 <= w[1].0, "scan output not sorted");
        }
    }

    #[test]
    fn test_batch_put_arrow_with_base_seq_partitions_across_shards() {
        use arrow::array::{BinaryArray, UInt8Array};
        use arrow::datatypes::{DataType, Field, Schema};

        let mt = ShardedMemTable::new(8, cfg());
        let n = 100;
        let owned: Vec<String> = (0..n).map(|i| format!("k{:05}", i)).collect();
        let key_refs: Vec<&[u8]> = owned.iter().map(|s| s.as_bytes()).collect();
        let val_refs: Vec<&[u8]> = owned.iter().map(|s| s.as_bytes()).collect();
        let ops = vec![0u8; n];

        let key_arr = BinaryArray::from(key_refs.iter().map(|s| Some(*s)).collect::<Vec<_>>());
        let val_arr = BinaryArray::from(val_refs.iter().map(|s| Some(*s)).collect::<Vec<_>>());
        let op_arr = UInt8Array::from(ops);
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("op_type", DataType::UInt8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(key_arr),
                std::sync::Arc::new(val_arr),
                std::sync::Arc::new(op_arr),
            ],
        )
        .unwrap();

        let inserted = mt.batch_put_arrow_with_base_seq(&batch, 1).unwrap();
        assert_eq!(inserted, n);
        assert_eq!(mt.num_entries(), n);
        // Each row should be readable.
        for (i, k) in owned.iter().enumerate() {
            let r = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.sequence, 1 + i as u64);
        }
    }
}
