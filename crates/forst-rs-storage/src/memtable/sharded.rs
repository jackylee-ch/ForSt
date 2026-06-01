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
use std::sync::{Arc, RwLock};

use arrow::array::{Array, BinaryArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use forst_rs_common::{ForstError, ForstResult, OpType};

use super::vectorized::VectorizedMemTable;
use super::{GetBorrowedResult, GetResult, MemTableConfig, ScanRow, SinkGetOutcome, ValueSink};

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

        // R57-H1: ALL-OR-NOTHING multi-shard write.
        //
        // The pre-fix shape acquired and released each shard's write lock
        // one at a time. A concurrent `freeze()` (which walks shards in
        // ascending order with a take-freeze-release cycle per shard)
        // could interleave between our shard[i] release and our
        // shard[i+1] acquire — freezing shard[i+1] before we got there.
        // The result was a torn batch: shards 0..i committed, shard i+1
        // returned `FrozenMemTable`. The engine-level retry that R56-H1
        // added would then re-execute the WHOLE batch against the new
        // active memtable, duplicating shards 0..i at identical seqs in
        // both M_old and M_new.
        //
        // Fix: collect the non-empty shard indices, then acquire every
        // non-empty shard's write lock UP-FRONT in ascending shard-index
        // order (same order `freeze()` walks, so the two operations
        // either fully serialize or fail-fast — no deadlock). Once all
        // guards are held we check `is_frozen()` on each; if ANY shard
        // is frozen we drop all guards untouched and return
        // `FrozenMemTable`. Otherwise we perform every write while the
        // locks are still held, guaranteeing the batch is observed
        // atomically by `freeze()` (which is blocked on shard[0] for
        // the duration of our write).
        //
        // B-R12-NEW-H1: dispatch each shard's index slice directly via
        // [`VectorizedMemTable::batch_insert_with_explicit_seqs_via_indices`]
        // — no more per-shard `Vec<&[u8]>`/`Vec<Option<&[u8]>>`/`Vec<u8>`/
        // `Vec<u64>` materialization. The pre-fix `PerShardBatch` struct
        // allocated four small Vecs PER non-empty shard on the vectorized
        // FFI write hot path, defeating the B5-H1 borrowed-slice promise
        // at the shard layer. The new variant reads the same row payload
        // from the engine-level `keys`/`values`/`op_types` slices via the
        // already-computed `buckets[s]` index lists.
        //
        // Note: this serializes concurrent multi-shard batches against
        // each other on shard[0], but single-shard `put_with_seq` is
        // unaffected — that path acquires only its own shard's lock.
        // The vast majority of streaming writes are single-key and hit
        // `put_with_seq`; the cost of this fix lands on cross-shard
        // batches, which were the only paths exposed to the torn-batch
        // race anyway.
        let mut non_empty: Vec<usize> = Vec::new();
        for (shard_idx, bucket) in buckets.iter().enumerate() {
            if !bucket.is_empty() {
                non_empty.push(shard_idx);
            }
        }

        // Acquire all relevant shard locks in ascending order. Matches
        // `freeze()`'s acquisition order so the two operations cannot
        // form a circular wait.
        let mut guards: Vec<std::sync::RwLockWriteGuard<'_, VectorizedMemTable>> =
            Vec::with_capacity(non_empty.len());
        for &shard_idx in &non_empty {
            guards.push(self.shards[shard_idx].write().expect("lock poisoned"));
        }

        // All guards held. Check frozen state on every shard; if any
        // is frozen, drop all guards untouched (no partial write).
        for g in &guards {
            if g.is_frozen() {
                return Err(ForstError::FrozenMemTable);
            }
        }

        // Perform writes while every lock is still held.
        for (g, &shard_idx) in guards.iter_mut().zip(non_empty.iter()) {
            let bucket = &buckets[shard_idx];
            g.batch_insert_with_explicit_seqs_via_indices(
                keys, values, op_types, bucket, base_seq,
            )?;
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

        // R57-H2 (companion to R57-H1): same ALL-OR-NOTHING acquisition
        // pattern as `batch_insert_with_base_seq`. See that function's
        // comment for the full rationale — in short, pre-fix the per-
        // shard release-then-reacquire window let `freeze()` interleave
        // and produce a torn batch that the engine-level retry then
        // duplicated.
        //
        // B-R13-NEW-H2: zero-copy multi-shard dispatch. Pre-fix this path
        // built per-shard `Vec<&[u8]>` / `Vec<Option<&[u8]>>` / `Vec<u8>`
        // / `Vec<u64>` intermediates from the Arrow columns, then dispatched
        // into `batch_insert_with_explicit_seqs` — defeating the columnar
        // zero-copy contract C1 promised. The new
        // `batch_put_arrow_indices_with_explicit_seqs` reads directly from
        // Arrow `BinaryArray::value(i)` / `is_null(i)` / `op_values[i]`
        // at each scattered shard index. Only the per-shard `seqs` vector
        // is materialised (small: one u64 per row in that shard).
        struct PerShardArrowSlice<'a> {
            shard_idx: usize,
            indices: &'a [usize],
            seqs: Vec<u64>,
        }
        let mut prepared: Vec<PerShardArrowSlice<'_>> = Vec::new();
        for (shard_idx, indices) in buckets.iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            prepared.push(PerShardArrowSlice {
                shard_idx,
                indices: indices.as_slice(),
                seqs: indices.iter().map(|&i| base_seq + i as u64).collect(),
            });
        }

        let mut guards: Vec<std::sync::RwLockWriteGuard<'_, VectorizedMemTable>> =
            Vec::with_capacity(prepared.len());
        for p in &prepared {
            guards.push(self.shards[p.shard_idx].write().expect("lock poisoned"));
        }
        for g in &guards {
            if g.is_frozen() {
                return Err(ForstError::FrozenMemTable);
            }
        }
        for (g, p) in guards.iter_mut().zip(prepared.iter()) {
            g.batch_put_arrow_indices_with_explicit_seqs(batch, p.indices, &p.seqs)?;
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

    /// Borrowed-value point lookup over the sharded memtable.
    ///
    /// PR-B7-H2: callback-based variant of
    /// [`VectorizedMemTable::get_borrowed`] that holds the shard read
    /// lock for the duration of `f`'s execution. The closure receives
    /// `Option<&GetBorrowedResult<'_>>` — the borrow is valid only
    /// inside `f`; the lock is released when `f` returns. Callers
    /// that need to retain the value beyond the closure should
    /// materialise it via [`MemtableValueRef::into_owned`] or
    /// [`MemtableValueRef::as_bytes`] inside `f`.
    ///
    /// Lock-cost is identical to [`Self::get`] (one shard read lock);
    /// the win is the avoided per-key `Vec<u8>` allocation that
    /// `get()` performs for the inline-cache and columnar fast paths.
    #[inline]
    pub fn get_borrowed_with<R>(
        &self,
        key: &[u8],
        read_sequence: u64,
        f: impl FnOnce(ForstResult<Option<&GetBorrowedResult<'_>>>) -> R,
    ) -> R {
        let idx = self.shard_for_key(key);
        let shard = self.shards[idx].read().expect("lock poisoned");
        match shard.get_borrowed(key, read_sequence) {
            Ok(opt) => f(Ok(opt.as_ref())),
            Err(e) => f(Err(e)),
        }
    }

    /// Sink-aware point lookup. See [`VectorizedMemTable::get_into`].
    ///
    /// PR-C6-H2: holds the shard's read lock for the duration of the
    /// sink write so the inline `Box<[u8]>` cannot be reallocated mid
    /// `append_borrowed` by a concurrent writer. The lock cost is the
    /// same as the legacy `get()` — but `get_into` saves the per-key
    /// `Vec<u8>` allocation that `get()` would have made.
    #[inline]
    pub fn get_into<S: ValueSink + ?Sized>(
        &self,
        key: &[u8],
        read_sequence: u64,
        sink: &mut S,
    ) -> SinkGetOutcome {
        let idx = self.shard_for_key(key);
        let shard = self.shards[idx].read().expect("lock poisoned");
        shard.get_into(key, read_sequence, sink)
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
            // C-C5R1-NEW-1: move `k` on the LAST version; clone only on
            // preceding ones. For V versions this collapses V allocations
            // to V-1 clones + 1 zero-cost move. V=1 (no rewrite history,
            // steady-state common case) eliminates the per-row alloc.
            let n = versions.len();
            for (i, (seq, v, op)) in versions.into_iter().enumerate() {
                if i + 1 == n {
                    out.push((k, v, seq, op));
                    break;
                }
                out.push((k.clone(), v, seq, op));
            }
        }
        out
    }

    pub fn prefix_scan_keys(&self, lower: &[u8], upper: Option<&[u8]>) -> Vec<Arc<[u8]>> {
        let mut keys: Vec<Arc<[u8]>> = Vec::new();
        for shard in &self.shards {
            let guard = shard.read().expect("lock poisoned");
            keys.extend(guard.prefix_scan_keys(lower, upper));
        }
        keys.sort();
        keys.dedup();
        keys
    }

    /// C9-H1: Lazy cross-shard k-way-merge cursor for the active / immutable
    /// memtable tier.
    ///
    /// The previous [`Self::prefix_scan_keys`] path materialised the union of
    /// every shard's matching keys, then ran a **global** sort+dedup before the
    /// caller could read the first row. For a 100K-row memtable that meant
    /// ~100K `Arc<[u8]>` clones AND an `O(N log N)` global sort paid at
    /// `LazyPrefixIter` construction — defeating the "lazy first row"
    /// contract that the SST tier already honours via block-streaming.
    ///
    /// This cursor instead:
    ///   1. Snapshots each shard's matching keys via the existing
    ///      [`VectorizedMemTable::prefix_scan_keys`] (cheap: `Arc::clone` per
    ///      key on the prefix-index fast path; one alloc per key on the
    ///      sorted_index fallback). The snapshot is the only "upfront" work.
    ///   2. Sorts each per-shard snapshot once (the prefix_index fast path
    ///      returns insertion-order, so per-shard sort is required for the
    ///      heap invariant). Per-shard sort is `O((N/shards) log (N/shards))`
    ///      — for the default 16 shards this is ~5x cheaper in compares than
    ///      a global sort even ignoring the constant factor of a smaller heap.
    ///   3. Uses a `BinaryHeap<Reverse<(Arc<[u8]>, shard_idx)>>` to advance
    ///      lazily across shards. Cross-shard dedup is handled by the
    ///      `last_emitted` filter in the caller (`LazyPrefixIter::next`).
    ///
    /// The cursor takes shard `read()` guards ONLY during the snapshot
    /// step; it does not hold any guards across `peek`/`advance` calls
    /// (which would deadlock with concurrent writers on the same shard).
    /// This trades "true cursor over a live BTreeMap" — which would require
    /// self-referential ownership of an `RwLockReadGuard` plus a
    /// `BTreeMap::range` iterator — for "snapshot once, merge lazily".
    /// Holding read locks across iterator emission would also block writers
    /// for the full duration of the scan, which is unacceptable on the
    /// Q11/Q12 hot path where writers and scanners overlap.
    pub fn prefix_scan_cursor(&self, lower: &[u8], upper: Option<&[u8]>) -> MemTierCursor {
        // Per-shard sorted snapshots. We sort each shard's bucket once;
        // total compares are dominated by the per-shard sort, which is
        // ~`(N/16) log(N/16)` for the default config — about 5× cheaper
        // than the global `N log N` sort that the legacy path paid.
        let mut shard_snapshots: Vec<Vec<Arc<[u8]>>> = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            // FRS-READLOCK-SCAN (2026-06-01): use a READ lock and DO NOT merge on
            // the scan path. `prefix_scan_keys` is `&self` and already enumerates
            // BOTH the sorted_index range AND the unsorted_lookup filter, so the
            // merge is a pure perf optimization, not a correctness requirement;
            // the unsorted buffer is bounded by MAX_UNSORTED_MERGE_THRESHOLD
            // (4096), so the unsorted filter is O(4096), not O(N). Mirrors
            // `range_scan_cursor`, which already scans under a read lock.
            //
            // WHY: the prior `shard.write()` + `merge_if_dirty()` held a WRITE
            // lock across a potentially-expensive merge (up to 4096 entries into
            // a multi-million-entry BTree) on EVERY prefix scan. The old comment
            // claimed "uncontended under single-threaded access", but with the
            // MapStateCache bypassed (all reads routed to the engine) plus the
            // background flush worker, a JFR/native profile of q9 at the memtable
            // spill showed ~32 % of the Join thread in `RwLock::lock_contended`
            // here — the heavy-join cap. A read lock with no merge holds the lock
            // only for the O(log N + K) scan, eliminating the contention. The
            // merge still happens on the INSERT path when the unsorted buffer
            // exceeds the threshold, so sorted_index stays compact over time.
            let guard = shard.read().expect("lock poisoned");
            let mut keys = guard.prefix_scan_keys(lower, upper);
            drop(guard);
            // The `prefix_index` fast path returns insertion-order; the
            // `sorted_index` fallback returns sorted. Sort unconditionally
            // here so the cursor's heap invariant holds in both cases.
            keys.sort();
            shard_snapshots.push(keys);
        }
        MemTierCursor::new(shard_snapshots)
    }

    /// B-R7-NEW-H1: range-bounded variant of [`Self::prefix_scan_cursor`].
    ///
    /// Mirrors `prefix_scan_cursor` but routes through
    /// [`VectorizedMemTable::range_scan_keys`] which SKIPS the prefix-index
    /// fast path — a general `[lower, upper)` range scan cannot reuse the
    /// prefix-index shortcut because that shortcut ignores `upper` and only
    /// activates when `lower` ends with `/`. Used by `DbImpl::scan_iter` to
    /// build the lazy memtable tier source without eagerly materialising
    /// the full range result up front (matching the prefix path's "first
    /// row latency = O(num_shards)" guarantee).
    pub fn range_scan_cursor(&self, lower: &[u8], upper: Option<&[u8]>) -> MemTierCursor {
        let mut shard_snapshots: Vec<Vec<Arc<[u8]>>> = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let guard = shard.read().expect("lock poisoned");
            let mut keys = guard.range_scan_keys(lower, upper);
            // `range_scan_keys` returns sorted from the sorted_index path,
            // but may have merged unsorted entries — sort unconditionally so
            // the cursor's heap invariant holds.
            keys.sort();
            shard_snapshots.push(keys);
        }
        MemTierCursor::new(shard_snapshots)
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
        // FRS-ARROW-OFFSET-FIX (2026-06-01): single-shard fast path. The general
        // path below `concat_batches` the per-shard batches into ONE combined
        // batch before sorting — which re-merges the byte-bounded per-shard
        // batches back into a single >2 GiB batch and overflows the Arrow i32
        // offset on a large (multi-GiB) memtable. With one shard the data is
        // already globally (key ASC, seq DESC) sorted, so emit its byte-bounded
        // batches directly (passing the REAL `batch_size`, not `usize::MAX`).
        if self.shards.len() == 1 {
            let guard = self.shards[0].read().expect("lock poisoned");
            return guard.to_flush_batches(batch_size);
        }

        // B-R9-NEW-H1: per-shard zero-copy `to_flush_batches` followed by a
        // bulk Arrow concat + lexicographic sort + take. The pre-fix path
        // called `collect_range_entries` per shard which materialized every
        // row as an owned `(Vec<u8>, Option<Vec<u8>>, u64, u8)` tuple, then
        // sorted those tuples globally with `Vec<u8>` key compares, then
        // ran a SECOND memcpy per row through `BinaryBuilder.append_value`.
        // On a 100k-row memtable that was ~2N small allocations + N tuple
        // allocations + an O(N log N) sort with heap-key compares + 2N
        // memcpys on the flush hot path.
        //
        // The replacement:
        //   1. Per-shard `VectorizedMemTable::to_flush_batches(usize::MAX)`
        //      returns a single sorted `RecordBatch` per non-empty shard,
        //      built via `BinaryBuilder` with one memcpy per row directly
        //      from the columnar `key_data`/`value_data` Vecs (no owned
        //      tuple intermediate).
        //   2. `arrow::compute::concat_batches` glues per-shard batches
        //      into one combined batch via bulk Arrow buffer concat.
        //   3. `lexsort_to_indices((key ASC, seq DESC))` produces a global
        //      ordering on Arrow's typed arrays (faster cache locality
        //      than sorting `Vec<u8>` tuples).
        //   4. `take` applies the indices to materialise the sorted batch.
        //   5. `RecordBatch::slice` (zero-copy via Arrow buffer slicing)
        //      chunks the result into `batch_size` pieces.
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("sequence", DataType::UInt64, false),
            Field::new("op_type", DataType::UInt8, false),
        ]));

        let mut per_shard: Vec<RecordBatch> = Vec::new();
        for shard in &self.shards {
            let guard = shard.read().expect("lock poisoned");
            // `usize::MAX` => one RecordBatch per shard containing every
            // visible row (already sorted by (key ASC, seq DESC) within
            // the shard).
            let mut sb = guard.to_flush_batches(usize::MAX)?;
            per_shard.append(&mut sb);
        }

        if per_shard.is_empty() {
            return Ok(vec![]);
        }

        // Concat per-shard sorted batches. Arrow buffer concat is bulk
        // memcpy across Arrow internals, not per-row.
        let combined = arrow::compute::concat_batches(&schema, per_shard.iter())
            .map_err(|e| ForstError::corruption(format!("concat_batches: {}", e)))?;

        // Global lexicographic sort: (key ASC, sequence DESC). Matches the
        // VectorizedMemTable::to_flush_batches contract and SstWriterImpl's
        // input invariant.
        let key_col = combined.column(0).clone();
        let seq_col = combined.column(2).clone();
        let sort_columns = vec![
            arrow::compute::SortColumn {
                values: key_col,
                options: Some(arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            },
            arrow::compute::SortColumn {
                values: seq_col,
                options: Some(arrow::compute::SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            },
        ];
        let indices = arrow::compute::lexsort_to_indices(&sort_columns, None)
            .map_err(|e| ForstError::corruption(format!("lexsort_to_indices: {}", e)))?;

        // Apply indices via Arrow's vectorized `take` (zero-row-by-row work;
        // internally just builds new offset arrays + memcpys data).
        let sorted_arrays: Vec<std::sync::Arc<dyn Array>> = combined
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ForstError::corruption(format!("take: {}", e)))?;
        let sorted = RecordBatch::try_new(schema.clone(), sorted_arrays)
            .map_err(|e| ForstError::corruption(format!("RecordBatch::try_new: {}", e)))?;

        // Chunk into `batch_size`-sized output batches via zero-copy slicing.
        let total = sorted.num_rows();
        let mut batches = Vec::new();
        let mut row_idx = 0;
        while row_idx < total {
            let slice_len = (row_idx + batch_size).min(total) - row_idx;
            batches.push(sorted.slice(row_idx, slice_len));
            row_idx += slice_len;
        }
        Ok(batches)
    }

    /// FRS-CKPT-NOFLUSH (2026-06-01): serialise the LIVE sharded memtable to
    /// globally-sorted Arrow `RecordBatch`es WITHOUT freezing it — the
    /// cross-shard analogue of [`VectorizedMemTable::snapshot_batches`]. Each
    /// shard merges its unsorted buffer (under its write lock) and emits its
    /// rows; the per-shard batches are concat'd + globally lexsorted
    /// (key ASC, seq DESC) exactly like [`Self::to_flush_batches`]. The memtable
    /// stays live + writable. Used by the checkpoint-without-flush path so a
    /// snapshot durably captures the memtable to an Arrow-IPC artifact while it
    /// remains the resident, unfragmented read structure (avoiding the L0-SST
    /// fan-out that collapses heavy joins under ckpt-ON).
    pub fn snapshot_batches(&self, batch_size: usize) -> ForstResult<Vec<RecordBatch>> {
        self.snapshot_batches_bounded(batch_size, None)
    }

    /// Streaming variant of [`Self::snapshot_batches_bounded`]: hands each batch
    /// to `emit` and drops it before building the next. On the single-shard fast
    /// path (the production default `memtable_shards = 1`) this streams a
    /// multi-GB memtable to the caller's writer with peak memory of ONE batch,
    /// avoiding the snapshot RAM doubling that OOM'd the TaskManager at large
    /// `writebuffer.size`. The multi-shard path must concat+lexsort all shards
    /// to globally sort, so it materializes the full set then emits each batch
    /// (no streaming benefit there, but multi-shard is not the OOM case).
    pub fn snapshot_batches_bounded_for_each<F: FnMut(RecordBatch) -> ForstResult<()>>(
        &self,
        batch_size: usize,
        max_seq: Option<u64>,
        mut emit: F,
    ) -> ForstResult<()> {
        if self.shards.len() == 1 {
            let mut guard = self.shards[0].write().expect("lock poisoned");
            return guard.snapshot_batches_bounded_for_each(batch_size, max_seq, emit);
        }
        // Multi-shard: global sort requires all shards materialized; emit each.
        for b in self.snapshot_batches_bounded(batch_size, max_seq)? {
            emit(b)?;
        }
        Ok(())
    }

    /// As [`Self::snapshot_batches`] but bounds entries to `sequence <= max_seq`
    /// when `max_seq` is `Some` — the consistent checkpoint-without-flush cut.
    pub fn snapshot_batches_bounded(
        &self,
        batch_size: usize,
        max_seq: Option<u64>,
    ) -> ForstResult<Vec<RecordBatch>> {
        // FRS-ARROW-OFFSET-FIX (2026-06-01): single-shard fast path — skip the
        // concat-into-one-batch (which overflows the Arrow i32 offset on a
        // multi-GiB memtable). One shard is already globally sorted; emit its
        // byte-bounded batches directly with the REAL `batch_size`.
        if self.shards.len() == 1 {
            let mut guard = self.shards[0].write().expect("lock poisoned");
            return guard.snapshot_batches_bounded(batch_size, max_seq);
        }
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("sequence", DataType::UInt64, false),
            Field::new("op_type", DataType::UInt8, false),
        ]));

        let mut per_shard: Vec<RecordBatch> = Vec::new();
        for shard in &self.shards {
            // Write lock: snapshot_batches merges the unsorted buffer in place.
            let mut guard = shard.write().expect("lock poisoned");
            let mut sb = guard.snapshot_batches_bounded(usize::MAX, max_seq)?;
            per_shard.append(&mut sb);
        }
        if per_shard.is_empty() {
            return Ok(vec![]);
        }

        let combined = arrow::compute::concat_batches(&schema, per_shard.iter())
            .map_err(|e| ForstError::corruption(format!("concat_batches: {}", e)))?;
        let sort_columns = vec![
            arrow::compute::SortColumn {
                values: combined.column(0).clone(),
                options: Some(arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: false,
                }),
            },
            arrow::compute::SortColumn {
                values: combined.column(2).clone(),
                options: Some(arrow::compute::SortOptions {
                    descending: true,
                    nulls_first: false,
                }),
            },
        ];
        let indices = arrow::compute::lexsort_to_indices(&sort_columns, None)
            .map_err(|e| ForstError::corruption(format!("lexsort_to_indices: {}", e)))?;
        let sorted_arrays: Vec<std::sync::Arc<dyn Array>> = combined
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ForstError::corruption(format!("take: {}", e)))?;
        let sorted = RecordBatch::try_new(schema.clone(), sorted_arrays)
            .map_err(|e| ForstError::corruption(format!("RecordBatch::try_new: {}", e)))?;

        let total = sorted.num_rows();
        let mut batches = Vec::new();
        let mut row_idx = 0;
        while row_idx < total {
            let slice_len = (row_idx + batch_size).min(total) - row_idx;
            batches.push(sorted.slice(row_idx, slice_len));
            row_idx += slice_len;
        }
        Ok(batches)
    }
}

// ---------------------------------------------------------------------
// MemTierCursor (C9-H1): lazy cross-shard k-way merge
// ---------------------------------------------------------------------

/// Min-heap entry: `(Arc<[u8]>, shard_idx)`, ordered ascending by key bytes.
/// Wrapped in `std::cmp::Reverse` when pushed into the `BinaryHeap` (which
/// is a max-heap by default) so `peek()` returns the lexicographically
/// smallest pending key across all shards.
#[derive(Eq, PartialEq)]
struct HeapEntry {
    key: Arc<[u8]>,
    shard_idx: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Order by the key bytes first; tie-break on shard_idx so distinct
        // shards holding identical keys are deterministic (the caller's
        // dedup filter relies only on the key, not on shard ordering).
        self.key
            .as_ref()
            .cmp(other.key.as_ref())
            .then_with(|| self.shard_idx.cmp(&other.shard_idx))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Lazy cursor that emits `Arc<[u8]>` keys across N per-shard snapshots in
/// sorted order, advancing one shard at a time without ever building a
/// global sorted vector.
///
/// Design notes (see `ShardedMemTable::prefix_scan_cursor` for the bigger
/// picture):
///   * Each shard owns its own `Vec<Arc<[u8]>>` snapshot + monotonic `pos`.
///   * The heap holds at most ONE pending entry per non-empty shard, so the
///     resident footprint is `O(num_shards)` — independent of the total
///     number of matching keys. For the default 16 shards that is at most
///     16 `Arc<[u8]>` headers + 16 `usize` shard ids ≈ a few hundred bytes.
///   * `peek` is `O(1)`; `advance` is `O(log num_shards)` (one heap pop +
///     one heap push when the popped shard still has more keys).
///   * Cross-tier dedup (active mem vs imm mems vs SSTs) is OUT of scope
///     here — that is the caller's job (the `last_emitted` filter inside
///     `LazyPrefixIter::next`). This cursor only dedupes WITHIN its own
///     snapshots via shard-local `pos` advancement (each shard's snapshot
///     is already deduped by `VectorizedMemTable::prefix_scan_keys`).
pub struct MemTierCursor {
    shards: Vec<Vec<Arc<[u8]>>>,
    positions: Vec<usize>,
    heap: std::collections::BinaryHeap<Reverse<HeapEntry>>,
}

impl MemTierCursor {
    fn new(shard_snapshots: Vec<Vec<Arc<[u8]>>>) -> Self {
        let n = shard_snapshots.len();
        let mut heap = std::collections::BinaryHeap::with_capacity(n);
        let positions = vec![0usize; n];
        for (i, snap) in shard_snapshots.iter().enumerate() {
            if let Some(first) = snap.first() {
                heap.push(Reverse(HeapEntry {
                    key: Arc::clone(first),
                    shard_idx: i,
                }));
            }
        }
        Self {
            shards: shard_snapshots,
            positions,
            heap,
        }
    }

    /// Returns whether the cursor has any more pending keys.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// FRS-ITER-DIAG: total number of snapshotted keys across all shards
    /// (the upfront work this cursor materialised). Used only by the gated
    /// `build_lazy_prefix_key_stream` diagnostic to attribute prefix-scan
    /// cost to result size vs tier count.
    pub fn snapshot_len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    /// Peeks at the next key (the lex-smallest across all shards) without
    /// consuming it. Returns `None` when the cursor is fully drained.
    /// The returned slice is borrowed from the underlying `Arc<[u8]>` —
    /// callers MUST NOT cache it across an `advance()` call.
    pub fn peek(&self) -> Option<&[u8]> {
        self.heap.peek().map(|Reverse(e)| e.key.as_ref())
    }

    /// Returns the currently-peeked key as a cheap `Arc<[u8]>` clone
    /// (atomic refcount bump, no byte copy). Returns `None` when the
    /// cursor is drained.
    pub fn peek_arc(&self) -> Option<Arc<[u8]>> {
        self.heap.peek().map(|Reverse(e)| Arc::clone(&e.key))
    }

    /// Advances past the currently-peeked key and replenishes the heap
    /// from the same shard if it still has more keys.
    pub fn advance(&mut self) {
        if let Some(Reverse(entry)) = self.heap.pop() {
            let s = entry.shard_idx;
            self.positions[s] += 1;
            let next_pos = self.positions[s];
            if next_pos < self.shards[s].len() {
                let next_key = Arc::clone(&self.shards[s][next_pos]);
                self.heap.push(Reverse(HeapEntry {
                    key: next_key,
                    shard_idx: s,
                }));
            }
        }
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
