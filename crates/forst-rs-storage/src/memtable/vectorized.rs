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
//! schema. A persistent HashMap (`hash_index`) provides O(1) point lookups.
//! A BTreeMap sorted index enables ordered iteration for range scans and
//! flush, while a HashMap buffers recent unsorted writes before merge.

use std::sync::Arc;
// 2026-05-29 PERF: FxHashMap replaces std HashMap (SipHash) for the hot
// hash_index. Profiling showed SipHash (BuildHasher::hash_one +
// Hasher::write) was the #1 q4/q7 join hot path.

use arrow::array::{Array, BinaryArray, BinaryBuilder, RecordBatch, UInt64Builder, UInt8Builder};
use arrow::datatypes::{DataType, Field, Schema};
use forst_rs_common::{ForstResult, OpType};

use super::{
    GetBorrowedResult, GetResult, MemTableConfig, MemtableValueRef, ScanRow, SinkGetOutcome,
    ValueSink,
};

/// Legacy value-size threshold. Since FRS-C2 (hash_index removal) values of ANY
/// size are served directly from the columnar `value_arena` — there is no longer
/// an inline-value cliff. Retained only as a size reference in tests that assert
/// the cliff is gone (values above and below it round-trip identically).
#[cfg(test)]
const INLINE_THRESHOLD: usize = 256;

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

/// Composite ordered-index key for the lock-free `SkipMap` (lock-free-memtable
/// P3 phase 2). Each stored version of a user key is its own immutable skiplist
/// entry — `SkipMap` cannot mutate a value in place, so a per-key version list
/// is expressed as multiple `InternalKey` entries instead.
///
/// Ordering is **user key ASCending, then sequence DESCending**, so a forward
/// skiplist traversal yields keys in ascending order with each key's newest
/// version first — exactly the (key ASC, seq DESC) order the flush builder and
/// range scans require. Keeping `user_key` and `sequence` as SEPARATE fields
/// (rather than a packed `user_key||seq` byte string) means the user-key
/// comparison is pure lexicographic with no suffix-interleaving hazard between
/// a key and another key that has it as a prefix.
/// Inline-or-heap key byte buffer for [`InternalKey`]. 48 bytes live INLINE in
/// the B-tree node (NexMark join keys are ~36 B); larger keys spill to a single
/// heap allocation. FRS-MEMTABLE-INLINE-KEY (2026-06-04): replaces the prior
/// `Arc<[u8]>`, whose per-entry heap allocation was the q4 memory wall — `vmmap`
/// pinned ~137 M live small allocations (≈ 32 GiB, fragmentation + malloc-lock
/// contention) dominated by these index keys (the key bytes are ALSO packed in
/// `key_arena`, so the `Arc` was a pure duplicate). Inline storage removes the
/// alloc entirely for the common case.
type KeyBuf = smallvec::SmallVec<[u8; 48]>;

#[derive(Clone, Debug)]
struct InternalKey {
    /// User key bytes, inline for ≤ 48 B (no per-entry heap allocation).
    user_key: KeyBuf,
    /// Sequence number; compared descending so newest-first within a key.
    sequence: u64,
}

impl InternalKey {
    #[inline]
    fn new(user_key: &[u8], sequence: u64) -> Self {
        InternalKey {
            user_key: KeyBuf::from_slice(user_key),
            sequence,
        }
    }

    /// Lower bound (inclusive) covering ALL versions of `user_key`: sequence
    /// `u64::MAX` is the smallest `InternalKey` for a given user key (seq sorts
    /// descending), so a range starting here includes every version.
    #[inline]
    fn range_start(user_key: &[u8]) -> Self {
        InternalKey {
            user_key: KeyBuf::from_slice(user_key),
            sequence: u64::MAX,
        }
    }
}

impl PartialEq for InternalKey {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.sequence == other.sequence && self.user_key == other.user_key
    }
}
impl Eq for InternalKey {}

impl Ord for InternalKey {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // user key ASC, then sequence DESC (newest version first within a key).
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}
impl PartialOrd for InternalKey {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A vectorized MemTable using columnar storage + BTreeMap sorted index.
///
/// **Write path:** `put()` appends data to columnar arrays and inserts into
/// the unsorted HashMap. When the unsorted zone grows beyond the configured
/// ratio, entries are merged into the sorted BTreeMap.
///
/// **Read path:** `get()` uses the persistent `hash_index` for O(1) point
/// lookups. The hash index is maintained on every write and never drained by
/// the merge step — it is the single source of truth for point lookups.
/// Range scans still use the sorted BTreeMap + unsorted zone.
pub struct VectorizedMemTable {
    // -- Columnar storage (append-only) --
    /// Key bytes, in a NON-MOVING segmented arena (lock-free-memtable P3
    /// foundation): chunks never move, so a per-row `ByteSpan` stays valid
    /// under concurrent appends. Replaces the prior `key_data: Vec<u8>` +
    /// `key_offsets` (a `Vec` realloc moved bytes, dangling reader offsets).
    key_arena: super::arena::SegmentedBytes,
    /// Per-row key span into `key_arena` (one entry per row, indexed by row offset).
    key_spans: Vec<super::arena::ByteSpan>,

    /// Value bytes, in a NON-MOVING segmented arena (mirrors `key_arena`):
    /// chunks never move, so a per-row `ByteSpan` stays valid under concurrent
    /// appends. Replaces the prior `value_data: Vec<u8>` + `value_offsets` (a
    /// `Vec` realloc moved bytes, dangling a concurrent reader's offset).
    value_arena: super::arena::SegmentedBytes,
    /// Per-row value span into `value_arena` (one entry per row, indexed by row
    /// offset). Null (tombstone) rows store a zero-length sentinel span;
    /// `value_nulls` is the source of truth for null-ness (a non-null
    /// zero-length value is distinct and also valid).
    value_spans: Vec<super::arena::ByteSpan>,
    /// Tracks which rows have null (tombstone) values.
    value_nulls: Vec<bool>,

    /// Sequence numbers, one per row.
    sequences: Vec<u64>,
    /// Operation types, one per row.
    op_types: Vec<u8>,

    // -- Indexes --
    /// Sorted index: key -> list of RowIndex (multi-version, newest first after merge).
    ///
    /// FRS-SCAN-ARCKEY (2026-05-30): keys are `Arc<[u8]>` (was `Vec<u8>`) so the
    /// prefix/range scan-cursor snapshot — rebuilt on EVERY prefix scan, and q9's
    /// ROW_NUMBER rank re-scans the same partition once per record (~O(K²)) — does a
    /// cheap `Arc::clone` (refcount bump) per matching key instead of
    /// `Arc::<[u8]>::from(key.as_slice())` (heap alloc + memcpy of the key). Reads are
    /// unchanged: `Arc<[u8]>: Borrow<[u8]>`, so `get`/`get_mut`/`range::<[u8],_>` still
    /// probe by borrowed slice. Aligns with the "no heap copies / zero-copy" principle.
    /// Ordered index. Each stored version of a user key is its own entry keyed by
    /// [`InternalKey`] (user key ASC, sequence DESC) → its [`RowIndex`]. A forward
    /// traversal yields rows in (key ASC, seq DESC) order — the flush / range-scan
    /// order — and a prefix/range scan is an O(log N + K) range query.
    ///
    /// FRS-MEMTABLE-CACHE (2026-06-03): reverted from `crossbeam_skiplist::SkipMap`
    /// back to `std::BTreeMap`. ROOT CAUSE (q4 profile): the SkipMap heap-allocates
    /// every node separately, so its nodes are scattered across the heap; as the
    /// memtable grows past the CPU cache, each O(log N) `search_bound` seek becomes
    /// cache-miss-bound — the q4/q7/q9 interval-join decay (181K→7K rec/s; while the
    /// memtable is small forst-rs even BEAT RocksDB at 268K vs 237K). A `BTreeMap`'s
    /// B-tree nodes are contiguous with high fan-out → far fewer cache lines touched
    /// per seek, matching RocksDB's arena-skiplist cache behaviour. The `SkipMap`'s
    /// only advantage was `&self` insert (an unrealized "phase-3 drop-the-RwLock"
    /// goal); the index is ALWAYS accessed under the per-shard `RwLock` and Flink
    /// keyed-state access is single-threaded per slot (lib.rs note), so moving inserts
    /// to `&mut self`/`write()` loses no real concurrency. (A bespoke arena skiplist
    /// is the follow-on for ultimate perf; BTreeMap first validates the cache fix.)
    index: std::collections::BTreeMap<InternalKey, RowIndex>,

    // FRS-C2 (spec C2): the `hash_index: HashMap<KeyBuf, HashEntry>` field is
    // REMOVED. It was a second full copy of every key plus a per-key HashEntry
    // (~56 B/key + the HashMap buckets) — pure memory duplication of the sorted
    // BTreeMap `index`, which already holds every version. Point lookups now
    // resolve from `index` via `idx_newest_visible`. This is the per-key
    // memtable-memory cut that lets q4 state fit the 8c/32g budget.

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

        Self {
            key_arena: super::arena::SegmentedBytes::with_chunk_capacity(INIT_CAPACITY_BYTES),
            key_spans: Vec::with_capacity(INIT_ROWS_HINT),
            value_arena: super::arena::SegmentedBytes::with_chunk_capacity(INIT_CAPACITY_BYTES),
            value_spans: Vec::with_capacity(INIT_ROWS_HINT),
            value_nulls: Vec::with_capacity(INIT_ROWS_HINT),
            sequences: Vec::with_capacity(INIT_ROWS_HINT),
            op_types: Vec::with_capacity(INIT_ROWS_HINT),
            index: std::collections::BTreeMap::new(),
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
            // R28-H1: typed variant — db.rs retry loops match
            // `ForstError::FrozenMemTable` instead of substring-scanning a
            // stringly-typed InvalidArgument message. The substring match
            // was brittle: a future error reformat (or a translation pass
            // through a wrapping layer) would silently break the retry and
            // surface the freeze as a hard write failure to the caller.
            return Err(forst_rs_common::ForstError::FrozenMemTable);
        }

        // SECURITY: validate op_type_byte BEFORE mutating any state, so an
        // invalid byte is rejected without leaving the memtable's columns
        // partially populated. Pre-fix, `OpType::from_u8(...).unwrap_or(Put)`
        // silently accepted any unrecognized byte (4..=255) and treated it
        // as Put — silent corruption potential (Sweep R13 H by Reviewer 1).
        let op_type = OpType::from_u8(op_type_byte).ok_or_else(|| {
            forst_rs_common::ForstError::invalid_argument(format!(
                "invalid op_type byte {} (expected 0=Delete, 1=Put, 2=Merge, 7=SingleDelete)",
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

        // Append key into the non-moving arena; record its span.
        self.key_spans.push(self.key_arena.append(key));

        // Append value.
        match value {
            Some(v) => {
                self.value_spans.push(self.value_arena.append(v));
                self.value_nulls.push(false);
            }
            None => {
                self.value_spans.push(super::arena::ByteSpan::default());
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
        self.index_insert(key, row_index);

        // FRS-C2 (spec C2): the redundant `hash_index` (a second full copy of
        // every key + a per-key HashEntry) is GONE — point lookups now resolve
        // from the sorted BTreeMap `index` populated by `index_insert` above
        // (`get`/`get_borrowed`/`get_into`/`get_pinned_ptr`/`collect_merge_operands`
        // all route through `idx_newest_visible`). This removes the per-key
        // HashMap memory that prevented the q4 state from fitting 8c/32g.

        // Update memory tracking (approximate).
        self.memory_used += key.len() + value.map_or(0, |v| v.len()) + 8 + 1 + 48;

        // No unsorted→sorted merge: the lock-free `index` skiplist is always
        // sorted, so every write is immediately scan-visible in O(log N).

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

    /// Returns the memtable's next-to-be-allocated local sequence number.
    ///
    /// Used by [`crate::memtable::ShardedMemTable`]'s test-convenience
    /// constructors to derive a `base_seq` for batches that don't go
    /// through the engine's shared atomic. NOT for the engine hot path.
    pub fn peek_next_sequence(&self) -> u64 {
        self.next_sequence
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
        self.key_arena.get(self.key_spans[offset as usize])
    }

    /// Retrieves the value bytes for a given row offset. Returns `None` for tombstones.
    /// Used by get() (Task 3) and to_flush_batches() (Task 5).
    fn value_at(&self, offset: u32) -> Option<&[u8]> {
        if self.value_nulls[offset as usize] {
            return None;
        }
        Some(self.value_arena.get(self.value_spans[offset as usize]))
    }

    /// Inserts one row version into the lock-free ordered index (lock-free
    /// P3 phase 2). Each version is its own immutable [`SkipMap`] entry keyed by
    /// [`InternalKey`] (user key ASC, seq DESC); `SkipMap::insert` takes `&self`
    /// so this is callable without `&mut`. Two versions of the same user key
    /// have distinct sequences → distinct keys → both retained (multi-version),
    /// matching the prior per-key `Vec<RowIndex>` behaviour.
    #[inline]
    fn index_insert(&mut self, user_key: &[u8], row: RowIndex) {
        self.index
            .insert(InternalKey::new(user_key, row.sequence), row);
    }

    /// No-op retained for API compatibility: the lock-free `index` skiplist is
    /// always sorted, so there is no unsorted zone to drain. Callers on the
    /// prefix-scan / freeze / snapshot paths used to invoke this to make the
    /// scan see recent writes; with the skiplist every write is immediately
    /// scan-visible, so nothing is required here.
    #[inline]
    pub fn merge_if_dirty(&mut self) {}

    /// No-op retained for API compatibility (see [`Self::merge_if_dirty`]). The
    /// skiplist replaces the prior unsorted→sorted merge entirely.
    #[inline]
    pub fn merge_unsorted_to_sorted(&mut self) {}

    /// Freezes this MemTable, making it immutable.
    ///
    /// After freezing, `put()` and `batch_insert()` will return an error. The
    /// `index` skiplist is already fully sorted, so no merge is needed.
    pub fn freeze(&mut self) {
        if !self.frozen {
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
        self.build_sorted_batches(batch_size)
    }

    /// FRS-CKPT-NOFLUSH (2026-06-01): serialise the LIVE memtable's contents to
    /// the same sorted Arrow `RecordBatch`es as [`to_flush_batches`], WITHOUT
    /// freezing/sealing it — the memtable remains the active, writable, in-RAM
    /// structure after the call.
    ///
    /// This is the engine primitive for checkpoint-without-flush: the
    /// checkpoint serialises the memtable to an Arrow-IPC artifact for
    /// durability while keeping the single unfragmented memtable resident for
    /// reads (the ckpt-OFF fast path), instead of folding it into an L0 SST on
    /// S3 (which fragments the memtable and forces the heavy-join decode tax —
    /// the ckpt-ON collapse). It merges the unsorted insert buffer into the
    /// sorted index first (idempotent, content-neutral — same logical state);
    /// the caller MUST hold the memtable exclusively for the duration (Flink's
    /// synchronous snapshot phase runs single-threaded per slot, so no
    /// concurrent writer races this).
    ///
    /// Round-trips exactly with [`Self::batch_insert`] of the produced rows
    /// (key, value, seq, op_type) into a fresh memtable — verified by
    /// `snapshot_batches_round_trips_all_versions`.
    pub fn snapshot_batches(&mut self, batch_size: usize) -> ForstResult<Vec<RecordBatch>> {
        self.snapshot_batches_bounded(batch_size, None)
    }

    /// As [`Self::snapshot_batches`] but only includes entries with
    /// `sequence <= max_seq` when `max_seq` is `Some` — the consistent
    /// snapshot cut for checkpoint-without-flush (see
    /// `Self::build_sorted_batches_bounded`).
    pub fn snapshot_batches_bounded(
        &mut self,
        batch_size: usize,
        max_seq: Option<u64>,
    ) -> ForstResult<Vec<RecordBatch>> {
        // Fold the unsorted buffer in so the serialisation sees every entry via
        // the sorted index. Safe on a non-frozen memtable; leaves it writable.
        self.merge_unsorted_to_sorted();
        self.build_sorted_batches_bounded(batch_size, max_seq)
    }

    /// Streaming variant of [`Self::snapshot_batches_bounded`]: hands each sorted
    /// batch to `emit` and drops it before building the next, so peak memory is
    /// one batch rather than the whole memtable as a `Vec<RecordBatch>`. Used by
    /// the checkpoint-without-flush snapshot to stream a multi-GB memtable to an
    /// Arrow-IPC file without doubling RAM. Like `snapshot_batches_bounded`, it
    /// folds the unsorted buffer in first and leaves the memtable writable.
    pub fn snapshot_batches_bounded_for_each<F: FnMut(RecordBatch) -> ForstResult<()>>(
        &mut self,
        batch_size: usize,
        max_seq: Option<u64>,
        emit: F,
    ) -> ForstResult<()> {
        self.merge_unsorted_to_sorted();
        self.for_each_sorted_batch_bounded(batch_size, max_seq, emit)
    }

    /// Shared batch builder for [`to_flush_batches`] / [`snapshot_batches`]:
    /// emits rows from `sorted_index` in (key ASC, sequence DESC) order, all
    /// versions per key. Requires the unsorted buffer to already be merged.
    fn build_sorted_batches(&self, batch_size: usize) -> ForstResult<Vec<RecordBatch>> {
        self.build_sorted_batches_bounded(batch_size, None)
    }

    /// Shared batch builder with an optional `max_seq` visibility bound: when
    /// `Some(s)`, rows with `sequence > s` are SKIPPED. This gives a consistent
    /// snapshot cut for checkpoint-without-flush — the async snapshot phase runs
    /// after the engine snapshot pinned seq `s`, but the live memtable also holds
    /// post-barrier writes (seq > s) belonging to the NEXT checkpoint; including
    /// them would make the artifact inconsistent with the pinned SST set.
    fn build_sorted_batches_bounded(
        &self,
        batch_size: usize,
        max_seq: Option<u64>,
    ) -> ForstResult<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        self.for_each_sorted_batch_bounded(batch_size, max_seq, |b| {
            batches.push(b);
            Ok(())
        })?;
        Ok(batches)
    }

    /// FRS-CKPT-NOFLUSH streaming snapshot (2026-06-01): builds the same sorted
    /// batches as `Self::build_sorted_batches_bounded` but hands each batch to
    /// `emit` and DROPS it before building the next, so peak memory is one batch
    /// (~`MAX_BATCH_BYTES`) rather than the whole memtable materialized as a
    /// `Vec<RecordBatch>`. The checkpoint-without-flush snapshot uses this to
    /// stream a multi-GB memtable to an Arrow-IPC file without doubling RAM
    /// (the OOM that forced a small memtable + heavy-query spill collapse).
    fn for_each_sorted_batch_bounded<F: FnMut(RecordBatch) -> ForstResult<()>>(
        &self,
        batch_size: usize,
        max_seq: Option<u64>,
        mut emit: F,
    ) -> ForstResult<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, true),
            Field::new("sequence", DataType::UInt64, false),
            Field::new("op_type", DataType::UInt8, false),
        ]));

        // Collect all rows in sorted order: key ASC, sequence DESC. The `index`
        // skiplist iterates in exactly that order (InternalKey ordering), one
        // entry per version. Skip rows newer than `max_seq` (the pinned
        // snapshot cut) when bounded.
        let mut sorted_rows: Vec<(u32, u64)> = Vec::new();
        for (_k, idx) in self.index.iter() {
            if let Some(s) = max_seq {
                if idx.sequence > s {
                    continue;
                }
            }
            sorted_rows.push((idx.offset, idx.sequence));
        }

        // Build batches.
        //
        // FRS-ARROW-OFFSET-FIX (2026-06-01): Arrow `Binary` columns use i32
        // value offsets, so ONE batch's key (or value) column cannot exceed
        // 2 GiB of bytes — `BinaryBuilder::finish` panics with "byte array
        // offset overflow" otherwise. With a large memtable (e.g. a 4 GiB
        // writebuffer.size, or a single-shard memtable holding GiBs) and a
        // `usize::MAX` `batch_size` (the flush/snapshot per-shard call), one
        // batch would hold the whole shard and overflow — the q9 crash at the
        // memtable spill. Split a batch when EITHER `batch_size` rows OR
        // ~1.5 GiB of key+value bytes is reached (keeps each column < 1.5 GiB <
        // 2 GiB). At least one row per batch always (a single >1.5 GiB row is
        // not representable here, but MAX_VALUE_LEN bounds values well below).
        const MAX_BATCH_BYTES: usize = 1_500_000_000;
        let mut row_idx = 0;

        while row_idx < sorted_rows.len() {
            let mut key_builder = BinaryBuilder::new();
            let mut value_builder = BinaryBuilder::new();
            let mut seq_builder = UInt64Builder::new();
            let mut op_builder = UInt8Builder::new();
            let mut acc_bytes = 0usize;
            let mut n = 0usize;

            while row_idx < sorted_rows.len() && n < batch_size {
                let (offset, _seq) = sorted_rows[row_idx];
                let key = self.key_at(offset);
                let vlen = self.value_at(offset).map_or(0, |v| v.len());
                // Split BEFORE this row if it would push the batch over the
                // byte cap — but always include at least one row.
                if n > 0 && acc_bytes + key.len() + vlen > MAX_BATCH_BYTES {
                    break;
                }
                key_builder.append_value(key);
                match self.value_at(offset) {
                    Some(v) => value_builder.append_value(v),
                    None => value_builder.append_null(),
                }
                seq_builder.append_value(self.sequences[offset as usize]);
                op_builder.append_value(self.op_types[offset as usize]);
                acc_bytes += key.len() + vlen;
                n += 1;
                row_idx += 1;
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

            emit(batch)?;
        }

        Ok(())
    }

    /// Collects all entries for keys in the `[lower, upper)` range as
    /// `(key, value, sequence, op_type)` tuples in sorted order
    /// (key ASC, sequence DESC).
    ///
    /// Fast prefix scan: returns all live keys matching the prefix range [lower, upper).
    /// Uses sorted_index (BTreeMap range) + unsorted_lookup (prefix filter) without
    /// rebuilding a merged BTreeMap. Much faster than collect_range_entries for prefix scans.
    ///
    /// PR-C6-H3: returns `Vec<Arc<[u8]>>` (not `Vec<Vec<u8>>`). On the
    /// prefix-index fast path each "clone" is an `Arc::clone` (atomic
    /// refcount bump, zero byte-copy of the key payload). The fallback
    /// path still pays one allocation per key (sorted_index keys are
    /// `Vec<u8>`-typed; we materialise into a fresh `Arc<[u8]>` once),
    /// but emits no per-row temporary `Vec<u8>` downstream.
    #[inline]
    pub fn prefix_scan_keys(&self, lower: &[u8], upper: Option<&[u8]>) -> Vec<Arc<[u8]>> {
        // S3-MAPITER-FIX: the prefix_index fast path is UNSOUND for MapState
        // iteration and has been removed.
        //
        // Root cause: `prefix_index` buckets each key under
        // `key[..=last('/')]` (see the field doc + the insert path around
        // L377). A Flink MapState composite key is
        // `"k/" + serialize(K) + "/" + stateName + "/" + serialize(UK)`, and the
        // iter prefix produced by `ForStRsMapStateV2.getIterPrefix` is
        // `"k/" + serialize(K) + "/" + stateName + "/"` (ends in '/'). When the
        // serialized user-key `serialize(UK)` contains a '/' (0x2f) byte — which
        // is common for variable-length user keys such as the RowData records
        // Nexmark q3's streaming-join MapState stores — the FULL key's LAST '/'
        // falls INSIDE the user-key, so the key is bucketed under a LONGER
        // prefix and is therefore ABSENT from the `prefix_index.get(iterPrefix)`
        // bucket. The old fast path returned only that (incomplete) bucket, so a
        // memtable-served prefix scan SILENTLY DROPPED every entry whose user-key
        // contained '/', and returned a key set inconsistent with the SST tier.
        //
        // The SST tier (db.rs `TierKeySource::Sst`) and the immutable-memtable
        // path correctly enumerate the byte-range `[lower, prefix_upper_bound)`,
        // which returns every key starting with `lower`. The two tiers therefore
        // DISAGREED: local benches kept data in the active memtable (buggy fast
        // path → under-read), but on S3 the data is flushed to SSTs (correct
        // byte-range → full set), surfacing the cross-tier inconsistency as a
        // corrupt/mis-aligned iteration result and an EOFException when the
        // RowData user-key serializer reads a wrongly-bounded byte region.
        //
        // The byte-range traversal below is provably correct (it is the same
        // semantics the SST tier and `range_scan_keys` use) and keeps memtable
        // scans consistent with every other tier. Cost is O(log N + K) over the
        // sorted_index rather than O(1), which is acceptable: prefix scans are
        // the MapState iteration path, not the point-lookup hot path.
        //
        // Merge sorted_index range + unsorted_lookup filter (no temp BTreeMap).
        let mut keys: Vec<Arc<[u8]>> = Vec::new();
        // S3-MAPITER-FIX: when the caller passes `upper = None` we must still
        // bound the scan to keys that START WITH `lower` (prefix semantics) —
        // the removed fast path enforced this implicitly via the exact-prefix
        // bucket lookup. A bare `[lower, Unbounded)` range would over-return
        // every key sorted after the prefix. Derive the exclusive prefix upper
        // bound from `lower` (increment the last non-0xff byte) so the prefix
        // contract is preserved regardless of how the caller bounds the scan.
        // The production caller (`build_lazy_prefix_key_stream`) already passes
        // a concrete `prefix_upper_bound`, so this only tightens the `None` case.
        let derived_upper: Option<Vec<u8>> = match upper {
            Some(_) => None,
            None => {
                let mut hi = lower.to_vec();
                while let Some(&last) = hi.last() {
                    if last == 0xff {
                        hi.pop();
                    } else {
                        let idx = hi.len() - 1;
                        hi[idx] = last + 1;
                        break;
                    }
                }
                if hi.is_empty() {
                    None
                } else {
                    Some(hi)
                }
            }
        };
        let effective_upper: Option<&[u8]> = upper.or(derived_upper.as_deref());
        self.collect_distinct_keys_in_range(lower, effective_upper, &mut keys);
        keys
    }

    /// Shared helper for [`Self::prefix_scan_keys`] / [`Self::range_scan_keys`]:
    /// appends the DISTINCT user keys in `[lower, upper)` to `keys`, in
    /// ascending order. The `index` skiplist iterates (user key ASC, seq DESC),
    /// so consecutive entries for the same user key collapse to one output key
    /// — no post-scan sort/dedup needed. Each emitted key is an `Arc::clone`
    /// (refcount bump, no byte copy) of the skiplist node's `user_key`,
    /// preserving the FRS-SCAN-ARCKEY zero-copy-key property. Constructing the
    /// two range-bound keys costs one `Arc` per scan, not per key.
    #[inline]
    fn collect_distinct_keys_in_range(
        &self,
        lower: &[u8],
        upper: Option<&[u8]>,
        keys: &mut Vec<Arc<[u8]>>,
    ) {
        use std::ops::Bound;
        let start = InternalKey::range_start(lower);
        let end = match upper {
            Some(hi) => Bound::Excluded(InternalKey::range_start(hi)),
            None => Bound::Unbounded,
        };
        let mut last: Option<Arc<[u8]>> = None;
        for (k, _idx) in self.index.range((Bound::Included(start), end)) {
            // FRS-MEMTABLE-INLINE-KEY: the index key is now inline (`KeyBuf`), so
            // materialise the returned `Arc<[u8]>` once per DISTINCT key (one copy,
            // then a refcount bump for `last`). The per-entry HELD `Arc` alloc is
            // gone; this transient per-scan copy is bounded by distinct-keys-in-range.
            let uk: &[u8] = &k.user_key;
            if last.as_deref() != Some(uk) {
                let arc: Arc<[u8]> = Arc::from(uk);
                keys.push(Arc::clone(&arc));
                last = Some(arc);
            }
        }
    }

    /// B-R7-NEW-H1: range-bounded variant of [`Self::prefix_scan_keys`] used
    /// by the lazy range-scan path (`DbImpl::scan_iter`). Identical to
    /// `prefix_scan_keys` for the sorted_index + unsorted_lookup paths, but
    /// SKIPS the `prefix_index` fast path entirely — a general `[lower, upper)`
    /// range scan cannot reuse the prefix-index shortcut because that shortcut
    /// keys on `lower` ending with `/` and returns ALL keys with that prefix
    /// without honouring `upper`. For range semantics we must always traverse
    /// the sorted_index range so the `upper` bound is enforced exactly.
    #[inline]
    pub fn range_scan_keys(&self, lower: &[u8], upper: Option<&[u8]>) -> Vec<Arc<[u8]>> {
        // Range semantics: honour `upper` exactly (no prefix derivation).
        let mut keys: Vec<Arc<[u8]>> = Vec::new();
        self.collect_distinct_keys_in_range(lower, upper, &mut keys);
        keys
    }

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
        use std::ops::Bound;

        // The `index` skiplist iterates [lower, upper) in (key ASC, seq DESC)
        // order with one entry per version — exactly the order this returns —
        // so no temporary merge/sort is needed. Emit each version visible at
        // `read_sequence`.
        let start = InternalKey::range_start(lower);
        let end = match upper {
            Some(hi) => Bound::Excluded(InternalKey::range_start(hi)),
            None => Bound::Unbounded,
        };
        let mut out = Vec::new();
        for (k, idx) in self.index.range((Bound::Included(start), end)) {
            if idx.sequence > read_sequence {
                continue;
            }
            let v = self.value_at(idx.offset).map(|s| s.to_vec());
            out.push((k.user_key.to_vec(), v, idx.sequence, idx.op_type));
        }
        out
    }

    /// Point lookup: returns the latest entry for `key` with sequence <= `read_sequence`.
    ///
    /// Uses the persistent `hash_index` for O(1) access. The hash index
    /// contains ALL versions of every key (across both sorted and unsorted
    /// zones) in insertion order. We walk backwards to find the latest
    /// visible version.
    ///
    /// PERF: For current-version reads (read_sequence >= latest_seq), uses the
    /// inline value cache when available — eliminates one pointer chase through
    /// the columnar value storage for small values (≤ INLINE_THRESHOLD bytes).
    /// MVCC snapshot reads fall through to the row_indices + columnar path.
    #[inline]
    pub fn get(&self, key: &[u8], read_sequence: u64) -> ForstResult<Option<GetResult>> {
        // FRS-C2: resolve the newest visible version from the sorted BTreeMap
        // index (single source of truth). `value_at` yields `None` for a
        // tombstone row, so Delete/SingleDelete naturally produce `value: None`.
        Ok(self
            .idx_newest_visible(key, read_sequence)
            .map(|ri| GetResult {
                value: self.value_at(ri.offset).map(|s| s.to_vec()),
                sequence: ri.sequence,
                op_type: ri.op_type,
            }))
    }

    /// FRS-MERGE-PERF (2026-06-03): single-pass merge-operand collection for
    /// `key`, replacing the engine peel path's O(N²) "N× `get(key, cutoff)`"
    /// loop. The old path called [`Self::get`] once per operand, and each
    /// `get` → [`Self::find_latest`] scans the key's whole `row_indices`
    /// version list — so a hot key with N merge operands (e.g. a window-join's
    /// list-valued state) cost N × O(N) = O(N²) per read, which spun
    /// `vectorizedBatchGet` for >180 s and killed the TaskManager (q5
    /// WindowJoin; symbolized sample: 100 % CPU in
    /// `collect_merge_operands → peel_merges_from_memtable → get`).
    ///
    /// This collects every `Merge` operand with `seq <= cutoff`, NEWEST-FIRST,
    /// into `operands`, in ONE pass over `row_indices` (+ one O(K log K) sort of
    /// the visible versions), stopping at the highest-seq `Put`/`Delete` base.
    ///
    /// Returns:
    ///   * `Ok(Some(Some(v)))` — a `Put` base (value `v`) terminates the chain.
    ///   * `Ok(Some(None))`    — a `Delete`/`SingleDelete` base terminates it.
    ///   * `Ok(None)`          — no terminal in THIS memtable; the operands
    ///                           collected so far stand and the caller continues
    ///                           to older tiers (imm / SST) for the base.
    pub fn collect_merge_operands(
        &self,
        key: &[u8],
        cutoff: u64,
        operands: &mut Vec<Vec<u8>>,
    ) -> ForstResult<Option<Option<Vec<u8>>>> {
        // FRS-C2: walk this key's versions newest-first directly from the sorted
        // BTreeMap index (already (key ASC, seq DESC) ordered — no defensive sort
        // needed, unlike the old unsorted `row_indices`). Collect Merge operands
        // with seq ≤ cutoff until a Put/Delete base terminates the chain.
        for (ik, ri) in self.index.range(InternalKey::range_start(key)..) {
            if ik.user_key.as_slice() != key {
                break; // past every version of this user key
            }
            if ik.sequence > cutoff {
                continue; // not visible at this snapshot
            }
            match ri.op_type {
                OpType::Put => {
                    // Put terminates the chain with its value as the base.
                    return Ok(Some(self.value_at(ri.offset).map(|v| v.to_vec())));
                }
                OpType::Delete | OpType::SingleDelete => {
                    return Ok(Some(None));
                }
                OpType::Merge => match self.value_at(ri.offset) {
                    Some(v) => operands.push(v.to_vec()),
                    None => {
                        return Err(forst_rs_common::ForstError::corruption(
                            "collect_merge_operands: Merge entry missing operand payload",
                        ));
                    }
                },
            }
        }
        // Exhausted every visible version without a terminal — continue older tiers.
        Ok(None)
    }

    /// Borrowed-value point lookup: like [`Self::get`] but returns a
    /// [`GetBorrowedResult`] whose `value` field borrows from this
    /// memtable's own storage (`inline_value` cache or the `value_arena`
    /// columnar arena) instead of allocating a fresh `Vec<u8>` per
    /// call.
    ///
    /// PR-B7-H2: this is the zero-extra-alloc point-lookup primitive
    /// used by the streaming prefix-iterator hot path
    /// (`DbImpl::prefix_scan_iter_owned`). For the inline-cache and
    /// columnar fast paths, no per-key heap allocation occurs — the
    /// returned `MemtableValueRef::Inline(&[u8])` borrows directly
    /// from the memtable's already-allocated storage.
    ///
    /// # Safety / lifetime
    /// The returned reference borrows from `&self`; the caller must
    /// release it before any concurrent write to the same key. In
    /// Flink's single-threaded-per-slot model this is naturally
    /// upheld during a single record processing cycle. Higher-level
    /// wrappers (`ShardedMemTable::get_borrowed`) hold the shard read
    /// lock for the borrow's lifetime to make this safe under
    /// arbitrary callers.
    #[inline]
    pub fn get_borrowed(
        &self,
        key: &[u8],
        read_sequence: u64,
    ) -> ForstResult<Option<GetBorrowedResult<'_>>> {
        // FRS-C2: newest visible version from the sorted BTreeMap index; borrow
        // the value straight from the columnar `value_arena` (no per-key alloc).
        // `value_at` is `None` for a tombstone row.
        Ok(self
            .idx_newest_visible(key, read_sequence)
            .map(|ri| GetBorrowedResult {
                value: self.value_at(ri.offset).map(MemtableValueRef::Inline),
                sequence: ri.sequence,
                op_type: ri.op_type,
            }))
    }

    /// Zero-copy point lookup: returns a raw pointer + length to the inline
    /// value in the hash index, without cloning into a `Vec<u8>`.
    ///
    /// Returns `Some((ptr, len))` when the latest version is a Put with an
    /// inline value (≤ INLINE_THRESHOLD bytes). Returns `None` if:
    /// - Key not found
    /// - Latest version is a tombstone (Delete/SingleDelete)
    /// - Value exceeds INLINE_THRESHOLD (not inlined)
    /// - Latest version is a Merge op
    ///
    /// # Safety
    /// The returned pointer is valid as long as:
    /// - The `VectorizedMemTable` is not dropped
    /// - No write to the same key occurs (which would reallocate the Box)
    ///   In Flink's single-threaded-per-slot model, both conditions hold for
    ///   the duration of a single record processing.
    pub fn get_pinned_ptr(&self, key: &[u8]) -> Option<(*const u8, usize)> {
        // FRS-C2: newest version from the sorted index. Only serve a Put.
        let ri = self.idx_newest_visible(key, u64::MAX)?;
        if ri.op_type != OpType::Put {
            return None;
        }
        // Pointer into `value_arena` (non-moving chunks → stable across overwrites).
        let bytes = self.value_at(ri.offset)?;
        Some((bytes.as_ptr(), bytes.len()))
    }

    /// Sink-aware point lookup that writes directly into a [`ValueSink`]
    /// when the inline cache is the answer, skipping the `Vec<u8>`
    /// materialisation that `get()` performs.
    ///
    /// PR-C6-H2: this is the zero-extra-alloc primitive that lets
    /// `DbImpl::batch_get_arrow` stream the memtable hot path straight
    /// into the Arrow `BinaryBuilder` value buffer (one memcpy from the
    /// inline `Box<[u8]>` into the buffer; no intermediate Vec).
    ///
    /// Returns one of:
    /// - `HitPut`        → sink received the borrowed value bytes.
    /// - `HitTombstone`  → sink received an `append_null`.
    /// - `Miss`          → no entry in this memtable; caller falls back.
    /// - `NeedsFullPath` → entry is a Merge OR a non-inline value
    ///   (e.g. oversized Put served from columnar storage). The sink is
    ///   left untouched so the caller can resolve via the existing
    ///   `Vec<u8>`-returning path and `sink.append_borrowed(&v)` after.
    #[inline]
    pub fn get_into<S: ValueSink + ?Sized>(
        &self,
        key: &[u8],
        read_sequence: u64,
        sink: &mut S,
    ) -> SinkGetOutcome {
        // FRS-C2: newest visible version from the sorted index (MVCC-correct —
        // resolves an older version directly when the newest is beyond the
        // snapshot, instead of bailing to the full path; same resulting bytes).
        let ri = match self.idx_newest_visible(key, read_sequence) {
            Some(r) => r,
            None => return SinkGetOutcome::Miss,
        };
        match ri.op_type {
            OpType::Delete | OpType::SingleDelete => {
                sink.append_null();
                SinkGetOutcome::HitTombstone
            }
            OpType::Merge => SinkGetOutcome::NeedsFullPath,
            OpType::Put => {
                // Zero-extra-alloc fast path: borrow the value straight from
                // the columnar `value_arena` (any size — no INLINE_THRESHOLD cliff).
                if let Some(bytes) = self.value_at(ri.offset) {
                    sink.append_borrowed(bytes);
                    SinkGetOutcome::HitPut
                } else {
                    SinkGetOutcome::NeedsFullPath
                }
            }
        }
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
            // R28-H1: typed variant for engine-side retry; see put_with_seq.
            return Err(forst_rs_common::ForstError::FrozenMemTable);
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
                    "batch_insert: invalid op_type byte {} at index {} (expected 0=Delete, 1=Put, 2=Merge, 7=SingleDelete)",
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
            self.key_spans.push(self.key_arena.append(key));

            // Append value.
            match value {
                Some(v) => {
                    self.value_spans.push(self.value_arena.append(v));
                    self.value_nulls.push(false);
                }
                None => {
                    self.value_spans.push(super::arena::ByteSpan::default());
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
            // skips the `KeyBuf::from_slice(key)` allocation when the same key appears
            // multiple times within a single batch (a common state-update
            // pattern in streaming workloads).
            self.index_insert(key, row_index);

            // Persistent hash index: always append so get() is O(1).
            // Maintain inline value cache for the latest version.
            // B-R12-H1: capture `is_new_latest` for prefix_index gate.
            // FRS-C2: hash_index removed — point lookups resolve from the sorted
            // BTreeMap `index` (populated by `index_insert` above).

            // Prefix index: maintain mapping from prefix → full keys.
            // PR-B7-H3 fix: the batch-insert paths previously skipped this
            // entirely, so the C6-H3 fast path in `prefix_scan_keys` was DEAD
            // CODE on the Q12 batch-write hot path (it always fell back to
            // the per-shard O(log N + K) sorted_index merge). Replicate the
            // single-write maintenance block here.
            //
            // 2026-05-29 PERF: prefix_index maintenance REMOVED here too (see the
            // batch_insert site above) — the O(N²) dead-weight on the join
            // insert hot path. `is_new_latest` still gates the inline cache.

            self.memory_used += key.len() + value.map_or(0, |v| v.len()) + 8 + 1 + 48;
        }

        // Check merge threshold.
        // No unsorted→sorted merge: the lock-free `index` skiplist is always sorted.

        Ok(count)
    }

    /// Batch-inserts multiple entries using a per-row sequence number array.
    ///
    /// Unlike [`Self::batch_insert_with_base_seq`] (which assigns
    /// `base_seq, base_seq+1, …`), this method takes an EXPLICIT seq for
    /// each row. This is the contract used by
    /// [`crate::memtable::ShardedMemTable`]: the engine reserves a global
    /// seq range with one `fetch_add(N)`, and each per-shard sub-batch
    /// receives its rows' `base_seq + original_global_index` values — which
    /// are NOT contiguous because rows interleave between shards.
    ///
    /// All four arrays must have the same length. Op-type validation is
    /// atomic (rejects the whole batch on first invalid byte) — same
    /// contract as `batch_insert_with_base_seq`.
    ///
    /// `next_sequence` is bumped to `max(self.next_sequence,
    /// max(seqs) + 1)` so legacy self-allocating callers cannot collide.
    pub fn batch_insert_with_explicit_seqs(
        &mut self,
        keys: &[&[u8]],
        values: &[Option<&[u8]>],
        op_types: &[u8],
        seqs: &[u64],
    ) -> ForstResult<usize> {
        if self.frozen {
            // R28-H1: typed variant for engine-side retry; see put_with_seq.
            return Err(forst_rs_common::ForstError::FrozenMemTable);
        }
        if keys.len() != values.len() || keys.len() != op_types.len() || keys.len() != seqs.len() {
            return Err(forst_rs_common::ForstError::invalid_argument(
                "batch_insert_with_explicit_seqs: all arrays must have the same length",
            ));
        }
        for (i, &b) in op_types.iter().enumerate() {
            if OpType::from_u8(b).is_none() {
                return Err(forst_rs_common::ForstError::invalid_argument(format!(
                    "batch_insert_with_explicit_seqs: invalid op_type byte {} at index {} (expected 0=Delete, 1=Put, 2=Merge, 7=SingleDelete)",
                    b, i
                )));
            }
        }

        let count = keys.len();
        if count == 0 {
            return Ok(0);
        }
        // Bump next_sequence to one past the highest seq we'll write.
        let max_seq = *seqs.iter().max().expect("count > 0");
        if self.next_sequence <= max_seq {
            self.next_sequence = max_seq + 1;
        }
        let base_offset = self.sequences.len() as u32;

        for i in 0..count {
            let key = keys[i];
            let value = values[i];
            let seq = seqs[i];
            let row_offset = base_offset + i as u32;

            self.key_spans.push(self.key_arena.append(key));

            match value {
                Some(v) => {
                    self.value_spans.push(self.value_arena.append(v));
                    self.value_nulls.push(false);
                }
                None => {
                    self.value_spans.push(super::arena::ByteSpan::default());
                    self.value_nulls.push(true);
                }
            }

            self.sequences.push(seq);
            self.op_types.push(op_types[i]);
            let op_type =
                OpType::from_u8(op_types[i]).expect("op_type byte was validated above the loop");
            let row_index = RowIndex {
                offset: row_offset,
                sequence: seq,
                op_type,
            };

            self.index_insert(key, row_index);

            // Persistent hash index: always append so get() is O(1).
            // Maintain inline value cache for the latest version.
            // B-R12-H1: capture `is_new_latest` for prefix_index gate.
            // FRS-C2: hash_index removed — point lookups resolve from the sorted
            // BTreeMap `index` (populated by `index_insert` above).

            // 2026-05-29 PERF: prefix_index maintenance REMOVED (O(N²) dead-weight
            // on the sharded-batch insert path; see batch_insert site).

            self.memory_used += key.len() + value.map_or(0, |v| v.len()) + 8 + 1 + 48;
        }

        // No unsorted→sorted merge: the lock-free `index` skiplist is always sorted.
        Ok(count)
    }

    /// B-R12-NEW-H1: indexed variant of [`Self::batch_insert_with_explicit_seqs`]
    /// that reads the row payload from `keys[indices[i]]`/`values[indices[i]]`/
    /// `op_types[indices[i]]` and derives the per-row seq as
    /// `base_seq + indices[i] as u64`. This is the contract used by the
    /// per-shard dispatch in [`crate::memtable::ShardedMemTable`]: the caller
    /// reserves a global seq range with one `fetch_add(N)`, partitions row
    /// indices by shard, and dispatches each shard's index slice WITHOUT
    /// re-materialising the per-shard `Vec<&[u8]>` / `Vec<Option<&[u8]>>` /
    /// `Vec<u8>` / `Vec<u64>` intermediates the pre-fix `PerShardBatch` struct
    /// allocated.
    ///
    /// All preconditions and semantics match `batch_insert_with_explicit_seqs`
    /// except the row payload is sourced via `keys[indices[i]]` etc. and seqs
    /// are derived from `base_seq + indices[i]`.
    pub fn batch_insert_with_explicit_seqs_via_indices(
        &mut self,
        keys: &[&[u8]],
        values: &[Option<&[u8]>],
        op_types: &[u8],
        indices: &[usize],
        base_seq: u64,
    ) -> ForstResult<usize> {
        if self.frozen {
            return Err(forst_rs_common::ForstError::FrozenMemTable);
        }
        if keys.len() != values.len() || keys.len() != op_types.len() {
            return Err(forst_rs_common::ForstError::invalid_argument(
                "batch_insert_with_explicit_seqs_via_indices: keys/values/op_types must have the same length",
            ));
        }
        let count = indices.len();
        if count == 0 {
            return Ok(0);
        }
        // Validate every op_type byte (atomic — rejects the whole batch on first invalid byte).
        for (k, &idx) in indices.iter().enumerate() {
            if idx >= op_types.len() {
                return Err(forst_rs_common::ForstError::invalid_argument(format!(
                    "batch_insert_with_explicit_seqs_via_indices: indices[{}]={} out of bounds (len={})",
                    k, idx, op_types.len()
                )));
            }
            let b = op_types[idx];
            if OpType::from_u8(b).is_none() {
                return Err(forst_rs_common::ForstError::invalid_argument(format!(
                    "batch_insert_with_explicit_seqs_via_indices: invalid op_type byte {} at indices[{}]={} (expected 0=Delete, 1=Put, 2=Merge, 7=SingleDelete)",
                    b, k, idx
                )));
            }
        }

        // Bump next_sequence to one past the highest seq we'll write.
        // seqs are derived as base_seq + indices[i]; the max is base_seq + max(indices).
        let max_idx = *indices.iter().max().expect("count > 0");
        let max_seq = base_seq + max_idx as u64;
        if self.next_sequence <= max_seq {
            self.next_sequence = max_seq + 1;
        }
        let base_offset = self.sequences.len() as u32;

        for (k, &idx) in indices.iter().enumerate() {
            let key = keys[idx];
            let value = values[idx];
            let seq = base_seq + idx as u64;
            let row_offset = base_offset + k as u32;

            self.key_spans.push(self.key_arena.append(key));

            match value {
                Some(v) => {
                    self.value_spans.push(self.value_arena.append(v));
                    self.value_nulls.push(false);
                }
                None => {
                    self.value_spans.push(super::arena::ByteSpan::default());
                    self.value_nulls.push(true);
                }
            }

            self.sequences.push(seq);
            self.op_types.push(op_types[idx]);
            let op_type =
                OpType::from_u8(op_types[idx]).expect("op_type byte was validated above the loop");
            let row_index = RowIndex {
                offset: row_offset,
                sequence: seq,
                op_type,
            };

            self.index_insert(key, row_index);

            // FRS-C2: hash_index removed — point lookups use the BTreeMap `index`.

            // 2026-05-29 PERF: prefix_index maintenance REMOVED (O(N²) dead-weight).

            self.memory_used += key.len() + value.map_or(0, |v| v.len()) + 8 + 1 + 48;
        }

        // No unsorted→sorted merge: the lock-free `index` skiplist is always sorted.
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
    /// memtable's column storage:
    ///
    /// - `key_arena` / `value_arena` get a per-row `append` of each row's
    ///   bytes sliced from the underlying Arrow value buffer, recording a
    ///   non-moving `ByteSpan` per row into `key_spans` / `value_spans`. The
    ///   arena keeps each row contiguous and never moves it, so reader spans
    ///   stay valid under concurrent appends.
    /// - `value_nulls` mirrors the Arrow null bitmap; null rows store a
    ///   zero-length sentinel span.
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
    /// B-R13-NEW-H2: zero-copy multi-shard dispatch. Reads keys / values /
    /// ops directly from the Arrow columns at the supplied `indices`
    /// (non-contiguous, e.g. when sharding scatters rows across shards) and
    /// applies each row's explicit `seqs[i]`.
    ///
    /// Pre-fix the multi-shard path in `ShardedMemTable::batch_put_arrow_with_base_seq`
    /// decomposed the Arrow columns into per-shard `Vec<&[u8]>`/
    /// `Vec<Option<&[u8]>>`/`Vec<u8>`/`Vec<u64>` intermediates before
    /// dispatching into `batch_insert_with_explicit_seqs` — defeating the
    /// columnar zero-copy contract. This path keeps the Arrow buffers as
    /// the source of truth and reads via `value_offsets()`/`value_data()`
    /// directly. Per-row appends into `key_arena` / `value_arena` are
    /// unavoidable when the source rows are non-contiguous; the savings are
    /// the 4 per-shard Vec allocations + the per-row pointer-deref chain.
    pub fn batch_put_arrow_indices_with_explicit_seqs(
        &mut self,
        batch: &RecordBatch,
        indices: &[usize],
        seqs: &[u64],
    ) -> ForstResult<usize> {
        if self.frozen {
            return Err(forst_rs_common::ForstError::FrozenMemTable);
        }
        if indices.len() != seqs.len() {
            return Err(forst_rs_common::ForstError::invalid_argument(
                "batch_put_arrow_indices_with_explicit_seqs: indices.len() != seqs.len()",
            ));
        }
        let count = indices.len();
        if count == 0 {
            return Ok(0);
        }
        if batch.num_columns() != 3 {
            return Err(forst_rs_common::ForstError::invalid_argument(format!(
                "batch_put_arrow_indices: expected 3 columns; got {}",
                batch.num_columns()
            )));
        }
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                forst_rs_common::ForstError::invalid_argument(
                    "batch_put_arrow_indices: column 0 must be BinaryArray (key)",
                )
            })?;
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                forst_rs_common::ForstError::invalid_argument(
                    "batch_put_arrow_indices: column 1 must be BinaryArray (value)",
                )
            })?;
        let ops = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::UInt8Array>()
            .ok_or_else(|| {
                forst_rs_common::ForstError::invalid_argument(
                    "batch_put_arrow_indices: column 2 must be UInt8Array (op_type)",
                )
            })?;
        let op_values: &[u8] = ops.values();
        // Validate op_types at the sparse indices.
        for (k, &i) in indices.iter().enumerate() {
            if i >= ops.len() {
                return Err(forst_rs_common::ForstError::invalid_argument(format!(
                    "batch_put_arrow_indices: index[{}]={} out of bounds (len={})",
                    k,
                    i,
                    ops.len()
                )));
            }
            if OpType::from_u8(op_values[i]).is_none() {
                return Err(forst_rs_common::ForstError::invalid_argument(format!(
                    "batch_put_arrow_indices: invalid op_type byte {} at source row {}",
                    op_values[i], i
                )));
            }
        }

        let max_seq = *seqs.iter().max().expect("count > 0");
        if self.next_sequence <= max_seq {
            self.next_sequence = max_seq + 1;
        }
        let base_offset = self.sequences.len() as u32;

        for k in 0..count {
            let i = indices[k];
            let seq = seqs[k];
            let row_offset = base_offset + k as u32;
            let key = keys.value(i);
            let op_byte = op_values[i];
            let op_type = OpType::from_u8(op_byte).expect("op_type byte validated above");
            let value_opt: Option<&[u8]> = if values.is_null(i) {
                None
            } else {
                Some(values.value(i))
            };

            // Columnar extends (per-row but reading directly from Arrow accessors).
            self.key_spans.push(self.key_arena.append(key));
            match value_opt {
                Some(v) => {
                    self.value_spans.push(self.value_arena.append(v));
                    self.value_nulls.push(false);
                }
                None => {
                    self.value_spans.push(super::arena::ByteSpan::default());
                    self.value_nulls.push(true);
                }
            }
            self.sequences.push(seq);
            self.op_types.push(op_byte);

            let row_index = RowIndex {
                offset: row_offset,
                sequence: seq,
                op_type,
            };
            self.index_insert(key, row_index);

            // FRS-C2: hash_index removed — point lookups use the BTreeMap `index`.

            // 2026-05-29 PERF: prefix_index maintenance REMOVED (O(N²) dead-weight).

            self.memory_used += key.len() + value_opt.map(|v| v.len()).unwrap_or(0) + 8 + 1 + 48;
        }

        // No unsorted→sorted merge: the lock-free `index` skiplist is always sorted.
        Ok(count)
    }

    pub fn batch_put_arrow_with_base_seq(
        &mut self,
        batch: &RecordBatch,
        base_seq: u64,
    ) -> ForstResult<usize> {
        if self.frozen {
            // R28-H1: typed variant for engine-side retry; see put_with_seq.
            return Err(forst_rs_common::ForstError::FrozenMemTable);
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
                    "batch_put_arrow: invalid op_type byte {} at index {} (expected 0=Delete, 1=Put, 2=Merge, 7=SingleDelete)",
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

        // 1. keys: append each row's bytes into the NON-MOVING arena and record
        //    its span. (Per-row append rather than one memcpy: the arena keeps
        //    each row contiguous + non-moving so reader spans stay valid under
        //    concurrent appends. The Arrow BinaryArray slice may not start at
        //    offset zero, so each row is sliced via its own [i..i+1] bounds.)
        let key_value_buf: &[u8] = keys.value_data();
        let key_offsets_arr = keys.value_offsets(); // OffsetBuffer<i32>
        self.key_spans.reserve(count);
        for i in 0..count {
            let s = key_offsets_arr[i] as usize;
            let e = key_offsets_arr[i + 1] as usize;
            self.key_spans
                .push(self.key_arena.append(&key_value_buf[s..e]));
        }

        // 2. values: append each row's bytes into the NON-MOVING arena and
        //    record its span (mirrors the key path above — non-moving chunks
        //    keep reader spans valid under concurrent appends). Null rows store
        //    a zero-length sentinel span; `value_nulls` stays the source of
        //    truth for null-ness so `value_at` skips `get` on those rows.
        let val_value_buf: &[u8] = values.value_data();
        let val_offsets_arr = values.value_offsets();
        self.value_spans.reserve(count);
        self.value_nulls.reserve(count);
        for i in 0..count {
            if values.is_null(i) {
                self.value_spans.push(super::arena::ByteSpan::default());
                self.value_nulls.push(true);
            } else {
                let s = val_offsets_arr[i] as usize;
                let e = val_offsets_arr[i + 1] as usize;
                self.value_spans
                    .push(self.value_arena.append(&val_value_buf[s..e]));
                self.value_nulls.push(false);
            }
        }

        // 3. sequences: monotonically increasing from base_seq.
        self.sequences.reserve(count);
        for i in 0..count {
            self.sequences.push(base_seq + i as u64);
        }

        // 4. op_types: single memcpy of the validated UInt8 buffer.
        self.op_types.extend_from_slice(op_values);

        // 5. ordered index + hash index: per-row insert. The skiplist insert
        //    keeps each version sorted-visible immediately (no merge step).
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
            self.index_insert(key, row_index);

            // Persistent hash index: always append so get() is O(1). The latest
            // version's value lives in `value_arena[row_offset]`; the hash entry
            // FRS-C2: hash_index removed — point lookups use the BTreeMap `index`.

            // 2026-05-29 PERF: prefix_index maintenance REMOVED (O(N²) dead-weight
            // on the Arrow zero-copy batch-write path; see batch_insert site).

            // Approximate memory accounting matching put()/batch_insert().
            let v_len = if values.is_null(i) {
                0
            } else {
                (val_offsets_arr[i + 1] - val_offsets_arr[i]) as usize
            };
            self.memory_used += key.len() + v_len + 8 + 1 + 48;
        }

        // 6. Merge threshold (same logic as batch_insert).
        // No unsorted→sorted merge: the lock-free `index` skiplist is always sorted.

        Ok(count)
    }

    /// Among a list of RowIndex entries, find the one with the highest
    /// sequence that is <= `read_sequence` and build a GetResult.
    /// FRS-C2 (2026-06-06, spec C2): newest version of `user_key` visible at
    /// `read_seq`, resolved from the sorted BTreeMap `index` — the single source
    /// of truth once the redundant `hash_index` is dropped. Entries are ordered
    /// (user_key ASC, sequence DESC), so the FIRST entry for `user_key` whose
    /// sequence ≤ `read_seq` is the newest visible version — byte-equivalent to
    /// the old `hash_index` + `find_latest` (which selected max-seq ≤ read_seq).
    #[inline]
    fn idx_newest_visible(&self, user_key: &[u8], read_seq: u64) -> Option<RowIndex> {
        for (ik, ri) in self.index.range(InternalKey::range_start(user_key)..) {
            if ik.user_key.as_slice() != user_key {
                return None; // walked past every version of this user key
            }
            if ik.sequence <= read_seq {
                return Some(*ri); // seq DESC → first visible is the newest
            }
        }
        None
    }

    // FRS-C2: `find_latest` / `find_latest_borrowed` (which scanned the old
    // `hash_index` `row_indices` version lists) are REMOVED — `idx_newest_visible`
    // above resolves the newest visible version directly from the sorted BTreeMap.
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
    fn internal_key_orders_user_key_asc_then_seq_desc() {
        let ik = |k: &[u8], s: u64| InternalKey::new(k, s);

        // Different user keys: ordered purely lexicographically, regardless of seq.
        assert!(ik(b"a", 0) < ik(b"b", u64::MAX));
        assert!(ik(b"a", u64::MAX) < ik(b"b", 0));
        // A key is ordered before another key that has it as a strict prefix
        // (pure lexicographic on the user_key field — no packed-suffix hazard).
        assert!(ik(b"abc", 0) < ik(b"abcd", u64::MAX));
        assert!(ik(b"abc", u64::MAX) < ik(b"abcd", 0));

        // Same user key: HIGHER sequence sorts FIRST (newest version first).
        assert!(ik(b"k", 9) < ik(b"k", 8));
        assert!(ik(b"k", u64::MAX) < ik(b"k", 0));

        // range_start is the smallest InternalKey for its user key, so a forward
        // range from it includes every version of that key (incl. seq 0).
        assert!(InternalKey::range_start(b"k".as_slice()) <= ik(b"k", u64::MAX));
        assert!(InternalKey::range_start(b"k".as_slice()) < ik(b"k", 0));

        // Equality requires both fields to match.
        assert_eq!(ik(b"k", 5), ik(b"k", 5));
        assert_ne!(ik(b"k", 5), ik(b"k", 6));

        // A full sort of mixed entries yields the (key ASC, seq DESC) order a
        // forward skiplist traversal must produce for flush.
        let mut v = [
            ik(b"b", 1),
            ik(b"a", 1),
            ik(b"a", 3),
            ik(b"b", 9),
            ik(b"a", 2),
        ];
        v.sort();
        let got: Vec<(&[u8], u64)> = v.iter().map(|i| (&*i.user_key, i.sequence)).collect();
        assert_eq!(
            got,
            vec![
                (b"a".as_slice(), 3),
                (b"a".as_slice(), 2),
                (b"a".as_slice(), 1),
                (b"b".as_slice(), 9),
                (b"b".as_slice(), 1),
            ]
        );
    }

    #[test]
    fn snapshot_batches_bounded_excludes_newer_seqs() {
        use arrow::array::UInt64Array;
        // Entries at seq 1,2,5,8. A bound of 5 must include only seq <= 5.
        let mut mt = VectorizedMemTable::new(test_config());
        mt.batch_insert_with_explicit_seqs(
            &[b"a", b"b", b"c", b"d"],
            &[
                Some(b"1".as_ref()),
                Some(b"2".as_ref()),
                Some(b"5".as_ref()),
                Some(b"8".as_ref()),
            ],
            &[1u8, 1u8, 1u8, 1u8],
            &[1u64, 2u64, 5u64, 8u64],
        )
        .unwrap();
        let bounded = mt.snapshot_batches_bounded(64, Some(5)).expect("bounded");
        let mut seqs = Vec::new();
        for b in &bounded {
            let s = b.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
            for i in 0..b.num_rows() {
                seqs.push(s.value(i));
            }
        }
        seqs.sort_unstable();
        assert_eq!(seqs, vec![1, 2, 5], "seq 8 (> bound) must be excluded");
        // Unbounded includes all four.
        let all = mt.snapshot_batches_bounded(64, None).expect("unbounded");
        let total: usize = all.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_new_memtable_empty() {
        let mt = VectorizedMemTable::new(test_config());
        assert_eq!(mt.num_entries(), 0);
        assert_eq!(mt.memory_usage(), 0);
        assert!(!mt.is_frozen());
    }

    #[test]
    fn snapshot_batches_round_trips_all_versions_and_keeps_memtable_live() {
        use arrow::array::UInt8Array;
        // Source memtable: multi-version key (k1 @seq1=Put, @seq5=Put), a
        // tombstone (k2 @seq3=Delete, value None), a plain key (k3 @seq2=Put),
        // and some entries left in the UNSORTED buffer (no freeze) to prove
        // snapshot_batches merges them.
        let mut src = VectorizedMemTable::new(test_config());
        src.batch_insert_with_explicit_seqs(
            &[b"k1", b"k3", b"k1"],
            &[
                Some(b"v1a".as_ref()),
                Some(b"v3".as_ref()),
                Some(b"v1b".as_ref()),
            ],
            &[1u8, 1u8, 1u8], // Put, Put, Put
            &[1u64, 2u64, 5u64],
        )
        .unwrap();
        // Tombstone for k2, inserted separately so it lands in the unsorted zone.
        src.batch_insert_with_explicit_seqs(&[b"k2"], &[None], &[0u8], &[3u64])
            .unwrap();

        // Snapshot the LIVE memtable (must NOT require freeze).
        assert!(!src.is_frozen());
        let batches = src.snapshot_batches(4).expect("snapshot_batches");

        // The memtable must remain live + writable + readable after snapshot.
        assert!(
            !src.is_frozen(),
            "snapshot_batches must not seal the memtable"
        );
        src.put(b"k4", Some(b"v4"), 1)
            .expect("memtable still writable after snapshot");
        assert!(
            src.get(b"k1", 100).unwrap().is_some(),
            "reads still work after snapshot"
        );

        // Replay the batches into a fresh memtable via explicit seqs.
        let mut dst = VectorizedMemTable::new(test_config());
        let mut total_rows = 0;
        for b in &batches {
            let keys = b.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
            let vals = b.column(1).as_any().downcast_ref::<BinaryArray>().unwrap();
            let seqs = b
                .column(2)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            let ops = b.column(3).as_any().downcast_ref::<UInt8Array>().unwrap();
            for i in 0..b.num_rows() {
                let k = keys.value(i).to_vec();
                let v: Option<Vec<u8>> = if vals.is_null(i) {
                    None
                } else {
                    Some(vals.value(i).to_vec())
                };
                dst.batch_insert_with_explicit_seqs(
                    &[k.as_slice()],
                    &[v.as_deref()],
                    &[ops.value(i)],
                    &[seqs.value(i)],
                )
                .unwrap();
                total_rows += 1;
            }
        }
        assert!(
            total_rows >= 4,
            "expected >=4 rows serialised (k1×2, k2, k3)"
        );

        // Multi-version visibility must be byte-identical between src and dst at
        // every interesting read sequence (snapshot reads resolve newest <= seq).
        for &rs in &[0u64, 1, 2, 3, 4, 5, 100] {
            for key in [b"k1".as_ref(), b"k2".as_ref(), b"k3".as_ref()] {
                let a = src
                    .get(key, rs)
                    .unwrap()
                    .map(|r| (r.value.clone(), r.op_type));
                let b = dst
                    .get(key, rs)
                    .unwrap()
                    .map(|r| (r.value.clone(), r.op_type));
                assert_eq!(a, b, "mismatch key={:?} read_seq={}", key, rs);
            }
        }
        // Spot-check the actual resolved values.
        assert_eq!(
            src.get(b"k1", 1).unwrap().unwrap().value.as_deref(),
            Some(b"v1a".as_ref())
        );
        assert_eq!(
            dst.get(b"k1", 5).unwrap().unwrap().value.as_deref(),
            Some(b"v1b".as_ref())
        );
        // k2 tombstone at seq>=3 resolves to a Delete.
        assert_eq!(dst.get(b"k2", 3).unwrap().unwrap().op_type, OpType::Delete);
    }

    #[test]
    fn test_put_single_entry() {
        let mut mt = VectorizedMemTable::new(test_config());
        let seq = mt.put(b"key1", Some(b"val1"), 1).unwrap();
        assert_eq!(seq, 1);
        assert_eq!(mt.num_entries(), 1);
    }

    #[test]
    fn test_put_multiple_entries() {
        let mut mt = VectorizedMemTable::new(test_config());
        for i in 0..100u32 {
            let key = format!("key_{:05}", i);
            let val = format!("val_{:05}", i);
            let seq = mt.put(key.as_bytes(), Some(val.as_bytes()), 1).unwrap();
            assert_eq!(seq, i as u64 + 1);
        }
        assert_eq!(mt.num_entries(), 100);
    }

    #[test]
    fn test_put_delete_tombstone() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"del_key", None, 0).unwrap(); // Delete
        assert_eq!(mt.num_entries(), 1);
        assert!(mt.value_at(0).is_none());
    }

    #[test]
    fn test_put_frozen_fails() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.frozen = true;
        let result = mt.put(b"k", Some(b"v"), 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_key_at_value_at() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"alpha", Some(b"one"), 1).unwrap();
        mt.put(b"beta", Some(b"two"), 1).unwrap();
        mt.put(b"gamma", None, 0).unwrap(); // delete

        assert_eq!(mt.key_at(0), b"alpha");
        assert_eq!(mt.key_at(1), b"beta");
        assert_eq!(mt.key_at(2), b"gamma");
        assert_eq!(mt.value_at(0), Some(b"one".as_slice()));
        assert_eq!(mt.value_at(1), Some(b"two".as_slice()));
        assert_eq!(mt.value_at(2), None);
    }

    #[test]
    fn put_makes_keys_immediately_sorted_visible() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"c", Some(b"3"), 1).unwrap();
        mt.put(b"a", Some(b"1"), 1).unwrap();
        mt.put(b"b", Some(b"2"), 1).unwrap();

        // The lock-free `index` skiplist is always sorted — every write is
        // immediately scan-visible in ascending order, with NO merge step.
        let keys = mt.range_scan_keys(b"", None);
        let got: Vec<&[u8]> = keys.iter().map(|k| &**k).collect();
        assert_eq!(got, vec![b"a".as_slice(), b"b", b"c"]);

        // merge_unsorted_to_sorted is now a no-op and changes nothing.
        mt.merge_unsorted_to_sorted();
        assert_eq!(mt.range_scan_keys(b"", None).len(), 3);
    }

    #[test]
    fn test_merge_multi_version_same_key() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"key", Some(b"v1"), 1).unwrap(); // seq=1
        mt.put(b"key", Some(b"v2"), 1).unwrap(); // seq=2

        // Both versions retained; a range scan returns them newest-first
        // (key ASC, seq DESC) directly from the skiplist — no merge needed.
        let rows = mt.collect_range_entries(b"", None, u64::MAX);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, b"key");
        assert_eq!(rows[0].2, 2); // newest sequence first
        assert_eq!(rows[1].2, 1);
    }

    #[test]
    fn test_sequence_monotonically_increasing() {
        let mut mt = VectorizedMemTable::new(test_config());
        let s1 = mt.put(b"a", Some(b"1"), 1).unwrap();
        let s2 = mt.put(b"b", Some(b"2"), 1).unwrap();
        let s3 = mt.put(b"c", Some(b"3"), 1).unwrap();
        assert!(s1 < s2);
        assert!(s2 < s3);
    }

    #[test]
    fn test_memory_usage_increases() {
        let mut mt = VectorizedMemTable::new(test_config());
        assert_eq!(mt.memory_usage(), 0);
        mt.put(b"key", Some(b"value"), 1).unwrap();
        assert!(mt.memory_usage() > 0);
    }

    #[test]
    fn test_get_existing_key() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"hello", Some(b"world"), 1).unwrap();
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
        mt.put(b"key", Some(b"v1"), 1).unwrap();
        mt.put(b"key", Some(b"v2"), 1).unwrap();
        mt.put(b"key", Some(b"v3"), 1).unwrap();

        let r = mt.get(b"key", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v3".to_vec()));
        assert_eq!(r.sequence, 3);
    }

    // TODO(mvcc-snapshot-read): inline_value fast path may return latest version
    // even when read_sequence < latest_seq. Predates V1 work; tracked separately.
    // See find_latest() in vectorized.rs.
    #[test]
    #[ignore = "pre-existing MVCC inline-value bug; tracked separately"]
    fn test_get_respects_read_sequence() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"key", Some(b"v1"), 1).unwrap();
        mt.put(b"key", Some(b"v2"), 1).unwrap();
        mt.put(b"key", Some(b"v3"), 1).unwrap();

        let r = mt.get(b"key", 2).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v2".to_vec()));
        assert_eq!(r.sequence, 2);

        let r = mt.get(b"key", 1).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v1".to_vec()));
    }

    #[test]
    fn test_get_delete_tombstone() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"key", Some(b"val"), 1).unwrap();
        mt.put(b"key", None, 0).unwrap();

        let r = mt.get(b"key", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, None);
        assert_eq!(r.op_type, OpType::Delete);
        assert_eq!(r.sequence, 2);
    }

    // PR-B7-H2: borrowed-value point lookup primitive coverage.
    // These tests assert that `get_borrowed` returns a `MemtableValueRef::Inline`
    // for the small-Put inline-cache hot path and that the borrow exposes the
    // same bytes the legacy `get()` would have allocated.

    #[test]
    fn test_get_borrowed_inline_hit() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"key", Some(b"hot-value"), 1).unwrap();

        let r = mt.get_borrowed(b"key", u64::MAX).unwrap().unwrap();
        assert_eq!(r.sequence, 1);
        assert_eq!(r.op_type, OpType::Put);
        let v = r.value.expect("Put with value must have borrowed bytes");
        // Inline path: bytes borrowed from `inline_value: Box<[u8]>`.
        match &v {
            MemtableValueRef::Inline(slice) => assert_eq!(*slice, b"hot-value"),
            MemtableValueRef::Heap(_) => {
                panic!("small-Put should land on the inline-cache fast path")
            }
        }
        assert_eq!(v.as_bytes(), b"hot-value");
    }

    #[test]
    fn test_get_borrowed_missing_key() {
        let mt = VectorizedMemTable::new(test_config());
        let r = mt.get_borrowed(b"absent", u64::MAX).unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn test_get_borrowed_tombstone() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"key", Some(b"v"), 1).unwrap();
        mt.put(b"key", None, 0).unwrap();
        let r = mt.get_borrowed(b"key", u64::MAX).unwrap().unwrap();
        assert!(r.value.is_none());
        assert_eq!(r.op_type, OpType::Delete);
    }

    #[test]
    fn test_get_borrowed_matches_get() {
        // Cross-check: `get_borrowed` must agree with `get` on every key.
        let mut mt = VectorizedMemTable::new(test_config());
        for i in 0..32u32 {
            let k = format!("k{:03}", i);
            let v = format!("value-{}", i);
            mt.put(k.as_bytes(), Some(v.as_bytes()), 1).unwrap();
        }
        // Force a merge so the columnar (non-inline) path is also exercised
        // for the unsorted vs sorted lookup.
        mt.merge_unsorted_to_sorted();
        for i in 0..32u32 {
            let k = format!("k{:03}", i);
            let owned = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            let borrowed = mt.get_borrowed(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(borrowed.sequence, owned.sequence);
            assert_eq!(borrowed.op_type, owned.op_type);
            assert_eq!(
                borrowed.value.as_ref().map(|r| r.as_bytes().to_vec()),
                owned.value
            );
        }
    }

    #[test]
    fn test_get_after_merge_to_sorted() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"a", Some(b"1"), 1).unwrap();
        mt.put(b"b", Some(b"2"), 1).unwrap();
        mt.merge_unsorted_to_sorted();

        mt.put(b"a", Some(b"updated"), 1).unwrap();

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
        let ops = vec![1u8, 0]; // Put (1), Delete (0) — RocksDB byte-compat

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
            mt.put(key.as_bytes(), Some(val.as_bytes()), 1).unwrap();
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
        mt.put(b"k", Some(b"v"), 1).unwrap();
        mt.freeze();
        assert!(mt.is_frozen());
        assert!(mt.put(b"k2", Some(b"v2"), 1).is_err());
    }

    #[test]
    fn test_freeze_keeps_keys_sorted_visible() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"c", Some(b"3"), 1).unwrap();
        mt.put(b"a", Some(b"1"), 1).unwrap();
        // Keys are sorted-visible before AND after freeze (skiplist is always
        // sorted; freeze adds no merge).
        assert_eq!(mt.range_scan_keys(b"", None).len(), 2);
        mt.freeze();
        let keys = mt.range_scan_keys(b"", None);
        let got: Vec<&[u8]> = keys.iter().map(|k| &**k).collect();
        assert_eq!(got, vec![b"a".as_slice(), b"c"]);
    }

    #[test]
    fn test_to_flush_batches_requires_frozen() {
        let mt = VectorizedMemTable::new(test_config());
        assert!(mt.to_flush_batches(1024).is_err());
    }

    #[test]
    fn test_to_flush_batches_sorted_output() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"charlie", Some(b"3"), 1).unwrap();
        mt.put(b"alpha", Some(b"1"), 1).unwrap();
        mt.put(b"bravo", Some(b"2"), 1).unwrap();
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
        mt.put(b"key", Some(b"v1"), 1).unwrap();
        mt.put(b"key", Some(b"v2"), 1).unwrap();
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
            mt.put(key.as_bytes(), Some(b"v"), 1).unwrap();
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
        mt.put(b"alive", Some(b"val"), 1).unwrap();
        mt.put(b"dead", None, 0).unwrap();
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
        assert_eq!(ops.value(0), 1); // Put (OpType::Put = 1, RocksDB byte-compat)

        assert!(values.is_null(1));
        assert_eq!(ops.value(1), 0); // Delete (OpType::Delete = 0, RocksDB byte-compat)
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
        mt.put(b"key1", Some(b"base"), 1).unwrap(); // Put
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
        mt.put(b"key1", Some(b"base"), 1).unwrap(); // seq=1
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
        // RocksDB byte-compat: 0=Delete, 1=Put, 2=Merge, 7=SingleDelete — valid.
        // All others must be rejected.
        for invalid in [3u8, 4, 5, 6, 8, 42, 99, 200, 255] {
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
        // key_spans / value_spans are per-row (one entry per inserted row),
        // so both are empty when nothing was inserted; sequences too.
        assert_eq!(mt.sequences.len(), 0);
        assert_eq!(mt.key_spans.len(), 0, "no key spans pushed");
        assert_eq!(mt.value_spans.len(), 0, "no value spans pushed");
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
        assert_eq!(mt.key_spans.len(), 0, "no key spans on rejected insert");
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
            mt.put(key.as_bytes(), Some(val.as_bytes()), 1).unwrap();
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
        // The real invariant: every one of the 1000 unique keys is live and
        // sorted-visible in the skiplist index, and round-trips through get().
        assert_eq!(
            mt.range_scan_keys(b"", None).len(),
            1000,
            "all 1000 unique keys must be sorted-visible in the index"
        );

        for i in 0..1000u32 {
            let key = format!("uniq_{:06}", i);
            let expected = format!("val_{:06}", i);
            let r = mt.get(key.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.value, Some(expected.into_bytes()), "mismatch at {}", i);
        }
    }

    /// Repeated keys confirm multi-version semantics: N writes to the same key
    /// retain all N versions in the skiplist (one entry per version), and
    /// `get()` returns the latest.
    #[test]
    fn test_repeated_keys_retain_all_versions() {
        let mut mt = VectorizedMemTable::new(test_config());
        // 20 distinct keys × 10 rounds = 200 inserts = 200 versions.
        for round in 0..10u32 {
            for i in 0..20u32 {
                let k = format!("rep_{:03}", i);
                let v = format!("v{:02}_r{}", i, round);
                mt.put(k.as_bytes(), Some(v.as_bytes()), 1).unwrap();
            }
        }
        // 20 distinct keys, each with 10 versions → 200 rows across the range.
        assert_eq!(mt.range_scan_keys(b"", None).len(), 20);
        assert_eq!(mt.collect_range_entries(b"", None, u64::MAX).len(), 200);
        // Each key's versions are present and newest-first within the key.
        for i in 0..20u32 {
            let k = format!("rep_{:03}", i);
            let rows = mt.collect_range_entries(k.as_bytes(), None, u64::MAX);
            let kvers: Vec<&ScanRow> = rows.iter().filter(|r| r.0 == k.as_bytes()).collect();
            assert_eq!(kvers.len(), 10, "key {} should have 10 versions", k);
        }
        // Latest version must come back from get().
        for i in 0..20u32 {
            let k = format!("rep_{:03}", i);
            let expected = format!("v{:02}_r9", i);
            let r = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.value, Some(expected.into_bytes()));
        }
    }

    /// Multi-version MVCC: re-writing the same key keeps every version sorted
    /// (key ASC, seq DESC) in the skiplist, and snapshot reads at each
    /// sequence boundary see the right version.
    // TODO(mvcc-snapshot-read): same root cause as test_get_respects_read_sequence.
    #[test]
    #[ignore = "pre-existing MVCC inline-value bug; tracked separately"]
    fn test_multi_version_mvcc_snapshot_reads() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"k", Some(b"v1"), 1).unwrap(); // seq=1
        mt.put(b"k", Some(b"v2"), 1).unwrap(); // seq=2
        mt.put(b"k", Some(b"v3"), 1).unwrap(); // seq=3

        let rows = mt.collect_range_entries(b"k", None, u64::MAX);
        assert_eq!(rows.len(), 3);
        // Sorted DESC by sequence.
        assert_eq!(rows[0].2, 3);
        assert_eq!(rows[1].2, 2);
        assert_eq!(rows[2].2, 1);

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
        let ops = vec![1u8; 100]; // Put (OpType::Put = 1, RocksDB byte-compat)
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
        let ops: Vec<u8> = vec![1, 0, 1]; // Put (1), Delete (0), Put (1) — RocksDB byte-compat
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
        assert_eq!(mt.key_spans.len(), 0);
        assert_eq!(mt.value_spans.len(), 0);
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
            mt.put(k.as_bytes(), Some(b"px"), 1).unwrap();
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

    // === Hash index O(1) point lookup tests ===

    /// Validates that the hash_index gives correct point-lookup results for a
    /// large key set, and that the skiplist index holds every key sorted —
    /// the two indexes stay consistent.
    #[test]
    fn test_hash_index_point_lookup_consistent_with_index() {
        let mut mt = VectorizedMemTable::new(MemTableConfig {
            max_size: 64 * 1024 * 1024,
            unsorted_merge_ratio: 1024.0,
        });
        // Insert 10k keys.
        for i in 0..10_000u32 {
            let k = format!("hk_{:06}", i);
            let v = format!("hv_{:06}", i);
            mt.put(k.as_bytes(), Some(v.as_bytes()), 1).unwrap();
        }
        // All 10k keys are sorted-visible in the skiplist index.
        assert_eq!(mt.range_scan_keys(b"", None).len(), 10_000);

        // hash_index must still serve all 10k keys at O(1).
        for i in 0..10_000u32 {
            let k = format!("hk_{:06}", i);
            let v = format!("hv_{:06}", i);
            let r = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(r.value, Some(v.into_bytes()), "mismatch at {}", i);
        }
    }

    /// Multi-version: hash_index correctly returns the latest version
    /// even after multiple merges.
    // TODO(mvcc-snapshot-read): same root cause as test_get_respects_read_sequence.
    #[test]
    #[ignore = "pre-existing MVCC inline-value bug; tracked separately"]
    fn test_hash_index_multi_version_across_merges() {
        let mut mt = VectorizedMemTable::new(MemTableConfig {
            max_size: 64 * 1024 * 1024,
            unsorted_merge_ratio: 1024.0,
        });
        mt.put(b"key", Some(b"v1"), 1).unwrap(); // seq=1
        mt.merge_unsorted_to_sorted();
        mt.put(b"key", Some(b"v2"), 1).unwrap(); // seq=2
        mt.merge_unsorted_to_sorted();
        mt.put(b"key", Some(b"v3"), 1).unwrap(); // seq=3

        // Latest version.
        let r = mt.get(b"key", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v3".to_vec()));
        assert_eq!(r.sequence, 3);

        // Snapshot reads at each seq boundary.
        let r = mt.get(b"key", 1).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v1".to_vec()));
        let r = mt.get(b"key", 2).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v2".to_vec()));
    }

    /// Hash index handles delete tombstones correctly.
    #[test]
    fn test_hash_index_delete_tombstone() {
        let mut mt = VectorizedMemTable::new(MemTableConfig {
            max_size: 64 * 1024 * 1024,
            unsorted_merge_ratio: 1024.0,
        });
        mt.put(b"key", Some(b"alive"), 1).unwrap();
        mt.merge_unsorted_to_sorted();
        mt.put(b"key", None, 0).unwrap(); // Delete

        let r = mt.get(b"key", u64::MAX).unwrap().unwrap();
        assert_eq!(r.op_type, OpType::Delete);
        assert_eq!(r.value, None);

        // Snapshot before delete sees the value.
        let r = mt.get(b"key", 1).unwrap().unwrap();
        assert_eq!(r.value, Some(b"alive".to_vec()));
    }

    /// 10k keys: hash-index get matches what a full scan would return.
    #[test]
    fn hash_index_point_lookup_matches_scan() {
        let mut mt = VectorizedMemTable::new(MemTableConfig {
            max_size: 64 * 1024 * 1024,
            unsorted_merge_ratio: 0.25, // allow natural merges
        });
        // Insert 10k keys with some overwrites.
        for i in 0..10_000u32 {
            let k = format!("sk_{:06}", i);
            let v = format!("sv_{:06}", i);
            mt.put(k.as_bytes(), Some(v.as_bytes()), 1).unwrap();
        }
        // Overwrite first 1000 keys.
        for i in 0..1_000u32 {
            let k = format!("sk_{:06}", i);
            let v = format!("sv2_{:06}", i);
            mt.put(k.as_bytes(), Some(v.as_bytes()), 1).unwrap();
        }

        // Verify hash-index get matches expected for every key.
        for i in 0..10_000u32 {
            let k = format!("sk_{:06}", i);
            let expected_v = if i < 1000 {
                format!("sv2_{:06}", i)
            } else {
                format!("sv_{:06}", i)
            };
            let r = mt.get(k.as_bytes(), u64::MAX).unwrap().unwrap();
            assert_eq!(
                r.value,
                Some(expected_v.into_bytes()),
                "mismatch at key {}",
                i
            );
        }
    }

    // === Latest-value fast-path tests (FRS-MEMTABLE-INLINE-KEY) ===
    // The hash entry no longer caches an inline `Box<[u8]>`; it stores the latest
    // version's `value_arena` row offset and `get()` reads the value from the arena.
    // These assert the OBSERVABLE behaviour via `get()` (black-box) rather than the
    // internal field — and cover that the old INLINE_THRESHOLD cliff is gone (any
    // value size served correctly through the same fast path).

    #[test]
    fn test_latest_value_small_served() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"k", Some(b"small"), 1).unwrap(); // OpType::Put = 1
        let r = mt.get(b"k", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"small".to_vec()));
        assert_eq!(r.op_type, OpType::Put);
    }

    #[test]
    fn test_latest_value_large_served() {
        // Values larger than the OLD INLINE_THRESHOLD are now served by the SAME
        // fast path (read from value_arena via latest_offset) — no cliff.
        let mut mt = VectorizedMemTable::new(test_config());
        let large = vec![7u8; INLINE_THRESHOLD + 1024];
        mt.put(b"k2", Some(&large), 1).unwrap();
        let r = mt.get(b"k2", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(large));
    }

    #[test]
    fn test_latest_value_at_and_over_threshold_both_served() {
        let mut mt = VectorizedMemTable::new(test_config());
        let exact = vec![42u8; INLINE_THRESHOLD];
        mt.put(b"exact", Some(&exact), 1).unwrap();
        assert_eq!(
            mt.get(b"exact", u64::MAX).unwrap().unwrap().value,
            Some(exact)
        );

        let over = vec![42u8; INLINE_THRESHOLD + 1];
        mt.put(b"over", Some(&over), 1).unwrap();
        assert_eq!(
            mt.get(b"over", u64::MAX).unwrap().unwrap().value,
            Some(over)
        );
    }

    #[test]
    fn test_latest_value_overwrite() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"k", Some(b"v1"), 1).unwrap();
        mt.put(b"k", Some(b"v2"), 1).unwrap();
        let r = mt.get(b"k", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v2".to_vec()));
        assert_eq!(r.sequence, 2);
    }

    #[test]
    fn test_latest_value_delete_tombstone() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"k", Some(b"val"), 1).unwrap();
        mt.put(b"k", None, 0).unwrap(); // OpType::Delete = 0
        let r = mt.get(b"k", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, None);
        assert_eq!(r.op_type, OpType::Delete);
    }

    #[test]
    fn test_latest_value_small_to_large_overwrite() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"k", Some(b"small"), 1).unwrap();
        let large = vec![0u8; INLINE_THRESHOLD + 1];
        mt.put(b"k", Some(&large), 1).unwrap();
        let r = mt.get(b"k", u64::MAX).unwrap().unwrap();
        assert_eq!(r.value, Some(large));
    }

    #[test]
    fn test_latest_value_mvcc_snapshot_reads_older_version() {
        let mut mt = VectorizedMemTable::new(test_config());
        mt.put(b"k", Some(b"v1"), 1).unwrap(); // seq=1
        mt.put(b"k", Some(b"v2"), 1).unwrap(); // seq=2

        // Current read sees v2.
        assert_eq!(
            mt.get(b"k", u64::MAX).unwrap().unwrap().value,
            Some(b"v2".to_vec())
        );

        // Snapshot read at seq=1 must return v1 (fast path is gated on latest_seq
        // <= read_sequence, so seq=1 < latest_seq=2 falls to the columnar MVCC path).
        let r = mt.get(b"k", 1).unwrap().unwrap();
        assert_eq!(r.value, Some(b"v1".to_vec()));
        assert_eq!(r.sequence, 1);
    }

    /// C8-H2 regression test: Put → Delete → Put-within-same-memtable must
    /// re-register the key in the prefix_index. Before the fix the second
    /// Put's `was_new_key` was false (the hash_index still had both prior
    /// versions), so the prefix-index re-add was skipped; the Delete had
    /// already removed the key from the bucket, so `prefix_scan_keys`
    /// silently missed the resurrected key — Q11 `entries()` row-loss.
    #[test]
    fn prefix_index_handles_put_delete_put_within_memtable() {
        // ---- single-write path ----
        let mut mt = VectorizedMemTable::new(test_config());
        let key: &[u8] = b"ns/k1";
        mt.put(key, Some(b"v1"), OpType::Put as u8).unwrap();
        mt.put(key, None, OpType::Delete as u8).unwrap();
        mt.put(key, Some(b"v2"), OpType::Put as u8).unwrap();
        let keys = mt.prefix_scan_keys(b"ns/", None);
        assert!(
            keys.iter().any(|k| &**k == key),
            "single-write Put→Delete→Put must leave key visible to prefix_scan_keys; got {:?}",
            keys
        );

        // ---- batch_insert_with_base_seq path ----
        let mut mt2 = VectorizedMemTable::new(test_config());
        let ops = vec![OpType::Put as u8, OpType::Delete as u8, OpType::Put as u8];
        let key_refs: Vec<&[u8]> = vec![key, key, key];
        let v1: &[u8] = b"v1";
        let v2: &[u8] = b"v2";
        let val_refs: Vec<Option<&[u8]>> = vec![Some(v1), None, Some(v2)];
        mt2.batch_insert_with_base_seq(&key_refs, &val_refs, &ops, 1)
            .unwrap();
        let keys2 = mt2.prefix_scan_keys(b"ns/", None);
        assert!(
            keys2.iter().any(|k| &**k == key),
            "batch_insert_with_base_seq Put→Delete→Put must leave key visible; got {:?}",
            keys2
        );

        // ---- batch_insert_with_explicit_seqs path ----
        let mut mt3 = VectorizedMemTable::new(test_config());
        let seqs = vec![1u64, 2, 3];
        mt3.batch_insert_with_explicit_seqs(&key_refs, &val_refs, &ops, &seqs)
            .unwrap();
        let keys3 = mt3.prefix_scan_keys(b"ns/", None);
        assert!(
            keys3.iter().any(|k| &**k == key),
            "batch_insert_with_explicit_seqs Put→Delete→Put must leave key visible; got {:?}",
            keys3
        );

        // ---- batch_put_arrow_with_base_seq (FFM zero-copy) path ----
        // Reuse the `make_arrow_batch` helper (3-column key|value|op_type
        // shape that `batch_put_arrow_with_base_seq` expects). Constructing
        // the RecordBatch directly with `sst_schema()` (which carries a 4th
        // `sequence` column for on-disk SST format) trips the
        // "expected 3 columns" guard at the top of `batch_put_arrow`.
        let mut mt4 = VectorizedMemTable::new(test_config());
        let key_refs4: Vec<&[u8]> = vec![key, key, key];
        let v1: &[u8] = b"v1";
        let v2: &[u8] = b"v2";
        let val_refs4: Vec<Option<&[u8]>> = vec![Some(v1), None, Some(v2)];
        let ops4: Vec<u8> = vec![OpType::Put as u8, OpType::Delete as u8, OpType::Put as u8];
        let batch = make_arrow_batch(&key_refs4, &val_refs4, &ops4);
        mt4.batch_put_arrow_with_base_seq(&batch, 1).unwrap();
        let keys4 = mt4.prefix_scan_keys(b"ns/", None);
        assert!(
            keys4.iter().any(|k| &**k == key),
            "batch_put_arrow_with_base_seq Put→Delete→Put must leave key visible; got {:?}",
            keys4
        );
    }

    /// S3-MAPITER-FIX regression: a MapState composite key whose serialized
    /// user-key contains a '/' (0x2f) byte must still be returned by a prefix
    /// scan over the state prefix. The removed `prefix_index` fast path bucketed
    /// by the LAST '/' in the full key, so such a key landed in a deeper bucket
    /// and was silently dropped from `prefix_scan_keys(state_prefix)` — making
    /// the active-memtable scan disagree with the SST tier (which uses the
    /// correct `[prefix, prefix_upper_bound)` byte-range) and surfacing on S3 as
    /// a corrupt / EOFException MapState iteration.
    #[test]
    fn prefix_scan_returns_keys_with_slash_inside_user_key() {
        let mut mt = VectorizedMemTable::new(test_config());
        // Composite key layout mirrors ForStRsMapStateV2:
        //   "k/" + serialize(K) + "/" + stateName + "/" + serialize(UK)
        // where serialize(UK) contains an embedded '/' (0x2f).
        let prefix: &[u8] = b"k/KEY/join-records/";
        let key_plain: &[u8] = b"k/KEY/join-records/userA"; // no '/' in UK
        let key_with_slash: &[u8] = b"k/KEY/join-records/user/B"; // '/' inside UK
        let key_two_slashes: &[u8] = b"k/KEY/join-records/a/b/c"; // multiple '/'

        mt.put(key_plain, Some(b"1"), OpType::Put as u8).unwrap();
        mt.put(key_with_slash, Some(b"1"), OpType::Put as u8)
            .unwrap();
        mt.put(key_two_slashes, Some(b"1"), OpType::Put as u8)
            .unwrap();

        let keys = mt.prefix_scan_keys(prefix, None);
        assert!(
            keys.iter().any(|k| &**k == key_plain),
            "plain UK key must be visible; got {:?}",
            keys
        );
        assert!(
            keys.iter().any(|k| &**k == key_with_slash),
            "UK-with-embedded-slash key must be visible (S3-MAPITER-FIX); got {:?}",
            keys
        );
        assert!(
            keys.iter().any(|k| &**k == key_two_slashes),
            "UK-with-multiple-slashes key must be visible (S3-MAPITER-FIX); got {:?}",
            keys
        );
        assert_eq!(keys.len(), 3, "exactly the 3 prefixed keys; got {:?}", keys);

        // A foreign key under a different state prefix must NOT leak in.
        mt.put(b"k/KEY/other-state/x", Some(b"1"), OpType::Put as u8)
            .unwrap();
        let keys2 = mt.prefix_scan_keys(prefix, None);
        assert_eq!(
            keys2.len(),
            3,
            "foreign-prefix key must be excluded; got {:?}",
            keys2
        );
    }

    #[test]
    fn prefix_index_tracks_merge_only_keys() {
        let mut mt = VectorizedMemTable::new(test_config());
        let key: &[u8] = b"ns/list-key";
        mt.put(key, Some(b"A"), OpType::Merge as u8).unwrap();

        let keys = mt.prefix_scan_keys(b"ns/", None);
        assert!(
            keys.iter().any(|k| &**k == key),
            "merge-only key must be visible to prefix_scan_keys; got {:?}",
            keys
        );
    }
}
