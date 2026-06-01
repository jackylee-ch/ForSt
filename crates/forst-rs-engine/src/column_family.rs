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

//! Column family handle, descriptor, and data. See `2.8_read_write_paths.md` §5.
//!
//! A [`ColumnFamilyHandle`] is a lightweight, cheaply cloneable identifier
//! returned by the engine when a CF is opened. A [`ColumnFamilyDescriptor`]
//! bundles the name, [`CfOptions`], and optional [`MergeOperator`] used when
//! creating/opening a column family. [`ColumnFamilyData`] holds the per-CF
//! mutable state (active memtable, immutable list, cached snapshot view).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arc_swap::ArcSwap;
use forst_rs_common::{CfOptions, ColumnFamilyId};
use forst_rs_storage::memtable::{MemTableConfig, ShardedMemTable};
use forst_rs_storage::merge_operator::MergeOperator;
use forst_rs_common::types::FileNumber;

/// FRS-RESIDENT-FLUSHED: bytes cap for the resident-flushed-memtable read cache.
///
/// At checkpoint, every imm memtable is flushed to an S3 L0 SST. Without a WAL
/// the flush itself is mandatory for durability, but the read path then pays
/// an S3 round-trip per join probe for state that was *just* in RAM. We retain
/// the flushed memtable in a bounded FIFO so the prefix-scan path can serve
/// recently-flushed hot data from RAM (Tier 2) and skip the byte-identical L0
/// SST (Tier 3) for as long as the memtable is still resident. Recovery is
/// unchanged (reads SSTs from the manifest); the resident set is a pure read
/// accelerator — a bug here can only slow reads, never lose data.
///
/// FRS-8C32G-RAMBUDGET (2026-05-30): cap is now **1 GiB per CF**, down from a
/// prior 16 GiB that was explicitly sized "for a 256 GiB host". That value made
/// forst-rs UNRUNNABLE on the professional 8c/32g benchmark box: the cap is
/// per-CF and a single Nexmark query opens ~8 stateful DB instances (Join ×p +
/// Rank ×p), so 16 GiB × 8 ≈ 128 GiB of RAM-shadow attempts → instant OOM on a
/// 32 GiB box. The resident shadow is a pure RAM read-accelerator; the DURABLE
/// path is the SST + the 64 GiB on-DISK `LocalCache` (NVMe, ~100 µs), which is
/// what actually hides S3 latency. So on a RAM-constrained box we keep the
/// shadow small (just-flushed hot tail) and let spilled state fall to the disk
/// cache — the same model ForSt uses. 1 GiB × ~8 instances ≈ 8 GiB, leaving room
/// for JVM heap + decoded-block cache + WBM within 32 GiB.
/// TODO(config-driven): size this from available RAM / instance count via
/// EngineOptions rather than a fixed const, and make it a GLOBAL cross-instance
/// budget so it cannot scale with instance count.
pub const DEFAULT_RESIDENT_FLUSHED_CAP_BYTES: usize = 1024 * 1024 * 1024;

/// Per-run override of the resident-flushed RAM-shadow cap, in MiB, via
/// `FRS_RESIDENT_SHADOW_MB`. Returns [`DEFAULT_RESIDENT_FLUSHED_CAP_BYTES`] when
/// unset/invalid.
///
/// FRS-RESIDENT-SHADOW-ENV (2026-05-31): the shadow keeps just-flushed state in
/// RAM in its DECODED memtable form, so reads avoid the S3-SST
/// decompress + Arrow-IPC-decode tax that collapses heavy joins under ckpt-ON
/// (measured: q4/q7/q9/q15/q16 cap at the 700 s wall once join state spills past
/// the 1 GiB shadow). Crucially the join's keyed state is PARTITIONED across the
/// `parallelism` backend instances, so EACH instance holds only ~1/parallelism
/// of the total working set — a shadow sized to one instance's share keeps that
/// share fully RAM-resident and restores the ckpt-OFF behaviour (state never
/// leaves RAM) that gave the heavy-join wins. This hook lets the operator size
/// the shadow against the box: e.g. a parallelism-4 join on 8c/32g can afford
/// ~3 GiB × 4 = 12 GiB alongside the 6 GiB WBM + 8 GiB JVM. Sizing is the
/// operator's responsibility — too large × instance-count OOMs the box (the
/// reason the default stays a conservative 1 GiB).
pub fn resident_flushed_cap_bytes() -> usize {
    match std::env::var("FRS_RESIDENT_SHADOW_MB")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&mb| mb > 0)
    {
        Some(mb) => mb.saturating_mul(1024 * 1024),
        None => DEFAULT_RESIDENT_FLUSHED_CAP_BYTES,
    }
}

use crate::compaction_filter::CompactionFilter;
use crate::snapshot_view::SnapshotView;

/// Lightweight handle to an open column family.
///
/// Contains the numeric id and a shared pointer to the name. Clone is O(1)
/// because `Arc` pointer bumps are cheap. Equality is defined by id only.
///
/// Holds a shared `dropped` flag (cloned from the `ColumnFamilyData` it
/// was minted against) so callers can ask `is_dropped()` without going
/// back through the engine. The handle's flag and the data's flag are the
/// same `Arc<AtomicBool>` — a `drop_cf` on the engine flips both at once.
#[derive(Clone, Debug)]
pub struct ColumnFamilyHandle {
    id: ColumnFamilyId,
    name: Arc<String>,
    dropped: Arc<AtomicBool>,
}

impl ColumnFamilyHandle {
    /// Creates a new handle. The `dropped` flag is fresh (`false`); the
    /// engine [`super::DbImpl::drop_cf`] path flips it via the shared
    /// `Arc<AtomicBool>` cloned into [`ColumnFamilyData`].
    pub fn new(id: ColumnFamilyId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: Arc::new(name.into()),
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns the column family id.
    pub fn id(&self) -> ColumnFamilyId {
        self.id
    }

    /// Returns the column family name.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Returns `true` if [`super::DbImpl::drop_cf`] has been called on
    /// this CF (or an equivalent handle sharing the same id). Cheap
    /// (single relaxed atomic load); intended for callers that want to
    /// short-circuit work on a dropped CF without paying the engine's
    /// `lookup_cf_by_id` cost.
    pub fn is_dropped(&self) -> bool {
        self.dropped.load(Ordering::Acquire)
    }

    /// Marks the handle as dropped. Used by the engine; external callers
    /// should not call this directly — they should call
    /// [`super::DbImpl::drop_cf`].
    pub(crate) fn mark_dropped(&self) {
        self.dropped.store(true, Ordering::Release);
    }
}

impl PartialEq for ColumnFamilyHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for ColumnFamilyHandle {}

impl std::hash::Hash for ColumnFamilyHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// Descriptor used to create or open a column family.
///
/// Unlike [`CfOptions`] (which only stores a merge operator *name*), this
/// descriptor carries an actual [`MergeOperator`] implementation so the
/// engine can wire it into the read path without a separate registry.
pub struct ColumnFamilyDescriptor {
    name: String,
    options: CfOptions,
    merge_operator: Option<Arc<dyn MergeOperator>>,
    compaction_filter: Option<Arc<dyn CompactionFilter>>,
}

impl ColumnFamilyDescriptor {
    /// Creates a descriptor with defaults.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            options: CfOptions::default(),
            merge_operator: None,
            compaction_filter: None,
        }
    }

    /// Sets the column family options.
    pub fn with_options(mut self, options: CfOptions) -> Self {
        self.options = options;
        self
    }

    /// Attaches a merge operator implementation.
    pub fn with_merge_operator(mut self, op: Arc<dyn MergeOperator>) -> Self {
        // Also record the operator name in the options so the two stay in sync.
        // R47-H3: `name()` now returns String (encodes per-instance config).
        self.options.merge_operator = Some(op.name());
        self.merge_operator = Some(op);
        self
    }

    /// Attaches a compaction filter (e.g. [`crate::TtlCompactionFilter`]).
    pub fn with_compaction_filter(mut self, f: Arc<dyn CompactionFilter>) -> Self {
        self.compaction_filter = Some(f);
        self
    }

    /// Returns the column family name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a reference to the configured options.
    pub fn options(&self) -> &CfOptions {
        &self.options
    }

    /// Returns a clone of the configured merge operator, if any.
    pub fn merge_operator(&self) -> Option<Arc<dyn MergeOperator>> {
        self.merge_operator.clone()
    }

    /// Returns a clone of the configured compaction filter, if any.
    pub fn compaction_filter(&self) -> Option<Arc<dyn CompactionFilter>> {
        self.compaction_filter.clone()
    }
}

impl std::fmt::Debug for ColumnFamilyDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnFamilyDescriptor")
            .field("name", &self.name)
            .field("options", &self.options)
            .field(
                "merge_operator",
                // R47-H3: `name()` returns String; format the Option<String>
                // via Debug so the formatter does not need to borrow a
                // temporary.
                &self.merge_operator.as_ref().map(|op| op.name()),
            )
            .finish()
    }
}

/// Type alias for a shared, internally-locked memtable. Used for both the
/// active memtable and the immutable queue.
///
/// E1: backed by [`ShardedMemTable`], which owns N independent
/// `RwLock<VectorizedMemTable>` shards. Writers hash by key into one shard;
/// readers consult one shard for `get` / merge across shards for `scan` and
/// `to_flush_batches`. Once moved to the immutable queue the memtable is
/// frozen via [`ShardedMemTable::freeze`] (every shard frozen
/// individually) so further writes are rejected at the shard level.
pub type SharedMemTable = Arc<ShardedMemTable>;

/// Mutable, per-column-family runtime state.
///
/// Holds the active memtable, the immutable memtable queue, and a lock-free
/// cached [`SnapshotView`] via [`ArcSwap`]. Read paths call
/// [`ColumnFamilyData::snapshot_view`] to atomically acquire a consistent
/// snapshot.
///
/// # Field-level mutability invariants (R47-M2)
///
/// * `handle`, `options`, `shard_count`: fixed at construction. Never
///   mutated after `new_with_filter_and_shards` returns.
/// * `merge_operator`: **FIXED AT CREATE TIME**. Unlike
///   [`Self::compaction_filter`] (which is post-create swappable via
///   [`Self::set_compaction_filter`]), the merge operator is set once
///   when the CF is created via `DbImpl::create_column_family` and
///   cannot be replaced afterwards. The engine's cross-CF homogeneity
///   check (R45-H1) treats the merge operator's identity as a stable
///   invariant of the CF — making it post-hoc swappable would require
///   re-validating against every other CF AND re-running compaction to
///   regenerate any previously-merged values under the new operator's
///   semantics, neither of which is currently implemented. The
///   `set_compaction_filter` path leaves the merge operator untouched
///   (see `DbImpl::set_compaction_filter`'s probe-descriptor logic).
/// * `compaction_filter`: post-create swappable via
///   [`Self::set_compaction_filter`]. Guarded by an internal `RwLock`.
/// * `active_memtable`, `imm_list`: mutated by writers under per-CF
///   locks; see field-level docs.
/// * `cached_snapshot_view`: lock-free swap via `ArcSwap`.
/// * `flush_mutex`: serializes per-CF flush operations.
pub struct ColumnFamilyData {
    handle: ColumnFamilyHandle,
    options: CfOptions,
    /// **Immutable after construction** (R47-M2). Fixed at CF create
    /// time. There is intentionally no setter — see the type-level
    /// "Field-level mutability invariants" section for rationale.
    merge_operator: Option<Arc<dyn MergeOperator>>,
    /// Optional compaction filter, swappable so consumers can install /
    /// replace it after the CF has been created (e.g. via the new
    /// `frs_cf_set_compaction_filter_ttl` FFI export, which mirrors the
    /// post-`open` configuration model that Flink's
    /// `FlinkCompactionFilterFactory` ultimately needs to drive).
    ///
    /// Reads happen once per compaction-job assembly (NOT per emitted
    /// key — the job captures a snapshot `Arc` and dispatches against it
    /// for the entire job lifetime), so the `RwLock` cost is irrelevant.
    /// `dyn CompactionFilter` is unsized so `ArcSwapOption` (which needs
    /// `Sized` inner) is not an option here.
    compaction_filter: RwLock<Option<Arc<dyn CompactionFilter>>>,
    active_memtable: RwLock<SharedMemTable>,
    imm_list: RwLock<Vec<SharedMemTable>>,
    cached_snapshot_view: ArcSwap<SnapshotView>,
    /// Serializes per-CF flush operations so concurrent callers cannot
    /// flush the same oldest imm twice.
    flush_mutex: Mutex<()>,
    /// Shard count for fresh memtables installed by `swap_active_memtable`.
    /// `0` falls back to [`forst_rs_storage::memtable::DEFAULT_SHARD_COUNT`].
    shard_count: usize,
    /// FRS-RESIDENT-FLUSHED: memtables that have been flushed to L0 SSTs and
    /// are retained in RAM (FIFO, oldest first) so the prefix-scan read path
    /// can serve hot recently-flushed data without an S3 round-trip. Each
    /// entry is `(sst_file_number, memtable)` where `memtable`'s content is
    /// byte-identical (same sequence numbers) to the L0 SST `sst_file_number`.
    /// Total size bounded by [`DEFAULT_RESIDENT_FLUSHED_CAP_BYTES`]; oldest
    /// entries are evicted when over cap. SSTs are immutable and file numbers
    /// are unique per instance, so the (file_number → memtable) binding is
    /// stable for the entry's lifetime, and dropping an entry is always safe
    /// (the data is still on the SST).
    resident_flushed: RwLock<Vec<ResidentEntry>>,
}

/// FRS-RESIDENT-FLUSHED entry: a flushed memtable retained in RAM, tagged with
/// its source SST file number and the SST's key bounds. The bounds let point-get
/// readers skip resident entries that cannot contain the probe key — turning the
/// O(num_resident) linear walk into O(matching-entries) (≈1 for join workloads
/// where each flush holds a disjoint key range). Bounds are inclusive
/// `[min_key, max_key]`, taken verbatim from the SST metadata at enrollment.
#[derive(Clone)]
pub struct ResidentEntry {
    pub file_number: FileNumber,
    pub memtable: SharedMemTable,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

impl ResidentEntry {
    /// True if `key` falls within this entry's inclusive `[min_key, max_key]`
    /// bound. Empty bounds (min_key.is_empty() && max_key.is_empty()) mean
    /// "unknown range" → always considered a candidate (conservative).
    #[inline]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.min_key.is_empty() && self.max_key.is_empty() {
            return true;
        }
        key >= self.min_key.as_slice() && key <= self.max_key.as_slice()
    }
}

impl ColumnFamilyData {
    /// Creates a new column family data object with a fresh active memtable.
    pub fn new(
        handle: ColumnFamilyHandle,
        options: CfOptions,
        merge_operator: Option<Arc<dyn MergeOperator>>,
        initial_snapshot: Arc<SnapshotView>,
    ) -> Self {
        Self::new_with_filter(handle, options, merge_operator, None, initial_snapshot)
    }

    /// Creates a new column family data object attaching a compaction filter.
    ///
    /// Uses [`forst_rs_storage::memtable::DEFAULT_SHARD_COUNT`] for the
    /// active memtable. Engine callers should prefer
    /// [`Self::new_with_filter_and_shards`] so the shard count is driven
    /// by `EngineOptions::memtable_shards`.
    pub fn new_with_filter(
        handle: ColumnFamilyHandle,
        options: CfOptions,
        merge_operator: Option<Arc<dyn MergeOperator>>,
        compaction_filter: Option<Arc<dyn CompactionFilter>>,
        initial_snapshot: Arc<SnapshotView>,
    ) -> Self {
        Self::new_with_filter_and_shards(
            handle,
            options,
            merge_operator,
            compaction_filter,
            initial_snapshot,
            0, // 0 → use DEFAULT_SHARD_COUNT
        )
    }

    /// Creates a new column family data object attaching a compaction
    /// filter and an explicit `shard_count`. `shard_count == 0` is treated
    /// as "use the default shard count" (clamped to `[1, MAX_SHARD_COUNT]`).
    pub fn new_with_filter_and_shards(
        handle: ColumnFamilyHandle,
        options: CfOptions,
        merge_operator: Option<Arc<dyn MergeOperator>>,
        compaction_filter: Option<Arc<dyn CompactionFilter>>,
        initial_snapshot: Arc<SnapshotView>,
        shard_count: usize,
    ) -> Self {
        let memtable = Arc::new(ShardedMemTable::new(shard_count, MemTableConfig::default()));
        Self {
            handle,
            options,
            merge_operator,
            compaction_filter: RwLock::new(compaction_filter),
            active_memtable: RwLock::new(memtable),
            imm_list: RwLock::new(Vec::new()),
            cached_snapshot_view: ArcSwap::new(initial_snapshot),
            flush_mutex: Mutex::new(()),
            shard_count,
            resident_flushed: RwLock::new(Vec::new()),
        }
    }

    /// Returns the handle for this column family.
    pub fn handle(&self) -> &ColumnFamilyHandle {
        &self.handle
    }

    /// Returns `true` once [`super::DbImpl::drop_cf`] has flipped this
    /// CF's `dropped` flag. The flag is shared (single
    /// `Arc<AtomicBool>`) between the data and every cloned
    /// [`ColumnFamilyHandle`], so callers either side observe the same
    /// state.
    pub fn is_dropped(&self) -> bool {
        self.handle.is_dropped()
    }

    /// Marks this CF as dropped (and, by extension, every
    /// [`ColumnFamilyHandle`] cloned off it). Engine-internal; callers
    /// invoke [`super::DbImpl::drop_cf`] instead.
    pub(crate) fn mark_dropped(&self) {
        self.handle.mark_dropped();
    }

    /// Returns the column family options.
    pub fn options(&self) -> &CfOptions {
        &self.options
    }

    /// Returns the configured merge operator, if any.
    pub fn merge_operator(&self) -> Option<&Arc<dyn MergeOperator>> {
        self.merge_operator.as_ref()
    }

    /// Returns a snapshot of the currently installed compaction filter.
    ///
    /// Compaction job assembly calls this every time it builds a job; the
    /// returned `Arc` is then handed to the job and is the snapshot used
    /// for that entire job's lifetime — racing
    /// [`Self::set_compaction_filter`] against an in-flight compaction
    /// will affect subsequent jobs but never the one already running.
    pub fn compaction_filter(&self) -> Option<Arc<dyn CompactionFilter>> {
        self.compaction_filter
            .read()
            .expect("compaction_filter lock poisoned")
            .clone()
    }

    /// Installs (or replaces) the compaction filter on this CF.
    ///
    /// `filter = None` clears the slot back to "no filter" (every entry is
    /// emitted unchanged). The next compaction job built for this CF will
    /// observe the new value; in-flight jobs run to completion against
    /// whatever snapshot they captured.
    pub fn set_compaction_filter(&self, filter: Option<Arc<dyn CompactionFilter>>) {
        *self
            .compaction_filter
            .write()
            .expect("compaction_filter lock poisoned") = filter;
    }

    /// Returns a clone of the active memtable `Arc`. Callers can then acquire
    /// a read or write lock on the returned pointer.
    pub fn active_memtable(&self) -> SharedMemTable {
        self.active_memtable.read().expect("lock poisoned").clone()
    }

    /// Returns a cloned snapshot of the immutable memtable list (oldest first).
    pub fn imm_memtables(&self) -> Vec<SharedMemTable> {
        self.imm_list.read().expect("lock poisoned").clone()
    }

    /// Returns the currently cached snapshot view.
    pub fn snapshot_view(&self) -> Arc<SnapshotView> {
        self.cached_snapshot_view.load_full()
    }

    /// Atomically installs a new cached snapshot view.
    pub fn store_snapshot_view(&self, view: Arc<SnapshotView>) {
        self.cached_snapshot_view.store(view);
    }

    /// Freezes the current active memtable and pushes it onto the immutable
    /// queue, installing a fresh empty memtable in its place. Returns the
    /// frozen memtable (shared `Arc<ShardedMemTable>`).
    pub fn swap_active_memtable(&self) -> SharedMemTable {
        // A-R8-H1: hold the active-memtable write lock across the imm
        // push so the swap is atomic to readers. Pre-fix the lock was
        // dropped (line 404) before pushing `old` onto `imm_list`,
        // leaving a window where:
        //   * cf_data.active_memtable() returns the FRESH (empty) active;
        //   * cf_data.imm_memtables() returns a vec WITHOUT `old`;
        //   * every entry in `old` is invisible to all reads
        //     (get_internal, iter_versions_of, scan, scan_at,
        //     prefix_scan_iter, batch_get) for the duration of the gap.
        // Acquiring imm_list's write lock while still holding
        // active_guard makes the (active swap) + (imm push) atomic
        // from a reader's perspective — readers that observe the new
        // active also observe `old` on imm_list.
        let mut active_guard = self.active_memtable.write().expect("lock poisoned");
        let old = std::mem::replace(
            &mut *active_guard,
            Arc::new(ShardedMemTable::new(
                self.shard_count,
                MemTableConfig::default(),
            )),
        );
        // Freeze the old memtable BEFORE the imm push so readers that
        // pick it up from imm_list immediately observe its frozen
        // state (no shard-level writes can race the freeze).
        old.freeze();
        // Push to imm_list while still holding active_guard.
        self.imm_list
            .write()
            .expect("lock poisoned")
            .push(old.clone());
        drop(active_guard);
        old
    }

    /// Returns the number of immutable memtables currently queued.
    pub fn imm_count(&self) -> usize {
        self.imm_list.read().expect("lock poisoned").len()
    }

    /// FRS-RESIDENT-FLUSHED: returns a cloned snapshot of the resident
    /// flushed-memtable list filtered to those whose source SST is STILL in
    /// the supplied live set. Used by the prefix-scan + point-get paths so
    /// resident entries whose underlying L0 SST has been compacted away (and
    /// thus collapsed/transformed in L1+ output) are NEVER read — that would
    /// surface PRE-compaction values (e.g. uncollapsed merge operands) and
    /// silently break correctness. The live filter is the read-time guard
    /// (background prune still runs after flush for memory hygiene).
    pub fn resident_flushed_visible(
        &self,
        live: &std::collections::HashSet<FileNumber>,
    ) -> (Vec<SharedMemTable>, std::collections::HashSet<FileNumber>) {
        let guard = self.resident_flushed.read().expect("lock poisoned");
        let mut mts = Vec::with_capacity(guard.len());
        let mut shadowed = std::collections::HashSet::with_capacity(guard.len());
        for e in guard.iter() {
            if live.contains(&e.file_number) {
                mts.push(e.memtable.clone());
                shadowed.insert(e.file_number);
            }
        }
        (mts, shadowed)
    }

    /// FRS-RESIDENT-FLUSHED point-get fast path (2026-05-29): like
    /// [`Self::resident_flushed_visible`] but additionally filters to entries
    /// whose `[min_key, max_key]` bound contains `key`. This is the q4/q5/q7/q9
    /// join hot path — without the key filter, every cache-miss point GET walks
    /// ALL resident memtables (up to cap/64MiB = 256 at the 16 GiB cap), doing a
    /// hash probe in each. With disjoint per-flush key ranges, the bound check
    /// skips ~all of them. Returns memtables newest-LAST (caller iterates
    /// `.rev()` for newest-first precedence, unchanged from the prior contract).
    /// `shadowed` still reflects ALL key-matching live entries so the SST layer
    /// correctly skips files whose resident memtable was consulted.
    pub fn resident_flushed_visible_for_key(
        &self,
        live: &std::collections::HashSet<FileNumber>,
        key: &[u8],
    ) -> (Vec<SharedMemTable>, std::collections::HashSet<FileNumber>) {
        let guard = self.resident_flushed.read().expect("lock poisoned");
        let mut mts = Vec::new();
        let mut shadowed = std::collections::HashSet::new();
        for e in guard.iter() {
            if live.contains(&e.file_number) && e.may_contain(key) {
                mts.push(e.memtable.clone());
                shadowed.insert(e.file_number);
            }
        }
        (mts, shadowed)
    }

    /// 2026-05-29 PERF-RESTORE batch-get accessor: returns cloned live-filtered
    /// resident entries (memtable + bounds) so the batched point-get path can
    /// check `ResidentEntry::may_contain(key)` per (entry, key) pair and skip
    /// hash probes for out-of-range keys. Ordered oldest→newest (caller iterates
    /// `.rev()` for newest-first precedence).
    pub fn resident_flushed_visible_entries(
        &self,
        live: &std::collections::HashSet<FileNumber>,
    ) -> Vec<ResidentEntry> {
        self.resident_flushed
            .read()
            .expect("lock poisoned")
            .iter()
            .filter(|e| live.contains(&e.file_number))
            .cloned()
            .collect()
    }

    /// Unfiltered accessor — only safe when the caller already knows every
    /// resident entry's source SST is live (e.g. tests that don't compact).
    /// Production read paths MUST use [`Self::resident_flushed_visible`].
    #[cfg(test)]
    pub fn resident_flushed_memtables(&self) -> Vec<SharedMemTable> {
        self.resident_flushed
            .read()
            .expect("lock poisoned")
            .iter()
            .map(|e| e.memtable.clone())
            .collect()
    }

    /// 2026-05-29 PERF-RESTORE-#4 fast-path predicate: returns true iff the
    /// resident-flushed set is non-empty. Read paths can short-circuit the
    /// `version_set.current()` snapshot + `live_sst_files()` HashSet build
    /// when this returns false. This restores v3.8's straight-through
    /// memtable→imm→SST lookup for the typical case (no resident memtables
    /// retained yet — i.e., before any flush has completed). Atomic load
    /// only — no lock.
    pub fn has_resident_flushed(&self) -> bool {
        // Acquire a brief read lock (cheap rwlock fast-path) — the alternative
        // (an AtomicUsize counter mirroring the Vec length) requires extra
        // bookkeeping in add/prune and is not worth the complexity. The lock
        // is held only for a single empty-check.
        !self.resident_flushed
            .read()
            .expect("lock poisoned")
            .is_empty()
    }

    /// Unfiltered accessor — see [`Self::resident_flushed_memtables`].
    pub fn resident_shadowed_file_numbers(&self) -> std::collections::HashSet<FileNumber> {
        self.resident_flushed
            .read()
            .expect("lock poisoned")
            .iter()
            .map(|e| e.file_number)
            .collect()
    }

    /// FRS-RESIDENT-FLUSHED: enrolls a just-flushed memtable into the resident
    /// read cache tagged with the SST file number it was flushed to, and
    /// evicts oldest entries (FIFO) until the total resident bytes are within
    /// `cap_bytes`. The data is durable on the SST regardless of resident
    /// retention; eviction is therefore always safe (the read path falls back
    /// to the SST in Tier 3 for evicted entries).
    pub fn add_resident_flushed(
        &self,
        file_number: FileNumber,
        memtable: SharedMemTable,
        cap_bytes: usize,
    ) {
        // Back-compat shim (tests + callers without bounds): enroll with empty
        // bounds → may_contain() always true → behaves like the pre-2026-05-29
        // unconditional walk.
        self.add_resident_flushed_with_bounds(
            file_number,
            memtable,
            Vec::new(),
            Vec::new(),
            cap_bytes,
        );
    }

    /// 2026-05-29: enroll with explicit `[min_key, max_key]` bounds (from SST
    /// metadata) so point-get readers can skip this entry when the probe key is
    /// outside the range. See [`ResidentEntry::may_contain`].
    pub fn add_resident_flushed_with_bounds(
        &self,
        file_number: FileNumber,
        memtable: SharedMemTable,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
        cap_bytes: usize,
    ) {
        if cap_bytes == 0 {
            return;
        }
        let mut guard = self.resident_flushed.write().expect("lock poisoned");
        guard.push(ResidentEntry {
            file_number,
            memtable,
            min_key,
            max_key,
        });
        // FIFO eviction: drop oldest while total > cap.
        let mut total: usize = guard.iter().map(|e| e.memtable.memory_usage()).sum();
        while total > cap_bytes && !guard.is_empty() {
            let evicted = guard.remove(0);
            total = total.saturating_sub(evicted.memtable.memory_usage());
        }
    }

    /// FRS-RESIDENT-FLUSHED: drops resident entries whose SST file number is
    /// no longer in the supplied live-file set (typically called after a
    /// version edit that retires SSTs through compaction). Keeps the resident
    /// set from outliving its underlying SSTs and reclaims the RAM. Always
    /// correct: the data still lives on the compaction output SST(s) the
    /// Tier-3 path will scan.
    pub fn prune_resident_flushed(
        &self,
        live_files: &std::collections::HashSet<FileNumber>,
    ) -> usize {
        let mut guard = self.resident_flushed.write().expect("lock poisoned");
        let before = guard.len();
        guard.retain(|e| live_files.contains(&e.file_number));
        before - guard.len()
    }

    /// Removes the oldest immutable memtable from the queue (called after
    /// flush completes).
    pub fn pop_oldest_imm(&self) -> Option<SharedMemTable> {
        let mut guard = self.imm_list.write().expect("lock poisoned");
        if guard.is_empty() {
            None
        } else {
            Some(guard.remove(0))
        }
    }

    /// Acquires the per-CF flush mutex. Held by `DbImpl::flush_cf_data` for
    /// the duration of a flush so concurrent flushes of the same CF do not
    /// duplicate SST output.
    pub fn lock_flush(&self) -> std::sync::MutexGuard<'_, ()> {
        self.flush_mutex.lock().expect("lock poisoned")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_common::CfOptions;
    use forst_rs_storage::merge_operator::ListAppendMergeOperator;

    // --- ColumnFamilyHandle ---

    #[test]
    fn test_handle_exposes_id_and_name() {
        let h = ColumnFamilyHandle::new(ColumnFamilyId(7), "orders");
        assert_eq!(h.id(), ColumnFamilyId(7));
        assert_eq!(h.name(), "orders");
    }

    #[test]
    fn test_handle_clone_is_cheap_and_equal() {
        let h = ColumnFamilyHandle::new(ColumnFamilyId(1), "default");
        let c = h.clone();
        assert_eq!(h, c);
    }

    #[test]
    fn test_handle_equality_is_by_id_only() {
        let a = ColumnFamilyHandle::new(ColumnFamilyId(1), "a");
        let b = ColumnFamilyHandle::new(ColumnFamilyId(1), "b");
        let c = ColumnFamilyHandle::new(ColumnFamilyId(2), "a");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    // Clippy flags interior-mutability on `Arc<AtomicBool>` inside the
    // handle, but the handle's `Hash` / `Eq` are defined over `id` only
    // — the `dropped` flag never participates in hashing. Suppress the
    // lint with an inline allow so the test exercises the real-world
    // usage (Flink stores handles in maps keyed by id).
    #[allow(clippy::mutable_key_type)]
    fn test_handle_hashable() {
        use std::collections::HashMap;
        let h = ColumnFamilyHandle::new(ColumnFamilyId(5), "x");
        let mut map = HashMap::new();
        map.insert(h.clone(), 42);
        assert_eq!(map.get(&h), Some(&42));
    }

    // --- ColumnFamilyDescriptor ---

    #[test]
    fn test_descriptor_defaults() {
        let d = ColumnFamilyDescriptor::new("default");
        assert_eq!(d.name(), "default");
        assert!(d.options().merge_operator.is_none());
        assert!(d.merge_operator().is_none());
    }

    #[test]
    fn test_descriptor_with_merge_operator_sets_name_and_instance() {
        let op: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        let d = ColumnFamilyDescriptor::new("lists").with_merge_operator(op);
        // R47-H3: ListAppendMergeOperator::name() now encodes the delimiter
        // (comma = 0x2C = 44). The recorded options.merge_operator string
        // tracks the identity returned by name().
        assert_eq!(
            d.options().merge_operator.as_deref(),
            Some("ListAppendMergeOperator(delim=44)")
        );
        assert!(d.merge_operator().is_some());
    }

    #[test]
    fn test_descriptor_with_options() {
        let opts = CfOptions {
            ttl_seconds: Some(3600),
            ..CfOptions::default()
        };
        let d = ColumnFamilyDescriptor::new("ttl").with_options(opts);
        assert_eq!(d.options().ttl_seconds, Some(3600));
    }

    #[test]
    fn test_descriptor_debug_includes_merge_op_name() {
        let op: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        let d = ColumnFamilyDescriptor::new("lists").with_merge_operator(op);
        let dbg = format!("{:?}", d);
        assert!(dbg.contains("ListAppendMergeOperator"));
    }

    // --- ColumnFamilyData ---

    fn empty_snapshot() -> Arc<SnapshotView> {
        Arc::new(SnapshotView::empty())
    }

    #[test]
    fn test_cf_data_basic_accessors() {
        let handle = ColumnFamilyHandle::new(ColumnFamilyId(1), "default");
        let data =
            ColumnFamilyData::new(handle.clone(), CfOptions::default(), None, empty_snapshot());
        assert_eq!(data.handle(), &handle);
        assert_eq!(data.imm_count(), 0);
    }

    #[test]
    fn test_cf_data_stores_merge_operator() {
        let handle = ColumnFamilyHandle::new(ColumnFamilyId(1), "default");
        let op: Arc<dyn MergeOperator> = Arc::new(ListAppendMergeOperator::with_comma());
        let data = ColumnFamilyData::new(
            handle,
            CfOptions::default(),
            Some(op.clone()),
            empty_snapshot(),
        );
        assert!(data.merge_operator().is_some());
        // R47-H3: name() encodes the delimiter (comma = 44).
        assert_eq!(
            data.merge_operator().unwrap().name(),
            "ListAppendMergeOperator(delim=44)"
        );
    }

    #[test]
    fn test_cf_data_swap_active_memtable_moves_to_imm() {
        let handle = ColumnFamilyHandle::new(ColumnFamilyId(1), "default");
        let data = ColumnFamilyData::new(handle, CfOptions::default(), None, empty_snapshot());
        assert_eq!(data.imm_count(), 0);
        let frozen = data.swap_active_memtable();
        assert!(frozen.is_frozen());
        assert_eq!(data.imm_count(), 1);
    }

    #[test]
    fn test_cf_data_pop_oldest_imm() {
        let handle = ColumnFamilyHandle::new(ColumnFamilyId(1), "default");
        let data = ColumnFamilyData::new(handle, CfOptions::default(), None, empty_snapshot());
        data.swap_active_memtable();
        data.swap_active_memtable();
        assert_eq!(data.imm_count(), 2);
        let popped = data.pop_oldest_imm();
        assert!(popped.is_some());
        assert_eq!(data.imm_count(), 1);
    }

    #[test]
    fn test_cf_data_resident_flushed_basic_accessors() {
        // FRS-RESIDENT-FLUSHED bookkeeping: add → present → prune by live set
        // → absent. Verifies the invariant the prefix-scan path depends on:
        // `resident_shadowed_file_numbers()` reports exactly the file numbers
        // whose data is currently served from Tier 2 (RAM) — so the SST-skip
        // in Tier 3 is provably safe and correct.
        let handle = ColumnFamilyHandle::new(ColumnFamilyId(1), "default");
        let data = ColumnFamilyData::new(handle, CfOptions::default(), None, empty_snapshot());

        // Start empty.
        assert!(data.resident_flushed_memtables().is_empty());
        assert!(data.resident_shadowed_file_numbers().is_empty());

        // Build two distinct memtables (single-shard so this is hermetic).
        let mt1 = Arc::new(ShardedMemTable::new(1, MemTableConfig::default()));
        let mt2 = Arc::new(ShardedMemTable::new(1, MemTableConfig::default()));
        let fn1 = FileNumber(42);
        let fn2 = FileNumber(43);

        // Enroll both with a generous cap so neither is FIFO-evicted.
        data.add_resident_flushed(fn1, mt1.clone(), 1 << 30);
        data.add_resident_flushed(fn2, mt2.clone(), 1 << 30);
        assert_eq!(data.resident_flushed_memtables().len(), 2);
        let shadow = data.resident_shadowed_file_numbers();
        assert!(shadow.contains(&fn1));
        assert!(shadow.contains(&fn2));

        // Prune against a live set that retains only fn2; the prefix-scan path
        // must stop shadowing fn1 (compaction retired it) — and the resident
        // entry is correctly dropped (its data lives on the compaction output).
        let mut live = std::collections::HashSet::new();
        live.insert(fn2);
        let pruned = data.prune_resident_flushed(&live);
        assert_eq!(pruned, 1);
        assert_eq!(data.resident_flushed_memtables().len(), 1);
        let shadow_after = data.resident_shadowed_file_numbers();
        assert!(!shadow_after.contains(&fn1));
        assert!(shadow_after.contains(&fn2));

        // Cap=0 is a no-op (feature disabled).
        let mt3 = Arc::new(ShardedMemTable::new(1, MemTableConfig::default()));
        data.add_resident_flushed(FileNumber(44), mt3, 0);
        assert_eq!(data.resident_flushed_memtables().len(), 1);
    }

    #[test]
    fn test_cf_data_resident_flushed_fifo_eviction() {
        // FIFO cap eviction: oldest entries are dropped first when the total
        // memtable bytes exceed `cap_bytes`. Dropping is correctness-safe (the
        // data is durable on the SST); this only bounds the read accelerator.
        let handle = ColumnFamilyHandle::new(ColumnFamilyId(1), "default");
        let data = ColumnFamilyData::new(handle, CfOptions::default(), None, empty_snapshot());

        // Build three memtables and write a row into each so memory_usage()>0
        // — otherwise the FIFO loop never evicts (sum stays at 0).
        let new_with_data = |seq: u64| -> SharedMemTable {
            let mt = Arc::new(ShardedMemTable::new(1, MemTableConfig::default()));
            // ShardedMemTable::put_with_seq writes a row of measurable size.
            let key = format!("k{}", seq).into_bytes();
            let val = vec![0u8; 64];
            mt.put_with_seq(&key, Some(&val), 0, seq).unwrap();
            mt
        };
        let m1 = new_with_data(1);
        let m2 = new_with_data(2);
        let m3 = new_with_data(3);

        // Cap small enough that adding the third evicts the first.
        let one_usage = m1.memory_usage();
        let cap = one_usage * 2 + one_usage / 2; // < 3× one_usage so 3 cannot fit
        data.add_resident_flushed(FileNumber(10), m1, cap);
        data.add_resident_flushed(FileNumber(11), m2, cap);
        data.add_resident_flushed(FileNumber(12), m3, cap);

        let shadow = data.resident_shadowed_file_numbers();
        // fn=10 (first) must be evicted; fn=11 and fn=12 retained.
        assert!(!shadow.contains(&FileNumber(10)), "oldest must be evicted");
        assert!(shadow.contains(&FileNumber(11)));
        assert!(shadow.contains(&FileNumber(12)));
    }

    #[test]
    fn test_cf_data_snapshot_store_and_load() {
        let handle = ColumnFamilyHandle::new(ColumnFamilyId(1), "default");
        let data = ColumnFamilyData::new(handle, CfOptions::default(), None, empty_snapshot());
        let view = data.snapshot_view();
        assert_eq!(view.sequence(), 0);

        let new_view = Arc::new(SnapshotView::empty().with_sequence(42));
        data.store_snapshot_view(new_view);
        assert_eq!(data.snapshot_view().sequence(), 42);
    }
}
