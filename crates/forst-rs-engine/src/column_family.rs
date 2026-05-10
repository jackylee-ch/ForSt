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

use std::sync::{Arc, Mutex, RwLock};

use arc_swap::ArcSwap;
use forst_rs_common::{CfOptions, ColumnFamilyId};
use forst_rs_storage::memtable::{MemTableConfig, ShardedMemTable};
use forst_rs_storage::merge_operator::MergeOperator;

use crate::compaction_filter::CompactionFilter;
use crate::snapshot_view::SnapshotView;

/// Lightweight handle to an open column family.
///
/// Contains the numeric id and a shared pointer to the name. Clone is O(1)
/// because `Arc` pointer bumps are cheap. Equality is defined by id only.
#[derive(Clone, Debug)]
pub struct ColumnFamilyHandle {
    id: ColumnFamilyId,
    name: Arc<String>,
}

impl ColumnFamilyHandle {
    /// Creates a new handle.
    pub fn new(id: ColumnFamilyId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: Arc::new(name.into()),
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
        self.options.merge_operator = Some(op.name().to_string());
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
pub struct ColumnFamilyData {
    handle: ColumnFamilyHandle,
    options: CfOptions,
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
        }
    }

    /// Returns the handle for this column family.
    pub fn handle(&self) -> &ColumnFamilyHandle {
        &self.handle
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
        let mut active_guard = self.active_memtable.write().expect("lock poisoned");
        let old = std::mem::replace(
            &mut *active_guard,
            Arc::new(ShardedMemTable::new(
                self.shard_count,
                MemTableConfig::default(),
            )),
        );
        drop(active_guard);

        // Freeze the old memtable. Subsequent shard-level writes return an
        // error; readers go through the sharded read API directly (no outer
        // lock to take).
        old.freeze();

        self.imm_list
            .write()
            .expect("lock poisoned")
            .push(old.clone());
        old
    }

    /// Returns the number of immutable memtables currently queued.
    pub fn imm_count(&self) -> usize {
        self.imm_list.read().expect("lock poisoned").len()
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
        assert_eq!(
            d.options().merge_operator.as_deref(),
            Some("ListAppendMergeOperator")
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
        assert_eq!(
            data.merge_operator().unwrap().name(),
            "ListAppendMergeOperator"
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
