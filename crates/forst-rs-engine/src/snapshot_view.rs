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

//! Immutable snapshot view (SuperVersion-like). See `2.8_read_write_paths.md` §5.1.
//!
//! A [`SnapshotView`] captures the complete read state of a column family at
//! a point in time — the active memtable, the immutable memtable queue, and
//! the current SST [`Version`]. Readers acquire a snapshot via
//! [`ColumnFamilyData::snapshot_view`](crate::ColumnFamilyData::snapshot_view),
//! which is a lock-free `ArcSwap::load_full` call. Subsequent Flush /
//! Compaction operations install a new snapshot atomically without affecting
//! existing readers.

use std::sync::{Arc, RwLock};

use forst_rs_storage::memtable::VectorizedMemTable;
use forst_rs_storage::version::Version;

/// Immutable read view over a column family at a specific sequence number.
#[derive(Clone)]
pub struct SnapshotView {
    active_memtable: Option<Arc<RwLock<VectorizedMemTable>>>,
    imm_list: Vec<Arc<RwLock<VectorizedMemTable>>>,
    version: Arc<Version>,
    sequence: u64,
}

impl SnapshotView {
    /// Creates a snapshot view with the given components.
    pub fn new(
        active_memtable: Arc<RwLock<VectorizedMemTable>>,
        imm_list: Vec<Arc<RwLock<VectorizedMemTable>>>,
        version: Arc<Version>,
        sequence: u64,
    ) -> Self {
        Self {
            active_memtable: Some(active_memtable),
            imm_list,
            version,
            sequence,
        }
    }

    /// Creates an empty snapshot — no active memtable, no immutables, empty
    /// Version, sequence 0. Useful during engine bootstrap and tests.
    pub fn empty() -> Self {
        Self {
            active_memtable: None,
            imm_list: Vec::new(),
            version: Arc::new(Version::new()),
            sequence: 0,
        }
    }

    /// Returns the active memtable, if any.
    pub fn active_memtable(&self) -> Option<&Arc<RwLock<VectorizedMemTable>>> {
        self.active_memtable.as_ref()
    }

    /// Returns the list of immutable memtables (oldest first, newest last).
    pub fn imm_list(&self) -> &[Arc<RwLock<VectorizedMemTable>>] {
        &self.imm_list
    }

    /// Returns the current SST [`Version`].
    pub fn version(&self) -> &Arc<Version> {
        &self.version
    }

    /// Returns the snapshot sequence number.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Builder-style helper (primarily for tests).
    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = sequence;
        self
    }
}

impl std::fmt::Debug for SnapshotView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotView")
            .field("has_active_memtable", &self.active_memtable.is_some())
            .field("imm_count", &self.imm_list.len())
            .field("num_levels", &self.version.num_levels())
            .field("sequence", &self.sequence)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_snapshot_defaults() {
        let s = SnapshotView::empty();
        assert!(s.active_memtable().is_none());
        assert!(s.imm_list().is_empty());
        assert_eq!(s.sequence(), 0);
    }

    #[test]
    fn test_with_sequence_overrides() {
        let s = SnapshotView::empty().with_sequence(123);
        assert_eq!(s.sequence(), 123);
    }

    #[test]
    fn test_snapshot_is_cheap_to_clone() {
        let s1 = SnapshotView::empty().with_sequence(10);
        let s2 = s1.clone();
        assert_eq!(s1.sequence(), s2.sequence());
    }

    #[test]
    fn test_snapshot_new_exposes_components() {
        let mem = Arc::new(RwLock::new(VectorizedMemTable::with_defaults()));
        let imm = vec![Arc::new(RwLock::new(VectorizedMemTable::with_defaults()))];
        let version = Arc::new(Version::new());
        let s = SnapshotView::new(mem.clone(), imm, version, 99);
        assert!(s.active_memtable().is_some());
        assert_eq!(s.imm_list().len(), 1);
        assert_eq!(s.sequence(), 99);
    }

    #[test]
    fn test_snapshot_debug_shows_state() {
        let s = SnapshotView::empty().with_sequence(5);
        let dbg = format!("{:?}", s);
        assert!(dbg.contains("SnapshotView"));
        assert!(dbg.contains("sequence"));
    }
}
