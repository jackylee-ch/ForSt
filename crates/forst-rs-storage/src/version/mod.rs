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

//! VersionSet -- lock-free version management for the LSM-tree.
//!
//! The version set tracks the current state of all SST files across levels.
//! It uses [`ArcSwap`] for lock-free reads and atomic version switching
//! after Flush/Compaction operations.

pub mod checkpoint;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use forst_rs_common::{FileNumber, ForstResult, SequenceNumber, MAX_LEVELS};

/// Metadata for a single SST file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstFileMeta {
    pub file_number: FileNumber,
    pub file_size: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub min_sequence: SequenceNumber,
    pub max_sequence: SequenceNumber,
    pub num_entries: u64,
}

/// Metadata for a single LSM-tree level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelMeta {
    pub level: u32,
    pub files: Vec<SstFileMeta>,
}

impl LevelMeta {
    /// Creates a new empty level.
    pub fn new(level: u32) -> Self {
        Self {
            level,
            files: Vec::new(),
        }
    }
}

/// A single immutable version: the complete SST file layout at a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    pub levels: Vec<LevelMeta>,
}

impl Version {
    /// Creates a new empty Version with MAX_LEVELS empty levels.
    pub fn new() -> Self {
        let levels = (0..MAX_LEVELS as u32).map(LevelMeta::new).collect();
        Self { levels }
    }

    /// Returns the files at level 0.
    pub fn l0_files(&self) -> &[SstFileMeta] {
        &self.levels[0].files
    }

    /// Returns the number of levels.
    pub fn num_levels(&self) -> usize {
        self.levels.len()
    }

    /// Apply a VersionEdit to produce a new Version.
    ///
    /// This creates a new Version by:
    /// 1. Cloning the current level structure
    /// 2. Removing deleted files
    /// 3. Adding new files
    /// 4. Sorting files within each level by smallest_key
    pub fn apply_edit(&self, edit: &VersionEdit) -> ForstResult<Version> {
        let mut new_levels = self.levels.clone();

        // Remove deleted files
        for &(level, file_number) in &edit.deleted_files {
            let level_idx = level as usize;
            if level_idx < new_levels.len() {
                new_levels[level_idx]
                    .files
                    .retain(|f| f.file_number != file_number);
            }
        }

        // Add new files
        for (level, file_meta) in &edit.new_files {
            let level_idx = *level as usize;
            if level_idx < new_levels.len() {
                new_levels[level_idx].files.push(file_meta.clone());
            }
        }

        // Sort files within each level by smallest_key
        for level_meta in &mut new_levels {
            level_meta
                .files
                .sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
        }

        Ok(Version { levels: new_levels })
    }

    /// Collect all live SST file metadata across all levels.
    pub fn live_sst_files(&self) -> Vec<SstFileMeta> {
        self.levels
            .iter()
            .flat_map(|level| level.files.iter().cloned())
            .collect()
    }

    /// Find the index of the SST file that may contain the given key at the
    /// specified level (binary search by key range for levels >= 1).
    ///
    /// For L0, returns None (L0 files may overlap; caller must check all).
    /// For L1+, uses binary search on smallest_key.
    pub fn find_sst_for_key(&self, level: usize, key: &[u8]) -> Option<usize> {
        if level == 0 || level >= self.levels.len() {
            return None;
        }
        let files = &self.levels[level].files;
        if files.is_empty() {
            return None;
        }
        // Binary search: find rightmost file where smallest_key <= key
        let idx = files.partition_point(|f| f.smallest_key.as_slice() <= key);
        if idx == 0 {
            // Key is before all files at this level
            return None;
        }
        let candidate = idx - 1;
        // Check if key is within this file's range
        if key <= files[candidate].largest_key.as_slice() {
            Some(candidate)
        } else {
            None
        }
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::new()
    }
}

/// Description of a version change (Flush/Compaction result).
#[derive(Debug, Clone, Default)]
pub struct VersionEdit {
    /// New SST files to add: (level, file_meta).
    pub new_files: Vec<(u32, SstFileMeta)>,
    /// SST files to remove: (level, file_number).
    pub deleted_files: Vec<(u32, FileNumber)>,
    /// Updated next file number (if changed).
    pub next_file_number: Option<FileNumber>,
    /// Updated last sequence number (if changed).
    pub last_sequence: Option<SequenceNumber>,
}

/// A frozen snapshot of the VersionSet state at a point in time.
#[derive(Debug, Clone)]
pub struct VersionSetSnapshot {
    pub version: Arc<Version>,
    pub next_file_number: u64,
    pub last_sequence: u64,
}

/// Lock-free version management using ArcSwap.
///
/// Readers call `current()` to get the latest `Arc<Version>` without locking.
/// Writers call `apply()` to atomically install a new version.
pub struct VersionSetImpl {
    current: ArcSwap<Version>,
    next_file_number: AtomicU64,
    last_sequence: AtomicU64,
}

impl VersionSetImpl {
    /// Creates a new VersionSet with an empty initial version.
    pub fn new() -> Self {
        Self {
            current: ArcSwap::from_pointee(Version::new()),
            next_file_number: AtomicU64::new(1),
            last_sequence: AtomicU64::new(0),
        }
    }

    /// Creates a VersionSet from a restored state.
    pub fn from_restored(version: Version, next_file_number: u64, last_sequence: u64) -> Self {
        Self {
            current: ArcSwap::from_pointee(version),
            next_file_number: AtomicU64::new(next_file_number),
            last_sequence: AtomicU64::new(last_sequence),
        }
    }

    /// Get the current version (lock-free read via ArcSwap).
    pub fn current(&self) -> Arc<Version> {
        self.current.load_full()
    }

    /// Atomically apply a VersionEdit and install a new version.
    ///
    /// Returns the new version.
    pub fn apply(&self, edit: &VersionEdit) -> ForstResult<Arc<Version>> {
        let old = self.current.load_full();
        let new_version = old.apply_edit(edit)?;

        // Update atomic counters if the edit carries new values
        if let Some(file_num) = edit.next_file_number {
            self.next_file_number
                .store(file_num.value(), Ordering::SeqCst);
        }
        if let Some(seq) = edit.last_sequence {
            self.last_sequence.store(seq.value(), Ordering::SeqCst);
        }

        let new_arc = Arc::new(new_version);
        self.current.store(new_arc.clone());
        Ok(new_arc)
    }

    /// Atomically take a snapshot of the current state.
    pub fn snapshot(&self) -> VersionSetSnapshot {
        VersionSetSnapshot {
            version: self.current.load_full(),
            next_file_number: self.next_file_number.load(Ordering::SeqCst),
            last_sequence: self.last_sequence.load(Ordering::SeqCst),
        }
    }

    /// Allocate a new file number (atomic increment).
    pub fn allocate_file_number(&self) -> FileNumber {
        FileNumber(self.next_file_number.fetch_add(1, Ordering::SeqCst))
    }

    /// Get the current next file number.
    pub fn next_file_number(&self) -> u64 {
        self.next_file_number.load(Ordering::SeqCst)
    }

    /// Get the current last sequence number.
    pub fn last_sequence(&self) -> u64 {
        self.last_sequence.load(Ordering::SeqCst)
    }

    /// Get all live SST files from the current version.
    pub fn live_sst_files(&self) -> Vec<SstFileMeta> {
        self.current().live_sst_files()
    }
}

impl Default for VersionSetImpl {
    fn default() -> Self {
        Self::new()
    }
}

// Tests
#[cfg(test)]
mod tests {
    use super::*;

    fn make_file(num: u64, smallest: &[u8], largest: &[u8]) -> SstFileMeta {
        SstFileMeta {
            file_number: FileNumber(num),
            file_size: 1024,
            smallest_key: smallest.to_vec(),
            largest_key: largest.to_vec(),
            min_sequence: SequenceNumber(1),
            max_sequence: SequenceNumber(100),
            num_entries: 50,
        }
    }

    #[test]
    fn test_version_new_has_max_levels() {
        let v = Version::new();
        assert_eq!(v.num_levels(), MAX_LEVELS);
        for level in &v.levels {
            assert!(level.files.is_empty());
        }
    }

    #[test]
    fn test_version_apply_edit_add_files() {
        let v = Version::new();
        let edit = VersionEdit {
            new_files: vec![
                (0, make_file(1, b"a", b"c")),
                (0, make_file(2, b"d", b"f")),
                (1, make_file(3, b"a", b"z")),
            ],
            ..Default::default()
        };
        let v2 = v.apply_edit(&edit).unwrap();
        assert_eq!(v2.levels[0].files.len(), 2);
        assert_eq!(v2.levels[1].files.len(), 1);
    }

    #[test]
    fn test_version_apply_edit_delete_files() {
        let v = Version::new();
        // First add files
        let edit1 = VersionEdit {
            new_files: vec![
                (0, make_file(1, b"a", b"c")),
                (0, make_file(2, b"d", b"f")),
            ],
            ..Default::default()
        };
        let v2 = v.apply_edit(&edit1).unwrap();
        assert_eq!(v2.levels[0].files.len(), 2);

        // Now delete one
        let edit2 = VersionEdit {
            deleted_files: vec![(0, FileNumber(1))],
            ..Default::default()
        };
        let v3 = v2.apply_edit(&edit2).unwrap();
        assert_eq!(v3.levels[0].files.len(), 1);
        assert_eq!(v3.levels[0].files[0].file_number, FileNumber(2));
    }

    #[test]
    fn test_version_apply_edit_compaction() {
        let v = Version::new();
        // Add L0 files
        let edit1 = VersionEdit {
            new_files: vec![
                (0, make_file(1, b"a", b"d")),
                (0, make_file(2, b"c", b"f")),
            ],
            ..Default::default()
        };
        let v2 = v.apply_edit(&edit1).unwrap();

        // Simulated compaction: delete L0 files, add L1 file
        let edit2 = VersionEdit {
            deleted_files: vec![(0, FileNumber(1)), (0, FileNumber(2))],
            new_files: vec![(1, make_file(3, b"a", b"f"))],
            ..Default::default()
        };
        let v3 = v2.apply_edit(&edit2).unwrap();
        assert_eq!(v3.levels[0].files.len(), 0);
        assert_eq!(v3.levels[1].files.len(), 1);
    }

    #[test]
    fn test_version_files_sorted_by_smallest_key() {
        let v = Version::new();
        // Add files in reverse order
        let edit = VersionEdit {
            new_files: vec![
                (1, make_file(1, b"z", b"zz")),
                (1, make_file(2, b"a", b"az")),
                (1, make_file(3, b"m", b"mz")),
            ],
            ..Default::default()
        };
        let v2 = v.apply_edit(&edit).unwrap();
        let files = &v2.levels[1].files;
        assert_eq!(files[0].file_number, FileNumber(2)); // "a"
        assert_eq!(files[1].file_number, FileNumber(3)); // "m"
        assert_eq!(files[2].file_number, FileNumber(1)); // "z"
    }

    #[test]
    fn test_version_find_sst_for_key() {
        let v = Version::new();
        let edit = VersionEdit {
            new_files: vec![
                (1, make_file(1, b"a", b"d")),
                (1, make_file(2, b"f", b"k")),
                (1, make_file(3, b"m", b"z")),
            ],
            ..Default::default()
        };
        let v2 = v.apply_edit(&edit).unwrap();

        // Key in first file
        assert_eq!(v2.find_sst_for_key(1, b"b"), Some(0));
        // Key in second file
        assert_eq!(v2.find_sst_for_key(1, b"g"), Some(1));
        // Key in third file
        assert_eq!(v2.find_sst_for_key(1, b"p"), Some(2));
        // Key between files (gap)
        assert_eq!(v2.find_sst_for_key(1, b"e"), None);
        // Key before all files
        assert_eq!(v2.find_sst_for_key(1, b"\x00"), None);
        // L0 always returns None
        assert_eq!(v2.find_sst_for_key(0, b"a"), None);
    }

    #[test]
    fn test_version_live_sst_files() {
        let v = Version::new();
        let edit = VersionEdit {
            new_files: vec![
                (0, make_file(1, b"a", b"c")),
                (1, make_file(2, b"d", b"f")),
            ],
            ..Default::default()
        };
        let v2 = v.apply_edit(&edit).unwrap();
        let live = v2.live_sst_files();
        assert_eq!(live.len(), 2);
    }

    #[test]
    fn test_version_set_new() {
        let vs = VersionSetImpl::new();
        assert_eq!(vs.next_file_number(), 1);
        assert_eq!(vs.last_sequence(), 0);
        assert_eq!(vs.current().levels.len(), MAX_LEVELS);
    }

    #[test]
    fn test_version_set_apply() {
        let vs = VersionSetImpl::new();
        let edit = VersionEdit {
            new_files: vec![(0, make_file(1, b"a", b"z"))],
            next_file_number: Some(FileNumber(2)),
            last_sequence: Some(SequenceNumber(100)),
            ..Default::default()
        };
        let v = vs.apply(&edit).unwrap();
        assert_eq!(v.levels[0].files.len(), 1);
        assert_eq!(vs.next_file_number(), 2);
        assert_eq!(vs.last_sequence(), 100);
    }

    #[test]
    fn test_version_set_snapshot() {
        let vs = VersionSetImpl::new();
        let edit = VersionEdit {
            new_files: vec![(0, make_file(1, b"a", b"z"))],
            next_file_number: Some(FileNumber(5)),
            last_sequence: Some(SequenceNumber(42)),
            ..Default::default()
        };
        vs.apply(&edit).unwrap();

        let snap = vs.snapshot();
        assert_eq!(snap.next_file_number, 5);
        assert_eq!(snap.last_sequence, 42);
        assert_eq!(snap.version.levels[0].files.len(), 1);
    }

    #[test]
    fn test_version_set_snapshot_isolation() {
        let vs = VersionSetImpl::new();
        let edit1 = VersionEdit {
            new_files: vec![(0, make_file(1, b"a", b"z"))],
            ..Default::default()
        };
        vs.apply(&edit1).unwrap();

        // Take snapshot
        let snap = vs.snapshot();
        assert_eq!(snap.version.levels[0].files.len(), 1);

        // Apply another edit -- snapshot should not be affected
        let edit2 = VersionEdit {
            new_files: vec![(0, make_file(2, b"b", b"y"))],
            ..Default::default()
        };
        vs.apply(&edit2).unwrap();

        // Snapshot still sees old version
        assert_eq!(snap.version.levels[0].files.len(), 1);
        // Current version sees new file
        assert_eq!(vs.current().levels[0].files.len(), 2);
    }

    #[test]
    fn test_version_set_allocate_file_number() {
        let vs = VersionSetImpl::new();
        assert_eq!(vs.allocate_file_number(), FileNumber(1));
        assert_eq!(vs.allocate_file_number(), FileNumber(2));
        assert_eq!(vs.allocate_file_number(), FileNumber(3));
        assert_eq!(vs.next_file_number(), 4);
    }

    #[test]
    fn test_version_set_concurrent_reads() {
        use std::thread;

        let vs = Arc::new(VersionSetImpl::new());
        let edit = VersionEdit {
            new_files: vec![(0, make_file(1, b"a", b"z"))],
            ..Default::default()
        };
        vs.apply(&edit).unwrap();

        // 10 concurrent readers + 1 writer
        let mut handles = Vec::new();
        for _ in 0..10 {
            let vs_clone = Arc::clone(&vs);
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    let v = vs_clone.current();
                    assert!(v.num_levels() == MAX_LEVELS);
                }
            }));
        }

        // Writer thread
        let vs_writer = Arc::clone(&vs);
        handles.push(thread::spawn(move || {
            for i in 2u64..102 {
                let edit = VersionEdit {
                    new_files: vec![(0, make_file(i, b"a", b"z"))],
                    ..Default::default()
                };
                vs_writer.apply(&edit).unwrap();
            }
        }));

        for h in handles {
            h.join().unwrap();
        }

        // After writer completes, current version should have many L0 files
        let v = vs.current();
        assert!(v.levels[0].files.len() >= 100);
    }

    #[test]
    fn test_version_set_from_restored() {
        let mut v = Version::new();
        v.levels[0].files.push(make_file(5, b"a", b"z"));
        let vs = VersionSetImpl::from_restored(v, 10, 500);
        assert_eq!(vs.next_file_number(), 10);
        assert_eq!(vs.last_sequence(), 500);
        assert_eq!(vs.current().levels[0].files.len(), 1);
    }
}
