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
use forst_rs_common::{FileNumber, ForstError, ForstResult, SequenceNumber, MAX_LEVELS};

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
    /// 1. Validating that every `deleted_files` entry is still present at the
    ///    declared level (R44-L2 / R44-H1 defense-in-depth). When two
    ///    compactions race past the `compaction_mutex` (e.g. a future
    ///    refactor removes it, or a test harness bypasses it), the second
    ///    compaction's edit will reference SST file numbers that the first
    ///    already deleted. Returning `ForstError::Busy` here lets the caller
    ///    discard its staged output SST and retry, instead of installing two
    ///    L1 files with overlapping ranges.
    /// 2. Cloning the current level structure
    /// 3. Removing deleted files
    /// 4. Adding new files
    /// 5. Sorting files within each level by smallest_key
    pub fn apply_edit(&self, edit: &VersionEdit) -> ForstResult<Version> {
        // Stale-edit validation (R44-L2). Every file the edit deletes must
        // still be present at the declared level in `self`. If even one is
        // missing, another writer raced ahead and the inputs we read are no
        // longer the current Version — fail with retry-able `Busy`.
        for &(level, file_number) in &edit.deleted_files {
            let level_idx = level as usize;
            if level_idx >= self.levels.len() {
                return Err(ForstError::busy(format!(
                    "Version::apply_edit: stale edit references out-of-range level {} \
                     (max {}); another writer must have rewritten the version",
                    level,
                    self.levels.len()
                )));
            }
            let still_present = self.levels[level_idx]
                .files
                .iter()
                .any(|f| f.file_number == file_number);
            if !still_present {
                return Err(ForstError::busy(format!(
                    "Version::apply_edit: stale edit deletes file {} at level {} \
                     but it is no longer present in the current version — \
                     another writer's edit already applied; caller should \
                     discard staged output and retry",
                    file_number.value(),
                    level
                )));
            }
        }

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

        // Add new files. R45-M2: mirror the deleted_files stale-edit guard
        // above — an out-of-range `level` is a structural error (the writer
        // staged an edit against a level layout that no longer exists),
        // not a silent drop. Returning `Busy` lets the caller retry with
        // a fresh Version snapshot, matching the deleted_files path.
        //
        // R46-M3: also reject duplicate file numbers. R45-M2 caught the
        // out-of-range-level half of the stale-edit hazard; this half
        // catches the "file_number already present in new_levels" case.
        // Two concurrent writers (flush + compaction) could otherwise
        // both stage an edit that inserts a file with the same number —
        // post-apply the Version would carry two SstFileMeta entries
        // pointing at the same on-disk file (or, worse, two different
        // files with the same number after one is later rewritten),
        // which is a Manifest-consistency bug.
        //
        // The check is `O(new_files * total_files_in_version)` in the
        // worst case; in practice `new_files.len()` is 1–O(low), and the
        // alternative (precomputing a HashSet) costs an allocation per
        // apply_edit on a hot path. Returns `Busy` symmetric with the
        // deleted_files "still present" check.
        for (level, file_meta) in &edit.new_files {
            let level_idx = *level as usize;
            if level_idx >= new_levels.len() {
                return Err(ForstError::busy(format!(
                    "Version::apply_edit: stale edit references out-of-range level {} \
                     for new file {} (max {}); another writer must have rewritten \
                     the version — caller should discard staged output and retry",
                    level,
                    file_meta.file_number.value(),
                    new_levels.len()
                )));
            }
            if file_number_already_present(&new_levels, file_meta.file_number) {
                return Err(ForstError::busy(format!(
                    "Version::apply_edit: stale edit inserts file {} but a file with that \
                     number is already present in the current version — another writer's \
                     edit already applied; caller should discard staged output and retry",
                    file_meta.file_number.value()
                )));
            }
            new_levels[level_idx].files.push(file_meta.clone());
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

/// R46-M3: returns `true` if any level in `new_levels` already contains an
/// SST with `file_number`. Used by [`Version::apply_edit`] to reject
/// duplicate-file-number stale edits (the other half of the R45-M2
/// stale-edit guard family).
fn file_number_already_present(new_levels: &[LevelMeta], file_number: FileNumber) -> bool {
    new_levels
        .iter()
        .any(|lvl| lvl.files.iter().any(|f| f.file_number == file_number))
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

/// Lock-free reads + serialized writes for version management.
///
/// Readers call `current()` to get the latest `Arc<Version>` without locking
/// (ArcSwap-based). Writers call `apply()` which is **serialized via
/// `apply_lock`** so concurrent calls cannot lose each other's updates
/// (Sweep R6 H by Reviewer 1: pre-fix, two concurrent applies could load
/// the same `V0`, compute `V1 = V0+edit_A` and `V2 = V0+edit_B`, then have
/// the second `store` overwrite the first — a classic lost-update race).
/// Reads remain lock-free; only writers contend on the `apply_lock`.
pub struct VersionSetImpl {
    current: ArcSwap<Version>,
    next_file_number: AtomicU64,
    last_sequence: AtomicU64,
    /// Serializes `apply()` so the (load, edit, store) sequence is atomic
    /// across concurrent writers (flush and compaction can both call apply).
    apply_lock: std::sync::Mutex<()>,
}

impl VersionSetImpl {
    /// Creates a new VersionSet with an empty initial version.
    pub fn new() -> Self {
        Self {
            current: ArcSwap::from_pointee(Version::new()),
            next_file_number: AtomicU64::new(1),
            last_sequence: AtomicU64::new(0),
            apply_lock: std::sync::Mutex::new(()),
        }
    }

    /// Creates a VersionSet from a restored state.
    pub fn from_restored(version: Version, next_file_number: u64, last_sequence: u64) -> Self {
        Self {
            current: ArcSwap::from_pointee(version),
            next_file_number: AtomicU64::new(next_file_number),
            last_sequence: AtomicU64::new(last_sequence),
            apply_lock: std::sync::Mutex::new(()),
        }
    }

    /// Get the current version (lock-free read via ArcSwap).
    pub fn current(&self) -> Arc<Version> {
        self.current.load_full()
    }

    /// Atomically apply a VersionEdit and install a new version.
    ///
    /// Serialized via `apply_lock` so concurrent flush/compaction calls do
    /// not lose each other's updates (the (load, edit, store) sequence
    /// must be atomic with respect to other writers).
    ///
    /// Returns the new version.
    pub fn apply(&self, edit: &VersionEdit) -> ForstResult<Arc<Version>> {
        let _guard = self
            .apply_lock
            .lock()
            .expect("VersionSetImpl::apply_lock poisoned");
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

    /// Take a snapshot AND run a closure while holding the apply_lock. This
    /// blocks concurrent writers (flush/compaction) for the duration of the
    /// closure, so callers may safely perform side effects — most notably
    /// pinning live files in [`FileDeletionGuard`] — atomically with the
    /// snapshot read.
    ///
    /// R31-H1: closes the TOCTOU race where a checkpoint reads the live-file
    /// set, then a compaction's `apply` + `delete_file_guarded` runs before
    /// the checkpoint can call `pin_batch`. Holding the apply_lock across the
    /// snapshot + pin ensures compaction's deletion phase cannot complete
    /// before the pin lands. The closure must NOT itself call
    /// `apply`/`snapshot_with_locked_view` (re-entrant lock → deadlock); only
    /// read-only inspection + external pinning are safe.
    pub fn snapshot_with_locked_view<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&VersionSetSnapshot) -> R,
    {
        let _guard = self
            .apply_lock
            .lock()
            .expect("VersionSetImpl::apply_lock poisoned");
        let snap = VersionSetSnapshot {
            version: self.current.load_full(),
            next_file_number: self.next_file_number.load(Ordering::SeqCst),
            last_sequence: self.last_sequence.load(Ordering::SeqCst),
        };
        f(&snap)
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
            new_files: vec![(0, make_file(1, b"a", b"c")), (0, make_file(2, b"d", b"f"))],
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
            new_files: vec![(0, make_file(1, b"a", b"d")), (0, make_file(2, b"c", b"f"))],
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
            new_files: vec![(0, make_file(1, b"a", b"c")), (1, make_file(2, b"d", b"f"))],
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

    /// R44-L2 / R44-H1 defense-in-depth: an edit whose deleted_files set
    /// references a file that is no longer present must be rejected with
    /// retry-able `Busy`. The caller can then discard its staged SST and
    /// re-pick inputs from the now-current Version.
    #[test]
    fn test_apply_edit_rejects_stale_delete() {
        let v = Version::new();
        let edit1 = VersionEdit {
            new_files: vec![(0, make_file(1, b"a", b"c")), (0, make_file(2, b"d", b"f"))],
            ..Default::default()
        };
        let v2 = v.apply_edit(&edit1).unwrap();
        // Apply a compaction-style edit that deletes file 1.
        let edit2 = VersionEdit {
            deleted_files: vec![(0, FileNumber(1))],
            new_files: vec![(1, make_file(3, b"a", b"c"))],
            ..Default::default()
        };
        let v3 = v2.apply_edit(&edit2).unwrap();
        // File 1 is now gone from v3. A second compaction whose inputs were
        // also picked off v2 (i.e. stale relative to v3) will try to delete
        // file 1 again — that MUST fail with Busy.
        let edit3_stale = VersionEdit {
            deleted_files: vec![(0, FileNumber(1))],
            new_files: vec![(1, make_file(4, b"a", b"c"))],
            ..Default::default()
        };
        let err = v3.apply_edit(&edit3_stale).unwrap_err();
        assert!(err.is_busy(), "expected Busy, got {:?}", err);
    }

    /// R44-L2: out-of-range level in deleted_files is also a stale-edit
    /// failure (Busy), not silent success. Pre-fix the loop body would
    /// just skip the delete via the `level_idx < new_levels.len()` guard,
    /// leaving the new_files installed without the corresponding delete —
    /// silent stale-input corruption.
    #[test]
    fn test_apply_edit_rejects_out_of_range_level() {
        let v = Version::new();
        let edit = VersionEdit {
            deleted_files: vec![(MAX_LEVELS as u32 + 5, FileNumber(99))],
            ..Default::default()
        };
        let err = v.apply_edit(&edit).unwrap_err();
        assert!(err.is_busy(), "expected Busy, got {:?}", err);
    }

    /// R45-M2: `new_files` at an out-of-range level used to be silently
    /// dropped — `if level_idx < new_levels.len()` was the only guard,
    /// asymmetric with the `deleted_files` validation. Mirror the
    /// stale-edit treatment so the caller observes a retry-able `Busy`
    /// rather than a silent loss of the file installation.
    #[test]
    fn test_apply_edit_rejects_out_of_range_new_file_level() {
        let v = Version::new();
        let edit = VersionEdit {
            new_files: vec![(MAX_LEVELS as u32 + 5, make_file(99, b"a", b"b"))],
            ..Default::default()
        };
        let err = v.apply_edit(&edit).unwrap_err();
        assert!(err.is_busy(), "expected Busy, got {:?}", err);
    }

    /// R46-M3: `new_files` carrying a file_number that already exists in
    /// the current version is a stale-edit (the other concurrent writer's
    /// apply landed first and installed a file with the same number).
    /// Pre-fix, `apply_edit` would happily push a second entry — Manifest
    /// inconsistency. Returns `Busy` symmetric with the deleted_files
    /// "still present" check.
    #[test]
    fn test_apply_edit_rejects_duplicate_new_file_number() {
        let v = Version::new();
        // Seed with file_number 7 at L0.
        let edit1 = VersionEdit {
            new_files: vec![(0, make_file(7, b"a", b"c"))],
            ..Default::default()
        };
        let v2 = v.apply_edit(&edit1).unwrap();
        assert_eq!(v2.levels[0].files.len(), 1);
        // A second edit that tries to install ANOTHER file_number 7 (at
        // any level) must be rejected as a stale edit.
        let edit2 = VersionEdit {
            new_files: vec![(1, make_file(7, b"d", b"f"))],
            ..Default::default()
        };
        let err = v2.apply_edit(&edit2).unwrap_err();
        assert!(err.is_busy(), "expected Busy, got {:?}", err);
    }

    /// R46-M3: also catches the case where a SINGLE edit stages two
    /// new files with the same file_number (e.g. a writer bug that
    /// double-inserts). The first push lands; the second observes the
    /// just-installed file via `file_number_already_present` and
    /// rejects with `Busy`. The check uses the in-progress
    /// `new_levels` so duplicates within a single edit are caught,
    /// not just duplicates between consecutive applies.
    #[test]
    fn test_apply_edit_rejects_duplicate_within_single_edit() {
        let v = Version::new();
        let edit = VersionEdit {
            new_files: vec![
                (0, make_file(11, b"a", b"c")),
                (0, make_file(11, b"d", b"f")), // duplicate of file 11
            ],
            ..Default::default()
        };
        let err = v.apply_edit(&edit).unwrap_err();
        assert!(err.is_busy(), "expected Busy, got {:?}", err);
    }

    /// R44-L2 via VersionSetImpl::apply: the validation surfaces through
    /// the public apply entrypoint so the engine-side compaction caller
    /// observes the retry-able error and can discard its staged SST.
    #[test]
    fn test_version_set_apply_rejects_stale_compaction_edit() {
        let vs = VersionSetImpl::new();
        // Seed v1 with two L0 files.
        let edit1 = VersionEdit {
            new_files: vec![(0, make_file(1, b"a", b"c")), (0, make_file(2, b"d", b"f"))],
            ..Default::default()
        };
        vs.apply(&edit1).unwrap();
        // Compaction A reads v1, builds edit_a deleting file 1 + adding L1 file 3.
        let edit_a = VersionEdit {
            deleted_files: vec![(0, FileNumber(1))],
            new_files: vec![(1, make_file(3, b"a", b"c"))],
            ..Default::default()
        };
        // Compaction B also reads v1, builds edit_b deleting file 1 + adding L1 file 4.
        let edit_b = VersionEdit {
            deleted_files: vec![(0, FileNumber(1))],
            new_files: vec![(1, make_file(4, b"a", b"c"))],
            ..Default::default()
        };
        // A wins.
        vs.apply(&edit_a).unwrap();
        // B's apply must now fail with Busy — file 1 is no longer at L0.
        let err = vs.apply(&edit_b).unwrap_err();
        assert!(err.is_busy(), "expected Busy, got {:?}", err);
        // And the version state reflects ONLY A's edit (file 4 must not have
        // been inserted by B).
        let v = vs.current();
        let l1_nums: Vec<u64> = v.levels[1]
            .files
            .iter()
            .map(|f| f.file_number.value())
            .collect();
        assert_eq!(l1_nums, vec![3]);
    }

    /// Regression test for Sweep R6 H (Reviewer 1): two concurrent
    /// `apply()` calls must NOT lose either update. Pre-fix, two threads
    /// could both load V0, compute V1=V0+edit_A and V2=V0+edit_B, then
    /// have the second store overwrite the first — a classic lost-update
    /// race. The Mutex around apply() serializes the (load, edit, store)
    /// sequence so both updates land.
    #[test]
    fn test_version_set_concurrent_writers_no_lost_update() {
        use std::thread;
        let vs = Arc::new(VersionSetImpl::new());
        const WRITERS: u64 = 4;
        const FILES_PER_WRITER: u64 = 50;
        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let vs_clone = Arc::clone(&vs);
            handles.push(thread::spawn(move || {
                for i in 0..FILES_PER_WRITER {
                    let file_num = 1 + w * FILES_PER_WRITER + i;
                    let edit = VersionEdit {
                        new_files: vec![(0, make_file(file_num, b"a", b"z"))],
                        ..Default::default()
                    };
                    vs_clone.apply(&edit).unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // All WRITERS × FILES_PER_WRITER updates must be visible. Pre-fix,
        // some updates would be lost to races, so file count would be
        // strictly less than the expected total.
        let v = vs.current();
        assert_eq!(
            v.levels[0].files.len() as u64,
            WRITERS * FILES_PER_WRITER,
            "lost-update race: missing {} files after concurrent applies",
            WRITERS * FILES_PER_WRITER - v.levels[0].files.len() as u64
        );
    }
}
