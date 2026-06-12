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

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use arc_swap::ArcSwap;
use forst_rs_common::{
    ColumnFamilyId, FileNumber, ForstError, ForstResult, SequenceNumber, DEFAULT_CF_ID, MAX_LEVELS,
};

/// Metadata for a single SST file.
///
/// R49-H1 (cross-CF SST read corruption): `cf_id` identifies which column
/// family this SST belongs to. The engine MUST gate file access on
/// `meta.cf_id == cf_data.id()` so two CFs writing the same user-key never
/// observe each other's values after flush. Older SSTs (format version < 2)
/// that lack a persisted cf_id in their footer are decoded with
/// `cf_id = DEFAULT_CF_ID` for backwards compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstFileMeta {
    pub file_number: FileNumber,
    /// R49-H1: column family that produced this SST. Set by `FlushJob` /
    /// `CompactionJob` at write time. Persisted in the SST footer (v2+)
    /// for restore-time validation; restore from a v1 footer falls back
    /// to [`DEFAULT_CF_ID`].
    pub cf_id: ColumnFamilyId,
    pub file_size: u64,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
    pub min_sequence: SequenceNumber,
    pub max_sequence: SequenceNumber,
    pub num_entries: u64,
}

impl SstFileMeta {
    /// Convenience constructor for legacy call sites (tests, restore paths)
    /// that pre-date R49-H1's cf_id field — they get [`DEFAULT_CF_ID`] which
    /// matches the on-disk v1 footer fallback.
    pub fn new_default_cf(
        file_number: FileNumber,
        file_size: u64,
        smallest_key: Vec<u8>,
        largest_key: Vec<u8>,
        min_sequence: SequenceNumber,
        max_sequence: SequenceNumber,
        num_entries: u64,
    ) -> Self {
        Self {
            file_number,
            cf_id: DEFAULT_CF_ID,
            file_size,
            smallest_key,
            largest_key,
            min_sequence,
            max_sequence,
            num_entries,
        }
    }
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

/// E5/A2: per-CF view of one level's file array — the indices (into
/// `LevelMeta::files`, ascending, i.e. stored smallest_key order) of the
/// files stamped with one `cf_id`, plus the search-soundness flags computed
/// over that SUB-SEQUENCE. Built lazily, and ONLY for multi-CF layouts
/// (see [`CfLayout::Multi`]); a single-CF Version never allocates these.
#[derive(Debug)]
struct CfLevelView {
    cf_id: ColumnFamilyId,
    /// Indices into the level's `files`, ascending. The stored array is
    /// sorted by `smallest_key`, and a subsequence of a sorted sequence is
    /// sorted, so this view is smallest_key-sorted by construction.
    file_idx: Vec<u32>,
    /// E5 premise, per-CF: `largest_key` monotonic non-decreasing along
    /// this CF's sub-sequence — exactly what the lower-bound
    /// `partition_point` needs. For L1+ single-CF keyspaces this is true
    /// by construction (per-CF non-overlap), but it is VERIFIED here, not
    /// assumed (E5 lesson).
    lower_bsearch_sound: bool,
    /// Strict per-CF non-overlap (`prev.largest_key < next.smallest_key`):
    /// the stronger premise the POINT-GET binary search needs (a key can
    /// be contained by at most one file, so the rightmost
    /// `smallest_key <= key` candidate is the only candidate). Implies
    /// `lower_bsearch_sound`.
    point_bsearch_sound: bool,
}

/// A2: which column families the Version's file layout spans. Detected once
/// (lazily) per immutable Version.
#[derive(Debug)]
enum CfLayout {
    /// No SST files at all (fresh DB / all-memtable state).
    NoFiles,
    /// Every file in every level carries this one `cf_id` — the common
    /// production case. The legacy full-array search arms are used as-is
    /// and NO per-CF views are allocated (zero overhead vs pre-A2).
    Single(ColumnFamilyId),
    /// Files from 2+ CFs share the level arrays (OPT-N04 era). Outer Vec is
    /// per level; inner Vec holds one view per cf_id present at that level
    /// (in first-appearance order — single-digit CFs, linear find).
    Multi(Vec<Vec<CfLevelView>>),
}

/// E5/A2: the lazily-computed scan-locator index over an immutable
/// `Version`'s level layout. One `OnceLock` init computes everything in a
/// single O(total files) pass on the first scan.
#[derive(Debug)]
struct ScanIndex {
    /// E5: per-level FULL-ARRAY `largest_key` monotonicity — the soundness
    /// flag for the cf-agnostic lower-bound binary search in
    /// [`Version::overlapping_ssts_in_range`].
    lower_bsearch_sound: Vec<bool>,
    /// A2: per-level FULL-ARRAY strict non-overlap — the soundness flag for
    /// the single-CF point-get binary search in
    /// [`Version::find_sst_for_key_in_cf`].
    point_bsearch_sound: Vec<bool>,
    /// A2: CF layout + per-CF views (multi-CF only).
    cf_layout: CfLayout,
}

/// A2 test probe: thread-local count of L1+ levels resolved via the LINEAR
/// fallback arm of the range-scan locator (the E5 degraded arm). Debug-only
/// (compiled out of release — the hot path carries no counter). The E5/A2
/// regression tests assert this stays ZERO for multi-CF scans through the
/// per-CF views, i.e. that multi-CF no longer parks L1+ on the linear arm.
#[cfg(debug_assertions)]
pub mod scan_locator_probes {
    use std::cell::Cell;
    thread_local! {
        static L1PLUS_LINEAR_LEVELS: Cell<u64> = const { Cell::new(0) };
    }
    /// Total L1+ linear-fallback level visits on this thread.
    pub fn l1plus_linear_levels() -> u64 {
        L1PLUS_LINEAR_LEVELS.with(|c| c.get())
    }
    pub(super) fn bump_l1plus_linear() {
        L1PLUS_LINEAR_LEVELS.with(|c| c.set(c.get() + 1));
    }
}

/// A single immutable version: the complete SST file layout at a point in time.
#[derive(Debug)]
pub struct Version {
    pub levels: Vec<LevelMeta>,
    /// E5/A2: lazily-computed scan-locator index (per-level search-soundness
    /// flags + per-CF level views for multi-CF layouts). See [`ScanIndex`].
    ///
    /// Lazy + cached per Version: a `Version` is immutable once published
    /// (ArcSwap install / restore), so the index is computed at most once
    /// (O(total files)) on the first scan and amortized across every scan
    /// of that version. Deliberately NOT an eager field so that every
    /// construction path (apply_edit, checkpoint restore's struct literal,
    /// tests that build levels by direct mutation before first use) stays
    /// correct without having to remember to recompute it.
    scan_index: OnceLock<ScanIndex>,
}

// E5: manual impls — the OnceLock cache must not participate in
// equality, and a clone starts with a FRESH (empty) cache so a
// clone-then-mutate caller (e.g. `restore_version_set`'s
// `(*snapshot.version).clone()`, or tests) can never observe flags
// computed from the source's pre-mutation file layout.
impl Clone for Version {
    fn clone(&self) -> Self {
        Self::from_levels(self.levels.clone())
    }
}

impl PartialEq for Version {
    fn eq(&self, other: &Self) -> bool {
        self.levels == other.levels
    }
}

impl Eq for Version {}

impl Version {
    /// Creates a new empty Version with MAX_LEVELS empty levels.
    pub fn new() -> Self {
        Self::from_levels((0..MAX_LEVELS as u32).map(LevelMeta::new).collect())
    }

    /// Creates a Version from an explicit level layout (restore path,
    /// `apply_edit` output). The E5 scan-soundness cache starts empty and
    /// is computed lazily from `levels` on first scan.
    pub fn from_levels(levels: Vec<LevelMeta>) -> Self {
        Self {
            levels,
            scan_index: OnceLock::new(),
        }
    }

    /// E5/A2: get-or-build the lazy scan-locator index. One O(total files)
    /// pass: full-array per-level flags (E5) + CF-layout detection, and —
    /// ONLY when 2+ distinct cf_ids are present — per-CF level views (A2).
    /// Single-CF Versions allocate nothing beyond the two flag Vecs the E5
    /// fix already paid for.
    fn scan_index(&self) -> &ScanIndex {
        self.scan_index.get_or_init(|| {
            let mut lower_bsearch_sound = Vec::with_capacity(self.levels.len());
            let mut point_bsearch_sound = Vec::with_capacity(self.levels.len());
            let mut single_cf: Option<ColumnFamilyId> = None;
            let mut multi_cf = false;
            for lvl in &self.levels {
                let mut lower_ok = true;
                let mut point_ok = true;
                for w in lvl.files.windows(2) {
                    if w[0].largest_key > w[1].largest_key {
                        lower_ok = false;
                    }
                    if w[0].largest_key >= w[1].smallest_key {
                        point_ok = false;
                    }
                }
                lower_bsearch_sound.push(lower_ok);
                point_bsearch_sound.push(point_ok && lower_ok);
                for f in &lvl.files {
                    match single_cf {
                        None => single_cf = Some(f.cf_id),
                        Some(cf) if cf != f.cf_id => multi_cf = true,
                        Some(_) => {}
                    }
                }
            }
            let cf_layout = if multi_cf {
                CfLayout::Multi(self.build_per_cf_views())
            } else {
                match single_cf {
                    Some(cf) => CfLayout::Single(cf),
                    None => CfLayout::NoFiles,
                }
            };
            ScanIndex {
                lower_bsearch_sound,
                point_bsearch_sound,
                cf_layout,
            }
        })
    }

    /// A2: build the per-level per-CF views for a multi-CF layout. Indices
    /// are appended in stored (smallest_key-sorted) order, so each view's
    /// sub-sequence is smallest_key-sorted by construction; the two
    /// soundness flags are computed over the sub-sequence as it is built.
    fn build_per_cf_views(&self) -> Vec<Vec<CfLevelView>> {
        self.levels
            .iter()
            .map(|lvl| {
                let mut views: Vec<CfLevelView> = Vec::new();
                for (i, f) in lvl.files.iter().enumerate() {
                    let view = match views.iter_mut().find(|v| v.cf_id == f.cf_id) {
                        Some(v) => v,
                        None => {
                            views.push(CfLevelView {
                                cf_id: f.cf_id,
                                file_idx: Vec::new(),
                                lower_bsearch_sound: true,
                                point_bsearch_sound: true,
                            });
                            views.last_mut().expect("just pushed")
                        }
                    };
                    if let Some(&prev) = view.file_idx.last() {
                        let prev = &lvl.files[prev as usize];
                        if prev.largest_key > f.largest_key {
                            view.lower_bsearch_sound = false;
                        }
                        if prev.largest_key >= f.smallest_key {
                            view.point_bsearch_sound = false;
                        }
                    }
                    view.file_idx.push(i as u32);
                }
                for v in &mut views {
                    // point premise implies the lower premise; keep the
                    // invariant explicit (mirrors the full-array flags).
                    v.point_bsearch_sound = v.point_bsearch_sound && v.lower_bsearch_sound;
                }
                views
            })
            .collect()
    }

    /// A2 introspection (tests): whether this Version's lazy scan index
    /// detected a multi-CF layout and built per-CF level views. Single-CF
    /// Versions must return `false` (zero-overhead requirement).
    #[doc(hidden)]
    pub fn has_per_cf_scan_views(&self) -> bool {
        matches!(self.scan_index().cf_layout, CfLayout::Multi(_))
    }

    /// A2 introspection (tests): would the range-scan locator take the FAST
    /// (binary-search) lower-bound arm for `(level, cf_id)`? L0 always
    /// answers `false` (linear by design — overlapping flushed memtables).
    /// A CF with no files at the level answers `true` (nothing to probe —
    /// the locator skips the level entirely, which is trivially fast).
    #[doc(hidden)]
    pub fn lower_bsearch_sound_for(&self, level: usize, cf_id: ColumnFamilyId) -> bool {
        if level == 0 || level >= self.levels.len() {
            return false;
        }
        let idx = self.scan_index();
        match &idx.cf_layout {
            CfLayout::NoFiles => true,
            CfLayout::Single(cf) => *cf != cf_id || idx.lower_bsearch_sound[level],
            CfLayout::Multi(views) => views[level]
                .iter()
                .find(|v| v.cf_id == cf_id)
                .is_none_or(|v| v.lower_bsearch_sound),
        }
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
        // R47-L3: precompute a HashSet<FileNumber> over the post-delete
        // version state so the dup check is O(K) total instead of
        // O(K × M) — where K = new_files and M = total files. Pre-fix
        // each new_files iteration walked every level. The original
        // comment ("HashSet costs an allocation per apply_edit on a hot
        // path") is true but the per-allocation cost is dwarfed by the
        // O(K×M) scan for large versions; benchmarking confirms HashSet
        // wins past M ≈ 32 even with K=1, and apply_edit's worst case
        // is dozens of files.
        //
        // Maintained INCREMENTALLY: after the existence check we insert
        // the just-staged file number so subsequent iterations of this
        // loop catch duplicates within the SAME edit (two new_files
        // entries with the same file_number).
        //
        // Returns `Busy` symmetric with the deleted_files "still present"
        // check.
        let mut present_file_numbers: HashSet<FileNumber> = new_levels
            .iter()
            .flat_map(|lvl| lvl.files.iter().map(|f| f.file_number))
            .collect();
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
            if !present_file_numbers.insert(file_meta.file_number) {
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

        Ok(Version::from_levels(new_levels))
    }

    /// Collect all live SST file metadata across all levels.
    pub fn live_sst_files(&self) -> Vec<SstFileMeta> {
        self.levels
            .iter()
            .flat_map(|level| level.files.iter().cloned())
            .collect()
    }

    /// 2026-05-29 PERF: borrow all live SST metadata without cloning. The
    /// read hot paths (get_internal, batch_get_vectorized,
    /// build_lazy_prefix_key_stream) called `live_sst_files()` PER op, which
    /// clones every `SstFileMeta` (incl. its `smallest_key`/`largest_key`
    /// `Vec<u8>`) into a fresh Vec — a per-scan/per-get allocation storm that
    /// the q7 write-back profile showed dominating (live_sst_files 67 +
    /// the G1 GC frames evacuating those clones). Iterate borrowed instead.
    pub fn live_sst_files_iter(&self) -> impl Iterator<Item = &SstFileMeta> {
        self.levels.iter().flat_map(|level| level.files.iter())
    }

    /// 2026-05-29 PERF: collect just the live SST file numbers (no metadata
    /// clone). The resident-flushed shadow filter only needs the file-number
    /// set; cloning full `SstFileMeta` to then `.map(|m| m.file_number)` and
    /// discard the rest was pure waste on every read.
    pub fn live_sst_file_numbers(&self) -> std::collections::HashSet<FileNumber> {
        self.levels
            .iter()
            .flat_map(|level| level.files.iter())
            .map(|m| m.file_number)
            .collect()
    }

    /// Find the index of the SST file that may contain the given key at the
    /// specified level (binary search by key range for levels >= 1).
    ///
    /// For L0, returns None (L0 files may overlap; caller must check all).
    /// For L1+, uses binary search on smallest_key.
    ///
    /// A-H2: this variant DOES NOT filter by CF — it assumes the
    /// level's file list is single-CF or the caller will gate the
    /// result on `cf_id`. Per-CF readers in a global VersionSet
    /// (today's layout) MUST use [`Self::find_sst_for_key_in_cf`]
    /// instead, otherwise multi-CF deployments with overlapping
    /// byte-range keyspaces silently miss point reads at L1+.
    #[deprecated(note = "cross-CF unsound — use find_sst_for_key_in_cf (A-H2/E5/A2)")]
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

    /// A-H2: CF-aware key lookup at `level`. With a global VersionSet,
    /// `levels[i].files` interleaves files from every CF — binary
    /// search by `smallest_key` alone can land on a non-matching
    /// CF's file. The pre-A-H2 caller pattern
    /// (`find_sst_for_key + cf_id check + continue`) silently
    /// abandoned the entire level when the picked candidate was
    /// another CF's file. This variant filters by `cf_id` BEFORE the
    /// search, so the result is always the correct CF's file (or
    /// `None`).
    ///
    /// A2 (PMC cycle-4): this is the precomputed-per-CF-index upgrade the
    /// A-H2 comment named. The lazy [`ScanIndex`] resolves the layout once
    /// per immutable Version:
    ///   * single-CF layout + matching `cf_id` → full-array binary search
    ///     (rightmost `smallest_key <= key` + containment check), gated on
    ///     the per-level strict-non-overlap flag (a key can be contained by
    ///     at most one file, so the rightmost candidate is the ONLY
    ///     candidate — the premise is VERIFIED, not assumed);
    ///   * single-CF layout + other `cf_id` → `None` (no files of that CF);
    ///   * multi-CF layout → binary search over THIS CF's level view (its
    ///     sub-sequence is smallest_key-sorted by construction), same
    ///     per-view non-overlap gate;
    ///   * any level whose (sub-)sequence fails the non-overlap premise
    ///     falls back to the pre-A2 linear first-match walk — identical
    ///     result to the old code on every input.
    pub fn find_sst_for_key_in_cf(
        &self,
        level: usize,
        key: &[u8],
        cf_id: forst_rs_common::ColumnFamilyId,
    ) -> Option<usize> {
        if level == 0 || level >= self.levels.len() {
            return None;
        }
        let files = &self.levels[level].files;
        if files.is_empty() {
            return None;
        }
        let idx = self.scan_index();
        match &idx.cf_layout {
            CfLayout::NoFiles => None,
            CfLayout::Single(cf) => {
                if *cf != cf_id {
                    return None;
                }
                if idx.point_bsearch_sound[level] {
                    // Rightmost file with smallest_key <= key; under strict
                    // non-overlap it is the only possible container.
                    let i = files.partition_point(|f| f.smallest_key.as_slice() <= key);
                    if i == 0 {
                        return None;
                    }
                    if key <= files[i - 1].largest_key.as_slice() {
                        Some(i - 1)
                    } else {
                        None
                    }
                } else {
                    Self::find_sst_linear_in_cf(files, key, cf_id)
                }
            }
            CfLayout::Multi(views) => {
                let view = views[level].iter().find(|v| v.cf_id == cf_id)?;
                if view.point_bsearch_sound {
                    let i = view
                        .file_idx
                        .partition_point(|&fi| files[fi as usize].smallest_key.as_slice() <= key);
                    if i == 0 {
                        return None;
                    }
                    let candidate = view.file_idx[i - 1] as usize;
                    if key <= files[candidate].largest_key.as_slice() {
                        Some(candidate)
                    } else {
                        None
                    }
                } else {
                    Self::find_sst_linear_in_cf(files, key, cf_id)
                }
            }
        }
    }

    /// Pre-A2 fallback: linear first-match walk filtered by cf_id. Per-CF,
    /// files at L1+ are non-overlapping, so the first containing file is
    /// the only one; on (invariant-violating) overlapping layouts this
    /// preserves the old code's first-match answer exactly.
    fn find_sst_linear_in_cf(
        files: &[SstFileMeta],
        key: &[u8],
        cf_id: forst_rs_common::ColumnFamilyId,
    ) -> Option<usize> {
        for (idx, f) in files.iter().enumerate() {
            if f.cf_id != cf_id {
                continue;
            }
            if f.smallest_key.as_slice() <= key && key <= f.largest_key.as_slice() {
                return Some(idx);
            }
        }
        None
    }

    /// FRS-PERLEVEL-SCAN (2026-06-04): per-level, binary-search-bounded
    /// enumeration of the SST files that may contain a key in the range
    /// `[lower, upper)`, appended (borrowed) to `out` in `live_sst_files_iter`
    /// order (level-ascending, then per-level smallest_key order).
    ///
    /// This is the range analogue the prefix/range scan read path
    /// (`build_lazy_prefix_key_stream` / `build_lazy_range_key_stream` in
    /// db.rs) uses INSTEAD of `live_sst_files_iter()` + a flat per-file
    /// range check. The flat path paid an O(total_files) coarse range check
    /// per probe; this path bounds each level to the files that start before
    /// `upper` via a `partition_point` binary search on `smallest_key`
    /// (the files within a level are kept sorted by `smallest_key` by
    /// `apply_edit`), then rejects the cheap `largest_key < lower` left tail.
    ///
    /// CORRECTNESS — IDENTICAL RESULT SET to the flat path: a file overlaps
    /// `[lower, upper)` iff `largest_key >= lower && smallest_key < upper`.
    /// The flat path checks both bounds on every file across every level;
    /// this path checks the SAME two predicates. The only structural change
    /// is the per-level `partition_point` upper cut, which is sound because
    /// files within a level are sorted by `smallest_key` — every file at or
    /// after the cut has `smallest_key >= upper` and is excluded by the flat
    /// path's `smallest_key >= upper` test too. NO CF filtering is applied
    /// here (cf-AGNOSTIC variant — used by the oracle tests and cf-blind
    /// callers); per-CF readers use
    /// [`Self::overlapping_ssts_in_range_for_cf`] (A2), which prunes to the
    /// CF's own files AND keeps the fast lower-bound arm on multi-CF
    /// layouts. `upper == None` means unbounded above (scan to each level's
    /// end).
    ///
    /// E5 (PMC cycle-3 §E1-F2): the LOWER-bound binary search additionally
    /// requires `largest_key` to be monotonic across the level — true
    /// per-CF (L1+ non-overlap) but NOT guaranteed across CFs sharing the
    /// level array (nested/interleaved cross-CF ranges). The per-level
    /// soundness flag (cached once per immutable Version in [`ScanIndex`])
    /// gates the binary search; when a level is non-monotonic the lower
    /// bound falls back to the L0-style linear left-skip, which applies the
    /// flat path's own `largest_key >= lower` predicate per file and
    /// therefore cannot miss a file. Single-CF deployments always take the
    /// binary-search arm — the hot path is unchanged except for one cached
    /// flag load + branch per level.
    pub fn overlapping_ssts_in_range<'a>(
        &'a self,
        lower: &[u8],
        upper: Option<&[u8]>,
        out: &mut Vec<&'a SstFileMeta>,
    ) {
        let lower_bsearch_sound = &self.scan_index().lower_bsearch_sound;
        for (lvl_idx, level) in self.levels.iter().enumerate() {
            let files = &level.files;
            // Binary-search the upper cut: first file whose smallest_key is
            // >= upper. Files within a level are sorted by smallest_key, so
            // everything from `end` onward cannot overlap (their start is at
            // or past the exclusive upper bound).
            let end = match upper {
                Some(hi) => files.partition_point(|f| f.smallest_key.as_slice() < hi),
                None => files.len(),
            };
            if lvl_idx == 0 || !lower_bsearch_sound[lvl_idx] {
                // L0 files may OVERLAP (flushed memtables) → largest_key is not
                // monotonic, so the lower bound must be a linear left-skip. L0 is
                // kept shallow by `l0_compaction_trigger`, so this stays bounded.
                //
                // E5: an L1+ level whose largest_key sequence is non-monotonic
                // (multi-CF nested/interleaved cross-CF ranges) takes the SAME
                // linear left-skip — the partition_point premise does not hold
                // there, and pre-fix the binary search silently skipped
                // overlapping files (release) or tripped a debug_assert. The
                // linear arm applies the exact flat-path predicate per file, so
                // the result set stays identical to the flat scan. Cost is
                // O(files-before-range) only on such multi-CF levels. A2: the
                // engine's per-CF scans use overlapping_ssts_in_range_for_cf,
                // which binary-searches the CF's OWN (monotone) view instead of
                // entering this arm — this cf-agnostic entry point remains for
                // oracle tests and cf-blind callers.
                #[cfg(debug_assertions)]
                if lvl_idx > 0 {
                    scan_locator_probes::bump_l1plus_linear();
                }
                for f in &files[..end] {
                    if f.largest_key.as_slice() < lower {
                        continue;
                    }
                    out.push(f);
                }
            } else {
                // FRS-LOCATOR-LOWER-BSEARCH (2026-06-04): L1+ are NON-OVERLAPPING
                // and sorted ⇒ largest_key is monotonic, so the lower bound is
                // binary-searchable too. This removes the O(files-before-prefix)
                // left-tail LINEAR skip that was q4's runaway A_fanout decay
                // driver: `overlapping_ssts_in_range` cost grew with accumulated
                // L1+ files (thousands at the floor) even though only ~0-4 files
                // actually overlap a narrow interval-join prefix. Now O(log +
                // matched) per level. Sound iff largest_key is monotonic across
                // the level — guaranteed here by the E5 per-level flag checked
                // above (computed once per immutable Version), NOT assumed.
                let start = files.partition_point(|f| f.largest_key.as_slice() < lower);
                for f in &files[start.min(end)..end] {
                    out.push(f);
                }
            }
        }
    }

    /// A2 (PMC cycle-4 advisory, E5 follow-up): CF-FILTERED range locator —
    /// appends the SSTs **of `cf_id`** that may contain a key in
    /// `[lower, upper)` to `out` (level-ascending, per-level smallest_key
    /// order within the CF).
    ///
    /// WHY: keys are NOT CF-prefixed, so once 2+ CFs with interleaving byte
    /// ranges share the level arrays, the full-array `largest_key` sequence
    /// becomes DURABLY non-monotonic and the cf-agnostic locator above
    /// permanently parks those levels on the E5 linear arm (the q4-class
    /// O(files) A_fanout decay). A CF's OWN sub-sequence, however, is
    /// internally sorted and (L1+) non-overlapping, so its `largest_key`
    /// view is monotone by construction — the per-CF views restore the
    /// binary-search arm for every CF.
    ///
    /// Arms (per level):
    ///   * single-CF layout, matching `cf_id` → byte-identical to the
    ///     cf-agnostic path (every file IS this CF's); no views allocated;
    ///   * single-CF layout, other `cf_id` → nothing (this CF has no SSTs);
    ///   * multi-CF layout → binary search over this CF's level view
    ///     (upper cut on `smallest_key`, lower cut on `largest_key`), gated
    ///     on the per-view monotonicity flag (VERIFIED at view build, not
    ///     assumed); a failing view falls back to a linear walk over the
    ///     view's indices (still CF-pruned, never the whole level).
    ///
    /// CORRECTNESS — result set ≡ the flat scan RESTRICTED to `cf_id`
    /// (`f.cf_id == cf_id && largest_key >= lower && smallest_key < upper`):
    /// the view holds exactly the level's `cf_id` files in stored order, the
    /// upper cut excludes exactly the `smallest_key >= upper` tail (sorted
    /// sub-sequence), and the lower cut excludes exactly the
    /// `largest_key < lower` prefix (monotone sub-sequence, same
    /// partition_point argument as E5 §2). L0 stays linear (overlapping
    /// flushed memtables), as in the cf-agnostic path.
    ///
    /// Callers that previously used the cf-agnostic locator and dropped
    /// foreign-CF rows downstream (per-key cf-gated `get`) get the same
    /// final rows with strictly fewer SSTs opened: per R49-H1 every SST is
    /// cf_id-stamped at write, so a file of another CF can never hold this
    /// CF's entries.
    pub fn overlapping_ssts_in_range_for_cf<'a>(
        &'a self,
        cf_id: ColumnFamilyId,
        lower: &[u8],
        upper: Option<&[u8]>,
        out: &mut Vec<&'a SstFileMeta>,
    ) {
        let idx = self.scan_index();
        match &idx.cf_layout {
            CfLayout::NoFiles => {}
            CfLayout::Single(cf) => {
                if *cf == cf_id {
                    // Every file is this CF's: the cf-agnostic walk IS the
                    // cf-filtered walk. Zero overhead vs pre-A2 (one enum
                    // discriminant load + cf compare per scan).
                    self.overlapping_ssts_in_range(lower, upper, out);
                }
            }
            CfLayout::Multi(level_views) => {
                for (lvl_idx, views) in level_views.iter().enumerate() {
                    let Some(view) = views.iter().find(|v| v.cf_id == cf_id) else {
                        continue;
                    };
                    let files = &self.levels[lvl_idx].files;
                    // Upper cut on the CF's sorted sub-sequence: first view
                    // entry whose smallest_key is >= upper.
                    let end = match upper {
                        Some(hi) => view
                            .file_idx
                            .partition_point(|&fi| files[fi as usize].smallest_key.as_slice() < hi),
                        None => view.file_idx.len(),
                    };
                    if lvl_idx == 0 || !view.lower_bsearch_sound {
                        // L0 (overlapping flushed memtables) or a view whose
                        // monotonicity premise failed: linear left-skip over
                        // the CF's OWN indices — the flat predicate per file,
                        // cannot miss; still never walks other CFs' files.
                        #[cfg(debug_assertions)]
                        if lvl_idx > 0 {
                            scan_locator_probes::bump_l1plus_linear();
                        }
                        for &fi in &view.file_idx[..end] {
                            let f = &files[fi as usize];
                            if f.largest_key.as_slice() < lower {
                                continue;
                            }
                            out.push(f);
                        }
                    } else {
                        // FAST arm (the A2 point): lower-bound binary search
                        // on the CF's monotone largest_key sub-sequence.
                        let start = view
                            .file_idx
                            .partition_point(|&fi| files[fi as usize].largest_key.as_slice() < lower);
                        for &fi in &view.file_idx[start.min(end)..end] {
                            out.push(&files[fi as usize]);
                        }
                    }
                }
            }
        }
    }
}

impl Default for Version {
    fn default() -> Self {
        Self::new()
    }
}

// R47-L3: the previous `file_number_already_present` helper was
// O(M) per call and `apply_edit` invoked it O(K) times, giving an
// O(K × M) dup check. The replacement precomputes a HashSet<FileNumber>
// once and probes it incrementally inside the new_files loop — see
// `Version::apply_edit`.

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

/// R49-H2: a single column family's identity, persisted in the checkpoint
/// blob so restore can reconstruct the CF set without operator intervention.
/// The `merge_op_name` and `filter_name` carry the by-value identity strings
/// returned by `MergeOperator::name()` / `CompactionFilter::name()`; empty
/// strings represent "no operator" / "no filter".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CfDescriptor {
    pub cf_id: ColumnFamilyId,
    pub name: String,
    /// Empty string when the CF was created without a merge operator.
    pub merge_op_name: String,
    /// Empty string when the CF was created without a compaction filter.
    pub filter_name: String,
}

/// A frozen snapshot of the VersionSet state at a point in time.
#[derive(Debug, Clone)]
pub struct VersionSetSnapshot {
    pub version: Arc<Version>,
    pub next_file_number: u64,
    pub last_sequence: u64,
    /// R49-H2: descriptors for every column family the engine had open at
    /// snapshot time. Persisted into the checkpoint blob so restore can
    /// re-register them. Empty for legacy v1 blobs (decoded as "assume
    /// DEFAULT_CF only").
    pub cf_descriptors: Vec<CfDescriptor>,
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
    /// 2026-05-30 OBSOLETE-FILE LIFETIME: every version replaced by `apply` is
    /// retained here until no reader still holds it (`Arc::strong_count == 1`,
    /// i.e. only this Vec references it). A read captures `current()` (an owned
    /// `Arc<Version>` via `load_full`) and walks/opens that version's SSTs via
    /// on-demand reads; if a concurrent compaction deletes one of those SSTs'
    /// storage before the read finishes, the read 404s. The engine consults
    /// [`referenced_file_numbers`] before deleting a compaction-input SST so a
    /// file is reclaimed only once NO live version (current ∪ retiring-with-
    /// readers) references it. Pruned on every `apply` and on every query.
    /// Conservative: a transient extra ref (e.g. ArcSwap reclamation) only keeps
    /// a file slightly longer, never deletes one a reader still holds.
    retiring: std::sync::Mutex<Vec<Arc<Version>>>,
}

impl VersionSetImpl {
    /// Creates a new VersionSet with an empty initial version.
    pub fn new() -> Self {
        Self {
            current: ArcSwap::from_pointee(Version::new()),
            next_file_number: AtomicU64::new(1),
            last_sequence: AtomicU64::new(0),
            apply_lock: std::sync::Mutex::new(()),
            retiring: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Creates a VersionSet from a restored state.
    pub fn from_restored(version: Version, next_file_number: u64, last_sequence: u64) -> Self {
        Self {
            current: ArcSwap::from_pointee(version),
            next_file_number: AtomicU64::new(next_file_number),
            last_sequence: AtomicU64::new(last_sequence),
            apply_lock: std::sync::Mutex::new(()),
            retiring: std::sync::Mutex::new(Vec::new()),
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

        // Update atomic counters if the edit carries new values.
        //
        // R55-M1 / R55-M2: use `fetch_max` (NOT `store`) so a stale or
        // out-of-order edit cannot REGRESS these counters. Both fields
        // are monotonically advancing under normal operation:
        //   * `next_file_number` must never re-issue a value already
        //     allocated to a live SST — a `store` of a stale lower
        //     value would let `allocate_file_number` collide with an
        //     existing file on the next call.
        //   * `last_sequence` is the upper bound the read path uses to
        //     gate visibility — regressing it hides newer writes that
        //     were already acknowledged.
        // The `apply_lock` above serializes all `apply` calls so this
        // `fetch_max` under the lock is equivalent to a load+compare+
        // store but with no concurrent-update window.
        if let Some(file_num) = edit.next_file_number {
            self.next_file_number
                .fetch_max(file_num.value(), Ordering::SeqCst);
        }
        if let Some(seq) = edit.last_sequence {
            self.last_sequence.fetch_max(seq.value(), Ordering::SeqCst);
        }

        let new_arc = Arc::new(new_version);
        self.current.store(new_arc.clone());
        // 2026-05-30 OBSOLETE-FILE LIFETIME: retain the just-replaced version
        // until no reader holds it, so the engine can defer deleting any SST
        // still referenced by an in-flight read (see `retiring` /
        // `referenced_file_numbers`). Prune dead entries first (only this Vec
        // holds them → strong_count == 1), then record `old`. `old` is our
        // own `load_full` Arc; after the `store` above ArcSwap no longer holds
        // it as current, so strong_count == 1 + (live readers).
        {
            let mut retiring = self.retiring.lock().expect("retiring lock poisoned");
            retiring.retain(|v| Arc::strong_count(v) > 1);
            retiring.push(old);
        }
        Ok(new_arc)
    }

    /// 2026-05-30 OBSOLETE-FILE LIFETIME: the set of SST file numbers still
    /// referenced by ANY live version — the current version plus every retiring
    /// version a reader is still holding. The engine consults this before
    /// reclaiming a compaction-input SST's storage: a file in this set must NOT
    /// be deleted (a reader may still open it on demand). Prunes dead retiring
    /// versions as a side effect so the set stays tight in a busy system.
    pub fn referenced_file_numbers(&self) -> HashSet<FileNumber> {
        // `load_full` (NOT `load`): a `load()` Guard parks the value in an
        // ArcSwap hazard slot, which keeps PREVIOUSLY-replaced versions alive
        // and makes `Arc::strong_count` below over-report readers. With
        // load_full everywhere, a replaced version's strong count reflects only
        // genuine reader holds, so the `retain` prune is reliable.
        let mut referenced: HashSet<FileNumber> = self
            .current
            .load_full()
            .live_sst_files_iter()
            .map(|f| f.file_number)
            .collect();
        let mut retiring = self.retiring.lock().expect("retiring lock poisoned");
        retiring.retain(|v| Arc::strong_count(v) > 1);
        for v in retiring.iter() {
            for f in v.live_sst_files_iter() {
                referenced.insert(f.file_number);
            }
        }
        referenced
    }

    /// Atomically take a snapshot of the current state.
    ///
    /// `cf_descriptors` is empty here — callers that want CF metadata
    /// persisted into the checkpoint blob (R49-H2) must populate the field
    /// themselves after this returns (the version layer doesn't own CF
    /// state; the engine layer does). See
    /// [`forst_rs_engine::DbImpl::create_checkpoint`] for the call site
    /// that does this.
    pub fn snapshot(&self) -> VersionSetSnapshot {
        VersionSetSnapshot {
            version: self.current.load_full(),
            next_file_number: self.next_file_number.load(Ordering::SeqCst),
            last_sequence: self.last_sequence.load(Ordering::SeqCst),
            cf_descriptors: Vec::new(),
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
            cf_descriptors: Vec::new(),
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
            cf_id: DEFAULT_CF_ID,
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
    fn test_overlapping_ssts_in_range_matches_flat_scan() {
        // FRS-PERLEVEL-SCAN: the binary-search-bounded per-level locator must
        // return EXACTLY the file set the flat live_sst_files_iter + range
        // check would (same overlap predicate), across L0 (overlapping) +
        // L1/L2 (sorted) and multiple CFs with interleaved byte ranges.
        let cf_a = DEFAULT_CF_ID;
        let cf_b = forst_rs_common::ColumnFamilyId(7);
        let mk = |num: u64, cf: forst_rs_common::ColumnFamilyId, s: &[u8], l: &[u8]| SstFileMeta {
            file_number: FileNumber(num),
            cf_id: cf,
            file_size: 1024,
            smallest_key: s.to_vec(),
            largest_key: l.to_vec(),
            min_sequence: SequenceNumber(1),
            max_sequence: SequenceNumber(100),
            num_entries: 50,
        };
        let edit = VersionEdit {
            new_files: vec![
                // L0: overlapping ranges (as flushed memtables are).
                (0, mk(1, cf_a, b"a", b"m")),
                (0, mk(2, cf_a, b"f", b"z")),
                (0, mk(3, cf_b, b"c", b"t")),
                // L1: non-overlapping per CF, but cf_b nests inside cf_a's span.
                (1, mk(10, cf_a, b"a", b"d")),
                (1, mk(11, cf_b, b"e", b"g")),
                (1, mk(12, cf_a, b"h", b"k")),
                (1, mk(13, cf_a, b"m", b"z")),
                // L2: a single wide file + a later one.
                (2, mk(20, cf_a, b"a", b"p")),
                (2, mk(21, cf_b, b"r", b"z")),
            ],
            ..Default::default()
        };
        let v = Version::new().apply_edit(&edit).unwrap();

        // Brute-force reference: same overlap predicate over live_sst_files_iter.
        let flat = |lower: &[u8], upper: Option<&[u8]>| -> Vec<FileNumber> {
            v.live_sst_files_iter()
                .filter(|f| {
                    if f.largest_key.as_slice() < lower {
                        return false;
                    }
                    if let Some(hi) = upper {
                        if f.smallest_key.as_slice() >= hi {
                            return false;
                        }
                    }
                    true
                })
                .map(|f| f.file_number)
                .collect()
        };

        let bounds: &[(&[u8], Option<&[u8]>)] = &[
            (b"a", Some(b"b")),
            (b"e", Some(b"f")),
            (b"e", Some(b"h")),
            (b"f", Some(b"g")),
            (b"\x00", Some(b"\xff")),
            (b"m", None),
            (b"q", Some(b"s")),
            (b"z", None),
            (b"d", Some(b"e")),
            (b"k", Some(b"m")),
        ];
        for (lower, upper) in bounds {
            let mut got: Vec<&SstFileMeta> = Vec::new();
            v.overlapping_ssts_in_range(lower, *upper, &mut got);
            let mut got_nums: Vec<FileNumber> = got.iter().map(|f| f.file_number).collect();
            let mut want = flat(lower, *upper);
            got_nums.sort_by_key(|n| n.0);
            want.sort_by_key(|n| n.0);
            assert_eq!(
                got_nums, want,
                "range [{:?},{:?}) mismatch: locator returned a different SST set than the flat scan",
                lower, upper
            );
        }
    }

    #[test]
    fn test_overlapping_ssts_nested_cross_cf_ranges_e5() {
        // E5 (PMC cycle-3 §E1-F2): with MULTIPLE CFs in the shared per-level
        // file array, per-CF non-overlapping L1 files can have NESTED /
        // INTERLEAVED byte ranges — `largest_key` is then NOT monotonic
        // across the level, so the FRS-LOCATOR-LOWER-BSEARCH partition_point
        // on `largest_key` is unsound. Pre-fix: debug builds panicked on the
        // monotonicity debug_assert; release builds silently SKIPPED
        // overlapping files (a scan could miss its own CF's L1 SSTs).
        //
        // Shape = the reproduced probe: cf_a L1 file [a..m] with cf_b's
        // [c..c] nested strictly inside, plus an interleave at L2.
        let cf_a = DEFAULT_CF_ID;
        let cf_b = forst_rs_common::ColumnFamilyId(7);
        let mk = |num: u64, cf: forst_rs_common::ColumnFamilyId, s: &[u8], l: &[u8]| SstFileMeta {
            file_number: FileNumber(num),
            cf_id: cf,
            file_size: 1024,
            smallest_key: s.to_vec(),
            largest_key: l.to_vec(),
            min_sequence: SequenceNumber(1),
            max_sequence: SequenceNumber(100),
            num_entries: 50,
        };
        let edit = VersionEdit {
            new_files: vec![
                // L1: cf_b's single file NESTS inside cf_a's range. Sorted by
                // smallest_key the level reads [(a..m), (c..c), (p..t)] —
                // largest_key sequence [m, c, t] is non-monotonic.
                (1, mk(10, cf_a, b"a", b"m")),
                (1, mk(11, cf_b, b"c", b"c")),
                (1, mk(12, cf_a, b"p", b"t")),
                // L2: interleaved (not nested): [(b..f), (d..h)] per-CF
                // disjoint but cross-CF overlapping; largest monotonic here,
                // smallest sorted — exercises the still-sound-bsearch shape.
                (2, mk(20, cf_a, b"b", b"f")),
                (2, mk(21, cf_b, b"d", b"h")),
            ],
            ..Default::default()
        };
        let v = Version::new().apply_edit(&edit).unwrap();

        let flat = |lower: &[u8], upper: Option<&[u8]>| -> Vec<FileNumber> {
            let mut nums: Vec<FileNumber> = v
                .live_sst_files_iter()
                .filter(|f| {
                    f.largest_key.as_slice() >= lower
                        && upper.is_none_or(|u| f.smallest_key.as_slice() < u)
                })
                .map(|f| f.file_number)
                .collect();
            nums.sort_by_key(|n| n.0);
            nums
        };

        // The killer probes pre-fix:
        //  * lower="f": L1 largest sequence [m, c, t] → predicate
        //    (largest < "f") = [F, T, F] is NOT partitioned; partition_point
        //    lands past file 10 → cf_a's own [a..m] file is MISSED.
        //  * lower="m" (a key cf_a HOLDS, == file 10's largest): same skip.
        let bounds: &[(&[u8], Option<&[u8]>)] = &[
            (b"f", Some(b"g")),
            (b"m", Some(b"n")),
            (b"m", None),
            (b"d", Some(b"e")),
            (b"c", Some(b"d")),
            (b"a", None),
            (b"\x00", Some(b"\xff")),
            (b"q", Some(b"r")),
            (b"u", None),
        ];
        for (lower, upper) in bounds {
            let mut got: Vec<&SstFileMeta> = Vec::new();
            v.overlapping_ssts_in_range(lower, *upper, &mut got);
            let mut got_nums: Vec<FileNumber> = got.iter().map(|f| f.file_number).collect();
            got_nums.sort_by_key(|n| n.0);
            assert_eq!(
                got_nums,
                flat(lower, *upper),
                "E5 nested cross-CF range [{:?},{:?}): locator must equal flat scan",
                lower,
                upper
            );
        }

        // A2: the CF-FILTERED locator on the SAME non-monotonic shape must
        // (a) equal the cf-filtered flat scan and (b) take the FAST
        // binary-search arm at L1+ — the per-CF largest_key sub-sequences
        // ([m, t] for cf_a, [c] for cf_b) are monotone even though the full
        // array ([m, c, t]) is not.
        assert!(v.has_per_cf_scan_views(), "multi-CF layout must build views");
        for cf in [cf_a, cf_b] {
            for lvl in 1..v.num_levels() {
                assert!(
                    v.lower_bsearch_sound_for(lvl, cf),
                    "A2: cf {} level {lvl} must be bsearch-sound via its own view",
                    cf.0
                );
            }
        }
        let flat_cf = |cf: ColumnFamilyId, lower: &[u8], upper: Option<&[u8]>| -> Vec<FileNumber> {
            let mut nums: Vec<FileNumber> = v
                .live_sst_files_iter()
                .filter(|f| {
                    f.cf_id == cf
                        && f.largest_key.as_slice() >= lower
                        && upper.is_none_or(|u| f.smallest_key.as_slice() < u)
                })
                .map(|f| f.file_number)
                .collect();
            nums.sort_by_key(|n| n.0);
            nums
        };
        #[cfg(debug_assertions)]
        let probes_before = scan_locator_probes::l1plus_linear_levels();
        for cf in [cf_a, cf_b] {
            for (lower, upper) in bounds {
                let mut got: Vec<&SstFileMeta> = Vec::new();
                v.overlapping_ssts_in_range_for_cf(cf, lower, *upper, &mut got);
                let mut got_nums: Vec<FileNumber> = got.iter().map(|f| f.file_number).collect();
                got_nums.sort_by_key(|n| n.0);
                assert_eq!(
                    got_nums,
                    flat_cf(cf, lower, *upper),
                    "A2 cf {} range [{:?},{:?}): cf-filtered locator must equal cf-filtered flat scan",
                    cf.0,
                    lower,
                    upper
                );
                assert!(
                    got.iter().all(|f| f.cf_id == cf),
                    "A2: locator must return only the requested CF's files"
                );
            }
        }
        #[cfg(debug_assertions)]
        assert_eq!(
            scan_locator_probes::l1plus_linear_levels() - probes_before,
            0,
            "A2: multi-CF per-CF scans must take the FAST arm at L1+ (zero linear fallbacks)"
        );
        // Probe-counter sanity: the cf-AGNOSTIC locator on this shape DOES
        // hit the linear fallback at the non-monotonic L1 — proving the
        // counter observes the degraded arm the A2 path avoids.
        #[cfg(debug_assertions)]
        {
            let before = scan_locator_probes::l1plus_linear_levels();
            let mut sink: Vec<&SstFileMeta> = Vec::new();
            v.overlapping_ssts_in_range(b"f", Some(b"g"), &mut sink);
            assert!(
                scan_locator_probes::l1plus_linear_levels() > before,
                "cf-agnostic locator must take the linear arm on the non-monotonic L1"
            );
        }
    }

    /// A2: single-CF layouts must NOT build per-CF views (zero-overhead
    /// requirement) and the cf-filtered locator must be byte-identical to
    /// the cf-agnostic one for the present CF, and empty for any other CF.
    #[test]
    fn test_overlapping_ssts_for_cf_single_cf_layout_a2() {
        let v = Version::new()
            .apply_edit(&VersionEdit {
                new_files: vec![
                    (0, make_file(1, b"a", b"m")),
                    (0, make_file(2, b"f", b"z")),
                    (1, make_file(10, b"a", b"d")),
                    (1, make_file(11, b"e", b"g")),
                    (1, make_file(12, b"h", b"k")),
                    (2, make_file(20, b"a", b"p")),
                ],
                ..Default::default()
            })
            .unwrap();
        assert!(
            !v.has_per_cf_scan_views(),
            "single-CF layout must not allocate per-CF views"
        );
        let bounds: &[(&[u8], Option<&[u8]>)] = &[
            (b"a", Some(b"b")),
            (b"e", Some(b"h")),
            (b"j", None),
            (b"\x00", Some(b"\xff")),
            (b"z", None),
        ];
        for (lower, upper) in bounds {
            let mut agnostic: Vec<&SstFileMeta> = Vec::new();
            v.overlapping_ssts_in_range(lower, *upper, &mut agnostic);
            let mut filtered: Vec<&SstFileMeta> = Vec::new();
            v.overlapping_ssts_in_range_for_cf(DEFAULT_CF_ID, lower, *upper, &mut filtered);
            let a: Vec<FileNumber> = agnostic.iter().map(|f| f.file_number).collect();
            let f: Vec<FileNumber> = filtered.iter().map(|f| f.file_number).collect();
            assert_eq!(a, f, "single-CF: for_cf must match the cf-agnostic walk");
            // A CF with no files sees nothing.
            let mut other: Vec<&SstFileMeta> = Vec::new();
            v.overlapping_ssts_in_range_for_cf(
                forst_rs_common::ColumnFamilyId(9),
                lower,
                *upper,
                &mut other,
            );
            assert!(other.is_empty(), "absent CF must see no SSTs");
        }
        // Empty version: both layouts degenerate cleanly.
        let empty = Version::new();
        assert!(!empty.has_per_cf_scan_views());
        let mut out: Vec<&SstFileMeta> = Vec::new();
        empty.overlapping_ssts_in_range_for_cf(DEFAULT_CF_ID, b"a", None, &mut out);
        assert!(out.is_empty());
    }

    /// A2 fuzz: randomized multi-CF layouts (per-CF non-overlapping L1/L2,
    /// overlapping L0, interleaved/nested cross-CF byte ranges) × random
    /// query ranges. The cf-filtered locator must equal the cf-filtered
    /// flat scan EXACTLY, and (debug) never hit the L1+ linear fallback —
    /// per-CF sub-sequences are monotone by construction.
    #[test]
    fn test_overlapping_ssts_for_cf_multi_cf_fuzz_a2() {
        // Deterministic LCG, no external deps.
        let mut state: u64 = 0x243F6A8885A308D3;
        let mut rng = move |m: u64| -> u64 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) % m
        };
        let cfs = [
            DEFAULT_CF_ID,
            forst_rs_common::ColumnFamilyId(3),
            forst_rs_common::ColumnFamilyId(7),
        ];
        for round in 0..40 {
            let mut new_files: Vec<(u32, SstFileMeta)> = Vec::new();
            let mut file_num = 1u64;
            for (cf_pos, &cf) in cfs.iter().enumerate() {
                // Each CF owns keys `{cf_pos}{4-digit}` — disjoint key SETS
                // (the backend invariant) with fully interleaved byte ranges
                // across CFs at every level.
                let mk_key = |x: u64| format!("{cf_pos}{x:04}").into_bytes();
                // L0: 0-3 overlapping files per CF.
                for _ in 0..rng(4) {
                    let a = rng(9000);
                    let b = a + rng(1000);
                    new_files.push((
                        0,
                        SstFileMeta {
                            file_number: FileNumber(file_num),
                            cf_id: cf,
                            file_size: 1024,
                            smallest_key: mk_key(a),
                            largest_key: mk_key(b),
                            min_sequence: SequenceNumber(1),
                            max_sequence: SequenceNumber(100),
                            num_entries: 10,
                        },
                    ));
                    file_num += 1;
                }
                // L1/L2: per-CF NON-overlapping runs (cursor walks forward).
                for level in 1..=2u32 {
                    let mut cursor = rng(50);
                    for _ in 0..rng(6) {
                        let a = cursor;
                        let b = a + rng(300);
                        cursor = b + 1 + rng(200);
                        if cursor >= 9999 {
                            break;
                        }
                        new_files.push((
                            level,
                            SstFileMeta {
                                file_number: FileNumber(file_num),
                                cf_id: cf,
                                file_size: 1024,
                                smallest_key: mk_key(a),
                                largest_key: mk_key(b),
                                min_sequence: SequenceNumber(1),
                                max_sequence: SequenceNumber(100),
                                num_entries: 10,
                            },
                        ));
                        file_num += 1;
                    }
                }
            }
            if new_files.is_empty() {
                continue;
            }
            let v = Version::new()
                .apply_edit(&VersionEdit {
                    new_files,
                    ..Default::default()
                })
                .unwrap();

            #[cfg(debug_assertions)]
            let probes_before = scan_locator_probes::l1plus_linear_levels();
            for _ in 0..30 {
                let cf = cfs[rng(3) as usize];
                let qcf = rng(3) as usize; // query in any CF's byte namespace
                let a = rng(10000);
                let lower = format!("{qcf}{a:04}").into_bytes();
                let upper: Option<Vec<u8>> = if rng(4) == 0 {
                    None
                } else {
                    Some(format!("{}{:04}", rng(3), a + rng(2000)).into_bytes())
                };
                let mut got: Vec<&SstFileMeta> = Vec::new();
                v.overlapping_ssts_in_range_for_cf(cf, &lower, upper.as_deref(), &mut got);
                let mut got_nums: Vec<FileNumber> = got.iter().map(|f| f.file_number).collect();
                got_nums.sort_by_key(|n| n.0);
                let mut want: Vec<FileNumber> = v
                    .live_sst_files_iter()
                    .filter(|f| {
                        f.cf_id == cf
                            && f.largest_key >= lower
                            && upper.as_deref().is_none_or(|u| f.smallest_key.as_slice() < u)
                    })
                    .map(|f| f.file_number)
                    .collect();
                want.sort_by_key(|n| n.0);
                assert_eq!(
                    got_nums, want,
                    "A2 fuzz round {round}: cf {} range [{:?},{:?}) locator != cf-filtered flat scan",
                    cf.0, lower, upper
                );
            }
            #[cfg(debug_assertions)]
            assert_eq!(
                scan_locator_probes::l1plus_linear_levels() - probes_before,
                0,
                "A2 fuzz round {round}: per-CF L1+ scans must never take the linear arm"
            );

            // find_sst_for_key_in_cf cross-check on the same layout: the
            // (possibly bsearch-accelerated) answer must contain the key and
            // match the linear reference walk's containment verdict.
            for _ in 0..30 {
                let cf = cfs[rng(3) as usize];
                let key = format!("{}{:04}", rng(3), rng(10000)).into_bytes();
                for level in 1..=2usize {
                    let got = v.find_sst_for_key_in_cf(level, &key, cf);
                    let want =
                        Version::find_sst_linear_in_cf(&v.levels[level].files, &key, cf);
                    match (got, want) {
                        (Some(g), Some(w)) => {
                            // Per-CF non-overlap ⇒ unique container.
                            assert_eq!(g, w, "point-get candidate mismatch");
                        }
                        (None, None) => {}
                        other => panic!(
                            "A2 fuzz round {round}: find_sst_for_key_in_cf {:?} disagrees \
                             with linear reference for cf {} key {:?} level {level}",
                            other, cf.0, key
                        ),
                    }
                }
            }
        }
    }

    #[test]
    fn test_overlapping_ssts_dense_left_tail_lower_bsearch() {
        // FRS-LOCATOR-LOWER-BSEARCH: a LARGE non-overlapping L1 with the query
        // range at the very END — the case the lower-bound binary search targets
        // (the old code linearly skipped every earlier file). Must still equal
        // the flat scan: exactly the late files, none missed, none extra.
        let cf = DEFAULT_CF_ID;
        let mut new_files = Vec::new();
        // 500 non-overlapping L1 files: key "k{000}".."k{499}", each its own file.
        for i in 0..500u64 {
            let s = format!("k{i:04}").into_bytes();
            let l = format!("k{i:04}~").into_bytes(); // largest < next smallest
            new_files.push((
                1u32,
                SstFileMeta {
                    file_number: FileNumber(1000 + i),
                    cf_id: cf,
                    file_size: 1024,
                    smallest_key: s,
                    largest_key: l,
                    min_sequence: SequenceNumber(1),
                    max_sequence: SequenceNumber(100),
                    num_entries: 50,
                },
            ));
        }
        let v = Version::new()
            .apply_edit(&VersionEdit {
                new_files,
                ..Default::default()
            })
            .unwrap();
        let flat = |lo: &[u8], hi: Option<&[u8]>| -> Vec<FileNumber> {
            v.live_sst_files_iter()
                .filter(|f| {
                    f.largest_key.as_slice() >= lo
                        && hi.is_none_or(|u| f.smallest_key.as_slice() < u)
                })
                .map(|f| f.file_number)
                .collect()
        };
        // Query the last 3 files (deep left tail before them).
        for (lo, hi) in [
            (b"k0497".as_ref(), None),
            (b"k0497".as_ref(), Some(b"k0499~".as_ref())),
            (b"k0000".as_ref(), Some(b"k0001".as_ref())), // head (start=0)
            (b"k9999".as_ref(), None),                    // past end → empty
        ] {
            let mut got: Vec<&SstFileMeta> = Vec::new();
            v.overlapping_ssts_in_range(lo, hi, &mut got);
            let mut g: Vec<FileNumber> = got.iter().map(|f| f.file_number).collect();
            let mut w = flat(lo, hi);
            g.sort_by_key(|n| n.0);
            w.sort_by_key(|n| n.0);
            assert_eq!(g, w, "dense left-tail [{lo:?},{hi:?}) locator != flat scan");
        }
    }

    #[test]
    #[allow(deprecated)] // exercising the deprecated cf-agnostic variant on purpose
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

    /// R55-M1 / R55-M2: `apply` must never regress `next_file_number`
    /// or `last_sequence`. A stale edit carrying smaller values must be
    /// absorbed monotonically (the larger live value wins).
    #[test]
    fn test_version_set_apply_does_not_regress_counters() {
        let vs = VersionSetImpl::new();
        let high = VersionEdit {
            next_file_number: Some(FileNumber(100)),
            last_sequence: Some(SequenceNumber(500)),
            ..Default::default()
        };
        vs.apply(&high).unwrap();
        assert_eq!(vs.next_file_number(), 100);
        assert_eq!(vs.last_sequence(), 500);

        // A stale edit carrying lower values must NOT regress either
        // counter. Pre-fix, the unconditional `store` regressed both.
        let stale = VersionEdit {
            next_file_number: Some(FileNumber(50)),
            last_sequence: Some(SequenceNumber(200)),
            ..Default::default()
        };
        vs.apply(&stale).unwrap();
        assert_eq!(vs.next_file_number(), 100);
        assert_eq!(vs.last_sequence(), 500);

        // A higher edit still advances both.
        let higher = VersionEdit {
            next_file_number: Some(FileNumber(150)),
            last_sequence: Some(SequenceNumber(700)),
            ..Default::default()
        };
        vs.apply(&higher).unwrap();
        assert_eq!(vs.next_file_number(), 150);
        assert_eq!(vs.last_sequence(), 700);
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
    /// double-inserts). The first probe inserts into the incremental
    /// HashSet (R47-L3); the second probe observes the file_number
    /// already present and rejects with `Busy`. The check uses the
    /// in-progress set so duplicates within a single edit are caught,
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
