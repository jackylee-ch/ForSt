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

//! Compaction *picking* policy (paper §2,
//! `2026-06-13-writeamp-fundamental-reusable-design.md`).
//!
//! This is the **reusable** half of the compaction seam. Execution is already
//! one strategy / two call sites ([`crate::compaction_executor`]); picking was
//! inlined and partially duplicated across `compact_l0_for_cf` and
//! `compact_level_for_cf` in `db.rs`. A [`CompactionPolicy`] is the single
//! authoritative *picker*: given an immutable [`Version`] and a CF it decides
//! WHAT to compact, returning a portable, executor-agnostic
//! [`CompactionDecision`]. The caller (`db.rs`) keeps everything that touches
//! the engine's mutable, monotonic authority — file-number allocation, reader
//! open, the snapshot horizon, the [`crate::compaction::CompactionJob`] build,
//! the executor call, and the `version_set.apply` install — so the same plan
//! runs byte-identically on the LOCAL and the REMOTE (offloaded) executor and
//! the byte-identical falsifier (`remote_compaction_it.rs`) gates any drift.
//!
//! REUSABILITY BY CONSTRUCTION: there is exactly one [`CompactionPolicy`]
//! implementation, [`SortedRunPolicy`], consumed by FOUR call sites — L0-local,
//! Ln-local, and (via the `CompactionPlan` provenance carried in
//! `CompactionJobDescriptor`) L0-remote-describe and Ln-remote-describe. The
//! picking is pure metadata over the `Version` (file metas, key bounds, cached
//! footer tombstone counts) and does ZERO I/O, so it costs the same whether the
//! SSTs live on NVMe or S3 and runs on the primary regardless of where bytes
//! live.

use forst_rs_common::ColumnFamilyId;
use forst_rs_storage::version::{SstFileMeta, Version};

/// Read-only inputs the policy needs that are NOT on the `Version`: level
/// geometry, the dynamic-levels / trivial-move flag state, and a tombstone
/// lookup over cached reader footers. Passed by ref so the policy does ZERO
/// I/O and holds no engine locks.
pub struct PolicyCtx<'a> {
    pub num_levels: usize,
    pub max_bytes_for_level_base: u64,
    pub max_bytes_for_level_multiplier: f64,
    pub dynamic_levels: bool,
    pub trivial_move: bool,
    /// `true` iff this CF carries a compaction filter (a filter forbids the
    /// metadata-only trivial-move arm — rows must be inspected).
    pub compaction_filter_active: bool,
    /// Tombstone count for a file (footer-v4 field via the cached reader; 0
    /// when unknown — conservative, no compensation). Boxed closure so the
    /// policy stays I/O-free and engine-state-agnostic.
    pub tombstones: &'a dyn Fn(forst_rs_common::FileNumber) -> u64,
}

/// The executor-agnostic merge description — everything the seam computes
/// BEFORE the `CompactionJob` is built: the input file identities + their
/// levels, the output level, and the bottommost flag. File-number allocation,
/// reader-open, and the snapshot horizon stay caller-side (the primary's
/// monotonic authority), exactly as the remote descriptor requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPlan {
    /// `(level, file)` for every input. For an L0 rollup the L0 inputs come
    /// first (level 0), then the output-level overlap files; for an Ln descent
    /// the single source file comes first, then the next-level overlap files.
    pub inputs: Vec<(u32, SstFileMeta)>,
    pub output_level: u32,
    pub is_bottommost: bool,
}

/// The picked compaction: either a metadata-only re-level (WA-V3 trivial move,
/// no merge / no executor) or a real merge to run through the executor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompactionDecision {
    /// Metadata-only re-level: move `files` from `from_level` to `to_level`
    /// with ZERO rewrite (the caller emits a delete-at-`from`/add-at-`to`
    /// `VersionEdit` directly — no `CompactionJob`).
    TrivialMove {
        files: Vec<SstFileMeta>,
        from_level: u32,
        to_level: u32,
    },
    /// A real merge the caller turns into a `CompactionJob` and hands to the
    /// `CompactionMergeExecutor`.
    Merge(CompactionPlan),
}

/// Which picking strategy is in force (diagnostics / asserts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPolicyKind {
    SortedRun,
}

/// Strategy for *picking* a compaction. One implementation
/// ([`SortedRunPolicy`]); four call sites all defer to it.
pub trait CompactionPolicy: Send + Sync {
    /// Pick the next L0→base rollup for `cf`, or `None` if L0 is empty.
    /// `output_level` is the CF's resting base level (caller-computed under the
    /// compaction gate so it is stable for the lifetime of the pick).
    fn pick_l0_rollup(
        &self,
        v: &Version,
        cf: ColumnFamilyId,
        output_level: u32,
        ctx: &PolicyCtx,
    ) -> Option<CompactionDecision>;

    /// Pick the next Ln→Ln+1 descent for `cf` from `level`, or `None`.
    fn pick_level_descent(
        &self,
        v: &Version,
        cf: ColumnFamilyId,
        level: u32,
        ctx: &PolicyCtx,
    ) -> Option<CompactionDecision>;

    fn kind(&self) -> CompactionPolicyKind;
}

/// THE one implementation. Owns the M1 clean-cut, M4 dynamic-level /
/// min-overlap-ratio / tombstone-compensation, and WA-V3 trivial-move picking
/// that was inlined in `db.rs`. No per-query branches — one discipline.
#[derive(Debug, Default, Clone, Copy)]
pub struct SortedRunPolicy;

impl CompactionPolicy for SortedRunPolicy {
    fn pick_l0_rollup(
        &self,
        v: &Version,
        cf: ColumnFamilyId,
        output_level: u32,
        ctx: &PolicyCtx,
    ) -> Option<CompactionDecision> {
        // R49-H1: only this CF's L0 files (and overlap into this CF's output
        // level), never another CF's data.
        let l0_files: Vec<SstFileMeta> = v
            .l0_files()
            .iter()
            .filter(|f| f.cf_id == cf)
            .cloned()
            .collect();
        if l0_files.is_empty() {
            return None;
        }
        // FRS-M1-OVERLAP-SCOPED: only the output-level files whose key range
        // overlaps the L0 union, expanded to a clean cut — not the whole level.
        let cf_out: Vec<SstFileMeta> = v.levels[output_level as usize]
            .files
            .iter()
            .filter(|f| f.cf_id == cf)
            .cloned()
            .collect();
        let out_overlap_files: Vec<SstFileMeta> = overlap_scoped_clean_cut(&l0_files, cf_out);

        // FRS-WA-V3 (link-compaction): a mutually key-disjoint L0 set with no
        // output-level overlap and no compaction filter is a metadata-only
        // re-level — zero rewrite, zero upload.
        if ctx.trivial_move
            && !ctx.compaction_filter_active
            && out_overlap_files.is_empty()
            && sst_metas_mutually_disjoint(&l0_files)
        {
            return Some(CompactionDecision::TrivialMove {
                files: l0_files,
                from_level: 0,
                to_level: output_level,
            });
        }

        // is_bottommost iff every level OTHER than the output level is empty
        // (so delete tombstones can be dropped — see the rationale at the L0
        // call site in db.rs).
        let is_bottommost = (1..v.num_levels())
            .all(|lvl| lvl == output_level as usize || v.levels[lvl].files.is_empty());

        let mut inputs: Vec<(u32, SstFileMeta)> =
            Vec::with_capacity(l0_files.len() + out_overlap_files.len());
        for meta in l0_files {
            inputs.push((0, meta));
        }
        for meta in out_overlap_files {
            inputs.push((output_level, meta));
        }
        Some(CompactionDecision::Merge(CompactionPlan {
            inputs,
            output_level,
            is_bottommost,
        }))
    }

    fn pick_level_descent(
        &self,
        v: &Version,
        cf: ColumnFamilyId,
        level: u32,
        ctx: &PolicyCtx,
    ) -> Option<CompactionDecision> {
        let level_idx = level as usize;
        if level_idx >= v.num_levels() {
            return None;
        }
        let all_src: Vec<SstFileMeta> = v.levels[level_idx]
            .files
            .iter()
            .filter(|f| f.cf_id == cf)
            .cloned()
            .collect();
        if all_src.is_empty() {
            return None;
        }
        let next_level = level_idx + 1;
        if next_level >= v.num_levels() {
            return None;
        }
        let dst_candidates: Vec<SstFileMeta> = v.levels[next_level]
            .files
            .iter()
            .filter(|f| f.cf_id == cf)
            .cloned()
            .collect();

        // Source pick. Dynamic mode = RocksDB kMinOverlappingRatio parity
        // (smallest next-level-overlap ÷ tombstone-compensated-size). Legacy =
        // lowest-key file (rotating). Ties → lowest-key (min_by keeps first).
        let src_pick: SstFileMeta = if ctx.dynamic_levels {
            let comp = |m: &SstFileMeta| -> u64 {
                compensated_file_size(m.file_size, m.num_entries, (ctx.tombstones)(m.file_number))
            };
            let overlap_bytes = |m: &SstFileMeta| -> u64 {
                dst_candidates
                    .iter()
                    .filter(|d| d.largest_key >= m.smallest_key && d.smallest_key <= m.largest_key)
                    .map(|d| d.file_size)
                    .sum()
            };
            all_src
                .iter()
                .min_by(|a, b| {
                    let (oa, ca) = (overlap_bytes(a) as u128, comp(a).max(1) as u128);
                    let (ob, cb) = (overlap_bytes(b) as u128, comp(b).max(1) as u128);
                    (oa * cb).cmp(&(ob * ca))
                })
                .cloned()
                .expect("all_src non-empty")
        } else {
            all_src[0].clone()
        };

        // Overlap gather: next-level files whose range overlaps the source.
        let overlap: Vec<SstFileMeta> = dst_candidates
            .iter()
            .filter(|d| {
                d.largest_key >= src_pick.smallest_key && d.smallest_key <= src_pick.largest_key
            })
            .cloned()
            .collect();

        // FRS-WA-V3 trivial move: disjoint source (single file is trivially
        // disjoint) with no next-level overlap and no compaction filter →
        // metadata-only demotion.
        if ctx.trivial_move && !ctx.compaction_filter_active && overlap.is_empty() {
            return Some(CompactionDecision::TrivialMove {
                files: vec![src_pick],
                from_level: level,
                to_level: next_level as u32,
            });
        }

        // is_bottommost iff next_level is the deepest non-empty level for this
        // CF (no older version below could be resurrected by a dropped
        // tombstone).
        let is_bottommost =
            (next_level + 1..v.num_levels()).all(|lvl| v.levels[lvl].files.is_empty());

        let mut inputs: Vec<(u32, SstFileMeta)> = Vec::with_capacity(1 + overlap.len());
        inputs.push((level, src_pick));
        for meta in overlap {
            inputs.push((next_level as u32, meta));
        }
        Some(CompactionDecision::Merge(CompactionPlan {
            inputs,
            output_level: next_level as u32,
            is_bottommost,
        }))
    }

    fn kind(&self) -> CompactionPolicyKind {
        CompactionPolicyKind::SortedRun
    }
}

// ---------------------------------------------------------------------------
// Pure picking helpers (moved verbatim from db.rs — pure functions, unit-tested
// directly here and via the policy-equivalence falsifier).
// ---------------------------------------------------------------------------

/// FRS-M1: from the union key-range of `upper_files`, select the `level_files`
/// whose range overlaps, expanded to a clean cut (fixpoint — ranges only grow,
/// so each pass selects ≥1 new file or terminates). Inclusive-range overlap.
pub fn overlap_scoped_clean_cut(
    upper_files: &[SstFileMeta],
    level_files: Vec<SstFileMeta>,
) -> Vec<SstFileMeta> {
    debug_assert!(!upper_files.is_empty());
    let mut lo: &[u8] = &upper_files[0].smallest_key;
    let mut hi: &[u8] = &upper_files[0].largest_key;
    for f in upper_files {
        if f.smallest_key.as_slice() < lo {
            lo = &f.smallest_key;
        }
        if f.largest_key.as_slice() > hi {
            hi = &f.largest_key;
        }
    }
    let mut selected = vec![false; level_files.len()];
    loop {
        let mut grew = false;
        for (i, f) in level_files.iter().enumerate() {
            if selected[i] {
                continue;
            }
            if f.largest_key.as_slice() >= lo && f.smallest_key.as_slice() <= hi {
                selected[i] = true;
                if f.smallest_key.as_slice() < lo {
                    lo = &f.smallest_key;
                }
                if f.largest_key.as_slice() > hi {
                    hi = &f.largest_key;
                }
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }
    level_files
        .iter()
        .zip(&selected)
        .filter(|(_, &s)| s)
        .map(|(f, _)| f.clone())
        .collect()
}

/// FRS-M4-DYNAMIC-LEVELS: the CF's BASE level — where L0 rollups output —
/// derived RocksDB-style (`CalculateBaseBytes` parity). Pure function.
pub fn compute_base_level(s_bottom: u64, bottom: u32, base_bytes: u64, mult: f64) -> u32 {
    if bottom <= 1 || s_bottom == 0 {
        return bottom.max(1);
    }
    let mult = mult.max(1.0 + f64::EPSILON);
    let mut cur = s_bottom as f64;
    let mut bl = bottom;
    while bl > 1 && cur > base_bytes as f64 {
        cur /= mult;
        bl -= 1;
    }
    bl
}

/// FRS-M4-DYNAMIC-LEVELS: dynamic size target for `level` ∈ [base, bottom):
/// `target(Ln) = size(bottom) / mult^(bottom - Ln)`. Floored at 1.
pub fn dynamic_level_target(s_bottom: u64, bottom: u32, level: u32, mult: f64) -> u64 {
    let mult = mult.max(1.0 + f64::EPSILON);
    let t = s_bottom as f64 / mult.powi((bottom.saturating_sub(level)) as i32);
    (t as u64).max(1)
}

/// FRS-M4 tombstone compensation (RocksDB `compensated_file_size` parity):
/// weight delete tombstones by twice the file's average entry size. Pure.
pub fn compensated_file_size(file_size: u64, num_entries: u64, tombstone_count: u64) -> u64 {
    if num_entries == 0 || tombstone_count == 0 {
        return file_size;
    }
    let avg = file_size / num_entries.max(1);
    file_size.saturating_add(tombstone_count.saturating_mul(avg.saturating_mul(2)))
}

/// All files mutually key-disjoint (no two share any key) — the trivial-move
/// eligibility test. Moved verbatim from db.rs (sort + adjacent strict-`<`).
pub fn sst_metas_mutually_disjoint(files: &[SstFileMeta]) -> bool {
    let mut sorted: Vec<&SstFileMeta> = files.iter().collect();
    sorted.sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
    sorted
        .windows(2)
        .all(|w| w[0].largest_key < w[1].smallest_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_common::{FileNumber, SequenceNumber};

    fn meta(fnum: u64, cf: u32, lo: &str, hi: &str, size: u64) -> SstFileMeta {
        SstFileMeta {
            file_number: FileNumber(fnum),
            cf_id: ColumnFamilyId(cf),
            file_size: size,
            smallest_key: lo.as_bytes().to_vec(),
            largest_key: hi.as_bytes().to_vec(),
            min_sequence: SequenceNumber(1),
            max_sequence: SequenceNumber(2),
            num_entries: 100,
            max_death: 0,
        }
    }

    fn no_tombstones(_: FileNumber) -> u64 {
        0
    }

    fn ctx<'a>(
        trivial_move: bool,
        dynamic: bool,
        tomb: &'a dyn Fn(FileNumber) -> u64,
    ) -> PolicyCtx<'a> {
        PolicyCtx {
            num_levels: 7,
            max_bytes_for_level_base: 256 * 1024 * 1024,
            max_bytes_for_level_multiplier: 10.0,
            dynamic_levels: dynamic,
            trivial_move,
            compaction_filter_active: false,
            tombstones: tomb,
        }
    }

    fn version(levels: Vec<Vec<SstFileMeta>>) -> Version {
        // Pad to 7 levels so `num_levels()` matches a real engine geometry.
        let mut padded = levels;
        while padded.len() < 7 {
            padded.push(Vec::new());
        }
        Version::from_levels_and_vlogs(
            padded
                .into_iter()
                .enumerate()
                .map(|(i, files)| forst_rs_storage::version::LevelMeta {
                    level: i as u32,
                    files,
                })
                .collect(),
            Vec::new(),
        )
    }

    #[test]
    fn l0_rollup_picks_only_cf_files_and_overlap() {
        let cf = ColumnFamilyId(1);
        // L0: one file [a,c]. base=L1 has [b,d] (overlaps) and [x,z] (no).
        let v = version(vec![
            vec![meta(10, 1, "a", "c", 100)],
            vec![meta(20, 1, "b", "d", 200), meta(21, 1, "x", "z", 200)],
        ]);
        let tomb = no_tombstones;
        let dec = SortedRunPolicy
            .pick_l0_rollup(&v, cf, 1, &ctx(false, true, &tomb))
            .expect("pick");
        match dec {
            CompactionDecision::Merge(plan) => {
                // L0 file + the overlapping L1 file [b,d], NOT [x,z].
                assert_eq!(plan.inputs.len(), 2);
                assert_eq!(plan.output_level, 1);
                assert!(plan
                    .inputs
                    .iter()
                    .any(|(l, m)| *l == 0 && m.file_number.0 == 10));
                assert!(plan
                    .inputs
                    .iter()
                    .any(|(l, m)| *l == 1 && m.file_number.0 == 20));
                assert!(!plan.inputs.iter().any(|(_, m)| m.file_number.0 == 21));
            }
            _ => panic!("expected Merge"),
        }
    }

    #[test]
    fn l0_rollup_trivial_move_when_disjoint_and_enabled() {
        let cf = ColumnFamilyId(1);
        // L0 single file, output level empty → disjoint, no overlap.
        let v = version(vec![vec![meta(10, 1, "a", "c", 100)], vec![]]);
        let tomb = no_tombstones;
        // trivial_move OFF → Merge.
        match SortedRunPolicy.pick_l0_rollup(&v, cf, 1, &ctx(false, true, &tomb)) {
            Some(CompactionDecision::Merge(_)) => {}
            other => panic!("trivial_move OFF must Merge, got {other:?}"),
        }
        // trivial_move ON → TrivialMove.
        match SortedRunPolicy.pick_l0_rollup(&v, cf, 1, &ctx(true, true, &tomb)) {
            Some(CompactionDecision::TrivialMove {
                files,
                from_level,
                to_level,
            }) => {
                assert_eq!(files.len(), 1);
                assert_eq!(from_level, 0);
                assert_eq!(to_level, 1);
            }
            other => panic!("trivial_move ON must TrivialMove, got {other:?}"),
        }
    }

    #[test]
    fn l0_rollup_none_when_empty() {
        let cf = ColumnFamilyId(1);
        let v = version(vec![vec![], vec![]]);
        let tomb = no_tombstones;
        assert!(SortedRunPolicy
            .pick_l0_rollup(&v, cf, 1, &ctx(true, true, &tomb))
            .is_none());
    }

    #[test]
    fn level_descent_min_overlap_ratio_pick() {
        let cf = ColumnFamilyId(1);
        // L1 has two files; A overlaps a big L2 file, B overlaps nothing.
        // Dynamic mode must pick B (smaller overlap ratio).
        let v = version(vec![
            vec![],
            vec![meta(30, 1, "a", "c", 100), meta(31, 1, "p", "r", 100)],
            vec![meta(40, 1, "a", "c", 9999)],
        ]);
        let tomb = no_tombstones;
        let dec = SortedRunPolicy
            .pick_level_descent(&v, cf, 1, &ctx(false, true, &tomb))
            .expect("pick");
        match dec {
            CompactionDecision::Merge(plan) => {
                // B (file 31, [p,r]) has zero overlap → picked; no L2 overlap.
                assert!(plan
                    .inputs
                    .iter()
                    .any(|(l, m)| *l == 1 && m.file_number.0 == 31));
                assert_eq!(plan.inputs.len(), 1);
            }
            _ => panic!("expected Merge"),
        }
    }

    #[test]
    fn level_descent_trivial_move_when_no_overlap() {
        let cf = ColumnFamilyId(1);
        let v = version(vec![
            vec![],
            vec![meta(30, 1, "a", "c", 100)],
            vec![meta(40, 1, "x", "z", 100)],
        ]);
        let tomb = no_tombstones;
        match SortedRunPolicy.pick_level_descent(&v, cf, 1, &ctx(true, true, &tomb)) {
            Some(CompactionDecision::TrivialMove {
                from_level,
                to_level,
                files,
            }) => {
                assert_eq!(from_level, 1);
                assert_eq!(to_level, 2);
                assert_eq!(files[0].file_number.0, 30);
            }
            other => panic!("expected TrivialMove, got {other:?}"),
        }
    }

    #[test]
    fn disjoint_and_compensation_pure_fns() {
        assert!(sst_metas_mutually_disjoint(&[
            meta(1, 1, "a", "c", 1),
            meta(2, 1, "d", "f", 1)
        ]));
        assert!(!sst_metas_mutually_disjoint(&[
            meta(1, 1, "a", "e", 1),
            meta(2, 1, "d", "f", 1)
        ]));
        // compensation weights tombstones.
        assert_eq!(compensated_file_size(1000, 100, 0), 1000);
        assert!(compensated_file_size(1000, 100, 10) > 1000);
        // base-level / target pure-fn parity.
        assert_eq!(compute_base_level(0, 6, 256, 10.0), 6);
        assert!(dynamic_level_target(1_000_000, 6, 5, 10.0) >= 1);
    }
}
