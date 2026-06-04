# forst-rs: q4/q7 heavy-temporal-join decay — range-scan prune + subcompaction (design)

**Date:** 2026-06-03
**Status:** DESIGN (blueprint for implementation after the in-flight q0–q22 sweep frees the machine).
**Components:** `crates/forst-rs-engine/src/db.rs` (range-scan source build, compaction),
`crates/forst-rs-storage/src/sst/reader.rs` (range prune), `compaction.rs` (subcompaction).

---

## 1. Problem

After the timer-O(N²), merge-chain-O(N²), and L0 point-read short-circuit fixes, NexMark
q4/q7 (temporal joins) still decay (181K → 25K rec/s, reach ~36–42M of 92M in ~8 min).
Their state is read via **prefix/range scans**, NOT point reads — so the L0 point-read
short-circuit (which fixed q11's ValueState point reads) does not apply. RocksDB also decays
on q7 (erratic, collapses to 1.6–11K), so forst-rs is competitive — but neither *finishes*
100M in a bounded budget, and the goal needs them faster.

## 2. Root cause (two layers)

### (a) Range-scan opens + decodes the first block of EVERY overlapping L0 SST
`build_lazy_range_key_stream` (db.rs ~5705-5759) builds a k-way merge over
memtable + imm + ALL SSTs from `version.live_sst_files_iter()`. It coarse-filters by
`smallest_key`/`largest_key` but — unlike the **prefix** path (db.rs ~5627, which calls
`reader.may_contain_range(prefix, upper)`) — the **range** path does NOT apply the
decode-free `may_contain_range` block-index prune. So a range scan opens and decodes the
first data block of every L0 SST whose coarse key-range brackets the range, even ones that
hold no key in `[lower, upper)`. For a join probe over deep L0 this is O(L0) first-block
decodes per record.

### (b) The genuinely-overlapping L0 SSTs still must all be merged
For a temporal-join key whose entries land in many L0 SSTs (continuous append under load),
even a perfect prune cannot skip them — the k-way merge has O(L0) live sources, and cost
grows with L0 depth. The only lever is **keeping L0 shallow**, i.e. compaction that keeps up
with the write rate. The single-thread compaction (global `compaction_mutex`) cannot, so L0
grows → decay. (Confirmed: trigger=4 alone made it WORSE — the single thread fell further
behind; the L0 short-circuit kept the point-read side cheap but the prefix-scan k-way merge
is unaffected.)

## 3. Fix — two changes, in order of risk/leverage

### Fix A (low risk, quick): backport the range-scan decode-free prune
Mirror the prefix path: before pushing an L0 SST as a merge source in
`build_lazy_range_key_stream`, skip it when the block index proves no key in `[lower, upper)`.
Prefer a `SstFileMeta`-level coarse check first (no reader open), then the opened-reader
`may_contain_range(lower, upper)` (reader.rs:615, already correct) as the precise gate. This
removes the wasted first-block decode for non-overlapping L0 SSTs in range scans. Helps any
range-scan query; bounded benefit for join keys that genuinely span L0 (those still merge).
TDD: a test that builds N L0 SSTs where only 1 overlaps `[lo,hi)` and asserts only 1 source
is opened (analogous to `test_l0_point_get_short_circuits_at_newest_base`).

### Fix B (medium risk, the real lever): subcompaction of a single CF's L0→L1
Per-CF parallel compaction does NOT help q4/q7 (one heavy state CF). Parallelize ONE CF's
L0→L1 across K threads by key-range partition so compaction keeps up at trigger=4 and L0
stays shallow → the k-way merge has few sources. Blueprint (from the compaction-path audit):
- Job assembly (db.rs `compact_l0_for_cf` ~5070): after CF-scoped input selection, merge-sort
  the input entries once, compute K-1 boundary keys (binary search on the sorted entry vec —
  NEW range-partition primitive), pre-allocate K output file numbers
  (`version_set.allocate_file_number()`, atomic).
- Per-partition execution: K threads (rayon or a small pool), each emits its key-range slice
  through the existing `emit_key_versions` + writer, producing one partial SST with its own
  `[smallest,largest]` and a single `new_files` entry. Shared, captured-once:
  `min_active_snapshot`, merge operator, compaction filter (all read-only → safe to share).
- Atomic install: collect the K partial `new_files` + all `deleted_files` into ONE
  `VersionEdit` and call `version_set.apply(&edit)` once (already supports K new_files;
  `apply_lock` serializes; R44-L2/R45-M2/R46-M3 stale-validation unchanged).
- Correctness constraints preserved: disjoint key ranges (no cross-partition interaction),
  unique `(sequence,file_number)` (disjoint file numbers; global seqs), single atomic apply,
  readers always see an atomically-installed `current()`.
- **Critical invariant for Fix B + the L0 short-circuit:** the point-read short-circuit
  relies on L0 files having disjoint, descending sequence ranges. Subcompaction outputs go to
  **L1**, not L0, so the L0 sequence invariant is untouched. ✓

### Optional Fix C (low effort, orthogonal): per-CF parallel compaction
Replace the global `compaction_mutex` with a per-CF `compaction_lock` + a small worker pool
(lock order: per-CF compaction_lock → per-CF flush_mutex, already the order used). Lets a
CF's state and timer CFs compact concurrently. Helps multi-CF workloads; not q4/q7's single
heavy CF, but cheap and composes with B.

## 4. Sequencing & validation
1. Fix A first (small, TDD, low risk) — measure q4/q7 delta.
2. Fix B (subcompaction) — the throughput lever; TDD the range-partition primitive + a
   compaction-correctness test (K-partition output == single-output for the same inputs,
   byte-identical merged result), then full engine/storage suite, then q4/q7/q11 benchmark.
3. Re-run the q0–q22 2-way sweep for the total.
All gated by: no byte[]/per-row regressions (these are engine-internal Rust over Arrow
RecordBatches — the Java vectorized/zero-copy path is untouched), engine+storage suites green.

## 5. Status
Designed from two read-only audits (range-scan path + compaction path), file:line-grounded.
Not yet implemented — the machine is running the q0–q22 reproducible-total sweep; concurrent
cargo builds would corrupt those throughput numbers. Implement once the sweep completes.
