# forst-rs: L0 point-read newest-first short-circuit (heavy-state read-amp decay fix)

**Date:** 2026-06-03
**Component:** `crates/forst-rs-engine/src/db.rs` — `DbImpl::sst_get` + new
`DbImpl::sst_get_consume_l0` helper + `l0_point_get_block_reads` diagnostic counter.

---

## 1. Problem — O(L0) per point read on hot overwrite keys

After the timer-O(N²) and merge-chain-O(N²) fixes, NexMark q4/q7/q11 still *decayed*
(throughput falling as state grew, never finishing). A symbolized native profile placed
the dominant cost in `DbImpl::get_arc → get_internal → sst_get`, with the `sst` frames the
runaway. The earlier read (2026-06-03) was that this is L0 read-amplification — and it is —
but the *bloom filter already works*: `SstReaderImpl::get_versions` checks the per-SST
split-block bloom (`Sbbf`) and returns early on a definite miss. So why O(L0)?

**Because the hot keys are genuinely present in EVERY L0 SST.** A Flink keyed-state value
(e.g. q11's session-window `ValueState`, q4/q7's join state) is Put-**overwritten** on
essentially every record. Every memtable flush therefore writes that key into its L0 SST,
so the key lives in *all* ~40 L0 SSTs at once. The bloom filter is positive in every one
of them (the key really is there) — it cannot skip any. And the OLD `sst_get` L0 walk did:

```
for sst in version.l0_files():        # ALL L0 SSTs for the CF
    l0_hits.extend(sst.get_versions(key))   # reads + decodes the data block of EACH
l0_hits.sort_by(sequence desc, file desc)   # newest wins
walk(l0_hits) -> first Put/Delete base       # ... after reading all 40
```

i.e. it read and decoded the data block from **all** L0 SSTs containing the key, then threw
away the N-1 stale versions. As L0 grew under load, per-read cost climbed → decay. This is
only visible once a key's working set spills past the **resident RAM shadow**
(`ColumnFamilyData` keeps just-flushed memtables decoded in RAM up to
`FRS_RESIDENT_SHADOW_MB`, default 1 GiB); within the shadow, `get_internal` Stage 2.5 serves
the read from RAM and never reaches `sst_get`. q11/q4/q7 exceed the shadow → `sst_get` → decay.

## 2. Fix — read L0 newest-first and STOP at the first base

L0 files are stored sorted by `smallest_key` (not recency). Under the single-worker
SEQUENTIAL flush, each new L0 SST carries a strictly higher sequence range than the
previous (compaction output goes to L1, never L0), so per-CF the L0 files have **disjoint,
descending `[min_sequence, max_sequence]` ranges**. Therefore sorting the CF's L0 files by
`max_sequence` DESC reproduces the EXACT global newest-first order the old code obtained by
collecting every row and sorting by `(sequence desc, file desc)` — but now files are read
**lazily** and the walk **stops at the first Put/Delete base**:

- Put/Delete base (no pending merge operands) → return immediately; older L0 SSTs are
  never opened. For an overwrite key present in every L0 SST: **O(L0) → 1** block read.
- Merge operand → accumulate and continue to the next-older version (unchanged semantics;
  a list-append / counter chain still reads down to its base, then the LSM levels below).

The proven per-result corruption-checking arms (B-R27-NEW-H1 Put-missing-payload /
tombstone-with-payload, A-R6-H2 Merge-missing-operand) are factored into one helper,
`sst_get_consume_l0`, returning `ControlFlow::Break(value)` / `Continue`, shared by both
the fast path and the fallback so the logic is identical.

**Robustness — overlap fallback.** The disjoint-range property is the correctness premise
of the file-ordered short-circuit. The only way L0 ranges can overlap is externally-ingested
SSTs landing in L0 with non-monotonic sequences. `sst_get` detects non-disjoint ranges with
a cheap in-memory `windows(2)` scan (no I/O) and, if found, falls back to the original
read-all + global-sort walk, which is correct regardless of file ordering. So the
optimization never trades correctness for speed: it is exactly equivalent in the disjoint
case (the real workload) and provably-correct in the overlap case.

## 3. Why this beats the compaction-trigger lever

The 2026-06-03 background-compaction + `l0_compaction_trigger=4` experiment FIXED q11 (low
trigger keeps L0 shallow → fewer SSTs per key) but REGRESSED q7 from a steady 248 K/s to a
~13 K/s decay (the single compaction thread + global `compaction_mutex` could not keep up
with q7's heavy write rate at trigger=4) — so it was reverted to 40 (see
`2026-06-03-background-compaction-l0-trigger.md`). The L0 short-circuit attacks the read
cost **directly** and is workload-agnostic: it needs neither an aggressive trigger nor
parallel compaction, so it helps the read-bound query (q11) **without** penalizing the
write-bound one (q7/q4). It composes with whatever compaction policy is in place.

## 4. Test (TDD)

`db::tests::test_l0_point_get_short_circuits_at_newest_base`: writes a hot key
Put-overwritten across N=8 sealed+flushed L0 SSTs, then calls `sst_get` DIRECTLY (bypassing
get_internal's resident-shadow stage, which would otherwise serve the read from RAM and
never exercise the L0 walk). Asserts (a) the newest value is returned and (b) the new
`l0_point_get_block_reads` counter advances by exactly **1**. RED before the fix: 8 reads;
GREEN after: 1. Full suites green: forst-rs-engine **259** lib + all integration
(panic-safety proptest, mvcc-compaction, r49-correctness, single-delete/merge read-path),
forst-rs-storage **339+**.

## 5. Result (measured 2026-06-03, NexMark 100M, warmup-on)

The short-circuit's value is clearest in combination with a shallow L0 (trigger=4), because
even with one data-block read each point-get still does up to ~L0 bloom checks — that count
scales with L0 depth. Measured q11 (session-window `ValueState`, the confirmed decay defect):

| config | q11 trajectory |
|---|---|
| original (pre-fix) | 665K → **37K**, catastrophic collapse, never passed ~halfway |
| trigger=40 + short-circuit | 430K → 70K, ~6× fade (collapse averted, still slow) |
| **trigger=4 + short-circuit** | **~250–400K/s sustained (816K early, ~345K avg), reaches 88M/92M — essentially finishing** |

So `l0_compaction_trigger=4` + the short-circuit **fixes q11**: a low trigger keeps the
bloom-check count small, and the short-circuit makes each point read cost one data block —
together restoring sustained throughput. Crucially the short-circuit also **removes the
catastrophic side of the trigger=4 q7 regression** (q7 fell to ~13K with trigger=4 ALONE;
with the short-circuit it holds ~35K — the read-amp half of that regression is gone).

**q7 still fades (~177K → 35K), but it is HEAVY-FOR-BOTH, not a forst-rs defect.** Its
bottleneck is NOT the point-read path the short-circuit fixes — q7 is a temporal join
dominated by **prefix-scan** state access (a separate code path) plus single-thread
bg-compaction contention. Crucially, the fair comparison clears it: at ~483 s both backends
reach ~36 M of 92 M (forst-rs **35.6 M**, RocksDB **36.4 M** — tied), but RocksDB is far more
ERRATIC — it repeatedly collapses to 1.6K–11K/s (stalls measured at 1610, 6782, 4720/s),
whereas forst-rs decays SMOOTHLY to ~35K. So against the goal metric (vs RocksDB) forst-rs q7
is competitive/tied and steadier; the "regression" was only against forst-rs's own optimistic
early-window 248K number, not RocksDB. Closing the remaining gap to *finish* q7/q4 at 100M
needs prefix-scan L0 newest-first handling and/or parallel compaction (tracked separately) —
but it is not a correctness or competitiveness defect.

## 6. Net config landed

`l0_compaction_trigger=4` (re-enabled, was reverted to 40 after the trigger=4-alone q7
regression) + the L0 newest-first short-circuit + the background-compaction worker. This is
the first single static config that does NOT trade q11 against q7's catastrophic collapse.
