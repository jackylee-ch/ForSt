# 8c/32g 3-backend NexMark sweep — verified time + accuracy (2026-06-08)

All 100M events, 8c/32g Docker, each backend with its own timer. `out_rows` = sink vertex
read-records (accuracy: forst-rs must match RocksDB). Bar: forst-rs ≥0.8× RocksDB OR ≤+50s,
AND faster than ForSt, AND out_rows match.

## 🔒 LOCKED uniform config — identical for EVERY query (no per-query changes)
forst-rs: **`noflush=false`** · **`write_buffer_size=1024mb` (1 G)** · global WBM budget **6 GB**
(`FRS_WBM_TOTAL_MB=6144`) · true WBM backpressure + 256 MiB force-switch · SST-write coalescing
(4 MiB) · `.so` loaded **container-local** (`/usr/local/lib`) · lz4 SST compression.
**PMC rationale for 1 G** (not 128 M): SST count drives join read-amp (the #1 remaining gap); large
memtables → fewer/larger SSTs → lower read-amp. The old 1 G downsides are gone — coalescing makes big
flushes cheap (202→20 ms/MB) and the 6 GB global WBM budget bounds total RAM (per-memtable size is
just an upper bound, RocksDB-style). 128 M was empirically worse (compaction-overload → restart).
**Any forst-rs number NOT from this exact config is INVALID.** (8c/32g RocksDB ~2× slower than host
→ only 8c/32g-vs-8c/32g counts.)

## ★★★ STATUS UPDATE 2026-06-08 (PM): OOM SET FIXED (memory) via true backpressure — speed now the blocker

**Strategic pivot (user):** learn from ForSt (the backend forst-rs replaces), not RocksDB. ForSt's
memory model = flush-on-checkpoint + a TRUE WriteBufferManager stall (allow_stall, no give-up) +
fast flush. forst-rs had `noflush=true` (S3-tuning leftover → checkpoints never flushed memtables)
AND its write-stall only lived on the unused single-write path — so the FFM batch-only backend had
**NO write backpressure at all** → memtables grew to 7-11 GB → 8c/32g OOM.

**Fix implemented (engine, uncommitted) — the uniform memory model:**
1. `noflush=false` (uniform, matches ForSt — checkpoints flush+bound memtables).
2. `wait_for_wbm_headroom` stalls on the REAL budget `over_budget()`, NO 120s give-up (60s
   no-downward-progress defense only); added to ALL 3 batch write leaves (was single-write only).
3. Force memtable switch on over-budget (RocksDB WBM behavior) with a **256 MiB floor** (16 MiB
   floor caused an L0 explosion → `l0_stop_trigger=64` → pipeline freeze → restart).
Global WBM budget 6 GB (FRS_WBM_TOTAL_MB=6144, uniform). + SST-write COALESCING (4 MiB; flush
202→20 ms/MB) + SIGBUS fix (.so loaded container-local, not FUSE bind mount). Engine UTs green.

**Result — OOM SOLVED + write-throughput FIXED (verified, committed config):**
- **q17: OOM@81s/31GB → FINISHED 76.7s (1.03× RocksDB 74.5 = PARITY), out_rows 92M ✓.** (was 817s/11×
  before coalescing → coalescing cut sstwrite 504s→9.9s.)
- **q18: OOM/SIGBUS → FINISHED 318.1s (0.80× RocksDB 396.1 = FASTER), out_rows 92M ✓.** (SIGBUS .so fix.)
- **q9 (join): OOM@31GB → memory-bounded (peak RSS 18.2 GB) but DNF (read-amp, ~53M/1300s).** ✗ speed —
  WORST case: join-probe read-amp over growing state → needs the read-path module.
- q4 (join): running (committed cfg). q20 (join): pending re-measure. Expect q9-like read-amp.

**Speed severity ranking (all memory-bounded now): joins (q9 read-amp) ≫ dedup (q18 now PASS)
> group-agg (q17 now PASS).** The join read-amp (per-probe re-walk of growing state) is the
dominant remaining speed blocker → the parallel-coalesced/value-carrying READ PATH is the
highest-leverage next module (spec: 2026-06-08-q9-read-path-gap-investigation-design.md).

**WBM budget sizing (uniform, FRS_WBM_TOTAL_MB) is a partial speed lever:** q9 2 GB budget →
38.4M/1300s (end 9 K/s); q9 **6 GB budget → 52.9M/1300s (end 36 K/s)**, still memory-bounded
(RSS ~18-22 GB < 32 GB). Bigger budget → bigger/fewer L0 SSTs → less read-amp. **Adopted 6 GB.**

**★ noflush DECISION (fixed, committed): noflush=FALSE (the ForSt semantic).** noflush is NOT a
tunable — checkpoint format differs (artifact vs SST) so it can't change once data exists. ForSt
supports noflush=false ONLY; noflush=true is a forst-rs hack (live-memtable artifact) that
contradicts disaggregation. **FAIRNESS CORRECTION: the bounded-state "wins" in the table below
(q16 366, q19 298 "FASTEST") used noflush=TRUE — UNFAIR (skipped the flush ForSt does) → MUST be
RE-MEASURED under the committed config (noflush=false + 6GB + coalescing).** Also: 8c/32g RocksDB is
MUCH slower than host (q17 74.5 vs host 57; q18 396 vs host 195) → judgments vs *host* are invalid.

REFUTED (all with 8c/32g data, no wasted code): resident-shadow retire (shadow=0 for joins);
jemalloc eager-return (retained not reclaimable, still OOM); decoded-block-cache balloon (charge()
correct); compaction 8→3 (still OOM); WBM hard-cap-with-120s-give-up (released → overran → OOM);
write_buffer_size 128mb (compaction-overload → restart). Merge-collapse REFUTED as the flush cost
(flush doesn't merge — code-verified); per-key CPU REFUTED (buffer 8s + encode 17s ≪ sstwrite 504s).

---

> ✅ **q17/q18/q4 are at the LOCKED config (1 G, noflush=false) — VALID.** Still INVALID (re-measure
> under locked cfg): q16/q19 (`noflush=TRUE` — removed); q7/q11/q15 (OLD fast cfg). Light queries
> (q0-q3,q10,q12-14,q21,q22) are source-bound and pre-lock — low-risk, re-confirm.

| query | RocksDB s | RocksDB out_rows | forst-rs s | forst-rs out_rows | ForSt s | acc | bar |
|-------|-----------|------------------|------------|-------------------|---------|-----|-----|
| q0    | 32.7      | 100,000,000      | 31.0       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |
| q1    | 30.6      | 100,000,000      | 29.4       | 100,000,000       | -          | ✓ | **PASS (correct + faster, 0.96×)** |
| q2    | 28.8      | 100,000,000      | 28.7       | 100,000,000       | -          | ✓ | **PASS (correct + parity)** |
| q3    | 36.8      | 2,201,068        | 36.7       | 2,201,068         | -          | ✓ | **PASS (correct + parity, 1.00×)** |
| q4    | **313.5 (8c/32g)** | **177,629,788** | **597.2** 🔒1G | **25,849,976** | (host 1217)| ⧗ | **out_rows 25.8M vs 177.6M (6.9× fewer) — but q4 is a RETRACT/changelog query → out_rows is NOT a clean accuracy gate (could be changelog cadence, not wrong final answer). NEEDS final-result compare.** Also 1.9× slower. OOM-fixed/finishes. |
| q5    | 162.2     | 29,988,376 (det) | 240/138    | 2.0/2.4/2.9M (var)| (host 297) | ✗ | **FAIL — WRONG: ~14× under-emit + non-deterministic (windowed-JOIN bug; both async backends)** |
| q7    | (pending 8c/32g) | — | **DNF @1300s** 🔒1G (~78.5M/92M, rate→19.7K/s) | — | (host 659) | ? | **read-amp DNF** under locked cfg (join). Old "671s" was unfair noflush=true. Joins q9/q20 → needs READ-PATH module. |
| q8    | 46.1      | 3,064,457        | 43.7 (OLD) | 3,010,888         | (pending)  | ✗? | time ~parity (0.95×) but out_rows 1.75% LOW (53K fewer) — windowed-JOIN under-emit (mild) |
| q9    | (pending 8c/32g) | — | **DNF** 🔒1G (43M@762s, rate→10K/s, read-amp collapse) | — | (pending) | ? | **read-amp DNF** (join). Memory-bounded (no OOM); CPU 375% = 4 slots busy/4 idle. Joins q7/q20 → READ-PATH module. |
| q10   | 120.3     | 100,000,000      | 128.7      | 100,000,000       | -          | ✓ | **PASS (correct; +8.4s ≤ +50s bar)** |
| q11   | 111.1     | 92,000,000       | 382 (OLD fast cfg) | 92,000,000 | (host 158)| ✓ | correct; 3.4× — **RE-MEASURE under committed cfg** |
| q12   | 42.1      | 92,000,000       | 50.6       | 92,000,000        | -          | ✓ | **PASS (out_rows match; +8.5s ≤ +50s bar)** |
| q13   | 30.1      | 100,000,000      | 30.1       | 100,000,000       | -          | ✓ | **PASS (correct + parity)** |
| q14   | 30.1      | 100,000,000      | 29.0       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |
| q15   | 228.3     | 92,000,000       | 173.9 (OLD)| 92,000,000        | (host 258) | ✓ | correct + faster — **RE-MEASURE under committed cfg** |
| q16   | 428.4 (8c/32g) | 92,000,000 | **322.9** 🔒1G | 92,000,000 | (host 379.5) | ✓ | **PASS vs RocksDB — 0.75× (FASTER) + accurate** (locked cfg, faster than old unfair 366). ForSt 8c/32g pending for full verdict. |
| q17   | **74.5 (8c/32g)** | 92,000,000 | **76.7** 🔒1G | 92,000,000 | **255.7 (8c/32g)** | ✓ | **FULL PASS — 1.03× RocksDB (parity) + 3.3× FASTER than ForSt + accurate.** |
| q18   | **396.1 (8c/32g)** | 92,000,000 | **318.1** 🔒1G | 92,000,000 | **422.8 (8c/32g)** | ✓ | **FULL PASS — 0.80× RocksDB (faster) + faster than ForSt (318<423) + accurate.** |
| q19   | 305 (8c/32g) | 92,000,000 | **542.5** 🔒1G | 92,000,000 | (host 310.5) | ✓acc | **FAIL — 1.78× slower than RocksDB** (542 vs 305) + likely slower than ForSt. Accurate (exact). NON-join OVER-window dedup → a SECOND perf gap (not join read-amp). Architecture investigate. |
| q20   | **800.3 (8c/32g)** | 93,201,404 | **DNF @1300s** 🔒1G (59.8M/93M, rate→23.6K/s) | — | (pending) | ✗ | **FAIL — DNF vs RocksDB 800s.** Join, OUTPUT-amplifying (auction⋈bid). NOT read-amp (n_ovl=2-3) NOT iter-decode (already zero-copy). Bottleneck = executor dispatch / output-emit (profile pending). Bar: must finish ≤1000s + beat ForSt. |
| q21   | 59.9      | 100,000,000      | 52.6       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |
| q22   | 44.8      | 100,000,000      | 43.6       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |

NOTE on STALE rows (q7/q8/q11/q15/q16/q19): measured on the OLD "fast cfg" (noflush=TRUE) or
pre-fix configs — their pass/fail is NOT trustworthy and must be RE-MEASURED under the committed
config (noflush=false + 6GB + coalescing + container-local .so). Light queries (q0-q3,q10,q12-14,
q21,q22) are source-bound parity — low-risk but also pre-commit; re-confirm.

## REFINED ROOT CAUSE: UNBOUNDED-state queries OOM on fast cfg → FIXED (2026-06-08)
OOM set = q4(join)/q9(join)/q17(group-agg)/q18(dedup)/q20(join) = ALL accumulate UNBOUNDED state.
Fixed by the uniform memory model (true backpressure bounds RAM regardless of state size). q17/q18
now PASS; q9/q4/q20 finish-memory-wise, join read-amp is the remaining speed gap (read-path module).

## ACCURACY methodology (PMC — out_rows is gate ONLY for append/dedup)
- **APPEND / dedup / dedup-changelog queries → out_rows IS a clean exact gate.** Verified EXACT
  (forst-rs == RocksDB): q17/q18/q19 = 92M. These are real accuracy passes.
- **RETRACT/changelog queries (running aggs with updates: q4) and WINDOWED queries (q5/q8) →
  out_rows is NOT a clean gate** — a different changelog *cadence* (fewer intermediate update rows)
  gives a smaller out_rows with the SAME final answer. Must compare the **final materialized result
  rows**, not the changelog count.

## JOIN-family out_rows mismatches — to be resolved by final-result compare
- **q4 (auction⋈bid + agg, retract changelog): out_rows 25.8M vs 177.6M (6.9× fewer).** SIGNAL, not
  confirmed-wrong (retract query — see methodology). Needs final-result compare.
- **q5 (HOP+join): CONFIRMED WRONG** — controlled same-input replay gave non-deterministic, ~14×-low
  counts (a real bug, not just cadence).
- q8 (tumbling+join): out_rows 1.75% low — windowed, needs final-result compare.
Pure non-join AGG (q16/q19) = EXACT. The join queries (q4/q5/q7/q8/q9/q11/q20) all need a
**final-result accuracy check** (upsert-result compare), not out_rows, before their perf counts.
q5 is the one confirmed-wrong; the rest are open. This is the accuracy work item for the join family.

## ★ WHAT'S LEFT (Phase-1 close)
1. **Fair baselines:** RocksDB 8c/32g + ForSt 8c/32g for the WHOLE set ("faster than ForSt" clause
   unverified almost everywhere; have RocksDB 8c/32g only for q17/q18).
2. **Re-measure the STALE rows** (q7/q8/q11/q15/q16/q19 + light) under the committed config.
3. **Finish OOM-set joins:** q4 (running), q20 (next); score vs bar.
4. **Read-path module** for join read-amp (q9) — the one architecturally-open perf gap.
5. **Correctness (GATE):** build a **final-result accuracy check** (materialize the changelog →
   compare result rows vs RocksDB) for the join/retract/windowed family — out_rows alone is not a
   valid gate there. q5 is CONFIRMED wrong (controlled replay); q4/q8 (and q7/q9/q11/q20) have
   out_rows mismatches that are SIGNALS to resolve with the final-result check, not yet confirmed
   bugs. Fix any confirmed wrong-answer before its perf counts.
6. **Commit** the verified fixes (memory model + coalescing + SIGBUS) after the re-measure confirms
   no regression; then compute overall NexMark total vs RocksDB.

## Verified architecture fixes this campaign (UNCOMMITTED)
- A: memory model (true WBM backpressure, force-switch) → OOM set bounded ≤18 GB (was 31 GB OOM).
- B: SST-write coalescing (per-block block_on → 4 MiB writes) → flush 202→20 ms/MB; q17 817→76.7s.
- C: SIGBUS fix (.so container-local, not FUSE mount) → q18 teardown crash → clean finish.
Tests: io 209 + storage + engine 277 green.
