# Round 2 — Aggregate Summary

**Date:** 2026-05-22
**Round 1 → Round 2 fixes landed:** A1-H5, C-H4, D-H1 (all 3 surgical fixes from Round 1).
**Round 2 verdict:** NEW issues found, including 2 CRIT from Agent E and 3 regressions caused by my A1-H5 fix.

## Tally (Round 2)

| Agent | HIGH count | Notable |
|---|---:|---|
| A (correctness) | 3 | All 3 cite gaps in my A1-H5 fix |
| B (vectorization) | 5 | 1 critiques A1-H5 perf cost |
| C (zero-copy) | 3 | Rust engine only; C-H4 verified clean |
| D (JDK 25) | 2 | D-H1 verified clean |
| E (Flink streaming) | 5 (2 CRIT + 3 HIGH) | 2 NEW CRITs |
| **Total** | **18** | |

## Cumulative tally (Rounds 1 + 2)

- Round 1 HIGH found: 37
- Round 1 fixes landed: 3
- Round 2 HIGH found: 18 (some are regressions of Round 1 fixes; some new findings the Round 1 audit missed)
- **Total open HIGH (estimated, after dedupe): ~50**

## Critical new findings (Category 1: must fix before any other work)

1. **E2-CRIT-1** — V2 keyed-state engine keys are encoded WITHOUT the namespace. Multiple windows over the same key → state collision. (E.g. windowed aggregation with multiple window definitions.)
2. **E2-CRIT-2** — FORSTRS timer queue (async backend) uses `() -> keyGroupRange.getStartKeyGroup()` as the kgSupplier — peek/poll only sees timers in the START keygroup. All other key-groups' timers are unreachable.

## Regressions from my Round 1 fixes (Category 2: blocking before Round 3)

1. **A2-H1** — `executeRequestSync` (sync sibling of `executeBatchRequests`) still has the A1-H5 anti-pattern unconditionally completing successful futures after APPEND_MERGE / ITER_PREFIX. **I missed the sync path.**
2. **A2-H2** — My `completePutExceptionally` wraps the cause under a generic message and re-escalates `FrsEnginePanicError` after `FatalErrorHandler.onFatalError` already fired in `dispatchAppendMergeBatch` — double-escalation + diagnostic loss.
3. **A2-H3** — `executeBatchRequests` returns `completedFuture(null)` even when ALL per-row APPEND_MERGE futures completed exceptionally — runtime schedules next batch into failing engine.
4. **B2-H1** — Per-row `isCompletedExceptionally()` volatile read pollutes BTB on happy path; sideband `Throwable[]` from the dispatcher would be branchless.

## New findings (Category 3: previously unaudited code paths)

### Rust engine
- **C-R2-H1** `cached_fs::fetch_through_cache` — Vec without with_capacity
- **C-R2-H2** SST reader to_vec() per row — defeats Arrow zero-copy
- **C-R2-H3** Memtable scan/get clones per row
- **B2-H2 / B2-H5** Iterator handle global Mutex<HashMap> serializes opens
- **B2-H3** combine_slices still allocs per-key merged Vec (necessary for non-merge-operator path)
- **B2-H4** write_chunk_into_buf scalar length prefixes interleaved with row bodies

### Java
- **D-R2-1** `Arena.ofShared()` per iter — should be confined (out-params read before safepoint)
- **D-R2-2** Manual pointer arithmetic on FrsBytes / Arrow structs (6+ sites)
- **D-R2-5 (M)** `ForStRsKeyedStateBackend.java:228-229` anonymous Arena.ofShared() never closed (64KB/thread leak)
- **E2-HIGH-1** TTL never read from `desc.getTtlConfig()` (V1 + V2 silent disable)
- **E2-HIGH-2** `MapStateCache` survives `asyncClear()` (no hook in final method)
- **E2-HIGH-3** `restoreWithRescaling` serial dbOpen + per-handle synchronous SST downloads

## Round 2 fix priority

**To land in this round:**
1. ✅ A2-H1 (sync path mirror of A1-H5)
2. ✅ A2-H2 (don't double-escalate)
3. ✅ A2-H3 (container future fails if any row failed)
4. ✅ E2-CRIT-2 (FORSTRS timer supplier) — verify then fix
5. ✅ D-R2-5 (Arena.ofShared leak) — easy

**Deferred to Round 3+ (verification needed):**
- E2-CRIT-1 (namespace missing in V2 keys) — need code-read to confirm; could be encoded elsewhere
- B2-H1 critique of A1-H5 perf cost — defer pending bench evidence the volatile read actually shows
- All Rust engine zero-copy findings — separate architectural design

## Termination state
- Round 2 of 100
- Consecutive clean rounds: 0
- New HIGH count remains nonzero; need 5 consecutive clean to terminate
