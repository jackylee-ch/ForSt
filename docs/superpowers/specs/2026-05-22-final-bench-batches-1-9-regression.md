# Final bench results 2026-05-22 — batches 1-9 regression report

**Bench artifact**: pre-batch-10 jar (`b33ab8528b6d`) + dylib (`f3ce7b9623b2`).
**Bench run**: `b43q3691g`, ran 2026-05-22 20:56 – 23:37 CST.
**Backends compared (this run)**: forst-rs only. (rocksdb baseline from `project_v3_bench_full_results`.)

## Raw result

| Query | forst-rs (this run) | v3 baseline (rocksdb) | Delta vs v3 |
|-------|---------------------|------------------------|-------------|
| q0  | 21.4s   | ~baseline | OK |
| q1  | 20.5s   | ~baseline | OK |
| q2  | 18.9s   | ~baseline | OK |
| q3  | **TIMEOUT** (>936s) | ~40s | **REGRESSION** |
| q4  | **TIMEOUT** (>936s) | ~40s | **REGRESSION** |
| q5  | 14.2s   | 586.7s (forst-rs v3.8 was 586.7) | **IMPROVED 41×** (but rocksdb still wins) |
| q7  | **TIMEOUT** | — | **REGRESSION** |
| q8  | 623.0s  | normal | **REGRESSION** |
| q9  | **TIMEOUT** | — | **REGRESSION** |
| q10 | 9.8s    | ~baseline | OK |
| q11 | **72.6s** | 98.7s rocks / 76.5s prior forst-rs | **WIN 1.36× vs rocks** (slight improvement vs prior) |
| q12 | **611.6s** | 31.3s rocks / 31.5s prior forst-rs | **DISASTER 19× slower** |
| q13 | 40.4s   | normal | OK |
| q14 | 19.4s   | ~baseline | OK |
| q15 | 18.3s   | ~baseline | OK |
| q16 | 149.1s  | ~15s | **REGRESSION 10×** |
| q17 | 84.8s   | ? | borderline |
| q18 | 597.4s  | normal | **REGRESSION** |
| q19 | **TIMEOUT** | — | **REGRESSION** |
| q20 | **TIMEOUT** | — | **REGRESSION** |
| q21 | 40.4s   | normal | OK |
| q22 | 31.0s   | normal | OK |
| q23 | **TIMEOUT** | — | **REGRESSION** |

## Outcome

**Net**: 8 timeouts + 4 catastrophic regressions (Q8, Q12, Q16, Q18). The 8 KEEP-OK queries are dominated by stateless ops; the heavy-state queries (Q8/Q12/Q16/Q18 + the 8 timeouts) all degraded.

**Q12 regression is the canary**: prior bench landed at 31.5s rocksdb-parity. Current bench at 611.6s = full async-state dispatch path is broken on at least the timer-heavy path.

**Q11 improvement is real**: 76.5 → 72.6s (1.36× faster than rocksdb 98.7s); the on-write-buffer fix continues to compound.

## Root cause hypothesis

Need to investigate (rate-limited, deferred). Candidates:
- Q12 timer/Aggregating path: A4-H2 RMW flushHandler wiring (batch 10) is NOT in this jar — but the underlying problem could be the V2 `setFlushHandler` was no-op in production all along (already proved by Round 4 / Agent A). However Q12 regressing 19× from baseline cannot be the no-op flushHandler alone — the no-op only loses accumulators at snapshot boundary, not at runtime throughput. So Q12 regression is something else.
- Possible culprits batches 1-9 introduced:
  - PR-C3 ReducingAggregatingCache (batch 4) — RMW cache eviction policy
  - PR-D2 SST streaming (batches 7-8) — could have changed scan throughput
  - PR-A1 snapshot (batch 5-6) — synchronous mailbox phases
  - PR-E2 inflight tracking (batch 7) — could have introduced backpressure
- For the **TIMEOUT** queries (Q3/Q4/Q7/Q9/Q19/Q20/Q23): these all involve **JOIN or HOP windows** (Q3=join, Q4=avg-by-category, Q7=tumble, Q9=tumble, Q19=time-tumble, Q20=multi-join, Q23=cross-join). Per project memory `project_q5_q8_q13_structural_gap.md`: V1-sync HOP/JOIN paths can't reach VectorizedExecutor and Flink lacks AsyncStateSlicingSharedWindowProcessor. But the v3 baseline ran these at <200s; if the current run can't finish in 15 minutes, something WORSE than the structural gap has happened.

## Action items (post rate-limit reset 1:20am Asia/Shanghai)

1. Rebuild jar + dylib including batches 10 + 11 commits (`1151723e611` flink, `d6e24999f` ForSt-rs)
2. Re-run bench against the **post-batch-11** artifact
3. If Q12 still regressed: bisect commits 1-9 to find the offending commit
4. Re-dispatch Round 6 verification (5 agents)
5. Continue review-and-fix loop until 5 consecutive clean rounds or 100 rounds total

## What changes if batch 10+11 helped

Batch 10 fixed:
- A4-H1 ListStateV2 CLEAR drain (but A5-H1 showed it was on dead path; batch 11 fixed correctly via onClear)
- A4-H2 RMW flushHandler (closes snapshot-time data loss, not runtime perf)
- A4-H3 snapshot error propagation
- A4-H4 restoreFromHandles symmetric restore

Batch 11 fixed:
- A5-H1/H2/H3 correctness on hot path
- Multiple per-row Arena allocations (3 sites) — **may help Q12 throughput**
- ArrowTimerBuffer.swapHeap byte[24] per heap-sift — **direct hit on Q12 timer queue**
- Off-heap AppendMergeBatchBuffer
- E5-H1 CANONICAL savepoint throw

The ArrowTimerBuffer + per-row Arena fixes are **most likely** to improve Q12. Bench rerun mandatory after rebuild.
