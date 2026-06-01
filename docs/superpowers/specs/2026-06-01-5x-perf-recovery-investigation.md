# forst-rs 5× perf-recovery investigation (2026-06-01)

**Goal:** forst-rs+local+JDK25 ≥ **5×** vs rocksdb+local+JDK17 (total q0-q22); forst-rs+S3+JDK25 = **1–2×** vs rocksdb+local+JDK17. (v3.8 already hit ~3×; target is 5×.)

**Autonomy directive (user, 2026-06-01):** "no more interact until the goal achieves… keep on and try your best… write docs in docs/superpowers for confirms/performances/redesigns." Bisect-against-v3.8 first to pin the regression.

## Status of the non-benchmark goal parts (DONE this session)
- E2E vectorization / batch / zero-copy (V2 multiGet-batched; V1 statebuf/append-merge/chunked-iter/batch-delete) — done + verified.
- All flink/forst GHA green (ForSt ci-rust 8/8, ci-security, ci-cross-engine-bench; flink ci-forst-rs @7c1a6a9ab after reverting a CI-fork-destabilizing test).
- ForsT capability parity incl. **incremental shared-SST checkpoints verified** (ckpt2 referenced 34193 SST bytes / 0 re-uploaded, uploaded only the 920 B delta).

## Benchmark ground truth (local, 100M, this session)
rocksdb-JDK17 baseline (all completed):
| query | rocksdb time | throughput |
|---|---|---|
| q7 (tumble+join) | 941.1 s | 106 K/s |
| q9 (join) | 522.2 s | 191 K/s |
| q16 (join) | 314.1 s | 318 K/s |
| q20 (join) | 417.9 s | 239 K/s |
| q3 (light) | 25.2 s | 3.97 M/s |
| q12 (light) | 32.5 s | 3.08 M/s |

forst-rs-JDK25:
| query | forst-rs | ratio |
|---|---|---|
| q3 | 29.4 s | 0.86× (source-bound — expected, can't drive 5×) |
| q12 | 33.5 s | 0.97× (source-bound) |
| **q7** | **>1200 s → TIMED OUT (watchdog-killed)** | **<0.78×, regressed** |
| q9/q16/q20 | not run (q7 timeout aborted forst-rs side) | — |

**Conclusion: forst-rs is in a heavy-query regression, NOT 5×.** Light queries are source-bound parity (the 5× can only come from heavy joins/windows). q7 timing out reproduces the project's 2026-05-29 finding (0.75× total, 8 heavy-query timeouts) vs the earlier v6f sweep (4.69× total, q7 ~22×).

## Bisect anchors
- **v3.8 baseline = flink commit `e0809571483`** ("size-aware AutoTuner + ValueState", `ForStRsKeyedStateBackend` = 1177 lines — matches the project's v3.8 marker).
- Growth `1177 → 1859` = the "fix(forst-rs-backend): harden …" batches (batch-14 … batch-66) — regression suspects (53+ commits of defensive guards with hot-path cost). This session added `1859 → 2192` (V1 vectorization).
- Per-step git-bisect with a 100M heavy sweep (~1–2h/step) is infeasible → using **hot-path diff + sampling profiler** instead.

## Config facts (config-forst-rs-local.yaml.tpl)
- `table.exec.async-state.enabled: true`, `mini-batch.enabled: false` → **joins use the V2 async (vectorized/batched) path**.
- BUT Flink has **no async-state window operator** (no AsyncStateSlicingSharedWindowProcessor) → **window ops (TUMBLE in q7) fall back to V1-sync per-record aggregation** → prime regression suspect.
- Checkpoint interval 30s EXACTLY_ONCE — matches rocksdb (not the differentiator).
- `setCurrentKey` hot method is lean (serialize-key + bytesEqual + generation bump) — no smoking gun.

## Method
1. **Sampling profiler** (`scripts/profile-q7-jstack.sh`) on a short forst-rs-local q7 run → find the dominant hot frames (window-agg? join? GC? FFM? lock? merge-chain?).
2. Cross-reference the hot frame with the v3.8→HEAD diff of that method → confirm added cost.
3. Strip the per-record hot-path cost; re-validate with a heavy sweep (q7).
4. Iterate; document each finding/decision here.

## Honest constraints
- **S3 1–2× is not validly measurable on this dev Mac** (~10 MB/s uplink to remote BOS → all S3 runs uplink-bound). That target needs the co-located 8c32g box.
- 5× is a deep, partly-structural recovery (V1-sync window path) that prior sessions did not fully close; each validation is a ~1–2h heavy sweep. Progress will be incremental and is documented here.

## Re-validation: the q7 timeout is REAL (not the port artifact)
A zombie NexMark `Benchmark` JVM held metric port 9098, making queries fail-to-bind and hang →
watchdog-killed as "timeout". After killing it + freeing 9098, a clean forst-rs q7 run **still
timed out (>1100s, job RUNNING, warmup done)** vs rocksdb 941s. So forst-rs genuinely has bad
heavy-query performance. (Harness lesson: `pkill run_query.sh` leaves the child Benchmark JVM
holding 9098 + the profiler needs the SQL gateway on 8083 started, not just start-cluster.)

## Profiler result — ROOT CAUSE (jstack, 25 samples of a RUNNING q7)
```
q7 = AsyncStateStreamingJoinOperator (V2 async join path — confirmed, NOT V1-sync)
  VectorizedExecutor.executeBatchRequests   85
    executeIters                            65   ← DOMINANT
      ForStRsDBIterRequest.process          65
        openVecIterIntoBuf                  61
          ForStRsLinker.frsVecIterPrefixOpen 61  (FFM downcall)
    executeGets / vectorizedBatchGet        20
```
**The q7 (and likely q9/q16/q20 join) bottleneck is per-record join-probe prefix-iterator OPENS**
(`frsVecIterPrefixOpen`), not gets or puts. The async join issues a prefix-scan per probe to find
matching records under the join key; each scan OPENS a fresh prefix iterator = a k-way merge over
(active memtable + every L0 SST), seeking each source. The OPEN dominates (61/85), not NEXT (which
is already chunk-batched). As periodic checkpoint flushes accumulate L0 SSTs, each open touches
O(num_L0) sources → the wall (matches the recorded "join probe opens O(num_L0) SSTs" finding).

**Highest-impact lever to investigate:** does the prefix-iterator open BLOOM-PRUNE non-matching L0
SSTs (rocksdb's key advantage on lookups) or blindly seek every SST? If no pruning, every open
pays O(num_L0); adding bloom-prune-on-open would cut it toward O(matching-SSTs). Also: block-cache
reuse of index/data blocks across opens; keeping L0 small via compaction.
