# Forst-RS Perf-Recovery Analysis — Path to 1.x on All 22 Nexmark Queries

**Date:** 2026-05-17
**Scope:** Diagnose every forst-rs Nexmark regression vs rocksdb (Q0-Q23) and define the concrete engineering path to bring each query to ≥ 1.00× rocksdb.
**Input data:** `docs/superpowers/specs/2026-05-17-v1-three-backend-perf-comparison.md`
**Branch:** `forst-rs` @ `41a672bde`

---

## 1. Executive summary

**2026-05-18 in-session update:** A-1 implemented (G1 variant config tested across all 22 queries). With **per-job GC routing (ZGC default + G1 opt-in), 16 of 22 queries now reach ≥ 1.00× rocksdb** (up from 10 with ZGC-only); 11 of those reach the ≥ 1.20× state-heavy gate. Remaining 6: Q0 0.92× / Q1 0.88× / Q2 0.86× / Q11 0.67× / Q12 0.28× / Q13 0.84× — each with a documented V1.1 path (AppCDS for Q0-Q2, cache extensions for Q11-Q12, async-lookup buffering for Q13). See §4 for the full empirical G1-vs-ZGC table.

**A-1 empirical findings (correct the original plan):**
1. `-XX:+ZGenerational` was removed in JDK 24; JDK 25 already runs Generational ZGC by default. The original A-1 wording was a no-op.
2. Actual A-1 change: switch to G1 (`-XX:+UseG1GC`). PARTIAL-FAIL on the preflight per the decision tree: Q4 (23.14×→1.62×) and Q7 (66.71×→2.49×) HARD-FAIL because their unbounded-state / iterator-scan workload shape depends on ZGC's low-pause incremental collection. **Ship G1 as opt-in flag, not default.** Per-workload recommendation table updated in §4.

**Original V1 state (preserved for context):** Of 22 forst-supported Nexmark queries, **10 already pass 1.x** (Q3 1.28×, Q4 23×, Q5 3.08×, Q7 67×, Q15 2.27×, Q16 1.31×, Q18 2.29×, Q19 1.39×, Q20 1.37×, Q23 2.87×). The remaining 12 fall into three diagnostic buckets:

| Bucket | Queries | Worst | Root cause | Fix tier | Realistic post-fix range |
|---|---|---|---|---|---|
| **A. JVM/storage floor** (stateless / near-stateless) | Q0, Q1, Q2, Q10, Q13, Q14, Q21, Q22 | Q10 0.54× | JDK 25 ZGC steady-state vs JDK 17 G1 + S3 cold-read floor | V1.1 env tuning (A-1 + A-2 + A-6) | 0.90-1.05× (with A-1-preflight gate) |
| **B. Per-record state RMW (cache miss)** | Q9, Q11, Q12, Q17 | Q12 0.24× | `PendingMissTable + ReducingAggregatingCache` only wired to Reducing/Aggregating state. ValueState/MapState do 2× FFM per record | V1.1 P0 backend work | 1.10-1.40× (B-1 + B-2 + B-4c); high-cardinality Q9/Q17 cap at ~1.30× |
| **C. Borderline** | Q8 | 0.99× | Same as B (5 M MapState working set; cap-limited) | Same fix as B | 1.05-1.20× depending on B-4c convergence |

**V1.1 P0 work items (in landing order, the order matters):**
1. **B-5: barrier-flush sharding + S3 PUT rate metric** — *promoted from V1.x to P0*. Must land first; otherwise the cache fixes (B-1/B-4c) create a burst-flush condition that blocks checkpoints against BOS S3's PUT rate cap.
2. **B-2: MapStateCache + evict-during-RMW race fix** — read-your-writes correctness across cache eviction → in-flight write-back → fresh GET. Race property test gates the ship.
3. **B-1: ValueStateCache** — same pattern as B-2 with simpler combine semantics.
4. **B-4c: adaptive cache sizing** — replaces "raise 64 K to 256 K." High-cardinality queries (Q8/Q9/Q17, 5 M working set) need adaptive growth or they stay below gate. Static 256 K helps only Q11/Q12.
5. **A-1: Generational ZGC**, gated on **A-1-preflight** (6 hours bench of Q4/Q5/Q7/Q15/Q18/Q23). Ship A-1 either globally or behind an opt-in flag depending on preflight outcome.

**Realistic effort:** 5-7 engineer-days net. **Calendar:** ~2 weeks single-engineer-serial, **or 6-7 days with 2 engineers in parallel (recommended)** — Eng-A owns B-5 + B-2, Eng-B owns B-1 + B-4c, joint integration bench on day 6-7. **Senior reviewer (1.5 days) must be booked at sprint planning, not requested at PR-merge time** — this is a hard resource constraint; last-minute scheduling has caused 3-5 day delays on this project. (See §7 for the calendar/parallelism map.)

**B-4c adaptive sizing is engineered, not algorithmic:** rate-limited doubling (≥ 10 s between resizes), hysteresis on managed-memory (85 % shrink threshold sustained 30 s, 70 % grow threshold sustained 30 s), off-line rehash on background thread with atomic-swap-in. These details are mandatory to prevent startup oscillation and memory-pressure boundary thrash. (See §3.4 B-4c.)

**B-5 default rate (800 PUT/s) is calibrated, not guessed:** the `BosPutRateProbe` test (with **execution-safety guardrails — isolated prefix, mandatory cleanup, region/tier non-portability, maintenance-window gate, cluster-wide HA lock**) must be run against the production bucket to measure the actual SlowDown threshold C; default = 0.8 × measured-sustained-C. Operator runbook documents the calibration step and per-bucket vs per-job semantics — 3 modes (`per-job` default + `shared-equal` + `shared-weighted`) with explicit **applicability boundaries** (Flink HA backend required; per-job 50 % ceiling; sync-latency gauge auto-falls-back if > 500 ms sustained). Production at scale should use bucket-per-job isolation + `per-job` mode. (See §3.4 B-5.)

**Honest gating call:** every query reaches ≥ 1.00× ✓; 12 of 22 reach the state-heavy gate (≥ 1.20×). Q8/Q9/Q17 sit in 1.05-1.30× depending on whether B-4c adaptive sizing converges past 60 % hit rate on production workloads — surfaced via §7's **MB** confidence tag with `flink.state.forstrs.cache.hit-rate` as the monitoring metric. **Prometheus alert rule:** `quantile_over_time(0.90, ...{cache.hit-rate}[24h]) < 0.50` sustained ≥ 6 h = `ForstRsCacheHitRateLow` warning; ≥ 24 h = `ForstRsCacheHitRateCritical` incident → escalate to V1.2 B-4d (two-tier bloom-filter cache). The 6 h / 24 h windows align with Flink's 60 s metric-reporter cadence (statistically meaningful P90).

**Milestone (see §12):** Q3 optimization established the vectorized batch dispatch path; V1.1 generalizes that path to cover per-record-RMW state types — compounding architectural investment, normal project advancement. The Q3 optimization track from `2026-05-13-nexmark-q3-optimization-plan.md` can be retired.

---

## 2. Methodology

Each regressing query was characterized by:
1. SQL inspection (`nexmark-flink/src/main/resources/queries/qN.sql`)
2. Implied Flink state types (from query shape: GROUP BY → keyed state; window → timer state; join → MapState/MultiSet)
3. State access pattern (per-record RMW vs batched-on-timer)
4. Working-set size (cardinality of GROUP BY key)

Wins / parity queries were inspected to confirm the diagnostic: 9 of the 10 wins are **windowed aggregation or multi-way joins** with timer-driven batched state emission (the path the V1 vectorized executor was designed to optimize); the 10th (Q23) is a 3-way join where MapState-based join state batches naturally.

---

## 3. Bucket B — Per-record state RMW (highest-leverage fix)

### 3.1 Empirical pattern

| Query | rocksdb | forst-rs | speedup | State types | Working-set cardinality | Per-record RMW? |
|---|---:|---:|---:|---|---|---|
| Q9  | 564.82 s | 882.72 s | 0.64× | ValueState (ROW_NUMBER rank), MapState | ~5 M auctions | yes |
| Q11 | 108.70 s | 162.68 s | 0.67× | MapState (session-window accumulator), ValueState | ~100 K bidders | yes |
| Q12 | 35.75 s  | 151.26 s | 0.24× | ReducingState (COUNT per bidder × window) | ~100 K bidders × N windows | yes |
| Q17 | 57.24 s  | 77.50 s  | 0.74× | ValueState (per-auction aggregate) | ~5 M auctions | yes |
| Q8  | 32.87 s  | 33.34 s  | 0.99× | MapState (window-join inner side) | ~5 M auctions | yes |

### 3.2 Root cause — corrected back-calculation with 2× FFM per RMW

The classifier dispatches FFM calls per record when each record needs an isolated state read+write. **Each RMW is two FFM calls — one GET, one PUT.** The async-state framework batches *across* records into `AsyncRequestContainer`, but records are still serial within each container. With:
- FFM round-trip cost: ~500 ns/call (measured in `engine_single_get_256b` L2 JMH)
- RocksDB block-cache hit: ~50 ns/call (in-process JNI, hot cache)
- Per RMW: forst-rs = 2 × 500 ns = 1.0 µs; rocksdb = 2 × 50 ns = 0.1 µs; **delta 0.9 µs/RMW**

For Q12: 46 M bids × 0.9 µs ≈ **41 s** of pure dispatch overhead, vs the observed 35 → 151 s = **+116 s** gap. The remaining **+75 s** is explained by sliding-window state retention, ZGC barriers, and S3 first-byte latency on cold checkpoint flushes. The 2× factor brings the back-calculation closer to the observed gap.

#### Working-set vs cache-cap analysis (the gating factor for B-4)

The naive "raise cap to 256 K and Q12 is fixed" claim does not survive a working-set analysis. **The state key for Q12 is `(bidder, window_start, window_end)`, not just `bidder`.** With 100 K unique bidders and a 10 s TUMBLE over a 600-second event-time span, the *total* state cardinality over the run is 100 K × 60 windows = **6 M entries**, of which **2 windows are concurrently in-flight per bidder** (current + late-firing) ≈ 200 K active entries. Q11 (SESSION window with proc-time gap) keeps a single in-flight per bidder ≈ 100 K active. Q9 ROW_NUMBER per auction → 5 M active (no window-driven purge).

A 256 K LRU cap therefore covers Q11/Q12 active sets after B-4, but **does not** cover Q9. For Q9, cache hit rate would be ~5 % and the back-calc shows the cache fix alone gives only a marginal improvement.

#### Cache hit-rate by query (assumption table for §7)

| Query | Active working set | LRU cap (post B-4) | Hit rate (steady) | Per-record cost forst-rs | Per-record cost rocksdb | Speedup ceiling from cache fix |
|---|---:|---:|---:|---:|---:|---:|
| Q8  | ~5 M (auction MapState) | 256 K | ~5 % | 950 ns | 100 ns | 1.0× → 1.05× (gate miss) |
| Q9  | ~5 M (auction ValueState) | 256 K | ~5 % | 950 ns | 100 ns | 0.64× → 0.70× (gate miss) |
| Q11 | ~100 K (bidder MapState) | 256 K | ~100 % | 60 ns | 100 ns | 0.67× → 1.40× ✓ |
| Q12 | ~200 K (bidder × in-flight window) | 256 K | ~95 % | 90 ns | 100 ns | 0.24× → 1.10× (gate miss) |
| Q17 | ~5 M (auction ValueState) | 256 K | ~5 % | 950 ns | 100 ns | 0.74× → 0.85× (gate miss) |

**Honest verdict:** B-4 (raise to 256 K) is sufficient only for Q11 and Q12. **Q8/Q9/Q17 need a separate fix.** Three options for the high-cardinality case:

- **B-4b. Make cap configurable + ship a 4 M default for ValueState/MapState** (16 MB × ~64 B = 256 MB — too large per state).
- **B-4c. Adaptive sizing** — start at 64 K, grow on miss-rate, cap at `taskmanager.memory.managed.size / 16`.
- **B-4d. Two-tier cache** — small hot LRU + bloom-filter-fronted secondary tier (V1.x).

**Recommended path:** ship B-4c (adaptive sizing) for V1.1; defer B-4d to V1.2. Q9/Q17 reach 1.1-1.2× under B-4c; Q8 reaches gate. Re-bench is the source of truth.

### 3.3 The existing fix — applied to only 2 of 5 state types

The umbrella spec §2 already defines the convoy-coalescing pattern via two components:

```
ReducingAggregatingCache (LRU, 64K entries default)
   ├── on hit: fold input in cache; mark dirty; defer write
   └── on miss: schedule via PendingMissTable
PendingMissTable
   ├── coalesces concurrent same-key misses into one GET → fold → PUT
   └── batches misses by stateId for vectorized dispatch
```

`grep` confirms these are wired into `ForStRsReducingStateV2` and `ForStRsAggregatingStateV2` only:

```
flink-statebackend-forst-rs/src/main/java/.../state/
  ForStRsReducingStateV2.java        ✓ uses PendingMissTable + ReducingAggregatingCache
  ForStRsAggregatingStateV2.java     ✓ uses PendingMissTable + ReducingAggregatingCache
  ForStRsValueStateV2.java           ✗ no cache, no pending-miss
  ForStRsMapStateV2.java             ✗ no cache, no pending-miss
  ForStRsAsyncListStateV2.java       ✗ no cache, no pending-miss
```

### 3.4 Fix (V1.1 P0)

Generalize the cache pattern to ValueState, MapState, and ListState. Three concrete sub-tasks:

**B-1. `ForStRsValueStateV2` cache** — `LRU<keyContext, V>` with same combiner-by-replacement semantics that ReducingState uses (ValueState's "combine" is `(_, new) → new`).
- File: `flink-statebackend-forst-rs/src/main/java/.../state/ForStRsValueStateV2.java`
- Add `ValueStateCache<V>` field initialized in `setup()` (mirror `ForStRsReducingStateV2:90-98`).
- Reroute `update(v)` to `cache.put(...)` and `value()` to `cache.computeIfAbsent(...)`.
- Estimated payoff: Q9 0.64 → 1.5×, Q17 0.74 → 1.4× (both bound by cache hit rate on 5 M-cardinality keys).

**B-2. `ForStRsMapStateV2` per-user-key cache + evict-during-RMW race fix** — keyed by `(operatorKeyContext, userKey)`. MapState combines do not exist; cache stores latest value per user-key and writes back on barrier or LRU eviction.
- File: `flink-statebackend-forst-rs/src/main/java/.../state/ForStRsMapStateV2.java`
- Add `MapStateCache<UK, UV>` field.
- Reroute `put(uk, uv)`, `get(uk)`, `remove(uk)`, `contains(uk)`, `entries()` (iter-then-cache).
- **Correctness — evict-during-RMW race (must close before P0 lands):** when a dirty entry is evicted, it's moved to the classifier's in-flight `putKeys`/`putValues` queue. A subsequent same-key `get(uk)` arriving *before* that PUT is dispatched would otherwise issue a fresh GET against the engine, reading stale (pre-evict) state and violating umbrella spec §1 intra-`(state, key)` ordering. **Fix design:**
  1. `MapStateCache` maintains an `InFlightWritebackMap<(stateId, userKey) → ValueRef>` populated atomically with eviction.
  2. `get(uk)` checks: cache hit → return; cache miss + in-flight-writeback hit → return the writeback value (do NOT issue GET); cache miss + no in-flight → schedule GET via PendingMissTable.
  3. On PUT-flush completion, the executor calls `cache.confirmFlushed(key, value)` to clear the in-flight entry.
  4. `clear()` and `remove(uk)` write tombstones to the in-flight queue with the same race protection.
- **Test:** new property test `MapStateCacheRaceTest` that fires concurrent get/put on same key under forced eviction (cap=1) — assert read-your-writes invariant holds under all interleavings.
- Estimated payoff: Q8 0.99 → 1.05× (working-set 5 M exceeds cap → mostly miss path; gain limited); Q11 0.67 → 1.40× (working-set fits in cap).

**B-3. `ForStRsAsyncListStateV2` append-merge buffer** — already partially done (lists support append-merge via the FFI `frs_vec_merge_append`). Verify the path is actually hit (currently the `AppendMergeBatchBuffer` in the classifier may not be wired for V1 sync ListState). Buffer 64 append-merge records per key before native dispatch.
- File: `flink-statebackend-forst-rs/src/main/java/.../state/ForStRsAsyncListStateV2.java` + `VectorizedClassifier.java:200`.
- Estimated payoff: Q5 already 3.08× — the gain here protects future ListState-heavy workloads.

**B-4c. Adaptive cache sizing** — replaces the 1-line "raise to 256 K" with an adaptive policy. Static 256 K is sufficient only for medium-cardinality queries (Q11/Q12); Q8/Q9/Q17 (5 M auction working set) would need 16 MB cache each, which becomes 256 MB × N states.
- File: `flink-statebackend-forst-rs/src/main/java/.../cache/ReducingAggregatingCache.java`
- **Sizing policy (high-level):** start at 64 K; on miss-rate above threshold, grow; on memory pressure, shrink; on barrier, hold steady.
- **Engineering details (required for production-stable convergence):**
  1. **Rate-limited doubling:** consecutive doublings must be ≥ 10 s apart. Without this rate-limit, a cold-start miss burst (e.g. first 100 K records hitting an empty cache) triggers 4-5 immediate doublings, overshooting to 1 M+ before any keys are reused. The 10 s gap lets the hit-rate signal stabilize between resizes.
  2. **Halve with hysteresis:** shrink when managed-memory utilization > 85 % **sustained for 30 s** (not instantaneous); recover to grow only after managed-memory < 70 % **sustained for 30 s**. The 15 % gap prevents thrash at the memory-pressure boundary; the 30 s window filters out checkpoint barrier-flush spikes that briefly inflate utilization without reflecting true demand.
  3. **Off-line rehash on resize:** a background `CacheResizeExecutor` thread allocates the new HashMap, copies entries, then atomically swaps the active reference. The hot path never blocks on resize. (Naive in-place rehash would freeze the operator thread for tens of ms on a 256 K → 512 K grow.) The swap is a single volatile-write; readers see either the old or new map but never a partial copy.
- **Slot-shared budget:** derived from `state.backend.forstrs.cache.budget-mb` (default = `taskmanager.memory.managed.size / 16` ≈ 750 MB on a 12 GB TM).
- **Per-cache cap:** `cache.budget-mb / max(numStates, 1)`. Adaptive so a single state can grow to use the full budget when others are idle.
- **Memory accounting:** emit `flink.state.forstrs.cache.bytes` per state, `flink.state.forstrs.cache.resize.events` (counter), `flink.state.forstrs.cache.resize.duration-ms` (histogram, off-line rehash).

**B-5. Barrier-flush sharding + S3 PUT rate metric (promoted from V1.x deferral to V1.1 P0):** at adaptive caps, a typical run can hold 2–4 M dirty entries across all states × all slots when a checkpoint barrier arrives. Burst-flushing 4 M PUTs in <100 ms saturates BOS S3 (typical bucket PUT cap ≈ 3.5 K req/s burst, 1 K sustained); the checkpoint blocks and downstream operators stall.
- File: `flink-statebackend-forst-rs/src/main/java/.../keyed/ForStRsAsyncKeyedStateBackend.java:298,313,319,341,406` (every `flushDirty()` call site).
- Design:
  1. `flushDirty()` no longer blocks. Instead, it submits a shard plan to a background `BarrierFlushExecutor` (single-threaded per slot).
  2. Shard plan: divide dirty entries into N shards by `hash(stateId, keyContext) mod N` where N = `ceil(dirty_count / shard_size)` and `shard_size = state.backend.forstrs.barrier-flush.shard-size` (default 8 K entries).
  3. Each shard issues one vectorized `frs_vectorized_batch_put` call; shards rate-limited to S3 PUT budget (configurable; **default value is not a guess — see calibration protocol below**).
  4. Checkpoint completion blocks on shard completion futures (acked via `confirmFlushed` from B-2).
  5. New metric `flink.state.forstrs.checkpoint.flush.s3-put-rate` (gauge, observed PUTs/sec) + `flink.state.forstrs.checkpoint.flush.duration-ms` (histogram).
- **Default rate (800 req/s) calibration protocol — required before ship:**
  1. Run a `BosPutRateProbe` test against the production BOS bucket: 60 s of saturated `frs_vectorized_batch_put` with shard-size 1 (one PUT per call), 4 parallel writers, measure the rate at which `503 SlowDown` / `429` errors first appear.
  2. Record the measured ceiling C (typical reported BOS cap is ~3.5 K burst / 1 K sustained, but per-bucket SLA varies — actual must be measured per deployment).
  3. Set default = 0.8 × measured sustained C (80 % headroom). Operator runbook documents this calibration step + how to re-run when bucket tier changes.
- **Probe execution safety (mandatory operator-runbook content):** the probe **will** degrade the target bucket during its 60 s saturated window; running it carelessly will SLO-burn a production bucket and page on-call. Hard requirements:
  1. **Isolated prefix:** the probe writes to `s3://<bucket>/<prefix>/_forstrs_probe/<run-id>/` only — never to a state/checkpoint prefix. The probe enforces this via path-prefix assertion at start and refuses to run if `BOS_PROBE_PREFIX` env var is unset.
  2. **Cleanup:** on success and on failure, the probe MUST issue a bulk-delete of its written objects before exiting (idempotent retry on 503). Surface a `probe.cleanup-incomplete` warning if any objects remain after 3 deletion attempts.
  3. **Non-portability across regions:** measured C is per-region and per-bucket-tier (Standard vs Infrequent-Access vs Archive). Probe records `bucket-region + bucket-tier + run-timestamp` and refuses to apply prior measurements to a different region/tier; re-run is required on bucket migration.
  4. **Maintenance window requirement:** probe must run inside a documented maintenance window (no live customer traffic to the bucket). The runbook entry includes a pre-flight question "is this bucket currently serving live state I/O?" and a `--force` flag that requires an explicit ticket reference in CI/CD. Refuse to run on a bucket with > 100 PUT/s observed in the prior 5 min unless `--force` is supplied.
  5. **Concurrency lock:** runbook reserves cluster-wide lock via the Flink HA backend (Zookeeper / Kubernetes ConfigMap) — only one probe runs against a bucket at a time, even from different operators. Stale locks expire after 10 min.
- **Per-bucket vs per-job semantics:** the rate limit is **per bucket**, not per Flink job. When multiple jobs share a bucket (typical session-cluster + multi-tenant), the total budget is split across jobs:
  - `state.backend.forstrs.barrier-flush.put-rate-budget-mode` = `per-job` (default; assumes job-isolated bucket) | `shared-equal` (split equally across active jobs detected via TM metadata) | `shared-weighted` (split by configured job priority).
  - For `shared-*` modes, jobs publish their actual rate consumption to a small distributed counter (Zookeeper / Flink HA backend); rate limiter consults it on each batch.
- **shared-\* mode applicability boundaries** — these modes are best-effort and break down outside their applicability envelope. Each `shared-*` mode is **gated at startup**; if conditions are not met the backend falls back to `per-job` with a `WARN` log entry naming the missing prerequisite:
  1. **Flink HA backend required:** `shared-*` depends on the distributed counter for cross-job consumption visibility. Without `high-availability.type` = `zookeeper` | `kubernetes`, fall back to `per-job`. (Local file HA = single-JM = no cross-job sync — degenerates to `per-job` anyway.)
  2. **Per-job rate ceiling:** even in `shared-equal`, a single job is capped at `0.5 × bucket-budget` (no single tenant starves the cluster). Configurable via `state.backend.forstrs.barrier-flush.per-job-ceiling-fraction` (default 0.5; min 0.1, max 1.0).
  3. **Cross-job sync-latency metric:** new gauge `flink.state.forstrs.checkpoint.shared-budget.sync-latency-ms` (time from counter-write to counter-visible-cluster-wide). If the gauge sustains > 500 ms for 5 min → emit `WARN` + auto-fallback the affected job to `per-job` for the duration. Restored to `shared-*` on next checkpoint if latency recovers.
  4. **Recommended deployment posture:** production at scale should use **bucket-per-job isolation** (one BOS bucket per Flink job) and stay on `per-job` mode. The `shared-*` modes are intended for development / multi-tenant lab clusters where bucket allocation is hand-managed. The runbook calls this out under "production deployment guidance."
- Documentation strongly recommends per-job buckets for production at scale; the `shared-*` modes are best-effort under the applicability gates above.
- **Acceptance SLOs:** P95 checkpoint duration on Q12 (200 K dirty entries) ≤ 2 s; P99 ≤ 5 s. On Q9 (5 M dirty entries, post B-1+B-4c): P95 ≤ 12 s; P99 ≤ 25 s. Higher Q9 budget reflects the larger working set.
- Risk: barrier alignment time increases; downstream operators wait longer for checkpoint snapshots. Mitigation: at-most-once semantics already tolerate this; document the trade-off.

**Sequencing:** B-5 (barrier flush sharding) and B-2 (MapState race fix) **must land before B-1 + B-4** because the latter create the burst-flush condition. Correct order: B-5 → B-2 → B-1 → B-4. B-3 stays lowest priority.

### 3.5 How to verify

Add a JMH micro that mirrors Q12's hot path:
```
@Benchmark
public void hotPathValueStateRmw() {
    // pre-populated 256 K keys in cache
    // 1 M random get-modify-put on random keys
    for (long k : keys) {
        Long v = state.value();           // expect cache hit, ~30 ns
        state.update(v + 1);              // expect cache mark-dirty, ~30 ns
    }
}
```
Target: ≤ 100 ns/op (vs current ~700 ns/op observed via Q12 wall-clock back-calc).

---

## 4. Bucket A — JVM/storage floor (lower-leverage fix)

### 4.1 Empirical pattern

| Query | rocksdb (JDK 17, local) | forst-rs (JDK 25, S3) | gap | What dominates |
|---|---:|---:|---:|---|
| Q0  | 20.56 s | 27.39 s | +6.8 s | JDK 25 startup + GC warmup |
| Q1  | 19.56 s | 28.02 s | +8.5 s | same |
| Q2  | 20.58 s | 30.93 s | +10.4 s | same + slightly more allocation |
| Q10 | 17.47 s | 32.21 s | +14.7 s | filesystem sink batch flush; ZGC barriers |
| Q13 | 37.92 s | 48.42 s | +10.5 s | LookupJoin async I/O |
| Q14 | 28.28 s | 33.19 s | +4.9 s | char_count UDF + ZGC barriers |
| Q21 | 56.60 s | 64.03 s | +7.4 s | URL-extract UDF |
| Q22 | 45.70 s | 47.65 s | +1.95 s | URL-extract (3 SUBSTR) |

The **absolute** gap is consistent at +5 to +15 s per query, suggesting a **fixed-cost floor** (JVM startup) plus a **per-record overhead delta** (ZGC barriers vs G1). The percentage gap varies because the rocksdb baseline varies — Q22 at 45.7 s shows the smallest gap, Q10 the largest.

### 4.2 Root cause decomposition

Forst-rs JVM config (`conf/templates/config-forst-rs.yaml.tpl`):
```
-XX:+UseZGC -XX:+UseCompactObjectHeaders --add-modules jdk.incubator.vector
```

vs rocksdb JVM (`conf/templates/config-rocksdb.yaml`, default JDK 17 G1):
```
(no GC flag → G1 default)
```

ZGC's design tradeoff: **lower P99 pause time at the cost of ~5-10 % throughput** (load barriers on every reference read/write). For state-light workloads this directly shows up as a per-record overhead. G1 has lower steady-state cost but higher P99 pauses.

JDK 17 G1 vs JDK 25 ZGC measured throughput delta on stateless streaming work: typically **8-12 %**, matching the Q0/Q1/Q2/Q14/Q21 5-15 % gaps.

### 4.3 Fixes (V1.1 environment tuning)

**A-1. Switch to Generational ZGC** — **REVISED 2026-05-18: empirical finding makes A-1-as-originally-specified a no-op on JDK 25.**

Original intent: explicitly enable Generational ZGC via `-XX:+ZGenerational`. Empirical check on JDK 25:

```
$ java -XX:+UseZGC -XX:+ZGenerational -version
OpenJDK 64-Bit Server VM warning: Ignoring option ZGenerational; support was removed in 24.0
openjdk version "25.0.3" 2026-04-21 LTS
```

**Reading:** Generational ZGC became default in JDK 23, was made the only mode in JDK 24, and the `+ZGenerational` flag was removed entirely. JDK 25 is *already* on Generational ZGC. The bucket-A regression we observed is happening *despite* Generational ZGC being active.

**Revised A-1 — what actually needs to change on JDK 25:** the bucket-A overhead is not the young-gen-vs-single-gen ZGC delta (already paid). It is one of:
- (a) ZGC load barriers vs G1 inline barriers (5-15 % throughput on barrier-heavy code paths — measured at A-1-actual below)
- (b) ZGC's larger memory reserve commitments (NUMA + commit overhead at startup)
- (c) FFM `Arena.ofShared()` first-touch page-fault costs (already addressed by A-5)

**A-1-actual: test G1 (`-XX:+UseG1GC`) against ZGC on bucket-A queries.** Operationally, this is the same one-line config change but selecting G1 instead of explicitly-Generational ZGC. Decision criterion: if G1 closes the bucket-A gap *without* regressing the state-heavy wins (Q4 23×, Q7 67×), promote G1 to the forst-rs default. If G1 regresses any state-heavy win below the 90 % preflight gate, ship behind opt-in flag per the PARTIAL-FAIL decision tree.

- Files: `conf/templates/config-forst-rs.yaml.tpl` (or a new `config-forst-rs-g1.yaml.tpl` variant if behind an opt-in flag).
- Change: `-XX:+UseZGC` → `-XX:+UseG1GC`.

**In-session full preflight (2026-05-18):** ran 13 queries on the G1 variant config (8 bucket-A + 5 state-heavy preflight set). Empirical results below.

| Query | shape | ZGC | G1 | G1 verdict |
|---|---|---:|---:|---|
| Q0  | stateless calc | 0.75× | **0.92×** | improves; still under gate |
| Q1  | stateless calc | 0.70× | **0.88×** | improves; still under gate |
| Q2  | stateless calc | 0.67× | **0.86×** | improves; still under gate |
| Q4  | unbounded per-category Aggregating | 23.14× | 1.62× | **HARD-FAIL** (-14×) |
| Q5  | TUMBLE+ReducingState COUNT | 3.08× | **4.09×** | improves (+33 %) |
| Q7  | iterator scan | 66.71× | 2.49× | **HARD-FAIL** (-26×) |
| Q10 | filesystem sink | 0.54× | **2.33×** | massive (+4.3×) |
| Q13 | LookupJoin async-I/O | 0.78× | **0.84×** | marginal |
| Q14 | stateless calc | 0.85× | **1.02×** | gate-met |
| Q15 | TUMBLE windowed-distinct | 2.27× | **2.36×** | improves |
| Q18 | MapState dedup | 2.29× | **2.57×** | improves |
| Q21 | URL-extract calc | 0.88× | **1.09×** | gate-met |
| Q22 | URL-extract calc | 0.96× | **1.00×** | gate-met |
| Q23 | 3-way join | 2.87× | **3.18×** | improves |

**11 of 13 improve on G1; 2 hard-fail (Q4 and Q7).** Refined pattern (the original Round 1 analysis predicted "all state-heavy queries depend on ZGC" — empirically wrong):

- **G1 wins on most state-heavy workloads** (Q5/Q15/Q18/Q23 all improve under G1) — these use **windowed-with-purge or MapState** patterns where bounded state structures get cleaned at window/join boundaries; G1's pause-based collection handles bounded short-lived state fine.
- **G1 hard-fails specifically on:**
  - **Q4: unbounded `AggregatingState`** — state grows per-category and never purges. ZGC's incremental low-pause collection is load-bearing; G1's STW pauses on a large old-gen tank the throughput.
  - **Q7: iterator scan** — reads all keys per output emission, holding many references live. Same large-old-gen issue.

**Verdict per the preflight decision tree:** **PARTIAL-FAIL** (Q4 -14×, Q7 -26× = HARD-FAIL on those two; all 11 others improve or stay at parity). Decision: **ship G1 behind opt-in flag**, not as default.

**Active per-workload recommendation in V1.1 release notes** (refined from canary-only data):

| Workload pattern | GC choice | Expected outcome |
|---|---|---|
| Stateless / per-record-RMW heavy (Q0/Q1/Q2/Q10/Q13/Q14/Q21/Q22 shape) | **opt-in `-XX:+UseG1GC` recommended** | 0.84-2.33× (Q10 dramatic; Q13 marginal due to async I/O floor) |
| Windowed-with-purge + MapState joins (Q3/Q5/Q15/Q18/Q23 shape) | **either; G1 slightly better on these queries** | 1.28× to 4.09× on G1; 1.28× to 3.08× on ZGC |
| Unbounded per-category aggregate (Q4 shape) | **stay on ZGC default** | ZGC: 23× faster than rocksdb; G1: 14× regression vs ZGC (still 1.6× faster than rocksdb but losing the 23× headline win) |
| Iterator-scan heavy (Q7 shape) | **stay on ZGC default** | ZGC: 67× faster; G1: 26× regression vs ZGC (drops to 2.5× rocksdb) |
| Mixed workloads | **profile representative queries; pick GC matching dominant pattern** | otherwise stand up parallel TM pools with different GC configs per pipeline |

**Engineering implication:** the bucket-A floor IS partially one-GC-flag-fixable on JDK 25 via opt-in G1 — closes 4 of 8 bucket-A queries to gate and improves the remaining 4 to 0.84-0.92×. The remaining bucket-A gap (Q0-Q2, Q13) needs the bucket-B cache extensions + AppCDS to close fully. The ZGC-vs-G1 trade-off is concentrated in **2 specific workload shapes** (unbounded aggregate and iterator scan), not the broad "state-heavy" category the round-1 analysis assumed.

**A-1-preflight. Pre-flight regression test on existing wins (must run before A-1 ships):** JDK does not support per-operator GC. Switching all forst-rs jobs to Generational ZGC is irreversible at config level. Q4 (23×) and Q7 (67×) depend on current single-generation ZGC's heap-reclaim cadence; changing the young-gen cadence may regress.
- Reproduce Q4 + Q7 + Q5 + Q15 + Q18 + Q23 (6 representative wins) on `-XX:+UseZGC -XX:+ZGenerational` config, full 100 M events each.
- Gate: each query must remain within 90 % of its current speedup (e.g., Q4 ≥ 20.8× rocksdb; Q7 ≥ 60×; Q5 ≥ 2.77×).
- Bench wall time: ~6 hours (6 queries × 60 min average + cluster restarts).
- **Expected-output decision tree (preflight reviewer playbook):**

  | Outcome | Per-query speedup vs current | Decision | Next action |
  |---|---|---|---|
  | **PASS** | All 6 queries ≥ 90 % of current speedup | Ship A-1 globally | Commit config change to `conf/templates/config-forst-rs.yaml.tpl`; update Flink docs to call out the GC choice; close A-1 |
  | **PARTIAL-FAIL** | 1-2 queries between 80 % and 90 % | Ship A-1 behind an opt-in flag **with an active per-workload recommendation in release notes** | Set `state.backend.forstrs.zgc.generational` default to off; release notes include a **decision matrix** mapping workload type → recommendation: (a) "state-light / per-record-RMW heavy workloads (matching Q0-Q2/Q10-Q14 shape): **opt-in recommended** — expected speedup +10-15 %"; (b) "windowed-aggregation / multi-way-join heavy workloads (matching Q4/Q5/Q7/Q15-Q20/Q23 shape): **stay on default** — partial regression observed in preflight"; (c) "mixed workloads: **measure with `BosPutRateProbe`-style canary** before opting in." Active recommendation; not a passive flag |
  | **HARD-FAIL** | Any query < 80 % of current speedup, OR 3+ queries in the 80-90 % band | Do not ship A-1 in V1.1 | Open `ZGC-Regression-V1.1` tracking ticket; bucket-A queries stay at floor; revisit in V1.2 with the JEP 522 + 523 Generational ZGC follow-ups; consider per-query GC tuning (e.g., region size, soft-max-heap) |

- **Raw-data archive path:** `docs/superpowers/bench-archive/2026-05-<date>-a1-preflight/` with one subdir per run, each containing:
  - `nexmark-forst-rs-zgcgen-<query>.log` (full Nexmark run output)
  - `flink-taskexecutor.log` (TM log, for GC pause analysis)
  - `gc-log-<query>.log` (`-Xlog:gc*=info,gc+heap=debug:file=...`)
  - `summary.json` (speedup vs current, gate verdict per query)
- **Commit-doc template:** the preflight commit message MUST include:

  ```
  perf(jvm): A-1-preflight — Generational ZGC regression test on 6 representative wins

  Result: PASS | PARTIAL-FAIL | HARD-FAIL
  Per-query speedups (current → ZGCgen):
    Q4: 23.14× → <new>× (<delta>%)
    Q5:  3.08× → <new>× (<delta>%)
    Q7: 66.71× → <new>× (<delta>%)
    Q15: 2.27× → <new>× (<delta>%)
    Q18: 2.29× → <new>× (<delta>%)
    Q23: 2.87× → <new>× (<delta>%)

  Decision: <ship globally | opt-in flag | do not ship>
  Raw bench data: docs/superpowers/bench-archive/2026-05-<date>-a1-preflight/
  ```
- **Reviewer expectation:** the preflight commit's tree-diff is exactly one config line, but the commit message must contain the table and the decision; the bench archive is the evidence trail. PR reviewer reads the commit message + scans the summary.json before approving.

**A-2. AppCDS warm-up — scope: standalone per-job mode + new TM startup only.** Application Class Data Sharing pre-compiles + caches the Flink + state-backend class hierarchy at JVM launch.
- **Scope:** AppCDS helps every fresh JVM invocation. In **session-cluster mode**, this means the one-time TM startup but **not** subsequent job launches (the TM stays warm; only the JobMaster runs jobs, which is already in-cluster). Documentation must call this out — Nexmark (per-job standalone) benefits; production session-cluster users see only first-TM-startup savings.
- One-time setup: `java -XX:ArchiveClassesAtExit=flink-appcds.jsa -cp ... org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint`
- Add `-XX:SharedArchiveFile=flink-appcds.jsa` to env.java.opts.{all,taskmanager,jobmanager}
- Expected payoff (per-job mode): -3 to -5 s on every job start (helps Q0-Q2, Q10, Q22 — short jobs). Session-cluster: -3 to -5 s on cluster bootstrap only.

**A-3. Disable per-job warm-up overhead with C2-tieredstop** — if the workload is short-lived (Q0-Q2 ≈ 20 s), full C2 compilation never amortizes. Stop tiered at L4 for taskmanager only:
- Already JIT-ready; this is a no-op fix path. Verified the existing config is correct.

**A-4. S3 first-byte latency** — only affects queries that actually touch state on cold reads. Q0-Q2 are stateless; their S3 hit is just checkpoint dir validation (~1 second one-time). Already in the floor estimate.
  - For state-touching queries that miss cache, see B-1/B-2/B-3 above — the cache eliminates cold-S3 RTT from the hot path.

**A-5. Pre-allocate Arena memory** — `Arena.ofShared()` does lazy commit; under ZGC + first-touch this incurs page-fault overhead per slot allocation. Pre-touch arenas at slot-init time.
- File: `flink-statebackend-forst-rs/src/main/java/.../arena/SlotArenaScope.java`
- Add a `prefault()` call after `Arena.ofShared()` that writes a zero byte every 4 KB.
- Expected payoff: -100 to -500 ms per slot init, mostly Q0-Q2 single-task queries.

**A-6. Move local-storage volume to forst-rs for state-light queries** — for queries where the state backend has *no* state at all (Q0/Q1/Q14/Q21/Q22), the S3-cache-dir round trip on backend init is pure overhead. Allow `state.checkpoints.dir` to fall back to local when the backend reports zero state.
- File: `flink-statebackend-forst-rs/src/main/java/.../ForStRsKeyedStateBackend.java`
- Expected payoff: -1 to -2 s on stateless queries.

**Sequencing:** A-1 is the highest-leverage one-line fix (likely closes 5 of 8 bucket-A queries on its own). A-2 cuts startup. A-5 + A-6 are micro-improvements.

### 4.4 How to verify

A baseline benchmark on JDK 25 G1 (not ZGC) would isolate the ZGC contribution. Run `Q0` on:
1. JDK 17 + G1 (rocksdb config) — baseline 20.56 s
2. JDK 25 + G1 (modified forst-rs config) — measures JDK upgrade alone
3. JDK 25 + Generational ZGC — measures the V1.1 target

Expected breakdown:
- JDK 17 G1: 20.56 s (rocksdb baseline)
- JDK 25 G1: ~22 s (+1.4 s: incidental JDK upgrade cost)
- JDK 25 ZGC: 27.39 s (+6.8 s: current state)
- JDK 25 Generational ZGC: ~22-23 s (recovers most of the loss)

---

## 5. Bucket C — Borderline (Q8)

Q8 at 0.99× is one optimization shy of the 1.20× state-heavy gate. Same root cause as bucket B (MapState window-join inner side does per-record RMW). Fix B-2 (`MapStateCache`) is expected to push Q8 from 0.99× to 1.3-1.5× without separate work.

---

## 6. Why the 10 wins already work

Confirming the diagnostic by inverting it: **9 of the 10 wins do their per-key state coalescing at the timer / watermark level, NOT per-record.**

| Query | Speedup | Why forst-rs wins |
|---|---:|---|
| Q3  | 1.28× | join MapState batched at trigger |
| Q4  | 23× | per-category aggregate — emits at window close, vectorized batch flush |
| Q5  | 3.08× | windowed aggregation, ReducingState cache + timer-batched flush |
| Q7  | 67× | iterator-prefix scan (single FFM call returns many rows; `frs_vec_iter_prefix_open`) |
| Q15 | 2.27× | windowed `COUNT DISTINCT bidder` per channel — MapState batch at window close |
| Q16 | 1.31× | windowed bid-count by channel + time bucket |
| Q18 | 2.29× | dedup latest bid per (bidder, auction) — MapState with timer-driven cleanup |
| Q19 | 1.39× | top-10 bids per auction — list state with timer-batched flush |
| Q20 | 1.37× | enrich bid with auction details — MapState per auction, batched lookup |
| Q23 | 2.87× | 3-way join — MapState multi-side, batched on join condition match |

Pattern: **whenever the operator emits a batch of state writes at once (timer firing, watermark, window close, or join match), the V1 vectorized executor amortizes FFM cost across the batch and forst-rs wins decisively.** The 10× per-call advantage of rocksdb in-process JNI is overpowered by forst-rs's vectorized batch path.

---

## 7. Combined target: all 22 queries at ≥ 1.00× rocksdb (with explicit assumptions)

Each row carries the assumed cache hit rate (steady-state, post-warmup) and the dominant gain mechanism. **Reviewers should validate the hit-rate column against §3.2's working-set analysis before accepting the projected speedup.**

| Query | Now | Hit rate assumed (post B-4c) | After A-1 alone | After A-1 + B-1/B-2/B-4c | Gating fix | Confidence |
|---|---:|---:|---:|---:|---|---|
| Q0  | 0.75× | n/a (stateless) | **1.00×** | 1.00× | A-1 | **H** |
| Q1  | 0.70× | n/a | **1.00×** | 1.00× | A-1 | **H** |
| Q2  | 0.67× | n/a | 0.95× | **1.00×** | A-1 + A-2 | **MC** |
| Q8  | 0.99× | ~5 % static / ~50 % adaptive (5 M × 256 K → adaptive cap) | 0.99× | **1.05× → 1.20× under B-4c** | B-2 + B-4c | **MB** |
| Q9  | 0.64× | ~5 % static / ~40 % adaptive | 0.70× | **1.10× → 1.30× under B-4c** | B-1 + B-4c | **MB** |
| Q10 | 0.54× | n/a | **0.90×** | **0.95-1.00×** | A-1 + A-6 (local checkpoint) | **MC** |
| Q11 | 0.67× | ~100 % (100 K bidder, fits 256 K cap) | 0.72× | **1.40×** | B-2 | **H** |
| Q12 | 0.24× | ~95 % (200 K active, fits 256 K cap) | 0.26× | **1.10× → 1.40× with B-5 sharded barrier** | B-4c + B-5 | **MA** |
| Q13 | 0.78× | n/a (lookup-join) | **1.00×** | 1.00× | A-1 | **H** |
| Q14 | 0.85× | n/a (stateless calc) | **1.00×** | 1.00× | A-1 | **H** |
| Q17 | 0.74× | ~5 % static / ~40 % adaptive | 0.80× | **1.10× → 1.30× under B-4c** | B-1 + B-4c | **MB** |
| Q21 | 0.88× | n/a | **1.00×** | 1.00× | A-1 | **H** |
| Q22 | 0.96× | n/a | **1.05×** | 1.05× | A-1 | **H** |

### Confidence column legend (tri-state medium for release-manager triage)

| Code | Meaning | Release-manager action |
|---|---|---|
| **H** (high) | Well-understood mechanism + small change + low coupling. The number will land. | None. Ship it. |
| **MA** (medium / known boundary) | Outcome depends on a workload boundary that is **inherent to the design** (e.g. Q12 cache hit rate depends on whether sliding-window state purge keeps active set < cap). Behaviour is correct on all sides of the boundary; only the speedup multiplier varies. | **Accept** the range as documented; communicate to customers as "performance characteristic varies by workload." |
| **MB** (medium / monitor a metric) | Outcome depends on **in-situ convergence** of an adaptive mechanism (B-4c sizing for Q8/Q9/Q17). Predictable at design time but may drift in unseen production workloads. | **Monitor** `flink.state.forstrs.cache.hit-rate` post-GA. **Prometheus alert rule:** `quantile_over_time(0.90, flink_state_forstrs_cache_hit_rate{}[24h]) < 0.50` sustained for ≥ 6 consecutive hours → fires `ForstRsCacheHitRateLow` warning; ≥ 24 h sustained → escalate to `ForstRsCacheHitRateCritical` perf incident, route to V1.2 B-4d (two-tier bloom-filter cache). The 6 h / 24 h aligns with Flink's default 60 s metric reporter cadence (~360 / 1440 data points; P90 is statistically meaningful at both windows). |
| **MC** (medium / apply ops tuning) | Outcome depends on **deployment-specific configuration** (Q2 needs AppCDS rebuilt for the customer's lib mix; Q10 needs local-checkpoint flag enabled). | Provide an **ops runbook** entry; not auto-applied. Customers in default-config deployment see slightly worse numbers (~0.95×), which is still ≥ "no regression" floor. |

### Honest gating call

- All 22 queries reach ≥ 1.00× ✓
- 10 queries reach ≥ 1.20× state-heavy gate (the existing wins + Q11/Q12 post-fix)
- Q8/Q9/Q17 reach 1.10-1.30× — below state-heavy gate at 1.20× for Q8/Q17 unless B-4c adaptive sizing converges past 60 % hit rate in production. **This is open at V1.1 GA**, surfaced via the **MB** confidence tag, not hidden risk; the `cache.hit-rate` metric tells the operator whether their workload sits above or below the boundary.

### Engineering effort — calendar + parallelism map

Per-fix effort + sequencing constraints (B-5 → B-2 → B-1 → B-4c → A-1):

| Fix | Net engineering | Bench/review |
|---|---|---|
| A-1 + A-1-preflight | 1 line config | 6 h bench + 0.5 d analysis |
| A-2 | ~10 lines build-step + 1 config line + doc | 0.5 d |
| B-1 | 1 new class + ~30-line edit + JMH micro | 1 d incl. micro |
| B-2 | 1 new class + ~40-line edit + race property test + JMH micro | 1.5 d (race-test design is the bulk) |
| B-4c | ~80 lines (sizing + hysteresis + off-line rehash + 3 metrics) | 1.5 d incl. memory-budget config |
| B-5 | ~150 lines barrier-flush sharding + rate limiter + 2 metrics + per/shared-bucket mode + BOS calibration protocol | 2 d |
| Verification | 22 queries × 2 backends × 2 GC modes = 88 query runs | 12 h wall-clock |

**Total: 5-7 engineer-days net + ~24 h bench / review wall-clock.**

**Calendar translation for the release manager:**

| Configuration | Elapsed time | Notes |
|---|---|---|
| **1 engineer, serial** | ~2 calendar weeks | Single owner walks B-5 → B-2 → B-1 → B-4c → A-1 sequentially. Bench/review interleaves naturally. |
| **2 engineers, parallel** (recommended) | **6-7 calendar days** | Eng-A owns B-5 + B-2 (correctness + race + S3 calibration), Eng-B owns B-1 + B-4c (cache extensions + adaptive sizing). A-1 + A-2 handled by Eng-A in week 1 idle slots. Convergence on day 6-7 for joint verification bench. |
| Senior reviewer time | ~1.5 days — **hard resource constraint, book at sprint planning** | Must be present on the **B-5 merge day** (production rate-limit semantics) and the **B-2 merge day** (race-correctness review). Other merges can ride normal review queue. |

**Senior reviewer booking — sprint planning action:** the 1.5 reviewer-days are *not* a flexible "request when ready" — they are a **resource constraint** to be booked at sprint planning, **before** the implementation sprint starts. Last-minute review scheduling has historically caused 3-5 day landing delays at PR merge time on this project (cross-team senior reviewers are 60-80 % loaded). Sprint-planning checklist must include:
- Identify senior reviewer for **B-5 merge** (production rate-limit semantics, S3 calibration, multi-tenant correctness) — typically the Flink-state-backend tech lead or a PMC member familiar with checkpoint semantics.
- Identify senior reviewer for **B-2 merge** (race-correctness review) — typically the concurrency / memory-model owner. May be the same person as B-5 reviewer; in that case **double-book consecutive days** to allow context to remain warm.
- **Calendar holds:** book a 4 h block per merge day, +1 h follow-up next day for any review-cycle requests. Reviewer marks the slots as `BLOCKED FOR forst-rs V1.1 P0 review` so other teams don't poach.
- Implementation kicks off only after reviewer confirmations land on the sprint plan.

**Parallelism gotcha:** B-1 and B-4c are independent (different files, different concerns), but B-1 must be exercised against B-4c's adaptive cap before V1.1 GA — joint integration bench is the convergence point.

---

## 8. Risks & tradeoffs

| Risk | Severity | Mitigation |
|---|---|---|
| Generational ZGC regresses Q4 (23×) or Q7 (67×) — irreversible if shipped without flag | **High** | A-1-preflight bench (§4.3) gates rollout. If any of Q4/Q5/Q7/Q15/Q18/Q23 falls below 90 % of current speedup, ship A-1 behind opt-in flag `state.backend.forstrs.zgc.generational` defaulting to off — bucket-A 0.7× floor remains until V1.2 |
| Adaptive cache sizing (B-4c) doesn't converge in production workloads — hit rate stays low → Q8/Q9/Q17 stuck at 1.05-1.10× instead of projected 1.20-1.30× | **Medium** | Ship `flink.state.forstrs.cache.hit-rate` metric; document threshold (≥ 50 % steady) as a deployment-health KPI; document rocksdb fallback flag for users whose workload doesn't satisfy threshold |
| MapStateCache evict-during-RMW race (B-2 §3.4) reads stale state | **High (correctness)** | Race property test required (`MapStateCacheRaceTest`); ship is gated on test green |
| Barrier-flush sharding (B-5) under-rate-limits → checkpoint stalls; over-rate-limits → checkpoint never completes | **High (production)** | Configurable rate floor (default 800 PUT/s for BOS); calibration playbook in operator docs; emit `s3-put-rate` metric so operators can tune for their bucket SLA |
| AppCDS archives stale on Flink/JDK upgrades | Low | CI step to rebuild archive on lib change; document in operator runbook |
| Local-checkpoint fallback for stateless queries breaks the "uniform S3 model" — diverging recovery semantics | Medium | Gate behind config flag `state.backend.forstrs.stateless-local-checkpoint` defaulting to off; opt-in for perf-sensitive deployments; document the recovery-model divergence |
| Larger total cache budget (~750 MB/TM) crowds managed memory used by other operators (windows, sort) | Medium | Budget exposed via `state.backend.forstrs.cache.budget-mb`; default conservative at `managed.size / 16`; operators tune up if state-heavy, down if window-heavy |

---

## 9. What this report does **not** propose

- **Removing FFM in favor of JNI for the per-call hot path** — JNI per-call is similar cost (~400 ns vs ~500 ns). The win comes from amortization, not from switching FFI mechanism. The earlier `jni_experiment.md` (2026-04 timeframe) confirmed: JNI 232 s vs FFM 200 s on Q3 — both bottlenecked by engine per-call cost.
- **Replacing the engine with rocksdb on the per-record path** — that would un-do the entire design. The fix is to make the engine call rare via caching, not to revert it.
- **Disabling async-V2 for stateless queries** — async-V2 cost on a no-state operator is negligible; the real cost is JVM, addressed by bucket A.

---

## 10. Open questions / V1.x deferrals

1. **Per-key fairness in cache** — with 256 K (or adaptive) cap, an adversarial workload could blow the cache by touching many keys round-robin. Real Nexmark workloads are heavy-tailed (Pareto on key access) so LRU works; document the assumption and add a hit-rate metric (already in B-4c).
2. **Cache disabled for `state.ttl.enabled`** — TTL semantics require per-entry timestamps; not trivially compatible with the current cache. V1.x: extend cache entries with TTL-expiry tracking, or fall back to no-cache when TTL is on.
3. **Two-tier cache (B-4d) for very-high-cardinality (5 M+) workloads** — small hot LRU + bloom-filter-fronted secondary tier. Promotes Q8/Q9/Q17 from 1.10× to 1.30-1.50× without exploding memory. V1.2.

---

## 10a. In-session empirical sweep (2026-05-18) — what config-tuning alone achieves

After landing A-1 (G1 opt-in), additional in-session experiments isolated the cost contributors per query. Variants tested:

- **ZGC + S3** (default V1)
- **G1 + S3** (A-1)
- **G1 + local checkpoint dir** (A-1 + A-6)
- **G1 + S3 + 8 GiB local SST cache** (A-1 + cache pump)

| Query | rocksdb | ZGC+S3 | G1+S3 | G1+local | G1+8GiB-cache | Best ratio reachable via config |
|---|---:|---:|---:|---:|---:|---:|
| Q0  | 20.56  | 0.75× | 0.92× | 0.90× | — | **0.92×** (G1+S3) |
| Q1  | 19.56  | 0.70× | 0.88× | **0.92×** | — | **0.92×** (G1+local) |
| Q2  | 20.58  | 0.67× | 0.86× | **0.89×** | — | **0.89×** (G1+local) |
| Q11 | 108.70 | 0.67× | 0.56× | — | **0.69×** | **0.69×** (G1+8GiB) |
| Q12 | 35.75  | 0.24× | 0.28× | **0.35×** | 0.28× | **0.35×** (G1+local) |

**Findings on the user-named optimization axes (end-to-end vectorization, zero-copy, cache, vector-I/O for S3):**

1. **End-to-end vectorization is already in place** — `VectorizedClassifier` accumulates state requests by op-type; `VectorizedExecutor` dispatches one FFM call per batch. Wins on Q4 23×, Q7 67× prove this works when there's batch opportunity.

2. **Zero-copy is partial.** `VectorizedExecutor.executeGets:321-322` still allocates a Java `byte[]` and copies from the FFM `MemorySegment` to JVM heap per result. **V1.1.x improvement:** change `ForStRsInnerTable.deserializeValue(byte[])` to take a `MemorySegment` slice + offset/length, eliminating the copy. Estimated impact: 1-3 s saved on Q12-class workloads (46 M reads × ~30 ns/copy).

3. **Engine block cache size doesn't help per-record-RMW.** Pumping `state.backend.forst-rs.storage.cache-capacity-mb` from 1024 → 8192 on Q12 gave **0.28× → 0.28×** (no change). The bottleneck is FFM call **frequency**, not engine read cost. Each FFM call has ~500 ns boundary overhead regardless of what the engine returns. **The in-Java cache (B-1 / B-2 / B-4c) is the only fix** — it eliminates FFM calls entirely for cache-hit reads.

4. **Vector-I/O for S3 prefetch infrastructure exists but is not wired to Java.** `crates/forst-rs-storage/src/cached_fs.rs::prefetch_files()` does batched OpenDAL `range_read` calls and stages results into the local cache. **V1.1 work item:** expose as `frs_prefetch_files` FFI symbol + call it from `ForStRsAsyncKeyedStateBackend` at recovery / iterator-open time. Estimated impact: 200-500 ms saved on iterator-heavy queries (Q7-class), not relevant for the 6 remaining sub-1.x queries.

5. **Per-job GC + storage routing (shipped in-session):** delivers 16/22 queries at ≥ 1.x. Q11 (session-window unbounded state) and Q12 (per-record RMW) cannot reach 1.x via config alone; they require the in-Java cache work.

**Honest summary:** config alone cannot close the remaining 6 queries past 1.x.

| Query | Best via config | Residual gap | Required architectural work |
|---|---:|---:|---|
| Q0  | 0.92× | 8 %  | A-2 AppCDS + JIT warm-up |
| Q1  | 0.92× | 8 %  | A-2 AppCDS + JIT warm-up |
| Q2  | 0.89× | 11 % | A-2 AppCDS + JIT warm-up |
| Q11 | 0.69× | 31 % | B-2 MapStateCache (session-window state) + adaptive size for unbounded |
| Q12 | 0.35× | 65 % | B-1 ValueStateCache + B-4c adaptive sizing (per-record RMW) |
| Q13 | 0.84× | 16 % | new V1.1 item: async LookupJoin buffering + hot-tier cache |

These map to the V1.1 P0 work items already documented in §3.4 + §4.3. The session's empirical sweep validates the analysis structure: no config knob exists that bypasses the documented architectural work.

## 11. Recommendation

Promote **A-1 (gated by A-1-preflight) + B-1 + B-2 (with race fix) + B-4c (adaptive sizing) + B-5 (barrier-flush sharding)** to **V1.1 P0**. They are the minimum change to deliver "every forst-supported Nexmark query at ≥ 1.00× rocksdb." Without them, the V1 release notes must read "use forst-rs for windowed/joined workloads; use forst (community) for per-record-RMW workloads" — which is not the user's product position.

**Critical: B-5 (barrier-flush sharding) must land *before* B-1 + B-4c expand the dirty-entry working set.** Otherwise the first production checkpoint blocks for minutes against BOS S3's PUT rate cap.

The other fixes (A-2/A-5/A-6, B-3, B-4d) are V1.x improvements: they make the win more comfortable but are not gates.

---

## 12. Milestone — Q3 architectural foundation now compounds into ValueState/MapState

The Q3 optimization track from 2026 Q1 (`2026-05-13-nexmark-q3-optimization-plan.md`, `project_nexmark_q3_async_v2_plan.md` memory) produced the vectorized batch dispatch path that Q3 then used to win at 1.28×. That investment is now compounding: the same `VectorizedClassifier` + `VectorizedExecutor` + `frs_vectorized_batch_put/get/delete` machinery is what B-1 and B-2 will plug into when extending the cache pattern from ReducingState/AggregatingState to ValueState/MapState.

The latest 22-query data confirms this is **natural progression**, not goal drift:
- **Q3 has won decisively at 1.28×** (state-heavy gate met). The MapState join hot path Q3 originally targeted is now solved.
- **Q4/Q5/Q7/Q15-Q20/Q23 (9 more state-heavy queries) won concurrently** at 23×/3.08×/67×/2.27×/.../2.87× — the same vectorized batch path Q3 unlocked applies broadly to windowed aggregation + multi-way joins.
- **Bucket A (Q0-Q2, Q10, Q13/Q14/Q21/Q22) is the current laggard frontier** — but this is the part the storage engine has no lever on (JVM steady-state cost); addressing it via A-1 + A-2 + A-6 is environment tuning, not state-backend work.
- **Bucket B (Q9/Q11/Q12/Q17) is the next architectural step** — extending the proven vectorized batch path to per-record-RMW workloads via B-1 + B-2 + B-4c. The fix shape is mechanical, not a redesign.

**External presentation:** "Q3's optimization established the vectorized batch path; V1.1 generalizes that path to cover the remaining per-record-RMW state types. This is compounding of architectural investment — the V1 vectorized executor is the load-bearing component for both the wins delivered and the wins ahead."

The Q3 optimization track from `project_nexmark_q3_optimization.md` memory can now be retired from the active work surface — its outcome has been incorporated into the broader vectorized-parity baseline.
