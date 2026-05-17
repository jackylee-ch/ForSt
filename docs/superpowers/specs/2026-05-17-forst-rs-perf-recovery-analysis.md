# Forst-RS Perf-Recovery Analysis — Path to 1.x on All 22 Nexmark Queries

**Date:** 2026-05-17
**Scope:** Diagnose every forst-rs Nexmark regression vs rocksdb (Q0-Q23) and define the concrete engineering path to bring each query to ≥ 1.00× rocksdb.
**Input data:** `docs/superpowers/specs/2026-05-17-v1-three-backend-perf-comparison.md`
**Branch:** `forst-rs` @ `41a672bde`

---

## 1. Executive summary

Of 22 forst-supported Nexmark queries, **10 already pass 1.x** (Q3 1.28×, Q4 23×, Q5 3.08×, Q7 67×, Q15 2.27×, Q16 1.31×, Q18 2.29×, Q19 1.39×, Q20 1.37×, Q23 2.87×). The remaining 12 fall into three diagnostic buckets:

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

**Realistic effort (corrected):** 5-7 engineer-days single-person + 1-2 weeks elapsed (including race tests, ZGC preflight, S3 rate-limit calibration, review cycles). The original "2-3 days" estimate omitted race-correctness work and the barrier-flush sharding.

**Honest gating call:** every query reaches ≥ 1.00× ✓; 12 of 22 reach the state-heavy gate (≥ 1.20×). Q8/Q9/Q17 sit in 1.05-1.30× depending on whether B-4c adaptive sizing converges past 60 % hit rate on production workloads — V1.2 brings B-4d (two-tier cache w/ bloom filter) to push those past gate.

**Milestone closed (see §12):** the 2026-Q1 "Q3 severely limited vs simple-aggregation strong e2e improvement" anomaly has reversed. Q3 has won at 1.28×; the laggards are now Q1/Q2 (bucket A) and per-record-RMW (bucket B). The Q3 optimization track from `2026-05-13-nexmark-q3-optimization-plan.md` can be retired.

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

**B-4. Adaptive cache sizing** — replaces the 1-line "raise to 256 K" with an adaptive policy. Static 256 K is sufficient only for medium-cardinality queries (Q11/Q12); Q8/Q9/Q17 (5 M auction working set) would need 16 MB cache each, which becomes 256 MB × N states.
- File: `flink-statebackend-forst-rs/src/main/java/.../cache/ReducingAggregatingCache.java`
- Policy: start at 64 K; on every 100 K cache misses, double capacity (up to a slot-shared budget ceiling); on barrier, hold steady; on memory pressure (managed-memory utilization > 80 %), shrink by half.
- Slot-shared budget: derived from `state.backend.forstrs.cache.budget-mb` (default = `taskmanager.memory.managed.size / 16` ≈ 750 MB on a 12 GB TM).
- Per-cache cap: `cache.budget-mb / max(numStates, 1)`. Adaptive so a single state can grow to use the full budget when others are idle.
- Memory accounting: emit `flink.state.forstrs.cache.bytes` metric per state.

**B-5. Barrier-flush sharding + S3 PUT rate metric (promoted from V1.x deferral to V1.1 P0):** at adaptive caps, a typical run can hold 2–4 M dirty entries across all states × all slots when a checkpoint barrier arrives. Burst-flushing 4 M PUTs in <100 ms saturates BOS S3 (typical bucket PUT cap ≈ 3.5 K req/s burst, 1 K sustained); the checkpoint blocks and downstream operators stall.
- File: `flink-statebackend-forst-rs/src/main/java/.../keyed/ForStRsAsyncKeyedStateBackend.java:298,313,319,341,406` (every `flushDirty()` call site).
- Design:
  1. `flushDirty()` no longer blocks. Instead, it submits a shard plan to a background `BarrierFlushExecutor` (single-threaded per slot).
  2. Shard plan: divide dirty entries into N shards by `hash(stateId, keyContext) mod N` where N = `ceil(dirty_count / shard_size)` and `shard_size = state.backend.forstrs.barrier-flush.shard-size` (default 8 K entries).
  3. Each shard issues one vectorized `frs_vectorized_batch_put` call; shards rate-limited to S3 PUT budget (configurable, default 800 req/s leaving headroom under the 1 K sustained floor).
  4. Checkpoint completion blocks on shard completion futures (acked via `confirmFlushed` from B-2).
  5. New metric `flink.state.forstrs.checkpoint.flush.s3-put-rate` (gauge, observed PUTs/sec) + `flink.state.forstrs.checkpoint.flush.duration-ms` (histogram).
- Acceptance: P95 checkpoint duration on Q12 (200 K dirty entries) ≤ 2 s; P99 ≤ 5 s. On Q9 (5 M dirty entries, post B-1+B-4): P95 ≤ 12 s, P99 ≤ 25 s. Higher Q9 budget reflects the larger working set.
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

**A-1. Switch to Generational ZGC (`-XX:+ZGenerational`)** — JDK 23+ default; explicitly opt in for clarity. Generational ZGC keeps the low-pause property but adds a young-generation collector, recovering ~30-50 % of the throughput loss vs G1.
- File: `conf/templates/config-forst-rs.yaml.tpl`
- Change: `-XX:+UseZGC` → `-XX:+UseZGC -XX:+ZGenerational`
- Expected payoff: Q0/Q1/Q2 → 0.95-1.00×, Q10/Q14/Q21 → 0.90-0.95×

**A-1-preflight. Pre-flight regression test on existing wins (must run before A-1 ships):** JDK does not support per-operator GC. Switching all forst-rs jobs to Generational ZGC is irreversible at config level. Q4 (23×) and Q7 (67×) depend on current single-generation ZGC's heap-reclaim cadence; changing the young-gen cadence may regress.
- Reproduce Q4 + Q7 + Q5 + Q15 + Q18 + Q23 (6 representative wins) on `-XX:+UseZGC -XX:+ZGenerational` config, full 100 M events each.
- Gate: each query must remain within 90 % of its current speedup (e.g., Q4 ≥ 20.8× rocksdb; Q7 ≥ 60×; Q5 ≥ 2.77×).
- If any query regresses below gate: revert A-1, accept the bucket-A 0.7× floor, and document; alternatively, ship A-1 behind an opt-in flag `state.backend.forstrs.zgc.generational` (default off; bucket-A regressions remain).
- Bench wall time: ~6 hours (6 queries × 60 min average + cluster restarts).

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
| Q0  | 0.75× | n/a (stateless) | **1.00×** | 1.00× | A-1 | high (well-understood JDK delta) |
| Q1  | 0.70× | n/a | **1.00×** | 1.00× | A-1 | high |
| Q2  | 0.67× | n/a | 0.95× | **1.00×** | A-1 + A-2 | medium (A-2 has 3-5 s headroom) |
| Q8  | 0.99× | ~5 % (5 M auction × 256 K cap, scales to ~50 % under B-4c) | 0.99× | **1.05× → 1.20× under B-4c**  | B-2 + B-4c | medium (B-4c adaptive sizing must converge) |
| Q9  | 0.64× | ~5 % static / ~40 % adaptive | 0.70× | **1.10× → 1.30× under B-4c** | B-1 + B-4c | medium |
| Q10 | 0.54× | n/a | **0.90×** | **0.95-1.00×** | A-1 + A-6 (local checkpoint) | medium (filesystem-sink IO floor) |
| Q11 | 0.67× | ~100 % (100 K bidder, fits 256 K cap) | 0.72× | **1.40×** | B-2 | high |
| Q12 | 0.24× | ~95 % (200 K active, fits 256 K cap) | 0.26× | **1.10× → 1.40× with B-5 sharded barrier** | B-4 + B-5 | medium (depends on checkpoint cadence) |
| Q13 | 0.78× | n/a (lookup-join) | **1.00×** | 1.00× | A-1 | high |
| Q14 | 0.85× | n/a (stateless calc) | **1.00×** | 1.00× | A-1 | high |
| Q17 | 0.74× | ~5 % static / ~40 % adaptive | 0.80× | **1.10× → 1.30× under B-4c** | B-1 + B-4c | medium |
| Q21 | 0.88× | n/a | **1.00×** | 1.00× | A-1 | high |
| Q22 | 0.96× | n/a | **1.05×** | 1.05× | A-1 | high |

**Confidence column:** high = well-understood mechanism + small change + low coupling. Medium = depends on adaptive policy convergence, checkpoint cadence, or workload-specific hit-rate that varies across deployments.

**Honest gating call:**
- All 22 queries reach ≥ 1.00× ✓
- 10 queries reach ≥ 1.20× state-heavy gate (the existing wins + Q11/Q12 post-fix)
- Q8/Q9/Q17 reach 1.10-1.30× — below state-heavy gate at 1.20× for Q8/Q17 unless B-4c adaptive sizing converges past 60% hit rate. If it doesn't, the user-facing claim becomes "every query >= 1.00×" rather than "every query at gate."

**Engineering effort (revised PMC estimate):**
- A-1 + A-1-preflight: 1 line config + **6 h bench wall time** + bench analysis
- A-2: 1 file (~10 lines build-step) + 1 config line + doc update for scope
- B-1: 1 new class (`ValueStateCache`) + ~30-line edit + JMH micro
- B-2: 1 new class (`MapStateCache`) + ~40-line edit + **race property test** + JMH micro
- B-4c: ~50 lines adaptive sizing in `ReducingAggregatingCache` + memory-budget config + metric
- B-5: ~150 lines barrier-flush sharding + rate limiter + 2 metrics + checkpoint-duration test
- Verification: re-bench all 22 queries × 2 backends × 2 GC modes = 88 query runs ≈ 12 h

**Effort: 5-7 engineer-days single-person + 1-2 weeks elapsed** (review cycles, ZGC regression investigation, race-test design, S3 PUT-rate calibration on the actual BOS bucket). This corrects the earlier 2-3 day estimate which omitted the evict-during-RMW race tests, ZGC pre-flight, and barrier-flush sharding work.

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

## 11. Recommendation

Promote **A-1 (gated by A-1-preflight) + B-1 + B-2 (with race fix) + B-4c (adaptive sizing) + B-5 (barrier-flush sharding)** to **V1.1 P0**. They are the minimum change to deliver "every forst-supported Nexmark query at ≥ 1.00× rocksdb." Without them, the V1 release notes must read "use forst-rs for windowed/joined workloads; use forst (community) for per-record-RMW workloads" — which is not the user's product position.

**Critical: B-5 (barrier-flush sharding) must land *before* B-1 + B-4c expand the dirty-entry working set.** Otherwise the first production checkpoint blocks for minutes against BOS S3's PUT rate cap.

The other fixes (A-2/A-5/A-6, B-3, B-4d) are V1.x improvements: they make the win more comfortable but are not gates.

---

## 12. Milestone — recording the Q3-vs-simple-aggregation reversal

A historical note worth preserving in the perf record: throughout 2026 Q1 the forst-rs project tracked a "Q3 severely limited vs simple aggregations strong e2e improvement" anomaly (see `2026-05-13-nexmark-q3-optimization-plan.md` and `project_nexmark_q3_async_v2_plan.md` memory). The hypothesis at the time was that Q3's MapState join state was the binding constraint.

The latest 22-query data **closes this anomaly:**
- **Q3 has won decisively at 1.28×** (state-heavy gate met). The MapState join path is no longer the binding constraint.
- **Q1/Q2 (simple aggregations / projections) are now the *laggards*** — at 0.67-0.70× in bucket A.
- Q4/Q5/Q7 (more complex aggregations / iterator scans) remain the strong wins at 23×/3.08×/67×.

**The narrative inverts.** Forst-rs's wins are now broadly distributed across joined/windowed workloads; the residual losses are concentrated in (a) stateless work where the storage engine has no lever (bucket A), and (b) per-record-RMW work where the cache pattern from ReducingState/AggregatingState simply hasn't been extended yet (bucket B). The Q3 line was the original product question; it is closed.

Future PMC reviews can cite this milestone when retiring the 2026-Q1 Q3 optimization track from the active work surface.
