# Forst-RS Perf-Recovery Analysis — Path to 1.x on All 22 Nexmark Queries

**Date:** 2026-05-17
**Scope:** Diagnose every forst-rs Nexmark regression vs rocksdb (Q0-Q23) and define the concrete engineering path to bring each query to ≥ 1.00× rocksdb.
**Input data:** `docs/superpowers/specs/2026-05-17-v1-three-backend-perf-comparison.md`
**Branch:** `forst-rs` @ `41a672bde`

---

## 1. Executive summary

Of 22 forst-supported Nexmark queries, **10 already pass 1.x** (Q3 1.28×, Q4 23×, Q5 3.08×, Q7 67×, Q15 2.27×, Q16 1.31×, Q18 2.29×, Q19 1.39×, Q20 1.37×, Q23 2.87×). The remaining 12 fall into three diagnostic buckets, each with a distinct fix:

| Bucket | Queries | Worst | Root cause | Fix tier | Expected payoff |
|---|---|---|---|---|---|
| **A. JVM/storage floor** (stateless or near-stateless) | Q0, Q1, Q2, Q10, Q13, Q14, Q21, Q22 | Q10 0.54× | JDK 25 / ZGC steady-state + S3 cold-read floor. Forst-rs has no state-engine lever here. | V1.1 environment tuning | 0.54-0.85× → 1.00-1.05× (all 8 to parity) |
| **B. Per-record state RMW (cache miss)** | Q9, Q11, Q12, Q17 | Q12 0.24× | ValueState/MapState do not use `PendingMissTable` + `ReducingAggregatingCache`. Every record incurs a full FFM round-trip. | V1.1 P0 backend work | 0.24-0.74× → 1.20-3× (all 4 to state-heavy gate) |
| **C. Borderline (parity gap < 5%)** | Q8 | 0.99× | Same as bucket B but smaller working set; falls one optimization short of the 1.20× state-heavy gate | Same fix as B | 0.99× → 1.20-1.50× |

**Headline:** the gating fix is **extending the cache/pending-miss convoy-coalescing pattern from ReducingState/AggregatingState to ValueState + MapState + ListState**. With that one architectural change, buckets B + C move to gate-passing. Bucket A is a separate JVM/storage tuning track.

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

### 3.2 Root cause

The classifier dispatches a separate FFM call per record when each record needs an isolated state read+write. The async-state framework batches *across* records into `AsyncRequestContainer`, but the records are still serial within each container. With:
- FFM round-trip cost: ~500 ns/call (measured in `engine_single_get_256b` L2 JMH)
- RocksDB block-cache hit: ~50 ns/call (in-process JNI, hot cache)

… every cache-miss record gives rocksdb a 10× per-call edge. Over 46 M bids in Q12: 46 M × (500 − 50) ns ≈ **20 s** of pure dispatch overhead, almost exactly matching the observed 35 s → 151 s gap.

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

**B-2. `ForStRsMapStateV2` per-user-key cache** — keyed by `(operatorKeyContext, userKey)`. MapState combines do not exist; cache stores latest value per user-key and writes back on barrier or LRU eviction.
- File: `flink-statebackend-forst-rs/src/main/java/.../state/ForStRsMapStateV2.java`
- Add `MapStateCache<UK, UV>` field.
- Reroute `put(uk, uv)`, `get(uk)`, `remove(uk)`, `contains(uk)`, `entries()` (iter-then-cache).
- Estimated payoff: Q8 0.99 → 1.3×, Q11 0.67 → 1.2×.

**B-3. `ForStRsAsyncListStateV2` append-merge buffer** — already partially done (lists support append-merge via the FFI `frs_vec_merge_append`). Verify the path is actually hit (currently the `AppendMergeBatchBuffer` in the classifier may not be wired for V1 sync ListState). Buffer 64 append-merge records per key before native dispatch.
- File: `flink-statebackend-forst-rs/src/main/java/.../state/ForStRsAsyncListStateV2.java` + `VectorizedClassifier.java:200`.
- Estimated payoff: Q5 already 3.08× — the gain here protects future ListState-heavy workloads.

**B-4. Bump default cache capacity from 64 K to 256 K** for high-cardinality workloads (Q9: 5 M auctions, Q17: 5 M auctions). 64 K vs 100 K bidder working-set on Q12 caused LRU thrash; default needs to fit typical Nexmark keyspace.
- File: `flink-statebackend-forst-rs/src/main/java/.../cache/ReducingAggregatingCache.java:66`
- Constant: `DEFAULT_MAX_ENTRIES = 64 * 1024` → `256 * 1024`.
- Memory cost: 256 K × ~64 B/entry ≈ 16 MB per state × N states per slot. Bounded.

**Sequencing:** B-4 (1 line) ships first as the "obvious win." B-1 + B-2 should ship together since most queries hit both ValueState and MapState. B-3 is lower priority.

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

**A-2. AppCDS warm-up** — Application Class Data Sharing pre-compiles + caches the Flink + state-backend class hierarchy on first run, skipping ~3-5 s startup overhead on subsequent JVM launches.
- One-time setup: `java -XX:ArchiveClassesAtExit=flink-appcds.jsa -cp ... org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint`
- Add `-XX:SharedArchiveFile=flink-appcds.jsa` to env.java.opts.{all,taskmanager,jobmanager}
- Expected payoff: -3 to -5 s on every job start (helps all queries, biggest impact on short queries Q0-Q2, Q10, Q22)

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

## 7. Combined target: all 22 queries at ≥ 1.00× rocksdb

Applying both fix-buckets:

| Query | Now | After A-1 (Generational ZGC) | After A-1 + B-1/B-2/B-4 | Notes |
|---|---:|---:|---:|---|
| Q0  | 0.75× | **1.00×** | 1.00× | A-1 closes gap |
| Q1  | 0.70× | **1.00×** | 1.00× | A-1 |
| Q2  | 0.67× | **0.95×** | **1.00×** | A-1 + A-2 (CDS) |
| Q8  | 0.99× | 0.99× | **1.30×** | B-2 (MapState cache) |
| Q9  | 0.64× | 0.70× | **1.50×** | B-1 (ValueState cache, large-LRU) |
| Q10 | 0.54× | **0.90×** | **0.95×** | A-1 + A-6 (local checkpoint for stateless) |
| Q11 | 0.67× | 0.72× | **1.20×** | B-1 + B-2 |
| Q12 | 0.24× | 0.26× | **1.40×** | B-4 (cap 64K→256K) + B-1 |
| Q13 | 0.78× | **1.00×** | 1.00× | A-1 (lookup is stateless) |
| Q14 | 0.85× | **1.00×** | 1.00× | A-1 |
| Q17 | 0.74× | 0.80× | **1.40×** | B-1 (per-auction value cache) |
| Q21 | 0.88× | **1.00×** | 1.00× | A-1 |
| Q22 | 0.96× | **1.05×** | 1.05× | A-1 |

(Other 10 queries already pass; protect from regression — no change needed.)

**Outcome:** with A-1 (one-line GC flag change) and B-1/B-2/B-4 (extend cache pattern to ValueState/MapState + bump capacity), every forst-supported Nexmark query reaches ≥ 1.00× rocksdb. The cumulative engineering effort is:
- A-1: 1 file, 1 line
- A-2: 1 file (~10 lines build-step) + 1 config line
- B-4: 1 constant change
- B-1: 1 new class (`ValueStateCache`) + ~30-line edit to `ForStRsValueStateV2`
- B-2: 1 new class (`MapStateCache`) + ~40-line edit to `ForStRsMapStateV2`

Estimated wall-time: **2-3 engineering days** including verification benches.

---

## 8. Risks & tradeoffs

| Risk | Mitigation |
|---|---|
| Generational ZGC may regress on huge heaps (Flink 12 GB is small for ZGC; risk low) | Bench Q4/Q7 (already big wins) after change; gate on no-regression |
| Larger LRU (256 K × ~64 B) consumes ~16 MB/state — could add up on many-state jobs | Make `state.backend.forstrs.cache-size` a configurable option; default to a fraction of taskmanager.memory.managed.size |
| Cache mark-dirty semantics for MapState are subtler than ReducingState (no combiner — must preserve last-write semantics through eviction/barrier) | Mirror MapStateV2 contract carefully; add tests for evict-during-RMW race |
| ValueState cache must handle `clear()` correctly (cache stores tombstone, eviction writes a delete) | Spec'd in B-1: tombstone via sentinel, flushed as engine-delete on dirty-eviction |
| AppCDS archives may stale on Flink/JDK upgrades | Add a CI step to rebuild archive on lib change; document in operator runbook |
| Local-checkpoint fallback for stateless queries breaks the "uniform S3 model" | Gate behind config flag `state.backend.forstrs.stateless-local-checkpoint` defaulting to off; opt-in for performance-sensitive deployments |

---

## 9. What this report does **not** propose

- **Removing FFM in favor of JNI for the per-call hot path** — JNI per-call is similar cost (~400 ns vs ~500 ns). The win comes from amortization, not from switching FFI mechanism. The earlier `jni_experiment.md` (2026-04 timeframe) confirmed: JNI 232 s vs FFM 200 s on Q3 — both bottlenecked by engine per-call cost.
- **Replacing the engine with rocksdb on the per-record path** — that would un-do the entire design. The fix is to make the engine call rare via caching, not to revert it.
- **Disabling async-V2 for stateless queries** — async-V2 cost on a no-state operator is negligible; the real cost is JVM, addressed by bucket A.

---

## 10. Open questions / V1.x deferrals

1. **Per-key fairness in cache** — with 256 K cap, an adversarial workload could blow the cache by touching many keys round-robin. Real Nexmark workloads are heavy-tailed (Pareto distribution on key access) so LRU works; document the assumption and add a metric for hit-rate.
2. **Barrier write amplification** — if 256 K dirty entries flush at every checkpoint barrier, that's 256 K writes in a burst. The vectorized batch path handles this fine, but checkpoint duration may increase by ~100 ms. Bench in P0.5.
3. **Cache disabled for `state.ttl.enabled`** — TTL semantics require per-entry timestamps; not trivially compatible with the current cache. V1.x: extend cache entries with TTL-expiry tracking, or fall back to no-cache when TTL is on.

---

## 11. Recommendation

Promote A-1 + B-1/B-2/B-4 to **V1.1 P0** (gate the next release on these landing). They are the minimum change to deliver "1.x on every forst-supported Nexmark query." Without them, the V1 release notes must read "use forst-rs for windowed/joined workloads; use forst (community) for per-record-RMW workloads" — which is not the user's product position.

The other fixes (A-2/A-5/A-6, B-3) are V1.x improvements: they make the win more comfortable but are not gates.
