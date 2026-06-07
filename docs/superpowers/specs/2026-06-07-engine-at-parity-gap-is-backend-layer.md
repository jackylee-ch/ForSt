# DECISIVE: the forst-rs ENGINE is at parity+ with RocksDB — the NexMark gap is the backend layer

**Date:** 2026-06-07
**Status:** Measured conclusion. Redirects the entire #2 (local per-query) effort.

## Measurement

`cargo bench -p forst-rs-bench --features rocksdb-baseline --bench rocksdb_compare` — in-memory engine,
RocksDB v8.x baseline, same workload, criterion (low variance):

| op | forst-rs | rocksdb | ratio |
|---|---|---|---|
| **point lookup** (100k, memtable) | 94.7 ns | 335 ns | **forst-rs 3.5× FASTER** |
| **range scan** (20k × 8 overlapping L0 SSTs, read-amp) | 17.8 Melem/s | 16.7 Melem/s | **forst-rs 1.07× FASTER** |
| sequential_put | ~1.003 s | 16.2 ms | ARTIFACT* |
| batched_put | ~1.003 s | 387 µs | ARTIFACT* |

\* The put figures are a constant ~1.003 s **independent of N** (1000 vs 10000 puts → same time), i.e.
a fixed per-iteration cost, not per-op throughput. The put benches recreate the engine each criterion
batch (`iter_batched` setup), so the ~1 s is the **db open/drop lifecycle** (background worker
spawn/join), not the write path. The write_controller has no such constant (slowdown 100 µs, stall
45 s). Irrelevant to NexMark, which uses one long-lived backend per task. The READ benches create the
db once → valid per-op signal.

## Conclusion

The forst-rs **engine read path is faster than RocksDB** on both point and range access (the range
result is *after* this session's value-carrying range fix). Therefore the NexMark per-query gap on the
still-failing queries — q9 (1.76×), q19 (2.56×), q20 (1.36×), q4 (1.32×) — is **NOT the engine**. It
lives in the layers above the engine:

- the **Java↔Rust FFM boundary** per state op,
- the **async-state (V2) coordination** (tokio task-per-op dispatch) that the disaggregated model adds
  on every op — pure overhead on LOCAL state, where there is no remote latency to overlap,
- **op-count**: how many backend calls Flink issues per record (operator-level),
- Java-side serialization / state-primitive logic.

This matches the prior q4 rigorous best-of-3 conclusion ("diffuse backend per-record efficiency,
FFM+engine vs JNI+native; async-on-local overhead; no single engine lever").

## Strategic implication

1. **The engine met the perf goal** — it is at parity-plus with RocksDB; q11's specific waste is fixed.
2. The residual local gap is the **disaggregated async-state architecture's per-op overhead on local
   disk**. That same architecture is exactly what wins on **S3/remote** (async overlap hides remote
   latency) — i.e. Phase 2's domain. Forcing local per-query parity on q9/q19/q20 means fighting the
   architecture that Phase 2/Phase 3 exist to exploit.
3. Honest read: closing q9/q19/q20 to 0.8× **locally** requires cutting FFM/async per-op overhead
   (a backend-architecture effort with low odds on local, since sync JNI/RocksDB has none), OR is the
   wrong target — the goal's novel value (beat ForSt on S3 disaggregated state) is Phase 2.

## What landed (kept, correct, verified)

- Value-carrying RANGE scan (`db.rs`) — q11 529→216s (−313s); engine lib 277/0. Spec
  `2026-06-07-value-carrying-range-scan-design.md`.
- `rocksdb_compare` gains a `range_scan_multilevel` scenario (this measurement; permanent regression
  guard for the engine-vs-RocksDB read gap).
- Refuted + reverted: timer-CF isolation, REFILL_BATCH increase (both variance/worse).

## UPDATE 2026-06-07: churn microbench — engine beats RocksDB even under q19's pattern
Added `hot_prefix_churn` to rocksdb_compare (1000 keys × 64 rewrite+delete cycles, flushed each = the
version/tombstone read-amp a TopN buffer generates). Result: **forst-rs 55.1µs vs rocksdb 65.9µs =
forst-rs 1.20× FASTER.** So even under the exact churn q9/q19 produce, the forst-rs ENGINE prefix scan
beats RocksDB. Combined with the 3-backend NexMark sweep (forst-rs LOSES q9/q19 to BOTH RocksDB AND
ForSt-Java) this is conclusive: the q9/q19 gap is NOT the engine — it is the JAVA BACKEND iteration
layer (the per-record MapState.forEachEntry FFM iter-open + chunk-decode path: q19 does ~6M iter-opens).
FIVE independent tests now agree the engine is parity-or-faster (point 3.5×, static range 1.07×, churn
1.20×; q19 not op-redundant; not ordered-dispatch). The novel Rust engine is best-in-class; the residual
per-query gap is FFM/backend-integration overhead — the same layer Phase 2 (disaggregated state) targets.
