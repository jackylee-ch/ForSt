# ForSt ↔ forst-rs Architecture Gap-Map — profiler-driven root-cause design

**Date:** 2026-06-08
**Status:** Design (approved — proceed to plan)
**Frame:** PMC top-down. The forst-rs↔ForSt/RocksDB perf gap is treated as a SYSTEMIC,
multi-dimensional architecture problem, NOT a single hotspot. The profiler's job is to put a
*number* on each architectural dimension so we get a ranked, multi-module upgrade verdict — the
"architecture upgrade across multiple modules simultaneously" the goal mandates.

## Why (measured motivation)
Fair 8c/32g, committed config (noflush=false + true backpressure + 6 GB budget):
- **q17 (merge-agg): forst-rs 817s vs RocksDB 74.5s = 11× (accuracy ✓ 92M==92M)** — worst relative gap.
- **q9 (join): RocksDB also slow (~1100s, 59 K/s); forst-rs ~53M@1300s (6 GB) → ~2-3×** — hard for both.
So the gap is uneven across query shapes → it is the SUM of several architectural differences,
strongest on the merge/agg + flush-throughput path. We must measure each, not guess one.

## Architectural dimensions to quantify (ForSt vs forst-rs)
| # | Dimension | ForSt / RocksDB | forst-rs | measurement |
|---|-----------|-----------------|----------|-------------|
| 1 | Async-state executor / parallelism | coordinator + parallel readThreads pool (`read-io-parallelism=3`) + writeThreads | single VectorizedExecutor per slot | stall/off-CPU wait counters; thread utilization |
| 2 | State-access coalescing | native `multiGetAsList` | `batch_get` | read ns/op, batch sizes |
| 3 | Engine flush throughput | RocksDB C++ (vectorized) | Rust LSM | flush **ms/MB** |
| 4 | Engine compaction throughput | RocksDB C++ | Rust LSM | compaction **ms/MB** |
| 5 | Merge / read-amp handling | RocksDB merge-operator combine | merge-operand chains | merge-operands-walked/read; chain depth |
| 6 | FFM/JNI per-op bridge | in-process JNI | Panama FFM crossings | per-op overhead (perf) |
(Memory model — WBM/cache/allow_stall — is item 0, largely closed this session via backpressure.)

## Method (profile first, layered)

### Phase A — in-engine wall-time attribution (gated `FRS_PROF_DIAG=1`, default OFF)
Add process-global cumulative counters (relaxed atomics: ns + count/bytes), logged periodically to
the diag file alongside FRS_MEM_DIAG. Seams (engine fns confirmed present in db.rs):
- **stall**: `wait_for_wbm_headroom` — ns blocked (dimension 1 backpressure share)
- **flush**: `run_flush` / `flush_cf_data` — ns + bytes → ms/MB (dim 3)
- **compaction**: `run_compaction` / `compact_l0_for_cf` — ns + bytes → ms/MB (dim 4)
- **read**: `get_internal` + batch read path — ns + ops → ns/op, and merge-operands-walked (dim 2,5)
Output a periodic line: `[FRS_PROF] stall_ms=.. flush_ms=../MB compaction_ms=../MB read_ns/op=..
merge_ops/read=.. ...`. Overhead = a few relaxed atomic adds per op; zero when gated off.
**This reveals concentrated vs diffuse** and the per-dimension share of wall-time.

### Phase B — hotspot flamegraph (perf + symbolized .so)
Build with `CARGO_PROFILE_RELEASE_STRIP=none` + `debug=2` (prior-art symbolized build), `perf
record -g` the Java TM PID during q17 + q9, fold native+JIT frames. Run ONLY on the dimension(s)
Phase A flags dominant, so we profile the right code.

### Phase C — architectural comparison + ranked verdict
For each dimension, place forst-rs's measured number beside ForSt/RocksDB's behavior (from source
already read this session: ForStStateExecutor parallel readThreads; ForStGeneralMultiGetOperation
multiGetAsList; RocksDB vectorized compaction/merge-operator). Produce a RANKED gap-map: each
dimension → measured contribution → the upgrade module that closes it → expected combined payoff.

## Targets & baselines
q17 (merge-agg, 11× — primary) + q9 (join, read-amp). Baselines: RocksDB q17 74.5s ✓; RocksDB q9
(~1100s, in progress); ForSt q17/q9 8c/32g (to run). All NexMark 92M/100M, each backend own timer.

## Output
`docs/superpowers/specs/2026-06-08-forst-vs-forstrs-architecture-gap-map-RESULTS.md` — the ranked
gap-map with the attribution table as evidence, naming the dominant module(s) and the coordinated
multi-module upgrade order. This feeds the next spec(s): the actual engine upgrades.

## Verification
- Phase-A category times sum to ≈ wall-time (attribution completeness sanity check).
- Counters default OFF (`FRS_PROF_DIAG` unset) → zero production overhead; an engine UT asserts the
  gate compiles to no-ops when off.
- Accuracy unaffected (counters are observational only; out_rows must stay == RocksDB).

## Non-goals (this spec)
The fixes themselves. This spec only produces the measured, ranked gap-map. Each upgrade module
(e.g., parallel read executor, vectorized compaction/merge, FFM batching) gets its own spec→plan
once the gap-map ranks it — implemented and verified one complete module at a time.
