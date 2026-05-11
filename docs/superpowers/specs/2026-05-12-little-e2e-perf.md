# LittleE2E perf bench — 4 backends through MiniCluster

**Date**: 2026-05-12
**Branch**: `flink:forst-rs-jdk25`, `ForSt:forst-rs`
**Spec ID**: `B-Prod-followup-LittleE2E`
**VP question answered**: Q5 — e2e perf comparison at a lighter scale than Nexmark

## Goal

Produce real Flink-state perf numbers (not engine-only micros) for four
backend variants on the **same** MiniCluster workload, isolated to the
state-access path (no checkpointing). This is the cheap-but-honest companion
to the heavier `B-Prod-followup-Nexmark` matrix:

1. **`rocksdb`** — Flink's bundled `EmbeddedRocksDBStateBackend`.
2. **`forst`** — community Flink `ForStStateBackend` (linking community
   `libforstjni`).
3. **`forst (libforstjni → libforst_rs_ffi)`** — same Flink backend, but the
   JNI lib is swapped at `java.library.path` to our forst-rs cdylib (proves
   the forst-rs lib is a drop-in for `libforstjni` at the JNI surface).
4. **`forst-rs`** — our `ForStRsStateBackendFactory` going through the JDK 25
   FFM bridge (`ForStRsLinker`).

## Workload

```
env.fromSequence(1, N).keyBy(x -> x % 100).flatMap(SumState).discard()
```

- `N` = 100,000 events by default; configurable up to 1M via the workflow
  `events` input (`workflow_dispatch`) or the `EVENTS` env var locally.
- `SumState` is a `RichFlatMapFunction` over a `ValueState<Long>` — one
  `state.value()` + `state.update()` per event.
- 100 distinct keys keyBy'd into 2 parallel slots (1 TM, 2 slots/TM,
  parallelism=2) so each slot owns ~50 keyed entries and the state path is
  actually exercised (not the trivial single-key fast path).
- **Checkpointing disabled** — this bench isolates state-access cost.
  Checkpoint snapshot/restore cost is a separate measurement (heavier
  Nexmark matrix; B-Prod-P5 JMH bench writes already cover the engine-side
  snapshot path).

## Method

- 1 warmup run + 1 measured run per backend (configurable via `WARMUPS=`)
- Single MiniCluster, 1 TM × 2 slots, parallelism = 2
- One process per backend variant (each invocation gets a clean JVM, so
  JIT warmup + class loading is fresh per variant — fair to compare with
  the JMH `--enable-native-access` / library-path overhead profile of the
  forst-rs FFM path)
- The bench emits a single `RESULT` line per run, e.g.
  `RESULT backend=forst-rs events=100000 elapsed_ms=2345.67 throughput_eps=42654`

## Files

| Path | Role |
|------|------|
| `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/perf/LittleE2EPerfBench.java` | Plain `main` driver — one backend per JVM, parametrized via `--backend`, `--events`, `--warmups` |
| `flink-state-backends/flink-statebackend-forst-rs/run-little-e2e-perf.sh` | Builds + runs all 4 variants, stages cdylib for the libforstjni-swap variant |
| `.github/workflows/little-e2e-perf-bench.yml` (Flink) | CI lane: build cdylib (ForSt repo) → run bench → publish `RESULT` rows to the GHA step summary |
| `docs/superpowers/specs/2026-05-12-little-e2e-perf.md` (this file) | Writeup |

## How the 4 variants differ at the JNI/FFM layer

```
┌────────────────────────────────────────────────────────────────────┐
│ Variant 1 — rocksdb                                                │
│   Flink → EmbeddedRocksDBStateBackend → RocksDB-Java JNI →         │
│   librocksdbjni (bundled in rocksdbjni-8.x.x.jar)                  │
├────────────────────────────────────────────────────────────────────┤
│ Variant 2 — forst (community)                                      │
│   Flink → ForStStateBackend → org.forstdb.RocksDB JNI →            │
│   libforstjni (community ForSt build)                              │
├────────────────────────────────────────────────────────────────────┤
│ Variant 3 — forst, libforstjni→libforst_rs_ffi swap                │
│   Flink → ForStStateBackend → org.forstdb.RocksDB JNI →            │
│   libforst_rs_ffi (staged as libforstjni.so via java.library.path) │
│   PROVES: forst-rs cdylib exports JNI symbols compat with forst    │
├────────────────────────────────────────────────────────────────────┤
│ Variant 4 — forst-rs                                               │
│   Flink → ForStRsStateBackend → ForStRsLinker (FFM, no JNI) →      │
│   libforst_rs_ffi via Linker.nativeLinker() + Critical            │
│   PROVES: zero-JNI-overhead path is fastest                        │
└────────────────────────────────────────────────────────────────────┘
```

## Results

[populated post-run from the `little-e2e-results` GHA artifact +
`little-e2e.log` — appended once the workflow has been triggered with a
non-zero events budget]

## Notes

- This is "little" e2e (~100k events default, no checkpointing). Real-scale
  comparisons belong with `B-Prod-followup-Nexmark` (multi-hour matrix that
  exercises checkpoint cost + scale-out rescaling).
- Variant 3 requires the forst-rs cdylib to export the
  `Java_org_forstdb_RocksDB_*` JNI symbols. The current G-A cdylib does (see
  `crates/forst-rs-ffi/src/jni_compat/`); if a future build drops those
  exports, variant 3 will fail at `UnsatisfiedLinkError` and the other three
  still run.
- The bench is a plain `main` class (not `@Test`) so it can be driven from
  CI without going through Surefire — Surefire would invoke the bench on a
  fork-per-test-class basis and the `MiniClusterWithClientResource` lifecycle
  hooks would compete with JUnit's `@BeforeEach`/`@AfterEach`.

## Related work

- `B-Prod-followup-Nexmark` — heavier multi-backend matrix
- `2026-05-10-bprod-bench-results.md` — JMH engine micros (4-way: Rust /
  community / FFM / RocksDB-JNI)
- `2026-05-12-s3-vs-local-bench.md` — disaggregated storage perf
- `2026-05-12-bprod-vp-status-v2.md` — overall VP status
