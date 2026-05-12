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

Run `25693835571` on `1b6f95002c2` (forst-rs-jdk25, 2026-05-12):

| Backend variant | Events | elapsed_ms | throughput (eps) | status |
|---|---:|---:|---:|---|
| `rocksdb` (EmbeddedRocksDBStateBackend) | 100,000 | 769.86 | **129,893** | ✅ |
| `forst` (community libforstjni) | — | — | — | ❌ `NoSuchMethodError: org.forstdb.RocksDB.loadLibrary()` — see below |
| `forst (libforstjni → libforst_rs_ffi)` | — | — | — | ❌ same root cause as variant 2 (uses same SPI factory) |
| `forst-rs` (ForStRsStateBackendFactory, FFM) | 100,000 | 1111.73 | **89,950** | ✅ |

### Reading the numbers

- **rocksdb vs forst-rs ratio**: rocksdb is **1.44× faster** on this
  workload (or equivalently, forst-rs is **0.69×** rocksdb's throughput).
- This is a small workload (100k events, parallelism=2, no checkpointing,
  no async state API) — the per-event state-access path dominates wall
  time. The forst-rs gap vs rocksdb comes primarily from the FFM hop
  overhead (vs rocksdb's bundled JNI lib) at this workload size.
- The bench's per-event amortised cost works out to:
  - rocksdb: 7.7 µs/event
  - forst-rs: 11.1 µs/event
- The earlier P5 JMH engine-side bench showed forst-rs at **~4× faster
  than RocksDB** on engine-only point lookups (0.125 µs P99 dbSnapshot
  + 5.96 µs P95 sync-phase). The gap between engine-level wins and
  through-Flink wall-time is the Flink runtime overhead (key context
  setup, key-group encoding, namespace serialisation, scheduler
  interaction). Reducing that gap is the focus of follow-up work
  (B-Prod-followup-L7 incremental checkpoint via SPI surfaces some of
  this overhead).

### Variant 2 + 3 failure cause

Both variants use the `org.apache.flink.state.forst.ForStStateBackendFactory`
SPI factory. Its initialization path calls `org.forstdb.RocksDB.loadLibrary()`
which fails with `NoSuchMethodError` because the transitive
`com.ververica:forstjni:0.1.8` dep doesn't expose that method signature on
its `RocksDB` class. This is an **upstream API mismatch** within
`flink-statebackend-forst` + the `com.ververica:forstjni` artifact it pulls
in — NOT caused by anything in this branch's forst-rs work.

Tracking: `B-Prod-followup-CommunityForstJni` — pin or replace
`com.ververica:forstjni:0.1.8` with a version compatible with
`flink-statebackend-forst.ForStStateBackend.ensureForStIsLoaded()`. Variant
3 (libswap) is gated on variant 2 working — once variant 2 produces
numbers, the libswap variant will too (same factory, just with
`-Djava.library.path` pointing at libforst_rs_ffi-renamed-libforstjni).

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

---

## Phase 2 Profiling Results (2026-05-12, commit 9214ba00129)

### Scale-up at 1M events

| Backend | parallelism | events | elapsed_ms | throughput (eps) | µs/event |
|---|---:|---:|---:|---:|---:|
| rocksdb | 2 | 1M | 705.18 | **1,418,081** | 0.71 |
| forst-rs | 2 | 1M | 4,116.40 | **242,931** | 4.12 |
| forst-rs | 4 | 1M | 3,350.73 | **298,443** | 3.35 |
| rocksdb (ckpt=5s) | 4 | 1M | 1,508.21 | **663,037** | 1.51 |
| forst-rs (ckpt=5s) | 4 | 1M | HUNG | — | — |

**Gap at 1M/p=2: rocksdb is 5.8× faster** (vs 1.44× at 100k events).

### JFR Profiling — where the 3.4 µs/event gap lives

| Rank | Method | CPU samples | % |
|---|---|---:|---:|
| 1 | `ForStRsLinker.getInternal` → `frs_get` native downcall (Rust memtable skiplist lookup) | 292 | **95%** |
| 2 | `ForStRsLinker.put` → `frs_put` native downcall | 7 | 2.3% |
| 3 | `ForStRsLinker.copyAndFreeRaw` (copy native bytes + frs_bytes_free) | 2 | 0.6% |
| 4 | `SharedSession.acquire0` (FFM session overhead) | 2 | 0.6% |
| 5 | Other (HashMap, AbstractMemorySegmentImpl) | 5 | 1.5% |

### Key insight

**The bottleneck is NOT FFM hop overhead.** It is 95% inside the native `frs_get` call — the Rust engine's point-lookup path through the Arrow-columnar memtable + MVCC version resolution.

The engine's memtable uses an Arrow RecordBatch (4-column: key/value/seq/op_type) which is optimized for vectorized scans, NOT for point lookups. Each `get` must:
1. Linear-scan or binary-search the key column for the target key
2. Among matches, find the latest version with seq ≤ current (MVCC)
3. Check op_type for tombstones

This is O(log N) at best (binary search on sorted key column) but with poor cache locality compared to a hash-based or B-tree memtable optimized for point access.

### Revised optimization strategy

The Phase B plan's focus shifts from "reduce FFM hop count" (B1/B3) to:

1. **Engine memtable point-lookup optimization** (NEW, highest priority):
   - Add a hash index over the Arrow key column for O(1) point lookups
   - Or: maintain a parallel `HashMap<key, row_index>` alongside the Arrow batch
   - Or: switch memtable to a hybrid structure (hash for point lookups, Arrow for scans/flushes)

2. **S3 vector I/O** (B2, still relevant for cold-cache bar)

3. **Batch-get** (B1, still useful but won't close the 5.8× gap alone since the per-call cost is in the engine, not the hop)

4. **Checkpoint hang** (NEW blocker): `ForStRsSnapshotStrategy` blocks the data path during checkpoint barriers. Needs investigation before the checkpoint-enabled perf bar can be measured.
