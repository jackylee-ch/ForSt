# ForSt JNI / ForSt-RS JNI / ForSt-RS FFI Hot-Path Benchmark Design

Date: 2026-06-11

## Goal

Build a small, repeatable benchmark layer that explains whether ForStBackend can
beat community ForSt after replacing the native library with ForSt-RS. The
benchmark must not wait for full Nexmark to discover bridge regressions. It
must isolate the main native-engine and bridge paths before the 1M correctness
run and the 100M 8C32G Nexmark run.

Primary comparison:

- ForSt JNI: community `forstjni` through the existing `org.forstdb` JNI API.
- ForSt-RS JNI: `libforst_rs_ffi` built with `compat-jni`, loaded by the same
  `org.forstdb` Java mirror where the symbols match.

Cost decomposition:

- ForSt-RS FFI: Rust-side C ABI calls into `forst-rs-ffi`, without Java heap and
  JNI costs. This is the ceiling used to decide whether a slow Java result is a
  bridge problem or an engine problem.

## Non-Goals

- Do not replace Nexmark. These benchmarks are early guardrails.
- Do not run large local datasets. Local runs use short warmup/measurement
  windows and small in-memory datasets.
- Do not infer production throughput from one microbenchmark. Use the matrix to
  identify which interface path is blocking the later 100M Nexmark target.
- Do not change production backend behavior only to make a benchmark pass.

## Approaches Considered

### A. One monolithic Java benchmark

Run ForSt JNI, ForSt-RS JNI, and ForSt-RS FFM from one JVM harness.

Pros: one command, directly comparable wall-clock numbers.

Cons: community ForSt JNI, ForSt-RS compat JNI, and ForSt-RS FFM expose
different APIs. A single harness would either use the lowest common denominator
or carry too much variant-specific code.

### B. Existing 3-way JMH-style harness only

Keep using the current `run-jmh-3way.sh` workloads: point lookup, sequential
put, and batched put.

Pros: already exists and is easy to run.

Cons: it misses the interface shapes that show up in Nexmark joins and map
state: multi-get, prefix iteration, delete/tombstone, and flush/compact
read-after-write.

### C. Two-layer benchmark matrix

Use Java/JVM benchmarks for the primary JNI comparison and Rust criterion
benchmarks for the ForSt-RS FFI ceiling.

Pros: preserves apples-to-apples JNI comparisons while still isolating native
FFI overhead. Extends existing harnesses instead of inventing a new benchmark
framework.

Cons: requires reading two result files together.

Decision: use approach C.

## Benchmark Matrix

| Workload | Why it matters | ForSt-RS JNI | ForSt JNI | ForSt-RS FFI |
|---|---|---:|---:|---:|
| Open/default-CF/close | Backend startup and restore setup | yes | yes | yes |
| Point put | ValueState/update path | yes | yes | yes |
| Point get | ValueState/read path | yes | yes | yes |
| Delete | MapState clear/tombstone path | yes | yes | yes |
| WriteBatch/batchPut | Barrier flush and buffered writes | yes | yes | yes |
| batchGet/multiGet | Join probe and async read batches | yes | best effort | yes |
| Prefix iterator | MapState entries and timer scans | yes | best effort | yes |
| Flush + compact + read | Checkpoint/compaction aftermath | yes | best effort | yes |

`best effort` means the benchmark will run only when the community JNI mirror
has a stable local declaration for the symbol. Missing community symbols are
reported as unsupported instead of silently dropping the row.

## Workload Shape

Use deterministic synthetic keys that resemble Flink state layout:

- `value/q4/key-group=<kg>/key=<id>` for put/get/write batches.
- `join/q7/key-group=<kg>/key=<id>` for multi-get probes.
- `map/q11/key-group=<kg>/namespace=<ns>/user=<id>` for prefix scans.

Default local sizes:

- preload rows: 8192
- write batch rows: 512
- multi-get probe rows: 1024
- prefix rows per namespace: 512
- value sizes: 48, 64, and 96 bytes depending on workload
- local benchmark window: 100 ms warmup and 300 ms measurement for Rust;
  1 s warmup and 2 s measurement for JVM smoke runs

These limits keep the local benchmark below the user's resource constraint and
avoid competing with larger ForSt-RS local tests.

## Harness Layout

ForSt repo:

- Extend `crates/forst-rs-bench/benches/compat_jni_forst_backend_hotpaths.rs`.
- Keep it Rust criterion based.
- Measure ForSt-RS FFI paths:
  - `frs_put`, `frs_get`, `frs_delete`
  - `frs_batch_put`, `frs_batch_get`
  - prefix iterator open/next/close
  - flush/compact/read-after-compact

Flink repo:

- Extend the existing `flink-statebackend-forst-rs` benchmark harness.
- Add a JNI interface matrix class for the ForSt-RS compat JNI mirror.
- Add the same public workload shape to the community ForSt JNI source set
  where the community symbols are available.
- Update `run-jmh-3way.sh` with a `hotpaths` mode or a benchmark-class switch
  so smoke runs stay short and explicit.

Result docs:

- Write outputs under
  `docs/superpowers/benchmarks/2026-06-11-forst-jni-forstrs-jni-ffi-hotpaths/`.
- Keep raw stdout logs and a concise Markdown summary table.

## Accuracy Guard

Each workload validates a checksum before entering the timed section:

- put/get validates exact returned value length and seed bytes.
- delete validates the deleted key is absent.
- batchGet/multiGet validates row count and aggregate byte length.
- prefix iterator validates row count and prefix match.
- flush/compact validates all probed keys survive the operation.

If a validation fails, the benchmark must fail before reporting throughput.

## Interpretation Rules

- If ForSt-RS FFI is fast but ForSt-RS JNI is slow, optimize `compat_jni`.
- If both ForSt-RS FFI and ForSt-RS JNI are slow, inspect the native engine
  implementation or workload shape.
- If ForSt-RS JNI beats ForSt JNI on put/get but loses on batch/prefix scans,
  the Nexmark issue is likely a bridge batching or iterator materialization
  problem rather than a whole-engine design failure.
- If ForSt-RS JNI loses every row and ForSt-RS FFI is also weak, do not proceed
  to the 100M Nexmark run expecting a 1.5x win.

## Verification Commands

ForSt-RS FFI smoke:

```bash
cargo fmt --all -- --check
cargo bench -p forst-rs-bench --bench compat_jni_forst_backend_hotpaths --no-run
cargo bench -p forst-rs-bench --bench compat_jni_forst_backend_hotpaths -- --noplot
```

Flink/JNI smoke:

```bash
cd flink-state-backends/flink-statebackend-forst-rs
FORST_RS_LIB=/private/tmp/forst-compat-jni-forst-backend/target/release/libforst_rs_ffi.dylib \
  BENCH_WARMUP_S=1 BENCH_MEASURE_S=2 ./run-jmh-3way.sh forst-rs hotpaths
BENCH_WARMUP_S=1 BENCH_MEASURE_S=2 ./run-jmh-3way.sh forst hotpaths
```

Full Nexmark is gated behind:

- ForSt and Flink GHA green or failures proven unrelated and rerun.
- 1M CSV fixed-input Q0-Q22 correctness pass.
- Then 100M Q0-Q22 performance run using 8C32G TM resources.

## Self-Review

- No placeholder requirements.
- Scope is limited to benchmark design and small local validation.
- JNI and FFI numbers are intentionally separated to avoid false comparisons.
- Missing community JNI symbols are surfaced as unsupported rows, not ignored.
