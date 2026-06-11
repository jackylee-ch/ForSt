# ForSt JNI / ForSt-RS JNI / ForSt-RS FFI Hot-Path Smoke Results

Date: 2026-06-11

## Scope

This is a local, low-resource smoke benchmark for the hot paths that should be
validated before running 1M fixed-input Nexmark correctness and 100M 8C32G
Nexmark performance. It is not a full production benchmark.

## Verification

ForSt-RS FFI:

```text
cargo fmt --all -- --check
cargo bench -p forst-rs-bench --bench compat_jni_forst_backend_hotpaths --no-run
cargo bench -p forst-rs-bench --bench compat_jni_forst_backend_hotpaths -- --noplot
cargo test --release -p forst-rs-ffi --features compat-jni bench_compat -- --ignored --nocapture
```

ForSt-RS JNI:

```text
FORST_RS_LIB=/private/tmp/forst-compat-jni-forst-backend/target/release/libforst_rs_ffi.dylib \
  BENCH_WARMUP_S=1 BENCH_MEASURE_S=2 ./run-jmh-3way.sh forst-rs hotpaths
```

Community ForSt JNI:

```text
jar xf /Users/lijunqing/.m2/repository/com/ververica/forstjni/0.1.8/forstjni-0.1.8.jar \
  libforstjni-osx-arm64.jnilib
BENCH_WARMUP_S=1 BENCH_MEASURE_S=2 \
  FORST_LIB=/tmp/forstjni-0.1.8-extract/libforstjni-osx-arm64.jnilib \
  ./run-jmh-3way.sh forst hotpaths
```

The community Java hot-path class compiles and runs locally with the native
library extracted from the cached `com.ververica:forstjni:0.1.8` jar.

## ForSt-RS JNI Smoke

JDK: Zulu 25.0.3
Window: 1s warmup, 2s measurement
Library: local `libforst_rs_ffi.dylib` rebuilt with `--features compat-jni`

| Workload | Units | Time | Throughput |
|---|---:|---:|---:|
| openClose | 403 ops | 2.085 s | 193 ops/s |
| pointGet | 4,817,382 ops | 2.000 s | 2,408,651 ops/s |
| pointPut | 1,523,420 ops | 2.000 s | 761,710 ops/s |
| deleteThenGet | 1,189,193 ops | 2.000 s | 594,597 ops/s |
| batchPut | 2,624,512 rows | 2.000 s | 1,312,210 rows/s |
| batchGet | 2,091,008 rows | 2.000 s | 1,045,257 rows/s |
| multiGet | 1,931,264 rows | 2.001 s | 965,253 rows/s |
| prefixScan | 1,633,792 rows | 2.000 s | 816,719 rows/s |
| flushCompactRead | 4 ops | 2.051 s | 2 ops/s |

## Community ForSt JNI Smoke

JDK: Zulu 25.0.3
Window: 1s warmup, 2s measurement
Library: `/tmp/forstjni-0.1.8-extract/libforstjni-osx-arm64.jnilib`

| Workload | Units | Time | Throughput |
|---|---:|---:|---:|
| openClose | 41 ops | 2.032 s | 20 ops/s |
| pointGet | 4,933,240 ops | 2.000 s | 2,466,586 ops/s |
| pointPut | 718,534 ops | 2.000 s | 359,265 ops/s |
| deleteThenGet | 433,029 ops | 2.000 s | 216,514 ops/s |
| writeBatch | 2,400,768 rows | 2.001 s | 1,200,030 rows/s |
| multiGet | 1,763,328 rows | 2.001 s | 881,249 rows/s |

## JNI Hot-Path Comparison

| Workload | ForSt JNI | ForSt-RS JNI | ForSt-RS / ForSt |
|---|---:|---:|---:|
| openClose | 20 ops/s | 193 ops/s | 9.65x |
| pointGet | 2,466,586 ops/s | 2,408,651 ops/s | 0.98x |
| pointPut | 359,265 ops/s | 761,710 ops/s | 2.12x |
| deleteThenGet | 216,514 ops/s | 594,597 ops/s | 2.75x |
| batch/writeBatch | 1,200,030 rows/s | 1,312,210 rows/s | 1.09x |
| multiGet | 881,249 rows/s | 965,253 rows/s | 1.10x |

## ForSt-RS FFI Criterion Smoke

Criterion sample size: 10
Warmup: 100 ms
Measurement: 300 ms

| Workload | Variant | Median Time | Throughput / Ratio |
|---|---|---:|---:|
| write_batch_translation | per_entry_put / 512 | 251.82 us | baseline |
| write_batch_translation | ffi_batch_put / 512 | 210.87 us | 1.19x faster |
| join_probe_multiget | per_key_get / 1024 | 147.97 us | baseline |
| join_probe_multiget | ffi_batch_get / 1024 | 144.83 us | 1.02x faster |
| compacted_join_probe | flushed_compacted_per_key_get / 1024 | 440.81 us | baseline |
| compacted_join_probe | flushed_compacted_batch_get / 1024 | 449.27 us | 0.98x of baseline |
| delete_then_get | put_delete_get_miss | 565.77 ns | 1.77 Mops/s |
| prefix_scan | open_drain_close / 512 | 136.56 us | 3.75 Mrows/s |

## Targeted Compat-JNI Test Bench

```text
compat_write_batch benchmark: per_entry=470.041us, batch_write=108.625us, speedup=4.327x
compat_multi_get small-batch guard: per_key=489.625us, guarded=385.625us, ratio=1.270x
compat_rocks_iterator prefix benchmark: full_open_seek=901.958us, lazy_prefix=21.958us, speedup=41.077x
compat_multi_get single-CF shortcut benchmark: legacy_reorder=4.14275ms, single_cf=3.8155ms, speedup=1.086x
```

## Findings

- The new ForSt-RS JNI hot-path smoke runs end to end after fixing compact-JNI
  CF handle resolution for batch/prefix/iterator paths.
- The local community ForSt JNI comparison now runs from the cached
  `forstjni-0.1.8` native library. In this low-resource JVM smoke, ForSt-RS JNI
  is near-parity on point get and faster on put/delete/batch/multi-get bridge
  paths, but this is still a hot-path smoke rather than a Nexmark result.
- ForSt-RS FFI batch put is faster than per-entry put, but only 1.19x in this
  criterion smoke; the separate ignored compat bench still shows a stronger
  4.327x write-batch win.
- ForSt-RS FFI multi-get is currently near parity with per-key get in this
  small in-memory workload. This means Nexmark join gains should not be assumed
  from batching alone; the next optimization target is grouped SST/block-cache
  read locality and Java-side materialization cost.
- Prefix iteration remains the clearest bridge win: lazy prefix iterator is
  41.077x faster than the full-open-seek path in the targeted compat bench, and
  the new FFI prefix scan drains 512 rows at a 136.56 us median.
- Local reruns should respect the 40G memory reservation. During the latest
  local check, `memory_pressure -Q` showed only about 31G free, so the
  ForSt-RS side was not rerun after the community ForSt JNI smoke.

## Next Gate

Run the same hot-path JVM matrix on the remote server where the community ForSt
JNI library is available, then proceed to:

1. 1M fixed-input CSV Nexmark Q0-Q22 correctness.
2. 100M Nexmark Q0-Q22 8C32G comparison for `ForStBackend + local` and
   `ForStBackend + ForSt-RS compat JNI lib + local`.
