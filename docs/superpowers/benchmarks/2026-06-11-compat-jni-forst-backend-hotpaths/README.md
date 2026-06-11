# Compat JNI ForStBackend Hotpath Microbench

Date: 2026-06-11

## Scope

This microbench is a local, low-resource guardrail for the ForStBackend
compat-JNI path before running full Nexmark. It exercises the engine-facing C
ABI that the JNI shim resolves to after translating ForStBackend calls:

- WriteBatch-shaped state updates: per-entry `frs_put` vs `frs_batch_put`.
- Join probe multiGet: per-key `frs_get` vs `frs_batch_get`.
- Read-after-flush/compaction join probe: per-key `frs_get` vs `frs_batch_get`
  after `frs_flush` and `frs_compact_cf`.

This does not replace Q0-Q22 Nexmark accuracy/performance validation. It is
intended to catch local regressions and confirm that JNI-side batching has a
positive engine-facing signal before consuming 8C/32G remote test resources.

## Command

```bash
cargo bench -p forst-rs-bench --bench compat_jni_forst_backend_hotpaths -- --noplot
```

Benchmark target:

```bash
cargo bench -p forst-rs-bench --bench compat_jni_forst_backend_hotpaths --no-run
```

Internal compat-JNI release microbench smoke:

```bash
cargo test --release -p forst-rs-ffi --features compat-jni bench_compat -- --ignored --nocapture
```

## Configuration

- Criterion sample size: 10
- Warmup: 100 ms
- Measurement: 300 ms
- Engine: in-memory ForSt-RS DB through `forst-rs-ffi`
- Dataset sizes:
  - Write batch: 512 rows, 96-byte values
  - Join probe: 1,024 probe rows over 8,192 preloaded rows
  - Compacted join probe: 1,024 probe rows over 8,192 preloaded rows after
    flush and compaction

## 2026-06-11 Local Result

Criterion C ABI benchmark:

| Hot path | Baseline | Optimized path | Baseline p50-ish | Optimized p50-ish | Signal |
| --- | --- | --- | ---: | ---: | ---: |
| WriteBatch translation | per-entry put | `frs_batch_put` | 196.27 us / 512 rows | 124.20 us / 512 rows | 1.58x faster |
| Join probe multiGet | per-key get | `frs_batch_get` | 206.47 us / 1024 rows | 191.88 us / 1024 rows | 1.08x faster |
| Compacted join probe | per-key get | `frs_batch_get` | 459.68 us / 1024 rows | 558.48 us / 1024 rows | 0.82x, slower |

Internal compat-JNI release smoke:

| Hot path | Baseline | Optimized path | Baseline | Optimized | Signal |
| --- | --- | --- | ---: | ---: | ---: |
| WriteBatch apply | per-entry dispatch | `batch_write` | 355.917 us | 68.833 us | 5.17x faster |
| Small multiGet guard | direct per-key helper | guarded grouped path | 492.334 us | 388.417 us | 1.27x faster |
| Prefix iterator open | full iterator open+seek | lazy prefix iterator | 778.958 us | 15.541 us | 50.12x faster |
| Single-CF multiGet | legacy group+reorder | single-CF shortcut | 4.141 ms | 3.724 ms | 1.11x faster |

## Interpretation

The local signal supports keeping the compat-JNI batch-write path: batch put is
materially faster than per-entry dispatch on ForStBackend-shaped state updates.

The multiGet signal is positive but modest for a pure in-memory memtable
workload. That supports keeping the existing JNI guard that avoids forcing
small batches through the batch path too early. In the latest local
flush/compaction probe, batch get is slower than per-key get, so this microbench
must not be used to claim a read-path win by itself. Nexmark queries with
SST-backed join/session state still need the fixed-input accuracy run and the
100M performance run before the read-path policy is considered validated.

The small multiGet smoke compares `compat_multi_get_grouped` with the direct
per-key helper because the guarded path materializes Java-facing result buffers.
Comparing it against a hand-written hit-count-only loop would undercount the
baseline materialization cost and make the guard look falsely slow.

The next required gate is the fixed-input 1M Q0-Q22 accuracy run, followed by
the 100M Q0-Q22 8C/32G Docker performance comparison.
