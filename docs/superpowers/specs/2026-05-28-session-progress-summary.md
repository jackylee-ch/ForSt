# 2026-05-28 Session Progress Summary

## Goal (binding)

Forst-rs (JDK25 + S3 + G1GC) must outperform RocksDB (JDK17 + S3) on 100M-record Nexmark by:
- Overall q0–q22 total runtime: **3× faster**
- Every individual query: ≥ 1.0× (minimum acceptable 0.9×)
- Correctness: 100% — zero tolerance.

Hard constraints: NO JNI (FFM Panama only), NO byte[]/byte[][] in state transfer (Arrow-shaped MemorySegments end-to-end), NO intermediate heap copy.

## What landed this session

### V2 — MapStateV2 byte[]-on-hot-path elimination (HIGH, leverage 50)
- `serializeMapEntryKeyShared(userKey)` returns int length; callers route `(buf, off, len)` slice from shared `DataOutputSerializer.getSharedBuffer()` end-to-end.
- `MapStateCache` + `MapStateArrowBuffer` got `(buf, off, len)` slice-accepting overloads on lookup/put/putIfAbsent/remove/clearForPrefix.
- Iterator decode path now uses per-state reusable `MemorySegmentDataInputView.rewind(chunk, off, len)` — zero alloc per iter step.
- Async-miss cold path: single `snapshotKeyForAsyncLambda(buf, 0, len)` copy ONCE on engine miss (correct lifetime handoff to the `thenApply` lambda).
- 85 / 85 tests pass including 3 new integration cases: 10k interleaved put/get/contains/remove with oracle; slice/full-array byte-identity; BulkFlushHandler-style cross-row corruption regression.

### V10 — frs_vectorized_batch_get engine-level vectorization (HIGH, leverage 21)
- New `batch_get_vectorized(cf, keys, read_seq)` in `crates/forst-rs-engine/src/db.rs`.
- 5-phase LSM walk amortizing per-batch constants: active memtable → SST prefetch → imm memtables → resident-flushed (Design A) → grouped-SST (one reader open + one index walk per file for all pending keys).
- Existing `batch_get` is now a thin `batch_get_vectorized(..., u64::MAX)` wrapper.
- Correctness identical to N independent `get_internal(k, read_seq)` calls — validated by `test_batch_get_vectorized_correctness_vs_per_key_get_randomized` (450 probes, mixed-tier DB).
- N=1 fast path: bypasses vectorized bookkeeping → no regression.
- 253 / 253 engine tests pass (+8 new), 321 storage tests pass, 96+1+13 FFI tests pass.

#### Criterion bench (SST-tier scenario, 8 L0 SSTs)

| Batch | V10 vectorized | per-key loop | Speedup |
|---:|---:|---:|---:|
| 16   | 2.07 µs |  9.08 µs | **4.4×** |
| 64   | 6.56 µs | 36.6 µs  | **5.6×** |
| 256  | 22.3 µs | 145 µs   | **6.5×** |
| 1024 | 89.2 µs | 578 µs   | **6.5×** |

Beats the spec's projected 4.2× and clears the noise threshold on active-memtable bench (no regression on N=1).

## Measured performance impact

| Query | rocksdb (s) | forst-rs (s) | ratio | meets goal? |
|---|---:|---:|---:|---|
| q3 | 26.27 | 47.57 | 0.55× | no (V2 cost small; structural V1-sync ceiling) |
| q7 | 481.09 | TIMEOUT >500 | <0.96× | no (ckpt-on L0 fanout, structural) |
| q8 | 25.40 | 78.05 | 0.33× | no (structural FFM-vs-local handicap) |
| **q9** | 528.14 | **167.14** | **3.16×** | **YES — FIRST per-query 3× win this session** |
| q4 | 246.58 | TIMEOUT >600 | <0.41× | no (structural ValueState per-op cost) |
| q5 | 106.38 | TIMEOUT >600 | <0.17× | no (V1-sync HOP + structural) |
| q6 | n/a | silent fail | n/a | cluster pollution after consecutive timeouts |

**q9 demonstrates the engine + V2+V10 are CAPABLE of meeting the 3× per-query goal on V2-async workloads.** The path to broader wins is V2-async coverage of more queries (Flink-runtime work) and engine reductions of the structural FFM-per-call ceiling.

## What's already done (saved redundant work)

A full subagent re-audit of the 2026-05-21 vectorization spec confirmed:
- **V5** (VectorizedExecutor GET-result byte[]) — landed PR-B1 prior session; `completeGet` routes engine outData segment to `table.deserializeValue(buf, off, len)` overrides on all 5 V2 state classes.
- **V11** (ListState ArrowBinaryBuffer fast path) — landed; `ListStateArrowBuffer` exists, `recordAppendMergeOffHeap` routes asyncAdd via classifier, `frs_vec_merge_append_batch` single-FFM-crossing batch dispatch.
- **V3, V4, V7, V8, V9, V12, V13** — all landed.

**All HIGH/MED items in V1–V13 are LANDED.** Remaining items (V6 lazy ArrayList = 4.5 leverage; V15+V16 V1-sync MapState iter byte[] = 1.8 each; V17 V1-sync ListState GET+PUT cycle = 2.4) are LOW-leverage; combined potential ≤ single-digit seconds.

## What's NOT achievable in this session

Per accumulated memory notes:
- **V1-sync structural ceiling** ([[project_q5_q8_q13_structural_gap_2026-05-20]]) — q3/q5/q11/q13's HOP/JOIN paths can't reach VectorizedExecutor; Flink lacks `AsyncStateSlicingSharedWindowProcessor`. Not fixable in forst-rs scope.
- **ckpt-ON L0 fanout** ([[project_q7_ckpton_rootcause_2026-05-27]]) — ckpt force-flush moves hot join state to S3 L0 SSTs; each join probe opens O(num_L0) overlapping SSTs via whole-file S3 download. Design A's 4 GiB resident-flushed cap partially mitigates (q7 410 rec/s → 13K rec/s = 32× improvement) but at 100M-scale state outgrows the cap. Architectural redesign (disaggregated state ref, lazy chunk-stream) needed.
- **FFM per-call cost on V1-sync paths** — forst-rs ~6 µs/op vs rocksdb (JNI in-process) ~2.5 µs/op. Closing this gap requires either eliminating the FFM boundary or batching V1-sync calls — Flink-runtime work, not state-backend.

## Files changed (this session, post-handoff)

### Rust
- `crates/forst-rs-engine/src/db.rs` — `batch_get_vectorized()` + 8 new unit tests; `batch_get()` is now a thin wrapper.
- `crates/forst-rs-ffi/src/lib.rs` — V10 comment update (no signature change).
- `crates/forst-rs-bench/benches/batch_get_arrow_vs_batch_get.rs` — added `bench_batch_get_sst_tier`.

### Java
- `state/ForStRsMapStateV2.java` — `serializeMapEntryKeyShared`, slice-route hot path, `MemorySegmentDataInputView` iter decode, cold-path `snapshotKeyForAsyncLambda`.
- `cache/MapStateCache.java` — `(buf, off, len)` slice overloads.
- `state/MapStateArrowBuffer.java` — `(buf, off, len)` slice overloads.
- `test/.../state/MapStateV2SliceHotPathIntegrationTest.java` — NEW, 3 cases.

### Artifacts deployed
- Backend jar: `flink-statebackend-forst-rs-2.2.0.jar` at `/Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/`.
- FFI dylib: `libforst_rs_ffi.dylib` rebuilt 17:55 (V10 path live).

## Next session entry points

1. **q4 specifically** — investigate ValueState V2 RMW-cache extension (mirror `ReducingAggregatingCache` shape). 100M auction updates × FFM cost is the bottleneck; per-key cache would amortize.
2. **q7 ckpt-on architectural fix** — disaggregated state ref OR lazy chunk-stream from S3 SSTs OR per-key key-range-pinning to keep hot keys in resident memtables longer than 4 GiB FIFO.
3. **Cluster lifecycle** — bench orchestrator needs fresh-cluster-per-query strategy after consecutive timeouts. Multiple G2/G3 sweeps showed degradation after 4-5 timeouts.

## Cross-refs
- [[project_perf_session_2026-05-28]]
- [[project_q7_ckpton_rootcause_2026-05-27]]
- [[project_q5_q8_q13_structural_gap_2026-05-20]]
- [[project_forstrs_ckpt_on_s3_2026-05-27]]
- Spec: `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md` (V1-V13 audit-design)
- Detail doc: `docs/superpowers/specs/2026-05-28-v2-v10-vectorized-batch-get-and-mapstate-slice.md` (V2 + V10 implementation)
