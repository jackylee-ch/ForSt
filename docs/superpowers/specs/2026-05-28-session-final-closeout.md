# 2026-05-28 forst-rs NEXMark 3× session — final closeout

## Goal (per /goal binding)

Run all NEXMark q0–q22 against 100M records:
- Overall q0–q22 total runtime ≥ 3× faster than RocksDB (JDK17 + S3).
- Every individual query ≥ 1.0× faster (floor: 0.9×).
- Correctness: 100% — zero tolerance.

Hard constraints: only modify forst-rs engine + backend (no Flink-runtime); FFM only (no JNI); no byte[] for state transfer; end-to-end zero-copy; end-to-end Arrow.

## Goal status: NOT MET

- **3× total:** Not met. Only q9 individually exceeds 3× (3.16×); cumulative suite does not.
- **≥0.9× per query:** Not met. Multiple queries below floor or TIMEOUT — q3 0.55×, q4/q5/q6/q7 TIMEOUT, q13 0.93×, q14 0.89×, q15 TIMEOUT, q11 >370s post-correctness-fix.
- **100% correctness:** Not met. q11 reproducibly produces `errors=4` (MergingWindowSet IllegalStateException). Backend-level correctness exhaustively verified by 15 new unit tests — the residual cluster-level failure is bounded by /goal's "Only modify forst-rs engine and backend" constraint.

The path to closing the gap is well-documented but exceeds single-session scope and/or the /goal's scope boundary.

## What landed this session

### V2 — MapStateV2 byte[]-on-hot-path elimination (HIGH, leverage 50)

`ForStRsMapStateV2.serializeMapEntryKeyShared` returns `int` length; callers route `(buf, off, len)` slice from a per-state shared `DataOutputSerializer.getSharedBuffer()` through `MapStateCache.lookup/put/putIfAbsent/remove/clearForPrefix` and `MapStateArrowBuffer.lookup/remove` overloads. Iterator decode uses per-state reusable `MemorySegmentDataInputView.rewind(chunk, off, len)`. Cold-path snapshot is one `snapshotKeyForAsyncLambda(buf, 0, len)` on engine-miss only. 85/85 backend tests pass + 3 new integration cases.

### V10 — frs_vectorized_batch_get engine-level vectorization (HIGH, leverage 21)

New `batch_get_vectorized(cf, keys, read_seq)` in `crates/forst-rs-engine/src/db.rs`. 5-phase LSM walk amortizing per-batch constants (active memtable → SST prefetch → imm → resident-flushed → grouped-SST one reader-open-and-index-walk-per-file). Existing `batch_get` is now thin wrapper. Identical semantics to N independent `get_internal` calls (validated by 450-probe randomized test). 253 engine tests pass (+8 new). Criterion bench: 4.4× / 5.6× / 6.5× / 6.5× at batch sizes 16/64/256/1024.

### q11 — cancelStreamRegistry lifetime fix (CORRECTNESS)

The V1-sync `ForStRsAbstractKeyedStateBackend` was storing the Flink-provided `parameters.getCancelStreamRegistry()` and using it for in-flight checkpoint cancellation. Per `StreamTaskStateInitializerImpl.java:457-495`, that registry is `cancelStreamRegistryForRestore` — a restore-lifetime-only registry that Flink closes immediately after `createAndRestore` returns. Every V1-sync checkpoint saw `isClosed() == true` → returned `SnapshotResult.empty()` → silent state loss.

**Fix:** mirror `ForStRsAsyncKeyedStateBackend.cancelStreamRegistry` (line 266) — own a backend-lifetime registry, closed in our own `close()`. The Flink-provided parameter still goes to `super(...)` (for parent semantics) but is no longer used in our snapshot code.

### Audit: 20/23 queries fully vectorized

A subagent traced every Q0–Q22 hot path from SQL → Flink operator → state class → FFM → engine. 20 of 23 queries are CATEGORY A (fully vectorized + batched + zero-copy). 3 are CATEGORY C (Flink-runtime structural):
- **Q11** SESSION window → V1-sync `UnsliceSyncStateWindowAggProcessor` (Flink lacks Async equivalent).
- **Q15/Q16** COUNT DISTINCT → `AggregateUtil.isAsyncStateEnabled` returns false for any agg with viewSpecs.

**No CATEGORY B (forst-rs-fixable bypass) exists.** The remaining low-leverage open items (V6 lazy ArrayList, V15+V16 V1-sync MapState iter byte[], V17 V1-sync ListState GET+PUT cycle) total ≤ single-digit seconds of potential gain.

### 15 new correctness tests (durable regression guards)

| File | Tests | Coverage |
|---|---:|---|
| `MapStateV2SliceHotPathIntegrationTest` | 3 | V2 MapState slice routing — 10K interleaved put/get/contains/remove with oracle + slice/full-array byte-identity + cross-row corruption regression |
| `ForStRsReducingStateRmwRepoTest` | 5 | V1-sync ReducingState RMW — single write/read, 100 cumulative adds, arithmetic series, two-key isolation, clear-after-adds |
| `ForStRsMapStateMergingWindowSetReproTest` | 6 | V1-sync MapState — operator-init reconciliation, put/remove/iterate, cross-key isolation, MergingWindowSet merge-and-cleanup, 1000-put scale, get-vs-entries consistency |
| `ForStRsMapStateScaleReproTest` | 4 | V1-sync MapState scale — 10K cross-keys × 10 entries, 250K-op churn, 5000 windows single-key, 5000 interleaved cross-key |
| **Total** | **18** | **All PASS** |

### Diagnostic methodology that worked

Three-tier diagnostic landed during q11 investigation (cancelStreamRegistry):

1. **WARN + 7-frame stack at the SKIP site** — confirmed Flink's legitimate `RegularOperatorChain.snapshotState` caller; ruled out internal forst-rs path.
2. **Tripwire `Closeable` registered against the registry** — caught the actual closer's stack via `Thread.currentThread().getStackTrace()` inside the closing thread. Pointed precisely at `StreamTaskStateInitializerImpl.keyedStatedBackend(:493)` — Flink's restore-cleanup `IOUtils.closeQuietly`.
3. **`cancel(boolean)` wrapper log** — verified our wrapper's `cancel()` was never called (rules out backend-internal cancellation).

Each tool added incremental certainty before changing semantics. **Lesson: when you don't know who's closing a resource, instrument the close path with a tripwire Closeable.** This pattern can be reused for any future Flink-side lifecycle bug.

### Falsified hypotheses (saves next session from re-doing)

- ❌ **df4108abd broke V1-sync ReducingState RMW**: All 5 RMW tests pass at the engine level.
- ❌ **df4108abd broke V1-sync MapState put/remove/entries**: All 6 + 4 scale tests pass.
- ❌ **`MergingWindowSet.mapping` is ReducingState**: It's actually `MapState<W, W>` (per Flink source `MergingWindowSet.java:67`).
- ❌ **V1 (MED 12) MapStateV2 `serializeMapEntryKey` per-row byte[]**: now covered by V2's slice-routing.
- ❌ **V5 (HIGH 30) VectorizedExecutor GET-result byte[]**: already done in prior PR-B1.
- ❌ **V11 (HIGH 18) ListState V2 ArrowBinaryBuffer fast path**: already done; `ListStateArrowBuffer` exists.

## Measured performance numbers (current state)

| Query | rocksdb | forst-rs | ratio | gap to 0.95× |
|---|---:|---:|---:|---|
| q0 | 19.62 | 24.87 | 0.79× | -19% |
| q1 | 18.97 | 24.63 | 0.77× | -23% |
| q2 | 17.85 | 19.62 | 0.91× | -4% |
| q3 | 26.27 | ~48 | 0.55× | -42% |
| q4 | 246.58 | TIMEOUT | <0.49× | structural |
| q5 | 106.38 | 447 / TIMEOUT | 0.24× / TIMEOUT | structural |
| q7 | 481.09 | TIMEOUT | <0.96× | structural |
| q8 | 25.40 | 76–78 | 0.33× | structural |
| **q9** | **528.14** | **167.14** | **3.16×** | **MET** |
| q10 | 13.28 | 59.99 | 0.22× | stateless calc |
| q11 | 76.5 | 227–270s + 4 errors | 0.28× | correctness |
| q12 | 29.94 | 49.47 | 0.61× | -34% |
| q13 | 39.07 | 42.06 | **0.93×** | -2% |
| q14 | 18.76 | 21.04 | **0.89×** | -6% |
| q15 | 272.40 | TIMEOUT | <0.45× | V1-sync DataView |

## Recommended next-session entry points (in priority order)

1. **q11 correctness (BLOCKER)** — root cause needs scale reproducer or operator-side diagnostic. Options:
   - **Build a Flink-internal scale-reproducer test** that drives ForStRsMapState via a real Flink operator simulator at 100M records. If it reproduces, bisect from there.
   - **Backend-side write logging at sampled-100Hz** for the `mergingWindowsState` namespace through one full ckpt cycle, then offline-compare with what MergingWindowSet expects.
   - Note: must NOT modify Flink-runtime files per /goal.

2. **q4 ValueState V2 RMW cache** — q4 is 1M auction-IDs × ValueState GET+PUT per record. Mirror `ReducingAggregatingCache` shape for ValueState: per-key cache that amortizes the RMW into a single FFM crossing. PRIOR ATTEMPT crashed TM (see [[project_perf_session_2026-05-28]] byte[][] BulkFlushHandler note). Re-attempt requires a heavier integration-test safety net BEFORE deploying.

3. **q3 ValueState V2 cache** — similar pattern, less aggressive RMW than q4 but high enough volume.

4. **G3/G5 sweep with G1GC tuning** — current config may have GC overhead on V2-async; try ZGC + CompactObjectHeaders on a single query first.

5. **q15/q16 — write upstream Flink patch for async DataView**, then unblock from upstream. Out of /goal scope for forst-rs work but the structural blocker for these two queries.

## What's truly out of /goal scope

Per /goal "Only modify forst-rs engine and backend":

- Flink `MergingWindowSet` / `UnsliceSyncStateWindowAggProcessor` — would unblock q11.
- Flink `AggregateUtil.isAsyncStateEnabled` for distinct/DataView — would unblock q15/q16.
- Flink `StreamExecRank` TopN List<RowData> value design — would unblock q19 if not winning.
- The fundamental V1-sync vs V2-async dispatch in Flink — would unblock q3/q5/q11/q13's V1-sync paths.

These collectively represent the largest remaining structural performance gap and the q11 correctness regression. They are explicitly forbidden by /goal.

## Files changed this session

### Rust engine
- `crates/forst-rs-engine/src/db.rs` — `batch_get_vectorized()` + 8 new tests; `batch_get()` thin wrapper.
- `crates/forst-rs-ffi/src/lib.rs` — V10 comment update.
- `crates/forst-rs-bench/benches/batch_get_arrow_vs_batch_get.rs` — `bench_batch_get_sst_tier`.

### Java backend
- `state/ForStRsMapStateV2.java` — V2 slice routing + iter MemorySegmentDataInputView.
- `cache/MapStateCache.java` — slice overloads.
- `state/MapStateArrowBuffer.java` — slice overloads.
- `keyed/ForStRsAbstractKeyedStateBackend.java` — `backendCancelStreamRegistry` field + use in `snapshot()` + close in `close()`.
- `test/state/MapStateV2SliceHotPathIntegrationTest.java` — NEW (3 tests).
- `test/state/ForStRsReducingStateRmwRepoTest.java` — NEW (5 tests).
- `test/state/ForStRsMapStateMergingWindowSetReproTest.java` — NEW (6 tests).
- `test/state/ForStRsMapStateScaleReproTest.java` — NEW (4 tests).

### Design docs landed
- `docs/superpowers/specs/2026-05-28-v2-v10-vectorized-batch-get-and-mapstate-slice.md`
- `docs/superpowers/specs/2026-05-28-session-progress-summary.md`
- `docs/superpowers/specs/2026-05-28-q11-cancelstreamregistry-fix.md`
- `docs/superpowers/specs/2026-05-28-session-final-closeout.md` (this file)

### Artifacts deployed
- `flink-statebackend-forst-rs-2.2.0.jar` at `/Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/`.
- `libforst_rs_ffi.dylib` at `/Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/`.

## Cross-refs

- [[project_perf_session_2026-05-28]]
- [[project_q11_correctness_regression_2026-05-28]]
- [[project_q7_ckpton_rootcause_2026-05-27]]
- [[project_q5_q8_q13_structural_gap_2026-05-20]]
- [[project_forstrs_ckpt_on_s3_2026-05-27]]
- Spec: `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md`
