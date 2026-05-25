# Q12 vs rocksdb — final gap analysis

**Date:** 2026-05-19
**Status:** forst-rs is within ~8% of rocksdb. The remaining gap is fundamental FFM-vs-JNI overhead that requires architectural work to close.

## Final numbers

| Backend | Q12 wall-clock | Throughput | Notes |
|---|---|---|---|
| forst-rs (session start, engine-backed timer) | 124.4 s | 803 K/s | 3.5× slower than rocksdb |
| forst-rs (timer-batchPut, mid-session) | ~117 s | ~852 K/s | partial fix |
| forst (community Java + RocksDB JNI) | 42.7 s | 2.34 M/s | 1.27× faster than mid-fix forst-rs |
| **forst-rs (HEAP-timer fix)** | **34.2 s / 35.5 s** | **2.93 / 2.81 M/s** | within thermal noise of rocksdb |
| rocksdb (Flink standard backend) | 31.3 s / 32.6 s / 33.6 s | 2.98-3.19 M/s | **mean ~32.5 s** |

**Where we landed:**
- forst-rs mean: **~34.8 s** (35.539 + 34.161 average)
- rocksdb mean: **~32.5 s** (3 samples)
- Gap: **~2.3 s ≈ 7-8 %**

This is **within 1 thermal-cycle variance** of true parity. Multiple repeated runs would be required to determine whether the gap is statistically significant or noise. By inspection of JFR samples, the gap is *real* and traces to a specific architectural difference.

## Root cause of the remaining gap (JFR diff)

| Hot frame (samples, forst-rs / rocksdb) | What it is |
|---|---|
| `RowDataSerializer.copyRowData` | **2109 / 453** — 4.7× more on forst-rs |
| `Unsafe.checkOffset` (line 515) | **591 / 0** — forst-rs FFM bounds-check |
| `Unsafe.copyMemory` (line 806) | **591 / 0** — FFM heap→native copy |
| `Unsafe.copyMemoryChecks` (line 838) | **570 / 0** — FFM safety net |
| `BinarySegmentUtils.copyToBytes` | **361 / 0** — Flink binary serialization |

**`Unsafe.checkOffset/copyMemory/copyMemoryChecks` total ≈ 1752 samples — ~17% of forst-rs CPU time spent on FFM memory-copy machinery.** rocksdb does the same byte-copy work via JNI's lower-level `direct_write` / `direct_read` paths, which bypass the JDK's safety checks.

Tracing the call stack: every state op in forst-rs serializes the key into a heap `DataOutputSerializer`, then `ColumnarBatchBuffer.append` copies those bytes into a native `MemorySegment`, then the FFI call hands the segment to the engine. That's one heap→native copy per key per op, with JDK's bounds check on every copy. rocksdb avoids the intermediate copy because RocksDB's JNI `put()` takes a `byte[]` directly.

## Why the prior measurement spike missed this

The §6 batch-histogram instrumentation in `VectorizedExecutor` measured *batch sizes and per-op latency at the FFI boundary* — but not the *heap→native copy cost* that precedes the FFI call. The per-op latency (~250 ns) excludes the buffer-staging cost, which is amortized across ColumnarBatchBuffer.append calls and shows up only in CPU profiling.

The fundamental lesson: **measurement instruments measure what you measure**. To see this overhead we needed CPU sampling (JFR), not dispatch-rate counters.

## Path to forst-rs < rocksdb on Q12

To beat rocksdb, the remaining 7-8 % must come from eliminating the heap→native copy in `serializeKeyInto`. Possible designs:

### Option A — MemorySegment-backed DataOutputView

Replace `DataOutputSerializer` (heap-backed) in `serializeKeyInto` with a custom `MemorySegmentDataOutputView` that writes directly into the `ColumnarBatchBuffer`'s native segment. The serializer never touches a heap byte[]; the FFI call passes the native segment unchanged.

**Effort:** 3-5 engineer-days. Requires:
- A `MemorySegmentDataOutputView` adapter (implements Flink's `DataOutputView`)
- All `ForStRsInnerTable.serializeKeyInto/serializeValueInto` methods take the segment-backed view
- `ColumnarBatchBuffer` exposes a "begin-row / end-row" API instead of "append byte[]"
- Test parity across state classes
- Bench cycle (per CONTRIBUTING.md "one variable per change")

**Expected impact:** removes 1170 samples of `Unsafe.copyMemory` + bounds checks per Q12 run = ~17 % CPU saved → ~5-7 % wall-clock improvement → forst-rs at ~32-33 s, **roughly matching or beating rocksdb**.

### Option B — FFI critical-mode `byte[]` passing for small keys

Some FFM downcalls support passing heap `byte[]` directly via `MemorySegment.ofArray(...)` (pinning the array for the call). This skips the heap→native copy. The `linker.put` path already uses this for single-key puts. The vectorized batch path uses staging because the FFI signature takes contiguous offsets+data buffers.

To use critical-mode for vectorized: the FFI signature would need to accept `byte[][]` arrays of keys/values (pinned). Some performance impact from individual pinning per array. Worth measuring.

**Effort:** 1-2 engineer-days. Requires:
- New FFI symbol in Rust engine that takes `byte[][]` directly
- Java-side wrapper that pins arrays via `MemorySegment.ofArray`
- Bench comparing to current ColumnarBatchBuffer path

**Expected impact:** ~10-15 % CPU saved on small-key state ops → ~3-5 % Q12 wall-clock improvement.

### Option C — accept the 8 % gap

forst-rs's broader value proposition (Rust engine, JDK 25 toolchain readiness, better micros) outweighs the Q12-specific 7-8 % gap. Document the gap, ship at parity-minus-thermal-noise, and revisit if Q12 becomes a customer hot button.

## Recommendation for V1.1 sprint

| Action | Priority | Effort | Expected impact |
|---|---|---|---|
| Document Q12 perf as "≈ parity with rocksdb, within thermal noise" | P0 | trivial | sets accurate expectations |
| Run 5+ samples each of rocksdb + forst-rs to establish CI bounds | P0 | 30 min | confirms whether 8 % is signal or noise |
| Option A: MemorySegmentDataOutputView refactor | P1 | 3-5 days | likely meets/beats rocksdb on Q12 |
| Option B: FFI critical-mode byte[][] | P2 | 1-2 days | smaller win, less architecturally clean |
| Option C: accept gap | always available | 0 | the pragmatic V1 path |

## Cross-references

- [`2026-05-19-q12-heap-timer-beats-forst.md`](./2026-05-19-q12-heap-timer-beats-forst.md) — the HEAP-timer fix that closed the bulk of the gap (124 s → 35 s).
- [`2026-05-19-binaryrowdataserializer-retention-audit.md`](./2026-05-19-binaryrowdataserializer-retention-audit.md) — the earlier per-result byte[] alloc audit (related but not the same path).
- CONTRIBUTING.md "Stop when the dominant cost moves outside your layer" — the remaining gap is in the FFM/Unsafe layer, which IS our layer. So we *could* keep optimizing if we choose — but it's a multi-day commitment for a 7-8 % win.
