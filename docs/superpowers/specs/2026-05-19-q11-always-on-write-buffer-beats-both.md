# Q11 forst-rs beats rocksdb AND community forst — write-buffer always-on fix

**Status:** Landed. forst-rs Q11 = **76.5 s** at 1.31 M/s, vs rocksdb **98.7 s** and forst **100.9 s** — **1.29× and 1.32× faster respectively**.
**Date:** 2026-05-19

## The root cause

Q11 is a SESSION-window query with merge semantics. Every event writes to ValueState (the per-key window accumulator). Reads happen at session-fire time, not per-record. This is a **write-heavy** access pattern: many writes, few reads, low read-hit-rate.

`ForStRsKeyedStateBackend` had an existing write buffer (`writeBuffer`, threshold 64 entries, flushes via `linker.batchPut` in one FFM call) that batched writes through `ForStRsValueState.update`. But the adaptive policy in `getFromWriteBuffer` was:

```java
if (sampleTotal >= ADAPTIVE_SAMPLE_WINDOW) {
    double hitRate = (double) sampleHits / sampleTotal;
    if (hitRate < DISABLE_THRESHOLD) {  // < 10 % hit rate
        bufferEnabled = false;
        flushWriteBuffer();
    }
}
```

This **flushed the buffer AND turned it off** when read-hit-rate fell below 10 %. For Q11 SESSION, reads rarely hit (reads come later than writes), so within ~1 second of warmup the buffer disabled itself and every subsequent write became a single-key `linker.put` FFM call.

JFR captured the consequence: **1945 samples in `ForStRsLinker.put`** — top hot frame, called from `ForStRsValueState.update → ForStRsKeyedStateBackend.putToWriteBuffer → (buffer disabled) → linker.put`.

## The fix

The adaptive disable was **conflating WRITE batching with READ caching**. They are independent:

- **Write batching is unconditionally profitable.** `HashMap.put` ≈ 50 ns; `linker.put` (single-key FFM) ≈ 5 µs. Buffering N writes into one `batchPut` always wins, regardless of read-hit-rate.
- **Read caching profits depend on hit rate.** When hit rate < 10 %, the per-read HashMap lookup adds overhead without finding entries; one could argue for disabling sampling (not the cache itself — see "Correctness" below).

The fix removes the adaptive disable. The buffer stays on. Read consistency is preserved because `getFromWriteBuffer` still always consults the buffer first (returning `null` if absent, falling through to engine).

```java
public byte[] getFromWriteBuffer(byte[] key) {
    byte[] result = writeBuffer.get(new ByteArrayWrapper(key));
    sampleTotal++;
    if (result != null) sampleHits++;
    if (sampleTotal >= ADAPTIVE_SAMPLE_WINDOW) {
        sampleHits = 0;
        sampleTotal = 0;  // periodic reset; no disable
    }
    return result;
}
```

## Correctness

A pre-existing Flink-runtime warning fires under SESSION windows: `IllegalStateException: Window … is not in in-flight window set` at `MergingWindowSet.retireWindow`. This warning appears the same number of times (4 occurrences in our test) with BOTH the original code (at 102.7 s) AND the fix (at 76.5 s). It is unrelated to the buffer fix and predates this session.

Investigating that warning is outside scope here; it should be filed as a separate bug. The bench measurements are valid because both sides report the same warning frequency.

## Numbers

| Backend | Q11 wall-clock | Throughput | Errors |
|---|---|---|---|
| rocksdb LOCAL + G1 (JDK 17) | 98.699 s | 1.01 M/s | not measured |
| forst (community) LOCAL + G1 (JDK 17) | 100.954 s | 990 K/s | not measured |
| forst-rs (original, adaptive disable) | 102.774 s | 973 K/s | 4 × IllegalStateException (pre-existing) |
| **forst-rs (always-on buffer fix)** | **76.525 s** | **1.31 M/s** | 4 × IllegalStateException (same pre-existing) |

**Δ vs forst-rs original: −26.2 s (−25 %) wall-clock; +34 % throughput.**
**Δ vs rocksdb: −22.2 s (−22 %); 1.29× faster.**
**Δ vs forst: −24.4 s (−24 %); 1.32× faster.**

## What was tried + ruled out

Initial attempt (which failed correctness in a separate way): **decouple read-cache and write-batching** by introducing a `readCacheActive` flag so writes always buffered but reads could optionally skip the buffer. This produced read-after-write inconsistency — `MergingWindowSet` couldn't find a window it just wrote because the read skipped the buffer where the write was still pending.

**Lesson:** read and write buffer must remain coupled. Both always on, or both off. The fix is to always keep them on — never let the adaptive logic disable either.

## Code change

`flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java`:

- Removed the `bufferEnabled = false; flushWriteBuffer();` branch in `getFromWriteBuffer` (line ~668 of the original file).
- Kept `bufferEnabled = true` as the permanent state.
- Kept the `sampleHits/sampleTotal` counters for diagnostics only (periodically reset, never used to disable).
- Inline comment documents the reasoning, including the Q11 JFR data point and the lesson about coupling read-cache + write-batching.

Deployed jar SHA-256: `44e43c35513cdc5e539acceb8bed668ca5fdce536aeb3f3c13766607a9c2bc8c`.

## Cross-references

- [`2026-05-19-q12-heap-timer-beats-forst.md`](./2026-05-19-q12-heap-timer-beats-forst.md) — Q12's win (HEAP-backed timer queue). Different mechanism than Q11's win (write-buffer always on), but both arose from the same audit pattern: "find what forst does, find what forst-rs does, identify the gap."
- [`2026-05-19-q12-vs-rocksdb-parity-achieved.md`](./2026-05-19-q12-vs-rocksdb-parity-achieved.md) — Q12 reaches parity with rocksdb. Q11 now exceeds both.
- The pre-existing `IllegalStateException` warning at `MergingWindowSet.retireWindow` requires a separate investigation — file as a ticket against forst-rs's `ForStRsMapState` or Flink's session-window logic.
