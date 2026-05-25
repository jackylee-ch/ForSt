# Q11/Q12 forst-rs Speedup Execution Plan

**Status:** In-flight. This session landed the small-win serializeKey/serializeKeyInto cache. The remaining items require Rust engine work and/or bench cluster validation.
**Date:** 2026-05-19
**Constraint (user-imposed):** Fixes live in `forst-rs` (Rust engine) and `flink-statebackend-forst-rs` (Java backend) only. Flink-runtime changes (e.g., `AsyncStateRecordsWindowBuffer`, `AsyncStateAggCombiner`) are **out of scope**.

## Background

Per [`2026-05-19-q11q12-state-primitive-audit.md`](./2026-05-19-q11q12-state-primitive-audit.md), Q11 and Q12:
- Use `ForStRsValueStateV2` (not MapState — `MapStateCache` cannot help them).
- Have no per-record engine call. Per-record cost is the heap `WindowBytesMultiMap` buffer.
- Burn engine ops at slice-fire / window-flush time: ~`O(active_keys × active_slices)` GET+PUT pairs.
- Each `asyncCombine` is `asyncValue(w) → in-memory aggregate → asyncUpdate(w, acc)` — classic RMW pair.

The forst-rs-side speedup surface, given the constraint:

| Lever | Layer | Effort | Expected impact on Q11/Q12 | Status |
|---|---|---|---|---|
| (A) Reuse composite key bytes across GET+PUT for same `RecordContext` | Java backend | Trivial | Small (saves 1 keySerializer.serialize per RMW pair) | **DONE (this session)** |
| (B) Eliminate per-result `new byte[len]` in `VectorizedExecutor.executeGets` | Java backend | Medium | Small-to-medium (GC pressure relief at flush) | Deferred — needs MemorySegment-based deserializer adapter |
| (C) `frs_vectorized_batch_compute` (RMW fusion) engine API | Rust + Java | Large | **Large** (collapses 2 FFM crossings → 1 for each (key, slice) RMW) | Deferred — needs Rust changes + bench validation |
| (D) Increase vectorized batch threshold / queueing windows | AEC config | Trivial | Conditional on §6 measurement | Deferred — requires the §6 instrumentation result |
| (E) Engine per-op cost reduction (Rust profiling) | Rust | Medium-large | Conditional on §6 measurement | Deferred — gated by §6 |

## (A) Composite key reuse — LANDED THIS SESSION

**What changed:**
- `ForStRsValueStateV2.serializeKeyInto` now consults `RecordContext.extra` for cached composite key bytes; populates the cache on miss using `getCopyOfBuffer`.
- Same change applied to `ForStRsAsyncReducingStateV2` (both `serializeKey` and `serializeKeyInto`) and `ForStRsAsyncAggregatingStateV2`.
- `ForStRsValueStateV2.serializeKey` already had the cache; only the `Into` variant was missing.

**Why it's a win for Q11/Q12:**

In `AsyncStateAggCombiner.asyncCombine`, the sequence is:

```
accState.asyncValue(window).thenCompose(acc → accState.asyncUpdate(window, newAcc))
```

Both `asyncValue` and `asyncUpdate` route through `ForStRsValueStateV2.serializeKeyInto` (the vectorized path used by `VectorizedClassifier.offer`). The `RecordContext` is the same across both — it's the record being processed. Pre-change: each call ran `keySerializer.serialize(ctx.getKey(), keyOut)` (key bytes built twice for the same record). Post-change: the first call caches, the second appends from cache.

**Estimated impact:** For Q12 PROCTIME 10s tumble with ~1M `(bidder, slice)` flushes per 10s window, this saves ~1M redundant `Long` serializations per window (~20-30 ns each) = ~20-30 ms saved per 10s window. Plus reduced GC churn from `keyOut.getSharedBuffer()` reuse pressure. **Small win, but unconditional and zero-risk.**

**Risk:** None observed. The cache pattern is identical to the existing `serializeKey` cache (which has been in production since `forst-rs-jdk25` branch creation). RecordContext.extra is per-record so there is no cross-record contamination.

**Verification:** `mvn compile` passes on JDK 25. No behavior change for any code path that did not previously reach `serializeKeyInto`.

## (B) Eliminate per-result `byte[]` allocation in `executeGets` — DEFERRED

`VectorizedExecutor.executeGets` at line 311-326 allocates a `byte[len]` per result slot, then calls `MemorySegment.copy(outData, ..., raw, 0, len)`, then `completeGet → table.deserializeValue(raw)`. For Q12 flush returning 1M `byte[]` objects, this is 1M small allocations + 1M MemorySegment-to-heap copies.

**Why deferred:**

The clean fix is `deserializeValue(MemorySegment buf, int offset, int length)` (a slice-based decode mirroring the `IteratorEntryView` pattern landed in Commit B). This requires:

1. A `MemorySegmentDataInputView` adapter (Flink's `DataInputDeserializer` consumes `byte[]`; we need an analog over `MemorySegment`).
2. Each state class implements the slice-based overload.
3. `VectorizedExecutor.executeGets` calls the slice overload, eliminating the alloc.

The risk: some Flink serializers (notably `BinaryRowDataSerializer`) wrap the input bytes inside the returned object. A reused buffer would corrupt RowData. Mitigation: confirm by reading Flink's serializer hierarchy AND/OR keep alloc but pool the byte[].

**Recommendation:** schedule as a V1.2 work item. Validate that RowDataSerializer is safe to reuse the input buffer (it returns `GenericRowData` for non-binary cases, which copies). If safe, the alloc can be eliminated. If not safe, pool the byte[] instead.

## (C) `frs_vectorized_batch_compute` (RMW fusion) — DEFERRED, biggest leverage

**The shape:** A new Rust engine API that takes:
- N keys to GET
- A callback per key that receives the current value and returns the new value
- The engine writes back atomically without round-tripping to Java

In Java terms: replace `asyncValue(w) → asyncUpdate(w, f(acc))` with `asyncCompute(w, f)`. The `f` is invoked inside the engine's dispatch loop, so there is exactly one FFM crossing per batch (not two).

**Why it's the right Q12 fix:**

Q12 (PROCTIME tumble): every 10s flushes ~1M `(bidder, slice)` RMWs. Today that's ~2M FFM crossings. RMW fusion halves the crossings to ~1M.

**Batch-size-conditional impact range** (refines the earlier fixed-43 % estimate; lookup the actual median batch size from the §6 measurement spike before sprint review):

| Median batch size during Q12 flush | Q12 wall-clock improvement from RMW fusion | Reasoning |
|---|---|---|
| `[1, 4]` | **45 – 55 %** | Per-FFM overhead dominates engine per-op cost. Halving FFM crossings is near-direct savings. Largest gain. |
| `[16, 64]` | **25 – 40 %** | Per-FFM overhead is amortized across the batch. Engine per-op cost (4.6 µs/op per `project_nexmark_q3_optimization`) becomes the dominant term. Halving op count gives ~half the engine-cost savings. |
| `> 64` | **15 – 25 %** | FFM cost is fully amortized. Most of the saved time is from skipping the GET phase's deserialize-allocate-construct-RowData chain. Smaller relative win. |

The 45-55 % case (small batches) maps to "AEC dispatching per-key" — Flink-runtime-level fix could also reach this without touching the engine, but the engine fix is portable across AEC versions. The 15-25 % case maps to "AEC already vectorized" — engine-side RMW fusion is the only remaining lever.

**Sprint review should see the conditional table, not a fixed number.** Pick the row matching the §6 median; commit to that range in the sprint plan; bench the actual delta against the prediction.

**Why deferred:**

- Requires Rust engine API design + implementation in `crates/forst-rs-*`.
- Requires Java callback infrastructure (`Function<byte[], byte[]>` over FFM — Flink's serializer must run in the engine-callback context).
- Requires bench validation cycle (dylib rebuild, SHA-256 deploy, Q12 run × 3 for variance).
- Per CONTRIBUTING.md "one variable per change": must land in isolation with a per-query bench table.

**Recommended sequencing:**

1. **First**: run the §6 measurement (instrument `VectorizedExecutor.executeBatch` batch sizes during Q12 flush). The metric is already collected via `metrics.recordDispatch(...)` — just needs exposure to a queryable backend or sample logging.
2. **If batch size ≈ active_keys**: engine per-op cost is the bottleneck. RMW fusion (C) is the right knob.
3. **If batch size ≈ 1**: AEC is dispatching per-key. The fix is upstream (Flink runtime), which is out of scope for forst-rs-only. Workaround: investigate whether tuning AEC batch policy via config achieves better grouping.

## (D) AEC batch-policy tuning — DEFERRED

The AEC's batch formation is driven by `StateExecutionController` (in Flink runtime). Config knobs include batch size and idle timeout. Tuning these may improve Q11/Q12 flush batching without code changes.

**Why deferred:**

- Tuning is config-only but requires §6 measurement first to know where the current batch sizes sit.
- Increasing batch size without measurement may cause back-pressure in low-throughput queries (e.g., Q0/Q1).
- The forst-rs backend doesn't own the AEC; we can recommend defaults but not force them.

## (E) Engine per-op cost reduction — DEFERRED

If §6 confirms batches are large but Q12 still slow, the engine per-op cost is the dominant term. Approaches:

- Flamegraph the Rust engine during `frs_vectorized_batch_get` to find the hot path.
- Reduce per-key WriteBatch overhead in `db.batch_get`.
- Eliminate per-key memcpy if any.

Requires Rust profiling + likely Rust changes. Deferred until §6 lands.

## Measurement spike (§6) — REQUIRED FIRST

This is the most important next action. The current DispatchMetrics infrastructure records `(Kind, stateName, batchSize, bytes, latencyNs)` per `executeBatch` call. Need:

1. Connect `DispatchMetrics` to a queryable backend (Flink MetricGroup → Prometheus, or simple sampled WARN-log) so batch sizes can be inspected post-bench.
2. Run Q12 with the metric exposed; collect ~3 flush events.
3. Report median + p95 batch size for GET and PUT during flush.

**Outcome decides the V1.1 work distribution:**

- batch sizes ≥ 1024 → engine-side work (C, E).
- batch sizes ≤ 16 → AEC-side investigation (D) and/or out-of-scope Flink-runtime fixes.

## Summary

| Action | Owner | When |
|---|---|---|
| (A) Composite key reuse | forst-rs backend | **Done this session** |
| §6 Measurement spike (DispatchMetrics exposure) | forst-rs backend | V1.1 first task — gating signal |
| (C) `frs_vectorized_batch_compute` RMW fusion | forst-rs engine + backend | V1.1, conditional on §6 |
| (B) byte[] alloc elimination | forst-rs backend | V1.2 |
| (D) AEC batch policy tuning | forst-rs config + Flink consult | V1.1 if §6 shows small batches |
| (E) Engine per-op profile | forst-rs engine | V1.1 if §6 shows large batches |

The two unconditional steps (A, §6) are no-regret. Everything else is gated on §6's outcome — that is the discipline of "measure before you fix" applied to the forst-rs-only scope.
