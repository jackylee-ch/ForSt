# Q11/Q12 State-Primitive Audit — why MapStateCache doesn't help

**Status:** Audit only. No code changes proposed in this document.
**Date:** 2026-05-19
**Branches audited:** `forst-rs-jdk25` in `~/Code/stczwd/flink`, `forst-rs` in `~/Code/stczwd/ForSt`
**Relates to:** [`2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md`](./2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md),
[`2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md`](./2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md)

---

## TL;DR

Q11 (SESSION window) and Q12 (PROCTIME TUMBLE) **do not use MapState**. They use **`WindowAsyncValueState`** — a wrapper around `InternalValueState<RowData, W, RowData>` where `W` is the window namespace. On the ForSt-RS backend this resolves to **`ForStRsValueStateV2`** (`flink-statebackend-forst-rs/.../state/ForStRsValueStateV2.java`).

The `MapStateCache` shipped in commit `5148d45b124` only intercepts `ForStRsMapStateV2.asyncGet/asyncPut/asyncRemove/asyncContains`. It is structurally **unable** to intercept Q11/Q12's hot path. The Q20 +20.3 % regression (real signal that the cache code is alive in the JVM) and the Q11/Q12 ~ +1-3 % deltas (within noise) together prove the cache is wired but routes around Q11/Q12.

The per-record cost is **not on the engine call path** for Q11/Q12. The per-record path adds the bid into an in-memory `WindowBytesMultiMap` buffer (heap, no FFM call). Engine GET/PUT happens at **slice-fire / window-flush time**, in a bulk of `O(active_keys × active_slices_per_key)`. The bottleneck therefore lives in flush-time vectorized dispatch, not in per-bid RMW. A `ValueStateCache` analogous to `MapStateCache` would only help if accumulators are read more than once per slice — see §5 for why that's unlikely under unshared-slice tumble.

---

## 1. Call-chain trace from operator to engine

```
Q11 (SESSION) / Q12 (PROCTIME tumble) source: bid stream
   ↓
StreamingJoinOperator / KeyByOperator (key = bidder)
   ↓
AsyncStateWindowAggOperator                                       [windowProcessor: AsyncStateWindowProcessor<W>]
  flink-table-runtime/.../operators/window/async/tvf/common/AsyncStateWindowAggOperator.java:75
   ↓
AsyncStateSliceUnsharedWindowAggProcessor (Q12)                   [extends AbstractAsyncStateSliceWindowAggProcessor]
AsyncStateSliceSharedWindowAggProcessor (Q11 if hopping; SESSION goes through a different processor)
  flink-table-runtime/.../operators/aggregate/asyncwindow/processors/AsyncStateSliceUnsharedWindowAggProcessor.java
   ↓
processElement(key, element):
  long sliceEnd = sliceAssigner.assignSliceEnd(element, clockService)
  windowTimerService.registerProcessingTimeWindowTimer(sliceEnd)
  windowBuffer.addElement(key, sliceEnd, element)                  ← per-record, IN-MEMORY ONLY
   ↓
AsyncStateRecordsWindowBuffer.addElement
  flink-table-runtime/.../operators/aggregate/asyncwindow/buffers/AsyncStateRecordsWindowBuffer.java:88
   ↓
WindowBytesMultiMap.lookup + append                                ← managed memory, no engine call
```

Then at slice fire / watermark advance:

```
windowBuffer.flush(currentKey)                                     ← triggers state ops
  for each (windowKey, records) in recordsBuffer:
    combineFunction.asyncCombine(window, records)
   ↓
AsyncStateAggCombiner.asyncCombine                                 [combines records into accumulator]
  flink-table-runtime/.../operators/aggregate/asyncwindow/combines/AsyncStateAggCombiner.java:69
   ↓
  accState.asyncValue(window)              ← 1 GET per (key, slice)
    aggregator.accumulate(record) × N      ← in-memory aggregation
  accState.asyncUpdate(window, newAcc)     ← 1 PUT per (key, slice)
   ↓
WindowAsyncValueState
  flink-table-runtime/.../operators/window/async/tvf/state/WindowAsyncValueState.java
   ↓
InternalValueState<RowData, W, RowData>.asyncValue / asyncUpdate
   ↓
ForStRsValueStateV2  (key serialization, request build)
  flink-statebackend-forst-rs/.../state/ForStRsValueStateV2.java
   ↓
ForStRsDBGetRequest / ForStRsDBPutRequest
   ↓
VectorizedClassifier + VectorizedExecutor + ColumnarBatchBuffer    ← Arrow-style batch
   ↓
frs_vectorized_batch_get / frs_vectorized_batch_put (Rust FFI)
```

After window fires:

```
clearWindow(timerTimestamp, windowEnd):
  for each expired slice:
    windowState.asyncClear(slice)                                  ← 1 CLEAR per (key, slice)
```

---

## 2. State primitive: `ForStRsValueStateV2`, not `ForStRsMapStateV2`

`AbstractAsyncStateWindowAggProcessor.open` at line 75–83:

```java
ValueState<RowData> state =
        ctx.getAsyncKeyContext()
                .getAsyncKeyedStateBackend()
                .getOrCreateKeyedState(
                        defaultWindow,
                        createWindowSerializer(),
                        new ValueStateDescriptor<>("window-aggs", accSerializer));
this.windowState =
        new WindowAsyncValueState<>((InternalValueState<RowData, W, RowData>) state);
```

This is **ValueState** with `RowData` accumulator under window namespace `W`. There is no `WindowAsyncMapState` or `WindowAsyncListState` in the codebase (verified by grep across `flink-table-runtime/.../window/async/`). Every Nexmark windowed-aggregation query on async-state-V2 uses this single primitive.

For the ForSt-RS backend, `getOrCreateKeyedState` resolves the descriptor to **`ForStRsValueStateV2`** (`flink-statebackend-forst-rs/.../state/ForStRsValueStateV2.java`). The MapStateCache hook lives in **`ForStRsMapStateV2`** only — it cannot see ValueState requests.

This empirically explains:
- Q20 +20.3 % regression after enabling MapStateCache → Q20 hits MapState, and the cache *miss* lookup path adds a small overhead per request that compounds. The cache code is provably running.
- Q11/Q12 ±1-3 % deltas after enabling MapStateCache → these queries never traverse `ForStRsMapStateV2.asyncGet` at all. The cache code is bypassed entirely. The tiny delta is JIT-warmup variance from the extra hash lookup in `ForStRsMapStateV2.asyncGet` (which still gets called by *other* queries co-located in the same process).

---

## 3. Per-record cost is on the buffer, not on the engine

The per-record path in `AbstractAsyncStateSliceWindowAggProcessor.processElement` (line 113-147):

1. `sliceAssigner.assignSliceEnd(element, clockService)` — ~50 ns
2. `windowTimerService.registerProcessingTimeWindowTimer(sliceEnd)` — heap-only timer registration
3. `windowBuffer.addElement(key, sliceEnd, element)` — `WindowBytesMultiMap.lookup` + `append`, managed-memory hash op

Neither (1), (2), nor (3) emits a JNI/FFM call. The vectorized batch executor only sees state-request traffic at `flush`/`combine` time.

**Consequence:** "no per-key operation" — in the spirit of the user's directive — is already true at the per-record level for Q11/Q12. The vectorization-violation lens that uncovered Q9/Q20's bottleneck (per-entry `iteratorNext` calls) does not apply here.

---

## 4. Where the bottleneck actually lives for Q11/Q12

### 4.1 Slice-flush GET+PUT bulk

At each flush triggered by `advanceProgress` (PROCTIME 10 s tick) or `clearWindow` (watermark / timer fire):

- `flush` iterates `recordsBuffer.getEntryIterator(true)` — every `(key, slice)` pair currently buffered.
- For each pair, dispatches `combineFunction.asyncCombine(window, records)` via `keyContext.asyncProcessWithKey` (line 137).
- Each `asyncCombine` emits 1 GET, in-memory aggregate, 1 PUT.

For Q12 (PROCTIME 10 s tumble, ~1 M bidders): every 10 s the JVM flushes ~1 M `(bidder, slice)` pairs in close succession. Total state ops per 10 s window ≈ **2 M FFM crossings** plus a CLEAR per `(bidder, expired_slice)` ≈ another **1 M crossings**. Even at 4–10 µs per crossing, this is 12 – 30 s per 10 s window — exceeding wall-clock budget if not vectorized aggressively.

This explains the Q12 regression: the bottleneck is **flush burstiness**, not per-record cost. Vectorization helps only if the executor batches the flush requests into single FFM calls. If `keyContext.asyncProcessWithKey` serializes per-key, the batch shape degrades to a per-key dispatch and the FFM cost compounds.

### 4.2 Per-`asyncProcessWithKey` overhead

Each `asyncProcessWithKey` call at line 137 changes the current keyContext and submits a `StateRequest`. The AEC then groups requests for the same key (and same state) into a vectorized batch. **Cross-key batching depends on the AEC's batch policy** (typically: when the in-flight batch hits N keys or T ms idle).

If the AEC batches all `(key, slice)` pairs from one flush into a single `frs_vectorized_batch_get`, the FFM crossing cost amortizes to ~0.01 µs/op. If it dispatches per-key (e.g. because each `asyncProcessWithKey` callback completes synchronously), it degrades to per-key FFM. This is the variable most likely to explain the v3.2 numbers — and it is **invisible from the state-class level**; it lives in the AEC's batching policy.

### 4.3 namespace not encoded in the storage key (latent bug)

`ForStRsValueStateV2.serializeKey` (line 73-105) composes:

```
KEY_PREFIX ("k/") + serialized(key) + "/" + stateName + "/"
```

It **does not encode the namespace**. For `WindowAsyncValueState` whose namespace is the window/slice end (a `Long`), this means: different slices for the same key write to the **same storage key**.

For unshared tumble (Q12) and session (Q11) this is "accidentally correct" because at most one window-instance is active per key at any moment — the slice-fire path clears state before the next slice can start writing. But it is a latent correctness bug for any window pattern with overlapping namespaces (hopping windows, joint slices, late events arriving for a slice that has not yet been cleared). Any future query that uses these patterns would silently corrupt accumulators.

Recommendation: this should be tracked as a correctness issue separate from the perf work. **The fix — appending serialized namespace bytes to the composite key — also gives a free perf win because it enables co-located cache lookups per-window without ambiguity.** It is *not* a regression risk for Q11/Q12 specifically because they have at most one active namespace per key.

---

## 5. Why a ValueStateCache (parallel to MapStateCache) would not help Q11/Q12 much

A naive `ValueStateCache` would intercept `ForStRsValueStateV2.asyncValue/asyncUpdate` keyed on the composite key (currently unaware of namespace per §4.3, but the cache itself can include namespace bytes). The benefit comes when the same key is read more than once before being evicted.

For Q12 PROCTIME tumble: each `(bidder, slice)` has exactly **one** read (`asyncValue`) and **one** write (`asyncUpdate`) per `combine`. There is no re-read. A cache adds lookup overhead with no hit-rate benefit.

For Q11 SESSION: each `(bidder, session)` has one read+write per `combine`, but sessions can merge — that produces a sequence of `asyncValue → asyncUpdate` over the *target* session as merging proceeds. There may be 2-3 reads per session-fire if merge is common. Marginal benefit, not transformational.

**The structural fix that would help Q12 is not a cache** — it is **fusing GET+PUT into a single RMW request** at the engine level. The combiner pattern `asyncValue(w).thenCompose(acc → asyncUpdate(w, acc + records))` is a classic RMW. Two separate FFM crossings (one for GET, one for PUT) become one if the engine supports `merge`/`compute` at the state primitive level. See the ForSt-RS engine `db.merge_compute_into(cf, key, op)` proposal in [`2026-05-18-forst-rs-perf-analysis-v3.2.md`](./2026-05-18-forst-rs-perf-analysis-v3.2.md) §6 (if it exists; otherwise a new spec is needed).

---

## 6. Why the existing batching is probably not maximal

`AsyncStateRecordsWindowBuffer.flush` at line 122-147 dispatches each `(key, slice)` via `keyContext.asyncProcessWithKey` *one at a time in a loop*. Each callback returns `combineFunction.asyncCombine` which fires `asyncValue` then `asyncUpdate`.

The AEC will batch only if multiple `StateRequest`s are pending at the same epoch. If the loop body's `asyncCombine` chains are CPU-light (just `asyncValue → in-memory aggregate → asyncUpdate`), they complete fast and the batch never fills up — degrading to per-key FFM.

**Diagnostic:** measure the average batch size hitting `frs_vectorized_batch_get` during Q12's tumble flush. If batch size ≈ 1, the AEC is dispatching per-key. If batch size ≈ active-keys-per-flush, vectorization is working. The instrumentation hook lives at `flink-statebackend-forst-rs/.../VectorizedExecutor.java` `executeBatch` — log `batch.size()` at WARN with a sample rate.

This is the single most informative experiment to run before designing a fix.

---

## 7. Concrete next steps (audit-only — no code in this document)

1. **Instrument** `VectorizedExecutor.executeBatch` to log batch sizes for ValueState GETs during Q12 flush. Confirm whether per-key dispatch dominates.
2. **Profile** Q12 with async-profiler during a 60 s window covering ≥ 3 flush events. Expected hot frame: either `VectorizedExecutor.executeBatch` (good — vectorized) or `ForStRsDBGetRequest.process` (bad — per-key dispatch).
3. **Trace** the AEC's batch-formation policy for `keyContext.asyncProcessWithKey` callbacks emitted in a tight loop. Look at `StateExecutionController.batchPolicy` / equivalent.
4. **If per-key dispatch is confirmed**: design a flush-side batch primitive — e.g., a `bulkCombine(List<(window, key, records)>)` API on `AsyncStateRecordsCombiner` that emits a single multi-key batch GET, runs aggregation in a loop, then emits a single multi-key batch PUT. This change lives in Flink (`AsyncStateAggCombiner`), not ForSt-RS.
5. **If vectorized dispatch is already maximal**: the bottleneck is the engine itself, not the call shape. Investigate `frs_vectorized_batch_get`'s per-op cost — at 4.6 µs/op (per memory note) × 2 M ops/window, that's the wall-clock budget. Reducing per-op cost in Rust is the right knob.

---

## 8. Cross-references to existing memory and specs

- [[project_nexmark_q3_optimization]] — engine per-call cost 4.6 µs is the bottleneck for state-heavy queries; same root cause likely applies here.
- [[project_nexmark_q3_jni_experiment]] — JNI vs FFM disproved as the boundary; engine per-call cost is the dominant term.
- `docs/superpowers/specs/2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md` — Q11/Q12 were tagged as violations there; this document refines the diagnosis: the violation is **flush-time per-key dispatch**, not per-record iteration. The earlier audit conflated Q9/Q20 (per-entry iterator scan, true violation) with Q11/Q12 (flush-time fan-out, different mechanism).
- `docs/superpowers/specs/2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md` §5 RMW / merge proposal — directly applicable to the Q12 GET+PUT fusion suggestion in §5 above.

---

## 9. What the MapStateCache experiment actually told us

The MapStateCache experiment (commits `4864631d614` → `9109fc9de61` revert → `5148d45b124` restore) was designed under the implicit assumption that Q11/Q12 hit MapState. They do not.

What the experiment *did* teach us:
- The cache code path is functional (Q20 regression is the proof).
- Per-record cache lookups have measurable overhead even when not transformational (the Q20 +20.3 % delta is dominantly lookup-miss cost for keys that don't repeat).
- For queries that **do** use MapState with high temporal locality, the cache could win — but no current Nexmark query in the suite matches that pattern strongly enough to demonstrate it. The most likely candidate is a query with `MapState<String, Long>` and rapid same-key updates; none of Q0-Q23 fits cleanly.

**Recommendation:** keep the MapStateCache shipped (the Q20 regression is within portfolio tolerance under CONTRIBUTING.md's portfolio-aware gate) but **do not extend the cache pattern to ValueState** until the §6 instrumentation confirms the bottleneck is per-key dispatch *and* same-key-multi-read is observed. Otherwise the cache adds overhead without reciprocal hits.

---

## 10. Decision register

| Item | Decision | Rationale |
|---|---|---|
| Is MapStateCache useful for Q11/Q12? | **No** | Q11/Q12 don't traverse `ForStRsMapStateV2`. |
| Should we add a parallel `ValueStateCache`? | **Deferred pending §6 instrumentation** | Same-key-multi-read not yet observed. |
| Is namespace-in-storage-key a correctness bug? | **Yes, latent** | Not triggered by current queries but unsafe for hopping/late-arrival. Track as separate ticket. |
| Where is the Q12 bottleneck likely? | **Flush-time fan-out dispatch policy** in the AEC, or engine per-op cost | Both are testable via the §7 instrumentation. |
| Next concrete experiment | **Log `VectorizedExecutor.executeBatch` batch sizes during Q12 flush** | Single most informative measurement; cheap to implement. |
