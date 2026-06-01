# 2026-05-29 Profile-driven perf restoration (v3.8 regression recovery)

## Context

Full no-timeout sweep (2026-05-29) measured **forst-rs 4795s vs rocksdb 3597s = 0.75×** (33% slower), with 8 queries timing out at 600s (q4/q5/q7/q9/q15/q16/q17/q18/q19). v3.8 had forst-rs winning these same queries 5×–15× (on LOCAL FS). The /goal's only binding metric is **q0-q22 total ≥ 3× faster** (forst-rs ≤ 1199s).

Per user directive: V1-sync is acceptable, FFI (not JNI) is kept, Flink-runtime is immutable. Restore v3.8-level performance within forst-rs engine + backend only.

## Methodology: profile before patching

A v3.8→HEAD audit (subagent) identified 14 candidate per-record cost regressions. Five were landed blind (deleteFromWriteBuffer flush removal, get_internal HashSet fast-path, requiresOrderedDispatch iter gate, resident cap 4→16 GiB, fatal_error AtomicBool) — **none individually moved q4 off the 600s timeout.**

Switched to evidence-first (systematic-debugging Phase 1): jstack-sampled the TaskManager during a live q4 run, filtered to the `Join[10]` worker threads.

### Profile finding #1 — per-batch native MemorySegment allocation

Both Join threads RUNNABLE in:
```
jdk.internal.misc.Unsafe.allocateMemory0
  ← jdk.internal.foreign.SegmentFactories.allocateNativeSegment
  ← java.lang.foreign.ArenaImpl.allocate
  ← org.apache.flink.state.forstrs.ColumnarBatchBuffer.<init> (line 73-74)
  ← VectorizedClassifier.initNewKindBuffers (line 197)
  ← VectorizedExecutor.createRequestContainer (line 259)
  ← AsyncExecutionController.triggerIfNeeded → drainInflightRecords
```

`createRequestContainer()` was allocating a **fresh `VectorizedClassifier` + fresh `AppendMergeBatchBuffer` + fresh `ColumnarBatchBuffer` (native MemorySegments) per batch.** At q4's batch rate this is a native malloc + memset(0) per drain — pure overhead. The class's own javadoc documents the buffers are safely shareable across batches (synchronous-FFI contract), but `createRequestContainer` ignored that and rebuilt them.

### Profile finding #2 — O(N) clock-sweep eviction at 1M entries

After fix #1 deployed, re-profile showed both Join threads RUNNABLE in:
```
org.apache.flink.state.forstrs.cache.MapStateCache.evictClockSweep (line 540)
  ← MapStateCache.put (line 274)
  ← MapStateCache.putIfAbsent
  ← ForStRsMapStateV2.lambda$asyncGet$0 (line 308)
```
Accumulating 21s–65s CPU on a 64–99s elapsed window — i.e. >50% of wall time.

`evictClockSweep` did a **full O(N) linear scan** over the `accessTime` MemorySegment to find the global-oldest victim. At `DEFAULT_MAX_ENTRIES = 1_048_576`, once the cache fills every `put` triggers a 1M-element scan. For a join workload with working set > 1M, that's a 1M-read scan on essentially every record.

## Fixes landed

### Fix A — pooled VectorizedClassifier (`VectorizedExecutor.createRequestContainer`)

```java
VectorizedClassifier classifier = pooledClassifier;
if (classifier == null) {
    classifier = new VectorizedClassifier(getKeys, putKeys, putValues, deleteKeys);
    classifier.initNewKindBuffers(arena);
    pooledClassifier = classifier;
}
classifier.reset();
for (String name : listStateNames) classifier.registerListState(name);
return classifier;
```

The classifier + its native buffers are now allocated **once** and `reset()` between batches. Safe because: (1) FFI is synchronous (V1 contract) — batch N fully completes before N+1's container is requested; (2) the executor already shares `getKeys/putKeys/putValues/deleteKeys` across batches via the same mechanism. Eliminates one native MemorySegment alloc + memset per batch.

### Fix B — sampled-K eviction (`MapStateCache.evictClockSweep`)

```java
int sampleCount = Math.min(16, size);
int victim = clockHand;
long oldest = Long.MAX_VALUE;
for (int i = 0; i < sampleCount; i++) {
    int row = (clockHand + i) % size;
    long ts = accessTime.get(JAVA_LONG, (long) row * Long.BYTES);
    if (ts < oldest) { oldest = ts; victim = row; }
}
clockHand = (victim + 1) % Math.max(1, size);
evictRow(victim);
```

Replaces the O(N) global-oldest scan with a sampled-16 approximation (Redis-style LRU sampling). The clock hand advances so successive sweeps cover different windows. The R24-M3 correctness invariant (seed `oldest = Long.MAX_VALUE` to avoid post-clear stale-stamp bias) is preserved. Per-eviction cost drops from O(1M) to O(16).

## Other fixes landed this session (blind, correctness-preserved, retained)

1. `ForStRsKeyedStateBackend.deleteFromWriteBuffer` (line 1411) — removed per-CLEAR `flushWriteBuffer()`; invariant moves to snapshot boundary.
2. `column_family.rs:464` — new `has_resident_flushed()` predicate; `db.rs:6562` — fast-path skip resident HashSet build when empty.
3. `column_family.rs:47` — `DEFAULT_RESIDENT_FLUSHED_CAP_BYTES` 4→16 GiB.
4. `VectorizedExecutor.requiresOrderedDispatch` (line 540) — removed unconditional `iterRequests.isEmpty() → ordered dispatch` gate.
5. `db.rs:268` — `fatal_error_set` AtomicBool fast-path; per-op `check_fatal_error` is now a relaxed atomic load instead of a Mutex acquisition.

## Status

After fixes A+B, q4's jstack hot frame moved from `evictClockSweep` → `ForStRsLinker.vectorizedBatchGet` (genuine engine GET work) — the eviction bottleneck is closed. q4 still exceeds 600s in isolation (now engine-GET-bound, a separate cost). Full q0-q22 sweep in progress to measure aggregate; fixes A+B especially target q15/q16/q17 (DataView-heavy on the same MapStateCache) which should benefit from the eviction fix even where q4 doesn't.

## Next bottleneck (per latest jstack)

q4 is now bound on `vectorizedBatchGet` — the engine batch-GET FFM downcall. Candidates:
- Cache miss rate too high (1M-entry MapStateCache vs q4 join working set) → each miss is an engine GET hitting S3-backed SSTs.
- Engine GET LSM-walk cost on S3-primary storage (vs v3.8 LOCAL FS).
Next session: profile the Rust side of `vectorizedBatchGet` (where time goes inside the engine — memtable probe vs SST read vs S3 fetch).

## Cross-refs
- [[project_full_sweep_2026-05-29]]
- v3.8 baseline: `docs/superpowers/specs/2026-05-21-forst-rs-benchmark-report-v3.8.md`
- audit candidates: `project_full_sweep_2026-05-29` memory note
