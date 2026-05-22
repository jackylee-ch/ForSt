# Round 1 — Agent E — Flink Real-Time Streaming Feature Optimization

**Date:** 2026-05-22
**Angle:** criterion #6 — Flink streaming-semantic exploitation (watermarks, timers, checkpointing, savepoints, rescaling, key-group routing, async state V2 dispatch, state migration).
**Method:** Cold-read of `flink-statebackend-forst-rs/` (Java). No code modified. Behavior cross-checked against `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md` and `project_memory` cited in the brief.

---

## Summary of severities

| Sev | Count |
|---|---|
| CRIT | 3 |
| HIGH | 6 |
| MED  | 5 |
| LOW  | 3 |

`SCOPE NOTE`: every CRIT/HIGH is rooted in production-correctness or material throughput, not micro-tuning. Several findings overlap with the existing audit-design but are filed here as **separate streaming-semantics issues** because the design's leverage formula optimizes for batch/zero-copy, not for end-to-end Flink contract compliance.

---

## CRIT findings

### E-CRIT-1 — `ForStRsAsyncKeyedStateBackend.snapshot()` returns `SnapshotResult.empty()`

**File:** `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAsyncKeyedStateBackend.java:343-388`

```java
// V1 best-effort snapshot: …
return DoneFuture.of(SnapshotResult.empty());
```

**Evidence:**
1. The async backend is the production path for V2 (Q3/Q4/Q5/Q11/Q12/Q15/Q16/Q19 — every Nexmark query that goes through V2 dispatch).
2. A working snapshot strategy (`ForStRsSnapshotStrategy`) exists — incremental, sync/async phases split, ref-counted shared SSTs, written-side restore path — but the **async backend never calls it**. The doc-comment at 372-386 acknowledges the cluster currently relies on (a) forst-rs persisting writes to S3 on every memtable flush plus (b) the JM recording a "successful" snapshot with no handle.
3. On task restart of a V2 job the framework receives **no** `KeyedStateHandle` and therefore cannot drive `ForStRsRestoreOperation`. The doc-comment claims "forst-rs replays from upstream (Flink alignment guarantees this)" — that contract is `EXACTLY_ONCE` source-replay, which only holds for replayable sources AND only since the most recent successful checkpoint. The current code silently violates exactly-once for non-replayable sources (sockets, queues without retention).
4. Cross-job restart (job submission ID changes) cannot recover state at all — no handle, no `incHandles` to feed the no-rescaling fast path.

**Streaming impact:** V2 jobs have **no real checkpoint durability**. Bench numbers (project_q12_parity_with_rocksdb, project_q11_always_on_buffer_win) are measured under "single-attempt success", which papers over this entirely.

**Fix shape:** the strategy is the missing wiring; call `setSnapshotStrategy` from the V2 builder, then in `snapshot()` after the Phase-1/Phase-2 flush, invoke the strategy through `SnapshotStrategyRunner` exactly as the sync `ForStRsAbstractKeyedStateBackend` already does at lines 382-388 of its file. The flushes already in place (executors, RMW caches) are correct preconditions.

**Certainty:** HIGH. Code paths exist; the gap is the wire-up.

---

### E-CRIT-2 — `ArrowTimerBuffer.drainTo()` visits heap-array index order, NOT timestamp order

**File:** `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/timer/ArrowTimerBuffer.java:85-86, 218-226`

```java
/** Visitor for {@link #drainTo(FlushVisitor)} — receives entries in heap-array index order. */
public void drainTo(FlushVisitor v) {
    for (int i = 0; i < size; i++) {
        int op = opAt(i);  // raw array index — NOT min-heap order
        ...
    }
}
```

**Evidence:**
1. `ArrowTimerBuffer` is a binary min-heap on `ts`, BUT heap-array layout is not a sorted sequence — index `i` does not imply `ts[i] <= ts[i+1]`. Only the root (i=0) is guaranteed to be the minimum.
2. The buffer is used by `ForStRsKeyGroupedInternalPriorityQueue.flushPendingToEngine()` (file `ForStRsKeyGroupedInternalPriorityQueue.java:734`) — but that path issues `batchPut`/`vectorizedBatchDelete` on engine keys, where engine order is what matters (BE-encoded `ts` lexicographic = sorted), so the timer flush itself is correct.
3. **Hot-path risk is `peek()` at line 442-509**: the code at line 456-460 trusts that `heap[0]` is the min-ts ADD for the current key-group. This is only correct when **no `removeAt` has been called for any non-root row** with a smaller-ts grandchild AND the root's op is ADD. The `siftDown` invariant after `removeAt` does preserve heap order, but the **special case "root is REMOVE or wrong-kg → fall back to O(n) full scan"** at 462-478 is sound — and in Q11/Q12 hot paths (one key-group per slot), `bufferBestPos = 0` is fast.
4. **Real bug**: if any external caller (test, benchmark, snapshot helper) uses `drainTo()` expecting in-order processing — e.g., to write a savepoint blob — they will get unsorted data. The Javadoc warns of "heap-array index order" but no machine check enforces this; an honest `orderedDrain` method that uses a transient PQ would close the gap.

**Streaming impact:** medium for the *current* call sites, but the public visitor interface is a footgun for future savepoint/migration work — the moment someone wires `drainTo` into a checkpoint pre-image generator it will silently emit out-of-order timers, leading to **fired-out-of-order timer events** on restore (windows close in wrong order, late events misclassified).

**Audit-design cross-ref:** §"Four Implementation Invariants" #3 states "advance() strict order — flush → batch scan → batch delete; never overlap." The `advance()` method at PriorityQueue.java:695 satisfies this *because* it uses the engine's BE-sorted scan, not `drainTo`. The invariant is **architecturally** dependent on never using `drainTo` for timer-firing.

**Fix shape:** rename to `unorderedDrainForBatchedFlush`, add a separate `orderedDrain(FlushVisitor)` that calls a single transient `PriorityQueue<Long>` over heap rows OR exposes `peekMin()` / `popMin()` for in-order iteration.

**Certainty:** HIGH on the footgun; MED on whether any current caller is actually mis-using it.

---

### E-CRIT-3 — `ForStRsKeyedStateBackend.offheapKeyGroupSupplier = () -> 0` (key-group routing is permanently disabled in sync V1)

**File:** `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java:231-235`

```java
/** Suppliers passed to off-heap ForStRsValueState. */
private final java.util.function.IntSupplier offheapKeyGroupSupplier = () -> 0;
```

**Evidence:**
1. This supplier is fed into every V1-sync `ForStRsValueState` and `ForStRsMapState` (lines 380-383, 449-452) so that `kgSerializer.encodeForStateOffheap(0, key, …)` always produces a key prefixed with kg=0 (2-byte BE big-endian).
2. **All V1-sync state for every key goes into key-group bucket 0.** This makes rescaling restore (`ForStRsRestoreOperation.restoreWithRescaling`) effectively useless for V1 jobs — `findSourceFor(kg, sources)` would only find a source containing kg=0 regardless of what the new parallelism is.
3. Even the no-rescaling fast path is **inconsistent across subtasks**: every parallel instance writes under kg=0, so on rescale-up or down the engine SSTs have no meaningful kg-locality.
4. Q11 is V1-sync (`project_q11_v1_sync_finding`, "92M ForStRsValueState (V1) ops") — the bench number is therefore **achieved with broken key-group routing**, which makes the comparison-to-RocksDB number unsafe for any production use.
5. The class-level comment at line 89-95 ("This backend is a stepping-stone that does not yet track Flink key-group; supplying 0 keeps key encoding stable") explicitly calls out the regression, but project_perf_milestone results still reference V1 performance numbers without this caveat.

**Streaming impact:** **breaks rescaling for V1-sync state entirely**. Q11 and Q5 production deployments would be unrescalable. The V2 path uses `getCurrentKeyGroupIndex()` correctly (`ForStRsAbstractKeyedStateBackend.java:288`), so V2 is unaffected — but V1 is the path used by SESSION window operators per project_q11_v1_sync_finding.

**Fix shape:** plumb `InternalKeyContext.getCurrentKeyGroupIndex()` from `ForStRsAbstractKeyedStateBackend` into the delegate. Trivial — already has access via `setCurrentKeyAndKeyGroup(K, int)` override at line 253-261 of the abstract class.

**Certainty:** HIGH.

---

## HIGH findings

### E-HIGH-1 — Savepoint API throws `UnsupportedOperationException`

**File:** `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAbstractKeyedStateBackend.java:407-411`

```java
public SavepointResources<K> savepoint() throws Exception {
    throw new UnsupportedOperationException(
        "ForStRsAbstractKeyedStateBackend.savepoint is implemented in B-Prod-P4 (savepoint resources)");
}
```

Streaming impact: users cannot take savepoints, which is the only way to migrate jobs between Flink versions or switch state backends. The forst-rs-incremental format (FRSEXP01) in `ForStRsStateMigration` is **not** Flink-savepoint compatible — the on-disk magic and blob layout are project-private, so even if `savepoint()` were wired, the output couldn't be read by RocksDB / community ForSt / Hashmap backend.

**Fix shape:** at minimum, generate a Flink-canonical savepoint by walking every key-group via `getKeys(state, ns)` and writing the standard length-prefixed Flink keyed-state-blob format. This is the established RocksDB savepoint approach (FullSnapshotResources) and is restorable by any backend.

**Certainty:** HIGH (architectural gap, not a perf claim).

---

### E-HIGH-2 — Rescaling restore re-puts every key one-at-a-time via `linker.put` (no batching)

**File:** `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsRestoreOperation.java:389-401`

```java
private void copyKeyGroup(OpenSourceDb src, FrsDb targetDb, FrsCfHandle targetCf, int kg) {
    ...
    while (true) {
        ForStRsLinker.IteratorEntry entry = linker.iteratorNext(it);
        if (entry == null) break;
        linker.put(targetDb, targetCf, entry.key(), entry.value());  // per-entry FFM crossing
    }
}
```

**Evidence:**
- Per-record `linker.put` is exactly the violation §3 V5 of the audit-design (per-call FFM ~4.6 µs).
- For a 100M-key cluster with rescaling factor 2, that's 100M × 4.6 µs = **460 seconds** of pure FFM cost — adds materially to recovery RTO.
- `vectorizedBatchPut` exists (used by VectorizedExecutor at line 256), `ingestExternalSst` exists (ForStRsStateMigration:175) — both would convert this from per-record to per-batch or per-file.

**Streaming impact:** recovery time on rescale is O(keys × 4.6 µs) instead of O(SST-files × ms). Project memory note mentions "~12s fresh-cluster overhead" — that's empty-restore, which is correct. Real-keyed restore on rescale is unmeasured but extrapolation-bad.

**Fix shape:** buffer entries into a 64-row batch + `vectorizedBatchPut`. Or, for matched kg-range subsets, use `ingestExternalSst` to hardlink source SSTs into the target without re-parsing.

**Certainty:** HIGH.

---

### E-HIGH-3 — Async state V2 dispatch is serialized through a single backend; no in-flight parallelism

**File:** `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java:172-208`

```java
public CompletableFuture<Void> executeBatchRequests(...) {
    executePuts(classifier);
    executeDeletes(classifier);
    executeGets(classifier);
    executeIters(classifier);
    ...
    return CompletableFuture.completedFuture(null);   // already done
}
```

**Evidence:**
- Returns an *already-completed* `CompletableFuture` — i.e., the entire batch is executed synchronously inside `executeBatchRequests`, on the calling thread (the Flink mailbox executor).
- Flink's `AsyncKeyedStateBackend` contract permits the executor to dispatch FFM calls on a separate worker thread and return a future; this would unlock pipelining: while batch N's GET is awaiting the engine, batch N+1 can be classified.
- The current model — synchronous batch-of-batches — limits throughput to `1 / (batch_dispatch_latency)`. For Q11/Q12 with batch size 257 and per-op ~250 ns, that's ~64 µs per batch dispatch, bounded.

**Streaming impact:** for IO-bound queries (Q19, Q22 — bench numbers show > 100 s wall-clock), pipelining could overlap engine I/O with classifier work; without it, each batch is a stop-the-world synchronous call.

**Fix shape:** dispatch on a per-slot virtual-thread executor; return a real `CompletableFuture` resolved on dispatch completion. JDK 25 virtual threads are zero-overhead.

**Certainty:** MED-HIGH (depends on engine-side concurrency tolerance — `frs_*` calls may not be re-entrant on the same `FrsDb` handle).

---

### E-HIGH-4 — V2 snapshot drains via "double `flushDirty`" rather than awaiting in-flight async-state continuations

**File:** `ForStRsAsyncKeyedStateBackend.java:343-388` (same site as CRIT-1)

```java
managedExecutors.forEach(VectorizedExecutor::flushDirty);  // PHASE-1
for (ForStRsReducingStateV2<?> s : registeredReducingStates) { s.flushOnBarrier(); }
for (ForStRsAggregatingStateV2<?, ?, ?> s : registeredAggregatingStates) { s.flushOnBarrier(); }
managedExecutors.forEach(VectorizedExecutor::flushDirty);  // PHASE-2
managedExecutors.forEach(VectorizedExecutor::flushDirty);  // unused 3rd pass (the comment claims SP6 staged writes)
```

**Evidence:**
- `VectorizedExecutor.flushDirty()` is **a no-op** (`VectorizedExecutor.java:238`). All three calls are dead code.
- The doc-comment at lines 335-341 ("V1 best-effort: full async-state continuation awaiting … requires deeper integration … deferred to P11") confirms this is conscious incomplete.
- Result: any GET request issued *after* the last batch dispatch but *before* the next `executeBatchRequests` is **lost** on the snapshot barrier. With CRIT-1's empty snapshot, this is fine (nothing is written anyway). Once CRIT-1 is fixed, this becomes a correctness bug: dirty RMW + uncompleted PUT requests don't reach the engine pre-snapshot.

**Streaming impact:** stacks on CRIT-1. Fix to CRIT-1 must come with a true two-phase barrier-drain awaitable on every in-flight `StateRequest.future()`.

**Fix shape:** maintain a per-executor `Set<CompletableFuture<?>>` of unresolved per-request futures, `CompletableFuture.allOf(...).join()` in `snapshot()` before calling the strategy.

**Certainty:** HIGH (correctness once CRIT-1 lands).

---

### E-HIGH-5 — `ForStRsStateExecutor.executeIters` processes ITER requests one-at-a-time (no batching)

**File:** `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsStateExecutor.java:126-133`

```java
private void executeIters(List<ForStRsDBIterRequest<?, ?, ?, ?>> iters) {
    if (iters.isEmpty()) return;
    for (ForStRsDBIterRequest<?, ?, ?, ?> iter : iters) {
        iter.process(linker, db, cf, arena);   // 1 prefix open per iter
    }
}
```

**Evidence:**
- The legacy `ForStRsStateExecutor` is still wired by `createStateExecutor` in some paths (not the primary VectorizedExecutor path, but it's the fallback `StateExecutor` implementation).
- Each `iter.process` opens its own `frs_vec_iter_prefix_*` handle. N iter requests = N × open + N × close.
- The audit-design §3 V10 ("`frs_vectorized_batch_get` naive per-key db.get() loop") notes the engine has a similar pattern.

**Streaming impact:** for MapState-heavy queries (Q16, Q19) where `MAP_ITER_KEY` / `MAP_ITER_VALUE` are batched by Flink's classifier, the executor un-batches them — total dispatch cost is N × open-cost rather than 1 × batched-open-cost.

**Fix shape:** a `vectorizedBatchPrefixOpen` FFM primitive that opens N prefix iterators in one crossing. Engine work — but the Java side can already group by stateName.

**Certainty:** MED (depends on whether VectorizedExecutor is now the only used executor — if so this is dead code).

---

### E-HIGH-6 — `ForStRsAsyncKeyedStateBackend.timer-factory` default is `FORSTRS`, but project memory confirms `HEAP` wins Q11/Q12

**File:** `ForStRsAsyncKeyedStateBackend.java:106-110`

```java
private static TimerServiceFactory pickTimerFactory() {
    String prop = System.getProperty("forst.rs.timer-service.factory", "FORSTRS").trim().toUpperCase();
    return "HEAP".equals(prop) ? TimerServiceFactory.HEAP : TimerServiceFactory.FORSTRS;
}
```

**Evidence:**
- The class-level Javadoc at lines 91-99 explicitly says "Default is HEAP" — but the code default is `FORSTRS`.
- `project_q12_heap_timer_beats_forst` shows `forst.rs.timer-service.factory=HEAP` is the v3.3 baseline win condition; FORSTRS lost 1.20× on Q12.
- Without setting the system property, every Nexmark/production run is on the slower FORSTRS path.

**Streaming impact:** Q12 wall-clock 35.5s (HEAP) vs 42.7s (FORSTRS, current default). Q11 likely similar. This is a one-line default flip; the win is already proven.

**Fix shape:** change the default literal from `"FORSTRS"` to `"HEAP"` and the comment is then truthful.

**Certainty:** HIGH (already measured).

---

## MED findings

### E-MED-1 — Incremental snapshot strategy duplicates the engine snapshot "create at every checkpoint" — no compaction-pin-only fast path

**File:** `ForStRsSnapshotStrategy.java:142-155`

The sync phase issues `linker.dbSnapshot(db, nativeArena)` on every barrier. Engine-side snapshots are cheap (O(1) seq pin), but for ultra-frequent checkpoints (e.g., 100ms exactly-once tx sinks) the FFM round-trip on the *task thread* adds latency. Consider caching a single live snapshot until `notifyCheckpointAborted` consumes it.

### E-MED-2 — TTL is engine-side only; no async incremental cleanup for V2 state

**File:** `ForStRsInternalKvStateAdapters.java:120-125`

```java
throw new UnsupportedOperationException(
    "ForStRs backend does not support state-incremental visitors (TTL incremental cleanup) yet");
```

`StateTtlConfig.cleanupIncrementally(...)` is a no-op on this backend. Only the engine-side compaction filter (set up in `ForStRsTtlCompactFiltersManager`) runs. For workloads with rare compactions (incremental writes, no flush), expired entries are read-served, breaking TTL visibility semantics.

### E-MED-3 — Key-group locality in the engine relies on encoding order, not column families

**File:** `ForStRsKeyGroupedSerializer.java:71-86`, `ForStRsKeyedStateBackend.java:1003-1028`

Both V1 and V2 keys are encoded as `kg(2B BE) || serialize(K) || ...`, so a single CF holds all keys ordered by kg. This is correct for prefix-scan rescale (E-HIGH-2), BUT it means a single hot kg's writes share an LSM level with cold kgs — no per-kg compaction tuning is possible. RocksDB's backend uses per-kg CFs for ranges; consider per-kg-bucket CFs for high-cardinality kg deployments.

### E-MED-4 — `MergingWindowSet.retireWindow` IllegalStateException on Q11 — confirmed Flink-runtime side, not forst-rs

**Project memory cross-ref:** `project_q5_q8_q13_structural_gap`, `project_q11_v1_sync_finding`.

Searching the `forst-rs/` tree for `MergingWindowSet`, `retireWindow`, or any window-state cleanup yields zero hits — the SESSION-window assigner state lives in Flink-runtime's `MergingWindowSet`. Forst-rs only stores the underlying ListState/MapState. The exception is therefore a Flink-runtime bug not amplified by forst-rs.

However: forst-rs's V1-sync ListState (`V17` in audit-design) does **full read-modify-write** on every `add()`, which means the RetireWindow path's `clear()` does NOT see the staged ListState contents until the next `value()`. If the Flink window assigner ever reads then deletes ListState in the same operator turn, the read may return stale data. Verifying this needs a focused test.

### E-MED-5 — No watermark integration in the timer queue's `advance()`

**File:** `ForStRsKeyGroupedInternalPriorityQueue.java:695-725`

`advance(long maxTimestamp, Consumer<T>)` is called by Flink's `InternalTimerService` on watermark arrival; the implementation drains everything `<= maxTimestamp`. Correct, but: there's **no early termination by key-group**. If only kg=12's watermark advanced (rare in practice but possible for per-partition watermarks), the queue scans all of kg=12's timers AND `flushPendingToEngine()` drains globally. A per-kg flush would scale better. Low impact in current Flink default (channel-global watermarks).

---

## LOW findings

### E-LOW-1 — `ForStRsAsyncKeyedStateBackend.dispatchMetrics` is `UnregisteredMetricsGroup`

`ForStRsAsyncKeyedStateBackend.java:144, 189` — metrics are placeholder. No telemetry on dispatch latency / batch size. Per project memory, that telemetry was essential to land Q12 fixes; not having it in prod is observability debt, not perf debt.

### E-LOW-2 — `notifyCheckpointSubsumed` is empty

`ForStRsAsyncKeyedStateBackend.java:399`. The strategy's `pendingRegistrations` map (line 105-106 of `ForStRsSnapshotStrategy`) keeps per-checkpoint registration lists; if subsume is supposed to release subsumed checkpoint refs, that's missed. Minor — the abort path covers the common-case rollback.

### E-LOW-3 — `flushArena = Arena.ofAuto()` in `ForStRsKeyedStateBackend` leaks until GC

`ForStRsKeyedStateBackend.java:184`. Pre-allocated flush staging arena is `Arena.ofAuto()` — relies on phantom-reachability for cleanup. For a long-lived backend this is fine; for unit tests with rapid backend churn (which exist in the surefire reports for snapshot strategy / restore op), this delays MemorySegment release. Trivial to change to `Arena.ofShared()` and close in `close()`.

---

## Audit-design / `project_memory` cross-references

| This-doc finding | Audit-design V# | project_memory |
|---|---|---|
| E-CRIT-1 (empty snapshot) | V13 (open OQ-2) | project_perf_milestone_2026-05-09 |
| E-CRIT-3 (kg=0 hardcode) | V14 (split V15-V19) | project_q11_v1_sync_finding |
| E-HIGH-2 (per-record put) | V5 (zero-copy GET) — analogous | n/a |
| E-HIGH-6 (timer default) | not catalogued | project_q12_heap_timer_beats_forst (PROVEN WIN) |
| E-MED-4 (retireWindow) | not catalogued | project_q5_q8_q13_structural_gap |

---

## Recommendation ordering (by streaming-correctness leverage)

1. **E-CRIT-1** — wire V2 snapshot to existing strategy. Highest correctness leverage; without this, V2 jobs are not fault-tolerant.
2. **E-CRIT-3** — plumb real keygroup into V1-sync. Without this, V1 jobs cannot rescale.
3. **E-HIGH-1** — minimal savepoint emit. Without this, version migration is impossible.
4. **E-HIGH-6** — change timer-factory default to HEAP. One-line patch, measured 1.20× win on Q12.
5. **E-HIGH-4** — true async barrier drain (coupled to CRIT-1).
6. **E-HIGH-2** — batched rescale restore.
7. **E-CRIT-2** — rename `drainTo` and add `orderedDrain`. Footgun mitigation.
8. **E-HIGH-3** — pipelined V2 dispatch.
9. **E-HIGH-5** — batched ITER FFM primitive.

Numbers 1-3 are gating for any production deployment. Numbers 4-9 are pure throughput.
