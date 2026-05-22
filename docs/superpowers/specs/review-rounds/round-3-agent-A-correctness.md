# Round 3 Agent A — Correctness / Data Consistency Review

Reviewer angle: criterion #1 — verify Round 2 fixes at flink commit
`63ea1c2904e` and look for **NEW** correctness issues not on the
already-deferred list. Code paths re-examined:
`VectorizedExecutor.executeRequestSync`, `executeBatchRequests`,
`completePutExceptionally`, `dispatchAppendMerge*` and the brand-new
V2-state classes `ForStRsAsyncReducingStateV2`,
`ForStRsAsyncAggregatingStateV2`, `ForStRsMapStateV2`.

Already deferred (per task prompt) and not re-reported here unless a new
sub-defect was found: A1-H1..H6, A2-H1..H3, B2-H1, E-CRIT-1/2/3,
E2-CRIT-1/2.

## Summary
- HIGH findings: 3
- MEDIUM findings: 2
- LOW findings: 1

---

## HIGH severity

### A3-H1 — `executeRequestSync` has NO try/catch around `dispatchAppendMerge`; an unchecked throw leaves the sync StateRequest future dangling
- **File:line:** `flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java:232-278`
- **Code snippet:**
  ```java
  @Override
  public void executeRequestSync(StateRequest<?, ?, ?, ?> request) {
      VectorizedClassifier single = new VectorizedClassifier(...);
      single.reset();
      for (String name : listStateNames) single.registerListState(name);
      single.initNewKindBuffers(arena);
      single.offer(request);
      executePuts(single);                            // (a) may throw FrsEnginePanicError / FrsBackendException
      executeDeletes(single);                         // (a)
      executeGets(single);                            // (a) throws on line 386/388 of executeGets
      executeIters(single);                           // (a)
      AppendMergeBatchBuffer amBuf = single.appendMergeBuffer();
      if (amBuf != null && !amBuf.isEmpty()) {
          dispatchAppendMerge(amBuf);                 // (b) FFI throw skips the propagation loop below
          /* per-row future completion loop — only runs on happy path */
          StateRequest<?, ?, ?, ?>[] amReqs = single.appendMergeRequests();
          int amCount = single.appendMergeCount();
          List<CompletableFuture<Void>> amReqFutures = amBuf.futures();
          for (int i = 0; i < amCount; i++) { ... completePut / completePutExceptionally ... }
      }
      IterPrefixBatchBuffer ipBuf = single.iterPrefixBuffer();
      if (ipBuf != null && !ipBuf.isEmpty()) dispatchIterPrefix(ipBuf);
  }                                                   // <-- no try/catch, void return
  ```
  Compare with `executeBatchRequests` (lines 176-229) which wraps the
  same logic in `try { ... } catch (Exception e) { return failedFuture(e); }`.
- **Failure mode:** `executeRequestSync` is called by
  `AsyncExecutionController.insertActiveBuffer` (line 347 of
  AsyncExecutionController.java) when a sync request is offered. The
  caller's contract is "this method completes the request's framework
  future before returning". Three unchecked-throw paths in the body
  break this:
  1. `executeGets` at line 386 throws `FrsEnginePanicError`, line 388
     throws `FrsBackendException` — both leave the GET request's
     `InternalAsyncFuture` uncompleted (its completion happens after
     the throw, in `completeGet` at line 407). The framework's
     `StateRequest.getFuture()` stays pending forever.
  2. `dispatchAppendMerge` line 249 — if `linker.frsVecMergeAppend` or
     `linker.frsVecMergeAppendBatch` throws an unchecked at the FFI
     layer (e.g. `IllegalStateException` from a closed Arena scope, or
     a downcall handle mismatch under JDK panama post-finalization),
     the throw bypasses the Round-2 propagation loop. All
     `single.appendMergeRequests()[i].getFuture()` stay pending.
  3. `dispatchIterPrefix` — same anti-pattern, the loop at line 701-749
     calls `slotScope.registerIter(fh)` (line 744) which may throw if
     the slot has been closed concurrently (cancellation path).
  
  Round 2's `executeBatchRequests` fix exposes the asymmetry — it
  surfaces failures to the container future, but the sibling
  `executeRequestSync` has no return type to surface them through, AND
  no fail-all-pending-futures-then-rethrow shim. Net effect: on FFI /
  engine throw under the sync path, the Flink-runtime async
  controller hangs waiting on the request future, **identical to the
  A1-H5 / A2-H1 stall pattern Round 2 was supposed to close**.
- **Repro scenario:** Engine returns a panic-class `FrsErrorCode` on a
  GET dispatched via the sync path (e.g., during a manual
  `sync()` call from `StateRequestHandler`). `executeGets` throws
  `FrsEnginePanicError`. Caller `AsyncExecutionController.insertActiveBuffer`
  sees the throw and propagates up to `StreamTask.processInput`. The
  StateRequest's `getFuture()` is never touched. If the framework's
  upstream chain has any `thenAccept` waiting on this future (e.g.
  V2 callback), that callback is orphaned. Resource (RecordContext
  ref) leak. Operator stalls on the next checkpoint barrier wait.
- **Fix sketch:** Wrap the body of `executeRequestSync` in
  `try { ... } catch (Throwable t) { /* fail all per-kind requests' futures, then rethrow */ }`.
  Specifically: walk `single.getRequests()[0..getCount-1]`,
  `single.putRequests()[0..putCount-1]`,
  `single.deleteRequests()[0..deleteCount-1]`,
  `single.appendMergeRequests()[0..amCount-1]` and call
  `completePutExceptionally(req, t)` on any whose future is not yet
  complete; THEN rethrow. This matches the
  fail-then-rethrow-once-at-batch-edge that the framework expects
  for the sync path.

---

### A3-H2 — `executeBatchRequests` outer `try/catch (Exception e)` skips `Error`; engine `FrsEnginePanicError` (extends Error) bypasses container-future failure path
- **File:line:** `flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java:176-229`
- **Code snippet:**
  ```java
  try {
      ... executeGets(classifier) ...     // line 379-386: throws FrsEnginePanicError on isFailProcess
      ... dispatchAppendMerge ...
      ...
      return CompletableFuture.completedFuture(null);
  } catch (Exception e) {                  // <-- catches Exception, NOT Throwable
      return CompletableFuture.failedFuture(e);
  }
  ```
  And in `executeGets` line 379-387:
  ```java
  if (errCode.isFailProcess()) {
      FrsEnginePanicError panicErr = new FrsEnginePanicError(errCode, ...);
      if (fatalHandler != null) fatalHandler.onFatalError(panicErr);
      throw panicErr;                      // <-- Error, not Exception
  }
  ```
  `FrsEnginePanicError` extends `Error` (confirmed by import at line 33
  and existing review reference E-CRIT / A2-H2 mention).
- **Failure mode:** When `executeGets` raises a panic, the outer
  `catch (Exception e)` does not catch it — the Error propagates up
  out of `executeBatchRequests` to the caller (the AEC dispatch
  thread). The container future is **never returned at all** (control
  flow exits via the throw), so the runtime's
  `CompletableFuture<Void>` from
  `executeBatchRequests` is in an indeterminate state — the caller's
  `thenAccept`/`thenCompose` callback on the container future never
  fires. Per-row request futures inside the batch also stay
  uncompleted (they were going to be completed AFTER the throw, in
  `completeGet` at line 407). Result: every record in the batch is
  orphaned, the AEC thread sees an uncaught Error, the task is
  marked failed via the JVM uncaught handler, but the in-flight
  framework futures still leak refs. This is the same shape as A1-H4
  (deferred) but for the panic path specifically, and is amplified by
  the fact that the fatalHandler is ALREADY invoked at line 384 —
  Flink's `FatalErrorHandler.onFatalError` typically schedules an
  async task shutdown, but the `throw panicErr` here happens
  synchronously BEFORE that shutdown completes, so the next
  scheduled batch may still race in.
- **Repro scenario:** Mid-batch engine panic (`EnginePanicCaught`)
  returned from `vectorizedBatchGet`. `executeGets` constructs
  panic, calls fatalHandler.onFatalError(), then throws. Outer catch
  skips. Container future is lost; per-row futures hang.
- **Fix sketch:** Change `catch (Exception e)` to `catch (Throwable t)`,
  AND before returning `failedFuture(t)`, run the same fail-all-pending
  futures sweep proposed in A3-H1 across `classifier.getRequests()`,
  `putRequests()`, `deleteRequests()`, `appendMergeRequests()`.
  Distinguish `Error` vs `Exception` if needed to skip re-invoking
  fatalHandler (which `executeGets` already did).

---

### A3-H3 — `ForStRsMapStateV2.asyncClear` is FINAL on parent class and bypasses the LRU cache → asyncGet immediately after asyncClear returns stale cached value
- **File:line:** `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java:76, 100-107, 132-147`
- **Code snippet:**
  ```java
  // ForStRsMapStateV2.java line 76
  private final MapStateCache<UV> cache = new MapStateCache<>();
  // line 100-106 (existing comment acknowledging the gap)
  // asyncClear() is final in AbstractKeyedState and cannot be overridden — instead we hook the
  // namespace switch on setCurrentNamespace below if needed; for V1 we rely on the fact that
  // Flink's MapState.clear() is rare enough (end-of-window only in Q11/Q12) that cache staleness
  // for the cleared operator key is corrected on the next miss-resolve, which fetches from the
  // engine (where the clear already took effect). The corner case is asyncGet immediately after
  // asyncClear returning a stale cached value — addressed in V1.2 with a clear-hook.

  // AbstractKeyedState.java line 77
  public final StateFuture<Void> asyncClear() {
      return handleRequest(StateRequestType.CLEAR, null);  // engine clears, cache NOT touched
  }

  // ForStRsMapStateV2.asyncGet — cache hit returns cached value with no clear-awareness
  public StateFuture<UV> asyncGet(UK userKey) {
      byte[] keyBytes = serializeMapEntryKey(userKey);
      MapStateCache.Lookup<UV> hit = cache.lookup(keyBytes);
      if (hit != null && hit.cached()) {
          return StateFutureUtils.completedFuture(hit.value());  // STALE if asyncClear ran between put and now
      }
      ...
  }
  ```
- **Failure mode:** User code: `mapState.put(k, v); mapState.clear(); mapState.get(k);`. Trace:
  1. `asyncPut(k, v)` writes to cache (line 152) AND dispatches PUT to engine.
  2. `asyncClear()` (final, not overridden) dispatches CLEAR — the
     engine deletes the entry; **the per-state cache still holds
     `k → v`**.
  3. `asyncGet(k)` hits the cache (line 137-138) and returns `v`
     instead of `null`. **Wrong result, silent.**
  
  The in-code comment at line 100-106 acknowledges the gap and defers
  to "V1.2 with a clear-hook", which is NOT in the deferred-debt list
  provided. This is therefore a NEW finding and a real
  correctness bug for any operator that interleaves `clear()`+`get()`.
  Q11/Q12 are session/tumbling windows; `clear()` runs at window end
  but subsequent reads in the same operator on the same key (e.g.,
  on a side-output path or a window-trigger-then-process) would see
  the stale cache. Also problematic for any user-defined operator
  using MapState idiomatically.
  
  Sub-issue: `asyncClear` also clears ALL entries for the current
  operator key + namespace + state in the engine, but the cache is
  keyed by `serializeMapEntryKey(userKey)` for ONE userKey at a time.
  The cache has no per-(operatorKey,namespace) index, so even a
  selective invalidation can't be implemented without scanning the
  entire LRU. A correct fix needs either (a) a generation counter
  per-(operatorKey,namespace) that asyncClear bumps, with each cache
  entry tagged by the generation at write time; or (b) clearing the
  whole cache on `asyncClear` (correct but throws away the working
  set).
- **Repro scenario:** Unit test:
  ```java
  mapState.asyncPut("k", "v1").get();
  mapState.asyncClear().get();
  String got = mapState.asyncGet("k").get();  // observed: "v1", expected: null
  ```
- **Fix sketch:** Override `handleRequest(StateRequestType, payload)`
  on ForStRsMapStateV2 (it's not final on the abstract) to intercept
  `CLEAR` and call `cache.clear()` before delegating; OR add a
  hook at the StateRequestHandler dispatch boundary; OR (best) a
  generation-counter approach so cache.put records its generation at
  put time, asyncClear bumps the generation, asyncGet treats any
  cache hit with mismatched generation as a miss. The "V1.2
  clear-hook" referenced in the comment should be promoted from
  deferred-internal to a tracked correctness fix.

---

## MEDIUM severity

### A3-M1 — Per-instance `DataOutputSerializer keyOut` / `valueOut` shared between all V2 state methods (`serializeKey`, `serializeKeyInto`, `getIterPrefix`, `serializeMapEntryKey`); single-threaded assumption silently broken if any callback runs on a different thread
- **File:line:** `ForStRsMapStateV2.java:67-69, 114, 174, 253, 304` (and analogous fields in `ForStRsAsyncReducingStateV2.java:66-67`, `ForStRsAsyncAggregatingStateV2.java:69-70`, `ForStRsAsyncListStateV2.java:73-74`)
- **Failure mode:** All four V2 state classes hold `DataOutputSerializer keyOut` / `valueOut` as **instance fields**, and the same instance is used by (a) the override `asyncGet/Put/Remove` path that runs on the operator thread, (b) the `serializeKeyInto(StateRequest, ColumnarBatchBuffer)` path invoked by `VectorizedClassifier.offer()` on the AEC executor thread, and (c) iterator `getIterPrefix` on the iterator-loading thread. Each entry-point calls `keyOut.clear()` and then writes — if two entry-points overlap (which is supposed to never happen by the single-writer contract), the second's `clear()` rewinds the first's buffer mid-serialization, producing a CORRUPT key or value. Currently the framework's single-threaded scheduling protects this, but the assumption is implicit and not asserted anywhere; any future change that moves part of the work to a `CompletableFuture.thenApply` running on a different executor will silently corrupt serialized bytes.
- **Fix sketch:** Either (a) add a `Thread.holdsLock` /
  `ownerThreadId == Thread.currentThread().getId()` assertion at the
  top of each `serialize*` method, or (b) use ThreadLocal serializers,
  or (c) make the state object lazily-create a fresh
  `DataOutputSerializer` per call (cost: 64-byte alloc per request —
  measure first).

### A3-M2 — `completePutExceptionally` Round-2 fix uses `cause.getMessage()`; for `FrsException` with `code + rowIndex=-1 + empty detail bytes`, the message can be the bare class name without context
- **File:line:** `VectorizedExecutor.java:874-878`
- **Failure mode:** The Round-2 fix takes `String msg = cause.getMessage()` and falls back to `"ForSt-RS dispatch failed: " + class.getSimpleName()` only when `msg == null || msg.isEmpty()`. But `FrsException`'s `getMessage()` may return a non-empty but uninformative string when `rowIndex == -1` and `detail` is a zero-length byte array (constructed at line 646 of `dispatchAppendMergeBatch`: `new FrsException(code, -1, new byte[0])`). The message goes into `InternalAsyncFuture.completeExceptionally(msg, cause)` as the user-facing string for the operator-failure log — operators see "FrsException: code=ENGINE_IO row=-1" with no per-row attribution because the batched path uses `-1` as a sentinel. The fix preserved class but lost per-row index (still `-1`), so the diagnostic delta is small: A2-H2 was supposedly resolved, but actually it just changed WHERE the row-index-is-missing complaint surfaces.
- **Fix sketch:** When propagating a shared-batched-exception across the per-row loop, RECONSTRUCT the FrsException with the correct row index `i` (using `new FrsException(originalCode, i, originalDetail)`) before calling `completePutExceptionally`. This gives each per-row diagnostic the right row index even though the engine returned a single batch-level error. Document the sentinel `-1` semantics.

---

## LOW severity

### A3-L1 — `listStateNames` iteration in `executeRequestSync` uses a `ConcurrentHashMap.keySet()` weakly-consistent iterator; state added concurrently after iteration begins is silently skipped
- **File:line:** `VectorizedExecutor.java:87-88, 238-240`
- **Failure mode:** `listStateNames` is `ConcurrentHashMap.newKeySet()` so its iterator is weakly consistent. If `ForStRsAsyncKeyedStateBackend.createOrUpdateInternalState` (line 235-237) calls `exec.registerListState(name)` concurrently with `executeRequestSync` iterating the names, the new name may or may not appear in the iteration. If the request being executed sync references a state whose name was just added but missed by the iterator, the `single.offer(request)` → `recordAppendMerge` path will check `listStateNames.contains(name)` on the CLASSIFIER'S local set (which only got populated from the executor's set), find it absent, and route the LIST_ADD through `recordPut` (destructive overwrite) instead of `recordAppendMerge`. **Concretely, the first LIST_ADD on a freshly-created ListState executed via the sync path could be silently routed as PUT, replacing any prior accumulated list with the single-element payload.** Probability is low (sync path is rare, state creation usually happens at backend setup BEFORE any request) but not zero.
- **Fix sketch:** Snapshot `listStateNames` once at executor construction OR add a write-then-read happens-before fence (e.g., publish `listStateNames` via a `volatile` epoch counter that `executeRequestSync` reads before iterating). For minimal cost: document the invariant "all ListState primitives must be created before the first executeRequestSync" and assert it at state-creation time.

---

## Verification of Round 2 fixes

| Round 2 finding | Round 3 verdict |
|-----------------|-----------------|
| A2-H1 (executeRequestSync APPEND_MERGE propagation) | **FIX APPLIED CORRECTLY for happy path.** New gap A3-H1 (throws bypass the propagation loop). Double-init safety of `initNewKindBuffers` is sound — the method is null-guarded (VectorizedClassifier.java line 132-142) and idempotent. |
| A2-H2 (completePutExceptionally message wrapping) | **FIX APPLIED.** Preserves `cause.getMessage()`. New sub-issue A3-M2: `FrsException(code, -1, empty)` from batched path still loses per-row attribution because the dispatcher constructs a single shared instance with rowIndex=-1. |
| A2-H3 (container future fails on any-row failure) | **FIX APPLIED CORRECTLY.** `firstRowFailure` local-variable is single-thread-safe by design (executor's batch lifecycle). New gap A3-H2 — container future is never returned at all when an `Error` (not Exception) escapes the inner block, since the catch only covers `Exception`. The `firstRowFailure` tracking distinguishes per-row failure (which IS captured) from outer-try throws (which are captured by the catch block, BUT only for `Exception`). For an Error escape, neither path completes per-row futures nor the container future. |

## New-finding pivot: V2 RMW states (Reducing/Aggregating) and MapState V2

- `ForStRsAsyncReducingStateV2` and `ForStRsAsyncAggregatingStateV2`
  inherit RMW semantics from `AbstractReducingState`/`AbstractAggregatingState`
  — get, fold, put. The V2 classes only contribute key/value
  serialization. **No RMW cache** is present on these classes (compare to
  MapStateV2's LRU). This means every reducing-state `asyncAdd` is at
  least one engine GET + one PUT; no caching layer is at risk of
  staleness here. **No new defect on the RMW state classes**, but
  E2-CRIT-1 (deferred: missing namespace in key) DOES apply to them
  and is acknowledged.
- `ForStRsMapStateV2` carries a per-state LRU cache. New defect A3-H3
  reported above. The cache key includes operator key + stateName +
  userKey but NOT namespace — consistent with the engine-side
  `serializeKey` (also omits namespace), so cache vs engine stay
  consistent **on the GET path**. But asyncClear bypasses both the
  cache and the override hierarchy, breaking write-through semantics.

## Resource-leak audit (Round 2 changes)

- **No new Arena leaks.** The Round-2 changes did not add Arena
  allocations. `executeRequestSync` uses the long-lived executor Arena
  for `initNewKindBuffers` and shares the executor's `getKeys/putKeys/...`
  with the batched path. The per-call scratch Arenas (`Arena.ofConfined`
  at line 499 and 584 inside `dispatchAppendMerge*`) are closed in
  `finally`. The Round-2 fix did not change these.
- **MemorySegment lifetimes:** Round-2's per-row future completion
  loop reads from `amBuf.futures()`, no new segments. The
  AppendMergeBatchBuffer holds `MemorySegment`s registered with the
  classifier-passed Arena, freed when the classifier's Arena (the
  executor's long-lived arena) is closed at shutdown. No new leak.
- **No new MemorySegment holdings on the failure path.** When
  `completePutExceptionally` propagates the cause, it does not hold a
  reference to any MemorySegment beyond the framework's normal
  `InternalAsyncFuture` machinery.

## Round-3 verdict

**Correctness posture: 3 HIGH remain.** Round 2 closed the documented
A2-H1/H2/H3 surface area cleanly. However:

1. **A3-H1** (executeRequestSync lacks outer try/catch) and **A3-H2**
   (executeBatchRequests catches Exception not Throwable) are
   structurally equivalent gaps that pre-date Round 2 but become
   acutely visible now that the per-row propagation loop is the
   ONLY mechanism keeping per-row futures alive. Both should be
   addressed together with a single helper:
   `failAllPendingRequestFutures(classifier, cause)` invoked from a
   `catch (Throwable)` wrapping the entire body of BOTH executor
   entry-points.
2. **A3-H3** is a correctness defect on the V2 MapState that the
   in-code comment acknowledges but defers. With Q11/Q12 hitting
   MapState heavily, any code path that performs
   `put → clear → get` on the same operator key returns stale data.
   Recommend promoting from deferred (V1.2 clear-hook) to a Round 4
   blocker since it can produce wrong query results, not just hangs.

Recommend Round 4 patches address A3-H1/H2/H3 together with a unit
test for the failure-leg of `executeRequestSync` (throwing FFM linker
mock) and a put-clear-get integration test for MapStateV2.
