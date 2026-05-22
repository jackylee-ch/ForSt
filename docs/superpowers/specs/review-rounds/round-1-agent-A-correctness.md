# Round 1 Agent A — Correctness / Data Consistency Review

Reviewer angle: criterion #1 — data consistency, snapshot/restore drift, race
hazards, FFI error swallowing, future-completion correctness, ordering invariants.
This is a *cold-read* review with no prior session context beyond the V1–V20
catalogue noted in the audit-design spec.

## Summary
- HIGH findings: 6
- MEDIUM findings: 5
- LOW findings: 4

---

## HIGH severity

### A1-H1 — V2 async snapshot is a NO-OP: state cannot be restored after task restart
- **File:line:** `flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAsyncKeyedStateBackend.java:387`
- **Code snippet:**
  ```java
  // After three flushDirty() calls and Reducing/Aggregating barrier drains:
  return DoneFuture.of(SnapshotResult.empty());
  ```
- **Failure mode:** `ForStRsAsyncKeyedStateBackend.snapshot()` returns an
  *empty* `SnapshotResult` and never invokes `linker.createCheckpoint(...)` /
  `frs_create_checkpoint`. The engine memtables are not flushed (`db.flush_all`
  is not called either). On TM crash + restart, Flink's checkpoint coordinator
  treats the prior checkpoint as successful but has **no `KeyedStateHandle`** —
  the new task comes up with an empty engine. The in-code comment acknowledges
  this is "V1 best-effort" and depends on engine-side S3 persistence, but for
  any cluster *not* using a shared S3 / remote engine, ALL state since the last
  external durability event is lost. Even with remote engine, lossless restore
  requires a snapshot handle that records the sequence number / files of
  record — `SnapshotResult.empty()` cannot do this.
- **Repro scenario:** Flink job runs, makes 100 record progress, checkpoint N
  triggers, `snapshot()` returns empty. TM is killed (`SIGKILL` to force no
  graceful close). On restart Flink restores from checkpoint N → empty state
  → at-least-once duplicate counts (Q11/Q12 windowed aggregates would be
  drastically incorrect; Q5 windowed-state queries lose entire windows).
- **Fix sketch:** Implement a real snapshot path. Minimum viable: (a) call
  `linker.flush(db)` to seal the memtable, (b) `linker.createCheckpoint(db,
  targetDir)` to materialize SSTs at `targetDir`, (c) upload `targetDir` via
  `CheckpointStreamFactory`, (d) return a `SnapshotResult` carrying a
  `KeyedStateHandle` that points at the uploaded directory. The V1 sync
  `ForStRsKeyedStateBackend.snapshot(Path)` (line 547) already does (a)+(b) — port
  that into the async backend's checkpoint pipeline.

---

### A1-H2 — `VectorizedExecutor.flushDirty()` is a no-op while barrier drain depends on it
- **File:line:** `flink-statebackend-forst-rs/.../VectorizedExecutor.java:238`
- **Code snippet:**
  ```java
  public void flushDirty() {}
  ```
- **Failure mode:** `ForStRsAsyncKeyedStateBackend.snapshot()` calls
  `managedExecutors.forEach(VectorizedExecutor::flushDirty)` three times to
  drain in-flight batches (PHASE-1, PHASE-2, SP6 sweep — lines 349/364/370).
  Each call is a no-op, so any classifier holding un-dispatched requests when
  the barrier arrives never sees a forced dispatch. In addition, when used,
  classifier-internal `appendMergeBuffer` / `iterPrefixBuffer` / `iterRangeBuffer`
  buffers can hold rows that were `append()`ed but not yet executed —
  `flushDirty()` should force them through. Combined with A1-H1 this means even
  the "best-effort" barrier ordering is not preserved.
- **Repro scenario:** A batch of LIST_ADDs is offered to the classifier mid-record;
  the StateRequestHandler's framework decides to take a checkpoint barrier
  before `executeBatchRequests` is called → drain methods do nothing → those
  LIST_ADDs are silently dropped from the snapshot's view of state.
- **Fix sketch:** `flushDirty()` should dispatch any classifier sitting in
  `createRequestContainer()`'s pool (track it via a field) and additionally
  invoke `linker.flush(db)` to seal memtables before the snapshot reads the
  state. At minimum, propagate a "flush pending" flag to the next classifier
  reset so the next batch starts with an empty buffer view.

---

### A1-H3 — Timer queue `flushPendingToEngine()` is never wired into the async backend's snapshot
- **File:line:** `flink-statebackend-forst-rs/.../keyed/ForStRsAsyncKeyedStateBackend.java:343-388` (no call to `flushPendingToEngine`); `timer/ForStRsKeyGroupedInternalPriorityQueue.java:734`
- **Code snippet (from queue's javadoc, lines 76-81):**
  ```text
  flushPendingToEngine() is the mandatory pre-snapshot hook — invoked from the
  backend's snapshot() before any engine state is captured.
  ```
- **Failure mode:** The engine-backed timer queue maintains an off-heap
  `ArrowTimerBuffer` of pending ADD/REMOVE ops that have NOT yet been pushed
  to the engine (cancellation optimization). Its own spec invariant #4 requires
  the backend's `snapshot()` to call `flushPendingToEngine()` before reading any
  state. `ForStRsAsyncKeyedStateBackend.snapshot()` (and `dispose()`, and any
  other read path) does not register the priority queue instances, never calls
  `flushPendingToEngine()`. The `create(name, serializer, ...)` factory method
  (line 419-438) instantiates a `ForStRsKeyGroupedInternalPriorityQueue` and
  returns it to Flink without retaining a reference. Result: every pending
  timer ADD/REMOVE buffered at barrier time is invisible to the snapshot, even
  if A1-H1 is fixed.
- **Repro scenario:** Q12 with `forst.rs.timer-service.factory=FORSTRS`.
  Operator schedules 1000 event-time timers in the few millis before barrier.
  Barrier arrives → snapshot taken → 1000 timers in the ArrowTimerBuffer never
  flushed → restart from snapshot drops those timers → output windows fire
  late or never.
- **Fix sketch:** Maintain a `List<ForStRsKeyGroupedInternalPriorityQueue<?>>
  registeredQueues` (parallel to `registeredReducingStates`). In `create()`
  add the new queue to the list. In `snapshot()` iterate and call
  `flushPendingToEngine()` BEFORE the engine snapshot. Also in `dispose()` for
  any final shutdown drain.

---

### A1-H4 — `executeBatchRequests` throws on partial-batch failure, leaving PUT/DELETE/GET futures uncompleted
- **File:line:** `flink-statebackend-forst-rs/.../VectorizedExecutor.java:172-208`
- **Code snippet:**
  ```java
  try {
      executePuts(classifier);     // line 180; throws FrsBackendException on rc != OK
      executeDeletes(classifier);
      executeGets(classifier);
      executeIters(classifier);
      ...
      return CompletableFuture.completedFuture(null);
  } catch (Exception e) {
      return CompletableFuture.failedFuture(e);   // line 206
  }
  ```
- **Failure mode:** `linker.vectorizedBatchPut` calls `check(rc, ...)` which
  throws `FrsBackendException` on any non-OK rc. When this throws inside
  `executePuts`, control transfers to the outer catch, which fails the
  CONTAINER-level CompletableFuture. **But each StateRequest's per-row
  `getFuture()` is never completed** (completePut at line 269-272 is reached
  only on success). The Flink async-state runtime that submitted those
  requests is left waiting forever for those per-row futures — eventually the
  RecordContext's reference count never drops to 0 and progress halts.
  Equally bad: if PUT throws BUT some GET futures had already been completed
  successfully on a prior batch path, callers see *partial* visibility of
  results in an indeterminate state. **Replicated for DELETE (no per-row
  exception completion either).**
- **Repro scenario:** Engine returns `EngineIo` (e.g., S3 transient error)
  during a 1000-row PUT batch. `executePuts` throws → all 1000 StateRequest
  framework futures leak. Operator becomes stuck; Flink eventually times out
  the checkpoint, then the job fails — but the failure is reported as
  "checkpoint timeout" instead of the real engine error, complicating
  diagnostics.
- **Fix sketch:** Wrap each per-op-type executor in its own try/catch.
  On exception, drive every still-uncompleted request in that op type to
  `completeExceptionally(e)` BEFORE re-throwing or returning a failed
  container future. Mirror the `dispatchAppendMerge` per-row exception
  handling (lines 467-484) for PUT/DELETE/GET.

---

### A1-H5 — `dispatchAppendMerge` failure path silently completes StateRequest futures as success
- **File:line:** `flink-statebackend-forst-rs/.../VectorizedExecutor.java:184-197`
- **Code snippet:**
  ```java
  AppendMergeBatchBuffer amBuf = classifier.appendMergeBuffer();
  if (amBuf != null && !amBuf.isEmpty()) {
      dispatchAppendMerge(amBuf);           // may have failed per-row internally
      StateRequest<?, ?, ?, ?>[] amReqs = classifier.appendMergeRequests();
      int amCount = classifier.appendMergeCount();
      for (int i = 0; i < amCount; i++) {
          completePut(amReqs[i]);           // unconditional success!
      }
  }
  ```
- **Failure mode:** Inside `dispatchAppendMerge` / `dispatchAppendMergeBatch`,
  on engine failure each row's *internal* `AppendMergeRequest.future` is
  completed exceptionally (lines 477/483/604-606). But the parallel
  `appendMergeRequests[i]` array of `StateRequest`s is then iterated and
  completed with **`completePut(amReqs[i])` — which calls `.complete(null)`
  unconditionally** (line 808). The Flink async-state framework therefore sees
  the LIST_ADD as successful even when the engine returned `EngineIo`,
  `EngineCorrupted`, or `PanicCaught`. This is a silent data-loss corruption
  bug for ListState. (Q19 Top-N, Q11/Q12 timer payloads, Q5 windowed buffers
  all use ListState V2.)
- **Repro scenario:** ListState appendMerge batch hits a transient `EngineIo`
  (S3 throttle, disk full); the batched FFI returns non-OK; the per-row
  internal futures fail; `completePut(amReqs[i])` completes Flink's framework
  futures as success. Downstream operator processes a stale `LIST_GET`
  expecting the just-appended elements, sees them missing, emits incorrect
  output, checkpoint succeeds, restart cannot recover the lost rows.
- **Fix sketch:** In `dispatchAppendMerge[Batch]`, before returning, inspect
  the rc / per-row futures. If any row failed, propagate by completing the
  corresponding `appendMergeRequests[i]` **exceptionally** instead of via
  `completePut`. Or: route both completions through a single
  "complete-from-result(rc, exception)" helper that takes a Throwable and
  picks `.complete(null)` vs `.completeExceptionally(t)`. Add a unit test
  that wires a mock linker returning `FrsErrorCode.EngineIo` and asserts
  the StateRequest future is failed.

---

### A1-H6 — `ForStRsValueStateV2.serializeKey` caches composite key in shared `RecordContext.extra` — cross-state collision
- **File:line:** `flink-statebackend-forst-rs/.../state/ForStRsValueStateV2.java:73-105`
- **Code snippet:**
  ```java
  byte[] cached = (byte[]) ctx.getExtra();
  if (cached != null) {
      return cached;                       // <-- regardless of stateName!
  }
  ...
  ctx.setExtra(composite);                 // stores keyBytes+SLASH+stateNameBytes+SLASH
  ```
- **Failure mode:** `RecordContext.extra` is a single slot per RecordContext.
  When an operator has TWO ValueState instances (state name "A" and state
  name "B") for the same key, the first `serializeKey` (state A) stores
  `["k/" + key + "/" + "A" + "/"]` into ctx.extra. The second `serializeKey`
  (state B) reads `ctx.getExtra()` and gets state A's composite — silently
  returns the WRONG ForSt key. The PUT for state B then writes to state A's
  row, and the GET for state B reads state A's value. This is silent state
  corruption that can cross-pollute unrelated logical states under the same
  user key. Note: only impacts the non-vectorized fallback path
  (`buildDBGetRequest`/`buildDBPutRequest`); the vectorized
  `serializeKeyInto` (line 154-167) does not consult the cache and is
  correct.
- **Repro scenario:** Any KeyedProcessFunction with two ValueState<X> fields.
  First call to state A on key K caches composite K_A. Next call to state B
  on same key K reads cached K_A → reads/writes state A's slot.
- **Fix sketch:** The cache key must include stateNameBytes. Easiest: drop the
  cache (line 75-78 + 100). It optimizes one byte[] alloc on a path that the
  vectorized executor already avoids. If retained, key the cache by an
  identity-comparable per-state token, e.g. an instance-final
  `IdentityHashMap<ForStRsValueStateV2, byte[]>` on the RecordContext (still
  requires runtime support for arbitrary keyed-context attachment, which
  Flink does not currently expose; safest fix: remove the cache).

---

## MEDIUM severity

### A1-M1 — `managedExecutors` HashSet mutated from multiple paths without synchronization
- **File:line:** `flink-statebackend-forst-rs/.../keyed/ForStRsAsyncKeyedStateBackend.java:114, 235, 282`
- **Failure mode:** `createStateInternal` (state-create path) iterates
  `managedExecutors` to call `registerListState(name)` while
  `createStateExecutor` writes to the same `HashSet<VectorizedExecutor>`.
  In Flink's async-state runtime, state creation can happen on different
  threads (task-init thread vs operator thread). A
  `ConcurrentModificationException` or torn-set state is possible. Also
  `snapshot()` reads the set concurrently with `dispose()` (which clears it).
- **Fix sketch:** Use `CopyOnWriteArraySet` or `ConcurrentHashMap.newKeySet()`,
  or document and enforce that all mutations occur on a single thread.

### A1-M2 — Per-row scratch Arena in `dispatchAppendMergePerRow` not auto-bounded
- **File:line:** `flink-statebackend-forst-rs/.../VectorizedExecutor.java:449-487`
- **Failure mode:** A new `Arena.ofConfined()` is opened per row inside the
  per-row legacy path. For a batch of 10k multi-operand rows, this is 10k
  syscalls + close pairs — not a correctness bug, but a stability concern:
  if `Arena.ofConfined()` throws (OOM at native level), the future for that
  row is never completed (no catch around the alloc + the finally only
  closes the scratch). Same hazard as A1-H4 but localized to the legacy
  path.
- **Fix sketch:** Wrap the body in try/catch; on Arena.ofConfined()
  failure, `future.completeExceptionally(e)` then continue.

### A1-M3 — `frs_vec_merge_append_batch` is not atomic across keys in a batch
- **File:line:** `crates/forst-rs-ffi/src/lib.rs:3997-4057`
- **Failure mode:** The function performs N independent
  `get → combine → put` round-trips, one per distinct key. If the FFI
  call panics or returns mid-loop (e.g. on the 3rd key's put), the first
  two keys have been written but rows 3..N have not. The error code
  returned to Java is the LAST error encountered; the caller has no way to
  know which rows succeeded. In `dispatchAppendMergeBatch` this is then
  treated as "all-or-nothing" by completing every future exceptionally,
  but the engine state now has partial writes.
- **Fix sketch:** Use a `WriteBatch` to accumulate all puts after computing
  the merged values, then commit atomically via `db.batch_write(wb)`. This
  also reduces per-key WAL fsync cost. (Reads are already pre-batch so an
  intervening writer between the gets and the WriteBatch commit could
  miss our merge — acceptable under Flink's single-task-per-backend
  contract, but should be documented.)

### A1-M4 — ListState V2 multi-chunk decode trusts `count` without bounds checking
- **File:line:** `flink-statebackend-forst-rs/.../state/ForStRsAsyncListStateV2.java:217-228`
- **Failure mode:** The decode loop reads `count = valueIn.readInt()` and
  loops `for (int i = 0; i < count; i++)`. A negative `count` is rejected,
  but a *positive* count larger than the remaining bytes will trigger a
  serializer-level EOFException — caught by the outer try/catch and
  re-thrown as RuntimeException. If a write went bad (engine corruption or
  V20 format-mismatch edge case), the operator dies with a RuntimeException
  that's hard to diagnose. Risk if a stale single-chunk PUT (V20 format A)
  somehow interleaves with merge-operator multi-chunk concatenation.
- **Fix sketch:** Add a sanity bound (e.g. `count > availableBytes / minElemSize`
  fail-fast with a clearer error including the raw byte sample) and surface
  a `FormatMismatchException` rather than RuntimeException so callers can
  decide whether to retry or fail-fast.

### A1-M5 — APPEND_MERGE state-name registry timing: late registration loses prior batches
- **File:line:** `flink-statebackend-forst-rs/.../VectorizedExecutor.java:155-170; .../keyed/ForStRsAsyncKeyedStateBackend.java:233-237`
- **Failure mode:** `registerListState(name)` is called when the
  `ForStRsAsyncListStateV2` is created (backend `createStateInternal`,
  line 233-237). For each existing managed executor it adds the name. New
  executors created AFTER state creation pick up the name from the
  executor-level `listStateNames` set in `createRequestContainer` (line
  155). But: if a list-state is created AFTER the first batch has already
  classified LIST_ADD requests, those requests will have been routed to PUT
  (destructive) rather than APPEND_MERGE — silent destructive overwrite.
  In practice Flink creates all state primitives before processing records
  starts, so this is mostly defensive — but no assertion enforces that
  invariant.
- **Fix sketch:** In `VectorizedClassifier.offer()`'s LIST_ADD branch (line
  319-328), if `listStateNames` is empty AND `table.getStateName() != null`,
  fail fast with a clear error pointing at the registration-ordering bug
  rather than silently routing to recordPut.

---

## LOW severity

- **A1-L1** — `frs_create_checkpoint` (lib.rs:1508) does not flush the WAL /
  memtable before invoking `db.create_checkpoint`. Engine-level behavior
  may or may not implicitly flush; depending on engine impl, a snapshot
  could miss unflushed memtable rows. Verify by reading the engine
  side or pair with an explicit `db.flush_all` precondition.
- **A1-L2** — `frs_db_release_snapshot` (lib.rs:2902) is unchecked — if a
  caller releases a snapshot that was already freed, the engine's behavior
  may corrupt the snapshot table. The FFI does no double-free guard. Add
  a presence check or document that callers must single-release.
- **A1-L3** — `IterPrefixBatchBuffer` / `IterRangeBatchBuffer` per-iter
  `Arena.ofShared()` (VectorizedExecutor.java:658) is allocated PER ROW.
  Same per-row catch-and-continue gap as A1-M2: if `Arena.ofShared`
  throws, the row's future is never completed. Mirror A1-H4 fix here.
- **A1-L4** — `DataOutputSerializer keyOut/valueOut` in
  ValueStateV2/MapStateV2/ListStateV2 is per-instance not per-call.
  Comment claims "single-threaded operator thread", but the V2 async path
  could in principle issue concurrent serializer invocations across
  different futures' completion handlers (`thenApply` on a different
  executor). Verify by inspecting the async-state runtime's
  thread-confinement guarantees, or guard with an assertion.

---

## Verdict
**Overall correctness posture: CRITICAL.**

The async V2 backend cannot be used in any production-like setting until
A1-H1 (real snapshot), A1-H2 (real flushDirty), A1-H3 (timer-queue snapshot
hook), A1-H4 (future leak on engine error), A1-H5 (silent LIST_ADD success
on engine failure), and A1-H6 (cross-state extra-cache collision) are fixed.
A1-H4 + A1-H5 are silent data-corruption bugs that the test suite is unlikely
to catch without explicit fault-injection. A1-H1 + A1-H2 + A1-H3 together
mean that even a clean shutdown loses state between executions; any TM
crash loses everything since the last engine-side memtable flush.

The Round-1 recommendation is to land a fault-injecting FFI mock (returns
non-OK rcs deterministically) and an exit-from-TM-during-batch integration
test BEFORE further perf work. Both A1-H4 and A1-H5 will surface in those
tests immediately.
