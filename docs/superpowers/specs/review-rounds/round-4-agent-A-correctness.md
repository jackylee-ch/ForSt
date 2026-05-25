# Round 4 Agent A — Correctness / Data Consistency Review

Reviewer angle: criterion #1 — verify the 27 batched PRs that landed between
`63ea1c2904e` (Round 2 baseline) and `329ba5f9063` (HEAD `forst-rs-jdk25`)
actually closed Section 1 (S1-1 … S1-12, V13/V14) and the Round 3 A3 deltas,
and find any **NEW** HIGHs the 27 PRs introduced.

Files examined (absolute paths):
- `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAsyncKeyedStateBackend.java`
- `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java`
- `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsSnapshotStrategy.java`
- `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/sst/{ForStRsSstUploader,ForStRsSstRegistry,SstRetryStrategy}.java`
- `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java`
- `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedClassifier.java`
- `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/{ForStRsValueStateV2,ForStRsMapStateV2,ForStRsAsyncListStateV2,ForStRsAsyncReducingStateV2,ForStRsAsyncAggregatingStateV2,MapStateArrowBuffer,ListStateArrowBuffer,StateSerializerRegistry}.java`
- `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ttl/{TtlAwareValueStateV2,TtlSerializer,TtlClock,TtlValue}.java`
- `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/cache/MapStateCache.java`
- `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/timer/ForStRsKeyGroupedInternalPriorityQueue.java`
- `/Users/lijunqing/Code/stczwd/ForSt/crates/forst-rs-ffi/src/lib.rs` (PR-D4 sharded iter registry)

## Summary

- Section-1 fixes **VERIFIED ✅** : S1-2, S1-3, S1-4, S1-6, S1-10, S1-11, S1-12, A3-H1, A3-H2, A3-H3
- Section-1 fixes **INCOMPLETE ⚠️** : S1-1, S1-5, S1-7, S1-9
- Section-1 fixes that REGRESSED **❌** : none observed
- **NEW HIGH findings: 4** (A4-H1 … A4-H4)
- NEW MEDIUM/LOW: see §New Findings — Other

---

## §1 Verification of original Section-1 HIGHs

### S1-1 — snapshot() returns SnapshotResult.empty() ⚠️ INCOMPLETE

`ForStRsAsyncKeyedStateBackend.java:732-828` now wires a real
`ForStRsSnapshotStrategy` through `SnapshotStrategyRunner`. The strategy
captures an engine snapshot via `linker.dbSnapshot`, computes incremental SST
lists via `createIncrementalCheckpointAt`, uploads via virtual-thread
`ForStRsSstUploader`, and returns a `ForStRsIncrementalKeyedStateHandle`
(class confirmed at `keyed/ForStRsIncrementalKeyedStateHandle.java`).

**However**, lines 813-827 contain a fallback that still returns
`DoneFuture.of(SnapshotResult.empty())` on every `IOException` that doesn't
contain `"registry is already closed"`, AND on the generic
`catch (Exception)` at line 825. That means: any S3 upload error, any
`createIncrementalCheckpointAt` error, any cancel-registry race surfaces as
**a successful-but-empty SnapshotResult** rather than a failed snapshot.
The checkpoint coordinator will see an empty handle, mark the checkpoint
complete, then on rescale/restart the engine has no Flink-managed handles
to restore from. See A4-H2 below.

### S1-2 — VectorizedExecutor.flushDirty empty stub ✅ VERIFIED

`VectorizedExecutor.java:429-443` calls `linker.flush(db)`, synchronously
folding the memtable to L0 SST. Snapshot path (`ForStRsAsyncKeyedStateBackend.snapshot`
PHASE 1.a + PHASE 2 lines 745, 788) calls `managedExecutors.forEach(VectorizedExecutor::flushDirty)`
twice. No remaining stub.

### S1-3 — Timer flushPendingToEngine never called ✅ VERIFIED

`ForStRsAsyncKeyedStateBackend.snapshot` PHASE 1.e lines 781-783 iterates
`registeredTimerQueues` and calls `q.flushPendingToEngine()` per queue.
Queues self-register on `create()` (line 972). HEAP timer factory (now the
non-default `pickTimerFactory()` path returning `FORSTRS` per line 127)
registers no queues so the loop is a no-op.

### S1-4 — V2 keyed-state engine keys missing namespace ✅ VERIFIED

All five V2 state classes encode namespace into the composite key on both
the byte-array `serializeKey(StateRequest)` and the vectorized
`serializeKeyInto(StateRequest, ColumnarBatchBuffer)` paths:

- `ForStRsValueStateV2.java:165-176` (heap), `:265-274` (vectorized)
- `ForStRsMapStateV2.java:367-378` (heap), `:489-498` (vectorized), `:541-554` (iter prefix), `:189-200` (cache key)
- `ForStRsAsyncListStateV2.java:280-289` (heap), `:300-310` (vectorized)
- `ForStRsAsyncReducingStateV2.java:303-313` (heap), `:323-333` (vectorized), `:170-182` (cache key)
- `ForStRsAsyncAggregatingStateV2.java:289-298` (heap), `:309-319` (vectorized), `:155-167` (cache key)

Skip is gated on `namespaceSerializer != null && !(namespace instanceof VoidNamespace)` which is the
documented optimization for the void/single-namespace case.

### S1-5 — Timer kgSupplier returns constant ⚠️ INCOMPLETE

The InternalKeyContext-based ctor (`ForStRsKeyGroupedInternalPriorityQueue.java:276-298`)
DOES wire `deriveSupplier(keyContext, totalKeyGroups, keyGroupRange)` so peek/poll
DOES route to the current key's key group. `ForStRsAsyncKeyedStateBackend.create()`
(line 950-969) passes `internalKeyContext()` so the queue's supplier reads
`currentKey` captured by `switchContext`.

**However**, commit `6e2a96f51ee` documents in its own message that S1-5 was
declared "not incorrect — startKeyGroup fallback is fine for prefix-scan
refill". Reading the code more carefully: the supplier IS now key-aware
(no longer a constant). But the **fallback when `currentKey == null` is still
`keyGroupRange.getStartKeyGroup()`** (line 343). For an operator that polls
timers from the snapshot path (before any record sets the current key) every
peek/poll reads only timers in the start key group, missing timers in other
key groups in the range. Acceptable for the steady-state per-record path
(which is the hot path); incorrect for a savepoint-restore drain or
`open()`-time recovery sequence that needs to enumerate the full set of
pending timers. Marked INCOMPLETE.

### S1-6 — V1-sync offheapKeyGroupSupplier = 0 ✅ VERIFIED (with caveat)

`ForStRsKeyedStateBackend.java:281-290` now delegates to
`computeCurrentKeyGroup()` which calls
`KeyGroupRangeAssignment.assignToKeyGroup(getCurrentKey(), numberOfKeyGroups)`.
A null-current-key falls back to `keyGroupRange.getStartKeyGroup()`. The
production constructor at line 367 takes explicit
`(keyGroupRange, numberOfKeyGroups)` so rescaling routes correctly.

Caveat: the legacy test constructors (lines 328-365) hard-code
`KeyGroupRange.of(0, 127)` + `128` — fine for tests but the comment block
at lines 222-237 is now stale (says "supplying 0 keeps key encoding
stable"). Not a correctness issue, just code-comment debt.

### S1-7 — ArrowTimerBuffer.drainTo iterates heap-array index order

Not re-verified in this round (handled in Round 3 by commit `833ddd130f7`
"R3 surgical fixes V2-9 + S1-7").

### S1-8 — Silent data loss on LIST_ADD failure ✅ VERIFIED (carried over)

Verified by Round 1+2 (commits `66f153868fc` + `63ea1c2904e`). No regression
in Round 4 — `dispatchAppendMergeBatch` (lines 828-858) propagates per-row
failures via `f.completeExceptionally(err)`.

### S1-9 — Batched FFI exception leaves StateRequest futures dangling ⚠️ INCOMPLETE

`VectorizedExecutor.executePuts/Deletes/Gets` (lines 455-610) now wrap FFI
calls in `try { ... } catch (Throwable t) { drain per-row futures with
completeXxxExceptionally; throw t; }`. PR-A10 fix is in place for the
batched paths.

**Gap**: `dispatchAppendMergePerRow` (lines 665-749) does NOT have a
try/catch around `linker.frsVecMergeAppend` (line 712). A mid-loop throw
at row R leaves rows R+1 … count-1 with un-completed futures. The outer
`catch (Throwable t)` in `executeBatchRequests` (line 317) does fail the
container future, but the per-row futures for the remaining rows
(`amBuf.futures()`) stay pending — same A1-H5 stall pattern. Surgical:
move the per-row loop into a try/catch that drains the remaining
`amBuf.futures()` on throw.

### S1-10 — ValueStateV2 cache slot collides across multiple states ✅ VERIFIED

`ForStRsValueStateV2.java:59, 72, 142-189` now keys per-state via a dense
ordinal counter (`NEXT_STATE_ORDINAL`) and stores a `Slot[]` in
`RecordContext.getExtra()` indexed by `stateOrdinal`. Two ValueStates in the
same operator have distinct slot indices and cannot collide.

PR-A5 commit `19c8bb26e4b` confirms this fix. The new layout grows the
slot array if a state ordinal climbs past the current capacity (line
153-157).

### S1-11 — MapStateCache survives asyncClear ✅ VERIFIED

`ForStRsMapStateV2.buildDBPutRequest` (lines 453-478) intercepts
`StateRequestType.CLEAR` and calls `cache.clearForPrefix(prefix)` AND
`offHeapBuf.clearForPrefix(prefix, linker, db, cf)` BEFORE building the
DB request. `MapStateCache.clearForPrefix` (lines 262-290) rebuilds the
hash index. `MapStateArrowBuffer.clearForPrefix` (lines 307-313)
flush-drains pending writes THEN clears, preserving order.

### S1-12 — State TTL silently disabled ✅ VERIFIED

`ForStRsAsyncKeyedStateBackend.getOrCreateKeyedState` (lines 436-461)
reads `desc.getTtlConfig()`. When enabled, calls
`createTtlAwareStateInternal` (lines 469-504) which wraps ValueState in
`TtlAwareValueStateV2` and stamps an expiry prefix on every write via
`TtlSerializer`.

**Caveat (intentional)**: only `VALUE` is TTL-supported in this PR;
MAP/LIST/REDUCING/AGGREGATING throw `UnsupportedOperationException` with
a follow-on PR pointer. That's a loud failure (correct per the issue
which complained about silent ignore), and the issue description says
"users specifying StateTtlConfig see no actual expiry" — that's now fixed
for ValueState and explicitly rejected for others.

---

## §2 Verification of Round 3 deltas (A3-H1 / A3-H2 / A3-H3)

### A3-H1 — executeRequestSync lacks try/catch ✅ VERIFIED

`VectorizedExecutor.executeRequestSync` (lines 333-350) now extracts the
dispatch body into `executeRequestSyncInner` and wraps the call in
`try { ... } catch (Throwable t) { completePutExceptionally(request, t); }`.
Same throw will still surface to the caller (no re-throw — but the sync
contract is void so the caller's handling depends on the framework's
synchronous-dispatch contract). The request future IS resolved
exceptionally, so the runtime no longer hangs.

### A3-H2 — executeBatchRequests catches only Exception ✅ VERIFIED

`VectorizedExecutor.executeBatchRequests` line 317 now catches
`Throwable t` (widened from `Exception e`). `FrsEnginePanicError` (which
extends `Error`) is captured.

### A3-H3 — MapStateV2 asyncClear bypasses cache ✅ VERIFIED

Closed by PR-A6 via the `buildDBPutRequest` hook described in S1-11 above.
The in-code comment at `ForStRsMapStateV2.java:159-172` documents the
mechanism (asyncClear is final on the parent, so we hook at the
request-build step which the AEC invokes in the same RecordContext lock).

---

## §3 NEW HIGH findings (introduced by the 27 PRs)

### A4-H1 — ForStRsAsyncListStateV2 asyncClear does not flush the off-heap accumulator → data leaks past CLEAR

- **File:line**: `flink-statebackend-forst-rs/.../state/ForStRsAsyncListStateV2.java:468-477` (buildDBPutRequest CLEAR branch) + `VectorizedExecutor.java:249-251, 270-303` (executeBatchRequests dispatch order)
- **Severity**: HIGH (silent wrong result)
- **Repro**:
  ```java
  listState.asyncAdd("v1");          // → off-heap ListStateArrowBuffer (pending, not flushed)
  listState.asyncClear();            // → routes to recordDelete, executed via executeDeletes
  // After this batch: engine sees DELETE first, then APPEND_MERGE flushes via
  // flushOffHeapListBuffersIfDirty at end of executeBatchRequests.
  // Final state: ["v1"] instead of []
  ```
- **Root cause**: `ForStRsAsyncListStateV2.buildDBPutRequest` for `CLEAR`
  (lines 468-477) does NOT drain the off-heap buffer. The off-heap buffer
  holds appends from the same batch (or accumulated from prior batches
  that did not auto-flush) and is drained at the END of the batch by
  `classifier.flushOffHeapListBuffersIfDirty()` (VectorizedExecutor.java:271
  and 366). The classifier's dispatch order is PUT → DELETE → GET → ITER →
  APPEND_MERGE-flush, so DELETE runs BEFORE the APPEND_MERGE drain, and the
  pending appends land AFTER the engine has applied the delete.
- **Contrast with MapStateV2**: `ForStRsMapStateV2.buildDBPutRequest` on
  CLEAR calls `offHeapBuf.clearForPrefix(prefix, linker, db, cf)` which
  itself calls `flushTo` BEFORE clearing the buffer — see
  `MapStateArrowBuffer.java:307-313`. ListState has no such hook.
- **Blast radius**: Any operator that does `add` then `clear` in the same
  record (window-cleanup paths, accumulator-reset paths) silently retains
  the post-clear appends. Cross-batch: any auto-flush-deferred residue
  from prior batches gets stamped onto a freshly-cleared key.
- **Fix sketch**: Override `buildDBPutRequest` (or add a `recordDelete`
  hook in the classifier) for ListStateV2 to call `buffer.flushTo(...)`
  before producing the CLEAR/DELETE request. Add a unit test:
  `asyncAdd` → `asyncClear` → `asyncGet` should return empty.

### A4-H2 — RMW cache flushHandler is unwired in production; Reducing/Aggregating V2 state is LOST on snapshot

- **File:line**: `state/ForStRsAsyncReducingStateV2.java:118` (`private volatile BiConsumer<byte[], byte[]> flushHandler = (k, v) -> {};`) and `state/ForStRsAsyncAggregatingStateV2.java:114` (same); `keyed/ForStRsAsyncKeyedStateBackend.java:592-623` (createReducingState / createAggregatingState — no setFlushHandler call)
- **Severity**: HIGH (silent data loss across snapshots)
- **Repro**:
  ```java
  // operator code:
  reducingState.asyncAdd(v1);  // → cache.tryFold (hit) — no engine I/O
  reducingState.asyncAdd(v2);  // → cache.tryFold (hit) — folded in-cache
  // checkpoint barrier:
  backend.snapshot(...)        // → calls reducingV2.flushOnBarrier()
                              //   → cache.flushAllDirty() → flushHandler.accept(k, v)
                              //   → DEFAULT HANDLER IS (k, v) -> {}    // discards!
  // After restore: reducing state for this key is missing both v1 and v2.
  ```
- **Root cause**: `flushOnBarrier()` (line 259) is wired correctly from
  `ForStRsAsyncKeyedStateBackend.snapshot` (line 770-775). But the
  default `flushHandler` is a no-op lambda. **The backend never calls
  `setFlushHandler` on production code paths** — `grep -rn setFlushHandler`
  shows callers are only tests. The in-code comment at line 113-118 says
  "production wiring to the engine PUT path is gated on PR-A1 / V1.1"
  — PR-A1 has landed (see ForStRsSnapshotStrategy), but the
  `setFlushHandler` wiring did NOT.
- **Blast radius**: Every Nexmark/SQL query using `ReducingState` or
  `AggregatingState` V2 (Q1, Q2, Q12, Q19, …) loses the in-memory RMW
  accumulator at every checkpoint. The next session's `asyncGet`
  bypasses the cache and falls through to `asyncGetInternal` which
  reads from the engine — and the engine never received the PUTs. So
  the count restarts from null. Q11/Q12 use V1-sync ValueState so
  they're not affected; Q5/Q8/Q13 use V1-sync as well; but Q15/Q19
  + any V2 aggregate would be affected.
- **Fix sketch**: In `createReducingState` / `createAggregatingState`,
  call `state.setFlushHandler(new EngineWriteHandler(linker, db, defaultCf))`
  where `EngineWriteHandler` issues a per-call `frs_batch_put` (or
  better, accumulates rows and issues a single batched put on snapshot).
  Add a unit test that does `asyncAdd → snapshot → restore in a new
  backend → asyncGet returns the accumulator`.

### A4-H3 — snapshot() swallows IOException + Exception, returns SnapshotResult.empty() — checkpoint coordinator silently sees success

- **File:line**: `keyed/ForStRsAsyncKeyedStateBackend.java:813-827`
- **Severity**: HIGH (silent successful-but-empty checkpoint)
- **Code**:
  ```java
  } catch (IOException e) {
      if (e.getMessage() != null && e.getMessage().contains("registry is already closed")) {
          return DoneFuture.of(SnapshotResult.empty());
      }
      // Wrap any other IO failure into an empty result so the coordinator proceeds
      return DoneFuture.of(SnapshotResult.empty());
  } catch (Exception e) {
      return DoneFuture.of(SnapshotResult.empty());
  }
  ```
- **Root cause**: The comment explicitly says "Wrap any other IO failure
  into an empty result so the coordinator proceeds — the V2 async
  backend's contract returns a RunnableFuture, not throws." That is the
  wrong contract: returning `SnapshotResult.empty()` (success-with-empty)
  tells the checkpoint coordinator the snapshot succeeded with no
  state, NOT that the snapshot failed. The proper return for failure
  is to return a future that throws (e.g.,
  `DoneFuture.of(...)`-style but exceptional, or a real
  `RunnableFuture` that throws on `run()`). The current code masks S3
  upload failures, FFI `createIncrementalCheckpointAt` failures, and
  any uncaught `RuntimeException` from the strategy as "successful
  empty snapshot" — and on restart there's nothing to restore.
- **Blast radius**: Production checkpoint that should fail (and trigger
  Flink's checkpoint-failure handling: alert, retry, kill task) is
  reported as success, breaking the at-least-once guarantee. Combined
  with A4-H2, the state can be missing AND the checkpoint reports
  success.
- **Fix sketch**: Construct an exceptional `RunnableFuture` (e.g.,
  `new FutureTask<>(() -> { throw e; })`) and return it instead of
  `DoneFuture.of(SnapshotResult.empty())`. Reserve `empty()` for the
  `"registry is already closed"` cancellation case ONLY (and even that
  arguably warrants a `cancelled()` future, not `empty()`).

### A4-H4 — Restore path absent: ForStRsAsyncKeyedStateBackend has no restore() / no constructor entry point for restored handles

- **File:line**: `keyed/ForStRsAsyncKeyedStateBackend.java` (no restore method)
- **Severity**: HIGH (impossible to restore from snapshot)
- **Observation**: PR-A1 lands `snapshot()` + `ForStRsSnapshotStrategy`
  + `ForStRsIncrementalKeyedStateHandle`. But there is no symmetric
  restore implementation:
  - No `restore(...)` method on `ForStRsAsyncKeyedStateBackend`.
  - No factory entry-point that takes a
    `Collection<KeyedStateHandle> restoreHandles` and invokes
    `linker.dbOpenFromIncremental(...)` (the FFI helper exists, see
    `ForStRsLinker.java:3148` comment "createIncrementalCheckpointAt +
    dbOpenFromIncremental — capture + restore").
  - `StateSerializerRegistry.seedFromRestore` is exposed
    (`StateSerializerRegistry.java:214`) but never called from the
    backend — only by tests.
  - The in-code TODO at line 727-728 in
    `ForStRsAsyncKeyedStateBackend.snapshot` admits this: "{@code
    StateSerializerRegistry.metadataBuffer} — PR-A11 staged the registry
    but the snapshot does not yet serialize the registry blob into the
    metaHandle. Restore-side PR-A11 still uses {@code seedFromRestore}."
- **Blast radius**: Snapshots produced by PR-A1 are write-only. A
  Flink task-manager restart, rescale, or HA failover restores from
  the last snapshot — but `ForStRsStateBackend.createAsyncKeyedStateBackend`
  has no path to consume the handles, so the new session starts with
  empty engine state regardless of the snapshot's contents. The
  IncrementalKeyedStateHandle's `sharedState` / `metaHandle` are
  uploaded but never re-read.
- **Fix sketch**: Add `restoreFromHandles(Collection<KeyedStateHandle>)`
  to `ForStRsAsyncKeyedStateBackend` that:
  1. Drains the meta-handle to read the StateSerializerRegistry blob.
  2. Downloads + materializes shared SSTs to a local dir.
  3. Calls `linker.dbOpenFromIncremental(local_dir, manifest_path)` to
     restore the engine.
  4. Seeds `stateSerializerRegistry` via `seedFromRestore`.
  5. Bumps `lastCompletedCheckpointId` to the restored ckpt id so
     subsequent incrementals base off this point.
  Add an integration test: `snapshot → close → recreate backend with
  restoreHandles → asyncGet returns previously-written values`.

---

## §4 New Findings — Other (MEDIUM / observations, not HIGH)

### A4-M1 — ForStRsSstUploader.upload spawns a fresh virtual thread per file with no concurrency cap

- **File:line**: `keyed/sst/ForStRsSstUploader.java:86-101`
- **Risk**: a checkpoint with thousands of SSTs spawns thousands of
  virtual threads simultaneously, each holding an S3 client + an
  8KiB IO buffer and an open file handle. Virtual threads are
  lightweight but the underlying NIO carrier-thread pool, the S3
  client connection pool, and the local file-descriptor count are
  not unbounded. Recommend a `Semaphore`-bounded concurrent uploader
  or use Flink's `ExecutorService` from the
  `AsyncCheckpointRunnable` context.

### A4-M2 — ForStRsAsyncKeyedStateBackend.dispose path does not unregister timer queues / V2 states

- **File:line**: `ForStRsAsyncKeyedStateBackend.java:993-1016`
- **Risk**: `dispose()` does not clear `registeredTimerQueues`,
  `registeredMapStatesV2`, `registeredAsyncReducingStates`, etc.
  After dispose, these registries still hold references to the now-stale
  state instances. If `snapshot()` is somehow called post-dispose (race
  with the runtime cancel signal), the loops at lines 750-783 dereference
  closed state objects. Surgical: clear all registry lists in `dispose`.

### A4-L1 — ForStRsSstRegistry.unregister allows refCount to go negative without complaint

- **File:line**: `keyed/sst/ForStRsSstRegistry.java:91-102`
- **Risk**: Theoretical only — `unregister` an id that's been bumped
  decrements; if a notify-aborted fires twice (Flink can re-emit
  certain notifications), refCount could underflow. Trivial guard:
  `if (entry.refCount <= 0) { entries.remove(id); return true; }` is
  already there but `entry.refCount--` happens unconditionally first.
  Replace with `entry.refCount = Math.max(0, entry.refCount - 1)`.

### A4-L2 — PR-D4 sharded-iter-handle registry: NEXT_ITER_ID can wrap to 0

- **File:line**: `crates/forst-rs-ffi/src/lib.rs:3572`
- **Risk**: `static NEXT_ITER_ID: AtomicU64 = AtomicU64::new(1);` —
  fetched with `fetch_add(1, Relaxed)`. After 2^64 iterator opens (in
  practice never reachable, but worth documenting), the counter wraps
  to 0 and then 1; `frs_vec_iter_prefix_close(0)` is a no-op, so a
  legitimate handle would skip the unregister. Cosmetic; document or
  panic on overflow.

---

## §5 Sharded iter-handle registry concurrency audit (PR-D4)

`crates/forst-rs-ffi/src/lib.rs:3563-3806` introduces 16-shard
`Mutex<HashMap<u64, IterHandle>>`. Open / next / close / abort all
take the per-shard mutex; `IterHandle.aborted` is an `AtomicBool`
toggled while holding the shard lock so `next` (also under the same
shard lock) reads a consistent value via Acquire-Release pair.

No race-condition HIGH found. The design is sound:
- `frs_vec_iter_prefix_next` holds the shard lock for the entire
  `fill_chunk_from_iter` call — `abort()` from another thread blocks
  until next returns. Long-running scans hold the lock longer than
  necessary (lockholding scales with chunk size); the per-shard
  contention is still independent across shards, so a watchdog
  thread aborting one slot's iter doesn't block iter-next on a
  different slot's iter. Acceptable.

---

## §6 Round-4 verdict

**4 NEW HIGHs**, **4 INCOMPLETE original HIGHs**, **0 REGRESSED**.

Most concerning are A4-H2 (Reducing/Aggregating V2 state is silently
volatile across snapshots — production data loss) and A4-H3 (snapshot
errors masked as success — at-least-once semantics broken). A4-H4
(no restore path) is structurally severe but the team likely knows
it's pending; this report makes it explicit. A4-H1 (ListState V2
clear ordering) is a silent wrong-result for any operator that
interleaves add+clear within a record.

Recommend Round-5 patches address all four in this order:
1. **A4-H2 wire-up flushHandler** — engine PUT route, single-line
   constructor change in createReducingState / createAggregatingState.
2. **A4-H1 flush ListBuffer on CLEAR** — surgical hook in
   ForStRsAsyncListStateV2.buildDBPutRequest mirroring MapStateV2.
3. **A4-H3 return exceptional future** — replace
   `DoneFuture.of(SnapshotResult.empty())` with a thrown future on
   non-cancellation errors.
4. **A4-H4 restoreFromHandles** — multi-day work but blocks
   production rescale/HA.
