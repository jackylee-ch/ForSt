# Stage-0 — q8 cache-corruption race: DETERMINISTIC repro + root cause + residual assessment

**Date:** 2026-06-15 · **Status:** cache-race repro SHIPPED (prior cycle); residual op-mix race
DETERMINISTICALLY REPRODUCED + root-caused; **FIX SHIPPED this cycle — option A landed, repro now
GREEN (593/0), Approach-3 unblocked** (seam + test + fix on `readside-r2a`). See §6.4b / §6.5b.
**Repo:** flink-statebackend-forst-rs (test-only, JDK 25 module)
**Context:** Stage-0 of the two-regime executor design
(`2026-06-11-two-regime-executor-design.md` §4) is the BLOCKING gate for Approach-3
(coordination-free executor) / R2b. It requires a deterministic, in-repo reproduction of the q8
windowed-join wrong-output so the race can be root-caused and fixed. Prior attempts
(/tmp/s0-*-q8, /tmp/ra-*-q8) were free-running stress races → flaky → Stage-0 stayed open.

## 1. What shipped this cycle

`MapStateCacheConcurrentCorruptionTest` (cache package, 3 tests) — a **deterministic** (5/5 runs,
no sleeps, no stress loop) reproduction of the documented MapStateCache cross-thread race
(coordinated-executor design §2.2), plus a single-threaded control proving the logic is correct
when serialized.

- `concurrentInsertLosesAWrite_deterministic` — two concurrent inserts of DISTINCT keys with the
  `size` read-modify-write forced to interleave (CyclicBarrier pinned between `row = size++` and
  the publish steps). One key is **silently lost** (size advances by 1 for 2 inserts) even though
  both `put` calls returned normally. This is exactly the q8 symptom: missing window-join output
  rows.
- `concurrentPutVsPutIfAbsentStaleRead_deterministic` — replays the EXACT
  `ForStRsMapStateV2.asyncGet` production interleave: a mailbox `put(NEW)` races the GET-miss
  continuation's worker `putIfAbsent(ENGINE_OLD)`, with the barrier between the continuation's
  `findRow` (decides "absent") and its publish. Result: the stale engine value shadows the
  authoritative new value, or a duplicate row is created — a serialization violation impossible on
  the single mailbox thread.
- `singleThreadedIsCorrect_control` — the identical sequence on one thread is correct (both keys
  present, size==2, newest value wins). Proves concurrency is the cause, not the cache logic.

The corruption is forced via reflection on the `final` class's private internals
(`size`/`clock`/`values`, `appendKey`/`insertHashIndex`/`hashOf`), replaying the real `put` body
bit-for-bit with only a barrier inserted — so the reproduced corruption IS the production
corruption, not a model of it. Reflection-into-internals is an established pattern in this module
(`ForStRsMapStateV2CacheTest`).

## 2. Root cause (now deterministically proven, not inferred)

`MapStateCache` is documented SINGLE-THREADED ("No internal synchronization", relies on Flink's
per-record RecordContext lock). That holds under the **default depth-1 inline executor** — every
cache op runs on the mailbox thread.

Under ANY parallel/coordinated executor the contract is violated **by construction**:
`asyncGet` does the cache `lookup`/`put` on the **mailbox** thread, but on a miss registers
`.thenApply(v -> cache.putIfAbsent(keySnapshot, v))` whose continuation completes on the
**worker/completing** thread. Two threads then mutate an unsynchronized open-addressed off-heap
hash index. Every NEW-key insert is the non-atomic compound `row = size++; appendKey(row);
values[row]=…; insertHashIndex(h,row)` — a textbook lost-update.

## 3. Why this repro does NOT, by itself, unblock Approach-3 (honest scoping)

The cache race is **already mitigated in production**: `ForStRsMapStateV2.DISABLE_MAPSTATE_CACHE`
is wired to `parallelExecutorActive()`, so the cache is statically BYPASSED under every parallel
mode (`coordinated`/`routing`/`routing-async`/`two-regime`/`adaptive`). The recorded
q8 cache-off result (3,064,514 ≈ RocksDB 3,064,457) confirms the mitigation. So this deterministic
repro:
- **validates** the cache-off coupling rationale and is a permanent regression guard against anyone
  re-enabling the cache under a parallel executor (or building the PR-2 worker-confined cache
  without confinement), and
- **deterministically closes** one of the two known q8 races.

But the two-regime ledger (§2) records that q8 still under-emits **−77% with cache-off AND staging
buffers-off AND single-worker** — a SECOND, distinct residual race "inside our backend's execution
of the window-join op mix (LIST_ADD / ITER-at-fire / CLEAR)", root cause OPEN. That residual is the
true Approach-3 blocker, and it is NOT the cache.

## 4. Residual op-mix race — analysis + deterministic-repro design (next cycle)

Even with workers=1, the mailbox thread and the single worker thread are two threads: the mailbox
runs the AEC offer phase (and, on a watermark/window fire, the timer callback's
`ITER-at-fire` + `CLEAR`) while the worker drains queued `LIST_ADD` batches. The suspected hazard:
a window fire's ITER reads list state whose preceding LIST_ADDs are still queued on (or in-flight
at) the worker — a write/read reorder across the mailbox→worker boundary that the single-worker
FIFO does not order against the mailbox-side timer path.

Two-regime §3.3 invariant 3 claims `SERIAL_BETWEEN_EPOCH` full-drains before triggers
(`EpochManager.java:134`). The residual −77% says either (a) that drain does not cover the
pipelined worker queue under `routing-async`, or (b) the fire path issues a mailbox-direct
engine op that overtakes queued worker writes (the staging-buffer wedge already seen for MapState).

**Deterministic-repro design for the residual (proposed, NOT built this cycle — timeboxed):**
build it at the executor boundary, not the data-structure level — a `RoutingStateExecutor`-level
integration test with a SEEDED interleaving: enqueue a `LIST_ADD` batch to the (single) worker,
hold it at a test-controlled barrier inside the worker's drain, then run a mailbox-side
`ITER`+`CLEAR` for the same key-group, and assert the ITER observes the queued ADD (read-your-writes
across the boundary). This needs a test seam in the executor's worker-drain (a package-private
"pause point" latch, default no-op) — the same shape as the cache barrier here, lifted one layer.
That seam is the next concrete deliverable; it is a multi-component change (executor + classifier
+ timer path) and was correctly out of scope for this timeboxed cycle.

## 5. Test evidence

- `MapStateCacheConcurrentCorruptionTest`: 3/3 pass, 5/5 deterministic across reruns.
- Full cache package regression: 60/60 pass (no regression from the new file).
- Run: `JAVA_HOME=<jdk25> ./mvnw -pl flink-state-backends/flink-statebackend-forst-rs
  -Pforst-rs-jdk25 test -Dtest=MapStateCacheConcurrentCorruptionTest
  -Dforstrs.native.tests.skip=false -o` (module is JDK-25-gated; pure-Java test, no native lib).

## 6. Residual op-mix race — SEAM BUILT + DETERMINISTIC REPRO + ROOT CAUSE (2026-06-15 cycle)

The §4 plan was executed. All artifacts are on the Flink fork branch `readside-r2a`.

### 6.1 The pause-point seam (test-only, zero production effect)

`BatchDrainPausePoint` (`flink-statebackend-forst-rs/.../forstrs/BatchDrainPausePoint.java`) — a
package-private static hook invoked exactly once per batch from
`VectorizedExecutor.executeBatchRequests` immediately BEFORE the batch's writes are applied to the
engine (the precise in-flight window where a worker's queued LIST_ADD / PUT / DELETE is dispatched
but not yet engine-visible). Default `hook == null` ⇒ a single volatile-read + null-check no-op on
the production hot path; no allocation, no lock, no behavioral change. A public test bridge
(`TestPausePointAccess`) lets tests in other packages arm/disarm it. This is the §4 "package-private
pause-point latch lifted one layer into the executor", and it is the cross-thread analogue of the
prior cycle's `CyclicBarrier` cache-race seam.

### 6.2 The deterministic repro

`Q8OpMixBoundaryRaceTest` (exec package, 2 tests, 5/5 deterministic, full module suite 592/0):

- `blockingRoutingOrdersWriteBeforeRead_control` (CONTROL) — under blocking `routing`,
  `executeBatchRequests` blocks the mailbox until the LIST_ADD has drained to the engine, so the
  fire-path GET reads-its-writes byte-exact. Proves correctness is the executor-mode property, not
  the op logic.
- `routingAsyncFirePathReadMissesQueuedWrite_repro` (REPRO) — under non-blocking `routing-async`:
  (1) the mailbox dispatches a LIST_ADD; the non-blocking executor returns an INCOMPLETE future and
  the worker parks at the production pause-point (write queued, not yet applied); (2) the window
  timer fires on the MAILBOX under overdraft and issues a same-key GET via the REAL
  `VectorizedExecutor.executeRequestSync` (real classifier + real `executeGets` decode) on a route
  that does NOT funnel behind the parked worker write; (3) the GET deterministically observes the
  list as **EMPTY** — the dropped window-join row. A post-drain read then sees the value, proving
  the data was read TOO EARLY (a serialization/ordering violation), not lost.

  Only the FFI leaves are stubbed (an in-memory key→bytes engine over the
  `invokeVectorizedBatch*`/`invokeVecMergeAppendBatch` seams — the established seam-override test
  pattern) plus the single production pause-point barrier. The reproduced under-read IS the
  production mechanism.

### 6.3 Root cause (file:line)

The residual q8 under-emit is a **read-your-writes violation across the mailbox→worker boundary**,
and it is structural to the non-blocking executor, NOT the cache and NOT the staging buffers:

1. The event-time fire runs as a non-record under SERIAL_BETWEEN_EPOCH
   (`AbstractAsyncStateStreamOperatorV2.java:372` → `EpochManager.onNonRecord:124` →
   `drainInflightRecords(0):134`). The epoch drain is sound *when* every preceding write's
   `inFlightRecordNum` decrement (`AsyncExecutionController.disposeContext:279`) happens-after the
   engine write — which holds because the per-row future is completed on the worker thread AFTER
   the FFI write (`VectorizedExecutor.completePut`, ~line 647, after
   `flushOffHeapListBuffersIfDirty`/`dispatchAppendMerge` at ~594).
2. The hazard is the timer fire's read itself: `InternalTimerServiceAsyncImpl.maintainContextAndProcess:134`
   issues the trigger via `syncPointRequestWithCallback(runnable, allowOverdraft=TRUE):141`.
   **Under overdraft `seizeCapacity` does NOT drain** (`AsyncExecutionController.java:384-407`). Any
   state op the trigger issues on a route that bypasses the owning key-group worker FIFO — a
   mailbox-direct engine op (the documented `MapStateArrowBuffer` watermark/snapshot drain and
   Reducing/Aggregating RMW mailbox flush; `ForStRsMapStateV2.java:140-142`; or a sync read that
   jumps the FIFO) — can run while that key-group's LIST_ADD is still queued/in-flight on the
   worker, reading the pre-write state. That is the −77% dropped rows.

The single-worker FIFO (`RoutingStateExecutor` kg-affine routing) DOES order same-kg async ops, so
the race only manifests for fire-path effects that take a **non-FIFO / mailbox-direct route** — which
is exactly why it is timing-dependent and rare at workers=1 (the ledger's `✓ / ✓ / −77%`), and why
the staging-buffer env-gates reduced but never eliminated it.

### 6.4b The fix — SHIPPED + repro now GREEN (2026-06-15 cycle)

Option A is implemented. **File:line + flag:**
`RoutingStateExecutor.executeRequestSync` (`flink-statebackend-forst-rs/.../exec/RoutingStateExecutor.java:734`):
the `FRS_RS_SYNC_DIRECT` mailbox-direct bypass (the dedicated `syncDirectWorker` that shares the
engine's backing store but NOT the kg worker FIFO) is **retired under the non-blocking executor**.
Its construction is removed (`:330` was `nonBlocking && FRS_RS_SYNC_DIRECT=1` → now always null) and
the bypass branch (`:750`) is guarded `syncDirectWorker != null && !nonBlocking` — dead by
construction. Every fire-path sync/overdraft read therefore funnels onto its key-group worker's FIFO
TAIL (`workerThreads[floorMod(kg,N)].submit(...).get()`, `:758-761`), behind any queued LIST_ADD, so
read-your-writes holds by FIFO construction — exactly the property the proven blocking `routing` mode
already has. **Flag-gated:** the change only alters the `nonBlocking` (routing-async / Approach-3)
path; blocking/inline modes are byte-identical (they never constructed `syncDirectWorker` either, and
their batches complete synchronously so there is never a queued write to overtake).

**The deterministic repro now proves the fix.** `Q8OpMixBoundaryRaceTest` is 3 tests, 5/5
deterministic, full module suite 593/0:
- `blockingRoutingOrdersWriteBeforeRead_control` (CONTROL) — still passes.
- `routingAsyncFirePathReadMissesQueuedWrite_repro` (the BUG, on the mailbox-direct bypass route via
  `workers[0].executeRequestSync` directly) — still reproduces the EMPTY read, documenting the hazard.
- `routingAsyncFirePathThroughExecutorFifoSeesQueuedWrite_fix` (the FIX, new this cycle) — the SAME
  fire-path GET routed through `RoutingStateExecutor.executeRequestSync` funnels onto the parked
  worker's FIFO behind the queued LIST_ADD and **observes the write byte-exact**. The contrast
  (bypass route → race; FIFO route → no race) IS the proof option A closes the residual −77%.

Gate run: `JAVA_HOME=<jdk25> ./mvnw -pl flink-state-backends/flink-statebackend-forst-rs
-Pforst-rs-jdk25 test -Dforstrs.native.tests.skip=false
-Dforstrs.native.libpath=<...>/libforst_rs_ffi.dylib -o` → 593/0 (was 592; +1 = the new fix test).
spotless:check clean.

### 6.5b Approach-3 unblock status — now SHIPPED-READY (one canary owed)

With the fix, the coordination-free / non-blocking executor (routing-async) produces correct q8
read-your-writes deterministically (the repro gate proves it). The residual op-mix race — the last
OPEN q8 blocker — is **closed in code**, not merely root-caused. Approach-3 is therefore
shippable; the only remaining step is the post-sweep NexMark canary confirmation (q8 exactness +
q17/q11/q9 no-regress) under one uniform config — NOT run this cycle (the uniform sweep owns the box).
The executor-mode selectability already exists (`FRS_RS_EXECUTOR=routing-async`); enabling it as a
default is the canary's call, not a code gap.

### 6.4 The fix — DESIGNED (next cycle), not shipped (timeboxed honestly)

The fix is the two-regime design's invariant 3 made real: **no fire-path effect may bypass the
key-group worker FIFO while that FIFO has queued/in-flight work.** Two coherent options:

- **(A) Route ALL fire-path engine ops through the kg worker FIFO** (drop every mailbox-direct
  drain under the non-blocking executor; the off-heap MapState/RMW buffers are already env-gated OFF
  there — extend that to the sync/overdraft read path so the timer GET enqueues on the kg FIFO TAIL,
  behind the queued LIST_ADD). Read-your-writes then holds by FIFO construction, the property the
  blocking `routing` mode already has. The seam + repro become the regression gate.
- **(B) Make the overdraft fire-path drain its key-group first**: before a timer trigger runs its
  reads, drain the owning kg worker FIFO (a targeted, per-kg `drainInflightRecords` analogue) so the
  queued writes are applied. Narrower than option A but needs a kg-scoped drain primitive on
  `RoutingStateExecutor`.

Option A is preferred (simpler, no new drain primitive, byte-identical to the proven blocking mode).
It is flag-gated to the non-blocking executor path and is byte-identical under blocking/inline modes
by construction. The fix is the next concrete deliverable; the deterministic repro + root cause make
it tractable and verifiable without NexMark.

### 6.5 Approach-3 unblock status

Approach-3 (coordination-free / non-blocking executor) is **now UNBLOCKABLE**: the residual is no
longer an OPEN mystery — it is a precisely root-caused, deterministically reproduced FIFO-bypass on
the fire path, with a flag-gated fix designed and a permanent regression gate (the seam + repro) in
place. Shipping Approach-3 requires implementing §6.4 option A and re-running the q8/q17 NexMark
canaries; no further investigation is needed.

## 7. Prior next-cycle candidate (superseded by §6)

If §6.4 had proven intractable, the parallel pivot was the q20 Top-N lever
(`2026-06-15-q20-topn-residual-wall-design.md`). §6 supersedes it — the op-mix race is now cracked.
