# Coordinated Async Executor (PR-1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make forst-rs's async-state executor non-blocking and parallel by default (ForSt coordinator model), banking the measured opt-in wins (q11 134.7s, q20 finishes, q7 1052s) WITHOUT robbing q17 (the per-batch mailbox block) and WITHOUT the cache race (cache-off-when-parallel coupling stays, as in the measured spike).

**Architecture:** Three layers, built bottom-up: (1) per-classifier buffer ownership in `VectorizedExecutor` (today one pooled classifier shares executor-owned Arrow buffers — the documented blocker for depth>1); (2) a classifier pool so batch N+1's container can be created while batch N executes; (3) `CoordinatedStateExecutor` — key-group-affine routing (reused from `RoutingStateExecutor`) but returning an INCOMPLETE future immediately, with real `fullyLoaded()` accounting. Kill-switch envs select the executor.

**Tech Stack:** Java 25 (Panama FFM), Flink 2.2.1 async-state V2 runtime, JUnit 5. Repo: `~/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs`.

**Spec:** `docs/superpowers/specs/2026-06-10-forstrs-coordinated-executor-design.md` (ForSt repo).

**Build/test commands (run from `flink-state-backends/flink-statebackend-forst-rs`):**
- Unit tests (native): `JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home ../../mvnw -o test -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Denforcer.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true`
- Single test: append `-Dtest=ClassName`
- Bench jar deploy: `bash /Users/lijunqing/Code/stczwd/ForSt/scripts/run-8c32g.sh jar` (does `mvn clean package` — ALWAYS clean, the incremental stale-jar trap is documented)
- Bench run: `EVENTS_NUM=... bash /Users/lijunqing/Code/stczwd/ForSt/scripts/run-8c32g.sh run <q> forst-rs-ffm-local <maxsec> <tag>`

---

### Task 1: Per-classifier buffer ownership in VectorizedExecutor

Today `createRequestContainer()` (VectorizedExecutor.java:304-329) lazily creates ONE
`VectorizedClassifier` wired to the EXECUTOR-owned buffer quartet (`getKeys`, `putKeys`,
`putValues`, `deleteKeys`) and `reset()`s it per batch. Two outstanding containers would
clobber each other (documented at :371-379). This task gives each classifier its own
buffer quartet, allocated from the executor's arena at classifier construction.

**Files:**
- Modify: `src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java:304-332`
- Test: `src/test/java/org/apache/flink/state/forstrs/VectorizedExecutorContainerPoolTest.java` (create)

- [ ] **Step 1: Write the failing test**

```java
/*
 * (standard ASF license header — copy from VectorizedExecutor.java)
 */
package org.apache.flink.state.forstrs;

import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequest;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * PR-1 (coordinated executor): two outstanding containers must be DISTINCT objects with
 * distinct buffer sets, so batch N+1 can be filled while batch N executes on a worker.
 * After {@link VectorizedExecutor#releaseRequestContainer}, the instance is pooled and
 * reused.
 */
class VectorizedExecutorContainerPoolTest extends ForStRsNativeTestBase {

    @Test
    void twoOutstandingContainersAreDistinct() throws Exception {
        try (TestDb db = openTestDb()) {
            VectorizedExecutor ex = db.newExecutor();
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> a = ex.createRequestContainer();
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> b = ex.createRequestContainer();
            assertNotSame(a, b, "second container while first outstanding must be a new instance");
        }
    }

    @Test
    void releasedContainerIsReused() throws Exception {
        try (TestDb db = openTestDb()) {
            VectorizedExecutor ex = db.newExecutor();
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> a = ex.createRequestContainer();
            ex.releaseRequestContainer(a);
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> b = ex.createRequestContainer();
            assertSame(a, b, "released container must be pooled and reused");
        }
    }
}
```

Note: if the module has no `ForStRsNativeTestBase`/`TestDb` helper, copy the
open-db scaffolding from `VectorizedExecutorTest` (same package) — it already
constructs a `VectorizedExecutor` against a temp-dir native DB. Keep the test
gated the same way existing native tests are (they skip when
`forstrs.native.libpath` is absent).

- [ ] **Step 2: Run it to verify it fails**

Run: `../../mvnw -o test -Dtest=VectorizedExecutorContainerPoolTest -Dforstrs.native.libpath=... `
Expected: COMPILE FAILURE — `releaseRequestContainer` does not exist (and
`twoOutstandingContainersAreDistinct` would fail with `assertNotSame` once it compiles).

- [ ] **Step 3: Implement pool + per-classifier buffers**

In `VectorizedExecutor.java`, replace the single `pooledClassifier` (line 332) and
`createRequestContainer()` body (lines 304-329) with:

```java
    /** PR-1: pool of classifiers, each owning its own buffer quartet (depth>1 safe). */
    private final java.util.ArrayDeque<VectorizedClassifier> classifierPool =
            new java.util.ArrayDeque<>(4);

    @Override
    public AsyncRequestContainer<StateRequest<?, ?, ?, ?>> createRequestContainer() {
        VectorizedClassifier classifier = classifierPool.pollFirst();
        if (classifier == null) {
            // Each classifier owns a PRIVATE buffer quartet so two outstanding batches
            // never share fill-side state. Sizes mirror the executor-level buffers.
            ColumnarBatchBuffer cGetKeys = newKeyBuffer();
            ColumnarBatchBuffer cPutKeys = newKeyBuffer();
            ColumnarBatchBuffer cPutValues = newValueBuffer();
            ColumnarBatchBuffer cDeleteKeys = newKeyBuffer();
            classifier = new VectorizedClassifier(cGetKeys, cPutKeys, cPutValues, cDeleteKeys);
            classifier.initNewKindBuffers(arena);
        }
        classifier.reset();
        for (String name : listStateNames) {
            classifier.registerListState(name);
        }
        return classifier;
    }

    /** Returns a batch's classifier to the pool after its execution fully completes. */
    public void releaseRequestContainer(AsyncRequestContainer<StateRequest<?, ?, ?, ?>> c) {
        if (c instanceof VectorizedClassifier vc) {
            classifierPool.addLast(vc);
        }
    }
```

`newKeyBuffer()`/`newValueBuffer()` are small private helpers that allocate a
`ColumnarBatchBuffer` from `arena` with the same initial capacities the executor uses
for its existing `getKeys`/`putValues` fields (read the field initializers near the
constructor and reuse those constants — do not invent new sizes). The executor-level
quartet fields stay for now (execution-side code reads row data via the classifier
passed to `executeBatchRequests`; verify with grep that `executeBatchRequests` reads
buffers via the container argument — `grep -n "getKeys\|putKeys" VectorizedExecutor.java`
— and for any site that reads the EXECUTOR fields during execution, change it to read
the classifier's buffers instead; the GET OUTPUT segments `outOffsets/outValidity/outData`
stay executor-owned — execution is serialized per worker thread so output reuse is safe).

Thread-safety note: `createRequestContainer`/`releaseRequestContainer` are called from
the mailbox thread (inline mode) or from coordinator/worker completion (Task 3); the
pool is confined to one caller thread per executor instance in BOTH modes (each worker's
executor is only touched by its routing container + its worker thread completion, which
Task 3 serializes through the worker's single thread). Add this as a javadoc on the pool.

- [ ] **Step 4: Run the test again — both tests pass**

Run: same command. Expected: PASS (2/2).

- [ ] **Step 5: Run the FULL native suite (the depth-1 inline path must be unaffected)**

Run: full `mvnw -o test -Dforstrs.native.libpath=...`
Expected: all green (528+ tests; inline mode still create→execute→implicit-release —
inline mode never calls release, the pool simply grows to 1 entry, same as before).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java \
        src/test/java/org/apache/flink/state/forstrs/VectorizedExecutorContainerPoolTest.java
git commit -m "feat(forst-rs): per-classifier buffer ownership + classifier pool (depth>1 prerequisite)"
```

---

### Task 2: CoordinatedStateExecutor — non-blocking, key-group-affine, real fullyLoaded

**Files:**
- Create: `src/main/java/org/apache/flink/state/forstrs/exec/CoordinatedStateExecutor.java`
- Test: `src/test/java/org/apache/flink/state/forstrs/exec/CoordinatedStateExecutorTest.java` (create)
- Reference (do not modify): `exec/RoutingStateExecutor.java` — reuse its routing container verbatim.

- [ ] **Step 1: Write the failing test**

```java
/* (ASF header) */
package org.apache.flink.state.forstrs.exec;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PR-1 contract tests for the non-blocking coordinated executor. Uses the same native
 * test scaffolding as RoutingStateExecutor's tests (grep for its test class; if none
 * exists, build on VectorizedExecutorTest's TestDb helper).
 *
 * The three contracts that distinguish this executor from RoutingStateExecutor:
 *  1. executeBatchRequests returns BEFORE the batch completes (incomplete future).
 *  2. fullyLoaded() flips true while >= workerCount() sub-batches are outstanding.
 *  3. The future completes (successfully) without the caller thread doing any work.
 */
class CoordinatedStateExecutorTest extends ForStRsNativeTestBase {

    @Test
    void batchFutureIsIncompleteAtReturnAndCompletesAsync() throws Exception {
        try (TestHarness h = openHarness()) {
            // Build a container with one slow-ish GET batch (use the harness helper that
            // offers N MAP_GET StateRequests with distinct key-groups).
            var container = h.executor().createRequestContainer();
            h.offerGets(container, /*count=*/ 256);
            CompletableFuture<Void> f = h.executor().executeBatchRequests(container);
            // Contract 1: must NOT be the completedFuture-inline pattern.
            // (A fast batch may legitimately complete quickly; assert on the FIRST
            // nanosecond only via a latch the harness injects in the worker — see
            // harness helper blockWorkers()/unblockWorkers() below.)
            assertFalse(h.workersWereEnteredOnCallerThread(),
                    "batch must execute on worker threads, not the caller thread");
            f.get(30, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    @Test
    void fullyLoadedReflectsOutstandingBatches() throws Exception {
        try (TestHarness h = openHarness()) {
            h.blockWorkers(); // workers park before executing
            for (int i = 0; i < h.executor().workerCount(); i++) {
                var c = h.executor().createRequestContainer();
                h.offerGets(c, 8); // distinct key-groups → lands on worker i
                h.executor().executeBatchRequests(c);
            }
            assertTrue(h.executor().fullyLoaded(), "all workers busy => fullyLoaded");
            h.unblockWorkers();
            h.awaitQuiesce();
            assertFalse(h.executor().fullyLoaded(), "drained => not fullyLoaded");
        }
    }
}
```

The `TestHarness` is a small inner/static helper in the test file: it opens the native
test DB, constructs `CoordinatedStateExecutor` with `workerCount()` workers, exposes
`offerGets` (builds real `StateRequest` mocks the way existing executor tests do — copy
from the existing RoutingStateExecutor/VectorizedExecutor test utilities), and
implements `blockWorkers()` by submitting a latch-wait runnable to each worker thread
via a test hook `CoordinatedStateExecutor#submitToWorkerForTest(int, Runnable)`.
`workersWereEnteredOnCallerThread` records the executing thread name in a test hook —
worker threads are named `forst-rs-state-worker-*`.

- [ ] **Step 2: Run to verify it fails**

Expected: COMPILE FAILURE — `CoordinatedStateExecutor` does not exist.

- [ ] **Step 3: Implement CoordinatedStateExecutor**

Create `exec/CoordinatedStateExecutor.java`. It is `RoutingStateExecutor` with three
surgical changes (copy the class, then apply; keep the worker/arena construction,
`RoutingRequestContainer`, `executeRequestSync`, and `shutdown` IDENTICAL except
renames):

```java
/* (ASF header) */
package org.apache.flink.state.forstrs.exec;

// imports as RoutingStateExecutor, plus:
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PR-1 coordinated executor (ForSt ForStStateExecutor model, FFM-adapted):
 * key-group-affine routing (same container as RoutingStateExecutor) + NON-BLOCKING
 * dispatch — executeBatchRequests returns an INCOMPLETE future immediately and the
 * mailbox thread never waits — + REAL fullyLoaded() accounting so the AEC stops
 * admitting records when every worker has a batch outstanding.
 *
 * Deadlock-freedom vs the 2026-06-09 async-offload attempt: that version BLOCKED the
 * mailbox in leaseWorker() when no worker was free (mailbox waits for worker; worker
 * completion needs mailbox → cycle). Here NOTHING on the mailbox path blocks:
 * createRequestContainer grows the per-worker classifier pool instead of waiting
 * (Task 1), and admission is bounded by fullyLoaded(), which the AEC polls without
 * blocking. Completion flows worker → per-row future callbacks (CallbackRunnerWrapper
 * queues onto the mailbox) → AEC accounting; the batch future completes on the worker.
 */
public final class CoordinatedStateExecutor implements StateExecutor {

    private final VectorizedExecutor[] workers;
    private final ExecutorService[] workerThreads;
    private final Arena[] workerArenas;
    /** Outstanding sub-batches across all workers (fullyLoaded accounting). */
    private final AtomicInteger ongoing = new AtomicInteger();
    private volatile boolean shutdown = false;

    public static int workerCount() { /* identical to RoutingStateExecutor.workerCount() */ }

    public CoordinatedStateExecutor(
            ForStRsLinker linker, FrsDb db, FrsCfHandle cf,
            DispatchMetrics metrics, java.util.function.Consumer<VectorizedExecutor> register) {
        // identical body to RoutingStateExecutor's constructor
    }

    @Override
    public AsyncRequestContainer<StateRequest<?, ?, ?, ?>> createRequestContainer() {
        return new RoutingRequestContainer(); // copy the inner class verbatim
    }

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container) {
        RoutingRequestContainer rc = (RoutingRequestContainer) container;
        final var subs = rc.subs;
        int n = 0;
        for (var s : subs) {
            if (s != null && !s.isEmpty()) {
                n++;
            }
        }
        if (n == 0) {
            return CompletableFuture.completedFuture(null);
        }
        final CompletableFuture<Void> result = new CompletableFuture<>();
        final AtomicInteger remaining = new AtomicInteger(n);
        final java.util.concurrent.atomic.AtomicReference<Throwable> err =
                new java.util.concurrent.atomic.AtomicReference<>();
        for (int i = 0; i < subs.length; i++) {
            final var sub = subs[i];
            if (sub == null || sub.isEmpty()) {
                continue;
            }
            final int id = i;
            ongoing.incrementAndGet();
            workerThreads[id].execute(
                    () -> {
                        try {
                            // VectorizedExecutor's FFI is synchronous: this future is
                            // complete when the call returns.
                            CompletableFuture<Void> inner =
                                    workers[id].executeBatchRequests(sub);
                            Throwable t = inner.handle((v, e) -> e).getNow(null);
                            if (t != null) {
                                err.compareAndSet(null, t);
                            }
                        } catch (Throwable t) {
                            err.compareAndSet(null, t);
                        } finally {
                            workers[id].releaseRequestContainer(sub); // Task 1 API
                            ongoing.decrementAndGet();
                            if (remaining.decrementAndGet() == 0) {
                                Throwable e = err.get();
                                if (e != null) {
                                    result.completeExceptionally(e);
                                } else {
                                    result.complete(null);
                                }
                            }
                        }
                    });
        }
        return result; // INCOMPLETE — mailbox continues immediately
    }

    @Override
    public void executeRequestSync(StateRequest<?, ?, ?, ?> request) {
        // identical to RoutingStateExecutor (route by key-group, block — sync requests
        // are rare and MUST observe that worker's prior batches, which the worker's
        // single thread serializes).
    }

    @Override
    public boolean fullyLoaded() {
        return ongoing.get() >= workers.length;
    }

    @Override
    public void shutdown() { /* identical to RoutingStateExecutor.shutdown() */ }

    // RoutingRequestContainer inner class: copy verbatim from RoutingStateExecutor.
    // Test hooks (package-private): submitToWorkerForTest(int, Runnable), workerCountInstance().
}
```

IMPORTANT correctness detail to preserve while copying: the per-worker single-thread
executor serializes sub-batches per worker IN SUBMISSION ORDER, which (with key-group
affinity) preserves cross-batch per-key-group order. Do not replace the per-worker
single-thread executors with a shared pool.

- [ ] **Step 4: Run the two contract tests — PASS**

- [ ] **Step 5: Run the full native suite — all green**

- [ ] **Step 6: Commit**

```bash
git add src/main/java/org/apache/flink/state/forstrs/exec/CoordinatedStateExecutor.java \
        src/test/java/org/apache/flink/state/forstrs/exec/CoordinatedStateExecutorTest.java
git commit -m "feat(forst-rs): CoordinatedStateExecutor — non-blocking key-group-affine dispatch + real fullyLoaded"
```

---

### Task 3: Backend gate — three-way env switch, coordinated NOT yet default

**Files:**
- Modify: `src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAsyncKeyedStateBackend.java`
  (the existing OPT-01 gate block, ~lines 1137-1160 — grep `FRS_RS_PARALLEL_EXECUTOR`)
- Modify: `src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java`
  (the `DISABLE_MAPSTATE_CACHE` coupling, ~lines 115-129 — grep `FRS_RS_PARALLEL_EXECUTOR`)

- [ ] **Step 1: Replace the gate**

Replace the body of the existing `FRS_RS_PARALLEL_EXECUTOR` gate with a three-way
selector (keep the surrounding construction code; only the selection changes):

```java
        // PR-1 executor selection:
        //   FRS_RS_EXECUTOR=coordinated → CoordinatedStateExecutor (non-blocking; target default)
        //   FRS_RS_EXECUTOR=routing     → RoutingStateExecutor (the measured opt-in spike)
        //   FRS_RS_EXECUTOR=inline      → depth-1 VectorizedExecutor (kill switch)
        // Back-compat: FRS_RS_PARALLEL_EXECUTOR=1 (old opt-in) == routing.
        // Default REMAINS inline until the Task 5 benchmark gates pass; Task 6 flips it.
        String mode = System.getenv("FRS_RS_EXECUTOR");
        if (mode != null) {
            mode = mode.trim();
        }
        if (mode == null || mode.isEmpty()) {
            String legacy = System.getenv("FRS_RS_PARALLEL_EXECUTOR");
            mode = (legacy != null && legacy.trim().equals("1")) ? "routing" : "inline";
        }
```

and construct the matching executor (`coordinated` mirrors the existing `routing`
construction with `new CoordinatedStateExecutor(...)`).

In `ForStRsMapStateV2`, extend the cache-off coupling so the cache is also disabled
under `coordinated` (same reason as routing — the shared single-threaded cache races
under any parallel executor; per-worker cache is PR-2):

```java
    // cache disabled when ANY parallel executor is active (routing or coordinated)
    private static boolean parallelExecutorActive() {
        String m = System.getenv("FRS_RS_EXECUTOR");
        if (m != null) {
            String t = m.trim();
            if (t.equals("coordinated") || t.equals("routing")) {
                return true;
            }
        }
        String legacy = System.getenv("FRS_RS_PARALLEL_EXECUTOR");
        return legacy != null && legacy.trim().equals("1");
    }
```

and OR it into the existing `DISABLE_MAPSTATE_CACHE` initializer.

- [ ] **Step 2: Run full native suite — green (default unchanged = inline)**

- [ ] **Step 3: Commit**

```bash
git add src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAsyncKeyedStateBackend.java \
        src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java
git commit -m "feat(forst-rs): three-way executor gate (inline/routing/coordinated), cache-off under any parallel mode"
```

---

### Task 4: Push + GHA green (both repos' pipelines unaffected by default)

- [ ] **Step 1:** `git push` (flink fork `jackylee-ch/flink`, branch `forst-rs-jdk25`).
- [ ] **Step 2:** `gh run list -R jackylee-ch/flink --branch forst-rs-jdk25` → wait for
  `ci-forst-rs` green. If red: fix before any benchmarking.

---

### Task 5: Benchmark gates (coordinated, opt-in) — all on 8c/32g, recorded in sweep doc

Run with `FRS_RS_EXECUTOR=coordinated`, full 100M unless noted. Gates (ALL must hold
before flipping the default; record every number in
`docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md`):

- [ ] **q8 correctness band:** out_rows = 3,064,445 ± noise band (3,010,888-3,065,000
  observed band). Command: `FRS_RS_EXECUTOR=coordinated bash scripts/run-8c32g.sh run q8 forst-rs-ffm-local 400 pr1-q8`
- [ ] **q17 no-robbing:** ≤ 85s (depth-1 was 76.7-77.7s; the routing+synchronized-cache
  default made it 270s — THE regression this design exists to avoid).
- [ ] **q11:** ≤ 140s and out_rows exact (92M-row band as recorded 2026-06-09).
- [ ] **q20:** finishes ≤ 1610s with out_rows = 93,201,404.
- [ ] **q9 no-regress:** src_out at t=1200s ≥ 84M (depth-1 reached 84.5M@1204).
- [ ] **q5dbg canary:** out_rows = 60,218 at EVENTS_NUM=1000000 (windowed-agg sanity).

If a gate fails: STOP, root-cause, fix, re-run the failed gate + q8 before continuing.

---

### Task 6: Flip the default + full re-verify

- [ ] **Step 1:** In the Task 3 gate, change `mode = "inline"` default to
  `mode = "coordinated"` (kill-switch stays: `FRS_RS_EXECUTOR=inline`).
- [ ] **Step 2:** Full native suite green; commit
  `feat(forst-rs): coordinated executor DEFAULT (kill-switch FRS_RS_EXECUTOR=inline)`; push; GHA green.
- [ ] **Step 3:** Re-run the Task 5 gate set WITHOUT the env var (proving the default
  path) + q3 (light-query no-regress, ≤ 37s). Record in the sweep doc.
- [ ] **Step 4:** Update `docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md`
  CURRENT STATUS table rows for q7/q9/q11/q17/q20 and commit the doc.

---

## Explicitly OUT of scope (PR-2+, gate-driven)
- Worker-confined MapStateCache shards (only if q12/q19 regress under cache-off default —
  q11 passed at 134.7s WITH cache off, so the cache may matter less than assumed).
- Engine read-path lever (Plan B — blocked on the FRS_READ_AT_DIAG discriminator run).
- Worker-side key serialization (today's offer() fills classifier buffers on the mailbox;
  moving serialization to workers needs request-retention in the container — measure first).
