# Round 4 — Agent E — Flink Real-Time Streaming

**Date:** 2026-05-22
**Method:** read-only verification of the 27-PR landing against Section 5 + E3-HIGH-1..5 in the prior rounds. Spot-checks against Flink 2.2 `CheckpointOptions` / `SavepointType` / `TypeSerializerSnapshot` contracts and `community ForSt` equivalents.
**Scope:** `flink-statebackend-forst-rs/src/main/java/` + `crates/forst-rs-io/src/opendal_backend.rs`.

---

## Summary

| Sev    | Count |
|--------|-------|
| HIGH   | 2     |
| MEDIUM | 1     |

All five **E3-HIGH-1..5** findings from Round 3 are CLOSED. Two NEW HIGH issues and one MEDIUM surfaced.

---

## Per-PR verification

| PR     | Topic                                 | Status   | Evidence (file:line)                                                                                                                              |
|--------|---------------------------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| A1     | snapshot rewrite                      | PASS     | `ForStRsAsyncKeyedStateBackend.java:796-812` → `ensureSnapshotStrategy()` + `SnapshotStrategyRunner.snapshot(...)`; no more naked `SnapshotResult.empty()` on the happy path. `notifyCheckpointAborted` releases via `takePendingRegistrations(id)` + `reg.unregister(...)` (`:910-917`). |
| A4     | timer kg supplier                     | PASS     | `ForStRsKeyGroupedInternalPriorityQueue.java:341` reads `keyContext.getCurrentKey()` (was `null` in Round 1).                                     |
| A6     | cache clear on CLEAR                  | PASS     | `ForStRsMapStateV2.java:455-468`: `StateRequestType.CLEAR` invokes both `cache.clearForPrefix(prefix)` and `offHeapBuf.clearForPrefix(...)`.     |
| A7     | TTL (VALUE only)                      | PASS     | `ForStRsAsyncKeyedStateBackend.java:454-500`: TTL wired via `desc.getTtlConfig().isEnabled()`; MAP/LIST/REDUCING/AGGREGATING throw **before** state is constructed (`:490-500`) — *no corruption possible* since the throw fires inside `createTtlAwareStateInternal` before any FFI write. |
| A8     | stop-savepoint sync                   | PASS     | `ForStRsAsyncKeyedStateBackend.java:735-737, 803-804`: `((SavepointType) ctype).isSynchronous()` → `SnapshotExecutionType.SYNCHRONOUS`.            |
| A9     | CheckpointOptions branching           | PASS     | `:735-737`: reads `o.getCheckpointType()`, branches `isSavepoint`/`isSync`.                                                                       |
| A11    | StateSerializerRegistry               | PASS     | `:447-448` calls `verifyOrRegister(stateId, ordinal, serializer)`; `StateSerializerRegistry.java:166-204` invokes `resolveSchemaCompatibility`, throws `StateMigrationException` on INCOMPATIBLE. |
| A12    | retry layer                           | PASS     | Rust: `crates/forst-rs-io/src/opendal_backend.rs:203` `default_retry_layer()` applied at op construction (`:228`). Java: `ForStRsSstUploader.java:66-78` + `ForStRsRestoreOperation.java:285-307` route through `SstRetryStrategy`. |
| E1     | parallel restore                      | PASS     | `ForStRsRestoreOperation.java:730-803` `parallelDownloadSsts` + bounded `newRestoreExecutor(parallelism)`; `copyKeyGroup` batches through `vectorizedBatchPut` (`:534, 556-628`). |
| E2     | async parallelism observability       | PASS     | `DispatchMetrics.java:63-111`: `in_flight_batches` gauge + `in_flight_batch_depth` histogram; structural runtime change deferred with documented contract. |

E3-HIGH-1, -2, -3, -4, -5 all closed. Section 5 verified.

---

## NEW findings

### E4-HIGH-1 — Savepoint emits a non-portable incremental handle without a guard

**File:** `ForStRsAsyncKeyedStateBackend.java:735-812`.
**Evidence:** When `o.getCheckpointType().isSavepoint()` is true the code labels the runner `"ForStRs-async-savepoint"` and still routes through `ForStRsSnapshotStrategy` → `ForStRsIncrementalKeyedStateHandle`. The javadoc at `:712-719` correctly *notes* "emitting canonical Flink savepoint format … is a follow-on PR", but no `LOG.warn` / metric / option-gate fires at runtime — operators running `bin/flink savepoint` get a "successful" non-portable blob and the deferral is invisible.
**Impact:** matches the original E3-HIGH-1 streaming impact (cross-cluster migration impossible; SHARED-scope SSTs may be GC'd at savepoint cutover). The fix from Round 3 was applied to mechanics (sync drain, options branching) but the user-visible "savepoint not yet portable" warning is missing.
**Fix shape:** `if (isSavepoint) LOG.warn("forst-rs savepoint emits incremental handle; restore is forst-rs-only — canonical format pending");` *and* a `state.forst-rs.savepoint.strict-canonical` config (default false) that throws `UnsupportedOperationException` so production jobs opt-in to fail-loud.
**Certainty:** HIGH.

### E4-HIGH-2 — `parallelDownloadSsts` does not await termination on exception; in-flight downloads continue after failed restore

**File:** `ForStRsRestoreOperation.java:799-801`.
**Evidence:** `finally { dl.shutdown(); }` — graceful shutdown only stops *accepting* new tasks; already-submitted SST downloads keep running on daemon threads. If the first future throws `ForStRsCheckpointRestoreException`, the remaining N-1 downloads still consume S3 bandwidth and write into `downloadDir`. No `dl.shutdownNow()` on exception, no `dl.awaitTermination(...)`.
**Impact:** on a failed restore (e.g. one corrupt SST), the JM cancels the task, but the restore thread pool keeps copying gigabytes of SSTs into a directory the cleanup hook may already be deleting → races, half-written files, S3 cost. In a restart-loop this multiplies.
**Fix shape:** on exception in the `futures.get(i).get()` loop, before re-throw, call `dl.shutdownNow()` and `dl.awaitTermination(5, SECONDS)`. Also cancel pending futures (`for (Future f : futures) f.cancel(true);`).
**Certainty:** HIGH.

### E4-MED-1 — `snapshot()` swallows all non-"registry closed" IOExceptions as `SnapshotResult.empty()`

**File:** `ForStRsAsyncKeyedStateBackend.java:822-827`.
**Evidence:** Both the generic `IOException` catch (`:822-824`) and the broad `Exception` catch (`:825-827`) return `DoneFuture.of(SnapshotResult.empty())`. The original intent (the comment at `:817-818`) was only for "registry already closed" during task cancellation; the implementation collapses *all* failures to an empty result. Real upload failures, FFI crashes, OOMs in the strategy — all become silent "empty" success from the coordinator's POV.
**Impact:** the checkpoint coordinator sees the checkpoint succeed with no state handle and advances `lastCompletedCheckpointId`. On restart, the job restores from the *prior* checkpoint, but no `tolerable-failed-checkpoints` counter increments → silent data loss masquerading as healthy ckpt completion.
**Fix shape:** narrow the catch to only the "registry is already closed" path returning empty; for all other exceptions, return `DoneFuture.ofFailure(e)` so the coordinator sees the failure and respects the tolerance budget.
**Certainty:** MEDIUM (could be HIGH depending on operator policy).

---

## Confirmation: no NEW issues on

- **`notifyCheckpointAborted` ref counts** — fully released via `takePendingRegistrations(id)` + `reg.unregister(...)`. (`:910-917`)
- **`notifyCheckpointSubsumed` no-op** — matches community ForSt's `LOG.info(...)`-only impl (`flink-statebackend-forst/.../ForStKeyedStateBackend.java:561-563`); not a regression.
- **TTL throw timing** — happens inside `createTtlAwareStateInternal` *before* the inner `ForStRsValueStateV2` is constructed or registered (`ForStRsAsyncKeyedStateBackend.java:490-500`); zero engine writes possible on the throw path.
- **PHASE 1–3 drain on snapshot** — `:740-788` runs flushDirty / off-heap buffer / RMW / timer drains + a second flushDirty before strategy snapshot. Correctly ordered.
