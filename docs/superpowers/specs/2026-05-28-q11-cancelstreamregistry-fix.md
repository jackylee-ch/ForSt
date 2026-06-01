# V1-sync cancelStreamRegistry lifetime fix (q11 correctness)

**Date:** 2026-05-28
**Scope:** `flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAbstractKeyedStateBackend.java`
**Severity:** HIGH — silent state loss on every checkpoint of every V1-sync backend.

## Root cause

The V1-sync `ForStRsAbstractKeyedStateBackend` received its `cancelStreamRegistry` via constructor parameter (originating from `KeyedStateBackendParameters.getCancelStreamRegistry()`) and used it directly for in-flight checkpoint cancellation. That registry is **restore-lifetime-only**, not backend-lifetime.

Evidence from Flink source (`StreamTaskStateInitializerImpl.java:457-495`):

```java
// Line 457: Flink creates a RESTORE-ONLY registry
CloseableRegistry cancelStreamRegistryForRestore = new CloseableRegistry();
backendCloseableRegistry.registerCloseable(cancelStreamRegistryForRestore);

// Line 475: passes it via KeyedStateBackendParameters to backend construction
KeyedStateBackendParametersImpl<K> parameters = new KeyedStateBackendParametersImpl<>(
    ...,
    cancelStreamRegistryForRestore,   // <- this is what parameters.getCancelStreamRegistry() returns
    ...);
return keyedStateBackendCreator.create(...);

// Lines 491-494: Flink CLOSES IT IMMEDIATELY after restore completes
finally {
    if (backendCloseableRegistry.unregisterCloseable(cancelStreamRegistryForRestore)) {
        IOUtils.closeQuietly(cancelStreamRegistryForRestore);
    }
}
```

So the registry passed to our backend is closed by Flink the moment `createAndRestore(...)` returns. Any backend that holds onto it for the operator's lifetime is using a dead resource.

The V2-async `ForStRsAsyncKeyedStateBackend.java:266` already handled this correctly:

```java
private final CloseableRegistry cancelStreamRegistry = new CloseableRegistry();  // own private registry
```

But V1-sync `ForStRsAbstractKeyedStateBackend` was missing this — it stored only the Flink-provided restore-only registry.

## Symptom

`ForStRsAbstractKeyedStateBackend.snapshot()` had a defensive check:

```java
if (cancelStreamRegistry.isClosed()) {
    LOG.info("Checkpoint {} skipped — cancelStreamRegistry already closed");
    return DoneFuture.of(SnapshotResult.empty());
}
```

This was intended as a graceful fallback for the "task being cancelled" race. But because the registry was the restore-only one, the check **always tripped on every checkpoint of every V1-sync backend**. Result: `SnapshotResult.empty()` on every checkpoint → no state written → silent state loss.

Downstream consequence for q11 (V1-sync SESSION window):
1. Operator runs normally, MergingWindowSet HashMap accumulates `Window → stateWindow` mappings.
2. Operator writes per-window changes to `mergingWindowsState` (ReducingState).
3. Checkpoint 1 trigger fires → `snapshot()` returns empty → state isn't persisted.
4. Task fails for some downstream reason (or watermark advances on stale state).
5. Operator restarts from empty state.
6. Old timers (registered before restart) fire after restart for windows the new MergingWindowSet HashMap doesn't know about.
7. `MergingWindowSet.retireWindow` throws `IllegalStateException: Window ... is not in in-flight window set`.

q11 reproducibly hit this with `errors=4` across all 4 parallel writers.

## Diagnostic methodology

Three-step diagnostic landed during the investigation:

1. **WARN + 7-frame stack at the skip site** — confirmed Flink's `RegularOperatorChain.snapshotState` was the legitimate caller; no rogue close path inside forst-rs.
2. **Tripwire `Closeable` registered against the registry** — caught the actual closer via `Thread.currentThread().getStackTrace()` inside the closing thread. Stack pointed to `StreamTaskStateInitializerImpl.keyedStatedBackend(:493)` — Flink's restore-cleanup `IOUtils.closeQuietly`.
3. **`cancel(boolean)` wrapper log** — verified our wrapper's `cancel()` is NOT called during the failure (rules out backend-internal cancellation as the cause).

Each tool added incremental certainty before changing semantics.

## Fix

Add a backend-owned registry, mirroring V2-async:

```java
private final CloseableRegistry backendCancelStreamRegistry = new CloseableRegistry();
```

Use it for in-flight snapshot cancellation in `snapshot()`:

```java
snap = new SnapshotStrategyRunner<>(
        "ForStRs-incremental-snapshot",
        snapshotStrategy,
        backendCancelStreamRegistry,   // was: cancelStreamRegistry (the restore-only one)
        SnapshotExecutionType.ASYNCHRONOUS)
    .snapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
```

Update the closed-registry guard to check the new field:

```java
if (backendCancelStreamRegistry.isClosed()) {
    LOG.info("Checkpoint {} skipped — backend-owned cancelStreamRegistry closed (backend tearing down).", checkpointId);
    return DoneFuture.of(SnapshotResult.empty());
}
```

Close in our `close()` after `delegate.close()`:

```java
try {
    backendCancelStreamRegistry.close();
} catch (IOException ignored) { /* best-effort */ }
```

The Flink-provided constructor parameter is still passed to `super(...)` (so Flink's restore-time semantics still work via the parent), but we no longer use it for checkpoint-time cancellation.

## Verification

Before fix:
- Every V1-sync checkpoint logged `"Checkpoint N skipped — cancelStreamRegistry already closed"`.
- q11 finished fast (76.5s baseline, then 246s with `errors=4`) — fast precisely because no actual checkpointing happened.

After fix:
- No more "skipped — cancelStreamRegistry already closed" logs.
- Checkpoint actually runs and writes to S3 → q11 doesn't finish within the 370s early-break threshold (real checkpointing is genuine slow work for SESSION-window state).
- A SEPARATE second root cause for the Window error remains under investigation (see below).

## Files changed

- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAbstractKeyedStateBackend.java`:
  - Added field `backendCancelStreamRegistry` (line ~244, after `asyncExecutor` declaration).
  - `snapshot()` uses the new field at the `SnapshotStrategyRunner` construction site.
  - `snapshot()` catch-handler checks the new field.
  - `close()` closes the new field after `delegate.close()`.

## Still open after this fix

Even with cancelStreamRegistry correctly scoped, q11 still produces `errors=4` with the same `MergingWindowSet.retireWindow` IllegalStateException. The new pattern has NO preceding `CancellationException` (our cancel-wrapper diagnostic confirmed `cancel()` is never called).

Hypothesis: V1-sync `ForStRsReducingState` (backing `mergingWindowsState`) returns torn/stale reads after some recent engine change. The operator's `MergingWindowSet.HashMap`, reconciled on operator init from `mergingWindowsState`, drifts from what timer-fire expects.

Suspect commits:
- `df4108abd fix(forst-rs): harden vectorized consistency paths`
- `6fcd844a0 fix(forst-rs): harden engine consistency paths`

q11 baseline `errors=0` 2026-05-19 vs current `errors=4` 2026-05-28 → a recent change broke it. **Next-session work: bisect over the engine-side commits between 2026-05-19 and 2026-05-28 to find which one broke V1-sync ReducingState read consistency.**

## Cross-refs

- [[project_q11_correctness_regression_2026-05-28]]
- [[project_q11_v1_sync_finding]]
- [[project_q11_always_on_buffer_win]]
- Spec: `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md`
