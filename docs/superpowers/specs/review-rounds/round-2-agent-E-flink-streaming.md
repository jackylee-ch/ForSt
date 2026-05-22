# Round 2 — Agent E — Flink Real-Time Streaming Features

**Date:** 2026-05-22
**Angle:** new Flink-streaming-semantic issues missed by Round 1 — watermark propagation, async-state V2 in-flight parallelism, window-cleanup-after-barrier, TTL hooks, recovery-RTO.
**Method:** focused cold-read with Round 1 docket excluded. Cross-checked V2 state path (`ForStRsAsyncKeyedStateBackend`, `ForStRs*V2`) against Flink's `InternalTimerServiceImpl`, `AsyncExecutor` contract, and `AbstractKeyedState` semantics.

---

## Summary of severities

| Sev | Count |
|---|---|
| CRIT | 2 |
| HIGH | 3 |
| MED  | 2 |

R1 NOTE: confirmed `pickTimerFactory()` default is **FORSTRS** (Round 1 E-HIGH-6 was filed as a default-mismatch; brief flags it false-positive on the basis that the FORSTRS batched-off-heap variant from commit a5fd9f70dd6 is the proven 1.x win). My new findings BELOW affect FORSTRS variant correctness independently — the win measurement was on a fixed-supplier-startKg=0 single-task path that does not exercise multi-kg event-time draining.

---

## CRIT findings

### E2-CRIT-1 — V2 keyed state encodes engine keys WITHOUT namespace; cross-window state collision

**Files (all V2 state classes):**
- `flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueStateV2.java:72-105`
- `…/state/ForStRsMapStateV2.java:114-130, 173-201`
- `…/state/ForStRsAsyncListStateV2.java:98-128`
- `…/state/ForStRsAsyncReducingStateV2.java:88-104`
- `…/state/ForStRsAsyncAggregatingStateV2.java:91-108`

**Evidence:**

```java
// ForStRsValueStateV2.serializeKey:
//   composite = KEY_PREFIX || serialize(K) || "/" || stateName || "/"
//   NAMESPACE is not part of the composite at all.
```

Every V2 serializeKey / serializeKeyInto path encodes `KEY_PREFIX || operatorKey || / || stateName || /[userKey]`. The `StateRequest<K, N, …>` `N` (namespace) parameter is type-only — `RecordContext.getKey()` returns the operator key but no namespace is read. `ForStRsAsyncKeyedStateBackend.getOrCreateKeyedState(ns, nsSer, desc)` (line 200) accepts `nsSer` but does NOT forward it to the state class constructors (lines 218-247). The `MapStateCache` cache key in `ForStRsMapStateV2.serializeMapEntryKey` is also namespace-blind (line 114-130).

Consequence on real Flink jobs:

1. Any operator that calls `state.setCurrentNamespace(window)` between accesses (every windowed operator — `WindowOperator`, `IntervalJoinOperator`, `TimerWindowOperator`) writes ALL namespaces into the SAME engine key. Subsequent reads return the value last written under any namespace.
2. Window cleanup (`state.clear()` on namespace=window_N) deletes ALL namespaces' state for that operator key. Next window opening for the same operator key sees an empty state (correct by accident) but the wrong reason — namespace isolation never existed.
3. Bench results for Q11/Q12 (session windows) happen to PASS because session-window assigner uses VoidNamespace + per-key state semantics where Flink's MergingWindowSet does its own namespace bookkeeping in a SEPARATE ListState — but a generic `TUMBLE/HOP` over a non-VoidNamespace operator state would silently corrupt.

The V1-sync path encodes via `ForStRsKeyGroupedSerializer.encodeForState(kg, K, stateName)` which ALSO appears not to include namespace — let me cross-check:

Looking at `ForStRsAbstractKeyedStateBackend.getOrEncodeKey(stateName, kgSerializer)` line 288 — only `(kg, K, stateName)` are passed; no namespace. So the V1-sync state classes inherit the same gap.

**Streaming impact:** any V2 windowed operator that depends on per-namespace isolation will silently corrupt. The fact that this hasn't blown up in Nexmark bench is masked by:
- Q11/Q12 sessions use VoidNamespace for MapState (each operator key gets its own MapState namespace via MergingWindowSet wrapper, not via FoldedKey + namespace encoding).
- Q3/Q4/Q5/Q8/Q15 (group-aggregates) use a single global namespace.

**Fix shape:** extend `serializeKey` / `serializeKeyInto` to read `request.getNamespace()` (or pull from `StateRequest.getNamespace()` if exposed; otherwise `ctx.getNamespace()` once the V2 framework wires it) and append a `serialize(N)` after `stateName + /`. Mirror the convention `RocksDBKeyedStateBackend` uses: `kg || serialize(K) || serialize(N) || stateName`.

**Certainty:** HIGH. Visible in the source; reproducible with any per-window TUMBLE aggregation on V2 backend.

---

### E2-CRIT-2 — FORSTRS engine-backed timer queue uses a CONSTANT key-group supplier; `peek()`/`poll()` only sees timers in `startKeyGroup`

**File:** `flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAsyncKeyedStateBackend.java:436`

```java
return new ForStRsKeyGroupedInternalPriorityQueue<>(
        linker, db, defaultCf, arena, n, s,
        element -> { … },
        () -> keyGroupRange.getStartKeyGroup(),   // ← CONSTANT, not dynamic
        keyGroupRange);
```

**Evidence:**

1. `ForStRsKeyGroupedInternalPriorityQueue.peek()` (PriorityQueue.java:441-509) reads `currentKeyGroupSupplier.getAsInt()` and scans ONLY timers prefixed with that kg.
2. Flink's `InternalTimerServiceImpl.tryAdvanceWatermark` (`flink-runtime/.../InternalTimerServiceImpl.java:328-347`) drives the queue with the strict contract:
   ```java
   while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
       keyContext.setCurrentKey(timer.getKey());  // setCurrentKey AFTER peek
       eventTimeTimersQueue.poll();
       triggerTarget.onEventTime(timer);
   }
   ```
   The runtime expects `peek()` to return the **global** minimum-ts timer across ALL key groups in the local range. Only after dequeueing does it `setCurrentKey`.
3. With the supplier hardcoded to `startKeyGroup`, peek/poll only return timers whose composite key starts with kg=startKeyGroup. Timers registered for any other kg in the operator's range are **invisible** to watermark advance.
4. The V1-sync abstract backend wires this correctly: `ForStRsAbstractKeyedStateBackend.createInternalPriorityQueue` line 554 passes `this::getCurrentKeyGroupIndex`. The V2 async backend's `create()` override (line 416-437) regressed.
5. `getSubsetForKeyGroup(int)` (PriorityQueue.java:640-653) takes the kg as an argument and is correct — so per-kg snapshot blob writes would work even with the broken supplier. The breakage is strictly on the firing path.

**Streaming impact:** event-time and processing-time timer firing on V2 backend with FORSTRS variant misses all but one kg of timers. Concretely: a subtask owning kgs [10, 11, 12, 13, 14] only fires timers for kg=10. Timers in kgs 11-14 accumulate forever in the engine. Q12 wall-clock bench (HEAP) doesn't exercise this path; FORSTRS variant of Q12 had 1.20× slowdown blamed on FFM cost in `project_q12_heap_timer_beats_forst` — actual root cause is probably this correctness bug producing degenerate timer-set sizes plus per-event FFM. Independent of perf, this is a correctness gap.

**Fix shape:** lambda → `delegate::getCurrentKeyGroupIndex` — but `ForStRsAsyncKeyedStateBackend` doesn't have a delegate. The async backend needs to track its own `InternalKeyContext` and pass `keyContext::getCurrentKeyGroupIndex` as the supplier. The same context is used by `setCurrentKey` calls Flink's runtime makes between peek and poll — so wiring is straightforward.

**Certainty:** HIGH (code direct read; Flink contract verified against `InternalTimerServiceImpl`).

---

## HIGH findings

### E2-HIGH-1 — V2 `getOrCreateKeyedState` ignores `StateDescriptor.getTtlConfig()`; TTL is silently disabled

**File:** `ForStRsAsyncKeyedStateBackend.java:199-251`

```java
public <N, S extends State, SV> S getOrCreateKeyedState(
        N ns, TypeSerializer<N> nsSer, StateDescriptor<SV> desc) throws Exception { … }

private <N, S extends InternalKeyedState, SV> S createStateInternal(
        N ns, TypeSerializer<N> nsSer, StateDescriptor<SV> desc) throws Exception {
    switch (desc.getType()) {
        case VALUE: return new ForStRsValueStateV2<>(…);
        case MAP:   return new ForStRsMapStateV2<>(…);
        …
    }
}
```

Neither method consults `desc.getTtlConfig()`. `ForStRsTtlCompactFiltersManager.setTtlForState` exists and the engine-side wiring (`ForStRsLinker.setCompactionFilterTtl`) is present, but the V2 backend never invokes it. Round 1's E-MED-2 said "TTL cleanup-incrementally is unsupported" — the truth is broader: **TTL is wholly unsupported on V2 state**, including the engine compaction filter that would otherwise enforce time-bounded expiry.

V1 path: I grepped for `setTtlForState` callers — there are none in the `flink-statebackend-forst-rs` module. So TTL is silently disabled on BOTH V1 and V2.

**Streaming impact:** users setting `StateTtlConfig` on a V2 state descriptor (or V1) see zero enforcement. Pre-Round-1 audit-design did not catalogue this. For long-running jobs this is unbounded state growth.

**Fix shape:** in `createStateInternal`, check `desc.getTtlConfig()`; if non-null/enabled, look up `ForStRsTtlCompactFiltersManager` (needs to be instantiated and held by the backend) and call `setTtlFromStateTtlConfig(stateName, ttlConfig, stateType, timestampOffset)` before returning the state instance. Wrap returned state with a `TtlValueState`/`TtlMapState` adapter if cleanup-on-read semantics are needed (`StateTtlConfig.StateVisibility.NeverReturnExpired`).

**Certainty:** HIGH.

---

### E2-HIGH-2 — V2 `ForStRsMapStateV2.MapStateCache` survives `asyncClear()`; stale entries returned post-window-cleanup

**File:** `ForStRsMapStateV2.java:101-107` (comment), inherited final `asyncClear()` from `AbstractMapState`.

The class doc explicitly says:

```text
// asyncClear() is final in AbstractKeyedState and cannot be overridden — instead we hook the
// namespace switch on setCurrentNamespace below if needed; for V1 we rely on the fact that
// Flink's MapState.clear() is rare enough (end-of-window only in Q11/Q12) that cache staleness
// for the cleared operator key is corrected on the next miss-resolve, which fetches from the
// engine (where the clear already took effect). The corner case is asyncGet immediately after
// asyncClear returning a stale cached value — addressed in V1.2 with a clear-hook.
```

The author acknowledges the bug but defers it as V1.2 work. Within a single record's processing it IS possible to call `asyncGet` after `asyncClear` (e.g., a user function that conditionally clears then reads to populate a default). The cache lookup at line 135-138 returns a cached value with `hit.cached() == true` without consulting the engine — the cache thinks the key is still alive even though the engine cleared it.

Combine with E2-CRIT-1 (no namespace in cache key): when a window closes for `(operatorKey, namespace=W1)` and a record arrives for `(operatorKey, namespace=W2)`, the cache returns the OLD W1 value — silent corruption.

**Streaming impact:** windowed MapState reads after clear silently return stale data. Q11/Q12 don't reproduce because session-window cleanup uses `ListState`, not `MapState`. But TUMBLE windows with MapState aggregations would.

**Fix shape:** intercept `asyncClear` via the `setCurrentNamespace` hook hinted in the comment, OR add a `cache.clearForCurrentKey()` invocation by overriding `asyncClear` via reflection-bypass / by routing through a non-final method. Best fix: don't use a per-state cache for namespaced state until namespace is in the cache key.

**Certainty:** HIGH on cache-staleness; correctness amplification depends on E2-CRIT-1 being fixed first.

---

### E2-HIGH-3 — `ForStRsRestoreOperation.restoreWithRescaling` opens N source engines (one per source handle); each `dbOpen` scans S3-staged SST manifests serially

**File:** `flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsRestoreOperation.java:292-377`

**Evidence:**

```java
private RestoreResult restoreWithRescaling(List<ForStRsIncrementalKeyedStateHandle> handles) {
    for (int i = 0; i < handles.size(); i++) {
        OpenSourceDb src = openSingleHandleAt(h, subDir);   // → linker.dbOpen + dbDefaultCf
        sources.add(src);
    }
    …
    // 2. Open an empty target DB.
    FrsDb targetDb = linker.dbOpen(arena, targetEngine.toString());
    …
    // 3. For each kg, find source, copyKeyGroup (per-record put — Round-1 E-HIGH-2).
}
```

Each `linker.dbOpen` does a full RocksDB-style manifest read + SST list-and-version-set rebuild. With N source handles, restore time is O(N × dbOpen_cost). For a rescale-down 256 → 64 (4× reduction), every subtask receives 4 source handles → 4 serial dbOpens before the per-key copy loop starts.

The user-attributable contribution to the brief's "12s cluster restart overhead" is plausibly here: for an empty cluster, `dbOpen` on an empty staging dir is cheap (no SSTs); for a real restore the cost scales with manifest size. Round 1 E-HIGH-2 covered the per-record copyKeyGroup cost; this is the dbOpen-fan-out cost that's separate and additive.

A second concern: `openSingleHandleAt` (line 368-377) downloads SSTs to a temp dir via `downloadHandleStrict` (line 238-286) using an 8 KiB buffer reading **serially** from `handle.openInputStream()`. For BOS/S3 endpoints, this is one-stream-per-SST single-threaded — no parallel download, no multipart, no pipelining with the engine warmup.

**Streaming impact:** recovery time after JM/TM restart scales linearly with (number of source handles) × (manifest + SST download time). For large checkpoint sizes (multi-GB SSTs), this dominates restart RTO. The brief's 12s figure is empty-restore; real-state restores extrapolate badly.

**Fix shape:** (a) parallelize `downloadHandleStrict` per-SST across an executor (4-8 threads); (b) batch `linker.dbOpen` calls — engine could expose `frs_db_open_multi` that opens N DBs in parallel; (c) cache the SST manifest read so subsequent kg-copy scans don't re-read. Best fix: skip the materialize-then-copy round-trip and use `ingestExternalSst` directly on the kg-prefix SST subset (already exists in `ForStRsStateMigration:175`, per Round 1).

**Certainty:** MED-HIGH (perf is structural; correctness is fine).

---

## MED findings

### E2-MED-1 — V2 backend has no `currentNamespace` tracking; `StateRequest.getNamespace()` is read but never reaches serializeKey

`ForStRsAsyncKeyedStateBackend` does not implement `org.apache.flink.runtime.state.NamespaceServiceProvider` or maintain a `currentNamespace` field. The base `AbstractKeyedState` may track namespace, but the V2 state subclasses' `serializeKey(StateRequest)` does not extract it from the request — the `<N>` type parameter is unused in serialization (see E2-CRIT-1). Root-cause of the namespace-omission bug is structural: no plumbing exists. Fixing E2-CRIT-1 requires this plumbing.

### E2-MED-2 — `notifyCheckpointComplete` on V2 backend is a no-op-with-flush; SST registry ref-counts NEVER drop

**File:** `ForStRsAsyncKeyedStateBackend.java:391-399`

```java
public void notifyCheckpointComplete(long id) {
    managedExecutors.forEach(VectorizedExecutor::flushDirty);   // flushDirty is a no-op
}
public void notifyCheckpointAborted(long id) {}
public void notifyCheckpointSubsumed(long id) {}
```

Compare with `ForStRsAbstractKeyedStateBackend.notifyCheckpointComplete` (line 603-615) which calls `snapshotStrategy.recordCompletedCheckpoint(id)` + `takePendingRegistrations(id)` to release SST registry ref-counts on completion. The V2 backend's empty implementation means: even if snapshot were wired (CRIT-1 in Round 1), the ref-count bookkeeping would never advance, leaking SST file references and preventing forst-rs's incremental compaction from dropping superseded snapshots.

Stacks on Round 1 E-CRIT-1 (empty snapshot). When snapshots become real, this is the second bookkeeping gap.

---

## Cross-references

| New finding | Round 1 link |
|---|---|
| E2-CRIT-1 (no namespace) | NOT covered — different from E-CRIT-3 (kg=0) and E-MED-3 (kg encoding) |
| E2-CRIT-2 (constant kg supplier in V2 timer) | adjacent to Round 1 E-CRIT-3 (kg=0 in V1 ValueState) but on different code path; brief marks E-HIGH-6 false-positive on perf grounds, not correctness — this is the correctness bug under FORSTRS |
| E2-HIGH-1 (no TTL) | EXTENDS Round 1 E-MED-2 (cleanupIncrementally) |
| E2-HIGH-2 (MapStateCache survives clear) | NOT covered |
| E2-HIGH-3 (dbOpen fan-out) | EXTENDS Round 1 E-HIGH-2 (per-record put on rescale) — additive cost |
| E2-MED-2 (notifyComplete no-op) | EXTENDS Round 1 E-CRIT-1 (empty snapshot) — wakes up after CRIT-1 fix |

---

## Recommendation ordering

1. **E2-CRIT-1** — namespace plumbing. Without this, any TUMBLE/HOP windowed V2 state silently corrupts. Same fix unblocks E2-HIGH-2 cache-staleness.
2. **E2-CRIT-2** — fix `currentKeyGroupSupplier` to dynamic. Without this, FORSTRS timer variant drops timers for all but one kg.
3. **E2-HIGH-1** — read `desc.getTtlConfig()` in `createStateInternal` and wire to `ForStRsTtlCompactFiltersManager`. Trivial once the manager is owned by the backend.
4. **E2-HIGH-3** — parallelize restore-side SST download + dbOpen; switch to `ingestExternalSst`.
5. **E2-MED-2** — once Round-1 E-CRIT-1 lands, also wire `notifyCheckpointComplete` to ref-count release.
