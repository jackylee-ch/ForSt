# Forst-RS V1-Sync State Cache Regression Fix — Design

**Author:** jackylee (PMC) + Claude
**Date:** 2026-05-20
**Branches:** `~/Code/stczwd/flink` `forst-rs-jdk25`, `~/Code/stczwd/ForSt` `forst-rs`
**Related:** [v3 bench report](2026-05-20-forst-rs-benchmark-report-v3.md), memory `project_q5_q8_q13_structural_gap.md` (to be updated after this lands)

## Problem

Forst-RS is 1.26–5.15× slower than RocksDB / community ForSt on Nexmark **Q5 (HOP), Q8 (TUMBLE+JOIN), Q13 (stream-side JOIN)**:

| Query | RocksDB | community ForSt | forst-rs (v3.3) | forst-rs gap |
|---|---|---|---|---|
| Q5  | 113.98 s | timeout      | 586.68 s | **5.15× slower** |
| Q8  | 32.81 s  | 31.08 s      | 53.41 s  | **1.63× slower** |
| Q13 | 33.75 s  | 35.50 s      | 42.42 s  | **1.26× slower** |

These queries route to the **V1 SYNC** state path (Flink table-runtime ships only Sync window-agg / join processors for the HOP-shared and JOIN patterns). The earlier audit framed this as a *structural* gap (FFM-per-call vs JNI-per-call). That was wrong: the JNI-vs-FFM micro-benchmark `project_nexmark_q3_jni_experiment` showed FFM (200 s) is actually FASTER than JNI (232 s) on the same engine. The real bottleneck is in the Java glue layer.

## Root Cause

`ForStRsKeyedStateBackend.setCurrentKey(K)` at `flink-state-backends/flink-statebackend-forst-rs/src/main/java/.../keyed/ForStRsKeyedStateBackend.java:288`:

```java
this.stateCache.clear();   // ← per-event cache invalidation
```

Combined with the legacy "static byte[] prefix" mode in `ForStRsValueState` (which bakes `currentKeyBytes` into a captured prefix at construction), every effective key change forces:

1. `stateCache.clear()` — wipes per-state-name cache entries.
2. Next `getValueState(stateName)` → cache MISS → fresh `new ForStRsValueState(...)` allocation.
3. `buildPrefix(stateName)` allocates a new composite-prefix `byte[]`.
4. `ForStRsValueState` constructor clones the prefix `byte[]`.
5. `stateName.getBytes(StandardCharsets.UTF_8)` inside the prefix builder — another allocation.

For Q5 at 100 M events × ~5 state ops/event with mostly-unique auction keys: **~100 M+ `ForStRsValueState` object allocations + comparable byte[] churn.** Community ForSt's `ForStSyncValueState` has none of this: state instance is constructed once per (stateName), `serializeCurrentKeyWithGroupAndNamespace()` produces a per-call composite key.

Q11 / Q12 are unaffected because the V2 async path uses a different cache (`ContextKey`-based) that does not trigger on `setCurrentKey`.

## Goal

**Beat RocksDB on Q5, Q8, Q13** (strict 1.x× KPI per memory `feedback_perf_targets`):

- Q5  < 113.98 s
- Q8  < 32.81 s
- Q13 < 33.75 s

while holding **all 16 existing wins within 5%** of v3.3 numbers.

## Non-Goals

- Modifying Flink runtime (`flink-table-runtime`, planner) — out of project scope.
- Modifying Forst-RS Rust engine — deferred to a separate Approach-C spec only if this design's KPI is missed.
- Refactoring V2 async state code paths — not on Q5/Q8/Q13 critical path.
- Iterator / VectorizedExecutor / classifier — already correct, unchanged.

## Architecture

### Current (broken)

```
ForStRsKeyedStateBackend
├── stateCache: Map<stateName, V1-sync state instance>
├── setCurrentKey(K)
│     ├── currentKeyBytes = serialize(K)
│     ├── keyGeneration++
│     └── stateCache.clear()                    ← THE BUG
└── getValueState(stateName)
      ├── cache miss → buildPrefix(stateName) allocs new byte[]
      └── new ForStRsValueState(prefix, …)     ← allocs per event

ForStRsValueState (legacy static-prefix ctor)
├── keyPrefix: byte[]                          ← stale once key changes
└── computeKey() → return keyPrefix
```

### Target

```
ForStRsKeyedStateBackend
├── stateCache: Map<stateName, V1-sync state instance>   (never cleared on key change)
├── currentKeyBytes: byte[]                              (already exists, kept live)
├── setCurrentKey(K)
│     ├── currentKeyBytes = serialize(K)
│     ├── keyGeneration++
│     └── (no cache clear)
└── getValueState(stateName)
      └── cache hit on every subsequent event of this operator

ForStRsValueState (kg-prefixed ctor + write-buffer hooks)
├── keyComputer: () -> byte[]                            (closes over backend ref + cached stateNameBytes)
├── stateNameBytes: byte[]                               (computed ONCE in ctor — eliminates per-call UTF-8 alloc)
└── computeKey() → reuses thread-local DataOutputSerializer, writes
                   [ns-marker | currentKeyBytes | / | stateNameBytes | /]
                   returns composite byte[] (one alloc per call — matches community ForSt)
```

Same pattern is applied to **ListState, MapState, ReducingState, AggregatingState**.

## Components Touched

| Component | File | Change |
|---|---|---|
| Backend cache invalidation | `ForStRsKeyedStateBackend.setCurrentKey:288` | Remove `stateCache.clear()`. Keep `keyGeneration++`. |
| ValueState wiring | `ForStRsKeyedStateBackend.getValueState` ~325-336 | Switch to keyComputer-mode ctor; drop `buildPrefix`. |
| ListState wiring | `getListState` ~341-355 | Same pattern. |
| MapState wiring | `getMapState` ~362-379 | The kg-prefixed ctor for MapState already exists (it was wired this way during the MapStateCache work — see memory `project_q11q12_state_primitive`). Verify the existing cache key is `(stateName)` not `(stateName + currentKey)`; if it's still keyed by current-key, switch to stateName-only and confirm with the existing MapStateCache tests. |
| ReducingState wiring | `getReducingState` ~391-410 | Same pattern. |
| AggregatingState wiring | `getAggregatingState` ~415-435 | Same pattern. |
| ValueState kg-prefixed ctor | `ForStRsValueState.java:139-156` | Extend signature to accept write-buffer hooks (`writeBufferGet/Put/Delete` — currently only the legacy ctor has them). |
| ListState kg-prefixed ctor | `ForStRsListState.java` | Add if absent. |
| ReducingState kg-prefixed ctor | `ForStRsReducingState.java` | Add if absent. |
| AggregatingState kg-prefixed ctor | `ForStRsAggregatingState.java` | Add if absent. |
| KeyGroupedSerializer per-call alloc | `ForStRsKeyGroupedSerializer.encodeForState:71-86` | Add overload `encodeForState(int kg, K userKey, byte[] preEncodedStateNameBytes)` that skips the `stateName.getBytes(UTF_8)` allocation. |
| State-name caching | new fields on each state class | Compute `stateNameBytes` ONCE in constructor; pass to encoder. |
| Write-buffer key wrapping | `ForStRsKeyedStateBackend.getFromWriteBuffer / putToWriteBuffer` | Reuse a thread-local mutable `ByteArrayWrapper` for the lookup key to drop per-call wrapper allocation. (Map storage still allocates a wrapper on insert, but reads — which dominate after adaptive disable — go alloc-free.) |

## Data Flow (Post-Fix)

Q5 event arrives:

1. `backend.setCurrentKey(auctionId)` → `currentKeyBytes` updated; `keyGeneration++`; **no cache clear**.
2. Window-agg processor calls `getValueState("accumulator")` → cache HIT → same instance.
3. `state.value()`:
   - `computeKey()` → reuses thread-local buffer; writes `[ns-marker | currentKeyBytes | / | stateNameBytes | /]`; returns composite byte[] (1 alloc).
   - `writeBufferGet.apply(key)` → mutable wrapper points at key; HashMap lookup; (0 wrapper allocs).
   - Cache miss → `linker.getPinned(db, cf, key)` → FFM call; result byte[] (1 alloc on hit, null on miss).
   - Skip-fallback flag still gates the optional `linker.getFast` call.
4. `serializer.deserialize` → returned value.

Per-event allocations: **3-4 (composite key + result byte[] + deserialized object)** — down from **6-9** in the current code, matching community ForSt.

## Correctness Invariants

- **Read-after-write consistency** — write-buffer hooks remain. `value()` checks buffer before native call. Pattern unchanged.
- **Key-group routing** — keyComputer reads `currentKeyGroupIndex` and `currentKeyBytes` lazily at the moment of `computeKey()`. Verified by existing `KeyGroupAwareIT` tests.
- **State isolation** — each backend's stateCache is independent. No cross-operator leakage.
- **Snapshot/restore** — cache is rebuilt by the same factory methods on restore. No on-disk format change.
- **Write-buffer flush ordering** — buffer flush still happens on threshold + checkpoint + close. Unchanged.

## Error Handling

No new error modes. Existing FFM error propagation (FRS_STATUS_OK / FALLBACK / ERROR), write-buffer overflow handling, checkpoint-skipped logic — all unchanged.

## Testing

### Existing tests that must still pass

- `ForStRsKeyedStateBackendTest` — setCurrentKey + getValueState semantics.
- `ForStRsValueStateTest`, `ForStRsListStateTest`, `ForStRsMapStateTest`, `ForStRsReducingStateTest`, `ForStRsAggregatingStateTest` — value/update/clear paths per type.
- `ForStRsKeyGroupedSerializerTest` — encode/decode round-trips.
- `KeyGroupAwareIT` (or equivalent) — multi-key-group state correctness.

### New unit tests

1. **State-instance identity across keys** — `getValueState("x")` returns the SAME instance after `setCurrentKey(k1)` then `setCurrentKey(k2)`. Counterpart tests for List/Map/Reducing/Aggregating.
2. **Allocation budget** — after 100 K alternating setCurrentKey calls, total `ForStRsValueState`-class instance count (heap dump via `ManagementFactory.getMemoryMXBean()` + class histogram) stays at O(1) per stateName, not O(events).
3. **Key-group correctness across boundaries** — setCurrentKey in keyGroup A, write; setCurrentKey in keyGroup B with the same logical user-key value, write distinct value; reading back from both key-groups returns correct distinct values.
4. **State-name pre-cache parity** — encoder produces byte-identical output via both `encodeForState(kg, k, stateName)` and `encodeForState(kg, k, preEncodedStateNameBytes)` overloads.

### Bench acceptance gates

| Query | Gate | Source baseline |
|---|---|---|
| Q5  | < 113.98 s  | rocksdb v3.2 |
| Q8  | < 32.81 s   | rocksdb v3.2 |
| Q13 | < 33.75 s   | rocksdb v3.2 |
| Q0-Q4, Q7, Q9-Q12, Q14-Q23 | within 5 % of v3.3 numbers | v3.3 (this report's prior section) |

If **any one** of Q5/Q8/Q13 misses, roll back the PR and open a follow-up Approach-C spec (Rust engine work). If any of the 16 wins regresses > 5 %, roll back the offending sub-change (we land per-state-type, one commit per type, so attribution is clean).

### Implementation order (one commit per step, bench between)

1. `ForStRsValueState` — add kg-prefixed-with-buffer-hooks ctor; cache `stateNameBytes`; keep legacy ctor for tests.
2. `ForStRsKeyedStateBackend.getValueState` — switch to new ctor; **remove `stateCache.clear()` line 288**. Bench Q5/Q8/Q13 + Q11/Q12 sanity.
3. `ForStRsListState`, `ForStRsMapState`, `ForStRsReducingState`, `ForStRsAggregatingState` — same pattern. Bench again.
4. `ForStRsKeyGroupedSerializer` — pre-encoded-stateName overload. Bench again.
5. `getFromWriteBuffer` thread-local mutable wrapper. Bench again.

This sequencing means each step ships an isolated, attributable delta — and if step 2 already meets KPI, we can ship there without the remaining cuts.

## Open Questions

None at design time. If steps 2-5 land and Q5 still misses 113.98 s, the follow-up Approach-C spec for engine-level work will be authored and reviewed separately.
