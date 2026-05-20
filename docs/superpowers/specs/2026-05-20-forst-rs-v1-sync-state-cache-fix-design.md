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

### Bench acceptance gates — tiered, portfolio-aware

Replaces the previous flat ±5 % gate. The flat gate would have blocked a Q5-class net portfolio win because a single high-multiple win wobbled by 7 %. The tiered scheme protects strategic position without conflating signal with noise.

| Tier | Trigger | Action |
|---|---|---|
| **T0 — strategic-threshold breach** | Any query that was ≥ 1.0× rocksdb in v3.3 drops below 1.0× | **Mandatory revert** |
| **T1 — large drift on strong win** | Any current ≥ 1.5× win regresses > 10 % | **Review-triggered** (not auto-revert) — PMC decision based on net portfolio delta |
| **T2 — catastrophic** | Any query regresses > 20 % regardless of position | **Mandatory revert** |
| **Target wins** | Q5 < 113.98 s, Q8 < 32.81 s, Q13 < 33.75 s | KPI met → PR-A lands; missed → re-bench / Approach-C spec |

Rationale: a 1.5× win dropping to 1.35× (10 % drift) is still a win and should not gate-block a 5× Q5 fix; but a win dropping below rocksdb (1.0×) is a regime change and forces revert.

### PR sequencing — split by risk profile

The original linear "one commit per step, bench between" sequencing is preserved at the technical level, but **PR boundaries** are drawn along risk-curve discontinuities:

#### PR-A (Headline): "Fix V1-Sync stateCache per-event clear regression"

Steps that share the same risk profile (single-key-update-hub change, no concurrent/reentrancy concerns):

- **Step A0** — Q11 / Q12 V2-async-path **instrumented verification** (≈ 0.5 engineer-day). Add temporary counters at `ForStRsValueStateV2.value/update` entry points, run Q11 / Q12 once, confirm the V2 path is the one taken AND the call counts match expected per-event work. Remove counters before commit. Closes the prior audit's "Q11/Q12 use ForStRsValueStateV2" claim which rested on grep + memory-recall, not instrumentation — and the prior audit already mis-identified the state class once (see CONTRIBUTING.md case studies). Do this BEFORE touching `setCurrentKey`.
- **Step A1** — `ForStRsValueState` adds kg-prefixed-with-buffer-hooks ctor; caches `stateNameBytes`; legacy ctor retained for tests.
- **Step A2** — `ForStRsKeyedStateBackend.getValueState` switches to new ctor; **remove `stateCache.clear()` line 288**. Bench Q5/Q8/Q13 + Q11/Q12 sanity.

PR-A is the maximum-visibility ship target — one-line removal + constructor switch carries the potential 5× win and benefits from concentrated PMC review.

#### PR-B chain (Follow-up): "Extend V1-sync alloc cuts to remaining state types"

Same risk profile as PR-A, but each sub-PR is independently reviewable:

- **PR-B1** — `ForStRsListState`, `ForStRsReducingState`, `ForStRsAggregatingState` switch to kg-prefixed ctor (+ MapState audit per its row above).
- **PR-B2** — `ForStRsKeyGroupedSerializer.encodeForState` overload accepting pre-encoded `stateNameBytes`.

Reuse the spike-then-design pattern: PR-A validates the approach; PR-B propagates it.

#### PR-C (Hardening sprint): "Thread-local mutable ByteArrayWrapper for V1 buffer lookups"

This step has a **different risk curve** from PR-A and PR-B because it introduces a thread-local mutable shared piece of state held under the buffer's HashMap lookup contract. Concurrency / reentrancy bugs here can be silent corruption (wrong cache hits across threads, recursive calls returning a stale wrapper view). Decoupling protects PR-A's headline win from being held up by a hardening question.

PR-C requires:
- Concurrent stress test: N threads × M setCurrentKey iterations × per-state read/write — assert HashMap returns correct value or null, no cross-thread contamination.
- Reentrancy assertion guard: the mutable wrapper holds an `inUse` flag asserted-set on borrow and asserted-unset on return; reentrant access fails-fast in dev builds.
- Bench-confirmed isolated win (alloc-rate JFR before/after).

Land PR-C only after PR-A and PR-B are merged and their wins are confirmed in production-equivalent benches.

#### PR-D (Process): "CONTRIBUTING.md — perf-audit hypothesis-falsification rule"

Independent of code. Adds a **MUST** rule grounded in the two empirical case studies (this Q5 cognitive failure + the prior 4.6 µs regime correction). See "CONTRIBUTING.md update" section below.

### CONTRIBUTING.md update

Add a new section under perf-work guidelines:

> **MUST: Search memory for falsifying evidence before locking a root cause.**
>
> Before forming the final root-cause hypothesis in any performance audit, run an explicit memory search ( `conversation_search` over `~/.claude/projects/.../memory/`, plus a grep of `docs/superpowers/specs/` for related audits ) for evidence that *falsifies* your working hypothesis. Examples of falsifying queries: "FFM vs JNI cost," "per-call overhead measurement," "engine micro-bench."
>
> If a memory entry contradicts the current hypothesis, the hypothesis MUST be revised before any fix is proposed.
>
> **Case studies — what this rule prevents:**
> 1. **Q5 / Q8 / Q13 cache-clear (2026-05-20)** — initial audit framed the 5× slowdown as an "FFM-per-call vs JNI-per-call structural gap" and proposed accepting the loss. Memory `project_nexmark_q3_jni_experiment` (saved earlier) had already measured FFM at 200 s vs JNI at 232 s on the same engine, directly falsifying the structural-gap framing. Not retrieving that memory delayed the real fix ( `stateCache.clear()` per-event reallocation) by one full review cycle.
> 2. **Q3 4.6 µs engine per-call cost (earlier session)** — initial audit framed the bottleneck as the FFM boundary; engine-level micro-benchmarks (already in memory) showed the per-call cost was 4.6 µs *inside* the engine, not at the boundary. Same failure mode.
>
> Both cases share the same cognitive pathology: the auditor formed a structural hypothesis and stopped looking. The mitigation is procedural — make "retrieve contradicting evidence" precede "form new hypothesis."

PR-D ships as a single commit on `forst-rs` branch with no code changes.

### Implementation summary

| PR | Contains | Bench acceptance | Order |
|---|---|---|---|
| PR-A | Step A0 (Q11/Q12 verification) + A1 + A2 | Tiered gates above + Q5 < 114 s target | Lands first |
| PR-B | B1 (List/Map/Reducing/Aggregating ctors) + B2 (serializer overload) | Same tiered gates + Q8 / Q13 KPI confirmation | After PR-A merged |
| PR-C | Mutable wrapper + concurrent stress test + reentrancy guard | Tiered gates + JFR alloc-rate confirmation | After PR-B merged |
| PR-D | CONTRIBUTING.md MUST rule + case studies | (docs-only) | Any time; can land in parallel with PR-A |

## Open Questions

None at design time. If PR-A + PR-B land and Q5 still misses 113.98 s, the follow-up Approach-C spec for engine-level work will be authored and reviewed separately.
