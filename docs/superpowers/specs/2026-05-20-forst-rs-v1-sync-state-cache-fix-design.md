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
| **T1 — large drift on strong win** | Any current ≥ 1.5× win regresses > 10 % | **Review-triggered** — PMC decision based on **net portfolio delta** (formula below) |
| **T2 — catastrophic** | Any query regresses > 20 % regardless of position | **Mandatory revert** |
| **Target wins** | Q5 < 113.98 s, Q8 < 32.81 s, Q13 < 33.75 s | KPI met → PR-A lands; missed → re-bench / Approach-C spec |

Rationale: a 1.5× win dropping to 1.35× (10 % drift) is still a win and should not gate-block a 5× Q5 fix; but a win dropping below rocksdb (1.0×) is a regime change and forces revert.

#### Net portfolio delta formula (T1 review default)

To prevent recurring subjective debate over "which metric to use" in T1 review, compute a single scalar from the bench results and use it as the default starting data for the review meeting:

```
net_delta = Σ_query  log10(new_time / old_time)
```

where the sum runs over every query that ran on both v3.3 and the candidate PR. **Smaller (more negative) = better.** Each query contributes a log-ratio so a 2× speedup on one query exactly cancels a 2× regression on another. Reviewers retain the right to override the formula, but the conversation starts from a fixed number.

This formula belongs in CONTRIBUTING.md alongside the tiered gates (PR-D below).

### PR sequencing — split by risk profile

The original linear "one commit per step, bench between" sequencing is preserved at the technical level, but **PR boundaries** are drawn along risk-curve discontinuities:

#### PR-A (Headline): "Fix V1-Sync stateCache per-event clear regression"

Sequenced internally as commits A0 → A1 → A2 following the RocksDB/LevelDB "new-code commit / behavior-switch commit separation" pattern. Each commit's role:

- **Commit A0** — Q11 / Q12 V2-async-path **instrumented verification** (≈ 0.5 engineer-day; temporary counters, removed before commit).

   Add `AtomicLong` counters at four call-site classes: `ForStRsValueStateV2.value`, `ForStRsValueStateV2.update`, `ForStRsValueState.value` (V1), `ForStRsValueState.update` (V1). Run Q11 + Q12 each for one normal duration, then evaluate **three falsifiable yes/no gates**:

   | # | Assertion | Pass condition | If fails |
   |---|---|---|---|
   | 1 | **Path identity** | `V2.value + V2.update > 0` AND `V1.value + V1.update == 0` | Q11/Q12 do not actually use V2 — design's "Q11/Q12 unaffected" claim is falsified; HALT before A2 |
   | 2 | **Cardinality sanity** | `V2.value + V2.update` is within 2× of `(input_records × expected_ops_per_record)` for the query | Counter wiring is wrong or per-record op assumption is wrong; investigate before proceeding |
   | 3 | **setCurrentKey cross-check** | `backend.setCurrentKey` invocation count is within 1× of `input_records`, AND `V2.value/update` count is within 1× of `setCurrentKey × expected_ops` | V2 ops aren't following setCurrentKey 1:1; the cache-clear change might affect V2 in unexpected ways; HALT |

   All three must pass for A2 to proceed. Counters are removed before A0 is committed (no permanent observability cost). A0 itself is verification-only and produces NO code change at the production paths — its commit contains only the temporary counter scaffold + a markdown attestation of the three pass/fail outcomes captured during the verification run.

- **Commit A1** — **Additive only**: `ForStRsValueState` adds the new kg-prefixed-with-buffer-hooks constructor; caches `stateNameBytes`; **legacy ctor retained, no call sites changed**.

   Why: the new constructor is dead code at commit time. Running the full Nexmark sweep against A1 must confirm **zero drift** on every query (within bench noise). Any drift here indicates a latent side effect from the new constructor (e.g., static initializer, class loader hit) that we'd otherwise blame on A2. Confirmed-zero-drift becomes the audit baseline for A2.

- **Commit A2** — **Behavior switch**: `ForStRsKeyedStateBackend.getValueState` switches to the new ctor; **`stateCache.clear()` line 288 removed**. This is the smallest possible commit that flips behavior — call-site rewire + one-line deletion + the cache-survival semantics.

   Bench post-A2 measures the **main-fix gain**. Because A1 already confirmed zero-drift, any A2 delta is attributable to the cache-survival + per-call composite-key encoding switch — no side-effects commingled.

PR-A is the maximum-visibility ship target — A2's one-line removal + call-site switch carries the potential 5× win and benefits from concentrated PMC review. The A0/A1/A2 separation makes the bench data tell a clean story: A0 confirms scope, A1 confirms zero-cost-of-additive-code, A2 confirms gain-from-behavior-flip.

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

### CONTRIBUTING.md update (PR-D)

Add a new section under perf-work guidelines. Two pieces: the MUST rule with a structured 5-item checklist, and the portfolio-aware bench gate + net-delta formula.

> **MUST: Search memory for falsifying evidence before locking a root cause.**
>
> Before forming the final root-cause hypothesis in any performance audit, execute the following 5-item checklist. Each item maps to a specific failure mode observed in the case studies below:
>
> 1. **Antonym search of the current hypothesis.** Phrase the inverse claim and search memory for it. *Example: hypothesis = "FFM-per-call is the bottleneck" → search "FFM faster than JNI", "boundary cost not bottleneck", "engine per-call dominates".*
> 2. **Recent micro-benches of the involved components.** Grep memory + `docs/superpowers/specs/` for the latest micro-benchmark numbers on each component named in your hypothesis. *Example: hypothesis names `getPinned` → find the most recent `getPinned` micro-bench result.*
> 3. **Regime correction for any earlier numerical floor.** Search for prior session notes containing "regime", "floor", "actually X ns/µs/s not Y" on the relevant ops. *Example: "engine per-call 4.6 µs" was a regime correction missed in a prior audit.*
> 4. **Earlier audits on the same query class.** Search for prior audits naming the affected queries OR the same access-pattern shape (HOP / sliding-shared / JOIN / per-key high-throughput). Read the conclusion before re-auditing.
> 5. **Grep last-6-month perf-related specs.** Run `git log --since=6.months docs/superpowers/specs/` and skim the titles for related audits. The cost is one minute and prevents re-walking solved ground.
>
> If any checklist item surfaces evidence that contradicts the current hypothesis, the hypothesis MUST be revised before any fix is proposed.
>
> **Case studies — failure modes this checklist prevents:**
>
> | Case | What was missed | Which checklist item would have caught it |
> |---|---|---|
> | Q5/Q8/Q13 cache-clear (2026-05-20) | `project_nexmark_q3_jni_experiment` showed FFM 200 s < JNI 232 s, falsifying the "FFM-per-call structural gap" framing | #1 (antonym search) — searching "FFM faster than JNI" would have hit it; #2 (recent micro-benches) — searching for the most recent FFM-vs-JNI measurement |
> | Q3 4.6 µs engine per-call (earlier session) | Engine-level micro-benchmark showed the cost was *inside* the engine, not at the boundary | #2 (recent micro-benches) — searching for "engine per-call cost" or "engine per-op cost" would have surfaced it; #3 (regime correction) — searching "actually 4.6 µs" or "regime" |
>
> Both cases share the same cognitive pathology: the auditor formed a structural hypothesis and stopped looking. The mitigation is procedural — make "retrieve contradicting evidence" precede "form new hypothesis."

> **MUST: Portfolio-aware bench gate.** Use the tiered T0/T1/T2 thresholds (see [tiered bench gates section](#bench-acceptance-gates--tiered-portfolio-aware) of the related design spec) for accepting / rejecting any PR claiming a perf win. Compute the **net portfolio delta** as the default starting number for any T1 review:
>
> ```
> net_delta = Σ_query  log10(new_time / old_time)
> ```
>
> Smaller is better. The formula is not a final arbiter — PMC review retains override — but it eliminates "which metric do we use" as an open question at review time.

PR-D ships as a single commit on `forst-rs` branch with no code changes.

### Implementation summary

| PR | Commits | Bench acceptance | Order |
|---|---|---|---|
| PR-A | A0 (Q11/Q12 verification, 3 falsifiable gates) + A1 (additive ctor, zero-drift bench) + A2 (call-site switch + cache-clear removal, main-fix bench) | Tiered T0/T1/T2 + Q5 < 114 s target + net_delta < 0 | Lands first |
| PR-B | B1 (List/Reducing/Aggregating ctors + MapState audit) + B2 (serializer overload) | Same tiered gates + Q8 / Q13 KPI confirmation + net_delta < 0 | After PR-A merged |
| PR-C | Mutable wrapper + concurrent stress test + reentrancy guard | Tiered gates + JFR alloc-rate confirmation | After PR-B merged |
| PR-D | CONTRIBUTING.md MUST rule (5-item checklist + case-study table) + portfolio gate + net_delta formula | (docs-only) | Any time; can land in parallel with PR-A |

### Spec template extraction

This design's structural sequence — Problem → Root Cause → Goal/Non-Goals → Architecture → Components → Data Flow → Invariants → Error Handling → Testing (tiered gates + net_delta) → PR sequencing by risk curve → CONTRIBUTING.md-coupled PR — is generalizable to any forst-rs performance fix.

After PR-A lands, archive this document's section structure as `docs/superpowers/templates/perf-fix-spec-template.md` so future perf-fix authors get the procedural reminders for free via empty template fields. This upgrades the workflow from discipline-based ("remember to consider tiered gates") to tool-based ("the template has a tiered-gate section; you fill it in or explicitly mark N/A"). The template extraction is tracked as a follow-on after PR-A so we don't bikeshed the template before the original spec ships.

## Open Questions

None at design time. If PR-A + PR-B land and Q5 still misses 113.98 s, the follow-up Approach-C spec for engine-level work will be authored and reviewed separately.
