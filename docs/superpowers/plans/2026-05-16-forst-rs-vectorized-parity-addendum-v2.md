# Forst-RS Vectorized Parity V1 — Plan Reconciliation Addendum (v2)

> **For agentic workers:** This addendum supersedes `2026-05-16-forst-rs-vectorized-parity.md` for execution sequencing. The original plan remains the contract for what must exist at V1; this document is the delta against today's working tree.

**Why this exists:** The original plan was written assuming a clean working tree. An audit on commit `91e709b6b` revealed substantial uncommitted WIP in the Flink repo that already implements parts of the design. After auditing 30+ untracked files and committing them (Flink branch `forst-rs-jdk25` commits `348b6730885` + `ade472046ff`), the actual execution sequence is materially different from what the original plan describes.

**Net effect:** ~40% of the planned code already exists. Plan PRs collapse and reshuffle.

---

## Component status — reconciled

| # | Component | Original plan | Reality | V1.x action |
|---|---|---|---|---|
| 1 | `VectorizedStateRequest` sealed interface + 6 subtypes | NEW | **Concrete `ForStRsDB{Get,Put,Iter,WriteBatch}Request` exist.** Sealed-interface refactor needed to unify under one type-safe envelope. | **Refactor:** introduce sealed `VectorizedStateRequest` permits the 6 spec subtypes; migrate existing concrete classes to be subtypes; preserve their field shape. |
| 2 | `VectorizedClassifier` | EXTEND 3→6 kinds | **EXISTING-COMPLETE (286 LOC)** with 3 kinds. | Extend to APPEND_MERGE / ITER_PREFIX / ITER_RANGE (3 new kinds). |
| 3 | `ColumnarBatchBuffer` | EXTEND | **EXISTING-COMPLETE (190 LOC).** Arrow BinaryArray in Arena. | Add merge-payload column + iter-cursor column. |
| 4 | `VectorizedExecutor` | EXTEND 3→6 dispatches | **EXISTING-COMPLETE (282 LOC).** | Add 3 dispatch methods + per-kind metric wire-up. |
| 5 | `FrsIterHandle` (`AutoCloseable`) | NEW | **Partial — `ForStRsMapIterator` (66 LOC) wraps native iterator.** | Formalize as `AutoCloseable`, register with `SlotArenaScope`, track `lastNextNs`. |
| 6 | `IterLifetimeWatchdog` | NEW | **NOT-FOUND.** | NEW per original plan. |
| 7 | `SlotArenaScope` | NEW | **NOT-FOUND.** `VectorizedExecutor` uses a single shared Arena today. | NEW per original plan — substantial refactor of how Arena memory is managed. |
| 8 | `DispatchMetrics` | NEW | **Partial — `ForStRsNativeMetricMonitor` exists** but doesn't cover dispatch hot-path. | Extend to publish `dispatch.<kind>.*` metric family per Spec §1 §c. |
| 9 | `ForStRsValueState` (refactor) | EXTEND | **`ForStRsValueStateV2` EXISTING-COMPLETE (187 LOC).** V1 `ForStRsValueState` also exists for sync. | Decide naming: rename V2→primary + drop V1 OR keep both (V1 sync still needed for Flink Table joins, per task #44). |
| 10 | `ForStRsMapState` (refactor) | EXTEND | **`ForStRsMapStateV2` EXISTING-COMPLETE (255 LOC).** Same as ValueState pattern. | Same decision as (9). |
| 11 | `ForStRsListState` (refactor + APPEND_MERGE) | EXTEND | **NOT-FOUND in V2 form** (only sync `ForStRsListState`). | NEW V2 + APPEND_MERGE per original plan. |
| 12 | `ForStRsReducingState` | NEW | **NOT-FOUND.** | NEW per original plan. |
| 13 | `ForStRsAggregatingState` | NEW | **NOT-FOUND.** | NEW per original plan. |
| 14 | `ReducingAggregatingCache` | NEW | **`FlatStateCache` EXISTING-COMPLETE (169 LOC).** GC-free hash table. | Adapt — point ReducingState/AggregatingState at `FlatStateCache` (may need rename or thin wrapper). |
| 15 | `PendingMissTable` + `PendingMiss` | NEW | **NOT-FOUND.** | NEW per original plan. |
| 16 | `FatalErrorHandler` integration + `FrsEnginePanicError` | NEW | **NOT-FOUND.** | NEW per original plan. |
| 17 | `FrsAbi` + `FrsAbiMismatchException` | NEW | **NOT-FOUND.** | NEW per original plan. |
| A | Rust `frs_vec_get/put/del` error-envelope tightening | EXTEND | **EXISTING (uncommitted in ForSt; now committed at `64199821c`).** | Tighten to typed FrsErrorCode per spec §4 (commit only had the FFI shape, not error envelope). |
| B | `frs_vec_merge_append` FFI | NEW | NOT-FOUND. | NEW per original plan. |
| C | `frs_vec_iter_prefix_*` FFI | NEW | **Linker bindings exist (`prefixGetAll`, `batchPrefixScan`); Rust FFI may exist.** Need to verify. | Check `crates/forst-rs-ffi/src/lib.rs` for symbol; if present, treat as EXISTING-COMPLETE and skip plan task; if absent, NEW. |
| D | `frs_vec_iter_range_*` FFI | NEW | NOT-FOUND. | NEW per original plan. |
| E | `crates/forst-rs-storage/src/iter.rs` native iter | NEW | Unknown — verify. | If absent, NEW; if a similar abstraction exists in another path, adapt. |
| F | `crates/forst-rs-engine/src/list_merge.rs` | NEW | NOT-FOUND. | NEW per original plan. |
| G | `frs_abi_version` FFI | NEW | **NOT-FOUND.** | NEW per original plan. |

### Legacy components scheduled for deletion

Two Flink files were superseded by the Vectorized* implementations but are still in-repo (committed at `348b6730885` for history):
- `ForStRsStateRequestClassifier` — legacy per-row classifier (135 LOC)
- `ForStRsStateExecutor` — legacy per-row executor (135 LOC)

These get explicit-deletion tasks in plan-v2.

---

## Plan-v2 PR sequencing

PRs that collapse (work already done):
- **P2 (Dispatch core)** is now mostly "sealed-interface refactor + extend 3→6 kinds"
- **P5 (Value/Map refactor)** is now "rename V2 classes to primary + remove legacy V1 *if* V1 sync isn't kept for Flink Table joins"

PRs that stand:
- **P0** (microbench gate) — can now bench REAL components instead of stubs, gating more meaningfully
- **P1** (SlotArenaScope + ABI) — NEW, stands
- **P3** (FrsIterHandle + Watchdog + ITER_PREFIX FFI) — half-done; formalize handle + add Watchdog
- **P4** (DispatchMetrics extension + FatalErrorHandler) — extend, then NEW
- **P6** (ListState + APPEND_MERGE) — stands
- **P7** (RMW with FlatStateCache adapter) — adapt cache + NEW PendingMissTable + NEW ReducingState
- **P8** (Aggregating + barrier drain) — NEW state + verify drain integration in existing AsyncKeyedStateBackend
- **P9** (ITER_RANGE) — stands
- **P10-P12** — stand

PR ordering remains: P0 → P1 → P2 → P3 → P4 → P5 → P6 → P7 → P8 → P9 → P10 → P11 → P12.

### Updated PR task deltas

**P0 (microbench gate):**

Now benches the existing `VectorizedClassifier` / `VectorizedExecutor` / `ColumnarBatchBuffer` directly rather than stubs. The original P0.3 stub benches become real measurements. Updated targets:

| Bench | Previous (stub) | Now (real component) |
|---|---|---|
| `classifierSubmit` | stub `ArrayDeque.add` | Actual `VectorizedClassifier.submit(StateRequest)` |
| `executorDispatch` | stub 64 int reads | Actual `VectorizedExecutor.dispatch(filledBuf)` (no FFI — use in-memory engine) |
| `pendingMissComputeIfAbsent` | stub `ConcurrentHashMap` | Reserved for P7 (`PendingMissTable` not yet implemented) — leave as stub for P0 |

P0.5 GO/REVISE/STOP decision unchanged.

**P1 (Foundations):**

P1.1 — Rust `frs_abi_version` — stands.
P1.2 — Java `FrsAbi` + `FrsAbiMismatchException` + Linker binding — stands.
P1.3 — Wire `verifyAbi()` into `ForStRsKeyedStateBackend` init — **also wire into `ForStRsAsyncKeyedStateBackend` init** (committed at `348b6730885`). Both backend entry points must check.
P1.4 — `SlotArenaScope` — stands. This is a real refactor since `VectorizedExecutor` uses a single shared Arena today; introducing `SlotArenaScope` requires migrating callers.
P1.5 — Wire `SlotArenaScope` into backend lifecycle — extend to **both** `ForStRsKeyedStateBackend` (sync V1) and `ForStRsAsyncKeyedStateBackend` (async V2).

**P2 (Dispatch core) — collapsed:**

P2.1 (Rust typed FrsErrorCode + FrsRowResult) — stands.
P2.2 (tighten existing frs_vec_get/put/del error envelope) — stands.
P2.3 (Java FrsException hierarchy) — stands.
~~P2.4 (VectorizedStateRequest sealed interface + 6 subtypes — full new)~~ → **P2.4 v2: refactor existing `ForStRsDB{Get,Put,Iter,WriteBatch}Request` classes to be subtypes of a new sealed `VectorizedStateRequest` interface.** The class names stay; the interface is new.
~~P2.5 (ColumnarBatchBuffer extension)~~ → **P2.5 v2: add `AppendMergeBatchBuffer`, `IterPrefixBatchBuffer`, `IterRangeBatchBuffer` subclasses to existing buffer infrastructure.**
~~P2.6 (VectorizedClassifier extension)~~ → **P2.6 v2: extend `VectorizedClassifier.submit` switch to handle 3 new kinds + add APPEND_MERGE guard (ListState-only per Spec §1 §a).** Verify intra-(state,key) ordering invariant test still passes.
~~P2.7 (VectorizedExecutor extension)~~ → **P2.7 v2: add `dispatchAppendMerge`, `dispatchIterPrefix`, `dispatchIterRange` to existing `VectorizedExecutor`.**

**P3 (Iterator path) — adjusted:**

P3.1 (Rust iter.rs) — **verify existing FFI first.** Read `crates/forst-rs-ffi/src/lib.rs` for `frs_vec_iter_prefix_*` and `frs_prefix_get_all`. If `prefix_get_all` (which the Linker calls) already returns a chunked-iterator handle, treat as existing-complete; if not, NEW per original P3.1.
P3.2 (frs_vec_iter_prefix_* FFI) — same gating as P3.1.
P3.3 (Java `FrsIterHandle`) — **refactor from `ForStRsMapIterator`**: keep MapIterator as a consumer of FrsIterHandle, but extract the handle wrapper + Arena tracking + AutoCloseable shape.
P3.4 (`IterLifetimeWatchdog`) — NEW per original plan.
P3.5 (wire ITER_PREFIX through executor) — extend existing.

**P4 (Metrics + FatalErrorHandler):**

P4.1 (`DispatchMetrics`) — **extend `ForStRsNativeMetricMonitor`** to add the `flink.state.forstrs.dispatch.<kind>.*` family with 128 cardinality cap.
P4.2 (wire DispatchMetrics into VectorizedExecutor) — stands.
P4.3 (FatalErrorHandler integration) — stands; integrate into both VectorizedExecutor + legacy ForStRsStateExecutor (the legacy one will be deleted in a later step, but until then it should respect the contract).

**P5 (Value + Map refactor) — collapsed:**

P5.1 v2: **Verify `ForStRsValueStateV2` matches Spec §3 Trace A.** If yes, mark complete. If gaps (e.g. SP6 6.2 staging not wired), add the gap as a single task.
P5.2 v2: Same for `ForStRsMapStateV2` (SP6 6.3 staging should already be present per spec audit).
P5.3 v2: `MapState.entries()` via ITER_PREFIX — verify against existing `ForStRsMapIterator`; either present already or add the missing piece.
P5.4 v2: `MapState.clear()` via point-delete-each-row — verify present.

**Decision in P5 — naming:** keep V2 suffix (forward-compatible) OR rename to drop suffix (drops legacy V1 sync). **Default: keep V2 suffix** since V1 sync is retained for Flink Table joins per prior session task #44. If user wants the rename, it's a separate cleanup PR (post-V12).

**P6, P7, P8, P9, P10, P11, P12 — stand as original plan** with these adjustments:

- **P7.2** uses `FlatStateCache` as the backing store instead of building `ReducingAggregatingCache` from scratch. Tasks: read FlatStateCache API, write a thin `ReducingAggregatingCache` adapter that exposes the spec-required surface (`tryFold`, `flushAllDirty`, etc.). If FlatStateCache lacks dirty-tracking, add it.
- **P8.2** (barrier drain in `snapshotState`): verify what `ForStRsAsyncKeyedStateBackend.snapshotState` already does. If it already calls `classifier.flushAll()`, extend to the two-phase pattern + cache drain.

### NEW PR — P12.0 (legacy cleanup, between P11 and P12)

**Files to delete:**
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsStateRequestClassifier.java`
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsStateExecutor.java`

**Verification tasks:**
- Grep for all references to these classes
- Migrate any callers to `VectorizedClassifier` / `VectorizedExecutor`
- Confirm no test exercises the legacy paths
- Commit deletion: `refactor(state-forst-rs): drop legacy per-row classifier+executor`

---

## Pre-dispatch readiness

| Check | Status |
|---|---|
| ForSt working tree clean | ✓ (modulo `nexmark` submodule + `META-INF/` build artifact) |
| Flink working tree clean | ✓ (modulo exploration artifacts: `docs/migrate/`, `findings.md`, `progress.md`, etc. — not plan-relevant) |
| ForSt branch | `forst-rs` at `c384b25b4` |
| Flink branch | `forst-rs-jdk25` at `ade472046ff` |
| Original plan committed | ✓ (`463378240` on ForSt) |
| This addendum committed | (next step) |
| Microbench harness | NEW per P0.1 |
| Spec audit re-validation | This addendum |

---

## Execution sequence post-reconciliation

Per the writing-plans/subagent-driven-development sequencing, after this addendum commits:

1. Dispatch P0.1 implementer (JMH harness setup) — UNCHANGED, the plan task stands.
2. P0.2 — UNCHANGED.
3. P0.3 — now uses REAL components for classifier/executor benches; pendingMisses stays stubbed.
4. P0.4 — Rust criterion benches — UNCHANGED.
5. P0.5 — GO/REVISE/STOP — now with real-component numbers, more meaningful gate.
6. After P0 gate passes, dispatch P1.1 implementer, then P1.2-P1.5 sequentially.
7. P2 onwards — work from the v2 tasks above, not the original plan's task text.

The original plan's task code blocks (test bodies, signatures, etc.) remain valid as **reference templates** even when the task itself collapses to a smaller refactor.

---

## Open questions for the user (not blocking P0 dispatch)

1. **Naming decision (Decision in P5):** Keep `V2` suffix on `ForStRsValueState` / `ForStRsMapState` indefinitely, or do a future rename PR? **Default: keep V2 suffix.** Defer.
2. **Legacy V1 sync state classes:** Retain alongside V2 (per Flink Table joins), or eventually drop? **Default: retain.** Out of V1 scope; decision deferred.
3. **`FlatStateCache` ↔ `ReducingAggregatingCache` naming:** Rename `FlatStateCache` to `ReducingAggregatingCache`, or wrap with an adapter? **Default: wrap with adapter** (P7.2 v2). Cleaner if you eventually want FlatStateCache for non-RMW use.

These are deferred per `feedback_delegation_style` — small lane decisions resolved during execution, not pre-dispatch gates.
