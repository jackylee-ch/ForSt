# SP1 — State Types V2 Parity (List / Reducing / Aggregating)

**Date:** 2026-05-16
**Status:** Design — implementation pending
**Parent:** `2026-05-15-forst-rs-whole-program-vectorization-design.md` §SP1

> **Umbrella spec:** [2026-05-16-forst-rs-vectorized-parity-design.md](2026-05-16-forst-rs-vectorized-parity-design.md). This SP implements components 9–13 (Java state primitives ListState, ReducingState, AggregatingState) and components B + F (Rust merge-operator FFI + engine list-concat logic) from the umbrella's §2 component table. Any change to this SP that touches a contract defined by the umbrella must update the cross-reference in the same PR.

## Goal

Bring `ListState`, `ReducingState`, and `AggregatingState` to the V2 vectorized
path. Today only `ValueState` and `MapState` flow through `VectorizedExecutor`;
the other three exist as V1 sync scaffolds that aren't wired anywhere.

Drop V1 (`state/ForStRsValueState.java`, `ForStRsListState.java`,
`ForStRsMapState.java`, `ForStRsReducingState.java`,
`ForStRsAggregatingState.java`) once the V2 replacements are correct.

## Why this needed a redesign

The session-2 attempt to wire `ForStRsListStateV2` exposed a hot-path mismatch:
`LIST_ADD` is **append-merge** semantics, not put. The current
`VectorizedClassifier` partitions requests into GET / PUT / DELETE buffers
— there is no slot for "merge an element onto an existing list value". A naive
read-modify-write per `LIST_ADD` would break batching (one read + one write per
add, no vectorization).

`ReducingState` and `AggregatingState` have a similar issue: their `add(v)`
folds the new element into the current state via a user-provided
reducer/aggregator function, which is Java code that can't be pushed to the
engine.

## Approach (3 options, recommendation = A)

### A — Engine merge-operator + 4th classifier slot (recommended)

Add a `MERGE` op type in `VectorizedClassifier` alongside GET / PUT / DELETE.
List CFs are configured at creation with the existing engine `list_append`
merge operator. `LIST_ADD` routes through the new MERGE slot and dispatches
via a new FFI `frs_vectorized_batch_merge` (mirror of `frs_vectorized_batch_put`,
but the engine `WriteBatch::merge` is invoked instead of `put`). Reads return
the accumulated bytes; the engine resolves the merge chain at flush/read time.

**Pros**: Native to the engine's merge-operator design. Vectorized end-to-end
(one FFM call per N adds). Reducing/Aggregating use the same path with a custom
reducer registered server-side OR a Java-side fold (see B/C).

**Cons**: Each state-type's CF needs the right merge operator wired at
creation. Reducing/Aggregating need an additional decision — engine-side
operator (faster, fixed user functions) or Java-side fold (slower, supports
arbitrary `ReduceFunction`).

### B — Read-batch / fold / Write-batch in a 2-phase executor

Extend `VectorizedExecutor` to support a "RMW" op type. For
Reducing/Aggregating: phase 1 reads all keys (vectorized batch GET), phase 2
runs the Java reducer per request on the returned values, phase 3 writes all
results (vectorized batch PUT). Each phase is one FFM call.

**Pros**: Works for arbitrary user-provided reducers. Java-side fold means no
engine-side custom code path.

**Cons**: 2× FFM crossings per batch (read + write). Doesn't help ListState
because list adds need ordering semantics that get/put can't preserve under
concurrent reads.

### C — Java-side accumulator with periodic flush

Buffer LIST_ADD / REDUCING_ADD / AGGREGATING_ADD in a Java-side accumulator
keyed by (record-key, state-name). Flush periodically as a single vectorized
PUT (overwrite). The accumulator stitches together the read-then-fold work in
Java.

**Pros**: No new FFI. Pure Java change.

**Cons**: Memory unbounded by accumulator size; checkpoint semantics get
weird (accumulator state must be flushed at checkpoint boundary, increasing
checkpoint latency); breaks under concurrent reads of the same key by other
operators (the accumulator's value is not visible to a fresh `get`).

### Recommendation

**A for ListState** (merge-operator is a perfect fit; one FFI per N adds).

**B for Reducing/Aggregating** (Java-side reducer must run; 2-phase executor
adds one extra FFM crossing per batch but keeps full vectorization).

## Components

### Rust FFI (new)

```c
int frs_vectorized_batch_merge(
    FrsDb, FrsCfHandle,
    const int32_t* key_offsets, const uint8_t* key_data,
    const int32_t* operand_offsets, const uint8_t* operand_data,
    size_t count);
```

Direct mirror of `frs_vectorized_batch_put`; calls `WriteBatch::merge` instead
of `put`. ~50 LOC including tests.

### Engine (Rust)

- `list_append` merge operator already exists
  (`crates/forst-rs-storage/src/merge_operator.rs::ListAppendMergeOperator`).
- New CF-creation overload that takes a merge-operator name —
  `frs_db_create_cf_with_merge` is already exported. Wire it for list-state
  CFs.

### Java — VectorizedClassifier

- Add `MERGE` op type and matching `mergeKeys` + `mergeValues` buffers.
- Route `LIST_ADD`, `LIST_ADD_ALL` through the merge path.
- Route `LIST_UPDATE` through PUT (full overwrite).
- `REDUCING_ADD` / `AGGREGATING_ADD` go to a new `RMW` op type.

### Java — VectorizedExecutor extensions

- `executeMerges()` — single FFM call to `frs_vectorized_batch_merge`.
- `executeRmws()` — two-phase: batch-get, Java-side fold per request, batch-put.

### Java — V2 state types

- `ForStRsListStateV2<K, N, V>` — extends `AbstractListState`. `add(v)` builds
  a merge request; `get()` does a get + deserialize via
  `ListDelimitedSerializer`.
- `ForStRsReducingStateV2<K, N, V>` — extends `AbstractReducingState`. `add(v)`
  builds an RMW request carrying the user's `ReduceFunction`.
- `ForStRsAggregatingStateV2<K, N, IN, ACC, OUT>` — extends
  `AbstractAggregatingState`. Same pattern with `AggregateFunction`.

### Wire-up

- `ForStRsAsyncKeyedStateBackend.create(...)` returns the new V2 types for
  LIST / REDUCING / AGGREGATING.
- CF-routing: list-states get their own merge-enabled CF when `cf.mode = per-state`.
  Under `cf.mode = single` (default), the single CF must have the list-append
  merge operator registered at backend init.

### Drop V1

Delete:
- `state/ForStRsValueState.java`
- `state/ForStRsListState.java`
- `state/ForStRsMapState.java`
- `state/ForStRsReducingState.java`
- `state/ForStRsAggregatingState.java`

Grep for references in `keyed/` adapters and update them to route through V2.

## Tests

- Rust: `frs_vectorized_batch_merge` round-trip (3 entries, merge produces
  delimited concatenation).
- Java: ListStateV2 — add/get/clear, add-all, ordering preserved across
  multiple adds.
- Java: ReducingStateV2 with `sum`, `min`, `max` reducers.
- Java: AggregatingStateV2 with `avg` aggregator (separate ACC + OUT types).
- Parity: each new V2 state type vs the corresponding forst test (binary
  format compatible since we use the same delimited serializer).

## Bench gates

- ListState `add(v)` throughput ≥ 2× rocksdb (the merge-operator path
  amortizes the read cost).
- ReducingState `add(v)` throughput ≥ 1× rocksdb (2-phase RMW has higher
  per-op FFI cost than rocksdb's single-call RMW; matching rocksdb is the
  acceptable bar).

## Risks

1. **Merge-operator misconfiguration** — if a list-state CF lacks the
   merge operator, `frs_vectorized_batch_merge` is a no-op-with-error. Fix
   at CF-creation time; assert at backend init.
2. **V1 drop cascade** — keyed-state backend has wiring for V1 types in
   `ForStRsKeyedStateBackend.java`. Need to grep + replace before deleting V1
   files.
3. **Reducer determinism** — user-provided `ReduceFunction` is called from
   the executor thread (the only thread on the hot path). Same contract as
   rocksdb backend; should be unproblematic.
4. **Aggregator ACC/OUT type difference** — `AggregateFunction<IN,ACC,OUT>`
   stores `ACC` in state but returns `OUT`. V2 type must hold the ACC
   serializer separately and apply `getResult(acc)` on read.
