# ForSt-RS Whole-Program Vectorization Design

**Date:** 2026-05-15
**Author:** PMC (Flink + ForSt backend)
**Status:** Architectural index — individual sub-project specs follow

## Goal

Drive the `forst-rs` (Rust + FFM) backend to full capability + performance parity
with the community Java `forst` backend, then beyond. Every hot path is
end-to-end vectorized and zero-copy: Java→Rust input via off-heap Arrow
BinaryArray, Rust→Java output via the same Arrow layout consumed through an
FFM-`MemorySegment`-backed `DataInputView` (no `byte[]` copy on the return
path). All vectorization primitives live in one runtime per `TaskSlot` so all
sub-projects share the same `Arena`, the same buffer pool, and the same SIMD
helpers — yielding TLB / L3-cache friendliness on the hot path.

## Constraints (unchanged from existing forst-rs spec)

- Flink `TypeSerializer` API is unchanged.
- User-visible state semantics are unchanged.
- JDK 25 required (FFM, Java Vector API, CompactObjectHeaders).
- Backend-only changes (no Flink core or operator changes).
- V1 (sync) state types are **dropped**; only V2 async path remains.
- FlinkFileSystem layer (`fs/` package in forst) is **out of scope** for this
  design cycle; engine-side file I/O continues through the `forst-rs-io` crate.

## Capability inventory (forst vs forst-rs as of 2026-05-15)

| Area | forst (Java) | forst-rs | Gap |
|---|---|---|---|
| State types V2 (async) | n/a (legacy only) | Value, Map | List, Reducing, Aggregating |
| Request types | 13 specialised | 3 generic | 10 specialised request shapes |
| Executor | `ForStStateExecutor` | `VectorizedExecutor` (shipped in prior session) | timer / iter / merge specialisations |
| WriteBatch wrapper | `ForStDBWriteBatchWrapper` | not exposed in Java | full |
| TTL | `ForStDBTtlCompactFiltersManager` | engine fn exists | Java factory + wiring |
| Native metrics | `ForStNativeMetricMonitor` | none | full |
| Memory mgmt | `MemoryConfiguration`, `MemoryControllerUtils`, `SharedResources(+Factory)`, `ResourceContainer` | options only | full Java surface |
| Options | `OptionsFactory`, `ConfigurableOptionsFactory`, `ConfigurableOptions` | `ForStRsOptions` only | configurable + factory layer |
| Snapshot / Restore | Incremental + NativeFull; Heap-timers + Incremental + None | one strategy each | additional strategies |
| DataTransfer | Copy + Reusable strategies | scaffold in `keyed/sst/` only | full |
| Iterators | List + Map with 3 distinct map-iter shapes | `ForStRsMapIterator` (single shape) | List iter + per-shape map iters |
| Vectorized batch FFI | n/a | `frs_vectorized_batch_get/put/delete` | timer / iter / merge variants |
| Timer service | sync `ForStDBPriorityQueueSetFactory` | `ForStRsKeyGroupedInternalPriorityQueue` (non-vectorized) | vectorized timer service |

Roughly **40–60 classes** to port plus engine work for new FFI shapes —
beyond single-spec scope. This document is the architectural index; each
sub-project lands as its own spec.

## Architecture

```
              ┌───────────────────────────────────────────────────────┐
   TaskSlot   │                  VectorizedRuntime                    │
              │  ┌──────────────────────────────────────────────────┐ │
              │  │   Arena (long-lived, owns all off-heap mem)      │ │
              │  └──────────────────────────────────────────────────┘ │
              │   ColumnarBatchBuffer pool   SimdOps   FFI linker     │
              │   MemorySegmentDataInputView pool                     │
              └─────────▲────────┬─────────────────────┬──────────────┘
                        │ borrow │ return              │ borrow
                        ▼        │                     ▼
              ┌─────────────────────┐    ┌─────────────────────────┐
              │ VectorizedExecutor  │    │ BatchTimerService       │  SP3
              │  (state ops)        │    │  (timer ADD/REMOVE/POLL)│
              └─────────────────────┘    └─────────────────────────┘
                       ▲                          ▲
                       │                          │
   State types V2 ── SP1 ── List, Reducing, Aggregating + Value, Map
   Iter vectorization ── SP2 ── ITER (entry/key/value) Arrow streaming
   WriteBatch+TTL+Mem ── SP4 ── atomic write-set, TTL factory, SharedResources
   Snapshot+Restore+DT ── SP5 ── incremental SST upload/download via Arrow
```

`VectorizedRuntime` is the single entry point for vectorized hot-path
resources. Created per TaskSlot in
`ForStRsAsyncKeyedStateBackend.createStateExecutor()`, holds one `Arena`,
owns a pool of reusable `ColumnarBatchBuffer`s and `MemorySegmentDataInputView`
instances, and exposes the FFI linker. Each per-batch consumer borrows what
it needs and returns it on batch completion.

**Why this shape:** all off-heap allocations live in one `Arena` → one
virtual-memory page set per slot → better TLB / L3 locality. Pool reuse
keeps the Arena free-list cold. Sub-projects extend by adding new consumer
classes that borrow from the same pool — no new lifecycle code per
sub-project.

## Shared primitives layer

Five primitives. Sub-projects must use them rather than rolling their own
off-heap mechanics.

1. **`ColumnarBatchBuffer`** *(already shipped)* — Arrow BinaryArray
   (`offsets[count+1]` + `data`) backed by `MemorySegment` from the runtime
   Arena. `reset()`, `append(byte[])`, `append(DataOutputSerializer)`,
   `appendEmpty()`, `dataSegment()`, `offsetsSegment()`. Pool-managed.

2. **`MemorySegmentDataInputView` (new)** — implements Flink's
   `org.apache.flink.core.memory.DataInputView` backed by an FFM
   `MemorySegment` slice. Lets `TypeSerializer.deserialize(in)` read directly
   off-heap. Eliminates the `byte[]` copy on the get-result path. Pool-managed
   (one instance per executor thread; rewind to `(segment, offset, length)`
   on each get). Implementation: ~150 LOC, no copy, uses
   `MemorySegment.get(JAVA_*, off)` for primitive reads.

3. **`SimdOps` (new)** — Java Vector API utilities (with scalar fallback for
   environments without `jdk.incubator.vector`):
   - `prefixSum(int[] lengths, int[] outOffsets, int count)` — used when
     staging batches in two phases.
   - `xxh3Batch(MemorySegment data, int[] offsets, int count, long[] outHashes)`
     — batch hashes for cache routing.
   - `validityScanFound(byte[] validity, int n, int[] outIndices) → int` —
     collapse validity bitmap to a list of "found" indices.
   - `foldLongMax(MemorySegment, int[] offsets, int count) → long` — SIMD
     reducer fast path for known PoD aggregators.

4. **`BufferPool<T>` (new)** — minimal borrow/return wrapper over a
   `Deque<T>`. Underflows allocate a new instance via a `Supplier`. Used for
   `ColumnarBatchBuffer` and `MemorySegmentDataInputView`. No synchronization
   needed (one TaskSlot = one thread on the hot path).

5. **`VectorizedRuntime` (new)** — top-level container; created per TaskSlot;
   holds Arena + pools + linker + optional shared `FlatStateCache` for
   cross-state caching. `Closeable`; releases the Arena on close, blocks on
   any borrowed instance still outstanding.

## Cross-cutting FFI manifest

All new FFI symbols across the 5 sub-projects follow the established
contract: **caller-owned buffers, Arrow BinaryArray layout, no allocations
cross the FFM boundary**. New symbols by sub-project:

| Sub-project | New FFI symbol | Purpose |
|---|---|---|
| SP3 timers | `frs_vectorized_timer_add` | Add `(kg, ts, elem)` batch as PUTs into timer CF |
| SP3 timers | `frs_vectorized_timer_poll` | Atomic pop: read first N timers from prefix + delete |
| SP3 timers | `frs_vectorized_timer_remove` | Remove `(kg, ts, elem)` batch |
| SP1 state | *(none new — uses existing `frs_vectorized_batch_*`)* | List values use delimited serialization client-side |
| SP2 iter | `frs_vectorized_iter_open` | Open multi-prefix Arrow-streamed iterator |
| SP2 iter | `frs_vectorized_iter_next_batch` | Pull next N `(key,val)` entries into caller buffers |
| SP2 iter | `frs_vectorized_iter_close` | Release iterator |
| SP4 writebatch | `frs_writebatch_open` / `_put` / `_delete` / `_commit` / `_close` | Explicit WriteBatch (Java holds the handle) |
| SP4 memory | `frs_shared_block_cache_*` | Cross-CF cache facade |
| SP4 TTL | *(uses existing `frs_cf_set_compaction_filter_ttl`)* | Java factory only |
| SP5 snapshot | `frs_snapshot_export_arrow` | List SST files + checksum metadata as Arrow batch |
| SP5 transfer | `frs_sst_upload_async` / `_download_async` | Async SST transfer with progress callback |

## Sub-project SP3 — BatchTimerService (highest perf lever)

**Goal**: Q5 463s → ≤200s by batching timer ADD / REMOVE / POLL through the
vectorized FFI.

**Key layout**: `timer/{kg:2B}/{ts:8B-big-endian}/{element:var}`. The kg
prefix groups timers per key-group; big-endian ts means lexicographic prefix
scan = chronological order.

**Components**:
- `BatchTimerService implements InternalTimerService` — replaces
  `ForStRsKeyGroupedInternalPriorityQueue`. Holds borrowed
  `ColumnarBatchBuffer`s for timer-keys-add and timer-keys-remove.
- Timer ADDs flow through `VectorizedClassifier` as PUTs into a dedicated
  `timer` CF.
- Timer REMOVEs flow as DELETEs.
- Timer POLL is a new FFI `frs_vectorized_timer_poll(kg_prefix, limit)` that
  atomically reads + deletes the first N timers — avoids a read-then-delete
  race with concurrent ADDs.
- A per-kg cursor caches the next-expected-ts in Java to short-circuit
  empty-poll batches.

**Correctness invariants** (spec §Correctness Inv. 3):
- Timer POLL must return the lowest-ts entry per kg.
- Concurrent ADD must not be lost between read-and-delete — solved by the
  FFI-atomic poll.
- Watermark advance triggers `poll(kg, lim=64)` for each kg with pending
  timers.

**Tests**: `BatchTimerServiceTest` — ADD/POLL ordering, ADD-during-POLL race,
REMOVE during pending, watermark progression.

**Bench gate**: Q5 ≤ 200s.

## Sub-project SP1 — State-types V2 parity

**Goal**: List, Reducing, Aggregating get V2 vectorized paths matching the
existing Value/Map V2 path.

**Components**:
- `ForStRsListStateV2<K, N, V> implements ListState<V>, ForStRsInnerTable` —
  value serialization via `ListDelimitedSerializer` (port from forst, ~60
  LOC). On `add(v)`: append-encode → MERGE via FFI; on `get()`: deserialize
  delimited list.
- `ForStRsReducingStateV2`, `ForStRsAggregatingStateV2` — both use
  read-modify-write via the existing `frs_get_and_put` FFI; reducer fn runs
  on the Java side post-fetch. `SimdOps.foldLongMax`/etc. for known PoD
  reducers.
- `VectorizedClassifier` already routes `LIST_GET` / `LIST_ADD` /
  `REDUCING_GET` / `AGGREGATING_GET` / etc. — no classifier changes.

**Vectorization**: List append is sequential per-key by Flink contract, but
N keys' appends batch into one vectorized PUT. Reducing/Aggregating
read-modify-writes also batch (N keys' RMW operations in one FFI call).

**Drop V1**: delete `state/ForStRsValueState.java`,
`state/ForStRsListState.java`, `state/ForStRsMapState.java`,
`state/ForStRsReducingState.java`, `state/ForStRsAggregatingState.java`.
Verify no consumer references.

**Tests**: parity tests against forst's `ListStateTest` / etc. — round-trip
+ edge cases (empty add, clear during iter).

**Bench gate**: per-type micro-bench ≥ 2× rocksdb on List / Reducing /
Aggregating.

## Sub-project SP2 — Iterator vectorization

**Goal**: `MAP_ITER` (entry / key / value) returns Arrow-streamed batches;
Q7 / Q8 feature parity.

**New FFI**:
- `frs_vectorized_iter_open(db, cf, prefix_offsets, prefix_data, count, out_iter_handle)`
  — multi-prefix iterator.
- `frs_vectorized_iter_next_batch(iter, max_n, out_key_offsets, out_key_data, out_val_offsets, out_val_data, out_count)`
  — pull next ≤ `max_n` entries.
- `frs_vectorized_iter_close(iter)`.

**Components**:
- `VectorizedMapIterator` — wraps the FFI iter handle, exposes
  `hasNext` / `next` over the Arrow batch. Refills on exhaustion.
- Specializations:
  - `MAP_ITER_KEY` — only key buffer populated server-side (val skipped).
  - `MAP_ITER_VALUE` — only val buffer populated.
  - `MAP_ITER` — both.
- Iter requests still process sequentially (one prefix = one iter), but each
  `next_batch` call returns ≤ 256 entries in one FFM crossing.

**Zero-copy decode**: iter consumers use `MemorySegmentDataInputView` to
deserialize keys / values directly off-heap.

**Tests**: map iter on 100k-entry state, validate ordering + batching at
boundary N = 255 / 256 / 257.

**Bench gate**: Q7/Q8 complete; MapState iter throughput ≥ 1M entries/s.

## Sub-project SP4 — WriteBatch + TTL + Memory mgmt

**Goal**: feature parity for atomic batch writes, TTL, shared memory mgmt.

**WriteBatch**:
- New FFI: `frs_writebatch_open(db) → handle`,
  `frs_writebatch_put(handle, kos, kd, vos, vd, n)`,
  `frs_writebatch_delete(handle, kos, kd, n)`,
  `frs_writebatch_commit(handle)`, `frs_writebatch_close(handle)`.
- Java `ForStRsDBWriteBatchWrapper` — accumulates ops in Java-side
  `ColumnarBatchBuffer`s, flushes on `flushIfNeeded(threshold)` or
  `flushAll()` boundary.

**TTL**:
- Java `ForStRsDBTtlCompactFiltersManager` — `setTtlForState(stateName,
  ttlMs, stateType)` calls existing `frs_cf_set_compaction_filter_ttl`.
  Single file, ~80 LOC.

**Memory mgmt**:
- Java `ForStRsMemoryConfiguration`, `ForStRsSharedResources`,
  `ForStRsSharedResourcesFactory` — facade over engine's existing block-cache
  + WBM. Surface `getSharedBlockCacheCapacity`, `getCurrentBytes` via
  existing `frs_db_write_buffer_manager_*` symbols.

**Native metrics** (folded in here):
- Java `ForStRsNativeMetricMonitor` + `ForStRsNativeMetricOptions` — register
  Flink metric group, poll engine via `frs_db_write_buffer_manager_*` and a
  new `frs_db_stats_arrow` FFI that returns engine stats as Arrow batch.

**Configurable options** (folded in here):
- Java `ForStRsOptionsFactory`, `ForStRsConfigurableOptionsFactory`,
  `ForStRsConfigurableOptions` — port from forst's options layer; expose
  user-facing knobs (write-buffer size, max-write-buffer-number, target-file-size,
  background-compactions, etc.) via Flink `ConfigOption` surface.

**Tests**: writebatch atomicity (crash during put → no partial state); TTL
expiry verification; memory pressure (WBM trigger flush).

**Bench gate**: no perf regression on existing benches.

## Sub-project SP5 — Snapshot + Restore + DataTransfer

**Goal**: incremental checkpoint + restore + async SST transfer.

**Snapshot**:
- Engine has `frs_create_incremental_checkpoint_at` already.
- New: `frs_snapshot_export_arrow` returns list of SST files + checksums as
  Arrow `RecordBatch`. Java enumerates and schedules uploads.

**Restore**:
- Engine has `frs_db_open_from_incremental`, `frs_db_open_from_checkpoint`.
- Java `ForStRsRestoreOperation` (exists) extended with
  `incrementalRestore(StateHandle)` and `heapTimersRestore(StateHandle)`.

**DataTransfer**:
- New FFI: `frs_sst_upload_async(file_path, target_uri, callback)`,
  `frs_sst_download_async(...)` — non-blocking SST file movement.
- Java `ForStRsDataTransferStrategy` — Copy + Reusable variants (port from
  forst).

**Tests**: round-trip snapshot/restore on 1 GB state; incremental savepoint
resume; concurrent upload during checkpoint.

**Bench gate**: 1 GB checkpoint + restore in ≤ 30s; no data-loss test.

## Sequencing + benchmark gates

| Order | Sub-project | Why this position | Bench gate |
|---|---|---|---|
| 1 | SP3 Timers | Single biggest perf lever (Q5) | Q5 ≤ 200s |
| 2 | SP1 State V2 parity | Pattern-setter for remaining state types | Per-type ≥ 2× rocksdb |
| 3 | SP2 Iter vectorization | Unblocks Q7 / Q8 | Q7/Q8 complete; ≥ 1M entries/s |
| 4 | SP4 WriteBatch + TTL + Mem | Operational feature parity | No perf regression |
| 5 | SP5 Snapshot + Restore + DT | Last; depends on SP4 stable | 1 GB checkpoint ≤ 30s |

Each sub-project gets its own
`docs/superpowers/specs/YYYY-MM-DD-forst-rs-<name>-design.md` brainstormed
BEFORE implementation. This whole-program doc is the architectural index they
all reference for shared primitives.

## Cross-cutting risks

1. **`Arena` lifetime across sub-projects** — if `VectorizedRuntime` closes
   before in-flight `MemorySegmentDataInputView` is consumed, SIGSEGV.
   Mitigation: explicit `runtime.close()` blocks on all borrowed instances
   being returned; pool tracks outstanding-borrows counter.
2. **Vector API enablement** — `--add-modules jdk.incubator.vector` JVM flag
   required. Mitigation: detect via reflection at `SimdOps` init; fall back
   to scalar implementations if unavailable (no functional difference, perf
   only).
3. **Buffer pool sizing** — under-sized pool → allocator thrash; over-sized
   → wasted memory. Mitigation: pool grows on borrow miss, never shrinks;
   initial size = 2× expected concurrent batches.
4. **`MemorySegmentDataInputView` reuse** — same instance reused across gets
   in a batch. Mitigation: `rewind(segment, offset, length)` resets state;
   pool returns the view to the supply on `release()`; never store the view
   across batch boundaries.
5. **V1 drop blast radius** — dropping V1 state types may break test
   fixtures or sync-API consumers. Mitigation: grep for V1 references before
   delete; route any remaining consumers through V2 + `complete().join()`.

## Testing strategy

- Each sub-project ships unit tests against an in-memory engine.
- Each sub-project ships a perf micro-bench (similar to existing
  `crates/forst-rs-bench/`).
- Cross-cutting `VectorizedRuntimeStressTest` (Java) — 8 concurrent executors
  borrowing from one runtime pool, mixed ops, 10 M ops total, no resource
  leaks via `Arena.scope` assertions.

## Success criteria summary

| Query | Baseline | Target | Source |
|---|---|---|---|
| Q0 | 29s | 29s (no regression) | spec §Success |
| Q3 | 188s | ≤ 28s | spec §Success |
| Q5 | 463s | ≤ 200s | spec §Success |
| Q7 | N/A | Completes | feature parity |
| Q8 | N/A | Completes | feature parity |

Plus capability parity for: ListState, ReducingState, AggregatingState,
WriteBatch atomicity, TTL, native metrics, memory configuration, snapshot /
restore strategies, async SST transfer.

## Out of scope (deferred to a later cycle)

- FlinkFileSystem Java layer (`fs/` package — 22 forst classes for caching
  FS, file mapping, byte-buffer streams). Engine-side caching/S3 continues
  through `forst-rs-io`.
- V1 sync API. Dropped.
- Arrow-Java end-to-end. Considered and rejected for this cycle in favour of
  raw `MemorySegment` + Arrow BinaryArray layout for FFI efficiency.

## Related specs

- `docs/superpowers/specs/2026-05-15-forst-rs-vectorized-executor-design.md`
  — the executor + columnar buffer spec that this whole-program doc extends.
- `docs/superpowers/specs/2026-05-14-nexmark-q3-perf-analysis.md` — Q3 perf
  root cause analysis; informs SP1/SP2 prefetch design.
