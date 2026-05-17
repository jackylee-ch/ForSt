# SP6 — V1 Sync Path Vectorization

**Date:** 2026-05-16
**Status:** Design — implementation pending
**Parent:** `2026-05-15-forst-rs-whole-program-vectorization-design.md` (extends; not replaced by)
**Constraint:** Backend-only changes (no Flink core / no Flink Table runtime changes).

> **Umbrella spec:** [2026-05-16-forst-rs-vectorized-parity-design.md](2026-05-16-forst-rs-vectorized-parity-design.md). This SP extends the umbrella's dispatch table (§1) to cover the V1 sync path in addition to async-v2, bringing components 1–4, 7–17 (Java runtime + all state types) and A–G (Rust FFI + engine ops) to the synchronous API surface. Any change to this SP that touches a dispatch contract or metrics namespace defined by the umbrella must update the cross-reference in the same PR.

## Why this sub-project exists

The prior four spec docs (whole-program, SP1, SP2, SP5) all targeted the **async-v2 state path**. Real-world benchmarks (Nexmark Q3 100M, this session) showed that **Flink Table runtime in Flink 2.2.1 does not use async-v2 for joins/aggregations** — it calls `createKeyedStateBackend` (the synchronous V1 path). Forst-rs's V1 sync path was prematurely deleted earlier in the project and then restored; with `frs_get_fast` added it now runs Q3 in **74.8 s vs rocksdb 33.5 s** — still 2.2× slower than rocksdb-local and 1.8× slower than community forst.

The vectorized executor + ColumnarBatchBuffer + `frs_vectorized_batch_*` FFI that the earlier specs delivered **does not run on Flink Table workloads** because those operators don't talk to the async-v2 backend. SP6 closes that gap: bring the same vectorization + zero-copy primitives into the V1 sync hot path that Flink Table actually uses.

## Goal

Forst-rs Q3-Q7 on V1 sync path ≤ rocksdb-local × 1.1 — i.e. forst-rs becomes the **fastest backend on Flink Table SQL workloads**, not just on async-v2-aware ones.

## Constraints

- **Backend-only changes**. Cannot modify Flink core, flink-runtime, flink-table-runtime, or any TypeSerializer.
- **Flink's `State` interface contract is fixed**. `state.value()` must return `V` synchronously — no lazy thunks or futures.
- **JDK 17 + JDK 25 compatibility**. The V1 sync path is exercised under JDK 17 by Flink Table operators today.
- **User-visible state semantics unchanged**.

## Honest review against the invariants

Within those constraints, what's actually achievable on a sync API?

| Path | Vectorized at FFI? | Zero-copy? | Achievable? |
|---|---|---|---|
| Writes (put / update / list-add / reducing-add / aggregating-add) | ✅ via deferred-flush of off-heap buffer | ✅ no byte[] in the data path | yes |
| Reads (value / mapState.get / contains) | ❌ — sync API contract forbids deferring a single read | ✅ via MemorySegmentDataInputView + frs_get_into_buf into a reusable off-heap buffer | yes (vectorization at FFI impossible on read; everything else is) |
| List-add (merge-op path) | ✅ frs_vectorized_batch_merge | ✅ | yes (with optional new FFI) |
| Reducing / Aggregating add (batched RMW) | ✅ vectorizedBatchGet + java-fold + vectorizedBatchPut | ✅ | yes |
| Iterators (`mapState.entries` etc.) | ❌ — SP2 future work | ✅ | partial (zero-copy only; SP2 lands FFI vectorization later) |

The ONLY invariant this design can not fully meet under the constraint is FFI-vectorized reads. Everything else is both vectorized AND zero-copy.

## Architecture

```
TaskSlot
├─ V1SyncRuntime  (new, per-slot, lazy-init)
│   ├─ Arena (off-heap, slot lifetime)
│   ├─ Pooled MemorySegmentDataOutputView (impl of Flink's DataOutputView,
│   │   backed by a slot from the Arena — TypeSerializer.serialize(v, view)
│   │   writes bytes directly off-heap; NO byte[])
│   ├─ Pooled MemorySegmentDataInputView (impl of Flink's DataInputView,
│   │   rewound per read — TypeSerializer.deserialize(view) reads off-heap)
│   ├─ Shared OffHeapFlatStateCache (entries as MemorySegment slices,
│   │   size-bounded with LRU eviction; per-TaskSlot memory control)
│   └─ FFI linker
│
├─ ForStRsValueState<K,V>       ─┐
├─ ForStRsMapState<K,UK,UV>     ─┤  Each owns a persistent
├─ ForStRsListState<K,V>        ─┤  ColumnarBatchBuffer pair (keys + values),
├─ ForStRsReducingState<K,V>    ─┤  a composite-key staging region,
└─ ForStRsAggregatingState<...> ─┘  and a reusable read-result buffer.
                                    All buffers borrowed from V1SyncRuntime.
```

Flush triggers per state:
1. Write buffer fills (count > N or bytes > B)
2. `setCurrentKey()` moves the operator to a different key
3. Checkpoint barrier (snapshot starts)
4. First read of a key that the state has dirty (writeCache consultation handles this implicitly)
5. `clear()` / `close()`

## Components

### New (backend-only)

- **`V1SyncRuntime`** — per-TaskSlot container. Owns the Arena, buffer pools, cache, linker. Lazy-init on first state creation.
- **`MemorySegmentDataOutputView`** — implements Flink's `org.apache.flink.core.memory.DataOutputView`, backed by a reusable off-heap `MemorySegment`. Tracks a write cursor. `TypeSerializer.serialize(v, view)` writes bytes through `writeByte` / `writeInt` / `write(byte[])` etc. — all routed to off-heap memory. Pooled (one per executor thread).
- **`MemorySegmentDataInputView`** — implements `DataInputView`, backed by an off-heap `MemorySegment` slice. `rewind(segment, offset, length)` resets it; `TypeSerializer.deserialize(view)` reads off-heap. Pooled.
- **`OffHeapKeyStaging`** — per-state composite-key region. Holds the prefix bytes (`q/` or `k/` + key-group + stateName + `/`) pre-written once; reset-cursor-and-append the serialized user key per call. The whole region is one `MemorySegment` slice from the runtime's Arena.
- **`BufferPool<ColumnarBatchBuffer>`** — reusable write buffers. `borrow()` returns a fresh-or-reused buffer; `return(buffer)` resets and pools.
- **`OffHeapFlatStateCache`** — replaces the existing on-heap `HashMap<ByteArrayKey, byte[]>` readCache / writeCache. Stores entries as `MemorySegment` slices (no byte[]). LRU eviction on byte budget; configured via `state.backend.forst-rs.cache.size`. Thread-safety: same single-threaded-per-slot contract as today.

### Modified (V1 sync state types)

- **`ForStRsValueState`**:
  - Remove `byte[] composite(...)`; replace with `composeIntoStaging()` which writes to the staging region.
  - Replace `linker.getFast(byte[])` with `linker.getIntoBuf(staging.segment, result_buffer)`; deserialize via MemorySegmentDataInputView.
  - Add write buffer + `flush()`.
- **`ForStRsMapState`**:
  - Same pattern.
  - Replace `flushMapWriteCache` (uses legacy `linker.batchPut(byte[][], byte[][])`) with `linker.vectorizedBatchPut(offsetsSeg, dataSeg, ...)`.
- **`ForStRsListState`**:
  - Path A: CF has `list_append` merge operator → append-via-merge ColumnarBatchBuffer, flush via `frs_vectorized_batch_merge` (1 FFI per N).
  - Path B: no merge op → 2-phase batched RMW (vectorizedBatchGet + java-append + vectorizedBatchPut).
  - Backend selects path A by configuring the CF with the engine's `list_append` merge operator at creation time.
- **`ForStRsReducingState`** / **`ForStRsAggregatingState`**:
  - Buffer pending adds (key + new-value pairs).
  - On flush: `vectorizedBatchGet` current values → java-side fold via reduce/aggregate fn → `vectorizedBatchPut` new values.
- **`ForStRsKeyedStateBackend`**:
  - `setCurrentKey()` hook: flush dirty states.
  - `applyToAllKeys` / `snapshot`: flush before iterating / snapshotting.

### Rust FFI additions

- **`frs_vectorized_batch_merge`** — optional, only if Path A for ListState is chosen. Mirrors `frs_vectorized_batch_put` but invokes `WriteBatch::merge`. ~50 LOC + 1 test.

## Data flow on the hot path

### Write (state.put / update / map.put / list.add / reducing.add / aggregating.add)

1. `composite_key_region.resetCursorAfterPrefix()`
2. `keySerializer.serialize(uk, outputView_pointed_at_key_region)` — bytes written directly off-heap
3. `write_buffer.appendKey(key_region.segment, length)` — records `(offset, length)`; shares the same off-heap region (the region holds the prefix in a stable slot; the user-key bytes are copied to the buffer's keys segment via `MemorySegment.copy(...)` — one off-heap-to-off-heap copy, no Java heap byte[])
4. `valueSerializer.serialize(v, outputView_pointed_at_value_buffer)` — same direct off-heap path
5. `write_buffer.commitValue(length)`
6. if `write_buffer.count > THRESHOLD`: `flush()` → `linker.vectorizedBatchPut(keys_seg, values_seg, count)` — 1 FFI per N
7. `writeCache.putOffHeap(key_slice, value_slice)` — consulted by subsequent reads before the buffer flushes

➜ Vectorized ✓ — Zero-copy ✓ (no byte[] in the data path).

### Read (state.value / mapState.get / contains / list.get)

1. `composite_key_region.resetCursorAfterPrefix()`
2. `keySerializer.serialize(currentKey, outputView_pointed_at_key_region)`
3. `cache_hit = writeCache.lookup(key_slice)` — if hit, `inputView.rewind(cache_entry_segment, len)` → `valueSerializer.deserialize(inputView)` → return V (zero-copy)
4. `cache_hit = readCache.lookup(key_slice)` — same
5. Miss: `linker.getIntoBuf(key_region.segment, result_buffer_segment)` — 1 FFI; result written into the state's reusable off-heap `result_buffer` segment
6. `inputView.rewind(result_buffer, valLen)` → `valueSerializer.deserialize(inputView)` → return V
7. `readCache.insertOffHeap(key, result_buffer.slice(0, valLen))`

➜ Vectorized at FFI ✗ (per-call by Flink contract) — Zero-copy ✓

### List-add (Path A: merge-op)

1. Stage composite key (same as write)
2. Serialize the new element into the values buffer (one element, no read)
3. Append entry to MERGE ColumnarBatchBuffer
4. On flush: `linker.vectorizedBatchMerge(keys, values, count)` — 1 FFI per N

The engine's `list_append` merge operator resolves merges at flush/read time. Reads call `linker.getIntoBuf(...)` and receive the accumulated bytes; the state deserializes them as a delimited list.

### Reducing/Aggregating-add

1. Stage composite key + serialize new element into pending buffer
2. On flush:
   - `linker.vectorizedBatchGet(pending_keys_seg, out_offsets, out_data, out_validity, ...)` — single FFI, results into our reusable off-heap segments
   - Java-side fold: for each entry, `inputView.rewind(current_value_slice)` → deserialize → `reducer.add(current, new_value)` → `outputView.write(folded)` into the new-values buffer
   - `linker.vectorizedBatchPut(keys_seg, new_values_seg, count)` — single FFI

➜ Vectorized ✓ (2 FFI per N) — Zero-copy ✓

## Error handling

- **FFI errors** → `FrsBackendException` with `FrsStatus`. Same convention as today.
- **Buffer overflow on flush** → caught in `vectorizedBatchPut` (`BUFFER_TOO_SMALL`). Recovery: grow then retry once; if growth fails, surface.
- **Read miss** → `out_val_len = 0` → return `null` for ValueState, "absent" for MapState.
- **Dirty-state-on-snapshot** → snapshot pre-flushes all states. In-flight writes durable before checkpoint barrier passes downstream.
- **Concurrent `setCurrentKey`** → V1 is single-threaded per RecordContext (Flink contract). Buffer flush on key-change happens synchronously before the new key's operations begin.
- **Cache eviction during write** → writeCache holds in-flight writes; never evicted before commit. readCache freely evictable.

## Testing

- **Unit per component**: `MemorySegmentDataOutputViewTest`, `MemorySegmentDataInputViewTest`, `OffHeapFlatStateCacheTest`, `OffHeapKeyStagingTest`, `BufferPoolTest`.
- **State-type roundtrip**: existing `state/ForStRs*Test.java` extended with assertions that the hot path allocates zero Java heap byte[] (via `ThreadMXBean.getThreadAllocatedBytes` or `GcDetectingMatcher`).
- **Cache coherence**: write-then-read-same-key tests verify writeCache → flush → engine → readCache eviction sequence.
- **Cross-serializer correctness**: parity test runs every Flink built-in `TypeSerializer` through `MemorySegmentDataOutputView` then back through `MemorySegmentDataInputView`; result must equal the original.
- **Micro-bench**: `V1VectorizedSyncBench` — 1M put-then-get-then-put loop. Bench gate: ≥ 2× current V1.
- **End-to-end (Nexmark)**: Q3 / Q4 / Q5 / Q7 across all three backends. Bench gate per query: forst-rs ≤ rocksdb-local × 1.1.

## Sequencing + bench gates

| Phase | Scope | Bench gate |
|---|---|---|
| 6.1 | Primitives: `MemorySegmentDataOutputView`, `MemorySegmentDataInputView`, `OffHeapKeyStaging`, `OffHeapFlatStateCache`, `BufferPool` | Unit tests pass |
| 6.2 | ValueState fully migrated | ValueState micro-bench ≥ 2× current |
| 6.3 | MapState fully migrated | Q3 ≤ rocksdb-local × 1.1 (≈ 37 s) |
| 6.4 | List + Reducing + Aggregating migrated | Q4 / Q5 / Q7 ≤ rocksdb-local × 1.2 |
| 6.5 | Optional `frs_vectorized_batch_merge` FFI + list_append CF wiring | Q4 specifically benefits (~30% extra on list-add-heavy queries) |

## Risks

1. **`MemorySegmentDataInputView` correctness across Flink TypeSerializers**. Serializers internally call `readInt`, `readLong`, `read(byte[], off, len)`, etc. We must implement every method correctly including endianness. *Mitigation*: parity-test against `DataInputDeserializer` on a corpus of built-in serializer outputs.
2. **Buffer-pool exhaustion under burst load**. A slot with many concurrent states could starve. *Mitigation*: grow-on-demand pool with a ceiling at the slot's configured memory budget; fail-fast with a clear error if hit.
3. **Composite-key staging region overflow**. User keys can be arbitrarily large. *Mitigation*: grow region on overflow (existing pattern in `ColumnarBatchBuffer.ensureData`).
4. **Cache coherence between writeCache/readCache/engine**. Flush ordering matters. *Mitigation*: writeCache → flush → engine → invalidate readCache. Single-threaded per slot keeps this deterministic.
5. **Serializer side-effects**. Some TypeSerializers may have stateful behaviour (reusable instances). *Mitigation*: our DataOutputView treats `serialize()` as opaque — we never inspect the produced bytes for semantics, only the byte count.
6. **List_append merge operator availability**. The engine has the operator but the CF must be created with it. *Mitigation*: detect on backend init; fall back to Path B (2-phase RMW) if absent.

## Out of scope

- Iterator vectorization at the FFI level (covered by **SP2** — needs `frs_vectorized_iter_*` FFI).
- Async-v2 state path optimization (covered by **SP1**).
- Snapshot/restore vectorization (covered by **SP5**).
- Flink upstream changes — forbidden by the project constraint.
- B (cross-state WriteAggregator) — explicitly deferred as a possible future iteration if A's buffer fill rate proves low on real workloads.

## Related specs

- `2026-05-15-forst-rs-vectorized-executor-design.md` — original executor design (async-v2 path)
- `2026-05-15-forst-rs-whole-program-vectorization-design.md` — whole-program index
- `2026-05-16-forst-rs-sp1-state-types-v2-design.md` — async-v2 state types
- `2026-05-16-forst-rs-sp2-iterator-vectorization-design.md` — async-v2 iterator
- `2026-05-16-forst-rs-sp5-snapshot-restore-datatransfer-design.md` — snapshot/restore/transfer
