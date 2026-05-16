# ForSt-RS Vectorized Executor Redesign

## Goal

Replace the per-request object-based execution model with a columnar, zero-copy, vectorized pipeline that eliminates heap allocations on the hot path, reduces FFM crossing overhead, and enables SIMD-accelerated batch operations. Target: 1.3× faster than rocksdb/JDK17 on all stateful Nexmark queries.

## Constraints

- Flink's TypeSerializer API is unchanged
- User-visible state semantics are unchanged
- Correctness is non-negotiable — no reordering of causally dependent operations
- Backend-only changes (forst-rs Java + Rust engine)
- JDK 25 required (FFM, Vector API, CompactObjectHeaders)

## Correctness Invariants

1. **Batch isolation**: Flink's async v2 framework guarantees that requests for the same key are never in the same batch (RecordContext lock). The executor may freely reorder within a batch by operation type.
2. **Flush-before-scan**: Any cached/deferred writes MUST be flushed to the engine before prefix scan or iterator operations.
3. **Timer ordering**: Timer poll returns the lowest-timestamp entry. Batch timer operations must not skip or reorder entries.
4. **Future completion**: Every request's InternalAsyncFuture must be completed exactly once, with the correct result, regardless of batch size or operation mix.

## Architecture

### Component 1: ColumnarBatchBuffer

Pre-allocated off-heap buffer in Arrow BinaryArray format.

```
struct ColumnarBatchBuffer {
    offsets: MemorySegment  // int[capacity+1], off-heap
    data: MemorySegment     // byte[dataCapacity], off-heap
    count: int              // current entry count
    dataPos: int            // current write position in data
}
```

**Operations:**
- `reset()` — sets count=0, dataPos=0 (no deallocation)
- `append(DataOutputSerializer src)` — copies serialized bytes from src's internal buffer into data segment, records offset
- `sliceAt(int index)` — returns (offset, length) for entry at index
- `memorySegment()` — returns the data segment pointer for FFM

**Sizing:** Initial 64KB data + 4K offsets. Grows 2× if exceeded (rare — batch sizes are bounded by async controller).

**Lifecycle:** Allocated once per executor in a long-lived Arena. Survives across all batches for the lifetime of the task slot.

### Component 2: VectorizedClassifier

Replaces ForStRsStateRequestClassifier. Zero per-request object allocation.

```java
class VectorizedClassifier implements AsyncRequestContainer<StateRequest<?,?,?,?>> {
    // Off-heap key/value buffers
    ColumnarBatchBuffer keys;
    ColumnarBatchBuffer values;
    
    // Metadata arrays (primitive, pre-allocated)
    int[] opTypes;          // GET=0, PUT=1, DEL=2, ITER=3, TIMER_ADD=4, TIMER_POLL=5
    Object[] futures;       // InternalAsyncFuture references
    Object[] tables;        // ForStRsInnerTable for deserialization
    int count;
}
```

**offer(StateRequest):**
1. Determine operation type from request.getRequestType()
2. Call table.serializeKeyInto(request, keys) — serializes directly into off-heap buffer
3. For PUTs: call table.serializeValueInto(request, values)
4. Record opType, future, table in parallel arrays
5. Increment count

**Key change to ForStRsInnerTable interface:**
```java
void serializeKeyInto(StateRequest<K,N,?,?> request, ColumnarBatchBuffer dest);
void serializeValueInto(StateRequest<K,N,?,?> request, ColumnarBatchBuffer dest);
```

These methods serialize directly into the off-heap buffer without intermediate byte[] allocation.

### Component 3: VectorizedExecutor

Replaces ForStRsStateExecutor. Dispatches entire batches via single FFM calls.

```java
class VectorizedExecutor implements StateExecutor {
    Arena arena;              // long-lived, owns all buffers
    ColumnarBatchBuffer resultBuffer;  // reusable return buffer
    ForStRsLinker linker;
    FrsDb db;
    FrsCfHandle cf;
}
```

**executeBatchRequests(container):**
1. Partition by opType (GETs, PUTs, DELETEs, ITERs, TIMERs) — index arrays, no object creation
2. Execute GETs: pass keys buffer slice → Rust writes results into resultBuffer → complete futures by index
3. Execute PUTs: pass keys + values buffer slices → Rust batch_put → complete futures
4. Execute DELETEs: pass keys buffer slice → Rust batch_delete → complete futures
5. Execute ITERs: flush any pending state, then per-iter prefix scan (unavoidable sequential)
6. Execute TIMERs: batch add via batch_put, batch poll via prefix_scan

**Correctness**: Steps 2-6 operate on disjoint key sets (guaranteed by async framework). Order within each step doesn't matter. ITERs flush first (invariant 2).

### Component 4: Rust FFI — Vectorized Batch API

New FFI functions that accept Arrow-format buffers directly:

```rust
// Batch get: keys in Arrow BinaryArray, results written to output Arrow BinaryArray
fn frs_vectorized_batch_get(
    db: *const Db, cf: *const CfHandle,
    key_offsets: *const i32, key_data: *const u8, count: usize,
    out_offsets: *mut i32, out_data: *mut u8, out_validity: *mut u8,
    out_data_cap: usize
) -> i32;

// Batch put: keys + values in Arrow BinaryArray format
fn frs_vectorized_batch_put(
    db: *const Db, cf: *const CfHandle,
    key_offsets: *const i32, key_data: *const u8,
    val_offsets: *const i32, val_data: *const u8,
    count: usize
) -> i32;

// Batch delete: keys in Arrow BinaryArray format
fn frs_vectorized_batch_delete(
    db: *const Db, cf: *const CfHandle,
    key_offsets: *const i32, key_data: *const u8, count: usize
) -> i32;

// Batch prefix scan: multiple prefixes, returns all matching entries
fn frs_vectorized_prefix_scan(
    db: *const Db, cf: *const CfHandle,
    prefix_offsets: *const i32, prefix_data: *const u8, count: usize,
    out_key_offsets: *mut i32, out_key_data: *mut u8,
    out_val_offsets: *mut i32, out_val_data: *mut u8,
    out_counts: *mut i32,  // entries per prefix
    out_data_cap: usize
) -> i32;
```

**Key property:** All buffers are caller-owned. Rust reads from input buffers and writes to output buffers without allocation. The Java side owns all memory via Arena.

### Component 5: BatchTimerService

Timer operations flow through the same VectorizedClassifier as state operations.

**Timer ADD:** Serialized as a PUT with the composite timer key (queue_prefix + kg + timestamp + element) into the keys buffer, value = 1-byte marker.

**Timer POLL:** Serialized as a prefix scan request with the key-group prefix. The executor reads the first entry from the scan result, then issues a DELETE for that key.

**Timer REMOVE:** Serialized as a DELETE with the full composite timer key.

**Batch benefit:** Q5 generates ~460M timer operations. Instead of 460M individual FFM calls, they're grouped into ~250K batches of ~1800 operations each. Each batch is one FFM call.

### Component 6: SIMD Utilities (Java Vector API)

Used in the ColumnarBatchBuffer for:
1. **Vectorized offset computation**: Prefix sum of key lengths to build the offsets array
2. **Vectorized key hashing**: Batch hash computation for cache routing
3. **Vectorized validity check**: Process Arrow validity bitmaps 64 bits at a time

```java
// Example: vectorized prefix sum for offset computation
static void computeOffsets(int[] lengths, int[] offsets, int count) {
    var species = IntVector.SPECIES_PREFERRED;  // AVX2: 8 ints, AVX512: 16 ints
    // ... SIMD prefix sum implementation
}
```

### Component 7: Write-Through Cache (Optional, Q3 optimization)

Layered on top of the vectorized path for join-heavy workloads.

- Uses existing `FlatStateCache` (GC-free flat hash table, off-heap-style)
- On GET: check cache first (no FFM) → miss falls through to vectorized batch
- On PUT: write to cache + include in batch (write-through, not write-back)
- On ITER: cache is transparent (all data in engine, cache is read-only optimization)

**Write-through vs write-back**: Write-through is simpler and correct by construction — every PUT goes to the engine immediately. The cache only accelerates reads. No flush-on-checkpoint needed.

## Phased Implementation

### Phase 1: ColumnarBatchBuffer + VectorizedClassifier
- Implement off-heap buffer with Arrow layout
- Add `serializeKeyInto` / `serializeValueInto` to ForStRsInnerTable
- Implement in ForStRsMapStateV2, ForStRsValueStateV2
- Classifier accumulates into buffers instead of creating objects
- Executor still uses existing FFM calls (reads from buffer, copies to existing API)
- **Validation**: Q3 correctness + no regression

### Phase 2: VectorizedExecutor + Rust FFI
- Implement `frs_vectorized_batch_get/put/delete` in Rust
- Executor dispatches directly from buffer to Rust (zero-copy)
- Results read from output buffer (zero-copy return)
- **Validation**: Q3 performance improvement

### Phase 3: BatchTimerService
- Timer operations flow through VectorizedClassifier
- Batch dispatch for timer ADD/REMOVE/POLL
- **Validation**: Q5 completes, performance competitive with rocksdb

### Phase 4: SIMD + Cache
- Java Vector API for offset computation and hashing
- Write-through cache for join patterns
- **Validation**: Full Nexmark suite, 1.3× target on state-heavy queries

## Success Criteria

| Query | Current | Target | Metric |
|-------|---------|--------|--------|
| Q0 | 29s | 29s | No regression |
| Q3 | 30s | ≤28s | Parity or better |
| Q5 | 463s | ≤200s | 2.3× improvement (timer batching) |
| Q7 | N/A | Completes | Feature parity |
| Q8 | N/A | Completes | Feature parity |

## Risks

1. **Off-heap buffer overflow**: Batch size is bounded by async controller (~32-128 requests). With avg key size 50B, max buffer = 128×50 = 6.4KB per batch. Overflow is unlikely but must be handled (grow buffer).
2. **Serializer side effects**: Some TypeSerializers may have side effects or state. The `serializeKeyInto` path must be equivalent to the current `serializeKey` path.
3. **Timer correctness**: Batch timer poll must handle the case where multiple polls in the same batch target the same key group (each poll must see the NEXT entry, not the same one).
