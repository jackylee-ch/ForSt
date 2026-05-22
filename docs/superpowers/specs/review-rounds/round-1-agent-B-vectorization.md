# Round 1 — Agent B Review: End-to-End Vectorization & Batch Execution

**Reviewer angle:** criterion #2 — eliminate per-record sync work, per-row FFM crossings, per-event allocations, virtual-call hotpoints, and branch-heavy if-chains in the hot path. Drive backend → engine → S3 as columnar batches with SIMD-friendly hot loops and JDK 25 Vector API where applicable.

**Scope read (cold):**
- `VectorizedExecutor.java` (840 lines)
- `VectorizedClassifier.java` (555 lines)
- `ColumnarBatchBuffer.java`, `AppendMergeBatchBuffer.java`, `IterPrefixBatchBuffer.java`, `IterRangeBatchBuffer.java`
- `state/ForStRsAsyncListStateV2.java`, `state/ForStRsValueStateV2.java`, `state/ForStRsMapStateV2.java`, `state/ForStRsListStateV2.java`, `state/ForStRsAsyncReducingStateV2.java`
- `timer/ForStRsKeyGroupedInternalPriorityQueue.java` (1014 lines), `timer/ArrowTimerBuffer.java`
- `state/ArrowBinaryBuffer.java`, `state/ArrowBinaryBufferAutoTuner.java`
- `ffm/ForStRsLinker.java` (3366 lines, hotspot zones around 1294–1450, 1500–1900, 1962–2070, 2480–2575, 3253–3300)
- `crates/forst-rs-ffi/src/lib.rs` (7289 lines — `frs_vectorized_batch_get` ~2350, `frs_vec_merge_append_batch` ~3997, `frs_vec_iter_prefix_open` ~3554, `write_chunk_into_buf` ~3508)
- `crates/forst-rs-engine/src/db.rs` `batch_get` ~2928, `prefix_scan` ~2352

Reference: `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md` V-catalogue.

Findings labeled by severity (HIGH = on the per-event hot path; MED = warm path / outer loop; LOW = correctness-only or rare path).

---

## HIGH findings

### H1. `VectorizedExecutor.dispatchAppendMergePerRow` does per-row `Arena.ofConfined()` + per-operand native copy (V4 legacy fallback)

**Location:** `VectorizedExecutor.java:414-498`

```java
for (int row = 0; row < count; row++) {
    ...
    Arena scratch = Arena.ofConfined();   // per-row Arena allocation
    try {
        MemorySegment ptrs = scratch.allocate(ValueLayout.ADDRESS, vs.length);
        MemorySegment lens = scratch.allocate(ValueLayout.JAVA_INT, vs.length);
        for (int i = 0; i < vs.length; i++) {
            long vLen = vs[i].byteSize();
            MemorySegment nativeV = scratch.allocate(vLen);
            MemorySegment.copy(vs[i], 0L, nativeV, 0L, vLen);  // heap→native per operand
            ptrs.setAtIndex(ValueLayout.ADDRESS, i, nativeV);
            ...
        }
        int rc = linker.frsVecMergeAppend(db.handle(), cf.handle(), keyPtr, keyLen, ptrs, lens, vs.length);
        ...
    } finally { scratch.close(); }
}
```

This is the multi-operand fallback path (V4 batched form only used when `allSingleOperand`). For multi-element `asyncAddAll(List<V>)` calls on a registered ListState, this is invoked once per row, paying: (1) per-row FFM crossing, (2) per-row `Arena.ofConfined()` open/close (50–200 ns), (3) per-operand heap→native memcpy. Three V-violations from the audit catalogue stacked.

**Fix:** the batched FFI already exists (`frsVecMergeAppendBatch`, audit V4 shipped). Reuse it for multi-operand rows by encoding each row's operand list as a single concatenated `[count][elem*][count][elem*]…` operand pre-pass in Java, then routing through `dispatchAppendMergeBatch` unconditionally. Removes the entire per-row Arena/FFM cost.

### H2. `frs_vectorized_batch_get` is NOT a true engine multi-get — per-key `db.get(cf, k)` loop in Rust

**Location:** `crates/forst-rs-ffi/src/lib.rs:2415-2437` (V10 in audit; still HIGH)

```rust
for i in 0..count {
    ...
    let k = &key_buf[ks..ke];
    match db.get(cf, k) { ... }     // per-key engine call
    ...
}
```

Java-side `VectorizedExecutor.executeGets` does one FFM crossing for `n` keys, but the Rust implementation discards that win and does `n` independent `db.get()` calls. Each `db.get()` performs a fresh `lookup_cf_by_id` + memtable lookup + (potentially) `get_internal` SST path. The engine **does** expose `db.batch_get` (`crates/forst-rs-engine/src/db.rs:2928`) which prefetches SST files for S3 vector I/O and short-circuits in the active memtable. `frs_vectorized_batch_get` should route through `db.batch_get` (and write its results into the caller's Arrow BinaryArray out-buffer columnar layout) — that's exactly the V10 fix from the audit, still open.

Concrete S3 implication: prefetch_sst_files_for_batch never runs on the V2 vectorized GET path, so an S3-backed deployment pays 1 round-trip per missing key in the worst case, not 1 round-trip per file.

### H3. `dispatchIterPrefix` / `dispatchIterRange` open ONE FFM call per row, plus `Arena.ofShared()` per iterator

**Location:** `VectorizedExecutor.java:637-709` (prefix) and `:724-800` (range)

```java
for (int row = 0; row < buffer.count(); row++) {
    ...
    Arena perIterArena = Arena.ofShared();   // per-iterator Arena
    MemorySegment outHandle = perIterArena.allocate(ValueLayout.JAVA_LONG);
    MemorySegment outRowCount = perIterArena.allocate(ValueLayout.JAVA_INT);
    MemorySegment outBytesUsed = perIterArena.allocate(ValueLayout.JAVA_INT);

    int rc = linker.frsVecIterPrefixOpen(db.handle(), cf.handle(),
            prefix, (int) prefix.byteSize(), chunkBuf, (int) chunkBuf.byteSize(),
            outHandle, outRowCount, outBytesUsed);
    ...
}
```

This is *not* a vectorized dispatch — it's a Java loop calling FFI per row. For Nexmark Q11/Q12/Q19 where windowing ops fan out hundreds of prefix scans per batch, this is N × (FFM crossing + Arena alloc + native iter registration). Engine already has `batch_prefix_scan` (`db.rs:2388`) — wire a true `frs_vec_batch_iter_prefix_open` that takes Arrow BinaryArrays of prefixes and writes N first-chunk buffers in one crossing, or at minimum reuse a single `Arena.ofShared()` per batch and amortize the out-param segments.

### H4. `frs_vec_iter_prefix_open` materializes the FULL prefix result set into a `Vec<(Vec<u8>, Vec<u8>)>` at open time

**Location:** `crates/forst-rs-ffi/src/lib.rs:3588-3593`

```rust
let rows = match db_ref.prefix_scan(cf_ref_, prefix) {  // ← full materialization
    Ok(r) => r, Err(_) => return ...,
};
let inner: Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + Send> = Box::new(rows.into_iter());
let mut native_iter = NativeIter::new(inner);
let chunk = native_iter.next_chunk(chunk_buf_cap as usize);
```

And `db.prefix_scan` itself (`db.rs:2352-2384`) loops over keys, copying each key + value into freshly-allocated `Vec<u8>`. The "chunked iterator" abstraction promised on the Java side is undone here: the entire scan result lives in two heap Vecs before the first chunk is written. Two consequences:

1. Per-row `Vec<u8>` allocs (× 2: key + value) destroy cache locality.
2. Memory peak is proportional to the full prefix size, not the chunk size — defeating the whole purpose of streaming chunks.

**Fix:** the inner iterator should be a true `rocksdb::DBIterator`-style lazy cursor that copies into the caller's `chunk_buf` directly (one bulk memcpy per chunk, zero `Vec` allocs).

### H5. `ColumnarBatchBuffer.append(byte[])` copies heap→off-heap; serializers still produce heap `byte[]`

**Location:** `ColumnarBatchBuffer.java:80-95`, used by every `serializeKeyInto` / `serializeValueInto` in V2 state classes via `keyOut.getSharedBuffer()` (so far so good)…

… BUT `recordAppendMerge` in `VectorizedClassifier.java:455-470` STILL allocates `byte[]` for both key and value before wrapping in a heap-MemorySegment:

```java
byte[] keyBytes = table.serializeKey(request);       // alloc
byte[] valBytes = table.serializeValue(request.getPayload());  // alloc
...
MemorySegment keySlice = MemorySegment.ofArray(keyBytes);  // heap segment
MemorySegment valSlice = MemorySegment.ofArray(valBytes);
```

And `AppendMergeBatchBuffer.append` then copies the heap segment into the columnar key buffer (`AppendMergeBatchBuffer.java:62-67`), THEN `dispatchAppendMergeBatch` copies the operand from the heap segment into `opsDataSeg` (yet another copy at line 562). So a single LIST_ADD pays: serialize→byte[]→heap-MemorySegment→ColumnarBatchBuffer→scratch Arena. Four buffers for one element.

**Fix:** add `recordAppendMergeInto(table, request, AppendMergeBatchBuffer)` that uses `serializeKeyInto` (already exists, writes to ColumnarBatchBuffer's data segment) AND a new `serializeValueIntoOperand(buffer)` that writes the `[count=1][elem_bytes]` operand directly into a second per-op ColumnarBatchBuffer owned by the AppendMergeBuffer. Then `dispatchAppendMergeBatch` can skip the per-row `MemorySegment.copy` at line 562 entirely.

### H6. `VectorizedExecutor.executeGets` allocates a fresh `new byte[len]` per GET result before deserialize (V5 in audit)

**Location:** `VectorizedExecutor.java:345-358`

```java
for (int i = 0; i < n; i++) {
    byte vld = outValidity.get(ValueLayout.JAVA_BYTE, i);
    byte[] raw = null;
    if (vld != 0) {
        int start = outOffsets.get(ValueLayout.JAVA_INT, (long) i * Integer.BYTES);
        int end = outOffsets.get(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES);
        int len = end - start;
        if (len > 0) {
            raw = new byte[len];                                    // alloc per row
            MemorySegment.copy(outData, ValueLayout.JAVA_BYTE, start, raw, 0, len);
        }
    }
    completeGet(reqs[i], tables[i], raw);
}
```

For an N-key batch, this allocates N `byte[]`s plus N bulk copies — purely for the `ForStRsInnerTable.deserializeValue(byte[])` API contract. Q16/Q19/Q11/Q12 take huge volumes through here.

**Fix paths (in order of effort):**
1. Add `ForStRsInnerTable.deserializeValueFrom(MemorySegment, long off, int len)` and route every V2 state to use it. The existing V2 deserializers wrap `DataInputDeserializer.setBuffer(byte[])` — replace with a `MemorySegment`-backed `DataInputView`. The class `v1sync/MemorySegmentDataInputView.java` already exists; promote it out of `v1sync` and use it on V2 GET decode.
2. JDK 25 Vector API: `ByteVector.fromMemorySegment(...)` lets JIT auto-SIMD the copy. But removing the copy entirely is the win, not vectorizing it.

### H7. `VectorizedClassifier.offer` switch on `StateRequestType` is a 27-case dispatch on the per-record hot path

**Location:** `VectorizedClassifier.java:304-358`

Every async-V2 state request goes through this `switch`. With 27 cases including 8 fall-through groups, the resulting `tableswitch` bytecode is large enough that the JIT's branch predictor table has to track all of them. For Q11/Q12 (massive `setCurrentKey` rates per record context), this `offer()` is on the inner hot loop.

Worse: inside `case LIST_ADD / LIST_ADD_ALL`, there are 3 nested branches (payload null → recordDelete, name+registry-containsCheck → recordAppendMerge vs recordPut). `listStateNames.contains(name)` is a `ConcurrentHashMap.containsKey` per LIST_ADD on the hot path — a String hash + bucket walk.

**Fix:** precompute a per-state-name routing token at registration time so `offer()` does a single `int routingId = table.getRoutingId()` lookup and dispatches with a primitive switch. Cache the boolean "is registered as ListState" on the table instance itself (`table.isListMergeable()`) so the per-event `Set.contains(stateName)` lookup is gone.

### H8. `MapStateCache.lookup/put/remove` allocates a `new BytesKey(key)` per call (V1 in audit)

**Location:** `cache/MapStateCache.java:90, 107, 113`

```java
public Lookup<V> lookup(byte[] key) {
    Object v = entries.get(new BytesKey(key));  // alloc per lookup
    ...
}
```

For Q11/Q12, MapStateV2 `asyncGet/Put/Contains/Remove` are the per-record entry points. The cache is consulted on EVERY call. Each call allocates `BytesKey` + computes `Arrays.hashCode(bytes)` (which itself is a per-byte loop, no SIMD).

**Fix paths:**
1. Replace `LinkedHashMap<BytesKey, Object>` with a primitive open-addressed `byte[]→Object` table modeled on `ArrowBinaryBuffer` (off-heap key bytes, open-addressed hash index over them). No per-lookup wrapper alloc. The `ArrowBinaryBuffer` already does this — extract a generic `OffHeapBytesKeyMap` and reuse.
2. Even before that, cache the BytesKey instance on `RecordContext.extra` (which `ForStRsValueStateV2` already does for the composite key, line 75–100) — one BytesKey per record context, not per state op.

### H9. `ArrowBinaryBuffer.hash` and `keysEqual` are scalar byte-by-byte loops (V8-adjacent)

**Location:** `state/ArrowBinaryBuffer.java:468-490`

```java
private int hash(MemorySegment seg, long offset, int len) {
    int h = 1;
    for (int i = 0; i < len; i++) {
        h = 31 * h + seg.get(ValueLayout.JAVA_BYTE, offset + i);  // 1 byte / iter
    }
    return h;
}

private boolean keysEqual(int row, MemorySegment seg, long offset, int len) {
    ...
    for (int i = 0; i < len; i++) {
        if (keyData.get(...) != seg.get(...)) return false;       // 1 byte / iter
    }
    return true;
}
```

Same scalar pattern in `timer/ArrowTimerBuffer.java:388-394` (`hashOf`) and `:396-411` (`rowKeyEquals`), and `ForStRsKeyGroupedInternalPriorityQueue.keyPrefixMatches` (line 511-522). These are on the V1-sync MapState write-buffer hot path AND the timer hot path. With composite keys typically 30–80 bytes, that's 30–80 individual `MemorySegment.get` per insert.

**Fix:** JDK 25 incubator `jdk.incubator.vector.ByteVector` makes this exactly the case where auto-SIMD shines (`ByteVector.fromMemorySegment` + lane-wise XOR + horizontal reduce for hash; `ByteVector.eq + allTrue` for equality). Even without the incubator API, `MemorySegment.mismatch(...)` is a JIT-intrinsified bulk operation (uses AVX2 on x86, NEON on aarch64) — `keysEqual` can become a one-liner `seg.asSlice(o1,l1).mismatch(other.asSlice(o2,l2)) < 0`.

### H10. `ForStRsLinker.{getPinnedSegment, putSegment, deleteSegment}` allocate a fresh `byte[]` then call the byte[] FFI — segment API is a lie

**Location:** `ForStRsLinker.java:1381-1440`

```java
public int getPinnedSegment(... MemorySegment keySegment, long keyOffset, int keyLen, ...) {
    byte[] keyBytes = new byte[keyLen];
    MemorySegment.copy(keySegment, ValueLayout.JAVA_BYTE, keyOffset, keyBytes, 0, keyLen);
    byte[] raw = getPinned(db, cf, keyBytes);                       // byte[]-FFI
    if (raw == null) { raw = getFast(db, cf, keyBytes); ... }
    ...
    MemorySegment.copy(raw, 0, outSegment, ValueLayout.JAVA_BYTE, outOffset, raw.length);
}
```

Three V1-sync state hotpath callers (`ForStRsValueState`, `ForStRsMapState`) invoke these "Segment" overloads expecting zero-copy. In reality every call:
1. Allocates `new byte[keyLen]` and copies the key off the off-heap buffer to heap,
2. Calls the byte[] FFI which internally builds another `MemorySegment.ofArray(key)`,
3. Returns a byte[], then copies it INTO the caller's outSegment.

That's 3 allocations + 2 bulk copies for one "zero-copy" lookup. `putSegment` is identical (alloc + copy for both key AND value).

**Fix:** wire `frs_get_pinned` / `frs_put` / `frs_delete` to take `(MemorySegment, long offset, long len)` triples directly. These are tiny FFI signatures and the engine side already supports raw pointers.

---

## MED findings

### M1. `ColumnarBatchBuffer.ensureCapacity` grows via fresh `arena.allocate` — no segment reclamation

**Location:** `ColumnarBatchBuffer.java:159-189`

Grow-by-doubling allocates a brand-new MemorySegment each time without freeing the old one (the `arena` lives for the executor lifetime). On a long-lived backend the buffer can grow to its high-water mark and stay there forever. Memory leak shape, not a perf hot-path issue.

**Fix:** track a generation counter; periodic `arena.reset()`-equivalent or use a per-batch `Arena.ofConfined` for the data segment with the offsets segment carried in the long-lived arena.

### M2. `dispatchAppendMergeBatch` allocates a per-batch `Arena.ofConfined()` for ops staging

**Location:** `VectorizedExecutor.java:534-621`

Even the "batched" path opens a fresh `Arena.ofConfined()` per batch (line 534) and allocates two segments (`opsOffSeg`, `opsDataSeg`) into it. For high batch rates (Nexmark Q12 timer-driven), this is per-batch Arena open+close cost. Compare with the GET/PUT/DELETE paths where `outOffsets/outData/outValidity` live in the executor's long-lived Arena.

**Fix:** reuse executor-level long-lived `opsOffSeg` / `opsDataSeg` with grow-on-demand, parallel to `outOffsets/outData`.

### M3. `AppendMergeBatchBuffer` uses `ArrayList<MemorySegment[]>` and `ArrayList<CompletableFuture>` per batch

**Location:** `AppendMergeBatchBuffer.java:47-48, 69-70`

Each append boxes references into `ArrayList`. For a batch of 1024 LIST_ADDs that's 1024 `MemorySegment[]` arrays (each containing 1 element) + 1024 `CompletableFuture`s in two ArrayLists. Better: keep parallel primitive arrays (matching the GET/PUT path's `StateRequest[] putRequests` pattern) — same as the audit's H1 critique on `dispatchAppendMergePerRow`.

### M4. `AppendMergeBatchBuffer.append` always re-copies the key into a `new byte[]`

**Location:** `AppendMergeBatchBuffer.java:55-71`

```java
byte[] keyBytes = new byte[(int) byteSize];
MemorySegment.copy(keySlice, 0L, MemorySegment.ofArray(keyBytes), 0L, byteSize);
keyBuffer.append(keyBytes);
```

The keySlice is already a (possibly heap) MemorySegment. The `keyBuffer.append(byte[])` path then does another copy into the off-heap data segment. Could add `keyBuffer.appendFromSegment(MemorySegment src, long off, int len)` to skip the intermediate byte[].

### M5. `IterPrefixBatchBuffer` / `IterRangeBatchBuffer` use 3-4 separate `ArrayList`s per batch

**Location:** `IterPrefixBatchBuffer.java:44-47`, `IterRangeBatchBuffer.java:44-48`

Same as M3 — multiple `ArrayList<MemorySegment>` with weak cache locality. Combined with H3 (per-row FFM in dispatch), the iterator path is the least vectorized part of the executor.

### M6. `ForStRsKeyGroupedInternalPriorityQueue.advance` does per-iter-entry `new byte[]` copy

**Location:** `timer/ForStRsKeyGroupedInternalPriorityQueue.java:700-715`

```java
while (true) {
    ForStRsLinker.IteratorEntry e = linker.iteratorNext(iter);
    if (e == null) break;
    long ts = decodeTimestamp(e.key());
    ...
    T element = decodeElement(e.key());
    visitor.accept(element);
    dueKeyList.add(e.key());                  // byte[] held in ArrayList
}
```

The `IteratorEntry.key()` is already a `byte[]` allocated inside the linker (which copies from a native pointer per entry). For high timer firing rates (Q12) the loop allocates one byte[] per due timer + appends to an ArrayList. Pair this with M1/M3-style critique: an `IterEntry` columnar buffer would batch these into one off-heap segment.

### M7. `ArrowBinaryBuffer.flushTo` walks the hash-index in slot order, scattered keys/values

**Location:** `state/ArrowBinaryBuffer.java:295-351`

```java
int outIdx = 0;
int slots = capacity * 2;
for (int i = 0; i < slots; i++) {
    int row = hashIndex.get(...);              // scan hash slots
    if (row == EMPTY_SLOT || row == TOMBSTONE) continue;
    int kStart = keyOffsets.get(...);
    int kEnd = keyOffsets.get(...);
    int vStart = valueOffsets.get(...);
    ...
    flushKeyPtrs.set(ValueLayout.ADDRESS, ..., MemorySegment.ofAddress(keyDataAddr + kStart));
    ...
}
```

Walking by hash-index slot order means key/value pointers are appended in scrambled (not insertion) order — bad for the engine's WriteBatch internal SST L0 lookup. Better: walk rows by row id (insertion order) and reference the hash index only to skip tombstones (e.g. precompute a bitmap of live rows during inserts/removes).

### M8. `ForStRsKeyGroupedInternalPriorityQueue.peek` does O(n) full scan when root entry doesn't match (Q12 hot path)

**Location:** `timer/ForStRsKeyGroupedInternalPriorityQueue.java:441-509`

The fallback branch (root not for current keygroup or root is REMOVE) loops the whole pending buffer — for buffers near FLUSH_THRESHOLD=1024 that's 1024 random `MemorySegment.get` reads per `peek()`. Since `peek()` is called on every `registerProcessingTimeTimer`, this can be on the inner hot loop in timer-heavy queries.

**Fix:** maintain a min-ts-per-keygroup index, or keep a dedicated per-kg secondary heap so the fallback is O(log n).

### M9. `ArrowTimerBuffer.swapHeap` uses a 24-byte heap `byte[]` scratch for every swap

**Location:** `timer/ArrowTimerBuffer.java:312-330`

```java
byte[] tmp = new byte[HEAP_ROW_BYTES];     // allocated per swap
MemorySegment.copy(heapArray, ..., tmp, 0, HEAP_ROW_BYTES);
...
MemorySegment.copy(tmp, 0, heapArray, ..., HEAP_ROW_BYTES);
```

Sift-up/down do many swaps per insert. The 24-byte allocation churn is small but constant: insert at 1024 capacity ≈ 10 swaps × 24 bytes = 240 bytes/insert. Use an off-heap thread-local scratch segment, or do the swap with three small native copies.

### M10. `ForStRsAsyncListStateV2.deserializeValue` walks the merged payload in scalar Java

**Location:** `state/ForStRsAsyncListStateV2.java:217-228`

```java
while (valueIn.available() > 0) {
    int count = valueIn.readInt();
    ...
    for (int i = 0; i < count; i++) {
        list.add(elementSerializer.deserialize(valueIn));
    }
}
```

For large merged lists (many APPEND_MERGE operands concatenated), the outer loop reads many `[count][elems*]` chunks. This is fine for correctness; for perf it's worth noting the `ArrayList<V>` grows-by-amortized-doubling under the hood. Sizing hint: walk once to sum total elements, then `new ArrayList<>(totalElems)`. Or move to off-heap `Vec<V>`-style storage.

### M11. `ForStRsValueStateV2.serializeKey` builds composite via 5 `System.arraycopy` calls

**Location:** `state/ForStRsValueStateV2.java:81-101`

Five separate `arraycopy` calls building the composite into a fresh `byte[]` per call. The fast path (`ctx.getExtra() != null`) caches it, but the cache miss path allocates AND copies five times. Compare with the `serializeKeyInto(... ColumnarBatchBuffer)` path (line 154–167) which writes through `keyOut.write(...)` to a reusable `DataOutputSerializer` — that's already the cleaner shape and should be the only path; the byte[]-returning variant should compose by sharing the same buffer.

### M12. `ColumnarBatchBuffer.ensureCapacity/ensureData` grows by `<<=1` doubling forever

**Location:** `ColumnarBatchBuffer.java:165, 180`

There's no upper bound on capacity growth and no shrink hook. A single pathological batch with one giant value would permanently inflate the executor's data segment. Couple with `ArrowBinaryBufferAutoTuner`'s dual-gate pattern (size + hit-rate) to get a balanced grow/shrink policy here too.

---

## LOW findings

### L1. `ForStRsKeyGroupedInternalPriorityQueue.encode` uses a fresh `DataOutputSerializer(64)` per timer add

**Location:** `timer/ForStRsKeyGroupedInternalPriorityQueue.java:268-278`

`encode(int keyGroup, T element)` is called from `encodeIntoScratch` (`add()`/`remove()` hot path). It instantiates a new `DataOutputSerializer(64)` per call — a JVM allocation per buffered timer add. The class has a long-lived `scratchSeg` for the OUTPUT but the intermediate `DataOutputSerializer` is reborn each call.

### L2. `MapStateCache.BytesKey.hashCode` is `Arrays.hashCode(bytes)` (no caching of repeated calls)

`Arrays.hashCode` is implemented as a scalar loop. Per H8 fix, replace with `MemorySegment.mismatch`-based open-addressed hash table.

### L3. `VectorizedExecutor.completePut` future completion loops are not pipelined

**Location:** `VectorizedExecutor.java:269-272, 282-284, 194-196`

After `executePuts` / `executeDeletes` / dispatch the batch result, the executor loops `for (int i = 0; i < n; i++) completePut(reqs[i])`. Each `completePut` calls `InternalAsyncFuture.complete(null)` which triggers continuations synchronously. If continuations are heavy, this serializes them. Not a correctness issue, just a place where future-completion-fan-out is sequential. Probably fine because Flink's async-state framework expects single-threaded per-slot.

### L4. `frs_vec_merge_append_batch` Rust builds a `HashMap<&[u8], Vec<&[u8]>>` for grouping

**Location:** `crates/forst-rs-ffi/src/lib.rs:4022-4035`

Standard `std::collections::HashMap` per-call build + per-row insert. For homogeneous batches where all rows have distinct keys (the common case after upstream key shuffling), the hash table is N entries with 1 operand each — wasted work compared to a sort-then-group on `keys_off`/`keys_data` directly (which is also more cache-friendly).

### L5. `ForStRsLinker.batchPut(byte[][], byte[][])` convenience overload allocates `count×4` segments per call

**Location:** `ForStRsLinker.java:1535-1575`

The "convenience" overload (per-row `local.allocate(k.length)` + `MemorySegment.copy`) is documented as "UNSUITABLE for benchmarking". Confirm no V2-async hotpath caller uses it; if any do, route them through the staged `MemorySegment`-direct overload.

### L6. `ArrowBinaryBuffer.isRowLive` is `O(capacity)` (called per iter-merge row)

**Location:** `state/ArrowBinaryBuffer.java:406-418`

Already self-documented as O(capacity). With capacity up to 1M slots this is quadratic if called inside an iteration walk. Hot path callers should use `liveRows()` (line 425) instead, which builds an int[] once per pass.

### L7. `ForStRsKeyGroupedInternalPriorityQueue.flushPendingToEngine` allocates `keys[][]` + `vals[][]` of byte[]s for the ADD path

**Location:** `timer/ForStRsKeyGroupedInternalPriorityQueue.java:758-775`

```java
byte[][] keys = new byte[addCount][];
byte[][] vals = new byte[addCount][];
...
byte[] k = new byte[kLen];
MemorySegment.copy(keyDataSeg, ..., k, 0, kLen);
keys[outIdx] = k;
vals[outIdx] = EMPTY_VAL_BYTES;
```

The REMOVE path uses the pre-staged `flushDelOffsets/flushDelData` Arrow layout (no per-row byte[]). The ADD path should mirror this — use `flushAddKeyPtrs/flushAddKeyLens` already allocated in `ensureFlushPairCapacity`, set them directly to `keyData.address() + kOff`, and call `linker.batchPut(... segment-overload)` not the byte[][]-overload.

### L8. `VectorizedExecutor.MIXED_STATE` string constant defeats per-state attribution metrics

**Location:** `VectorizedExecutor.java:248`

```java
private static final String MIXED_STATE = "_mixed";
```

All metrics use this single string. Means the `DispatchMetrics` histogram can't be sliced per state name, so it's impossible to tell from the metrics which state is the bottleneck in a multi-state operator. Tracking concern, not perf. Per-state attribution is on the P5 roadmap per the executor comment.

### L9. `ArrowBinaryBufferAutoTuner` resize hysteresis is per-buffer, not per-state-fleet

**Location:** `state/ArrowBinaryBufferAutoTuner.java`

Each state instance has its own tuner. In a job with 100 keyed states, each tuner samples independently. If they all grow at once at start-of-job, the cumulative resize is bursty. Centralized grow-quota coordination would smooth the allocation profile.

---

## Cross-cutting observations

1. **Two parallel zero-copy stacks coexist.** The V1-sync path (`ArrowBinaryBuffer` + segment-FFI primitives) and the V2-async path (`ColumnarBatchBuffer` + `vectorizedBatchGet/Put/Delete`) overlap in shape but don't share code. Extracting a common `OffHeapBinaryBuffer` abstraction would (a) remove duplication, (b) let the V2 path benefit from `ArrowBinaryBufferAutoTuner`, (c) let V1-sync MapState benefit from the dual-gate write-buffer pattern.

2. **No SIMD anywhere yet.** Every hot loop in `MemorySegment` land is scalar `byte/int/long`-at-a-time. JDK 25's `MemorySegment.mismatch(...)` is intrinsified and would fix H9 in one line per call site. The `jdk.incubator.vector` module is not imported anywhere in the backend.

3. **The "Arrow" naming is largely aspirational.** Buffers use Arrow's `offsets + data` BinaryArray *layout* but do not interoperate with Arrow-Java or Arrow-Rust. The two `frs_batch_*_arrow` FFI paths (`batchPutArrow`, `batchGetArrow`) do speak Arrow C Data Interface, but they're not on the V2 vectorized executor's hot path — only the bench compares them. If end-to-end Arrow is a goal, the V2 executor should route through `frs_batch_put_arrow` (which the bench notes is 1.8× faster than the WriteBatch path).

4. **S3 implications of H2/H4.** Both findings degrade S3 deployments specifically: H2 disables `prefetch_sst_files_for_batch` on the V2 GET path; H4 materializes the whole prefix into RAM before streaming the first chunk, which interacts badly with S3's GET-by-byte-range capability (cannot stream Arrow IPC frames directly from object storage).

---

## Summary

- **HIGH:** 10 (H1–H10) — every one is on the V2 async per-event hot path or per-iterator path
- **MED:** 12 (M1–M12)
- **LOW:** 9 (L1–L9)

The audit catalogue's V-classification (V1, V2, V5, V10, V11) accurately predicts about half of the HIGH list (H2 ≡ V10, H5 ≡ V11+V2 partial, H6 ≡ V5, H8 ≡ V1). The new HIGH findings beyond the audit are H1 (multi-operand fallback still uses per-row Arena), H3 (per-row iterator dispatch), H4 (full materialization in `frs_vec_iter_prefix_open` — a Rust-side bug, not in the Java catalogue), H7 (27-case classifier switch + Set.contains in hot path), H9 (no SIMD anywhere), H10 (segment-FFI overloads are wrappers around byte[]-FFI). H4 is the most surprising — the chunked iterator abstraction is structurally undone in Rust.

Top-3 leverage estimates (informal, based on touched call site frequency on Nexmark Q11/Q12/Q19/Q16):
- **H4** (Rust prefix scan materialization) — affects every MAP_ITER request; structural memory peak issue too
- **H7** (classifier dispatch) — every async-V2 record (~100M ops/job on Nexmark)
- **H6** (per-GET byte[] alloc) — every async-V2 GET (Q16/Q19 GET-heavy)

Tie-breakers H1, H2, H8 close behind for batch and S3 deployments.
