# Round 1 — Agent C — End-to-End Zero-Copy Audit

**Reviewer:** Agent C
**Angle:** End-to-end zero-copy (criterion #3) — full Arrow reuse + MemorySegment slice chains
**Scope:** Java buffer/state/timer/linker + Rust FFI + Rust storage S3 path
**Date:** 2026-05-22
**Methodology:** Cold read of the listed files only. Pattern-grep for `new byte[]`, `getCopyOfBuffer()`, `to_vec()`, `read_to_end`, `Vec::with_capacity` on hot paths; per-call vs per-batch attribution.

---

## Summary table

| Sev | # | Location | One-line |
|-----|---|----------|----------|
| H | C1 | `VectorizedExecutor.java:353` | `byte[] raw = new byte[len]` per GET result — kills the vectorized GET zero-copy win |
| H | C2 | `OpendalRandomAccessFile.read_at` (opendal_backend.rs:328-336) + `OpendalSequentialFile` (line 285) | S3 read path always does `buffer.to_vec()` then `copy_from_slice` — eager full-object download + extra heap copy per read |
| H | C3 | `OpendalWritableFile::persist` (opendal_backend.rs:393-395) | `self.buffer.clone()` per `sync()` — clones the **entire SST/snapshot** before sending to S3 |
| H | C4 | `frs_vec_merge_append_batch` (lib.rs:4040) and `frs_vec_merge_append` (lib.rs:3934, 3944) | Rust copies every operand into `Vec<u8>` via `slice.to_vec()` then `ListMergeCombiner::combine_with_base` — defeats the V4 batched-FFI zero-copy story on the Rust side |
| H | C5 | `ForStRsMapStateV2.deserializeUser{Key,Value}(IteratorEntryView)` (lines 349, 372) | `new byte[rangeLen]` per iter entry + `new DataInputDeserializer(buf,…)` — V5/V8 style hot-path heap alloc on every emitted row |
| H | C6 | `ForStRsLinker.getIntoBuf` (line 2353) + `getFast` (line 2396) | Threadlocal scratch already off-heap, but the function always allocates `new byte[valLen]` + `System.arraycopy` to return — every single V1-sync GET allocates a heap byte[] |
| H | C7 | `AppendMergeBatchBuffer.append` (line 64) | `byte[] keyBytes = new byte[byteSize]` per APPEND_MERGE request: MemorySegment → byte[] → off-heap data — two copies on the LIST_ADD hot path |
| M | C8 | `ForStRsValueStateV2.serializeKey` (lines 82-101) | Two heap allocs per cache-miss: `keyOut.getCopyOfBuffer()` + `new byte[len]` for composite — RecordContext-extra cache mitigates, but cold-path is allocated |
| M | C9 | `MapStateCache.lookup` (line 91) | `new BytesKey(key)` per lookup (V1 in audit) — plus `Arrays.hashCode(bytes)` walks the whole key array every constructor; cache is value-stored-by-reference (good) but key alloc is real |
| M | C10 | `ArrowTimerBuffer.swapHeap` (line 320) | `byte[] tmp = new byte[HEAP_ROW_BYTES]` per heap-swap in siftUp/siftDown — `O(log n)` heap allocs per timer add/remove |
| M | C11 | `SstWriterImpl.add` (writer.rs:175, 178, 188) | `key.to_vec()` 3× per entry for min/max/last_added_key tracking — per-record on the snapshot/compaction path |
| M | C12 | `SstWriterImpl.finish` (writer.rs:206 returns `(Vec<u8>, SstFileInfo)`) | Whole SST file is held in a single `Vec<u8>` buffer with no streaming writer API — compounds C3 |
| M | C13 | 30+ call sites of `DataOutputSerializer.getCopyOfBuffer()` across state classes | Every legacy/non-`serializeKeyInto` path does heap→heap memcpy; vectorized path already bypasses via `getSharedBuffer()` |
| M | C14 | `compat_jni.rs` exists at 316KB alongside `lib.rs` 274KB | JNI shim continues to allocate byte[] for jbyteArray returns (out of forst-rs scope but flagged as a co-tenant of the lib for tree-shaking) |
| L | C15 | `ArrowBinaryBuffer.copyValue` (line 354) + `liveRows()` (lines 426, 442-443) | Debug/test helpers + iter-merge walker allocs — non-hot |
| L | C16 | `FrsBytes::from_vec` (lib.rs:249) | `shrink_to_fit()` before `mem::forget`: copies into a new (smaller) Vec on every cross-boundary return when `cap > len * 0.9` — pathological for large slack |
| L | C17 | `CachedFileSystem::InMemorySequential/InMemoryRandom` (cached_fs.rs:303-358) | Mirrors C2 — full Vec<u8> hold; not on remote hot path because it's the local cache layer |

---

## H1 (C1) — Vectorized GET result decode allocates byte[] per row

**File:** `VectorizedExecutor.java:343-358`

```java
for (int i = 0; i < n; i++) {
    byte vld = outValidity.get(ValueLayout.JAVA_BYTE, i);
    byte[] raw = null;
    if (vld != 0) {
        int start = outOffsets.get(ValueLayout.JAVA_INT, (long) i * Integer.BYTES);
        int end = outOffsets.get(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES);
        int len = end - start;
        if (len > 0) {
            raw = new byte[len];                                              // ← H
            MemorySegment.copy(outData, ValueLayout.JAVA_BYTE, start, raw, 0, len);
        }
    }
    completeGet(reqs[i], tables[i], raw);
}
```

Why H: this is the precise hot path the vectorized executor exists to optimize. The engine has already written all values into one off-heap `outData` segment (a true Arrow BinaryArray). Then `completeGet` calls `table.deserializeValue(byte[])` which immediately rebuilds a `DataInputDeserializer` over the byte[]. That `byte[]` is throwaway. **Every** GET in every batch pays it (Q11 V1-sync confirmed 92M ops; Q12 V2 confirmed median batch 273 — at 4096-batch capacity this is millions of byte[] alloc/sec under Nexmark).

**Fix shape:** add a `deserializeValue(MemorySegment seg, int off, int len)` overload on `ForStRsInnerTable` and a thread-local `MemorySegmentDataInputView` (already exists for V1-sync — reuse it on V2 too). `completeGet` calls the segment-overload; only the legacy path keeps the byte[] form.

---

## H2 (C2) — S3 reads always `buffer.to_vec()` + extra `copy_from_slice`

**File:** `crates/forst-rs-io/src/opendal_backend.rs:283-343`

```rust
pub struct OpendalSequentialFile {
    bytes: Vec<u8>,            // ← entire object loaded into heap Vec
    pos: usize,
}
// ...
impl RandomAccessFile for OpendalRandomAccessFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        let buffer = self.op.read_with(&self.path).range(...).call()?;
        let bytes = buffer.to_vec();                          // ← copy 1
        let n = bytes.len().min(buf.len());
        buf[..n].copy_from_slice(&bytes[..n]);                // ← copy 2
        Ok(n)
    }
}
```

Why H: 
- `OpendalSequentialFile` requires the **entire S3 object** in a `Vec<u8>` at open time. Doc says "matches typical SST/WAL write pattern" but for **read** path during recovery and warm-fetch this means: full SST blob in heap, doubled.
- `OpendalRandomAccessFile.read_at`: opendal's returned `Buffer` is already a contiguous slice — `to_vec()` clones it, then `copy_from_slice` clones again into caller's `buf`. OpenDAL has [`Buffer::to_bytes()`](https://docs.rs/opendal) and direct write-to-slice idioms; we use neither.

The doc-comment on `cached_fs.rs:21` explicitly markets this as a S3 prefetch optimization — but the prefetch lands in a heap `Vec<u8>` so the local cache copy IS the second copy. The headline "zero-copy S3 reader" the docs imply is not what the code does.

---

## H3 (C3) — `OpendalWritableFile::persist` clones the entire write buffer per sync

**File:** `crates/forst-rs-io/src/opendal_backend.rs:389-398`

```rust
fn persist(&mut self) -> ForstResult<()> {
    self.op
        .write(&self.path, self.buffer.clone())   // ← clones full SST/WAL/manifest
        .map_err(...)?;
    self.flushed = true;
    Ok(())
}
```

Why H: an SST can be tens to hundreds of MB. `buffer.clone()` allocates a fresh `Vec<u8>` of equal size and memcpys every byte. The `&self.buffer` clone is unavoidable in OpenDAL's `write(&str, Vec<u8>)` shape only because we picked the simple API; OpenDAL has a `writer(...)` streaming form (`Writer::write(Bytes)`) and `into_bytes_sink()` that can take ownership. With ownership we drop the clone.

The compaction path lives upstream of this (compaction.rs:157 → `(bytes, info) = writer.finish()`), so even after fixing C12 (streaming writer) the C3 fix means switching `op.write()` to `op.writer()` and consuming `self.buffer` via `mem::take`.

---

## H4 (C4) — Batched APPEND_MERGE: `slice.to_vec()` per operand on Rust side

**File:** `crates/forst-rs-ffi/src/lib.rs:4037-4053`

```rust
for (key, ops) in grouped.iter() {
    let owned_ops: Vec<Vec<u8>> = ops.iter().map(|s| s.to_vec()).collect();   // ← H
    let existing: Vec<u8> = match db_ref.get(cf_ref, key) { ... };
    let merged = if existing.is_empty() {
        combiner.combine(&owned_ops)
    } else {
        combiner.combine_with_base(&existing, &owned_ops)
    };
    ...
}
```

Why H: Java side now passes a single batched FFM call (V4 win, committed). On the Rust side, every operand `&[u8]` referencing the caller's stable `ops_data` is **immediately cloned** into a `Vec<u8>` only because `ListMergeCombiner::combine_with_base` is typed `&[Vec<u8>]`. The combiner walks the operands once. There's no reason for ownership. Spec V4 explicitly cites this as the per-row Arena.ofConfined() / native copy bug fixed on the Java side; we did not fix the symmetric Rust side.

Also note: the per-call (non-batch) `frs_vec_merge_append` at lib.rs:3934-3946 does the same `slice.to_vec()` per operand. So the legacy per-row dispatch path C7 (Java) → C4 (Rust) **double-pays** on each FFI call.

**Fix shape:** change `ListMergeCombiner::combine_with_base` to take `&[&[u8]]` and pass borrowed slices directly. Cascade: `Vec<Vec<u8>>` → `Vec<&[u8]>` in callers, no allocation per operand.

---

## H5 (C5) — Iter-entry decode allocs byte[] per row (V5/V8 still present)

**File:** `ForStRsMapStateV2.java:345-380`

```java
@Override
public UK deserializeUserKey(IteratorEntryView view, int userKeyPrefixOffset) {
    int rangeLen = view.keyLength() - userKeyPrefixOffset;
    byte[] buf = new byte[rangeLen];                                  // ← H
    MemorySegment.copy(view.chunkBuf(), ..., view.keyOffset() + ..., buf, 0, rangeLen);
    DataInputDeserializer in = new DataInputDeserializer(buf, 0, rangeLen);
    return userKeySerializer.deserialize(in);
}

@Override
public UV deserializeUserValue(IteratorEntryView view) {
    int len = view.valueLength();
    byte[] buf = new byte[len];                                       // ← H
    MemorySegment.copy(view.chunkBuf(), ValueLayout.JAVA_BYTE, view.valueOffset(), buf, 0, len);
    DataInputDeserializer in = new DataInputDeserializer(buf, 0, len);
    return (UV) userValueSerializer.deserialize(in);
}
```

The comment at line 340-343 acknowledges: *"the inner DataInputDeserializer still consumes a byte[] (Flink's serializer contract); eliminating that final alloc requires a MemorySegment-backed DataInputView and is deferred to V1.2."*

Why H regardless of the comment: a `MemorySegmentDataInputView` **already exists** at `v1sync/MemorySegmentDataInputView.java`. It implements `DataInputView` and reads directly off the segment with no byte[]. The fix here is **not** "build a new abstraction" but "reuse the V1-sync view on V2 iter path." This is two-allocation-per-iter-row in Q3, Q5, Q19 and any FORWARD_PREFIX-heavy query.

---

## H6 (C6) — V1-sync `getIntoBuf` still allocates per GET

**File:** `ForStRsLinker.java:2317-2398`

```java
private static final ThreadLocal<byte[]> GET_INTO_BUF =
        ThreadLocal.withInitial(() -> new byte[GET_INTO_BUF_CAP]);     // ← reused, fine

public byte[] getIntoBuf(FrsDb db, FrsCfHandle cf, byte[] key) {
    // ... fills threadlocal scratch via FFI ...
    long valLen = ...;
    if (valLen == 0) return null;
    byte[] result = new byte[(int) valLen];                            // ← H every call
    System.arraycopy(outBuf, 0, result, 0, (int) valLen);              // ← second memcpy
    return result;
}
```

Same in `getFast` (line 2396).

Why H: this is the V1-sync hot path. Q11 audit confirms 92M `ForStRsValueState` ops → 92M of these. The threadlocal scratch is sized 4096; for typical Nexmark records (<256 B) the scratch already has the data — but the API contract returns a heap byte[]. The caller (ForStRsValueState.value()) immediately does `inputBuffer.setBuffer(oldRaw)` and calls the serializer, so the byte[] is throwaway again.

**Fix shape:** introduce `getInto(... MemorySegment outView)` that aims the existing `MemorySegmentDataInputView` at the scratch segment in place — caller deserializes directly. Same pattern as C1, but for V1.

---

## H7 (C7) — `AppendMergeBatchBuffer.append` does MemorySegment → heap byte[] → off-heap

**File:** `AppendMergeBatchBuffer.java:55-71`

```java
public void append(AppendMergeRequest req) {
    MemorySegment keySlice = req.keySlice();
    if (keySlice == null || keySlice == MemorySegment.NULL) {
        keyBuffer.appendEmpty();
    } else {
        long byteSize = keySlice.byteSize();
        if (byteSize == 0) {
            keyBuffer.appendEmpty();
        } else {
            byte[] keyBytes = new byte[(int) byteSize];                            // ← H
            MemorySegment.copy(keySlice, 0L, MemorySegment.ofArray(keyBytes), 0L, byteSize);
            keyBuffer.append(keyBytes);                                            // copies AGAIN into off-heap
        }
    }
    valueSliceLists.add(req.valueSlices());
    futures.add(req.future());
}
```

Why H: `keySlice` is already a `MemorySegment` — it could be appended into `keyBuffer` directly via a `MemorySegment.copy(src, srcOff, dst, dstOff, len)` overload (which `ColumnarBatchBuffer` doesn't expose but trivially could). Current code goes MemorySegment → heap byte[] (alloc + memcpy) → off-heap `data` (second memcpy). This is on the LIST_ADD path of every Q19/Q5/Q6 join — multi-Hz per slot.

**Fix shape:** add `ColumnarBatchBuffer.appendSegment(MemorySegment src, long srcOff, int len)` that grows data and does the segment-to-segment copy directly.

---

## M-level findings

### C8 (M) — `ForStRsValueStateV2.serializeKey` double-alloc on cache miss

ValueStateV2.java:80-101: `keyOut.getCopyOfBuffer()` then `new byte[len]` for the composite key with five System.arraycopy calls. The `ctx.setExtra(composite)` cache mitigates repeat lookups within a batch, but each new RecordContext eats two heap allocs. The vectorized `serializeKeyInto` path (line 154-167) shows the right pattern: serialize directly through `getSharedBuffer()` into a single off-heap append. The classifier already prefers `serializeKeyInto`; this byte[] path persists for the legacy `buildDBGetRequest` codepath, which is still hit when the classifier can't aggregate. Flag as M because it's the legacy fallback, not the primary hot path post-vectorization.

### C9 (M) — `MapStateCache.BytesKey` per lookup

cache/MapStateCache.java:91, 108, 113: each `lookup`/`put`/`remove` calls `new BytesKey(key)`. `BytesKey` constructor calls `Arrays.hashCode(bytes)` which walks the whole key. So per cache lookup: 1 BytesKey alloc + 1 full-key hash walk. The value side is fine (stores live `V` object by reference; only TOMBSTONE allocated once statically). Audit V1 — confirmed not closed.

### C10 (M) — `ArrowTimerBuffer.swapHeap` allocates per swap

ArrowTimerBuffer.java:320: `byte[] tmp = new byte[HEAP_ROW_BYTES]` (24 bytes) on every heap swap during siftUp/siftDown. The buffer reuses a `MemorySegment` arena for everything else; this single byte[] looks like an oversight. siftUp/siftDown are O(log n) per insert/removeAt → on a 65k-capacity heap each remove costs ~16 small heap allocs. Q12-style timer-heavy workloads will see this. Fix: use a fixed `MemorySegment` scratch field on the buffer instance.

### C11 (M) — `SstWriterImpl.add` clones key 3× per entry

sst/writer.rs:175, 178, 188: `key.to_vec()` for min/max/last_added tracking on **every** add. The min/max could be deferred to `finish()` by tracking the array indices (since Arrow builders retain the buffer). `last_added_key` could be reborrowed from `key_builder.values_slice()` after the append. Per-record on the snapshot path → millions of allocs per checkpoint at Nexmark rates.

### C12 (M) — `SstWriterImpl.finish` returns `(Vec<u8>, …)`

sst/writer.rs:206: monolithic in-memory SST construction. With C3 (streaming writer to S3) you'd want `finish_streaming(&mut dyn Write)` instead. Coupled to OpenDAL's `Writer` API choice in C3.

### C13 (M) — 30+ `DataOutputSerializer.getCopyOfBuffer()` call sites

(Inventory above in the grep output.) Each is a heap→heap memcpy that allocates a fresh byte[] sized exactly to the serializer's used length. The vectorized path (`serializeKeyInto` / `serializeValueInto`) uses `getSharedBuffer()` + `length()` for zero-copy. Recommend a sweep: every site that immediately hands the byte[] to a Linker call should switch to the segment-append idiom. Estimate: in legacy V1-sync path 100% of every put/get key serialization, and in the V2 `buildDBGetRequest` / `buildDBPutRequest` fallback path for ValueStateV2/MapStateV2/ListStateV2/AggregatingStateV2/ReducingStateV2.

### C14 (M) — `compat_jni.rs` co-tenant (316KB)

Living alongside `lib.rs` in the same crate. The JNI compat shim allocates byte[] via JNIEnv every call. Out of forst-rs perf-path scope but flagged because the build artifact ships both. Suggestion: gate behind a Cargo feature `compat-jni` so non-FFM builds don't link the JNI shim.

### Low-severity (C15-C17)

- C15: `ArrowBinaryBuffer.copyValue` (line 354) is `// For tests / debugging`; `liveRows()` allocates an `int[]` (line 426) — used by iter-merge once per iter open, low frequency.
- C16: `FrsBytes::from_vec` calls `shrink_to_fit()` before `mem::forget(v)` — if the Vec has slack >10% this allocates a fresh-sized Vec just to hand off. Worth dropping `shrink_to_fit` if caller doesn't expect to free the exact-size buffer (the consumer copies anyway in most paths).
- C17: `CachedFileSystem` in-memory file types mirror C2's issue.

---

## What the engine does right (zero-copy wins)

- `ArrowBinaryBuffer` (V1-sync T3 tier): genuinely zero-alloc-per-op on the hot path. `find` / `insert` take MemorySegment slices, hash and key-equal without byte[]. Off-heap append + retained Arena. `flushTo` pre-allocates the `flushKeyPtrs/Lens/...` segments at allocate time so each flush is one `linker.batchPut(...)` crossing. **Reference implementation**.
- `MemorySegmentDataInputView`: clean off-heap DataInputView. Underused — see C5, C6, C1.
- `ColumnarBatchBuffer.append(DataOutputSerializer ser)` via `getSharedBuffer()`: bypasses `getCopyOfBuffer()` for the vectorized path. The pattern is in the code; the lift is to **propagate** it to all serializer call sites.
- `frs_vectorized_batch_put/get/delete` (Rust): consume caller-owned Arrow buffers via `slice::from_raw_parts` — zero-copy on input. Output side of `vectorized_batch_get` is the right shape (out_data is a single caller-owned segment); the only copy is the `db.get` per-key into out_buf via `ptr::copy_nonoverlapping`, which is unavoidable until db.get returns a pointer (frs_get_pinned exists but isn't wired through the vectorized path).
- `frs_get_pinned` exists (lib.rs:1207) but no vectorized variant. **Untapped**: a `frs_vectorized_batch_get_pinned` would return out_offsets pointing into the memtable's value bytes directly, no copy.

---

## Cross-cutting recommendations

1. **Add a `byte[]`/MemorySegment polymorphic deserialize**: ForStRsInnerTable should expose `deserializeValue(MemorySegment, int, int)` overload. Implement on each state by reusing the V1-sync `MemorySegmentDataInputView` as a thread-local. Then C1, C5, C6 collapse to "use the segment overload."
2. **Audit all `slice.to_vec()` in the FFI crate**: there are at least three on hot paths (lib.rs:3934, 3944, 4040). Each defeats the carefully-built caller-side Arrow buffer. Threading `&[u8]` through the combiners is mechanical.
3. **Stream snapshot uploads**: introduce a streaming writer through `opendal::Writer` and pass through to `SstWriterImpl::finish_streaming`. Drops C3 and C12 together. Most material under Nexmark Q11/Q12 ~100 MB SST writes.
4. **Add `ColumnarBatchBuffer.appendSegment(MemorySegment, off, len)`** — covers C7 and unblocks any future MemorySegment-source data path.
5. **`MapStateCache`**: switch to a long-hash (xxh3) computed at key-bytes site, store `(long hash, byte[] keyRef)` and skip the `BytesKey` boxing. Or use an open-addressed off-heap hash like `ArrowBinaryBuffer`'s own — already in-tree pattern.

---

## Out-of-scope notes for this review

- **Arrow reuse claim**: `ArrowBinaryBuffer` and `ColumnarBatchBuffer` use the Arrow BinaryArray **layout** (`offsets[count+1]: i32` + flat `data: u8[]`) but do **not** use the Arrow crates' `BinaryArray` types. Downstream Parquet/Arrow-IPC integration would require a thin wrapper to expose these as real `arrow::array::BinaryArray` views. Not a perf finding; flag for the Arrow-native S3 sink theme.
- **`frs_vec_iter_prefix_next` chunk format**: not inspected in this round; the iter-entry alloc finding (C5) is about Java-side consumption only.

---

**End of Round 1 — Agent C**
