# Round 3 — Agent D — JDK 25 Feature Leverage Audit

**Reviewer:** Agent D
**Angle:** JDK 25 leverage opportunities not surfaced by Rounds 1/2.
**Date:** 2026-05-22
**Files re-read:** `ArrowTimerBuffer.java`, `FlatStateCache.java`, `VectorizedExecutor.java`, `ColumnarBatchBuffer.java`, `AppendMergeBatchBuffer.java`, `IterRangeBatchBuffer.java`, `IterPrefixBatchBuffer.java`, `ForStRsKeyGroupedSerializer.java`, `MapStateCache.java`, `ReducingAggregatingCache.java`, `ForStRsRestoreOperation.java`, `ForStRsIncrementalKeyedStateHandle.java`, `ForStRsKeyGroupedInternalPriorityQueue.java`.

---

## Summary table

| Sev | # | Location | One-line |
|-----|---|----------|----------|
| H | D-R3-1 | `ArrowTimerBuffer.java:388-394` (`hashOf`) | Byte-by-byte polynomial hash (`h = 31*h + seg.get(JAVA_BYTE, off+i)`) on the timer-buffer composite-key hot path — Round 1 D5 caught `ArrowBinaryBuffer.hash` but missed the timer-buffer analogue. Per project memory (Q12 timer burst), this runs on every `OP_ADD`/`OP_REMOVE`. `ByteVector`-rolling-31 hash matching `Arrays.hashCode(byte[])` semantics is the same fix shape Round 1 prescribed for ArrowBinaryBuffer. |
| H | D-R3-2 | `VectorizedExecutor.java:108,399-400,483-486,605-616,621-623` + `ColumnarBatchBuffer.java:62,66,73,88,113,141-149,168` | All `offsets` segments allocated with **`JAVA_INT`** (4-byte aligned layout), accessed via `(long) i * Integer.BYTES`. The aligned layout forces the JIT to emit an alignment check on every load/store, even though the indexed pattern is always 4-byte aligned by construction. `JAVA_INT_UNALIGNED` (already used correctly in `ForStRsDBIterRequest.java:390` and `ForStRsLinker.java:1362,1477,2719,2926`) lets the JIT drop the check and emit a single unaligned-mov on x86_64/aarch64 (both have free unaligned access). 8+ indexed-write call sites in the V1-sync and batched-dispatch hot paths. |
| H | D-R3-3 | `FlatStateCache.java:152-164` (`readInt`/`writeInt` on `byte[]`) | Manual byte-shift-OR int packing — runs on every cache `lookup`, `put`, `contains`. `MethodHandles.byteArrayViewVarHandle(int[].class, BIG_ENDIAN)` (JDK 9+, stable) compiles to a single `BSWAP` + `MOV` on x86_64. Currently each `readInt` is 4 array-load + 3 shift + 3 OR = 10 instructions; the VarHandle path is 2. **At 92M Q11 V1-sync ops, multi-call per op (kLen + vLen on every hit)**, this is on the critical path. The same pattern fix applies to the wider class of `byte[]`-backed encoders. |
| M | D-R3-4 | `VectorizedExecutor.java:608-616` | The `for (row=0..count) opsOffSeg.set(JAVA_INT, row*4, opsOffsets[row])` loop is a **bulk `int[] -> MemorySegment` copy** in disguise. `MemorySegment.copy(opsOffsets, 0, opsOffSeg, JAVA_INT_UNALIGNED, 0L, count+1)` is one JNI/intrinsic call (compiles to `memcpy`/`rep movsd`) instead of `count+1` `VarHandle.set` JIT-issued stores. Q12 batch median = 257 ⇒ ~257 store calls reduced to 1 bulk copy. |
| L | D-R3-5 | (negative — SequencedCollection / SequencedMap, JEP 431) | All `LinkedHashMap` uses are either (a) **LRU via `removeEldestEntry`** (`MapStateCache.java:74-80`, `ReducingAggregatingCache.java:118-125` — requires `accessOrder=true`, no SequencedMap equivalent), or (b) **deterministic insertion-order maps for checkpoint round-trip** (`ForStRsIncrementalKeyedStateHandle.java:86`, `ForStRsRestoreOperation.java:230,297`, `ForStRsSnapshotStrategy.java:124` — never call `first*/last*`). `ArrayDeque` (`ForStRsKeyGroupedInternalPriorityQueue.java:178`) already implements SequencedCollection — no API change needed. **No HIGH refactor candidates.** |
| L | D-R3-6 | (confirming — `ScopedValue`) | Re-confirmed Round 2 conclusion. All `ThreadLocal`s are mutable buffer pools; `ScopedValue` is immutable-within-scope. No change. |

**HIGH count: 3** (D-R3-1, D-R3-2, D-R3-3). M: 1. L (negative / confirmation): 2.

---

## D-R3-1 — Vector API for `ArrowTimerBuffer.hashOf`

`ArrowTimerBuffer.java:388-394`:
```java
private int hashOf(MemorySegment seg, long offset, int len) {
    int h = 1;
    for (int i = 0; i < len; i++) {
        h = 31 * h + seg.get(ValueLayout.JAVA_BYTE, offset + i);
    }
    return h;
}
```

Called from `add` (line 147) and `remove`/`addOrUpdate` (line 259, 534). Round 1 D5 caught `ArrowBinaryBuffer.hash` but the **timer buffer was missed** even though it is structurally identical and lives on the Q12 timer hot path (per `project_q12_timer_batchput_win`, timer ops dominate Q12 burst windows).

The Vector API rolling-31 hash matching `Arrays.hashCode(byte[])` is:
```java
ByteVector bv = ByteVector.fromMemorySegment(BYTE_SPECIES, seg, offset, ByteOrder.nativeOrder());
IntVector iv = (IntVector) bv.castShape(INT_SPECIES, 0).convert(VectorOperators.B2I, 0);
// fold lanes with h = 31*h + lane
```

Or, simpler, use `MemorySegment.toArray(JAVA_BYTE)` + `Arrays.hashCode(byte[])` (already intrinsified to SIMD on JDK 16+) if the per-call array allocation is amortized by the slot-arena turn region. Either form is a single SIMD pass.

Risk: low. Existing `ForStRsKeyGroupedSerializer.java` already imports `jdk.incubator.vector.*`, so the dependency is approved.

---

## D-R3-2 — `JAVA_INT` vs `JAVA_INT_UNALIGNED` on offsets segments

The codebase has **two consistent dialects** of int-segment access:

- `ForStRsLinker.java`, `ForStRsDBIterRequest.java`: use **`JAVA_INT_UNALIGNED` / `JAVA_LONG_UNALIGNED`** for indexed reads.
- `VectorizedExecutor.java`, `ColumnarBatchBuffer.java`: use **`JAVA_INT`** (aligned).

The aligned layout is correct (all indexed accesses are `i * Integer.BYTES`, hence 4-byte aligned), but JEP 442 (Foreign Function & Memory API, finalized in JDK 22) explicitly recommends `*_UNALIGNED` for arbitrary-offset access:

> When the alignment is statically known by the programmer, but cannot be inferred from the layout API, `*_UNALIGNED` layouts avoid an unnecessary alignment check at JIT time.

Concretely, `JAVA_INT` access at offset `o` is lowered to:
```
test rcx, 3        // alignment check
jnz   slow_path    // unaligned-fault handler
mov   eax, [rdi+rcx]
```

`JAVA_INT_UNALIGNED` is lowered to:
```
mov   eax, [rdi+rcx]
```

On x86_64 and aarch64, unaligned `mov` is free. The alignment check is pure dead weight.

Sites (8 read + 8 write, all in the V1-sync / batched-dispatch hot path):

| File:line | Op |
|---|---|
| `VectorizedExecutor.java:108` | alloc offsets segment with `JAVA_INT` layout |
| `VectorizedExecutor.java:399-400` | read offsets (per-row, in output decode) |
| `VectorizedExecutor.java:483-486` | read offsets (APPEND_MERGE per-row dispatch) |
| `VectorizedExecutor.java:605-616` | alloc + indexed write of opsOffSeg |
| `VectorizedExecutor.java:621-623` | read keysOffSeg in batch dispatch |
| `ColumnarBatchBuffer.java:62,66,73,88,113,141-149,168` | alloc + read + write of offsets |

**Fix shape:** mechanical rename `ValueLayout.JAVA_INT` → `ValueLayout.JAVA_INT_UNALIGNED` at every access site. Allocation can keep `JAVA_INT` (the layout determines alignment of the allocation, not the access mode), or switch to `JAVA_INT_UNALIGNED` for layout consistency.

Direct answer to prompt question #6: **YES** — `dispatchAppendMergeBatch`'s offsets segment writes use `JAVA_INT` (line 605, 609, 616), and switching to `JAVA_INT_UNALIGNED` is exactly the win the prompt anticipated. The pattern is uniform across `ColumnarBatchBuffer` too.

---

## D-R3-3 — `byteArrayViewVarHandle` for FlatStateCache header packing

`FlatStateCache.java:152-164`:
```java
private static int readInt(byte[] buf, int off) {
    return (buf[off] & 0xFF) << 24
            | (buf[off + 1] & 0xFF) << 16
            | (buf[off + 2] & 0xFF) << 8
            | (buf[off + 3] & 0xFF);
}

private static void writeInt(byte[] buf, int off, int val) {
    buf[off] = (byte) (val >>> 24);
    buf[off + 1] = (byte) (val >>> 16);
    buf[off + 2] = (byte) (val >>> 8);
    buf[off + 3] = (byte) val;
}
```

Called from every `lookup` (lines 67-68: reads kLen+vLen on each linear probe step), every `put` (lines 110-111, 122-123), every `contains` (line 140). For V1-sync Q11 92M ops with a typical 2-3 probe steps on a hot cache, this is hundreds of millions of `readInt` calls.

The drop-in JDK 25-stable fix:
```java
private static final VarHandle INT_VH_BE =
    MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

private static int readInt(byte[] buf, int off) {
    return (int) INT_VH_BE.get(buf, off);  // single BSWAP+MOV on x86_64
}
private static void writeInt(byte[] buf, int off, int val) {
    INT_VH_BE.set(buf, off, val);
}
```

C2 intrinsifies `byteArrayViewVarHandle` since JDK 9. On x86_64: `MOVBE` (or `MOV` + `BSWAP`). One instruction vs ten. Same semantics (big-endian).

Note: `readInt` byte 0 is sign-extended in the current code due to `<< 24` on a `(byte) & 0xFF` — the VarHandle version produces the same `int` directly (it loads 32 bits and byte-swaps).

Risk: zero. This is a textbook JDK idiom, stable since JDK 9.

---

## D-R3-4 — Bulk-copy the offsets array

In `dispatchAppendMergeBatch` (line 608-616), after computing `opsOffsets[]` as a Java `int[]`, the executor loop-stores them into `opsOffSeg` one int at a time:

```java
for (int row = 0; row < count; row++) {
    opsOffSeg.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, opsOffsets[row]);
    ...
}
opsOffSeg.set(ValueLayout.JAVA_INT, (long) count * Integer.BYTES, opsTotal);
```

The intent is `int[] → MemorySegment` copy; the implementation is `count+1` separate `VarHandle.set` calls. JDK ≥ 22 has the bulk overload:
```java
MemorySegment.copy(
    opsOffsets,       // src int[]
    0,                // src index
    opsOffSeg,        // dst segment
    ValueLayout.JAVA_INT_UNALIGNED,
    0L,               // dst offset (bytes)
    count + 1);       // element count
```

Which compiles to a single `Unsafe.copyMemory` (`memcpy`/`rep movsd`). At Q12 work-weighted batch median 257, this collapses ~257 store calls + the interleaved `MemorySegment.copy(op, ...)` for `opsDataSeg` into 1+1 bulk transfers.

---

## D-R3-5 — SequencedCollection (JEP 431) — negative finding

JEP 431 (finalized JDK 21) adds `SequencedCollection`/`SequencedMap` with `getFirst/getLast/reversed`. Reviewed all `LinkedHashMap`/`LinkedHashSet`/`ArrayDeque` uses:

| File | Use | SequencedMap fit? |
|---|---|---|
| `MapStateCache.java:74` | LRU via `accessOrder=true` + `removeEldestEntry` | **NO** — SequencedMap doesn't support access-order LRU |
| `ReducingAggregatingCache.java:118` | LRU via `accessOrder=true` + `removeEldestEntry` | **NO** — same |
| `ForStRsIncrementalKeyedStateHandle.java:86` | Insertion-order map for checkpoint round-trip | NO — no first/last calls; insertion order alone is sufficient |
| `ForStRsRestoreOperation.java:230,297,422` | Insertion-order map for CF merge | NO — same |
| `ForStRsSnapshotStrategy.java:124` | Insertion-order map | NO — same |
| `PerStateCfRouter.java:28` | Insertion-order routing map | NO — same |
| `ForStRsKeyedStateBackend.java:625` | `LinkedHashSet<K>` for `seen` dedup w/ insertion order | NO — `Set.contains` is the hot path |
| `ForStRsKeyGroupedInternalPriorityQueue.java:178,643` | `ArrayDeque<Entry>` (already SequencedCollection), `LinkedHashSet` | already-conformant or no-fit |
| `PendingMiss.java:36` | `ArrayDeque<IN>` for pending inputs FIFO | already SequencedCollection |

**No HIGH refactor candidates.** The LRU caches are the wrong shape (SequencedMap doesn't replace `removeEldestEntry`), and the other uses don't call `first*/last*`. Confirmed via grep for `getFirst|getLast|firstEntry|lastEntry|reversed`.

---

## D-R3-6 — `ScopedValue` reconfirmation

Re-checked per the prompt. All `ThreadLocal`s are mutable buffer pools (linker out-buf at 1326-1327, 2318-2321, 2685-2686; `scratchArenaTL` at backend 228; `POOL` at `ForStRsKeyGroupedSerializer.java:58`). `ScopedValue` is immutable-within-scope ⇒ wrong shape. Round 2 conclusion stands.

---

## Direct answers to prompt focus areas

1. **Vector API ByteVector for CRC/checksum/hash:** `ArrowTimerBuffer.hashOf` — **NEW HIGH** (D-R3-1). No SST block checksum or compaction merge hash in Java — those live in Rust.

2. **`byteArrayViewVarHandle` for branchless byte→int:** `FlatStateCache.readInt/writeInt` — **NEW HIGH** (D-R3-3). Other byte-shift-OR sites in `ForStRsLinker` already use `MemorySegment.get(JAVA_LONG_UNALIGNED, ...)` on `MemorySegment.ofArray(outBuf)`, which is equivalent.

3. **StringTemplate:** N/A (preview only, no use case here).

4. **`ScopedValue`:** rejected (D-R3-6 confirms Round 2).

5. **JDK 25 SequencedCollection:** no HIGH candidates (D-R3-5 — LinkedHashMap usage is LRU or insertion-order-only).

6. **`MemorySegment.set(JAVA_INT_UNALIGNED, ...)`:** YES — `dispatchAppendMergeBatch` uses `JAVA_INT` at offsets segment writes (line 605, 609, 616), and the whole `ColumnarBatchBuffer` family uses `JAVA_INT` consistently. **NEW HIGH** (D-R3-2). Cross-file uniformity issue: linker side uses UNALIGNED, executor/buffer side uses aligned.
