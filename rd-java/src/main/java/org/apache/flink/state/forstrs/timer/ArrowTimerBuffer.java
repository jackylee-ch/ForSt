/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forstrs.timer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.state.forstrs.SegmentHash;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.function.Supplier;

/**
 * Off-heap binary min-heap of timer pending-buffer entries (analogue of {@code
 * ArrowBinaryBuffer} for the timer queue).
 *
 * <p>Layout:
 *
 * <ul>
 *   <li><b>heapArray</b> — fixed-stride 24-byte rows: {@code {ts:long@0, op:int@8, keyOffset:int@12,
 *       keyLen:int@16, hash:int@20}}. Min-heap by {@code ts} (parent = (i-1)/2, children = 2i+1,
 *       2i+2).
 *   <li><b>keyData</b> — variable-length composite key bytes appended on insert. Grows on demand
 *       (doubles).
 *   <li><b>hashIndex</b> — open-addressed table of (hash:int, heapPos:int) slot pairs sized at
 *       {@code 2 × heap capacity}. EMPTY = -1, TOMBSTONE = -2.
 * </ul>
 *
 * <p>Zero Java alloc on the hot path. All state lives in a shared {@link Arena} closed by {@link
 * #close()}.
 *
 * <p>Single-threaded per Flink slot — no synchronization.
 */
@Internal
public final class ArrowTimerBuffer implements AutoCloseable {

    public static final int OP_ADD = 1;
    public static final int OP_REMOVE = 2;

    public static final int DEFAULT_INITIAL_CAPACITY = 1024;
    public static final int DEFAULT_MAX_CAPACITY = 65_536;

    /** Stride (bytes) per heap row: ts(8) + op(4) + keyOffset(4) + keyLen(4) + hash(4) = 24. */
    private static final int HEAP_ROW_BYTES = 24;

    private static final long TS_OFF = 0L;
    private static final long OP_OFF = 8L;
    private static final long KOFF_OFF = 12L;
    private static final long KLEN_OFF = 16L;
    private static final long HASH_OFF = 20L;

    /** Hash-index slot: (hash:int, heapPos:int) = 8 bytes per slot. */
    private static final int HASH_SLOT_BYTES = 8;

    private static final int EMPTY_SLOT = -1;
    private static final int TOMBSTONE = -2;

    private Arena arena;

    private MemorySegment heapArray; // capacity × HEAP_ROW_BYTES
    private MemorySegment keyData; // raw bytes — variable length
    private MemorySegment hashIndex; // (capacity * 2) × HASH_SLOT_BYTES

    private final int maxCapacity;
    private int capacity;
    private int size;

    private long keyDataUsed;
    private long keyDataCapacity;

    /**
     * TIMER-INDEX (2026-06-11): bytes of {@link #keyData} referenced by LIVE rows. The original
     * pending-buffer use cleared the whole buffer every flush, so {@code keyDataUsed} never
     * out-grew the live set. The memory-resident timer index is LONG-LIVED (it holds every live
     * timer for the queue's lifetime) and appends key bytes on every insert while {@link
     * #removeAt} only drops the row — without compaction {@code keyDataUsed} would grow without
     * bound (one ~50B key per timer ADD over the job's life). {@code liveKeyBytes} lets {@link
     * #appendKey} compact in place (O(live) staging copy) instead of growing when at least half
     * the key area is dead.
     */
    private long liveKeyBytes;

    /**
     * TIMER-INDEX (2026-06-11): count of TOMBSTONE slots in {@link #hashIndex}. Same long-lived
     * concern as {@link #liveKeyBytes}: the open-addressed probe loops only stop at EMPTY slots,
     * so millions of insert/remove cycles would saturate the table with tombstones and degrade
     * every miss-probe to a full-table scan. When tombstones exceed {@code capacity} the table
     * is rebuilt from the live rows (O(size), amortized O(1) per op).
     */
    private int hashTombstones;

    /**
     * B5-HIGH-6: reusable scratch buffer for {@link #swapHeap(int, int)} row copies. Q12-style
     * timer-heavy workloads issue O(log N) heap sifts per add/remove and millions of timer ops, so
     * allocating a fresh {@code byte[24]} per swap was a material hot-path allocation. The buffer
     * is single-threaded by {@link ArrowTimerBuffer}'s "one instance per Flink slot" contract,
     * matching the rest of this class's mutable state.
     */
    private final byte[] heapSwapScratch = new byte[HEAP_ROW_BYTES];

    /**
     * D8-H4 test seam: supplier of a fresh {@link Arena} used by {@link #resize(int)}. Production
     * code uses {@link Arena#ofShared()}; tests can inject a Supplier that returns an Arena whose
     * {@code allocate()} throws on the N-th call, to verify that {@code resize()} rolls back
     * cleanly and does not leak the old arena nor half-mutate instance state.
     */
    private Supplier<Arena> arenaSupplier = Arena::ofShared;

    /** Visitor for {@link #drainUnordered(FlushVisitor)} / {@link #drainTo(FlushVisitor)}. */
    public interface FlushVisitor {
        void visit(int op, MemorySegment keyData, int keyOff, int keyLen, long ts);
    }

    public ArrowTimerBuffer() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_MAX_CAPACITY);
    }

    public ArrowTimerBuffer(int initialCapacity) {
        this(initialCapacity, DEFAULT_MAX_CAPACITY);
    }

    public ArrowTimerBuffer(int initialCapacity, int maxCapacity) {
        // R15-L1: capacity MUST be a power of two — the hash-index linear-probe mask
        // {@code (capacity * 2) - 1} only spans all slots when {@code capacity * 2}
        // is a power of two. A non-pow2 capacity would skip slots and silently drop
        // entries on insert. Mirrors the FlatStateCache:60 rounding pattern.
        this.capacity = nextPow2(Math.max(initialCapacity, 16));
        // R16-M1: round maxCapacity up to the next power of two too. The resize path
        // {@code Math.min(capacity * 2, maxCapacity)} would otherwise produce a non-pow2 if
        // maxCapacity itself was non-pow2, breaking the hash-index linear-probe mask
        // invariant. Rounding here keeps every observable {@code capacity} value pow2.
        this.maxCapacity = nextPow2(Math.max(maxCapacity, this.capacity));
        this.arena = Arena.ofShared();
        allocate(this.capacity, 64 /* avg key bytes */);
    }

    /** R15-L1: round {@code n} up to the next power of two (capped at 2^30). */
    private static int nextPow2(int n) {
        if (n <= 1) {
            return 1;
        }
        int p = Integer.highestOneBit(n - 1) << 1;
        return p > 0 ? p : (1 << 30);
    }

    private void allocate(int cap, int avgKeyBytes) {
        this.keyDataCapacity = (long) cap * avgKeyBytes;
        this.heapArray = arena.allocate((long) cap * HEAP_ROW_BYTES);
        this.keyData = arena.allocate(keyDataCapacity == 0 ? 1 : keyDataCapacity);
        this.hashIndex = arena.allocate((long) cap * 2 * HASH_SLOT_BYTES);
        // initialize hashIndex slots to EMPTY_SLOT
        for (int i = 0; i < cap * 2; i++) {
            hashIndex.set(ValueLayout.JAVA_INT, (long) i * HASH_SLOT_BYTES + 4L, EMPTY_SLOT);
        }
    }

    /**
     * D8-H4 test seam: replaces the {@link Arena} supplier used by {@link #resize(int)}. Visible
     * for tests in this package only.
     */
    void setArenaSupplierForTest(Supplier<Arena> supplier) {
        this.arenaSupplier = supplier;
    }

    /** D8-H4 test seam: returns the current {@link Arena} (visible for tests in this package). */
    Arena arenaForTest() {
        return arena;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    public MemorySegment keyDataSegment() {
        return keyData;
    }

    // ------------------------------------------------------------------
    // Public API
    // ------------------------------------------------------------------

    /**
     * Inserts an ADD op for {@code (keySeg, keyOffset, keyLen)} with timestamp {@code ts}. Returns
     * the heap position.
     */
    public int insertAdd(MemorySegment keySeg, long keyOffset, int keyLen, long ts) {
        return insertNew(keySeg, keyOffset, keyLen, ts, OP_ADD);
    }

    /** Inserts a REMOVE op. Returns the heap position. */
    public int insertRemove(MemorySegment keySeg, long keyOffset, int keyLen, long ts) {
        return insertNew(keySeg, keyOffset, keyLen, ts, OP_REMOVE);
    }

    /** Lookup by key bytes. Returns heap position or -1 if absent. */
    public int find(MemorySegment keySeg, long keyOffset, int keyLen) {
        int h = hashOf(keySeg, keyOffset, keyLen);
        return probeFind(h, keySeg, keyOffset, keyLen);
    }

    /** Returns the op-code of the entry at the given heap position. */
    public int opAt(int heapPos) {
        return heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + OP_OFF);
    }

    /** Returns the timestamp of the entry at the given heap position. */
    public long tsAt(int heapPos) {
        return heapArray.get(ValueLayout.JAVA_LONG, (long) heapPos * HEAP_ROW_BYTES + TS_OFF);
    }

    /** Returns the keyData offset of the entry at the given heap position. */
    public int keyOffsetAt(int heapPos) {
        return heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + KOFF_OFF);
    }

    /** Returns the keyData length of the entry at the given heap position. */
    public int keyLenAt(int heapPos) {
        return heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + KLEN_OFF);
    }

    /**
     * Removes the entry at {@code heapPos}: swaps it with the last live row, decrements {@link
     * #size}, and re-heapifies. Also removes the hash-index entry that referenced {@code heapPos}.
     */
    public void removeAt(int heapPos) {
        if (heapPos < 0 || heapPos >= size) {
            return;
        }
        // TIMER-INDEX: long-lived index hygiene — rebuild the hash table when tombstones
        // dominate (see hashTombstones doc). Done at ENTRY, while every row (including the
        // one about to be removed) is still live, so the rebuilt table is consistent before
        // the tombstone/swap below mutates it.
        if (hashTombstones > capacity) {
            rebuildHashIndex();
        }
        int hashRemoved =
                heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + HASH_OFF);
        // Mark the hash-index slot referencing heapPos as TOMBSTONE.
        tombstoneHashSlot(hashRemoved, heapPos);
        // TIMER-INDEX: the removed row's key bytes are now dead in keyData.
        liveKeyBytes -= keyLenAt(heapPos);

        int last = size - 1;
        if (heapPos == last) {
            size--;
            return;
        }
        // Move last row into heapPos.
        copyRow(last, heapPos);
        // Update hash-index slot for the moved row from `last` to `heapPos`.
        int movedHash =
                heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + HASH_OFF);
        updateHashSlotPos(movedHash, last, heapPos);

        size--;
        // Re-heapify: sift down then up (one direction will be a no-op).
        if (heapPos > 0 && heapLess(heapPos, (heapPos - 1) >>> 1)) {
            siftUp(heapPos);
        } else {
            siftDown(heapPos);
        }
    }

    /**
     * Heap order: timestamp-major, composite-key-bytes tiebreak (unsigned lexicographic). The
     * tiebreak makes equal-ts ordering DETERMINISTIC and equal to the engine's composite-byte
     * order — same-kg same-ts timers fire in element (seq) order, matching both the legacy
     * engine-read path and the RocksDB backend, which our exactness gates compare against.
     */
    private boolean heapLess(int i, int j) {
        long tsI = heapArray.get(ValueLayout.JAVA_LONG, (long) i * HEAP_ROW_BYTES + TS_OFF);
        long tsJ = heapArray.get(ValueLayout.JAVA_LONG, (long) j * HEAP_ROW_BYTES + TS_OFF);
        if (tsI != tsJ) {
            return tsI < tsJ;
        }
        int offI = keyOffsetAt(i);
        int lenI = keyLenAt(i);
        int offJ = keyOffsetAt(j);
        int lenJ = keyLenAt(j);
        int n = Math.min(lenI, lenJ);
        for (int k = 0; k < n; k++) {
            int bi = keyData.get(ValueLayout.JAVA_BYTE, (long) offI + k) & 0xFF;
            int bj = keyData.get(ValueLayout.JAVA_BYTE, (long) offJ + k) & 0xFF;
            if (bi != bj) {
                return bi < bj;
            }
        }
        return lenI < lenJ;
    }

    /**
     * Iterates ALL entries (in heap-array index order) and passes each to the visitor. Used by
     * the priority queue's flush path — engine-side re-sorts on insert. Caller is responsible
     * for calling {@link #clear()} after draining if buffer reuse is desired.
     *
     * <p><b>Round-3 fix S1-7:</b> renamed from {@code drainTo} to make ordering explicit. The
     * old name implied min-heap timestamp order which it never delivered. Any future
     * savepoint/migration caller that needs ordered drain must use {@link #drainOrdered}.
     */
    public void drainUnordered(FlushVisitor v) {
        for (int i = 0; i < size; i++) {
            int op = opAt(i);
            int kOff = keyOffsetAt(i);
            int kLen = keyLenAt(i);
            long ts = tsAt(i);
            v.visit(op, keyData, kOff, kLen, ts);
        }
    }

    /**
     * Backward-compat shim for the old {@code drainTo} name. Delegates to {@link
     * #drainUnordered}. New callers should pick {@code drainUnordered} or {@code drainOrdered}
     * explicitly.
     *
     * @deprecated use {@link #drainUnordered} (heap-index order) or a future {@code
     *     drainOrdered} (strict timestamp order).
     */
    @Deprecated
    public void drainTo(FlushVisitor v) {
        drainUnordered(v);
    }

    /** Clears all entries; reuses capacity for next call. */
    public void clear() {
        size = 0;
        keyDataUsed = 0;
        liveKeyBytes = 0;
        clearHashSlots();
    }

    private void clearHashSlots() {
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            hashIndex.set(ValueLayout.JAVA_INT, (long) i * HASH_SLOT_BYTES + 4L, EMPTY_SLOT);
        }
        hashTombstones = 0;
    }

    /** Returns the stored hash of the entry at the given heap position. */
    private int hashAt(int heapPos) {
        return heapArray.get(ValueLayout.JAVA_INT, (long) heapPos * HEAP_ROW_BYTES + HASH_OFF);
    }

    /**
     * TIMER-INDEX: rebuilds {@link #hashIndex} from the live rows, dropping every TOMBSTONE.
     * O(size); triggered when tombstones exceed {@code capacity} (see {@link #hashTombstones}).
     */
    private void rebuildHashIndex() {
        clearHashSlots();
        for (int row = 0; row < size; row++) {
            hashInsert(hashAt(row), row);
        }
    }

    /**
     * TIMER-INDEX (spill support): returns the {@code rank}-th smallest timestamp currently in
     * the buffer (0-based). Used by the timer queue's cap-spill to choose the eviction cutoff
     * (e.g. {@code tsAtRank(cap/2)} keeps the smallest half). O(n log n) — rare path only.
     */
    public long tsAtRank(int rank) {
        if (size == 0) {
            throw new IllegalStateException("tsAtRank on empty buffer");
        }
        long[] all = new long[size];
        for (int i = 0; i < size; i++) {
            all[i] = tsAt(i);
        }
        Arrays.sort(all);
        return all[Math.min(Math.max(rank, 0), size - 1)];
    }

    /**
     * TIMER-INDEX (spill support): removes EVERY entry whose timestamp is {@code >= cutoffTs}.
     * Returns the number of rows removed. Implementation: in-place stable compaction of the
     * surviving rows, then full heap + hash-index rebuild — O(size), used only on the rare
     * spill/horizon-advance path (never on the per-timer hot path). A loop of {@link #removeAt}
     * would be both O(n²) and unsound to drive by index because each removal re-heapifies.
     */
    public int removeAtOrAboveTs(long cutoffTs) {
        int w = 0;
        long bytes = 0L;
        for (int r = 0; r < size; r++) {
            if (tsAt(r) < cutoffTs) {
                if (w != r) {
                    copyRow(r, w);
                }
                bytes += keyLenAt(w);
                w++;
            }
        }
        int removed = size - w;
        if (removed == 0) {
            return 0;
        }
        size = w;
        liveKeyBytes = bytes;
        // The hash table references old positions wholesale — clear it FIRST so the
        // heapify's swapHeap/updateHashSlotPos calls below hit EMPTY slots and no-op,
        // then re-insert every surviving row at its final position.
        clearHashSlots();
        for (int i = (size >>> 1) - 1; i >= 0; i--) {
            siftDown(i);
        }
        for (int row = 0; row < size; row++) {
            hashInsert(hashAt(row), row);
        }
        return removed;
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
            arena = null;
        }
    }

    // ------------------------------------------------------------------
    // Internal — insert path
    // ------------------------------------------------------------------

    private int insertNew(MemorySegment keySeg, long keyOffset, int keyLen, long ts, int op) {
        if (size >= capacity) {
            if (capacity < maxCapacity) {
                resize(Math.min(capacity * 2, maxCapacity));
            } else {
                throw new IllegalStateException(
                        "ArrowTimerBuffer at maxCapacity=" + maxCapacity + "; caller must flush");
            }
        }
        int hash = hashOf(keySeg, keyOffset, keyLen);
        int kStart = appendKey(keySeg, keyOffset, keyLen);
        int row = size;
        writeRow(row, ts, op, kStart, keyLen, hash);
        size++;
        liveKeyBytes += keyLen;
        hashInsert(hash, row);
        return siftUp(row);
    }

    private int appendKey(MemorySegment seg, long off, int len) {
        if (keyDataUsed + len > keyDataCapacity) {
            // TIMER-INDEX: prefer in-place compaction over growth when at least half of the
            // key area is dead bytes left behind by removeAt (long-lived index usage). Keeps
            // keyData bounded by ~2× the live key bytes instead of growing forever.
            if (liveKeyBytes + len <= keyDataCapacity / 2 && liveKeyBytes <= Integer.MAX_VALUE) {
                compactKeyData();
            } else {
                growKeyData(keyDataUsed + len);
            }
        }
        int start = (int) keyDataUsed;
        MemorySegment.copy(seg, off, keyData, keyDataUsed, len);
        keyDataUsed += len;
        return start;
    }

    /**
     * TIMER-INDEX: rewrites {@link #keyData} so only live rows' key bytes remain, updating each
     * row's keyOffset. Uses a transient on-heap staging copy (NOT a new segment — segments share
     * the arena and are only reclaimed at {@link #close()}, so allocating a fresh segment per
     * compaction would itself leak). O(liveKeyBytes); amortized O(1) per insert.
     */
    private void compactKeyData() {
        byte[] staging = new byte[(int) liveKeyBytes];
        int pos = 0;
        for (int row = 0; row < size; row++) {
            int kOff = keyOffsetAt(row);
            int kLen = keyLenAt(row);
            MemorySegment.copy(keyData, ValueLayout.JAVA_BYTE, kOff, staging, pos, kLen);
            heapArray.set(ValueLayout.JAVA_INT, (long) row * HEAP_ROW_BYTES + KOFF_OFF, pos);
            pos += kLen;
        }
        MemorySegment.copy(staging, 0, keyData, ValueLayout.JAVA_BYTE, 0L, pos);
        keyDataUsed = pos;
    }

    private void growKeyData(long needed) {
        long newCap = Math.max(keyDataCapacity * 2, needed);
        MemorySegment newSeg = arena.allocate(newCap);
        MemorySegment.copy(keyData, 0, newSeg, 0, keyDataUsed);
        keyData = newSeg;
        keyDataCapacity = newCap;
    }

    // ------------------------------------------------------------------
    // Internal — row helpers
    // ------------------------------------------------------------------

    private void writeRow(int row, long ts, int op, int keyOff, int keyLen, int hash) {
        long base = (long) row * HEAP_ROW_BYTES;
        heapArray.set(ValueLayout.JAVA_LONG, base + TS_OFF, ts);
        heapArray.set(ValueLayout.JAVA_INT, base + OP_OFF, op);
        heapArray.set(ValueLayout.JAVA_INT, base + KOFF_OFF, keyOff);
        heapArray.set(ValueLayout.JAVA_INT, base + KLEN_OFF, keyLen);
        heapArray.set(ValueLayout.JAVA_INT, base + HASH_OFF, hash);
    }

    private void copyRow(int srcRow, int dstRow) {
        MemorySegment.copy(
                heapArray,
                (long) srcRow * HEAP_ROW_BYTES,
                heapArray,
                (long) dstRow * HEAP_ROW_BYTES,
                HEAP_ROW_BYTES);
    }

    /**
     * Swaps rows {@code i} and {@code j} (heap + hashIndex). Hash slots are updated so the row
     * reference stays accurate.
     */
    private void swapHeap(int i, int j) {
        if (i == j) {
            return;
        }
        int hi = heapArray.get(ValueLayout.JAVA_INT, (long) i * HEAP_ROW_BYTES + HASH_OFF);
        int hj = heapArray.get(ValueLayout.JAVA_INT, (long) j * HEAP_ROW_BYTES + HASH_OFF);

        // B5-HIGH-6: swap row bytes using the per-instance scratch buffer. Allocating a fresh
        // byte[24] per swap was a hot-path allocation under Q12-style timer workloads
        // (O(log N) sifts × millions of ops). The scratch field is safe to reuse because
        // ArrowTimerBuffer is single-threaded per Flink slot.
        MemorySegment.copy(
                heapArray,
                ValueLayout.JAVA_BYTE,
                (long) i * HEAP_ROW_BYTES,
                heapSwapScratch,
                0,
                HEAP_ROW_BYTES);
        copyRow(j, i);
        MemorySegment.copy(
                heapSwapScratch,
                0,
                heapArray,
                ValueLayout.JAVA_BYTE,
                (long) j * HEAP_ROW_BYTES,
                HEAP_ROW_BYTES);

        // Update hash-index slots referencing i / j to point at their new rows.
        updateHashSlotPos(hi, i, j);
        updateHashSlotPos(hj, j, i);
    }

    private int siftUp(int i) {
        while (i > 0) {
            int parent = (i - 1) >>> 1;
            if (heapLess(i, parent)) {
                swapHeap(i, parent);
                i = parent;
            } else {
                break;
            }
        }
        return i;
    }

    private void siftDown(int i) {
        while (true) {
            int left = 2 * i + 1;
            int right = 2 * i + 2;
            int smallest = i;
            if (left < size && heapLess(left, smallest)) {
                smallest = left;
            }
            if (right < size && heapLess(right, smallest)) {
                smallest = right;
            }
            if (smallest != i) {
                swapHeap(i, smallest);
                i = smallest;
            } else {
                break;
            }
        }
    }

    // ------------------------------------------------------------------
    // Internal — hash index
    // ------------------------------------------------------------------

    /**
     * M5-V2 (hot-path alloc/copy audit 2026-06-12): the polynomial-31 hash is computed DIRECTLY
     * over the {@link MemorySegment} ({@link SegmentHash#polynomial31} — 8-byte {@code getLong}
     * strides folded with the precomputed {@code 31^k} power table, scalar tail ≤7 bytes). The
     * previous D-R4-4 route staged the slice into a thread-local scratch {@code byte[]} for the
     * {@code Arrays.hashCode} intrinsic; that paid (a) an unconditional segment→heap copy per
     * hash and (b) a scratch REALLOC on ANY width mismatch — its doc claimed increase-only, but
     * the code reallocated on every change, so a queue serving two timer states with different
     * key widths reallocated on EVERY find/insert (the V2/V7 code-vs-comment drift). Both the
     * copy and the scratch (and the TL lookup) are gone.
     *
     * <p>The hash output is bitwise-identical to {@code Arrays.hashCode(byte[])} and therefore to
     * every previous formula generation — accumulator starts at {@code 1}, each SIGNED byte folds
     * as {@code h = 31*h + b} — so existing open-addressed slot-layout entries remain compatible
     * (hard requirement; property-tested new-vs-old in ArrowTimerBufferTest).
     */
    private int hashOf(MemorySegment seg, long offset, int len) {
        return SegmentHash.polynomial31(seg, offset, len);
    }

    private boolean rowKeyEquals(int row, MemorySegment seg, long offset, int len) {
        int kStart =
                heapArray.get(ValueLayout.JAVA_INT, (long) row * HEAP_ROW_BYTES + KOFF_OFF);
        int kLen =
                heapArray.get(ValueLayout.JAVA_INT, (long) row * HEAP_ROW_BYTES + KLEN_OFF);
        if (kLen != len) {
            return false;
        }
        // M5-V3 (hot-path alloc/copy audit 2026-06-12): MemorySegment.mismatch
        // intrinsic (SWAR/SIMD, branch-cheap for the common equal case) replaces
        // the scalar per-byte loop run on every hash-probe hit/collision.
        // Semantics identical (kLen != len early-out retained above).
        return MemorySegment.mismatch(
                        keyData, kStart, kStart + (long) len, seg, offset, offset + len)
                == -1;
    }

    private int probeFind(int hash, MemorySegment seg, long offset, int len) {
        int mask = (capacity * 2) - 1;
        int probe = hash & mask;
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int slot = (probe + i) & mask;
            int row =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L);
            if (row == EMPTY_SLOT) {
                return -1;
            }
            if (row == TOMBSTONE) {
                continue;
            }
            int storedHash =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES);
            if (storedHash == hash && rowKeyEquals(row, seg, offset, len)) {
                return row;
            }
        }
        return -1;
    }

    private void hashInsert(int hash, int heapPos) {
        int mask = (capacity * 2) - 1;
        int probe = hash & mask;
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int slot = (probe + i) & mask;
            int existing =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L);
            if (existing == EMPTY_SLOT || existing == TOMBSTONE) {
                if (existing == TOMBSTONE) {
                    hashTombstones--; // TIMER-INDEX: slot reuse retires the tombstone
                }
                hashIndex.set(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES, hash);
                hashIndex.set(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L, heapPos);
                return;
            }
        }
        throw new IllegalStateException("hashIndex full — should not happen after resize");
    }

    /** Finds the slot with (hash, oldPos) and marks it TOMBSTONE. */
    private void tombstoneHashSlot(int hash, int heapPos) {
        int mask = (capacity * 2) - 1;
        int probe = hash & mask;
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int slot = (probe + i) & mask;
            int row =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L);
            if (row == EMPTY_SLOT) {
                return;
            }
            if (row == heapPos) {
                int storedHash =
                        hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES);
                if (storedHash == hash) {
                    hashIndex.set(
                            ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L, TOMBSTONE);
                    hashTombstones++; // TIMER-INDEX: see rebuild trigger in removeAt
                    return;
                }
            }
        }
    }

    /** Re-targets the slot that held (hash, oldPos) to (hash, newPos). */
    private void updateHashSlotPos(int hash, int oldPos, int newPos) {
        int mask = (capacity * 2) - 1;
        int probe = hash & mask;
        int slots = capacity * 2;
        for (int i = 0; i < slots; i++) {
            int slot = (probe + i) & mask;
            int row =
                    hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L);
            if (row == EMPTY_SLOT) {
                return;
            }
            if (row == oldPos) {
                int storedHash =
                        hashIndex.get(ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES);
                if (storedHash == hash) {
                    hashIndex.set(
                            ValueLayout.JAVA_INT, (long) slot * HASH_SLOT_BYTES + 4L, newPos);
                    return;
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Internal — resize
    // ------------------------------------------------------------------

    private void resize(int newCapacity) {
        if (newCapacity > maxCapacity) {
            newCapacity = maxCapacity;
        }
        // Snapshot CURRENT (old) state. We must not mutate any instance field until all newArena
        // allocations have succeeded — otherwise an OOM mid-resize would leave the instance in a
        // half-mutated state AND leak oldArena (D8-H4).
        final Arena oldArena = arena;
        final MemorySegment oldHeapArray = heapArray;
        final MemorySegment oldKeyData = keyData;
        final MemorySegment oldHashIndex = hashIndex;
        final int oldCapacity = capacity;
        final long oldKeyDataCapacity = keyDataCapacity;
        final int oldSize = size;
        final long oldKeyDataUsed = keyDataUsed;

        final long newKeyDataCapacity = Math.max((long) newCapacity * 64L, oldKeyDataUsed);

        // Allocate everything on newArena up-front; if ANY allocate() throws, close newArena and
        // re-throw — instance fields are untouched, so the caller still has a valid buffer.
        final Arena newArena = arenaSupplier.get();
        final MemorySegment newHeapArray;
        final MemorySegment newKeyData;
        final MemorySegment newHashIndex;
        try {
            newHeapArray = newArena.allocate((long) newCapacity * HEAP_ROW_BYTES);
            newKeyData = newArena.allocate(newKeyDataCapacity == 0 ? 1 : newKeyDataCapacity);
            newHashIndex = newArena.allocate((long) newCapacity * 2 * HASH_SLOT_BYTES);
        } catch (Throwable t) {
            // D8-H4: rollback. Close the partially-populated newArena; instance fields are still
            // pointing at oldArena's segments which remain valid. Re-throw so the caller can react.
            newArena.close();
            throw t;
        }

        // All allocations succeeded — now commit the new state. From here on, no further allocs
        // happen and the operations below are infallible (segment-typed set/copy + index rebuild).
        this.arena = newArena;
        this.capacity = newCapacity;
        this.keyDataCapacity = newKeyDataCapacity;
        this.heapArray = newHeapArray;
        this.keyData = newKeyData;
        this.hashIndex = newHashIndex;
        for (int i = 0; i < newCapacity * 2; i++) {
            hashIndex.set(ValueLayout.JAVA_INT, (long) i * HASH_SLOT_BYTES + 4L, EMPTY_SLOT);
        }
        hashTombstones = 0; // TIMER-INDEX: fresh table has no tombstones

        // liveKeyBytes is unchanged by resize (rows are copied verbatim below).

        // Copy heapArray rows + keyData bytes verbatim — keeps row indices stable, so the hash
        // index can be rebuilt by re-inserting from each existing row's stored hash.
        MemorySegment.copy(oldHeapArray, 0, heapArray, 0, (long) oldSize * HEAP_ROW_BYTES);
        MemorySegment.copy(oldKeyData, 0, keyData, 0, oldKeyDataUsed);
        this.keyDataUsed = oldKeyDataUsed;
        this.size = oldSize;
        for (int row = 0; row < oldSize; row++) {
            int hash =
                    heapArray.get(ValueLayout.JAVA_INT, (long) row * HEAP_ROW_BYTES + HASH_OFF);
            hashInsert(hash, row);
        }
        // Suppress unused-warning on the old segment locals — they intentionally pin the old
        // arena's memory through this call so the copies above are sound.
        assert oldHeapArray.address() != 0L
                && oldKeyData.address() != 0L
                && oldHashIndex.address() != 0L
                && oldCapacity >= 0
                && oldKeyDataCapacity >= 0L;
        oldArena.close();
    }
}
