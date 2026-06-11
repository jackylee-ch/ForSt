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

import org.apache.flink.state.forstrs.SegmentHash;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ArrowTimerBuffer} — covers the 4 critical correctness invariants:
 *
 * <ol>
 *   <li>insertAdd then insertRemove (via cancellation in the caller) yields no entries on drain.
 *   <li>Min-heap ordering by timestamp.
 *   <li>Overwrite-via-cancel-then-insert leaves only the latest entry surviving.
 *   <li>Resize on growth: insert beyond initial capacity, all entries retrievable in order.
 * </ol>
 */
class ArrowTimerBufferTest {

    private Arena scratchArena;
    private MemorySegment scratch;

    @org.junit.jupiter.api.BeforeEach
    void setUp() {
        scratchArena = Arena.ofShared();
        scratch = scratchArena.allocate(256);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        if (scratchArena != null) {
            scratchArena.close();
        }
    }

    /** Writes a 4-byte int into the scratch segment and returns its length. */
    private int putIntKey(int v) {
        scratch.set(ValueLayout.JAVA_INT, 0L, v);
        return 4;
    }

    /**
     * 1. insertAdd + removeAt cancels the entry — drainTo yields no rows.
     */
    @Test
    void insertAddThenRemoveCancels() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(32)) {
            int len = putIntKey(42);
            int pos = buf.insertAdd(scratch, 0L, len, 100L);
            assertEquals(1, buf.size());
            // Look up by key bytes, then remove — the cancellation pattern used by the priority
            // queue's add-then-remove path.
            int found = buf.find(scratch, 0L, len);
            assertEquals(pos, found);
            buf.removeAt(found);
            assertEquals(0, buf.size());

            List<Long> drained = new ArrayList<>();
            buf.drainTo((op, kd, kOff, kLen, ts) -> drained.add(ts));
            assertTrue(drained.isEmpty());
        }
    }

    /**
     * 2. Min-heap ordering: insert ts = 10, 5, 20, 1, 15; drainTo yields min-heap-walk ordering
     *    where the root is always the smallest. We assert root is smallest and at-least every
     *    parent ≤ children (heap invariant), then full sort comes from repeated remove-of-root.
     */
    @Test
    void minHeapOrderingByTs() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(32)) {
            long[] tss = {10L, 5L, 20L, 1L, 15L};
            for (int i = 0; i < tss.length; i++) {
                scratch.set(ValueLayout.JAVA_INT, 0L, i);
                buf.insertAdd(scratch, 0L, 4, tss[i]);
            }
            assertEquals(5, buf.size());

            // Heap invariant: root must be ts = 1.
            assertEquals(1L, buf.tsAt(0));
            // For every internal node i, ts[i] <= ts[2i+1] (if exists) and <= ts[2i+2] (if exists).
            int n = buf.size();
            for (int i = 0; i < n; i++) {
                int left = 2 * i + 1;
                int right = 2 * i + 2;
                long parentTs = buf.tsAt(i);
                if (left < n) {
                    assertTrue(parentTs <= buf.tsAt(left), "heap violation at left child");
                }
                if (right < n) {
                    assertTrue(parentTs <= buf.tsAt(right), "heap violation at right child");
                }
            }

            // Now drain by repeatedly removing root — yields ascending-ts sequence.
            List<Long> ordered = new ArrayList<>();
            while (buf.size() > 0) {
                ordered.add(buf.tsAt(0));
                buf.removeAt(0);
            }
            assertEquals(List.of(1L, 5L, 10L, 15L, 20L), ordered);
        }
    }

    /**
     * 3. Cancel then re-insert at the same key: only the latest entry survives. Models the
     *    priority queue's add-cancel-add cycle.
     */
    @Test
    void insertOverwriteRetainsHeapInvariant() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(32)) {
            int len = putIntKey(7);
            int first = buf.insertAdd(scratch, 0L, len, 100L);
            assertEquals(1, buf.size());
            assertEquals(100L, buf.tsAt(first));

            // Cancel
            int found = buf.find(scratch, 0L, len);
            buf.removeAt(found);
            assertEquals(0, buf.size());
            // Re-insert with a different ts.
            int second = buf.insertAdd(scratch, 0L, len, 50L);
            assertEquals(1, buf.size());
            assertEquals(50L, buf.tsAt(second));
            // The original entry should no longer be findable at ts=100; find() returns the
            // current row.
            int found2 = buf.find(scratch, 0L, len);
            assertNotEquals(-1, found2);
            assertEquals(50L, buf.tsAt(found2));
        }
    }

    /**
     * D8-H4: resize() must roll back cleanly when an allocate() throws on the new arena. Verifies:
     * <ul>
     *   <li>the failed newArena was closed (no native leak),
     *   <li>the buffer's old arena is still alive (instance state pristine),
     *   <li>the buffer remains fully functional — subsequent insert / drain succeed.
     * </ul>
     */
    @Test
    void resizeRollsBackOnAllocFailure() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(16, 1024)) {
            // Pre-populate up to initial capacity so the NEXT insertAdd triggers a resize.
            int initialCap = buf.capacity();
            for (int i = 0; i < initialCap; i++) {
                scratch.set(ValueLayout.JAVA_INT, 0L, i);
                buf.insertAdd(scratch, 0L, 4, 100L + i);
            }
            assertEquals(initialCap, buf.size());
            Arena oldArena = buf.arenaForTest();
            int snapshotSize = buf.size();

            // Inject a faulty Arena supplier — second allocate() on the new arena (i.e. keyData
            // allocate inside resize) throws. Track the produced arena so we can verify it gets
            // closed by the rollback path.
            List<Arena> producedArenas = new ArrayList<>();
            buf.setArenaSupplierForTest(
                    () -> {
                        Arena failing = new FailingArena(Arena.ofShared(), 2);
                        producedArenas.add(failing);
                        return failing;
                    });

            // The very next insert sees size >= capacity → resize → throws.
            scratch.set(ValueLayout.JAVA_INT, 0L, 9000);
            Throwable observed = null;
            try {
                buf.insertAdd(scratch, 0L, 4, 200L);
            } catch (Throwable t) {
                observed = t;
            }
            assertNotEquals(null, observed, "resize() should have propagated the alloc failure");
            assertTrue(
                    observed instanceof OutOfMemoryError,
                    "expected OutOfMemoryError, got " + observed);

            // The faulty arena must have been closed exactly once by the rollback path.
            assertEquals(1, producedArenas.size(), "supplier should fire exactly once");
            assertFalse(
                    producedArenas.get(0).scope().isAlive(),
                    "failed newArena must be closed by the rollback path");

            // The buffer's instance arena must still point at the original (old) arena and be
            // alive — the rollback path must not have swapped any field.
            assertTrue(oldArena.scope().isAlive(), "old arena must remain alive after rollback");
            assertEquals(oldArena, buf.arenaForTest(), "arena field must not have been swapped");
            assertEquals(snapshotSize, buf.size(), "size must not have been touched");
            assertEquals(initialCap, buf.capacity(), "capacity must not have been touched");

            // Restore the default supplier so a real grow can happen on the next insert.
            buf.setArenaSupplierForTest(Arena::ofShared);

            // The buffer must still be fully functional — insert succeeds (now triggers a real
            // resize on the same insertAdd path).
            scratch.set(ValueLayout.JAVA_INT, 0L, 9999);
            buf.insertAdd(scratch, 0L, 4, 50L);
            assertEquals(snapshotSize + 1, buf.size(), "post-rollback insert should succeed");
            // Min-heap root is now the new entry with ts=50.
            assertEquals(50L, buf.tsAt(0));
        }
    }

    /**
     * Wrapper {@link Arena} whose {@link #allocate(long, long)} throws an OutOfMemoryError on the
     * N-th call (1-indexed). Used by {@link #resizeRollsBackOnAllocFailure} to simulate mid-resize
     * allocation failure.
     */
    private static final class FailingArena implements Arena {
        private final Arena delegate;
        private final AtomicInteger calls = new AtomicInteger(0);
        private final int failOnCall;

        FailingArena(Arena delegate, int failOnCall) {
            this.delegate = delegate;
            this.failOnCall = failOnCall;
        }

        @Override
        public MemorySegment allocate(long byteSize, long byteAlignment) {
            int n = calls.incrementAndGet();
            if (n == failOnCall) {
                throw new OutOfMemoryError("FailingArena: forced failure on call " + n);
            }
            return delegate.allocate(byteSize, byteAlignment);
        }

        @Override
        public MemorySegment.Scope scope() {
            return delegate.scope();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    // ------------------------------------------------------------------
    // M5-V2/V3 (hot-path alloc/copy audit 2026-06-12)
    // ------------------------------------------------------------------

    /**
     * M5-V2 hash-identity property test (audit gate 1): {@link SegmentHash#polynomial31} must be
     * bitwise-identical to {@code Arrays.hashCode(byte[])} over the same bytes — open-addressed
     * hash-index slots store the hash, so any divergence breaks slot-layout compatibility.
     * Random lengths/contents including the audit's required 0/1/7/8/9 boundary set (stride is
     * 8 bytes: 7 = pure tail, 8 = pure stride, 9 = stride + 1 tail), at non-zero offsets too.
     */
    @Test
    void segmentHashMatchesArraysHashCodeProperty() {
        java.util.Random rnd = new java.util.Random(0x5EED_CAFE);
        int[] mandatoryLens = {0, 1, 7, 8, 9, 15, 16, 17, 31, 32, 100, 255};
        try (Arena a = Arena.ofConfined()) {
            MemorySegment seg = a.allocate(4096);
            byte[] backing = new byte[4096];
            rnd.nextBytes(backing);
            MemorySegment.copy(backing, 0, seg, ValueLayout.JAVA_BYTE, 0L, backing.length);

            // Mandatory boundary lengths at offset 0 and at random offsets.
            for (int len : mandatoryLens) {
                for (int rep = 0; rep < 8; rep++) {
                    int off = rep == 0 ? 0 : rnd.nextInt(backing.length - len + 1);
                    byte[] expectBytes = java.util.Arrays.copyOfRange(backing, off, off + len);
                    assertEquals(
                            java.util.Arrays.hashCode(expectBytes),
                            SegmentHash.polynomial31(seg, off, len),
                            "len=" + len + " off=" + off);
                }
            }
            // Random fuzz: 500 (offset, length) pairs.
            for (int i = 0; i < 500; i++) {
                int len = rnd.nextInt(300);
                int off = rnd.nextInt(backing.length - len + 1);
                byte[] expectBytes = java.util.Arrays.copyOfRange(backing, off, off + len);
                assertEquals(
                        java.util.Arrays.hashCode(expectBytes),
                        SegmentHash.polynomial31(seg, off, len),
                        "fuzz len=" + len + " off=" + off);
            }
        }
    }

    /**
     * M5-V3 mismatch-equivalence at stride boundaries: find() (hashOf + rowKeyEquals) must keep
     * exact-equality semantics for key lengths around the 8-byte stride (1/7/8/9/16/17/33) — a
     * stored key is found by its exact bytes, and near-miss probes (first byte, last byte, or a
     * straddling middle byte flipped; length ±1) must MISS, never falsely hit.
     */
    @Test
    void findExactAndNearMissAtStrideBoundaryLengths() {
        int[] lens = {1, 7, 8, 9, 16, 17, 33};
        java.util.Random rnd = new java.util.Random(0xBEEF);
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(64)) {
            byte[][] keys = new byte[lens.length][];
            for (int i = 0; i < lens.length; i++) {
                keys[i] = new byte[lens[i]];
                rnd.nextBytes(keys[i]);
                keys[i][0] = (byte) i; // disambiguate any accidental prefix collision
                MemorySegment.copy(keys[i], 0, scratch, ValueLayout.JAVA_BYTE, 0L, lens[i]);
                buf.insertAdd(scratch, 0L, lens[i], 1000L + i);
            }
            for (int i = 0; i < lens.length; i++) {
                int len = lens[i];
                // Exact bytes → HIT with the right ts.
                MemorySegment.copy(keys[i], 0, scratch, ValueLayout.JAVA_BYTE, 0L, len);
                int row = buf.find(scratch, 0L, len);
                assertNotEquals(-1, row, "exact find must hit, len=" + len);
                assertEquals(1000L + i, buf.tsAt(row));

                // Last byte flipped → MISS.
                scratch.set(
                        ValueLayout.JAVA_BYTE,
                        len - 1,
                        (byte) (keys[i][len - 1] ^ 0x01));
                assertEquals(-1, buf.find(scratch, 0L, len), "last-byte near-miss, len=" + len);

                // Middle byte flipped (stride-straddling for len > 8) → MISS.
                MemorySegment.copy(keys[i], 0, scratch, ValueLayout.JAVA_BYTE, 0L, len);
                scratch.set(ValueLayout.JAVA_BYTE, len / 2, (byte) (keys[i][len / 2] ^ 0x80));
                assertEquals(-1, buf.find(scratch, 0L, len), "middle-byte near-miss, len=" + len);

                // Length-1 prefix of the stored key → MISS (length is part of equality).
                if (len > 1) {
                    MemorySegment.copy(keys[i], 0, scratch, ValueLayout.JAVA_BYTE, 0L, len);
                    assertEquals(
                            -1, buf.find(scratch, 0L, len - 1), "short-probe near-miss, len=" + len);
                }
            }
        }
    }

    /**
     * M5-V2 alternating-width regression: a buffer serving keys of two different widths must keep
     * find/insert exact across alternation. (Pre-V2 this was also the realloc-per-call pathology:
     * the TL scratch was replaced on ANY width mismatch while its doc claimed increase-only — the
     * scratch is gone entirely now; this pins the correctness half.)
     */
    @Test
    void alternatingKeyWidthsFindExact() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(64)) {
            for (int i = 0; i < 32; i++) {
                if ((i & 1) == 0) {
                    scratch.set(ValueLayout.JAVA_INT, 0L, i);
                    buf.insertAdd(scratch, 0L, 4, 100L + i);
                } else {
                    scratch.set(ValueLayout.JAVA_LONG, 0L, i);
                    scratch.set(ValueLayout.JAVA_INT, 8L, i);
                    buf.insertAdd(scratch, 0L, 12, 100L + i);
                }
            }
            assertEquals(32, buf.size());
            for (int i = 0; i < 32; i++) {
                int row;
                if ((i & 1) == 0) {
                    scratch.set(ValueLayout.JAVA_INT, 0L, i);
                    row = buf.find(scratch, 0L, 4);
                } else {
                    scratch.set(ValueLayout.JAVA_LONG, 0L, i);
                    scratch.set(ValueLayout.JAVA_INT, 8L, i);
                    row = buf.find(scratch, 0L, 12);
                }
                assertNotEquals(-1, row, "alternating-width find must hit, i=" + i);
                assertEquals(100L + i, buf.tsAt(row));
            }
        }
    }

    /**
     * 4. Resize on growth: insert beyond initial capacity = 16. All entries retrievable in
     *    ascending-ts order after the buffer auto-grows.
     */
    @Test
    void resizeOnGrowth() {
        try (ArrowTimerBuffer buf = new ArrowTimerBuffer(16, 1024)) {
            int target = 200; // well past initial 16
            // Insert in reverse order so the heap actively re-sifts.
            for (int i = target - 1; i >= 0; i--) {
                scratch.set(ValueLayout.JAVA_INT, 0L, i);
                buf.insertAdd(scratch, 0L, 4, (long) i);
            }
            assertEquals(target, buf.size());
            // Drain root repeatedly — should yield 0..target-1 in order.
            for (int i = 0; i < target; i++) {
                assertEquals((long) i, buf.tsAt(0));
                buf.removeAt(0);
            }
            assertEquals(0, buf.size());
        }
    }
}
