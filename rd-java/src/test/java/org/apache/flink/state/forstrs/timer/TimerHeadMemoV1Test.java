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

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M5-V1 (hot-path alloc/copy audit 2026-06-12) focused regression tests for the segment-direct
 * head-memo peek/poll rewrite of {@link ForStRsKeyGroupedInternalPriorityQueue}: the memo-HIT
 * compare now runs against {@code liveIndex.keyDataSegment()} via {@code MemorySegment.mismatch}
 * (no composite alloc/copy), and {@code poll()} probes the pending buffer segment-direct BEFORE
 * {@code removeAt(0)} instead of copying the composite to heap and back into scratch.
 *
 * <p>These tests pin the EXACT observable semantics across memo hits/misses, head changes between
 * peeks, the pending-ADD-cancel branch, and the staged-delete branch after an engine flush. The
 * full suites ({@code ForStRsKeyGroupedInternalPriorityQueueTest},
 * {@code ...BatchedTest}, {@code MultiKeygroupTimerFireTest}) plus the Stage-0 lockstep rule
 * remain the broader gate for any timer-path edit.
 */
class TimerHeadMemoV1Test {

    private Arena arena;
    private ForStRsLinker linker;
    private FrsDb db;
    private FrsCfHandle cf;

    @BeforeEach
    void setUp() {
        arena = Arena.ofShared();
        linker = new ForStRsLinker(arena);
        db = linker.dbOpenMemory(arena);
        cf = linker.dbDefaultCf(db, arena);
    }

    @AfterEach
    void tearDown() {
        try {
            cf.close();
        } catch (Exception ignored) {
        }
        try {
            db.close();
        } catch (Exception ignored) {
        }
        arena.close();
    }

    private ForStRsKeyGroupedInternalPriorityQueue<
                    ForStRsKeyGroupedInternalPriorityQueueTest.TestElement>
            newQueue(String name) {
        return new ForStRsKeyGroupedInternalPriorityQueue<>(
                linker,
                db,
                cf,
                arena,
                name,
                ForStRsKeyGroupedInternalPriorityQueueTest.TestElementSerializer.INSTANCE,
                e -> e.ts,
                () -> 0,
                new KeyGroupRange(0, 0));
    }

    private static ForStRsKeyGroupedInternalPriorityQueueTest.TestElement te(long ts, int seq) {
        return new ForStRsKeyGroupedInternalPriorityQueueTest.TestElement(ts, seq);
    }

    /** Repeated peeks of the same head are memo HITs and must keep returning the head element. */
    @Test
    void repeatedPeekServesSameHead() {
        var q = newQueue("memo-peek");
        q.add(te(30L, 3));
        q.add(te(10L, 1));
        q.add(te(20L, 2));
        for (int i = 0; i < 100; i++) {
            var head = q.peek();
            assertEquals(te(10L, 1), head, "peek #" + i);
        }
        assertEquals(te(10L, 1), q.poll());
        assertEquals(te(20L, 2), q.peek());
    }

    /**
     * Head changes BETWEEN peeks (a smaller-ts add) — the memo must miss on the new head bytes
     * and re-decode; serving the stale memo element here would be the V1 regression.
     */
    @Test
    void peekTracksHeadChangeBetweenPeeks() {
        var q = newQueue("memo-headchange");
        q.add(te(50L, 5));
        assertEquals(te(50L, 5), q.peek());
        // New minimum arrives — head bytes at index position 0 change.
        q.add(te(5L, 1));
        assertEquals(te(5L, 1), q.peek(), "peek must re-decode after head change");
        // And poll order is exact.
        assertEquals(te(5L, 1), q.poll());
        assertEquals(te(50L, 5), q.poll());
        assertNull(q.poll());
    }

    /**
     * Poll of a never-flushed timer takes the pending-ADD-cancel branch (no composite alloc at
     * all post-V1): element is returned, the queue drains empty, and nothing resurrects.
     */
    @Test
    void pollCancelsUnflushedPendingAdd() {
        var q = newQueue("memo-cancel");
        q.add(te(7L, 1));
        assertEquals(te(7L, 1), q.poll());
        assertTrue(q.isEmpty(), "polled-before-flush timer must not survive");
        assertNull(q.peek());
        assertNull(q.poll());
    }

    /**
     * Flush-then-poll takes the staged-delete branch: after {@code flushPendingToEngine()} the
     * pending buffer no longer holds the ADDs, so each poll stages an owned composite on
     * pendingPollDeletes (post-V1: the memo array on memo HIT, the fresh decode copy on miss).
     * Exact drain order and emptiness pin the behavior; a second add/poll round verifies no
     * staged-delete resurrection or aliasing corruption.
     */
    @Test
    void pollAfterEngineFlushStagesDeletesExactly() {
        var q = newQueue("memo-staged-delete");
        final int n = 64;
        for (int i = n - 1; i >= 0; i--) {
            q.add(te(i, i));
        }
        q.flushPendingToEngine();

        for (int i = 0; i < n; i++) {
            // peek-then-poll per element: poll's memo compare is a HIT (peek just decoded the
            // same head) — exercises the memo-array reuse on the staging branch.
            assertEquals(te(i, i), q.peek(), "peek i=" + i);
            assertEquals(te(i, i), q.poll(), "poll i=" + i);
        }
        assertTrue(q.isEmpty());

        // Round 2: re-add a key that was just delete-staged; it must live again and drain once.
        q.add(te(3L, 3));
        q.flushPendingToEngine();
        assertEquals(te(3L, 3), q.poll());
        assertNull(q.poll());
        assertTrue(q.isEmpty());
    }
}
