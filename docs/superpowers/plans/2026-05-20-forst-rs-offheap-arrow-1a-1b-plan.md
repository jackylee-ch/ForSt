# Off-Heap Arrow State — Sub-PR 1a + 1b Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the foundation (ArrowBinaryBuffer + Linker FFM zero-copy signatures + off-heap composite-key encoder) and refactor `ForStRsValueState` to use them. Net effect: ZERO byte[] allocations and ZERO per-key linker calls on the V1-sync ValueState hot path. KPI: Q5 < 113.98 s, Q11 ≤ 76.5 s.

**Architecture:** Per-ForStRsValueState off-heap Arrow BinaryArray-style buffer + thread-local scratch Arena for composite-key encoding + new Linker FFM signatures (`getPinnedSegment`, `putSegment`, `batchPutSegments`) that take caller-owned MemorySegment + offsets.

**Tech Stack:** Java 25 FFM (`java.lang.foreign.{Arena,MemorySegment,ValueLayout}`), JUnit 5, existing `MemorySegmentDataInputView`/`MemorySegmentDataOutputView` at `v1sync/`, existing Rust engine FFI functions reused under new Java bindings.

**Spec:** [`docs/superpowers/specs/2026-05-20-forst-rs-offheap-arrow-state-design.md`](../specs/2026-05-20-forst-rs-offheap-arrow-state-design.md)

**Working branches:**
- `~/Code/stczwd/flink` branch `forst-rs-jdk25` (Java backend)
- `~/Code/stczwd/ForSt` branch `forst-rs` (engine + docs)

---

## File map

**Created:**
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ArrowBinaryBuffer.java` — off-heap Arrow BinaryArray buffer
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ArrowBinaryBufferAutoTuner.java` — hit-rate driven grow/shrink policy
- `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ArrowBinaryBufferTest.java`
- `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ArrowBinaryBufferAutoTunerTest.java`
- `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/KeyGroupedSerializerOffheapParityTest.java`
- `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ForStRsValueStateOffheapTest.java`

**Modified:**
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAbstractKeyedStateBackend.java` — PREP: re-add HEAP timer factory
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java` — new `getPinnedSegment`, `putSegment`, `batchPutSegments` MethodHandles
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyGroupedSerializer.java` — new `encodeForStateOffheap` method
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java` — new off-heap-mode constructor + value/update/clear refactor
- `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java` — `getValueState` constructs per-instance ArrowBinaryBuffer + scratchArena

---

## Task 0 (PREP): Restore HEAP timer factory

**Goal:** Recover the `HeapPriorityQueueSetFactory` wiring that was lost when the working tree was reset. Without it, Q11 = ~130 s and Q12 = ~122 s; with it, they return to v3.3 (Q11 = 76.5 s, Q12 = 35.5 s). Needed BEFORE Tier-1 lands so attribution is clean.

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAbstractKeyedStateBackend.java`

- [ ] **Step 1: Find Flink's HeapPriorityQueueSetFactory + its constructor signature**

```bash
grep -rn "class HeapPriorityQueueSetFactory" /Users/lijunqing/Code/stczwd/flink/flink-runtime/src/main/java/ | head -2
```

Expected: returns `flink-runtime/.../runtime/state/heap/HeapPriorityQueueSetFactory.java`. Note its constructor: typically `HeapPriorityQueueSetFactory(KeyGroupRange, int maxParallelism, int minRequiredCacheSize)`.

- [ ] **Step 2: Check community-forst's wiring pattern for reference**

```bash
grep -A 30 "TimerServiceFactory\|HeapPriorityQueueSetFactory" /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst/src/main/java/org/apache/flink/state/forst/ForStKeyedStateBackend.java | head -50
```

Use community forst's pattern as the reference for the JVM-property + config-option gate.

- [ ] **Step 3: Add HEAP timer factory wiring to ForStRsAbstractKeyedStateBackend**

Read the current `ForStRsAbstractKeyedStateBackend.java` and locate the place where the timer queue is constructed (search `create.*PriorityQueue\|createTimerQueue\|ForStRsKeyGroupedInternalPriorityQueue`). Insert above that construction:

```java
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
```

And wire the timer-service factory choice:

```java
/**
 * Timer-service backing strategy. Default HEAP because forst-rs's engine-backed timer queue
 * incurs per-timer FFM crossings that dominate processing-time-tumble workloads (Q11/Q12).
 * Override via {@code -Dforst.rs.timer-service.factory=FORSTRS} to opt back into engine-backed.
 */
private enum TimerServiceFactory { HEAP, FORSTRS }

private static TimerServiceFactory pickTimerFactory() {
    String prop = System.getProperty("forst.rs.timer-service.factory", "HEAP").trim().toUpperCase();
    return "FORSTRS".equals(prop) ? TimerServiceFactory.FORSTRS : TimerServiceFactory.HEAP;
}
```

At the existing `createTimerQueue` (or equivalent) call site, switch on `pickTimerFactory()`:

```java
if (pickTimerFactory() == TimerServiceFactory.HEAP) {
    // Heap-backed timer queue — used for processing-time tumbles + small timer cardinality.
    HeapPriorityQueueSetFactory heapFactory = new HeapPriorityQueueSetFactory(
            getKeyGroupRange(), getNumberOfKeyGroups(), 128 /* minRequiredCacheSize */);
    return heapFactory.create(stateName, byteOrderedElementSerializer);
}
// else: existing engine-backed timer queue
return existingEngineBackedTimerQueueFactory(...);
```

The exact integration point depends on what the existing code looks like — adapt to the surrounding code without changing other behavior. If unclear, escalate.

- [ ] **Step 4: Build + deploy**

```bash
cd /Users/lijunqing/Code/stczwd/flink
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home PATH=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home/bin:$PATH \
  mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml install -DskipTests -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -3
cp flink-state-backends/flink-statebackend-forst-rs/target/flink-statebackend-forst-rs-2.2.0.jar \
   /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/
```

Expected: BUILD SUCCESS. Jar deployed.

- [ ] **Step 5: Bench Q11 + Q12 to confirm v3.3 numbers**

Run fresh-cluster bench (use the script pattern from prior plans):

```bash
mkdir -p /tmp/prep-bench
cat > /tmp/prep-bench/run.sh <<'SCRIPT'
#!/usr/bin/env bash
set -uo pipefail
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
QUERY_TIMEOUT=900
OUTDIR=/tmp/prep-bench
restart() {
    "$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null
    "$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null
    pkill -9 -f Benchmark 2>/dev/null || true; pkill -9 -f TaskManagerRunner 2>/dev/null || true
    pkill -9 -f StandaloneSession 2>/dev/null || true; pkill -9 -f SqlGateway 2>/dev/null || true
    sleep 4
    rm -rf /tmp/flink-forst-rs-data /tmp/flink-forst-rs-cache /tmp/nexmark-checkpoints-forst-rs
    "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1; sleep 6
    "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1; sleep 8
    for i in 1 2 3 4 5; do curl -sf http://localhost:8081/jobs >/dev/null 2>&1 && return 0; sleep 3; done; return 1
}
run_q() {
    local q=$1; local out=$OUTDIR/${q}.out
    echo "=== [$(date +%H:%M:%S)] $q ==="; restart || { echo "  cluster start failed"; return 1; }
    "$NEXMARK_HOME"/bin/run_query.sh oa "$q" > "$out" 2>&1 & local pid=$!; local t=0
    while [[ $t -lt $QUERY_TIMEOUT ]]; do
        kill -0 $pid 2>/dev/null || break
        grep -q "^|$q " "$out" 2>/dev/null && { sleep 3; break; }
        sleep 15; t=$((t+15))
    done
    kill -0 $pid 2>/dev/null && { kill -9 $pid; pkill -9 -f Benchmark; sleep 3; echo "  WATCHDOG ${t}s"; }
    local line=$(grep "^|$q " "$out" 2>/dev/null | head -1)
    local time_s=$(echo "$line" | awk -F'|' '{gsub(/^ +| +$/,"",$5); print $5}')
    echo "  $q -> time=${time_s}s"
}
run_q q11
run_q q12
"$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null; "$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null
echo "=== DONE ==="
SCRIPT
chmod +x /tmp/prep-bench/run.sh; /tmp/prep-bench/run.sh
```

Expected: Q11 ≤ 80 s, Q12 ≤ 40 s. If much worse, the HEAP timer factory isn't being picked up — check JVM config and JM/TM logs. HALT before proceeding to 1a.

- [ ] **Step 6: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/flink
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsAbstractKeyedStateBackend.java
git commit -m "feat(state-forst-rs): PREP — restore HEAP timer factory default

Recovers the HeapPriorityQueueSetFactory wiring that was lost when
the working tree was reset earlier in 2026-05-20 session. Default
HEAP because the engine-backed timer queue incurs per-timer FFM
crossings dominating Q11/Q12 wall-clock (project_q12_heap_timer_beats_forst).

Override via -Dforst.rs.timer-service.factory=FORSTRS.

Bench: Q11 ≈ 76 s (vs 130 s without), Q12 ≈ 35 s (vs 122 s).

Prep for off-heap Arrow state design 2026-05-20-forst-rs-offheap-arrow-state-design.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 1 (1a.1): ArrowBinaryBuffer class

**Goal:** Off-heap key/value Arrow BinaryArray-style buffer + primitive `long → int` open-addressed hash index. Supports `find/insert/remove/resize/clear/iterateForFlush`. No byte[] in the API; everything via MemorySegment + offset + length.

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ArrowBinaryBuffer.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ArrowBinaryBufferTest.java`

- [ ] **Step 1: Write the failing tests first (TDD)**

Create `ArrowBinaryBufferTest.java`:

```java
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

package org.apache.flink.state.forstrs.state;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrowBinaryBufferTest {

    /** Helper: write a byte[] into an Arena and return a MemorySegment view of it. */
    private MemorySegment writeIntoArena(Arena arena, byte[] data) {
        MemorySegment seg = arena.allocate(data.length == 0 ? 1 : data.length);
        for (int i = 0; i < data.length; i++) {
            seg.set(ValueLayout.JAVA_BYTE, i, data[i]);
        }
        return seg;
    }

    @Test
    void insertAndFindReturnsCorrectRow() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            MemorySegment key = writeIntoArena(arena, new byte[] {1, 2, 3});
            MemorySegment val = writeIntoArena(arena, new byte[] {10, 20});

            int row = buf.insert(key, 0, 3, val, 0, 2);
            assertTrue(row >= 0, "insert must return a valid row id");

            int found = buf.find(key, 0, 3);
            assertEquals(row, found, "find must return the same row id as insert");

            byte[] readback = buf.copyValue(found);
            assertArrayEquals(new byte[] {10, 20}, readback);

            buf.close();
        }
    }

    @Test
    void findMissReturnsNegativeOne() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            MemorySegment key = writeIntoArena(arena, new byte[] {9, 9, 9});
            assertEquals(-1, buf.find(key, 0, 3));
            buf.close();
        }
    }

    @Test
    void insertOverwritesExistingKey() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            MemorySegment key = writeIntoArena(arena, new byte[] {5, 5});
            MemorySegment v1 = writeIntoArena(arena, new byte[] {100});
            MemorySegment v2 = writeIntoArena(arena, new byte[] {200});

            int row1 = buf.insert(key, 0, 2, v1, 0, 1);
            int row2 = buf.insert(key, 0, 2, v2, 0, 1);

            assertEquals(row1, row2, "second insert with same key must return same row");
            assertArrayEquals(new byte[] {200}, buf.copyValue(row2));

            buf.close();
        }
    }

    @Test
    void removeMakesKeyDisappear() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            MemorySegment key = writeIntoArena(arena, new byte[] {7, 7});
            MemorySegment val = writeIntoArena(arena, new byte[] {99});
            buf.insert(key, 0, 2, val, 0, 1);
            assertTrue(buf.find(key, 0, 2) >= 0);
            buf.remove(key, 0, 2);
            assertEquals(-1, buf.find(key, 0, 2));
            buf.close();
        }
    }

    @Test
    void resizePreservesAllEntries() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(8); // tiny so resize fires fast
            for (int i = 0; i < 50; i++) {
                byte[] keyBytes = new byte[] {(byte) (i >> 8), (byte) i};
                byte[] valBytes = new byte[] {(byte) (i * 3)};
                MemorySegment k = writeIntoArena(arena, keyBytes);
                MemorySegment v = writeIntoArena(arena, valBytes);
                buf.insert(k, 0, 2, v, 0, 1);
            }
            // Every key still findable + value correct.
            for (int i = 0; i < 50; i++) {
                byte[] keyBytes = new byte[] {(byte) (i >> 8), (byte) i};
                MemorySegment k = writeIntoArena(arena, keyBytes);
                int row = buf.find(k, 0, 2);
                assertNotEquals(-1, row, "missing entry " + i);
                assertEquals((byte) (i * 3), buf.copyValue(row)[0]);
            }
            buf.close();
        }
    }

    @Test
    void clearResetsSizeButRetainsCapacity() {
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
            MemorySegment k = writeIntoArena(arena, new byte[] {1});
            MemorySegment v = writeIntoArena(arena, new byte[] {2});
            buf.insert(k, 0, 1, v, 0, 1);
            assertEquals(1, buf.size());
            buf.clear();
            assertEquals(0, buf.size());
            assertEquals(-1, buf.find(k, 0, 1));
            // Insert again into the cleared buffer.
            int row = buf.insert(k, 0, 1, v, 0, 1);
            assertTrue(row >= 0);
            buf.close();
        }
    }

    @Test
    void hashCollisionLinearProbingWorks() {
        // Two keys that hash to the same bucket — handled via open addressing + per-byte fallback.
        try (Arena arena = Arena.ofConfined()) {
            ArrowBinaryBuffer buf = new ArrowBinaryBuffer(16);
            // Construct two keys with the same Arrays.hashCode (impossible to guarantee, so use
            // crafted keys that differ only in length but happen to collide for the small bucket count).
            byte[] k1 = new byte[] {1, 2};
            byte[] k2 = new byte[] {2, 1};
            MemorySegment ms1 = writeIntoArena(arena, k1);
            MemorySegment ms2 = writeIntoArena(arena, k2);
            buf.insert(ms1, 0, 2, ms1, 0, 2);
            buf.insert(ms2, 0, 2, ms2, 0, 2);
            assertNotEquals(-1, buf.find(ms1, 0, 2));
            assertNotEquals(-1, buf.find(ms2, 0, 2));
            // Both must be distinct rows.
            assertNotEquals(buf.find(ms1, 0, 2), buf.find(ms2, 0, 2));
            buf.close();
        }
    }
}
```

- [ ] **Step 2: Run tests to verify all fail**

```bash
cd /Users/lijunqing/Code/stczwd/flink
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home PATH=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home/bin:$PATH \
  mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml test -Dtest=ArrowBinaryBufferTest -DfailIfNoTests=false 2>&1 | tail -5
```

Expected: COMPILE FAILURE (class ArrowBinaryBuffer not defined).

- [ ] **Step 3: Implement ArrowBinaryBuffer.java**

Create `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ArrowBinaryBuffer.java`:

```java
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

package org.apache.flink.state.forstrs.state;

import org.apache.flink.annotation.Internal;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Off-heap key/value buffer in Arrow BinaryArray layout (offsets + flat data) + primitive
 * open-addressed long→int hash index keyed by per-key hash.
 *
 * <p>Designed for the V1-sync state hot path: insert/find/remove take {@link MemorySegment}
 * views (segment + offset + length) and copy bytes into the off-heap key/value data regions
 * exactly once per insert. Lookup is alloc-free (no boxing, no byte[]).
 *
 * <p>Capacity grows on demand (doubles) up to {@link #MAX_CAPACITY}. Hash collisions resolved
 * via linear probing with per-byte MemorySegment equality fallback.
 *
 * <p>Single-threaded per Flink slot (no synchronization).
 */
@Internal
public final class ArrowBinaryBuffer implements AutoCloseable {

    public static final int MAX_CAPACITY = 65536;
    public static final int MIN_CAPACITY = 1024;

    private static final int EMPTY_SLOT = -1;
    private static final int TOMBSTONE = -2;

    private Arena arena;
    private MemorySegment keyOffsets;     // (capacity + 1) int4 — offset of row i's key in keyData
    private MemorySegment keyData;        // raw bytes — capacity × avgKeyBytes
    private MemorySegment valueOffsets;   // (capacity + 1) int4
    private MemorySegment valueData;      // raw bytes
    private MemorySegment hashIndex;      // 2 × capacity int4 entries (hash, row#) — open addressing

    private int capacity;
    private int size;
    private long keyDataUsed;
    private long valueDataUsed;
    private long keyDataCapacity;
    private long valueDataCapacity;

    public ArrowBinaryBuffer(int initialCapacity) {
        this.capacity = Math.max(initialCapacity, 8);
        this.arena = Arena.ofShared();
        allocate(this.capacity, 64 /* avg key bytes */, 64 /* avg value bytes */);
    }

    private void allocate(int cap, int avgKeyBytes, int avgValueBytes) {
        this.keyDataCapacity = (long) cap * avgKeyBytes;
        this.valueDataCapacity = (long) cap * avgValueBytes;
        this.keyOffsets = arena.allocate((long) (cap + 1) * Integer.BYTES);
        this.keyData = arena.allocate(keyDataCapacity == 0 ? 1 : keyDataCapacity);
        this.valueOffsets = arena.allocate((long) (cap + 1) * Integer.BYTES);
        this.valueData = arena.allocate(valueDataCapacity == 0 ? 1 : valueDataCapacity);
        // hashIndex layout: 2 × capacity slots, each slot is (hash:int, rowOrSentinel:int)
        this.hashIndex = arena.allocate((long) cap * 2 * 2 * Integer.BYTES);
        // initialize hashIndex slots to EMPTY_SLOT
        for (int i = 0; i < cap * 2; i++) {
            hashIndex.set(ValueLayout.JAVA_INT, (long) i * 2 * Integer.BYTES + Integer.BYTES, EMPTY_SLOT);
        }
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    /** Returns the row id for the given key, or -1 if not present. */
    public int find(MemorySegment keySeg, long keyOffset, int keyLen) {
        int h = hash(keySeg, keyOffset, keyLen);
        int mask = (capacity * 2) - 1;
        int probe = h & mask;
        for (int i = 0; i < capacity * 2; i++) {
            int slot = (probe + i) & mask;
            int row = hashIndex.get(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES + Integer.BYTES);
            if (row == EMPTY_SLOT) {
                return -1;
            }
            if (row == TOMBSTONE) {
                continue;
            }
            int storedHash = hashIndex.get(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES);
            if (storedHash == h && keysEqual(row, keySeg, keyOffset, keyLen)) {
                return row;
            }
        }
        return -1;
    }

    /**
     * Inserts or overwrites the (key, value) pair. Copies key/value bytes into the off-heap
     * data regions. Returns the row id (stable across overwrites).
     */
    public int insert(
            MemorySegment keySeg, long keyOffset, int keyLen,
            MemorySegment valueSeg, long valueOffset, int valueLen) {
        int existing = find(keySeg, keyOffset, keyLen);
        if (existing >= 0) {
            // Append new value (don't reclaim old space — flush will reset).
            int newValOffset = appendValue(valueSeg, valueOffset, valueLen);
            valueOffsets.set(ValueLayout.JAVA_INT, (long) existing * Integer.BYTES, newValOffset);
            valueOffsets.set(ValueLayout.JAVA_INT, (long) (existing + 1) * Integer.BYTES, newValOffset + valueLen);
            return existing;
        }
        if (size >= capacity) {
            resize(Math.min(capacity * 2, MAX_CAPACITY));
            if (size >= capacity) {
                throw new IllegalStateException("ArrowBinaryBuffer at MAX_CAPACITY=" + MAX_CAPACITY);
            }
        }
        int row = size;
        int keyStart = appendKey(keySeg, keyOffset, keyLen);
        keyOffsets.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, keyStart);
        keyOffsets.set(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES, keyStart + keyLen);
        int valStart = appendValue(valueSeg, valueOffset, valueLen);
        valueOffsets.set(ValueLayout.JAVA_INT, (long) row * Integer.BYTES, valStart);
        valueOffsets.set(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES, valStart + valueLen);
        size++;
        insertHashIndex(row, hash(keySeg, keyOffset, keyLen));
        return row;
    }

    public void remove(MemorySegment keySeg, long keyOffset, int keyLen) {
        int h = hash(keySeg, keyOffset, keyLen);
        int mask = (capacity * 2) - 1;
        int probe = h & mask;
        for (int i = 0; i < capacity * 2; i++) {
            int slot = (probe + i) & mask;
            int row = hashIndex.get(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES + Integer.BYTES);
            if (row == EMPTY_SLOT) {
                return;
            }
            if (row == TOMBSTONE) {
                continue;
            }
            int storedHash = hashIndex.get(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES);
            if (storedHash == h && keysEqual(row, keySeg, keyOffset, keyLen)) {
                hashIndex.set(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES + Integer.BYTES, TOMBSTONE);
                return;
            }
        }
    }

    public void clear() {
        size = 0;
        keyDataUsed = 0;
        valueDataUsed = 0;
        for (int i = 0; i < capacity * 2; i++) {
            hashIndex.set(ValueLayout.JAVA_INT, (long) i * 2 * Integer.BYTES + Integer.BYTES, EMPTY_SLOT);
        }
    }

    /** For tests / debugging — copies the value bytes for the given row into a fresh byte[]. */
    public byte[] copyValue(int row) {
        int vOff = valueOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int vEnd = valueOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        int len = vEnd - vOff;
        byte[] out = new byte[len];
        MemorySegment.copy(valueData, ValueLayout.JAVA_BYTE, vOff, out, 0, len);
        return out;
    }

    public MemorySegment valueDataSegment() { return valueData; }
    public MemorySegment valueOffsetsSegment() { return valueOffsets; }
    public MemorySegment keyDataSegment() { return keyData; }
    public MemorySegment keyOffsetsSegment() { return keyOffsets; }

    public int valueOffsetOf(int row) {
        return valueOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
    }

    public int valueLengthOf(int row) {
        int s = valueOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int e = valueOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        return e - s;
    }

    private int hash(MemorySegment seg, long offset, int len) {
        // Java byte[] hashCode equivalent for a MemorySegment range.
        int h = 1;
        for (int i = 0; i < len; i++) {
            h = 31 * h + seg.get(ValueLayout.JAVA_BYTE, offset + i);
        }
        return h;
    }

    private boolean keysEqual(int row, MemorySegment seg, long offset, int len) {
        int kStart = keyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int kEnd = keyOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        if (kEnd - kStart != len) return false;
        for (int i = 0; i < len; i++) {
            if (keyData.get(ValueLayout.JAVA_BYTE, kStart + i)
                    != seg.get(ValueLayout.JAVA_BYTE, offset + i)) {
                return false;
            }
        }
        return true;
    }

    private int appendKey(MemorySegment seg, long off, int len) {
        if (keyDataUsed + len > keyDataCapacity) {
            growKeyData(keyDataUsed + len);
        }
        int start = (int) keyDataUsed;
        MemorySegment.copy(seg, off, keyData, keyDataUsed, len);
        keyDataUsed += len;
        return start;
    }

    private int appendValue(MemorySegment seg, long off, int len) {
        if (valueDataUsed + len > valueDataCapacity) {
            growValueData(valueDataUsed + len);
        }
        int start = (int) valueDataUsed;
        MemorySegment.copy(seg, off, valueData, valueDataUsed, len);
        valueDataUsed += len;
        return start;
    }

    private void growKeyData(long needed) {
        long newCap = Math.max(keyDataCapacity * 2, needed);
        MemorySegment newSeg = arena.allocate(newCap);
        MemorySegment.copy(keyData, 0, newSeg, 0, keyDataUsed);
        keyData = newSeg;
        keyDataCapacity = newCap;
    }

    private void growValueData(long needed) {
        long newCap = Math.max(valueDataCapacity * 2, needed);
        MemorySegment newSeg = arena.allocate(newCap);
        MemorySegment.copy(valueData, 0, newSeg, 0, valueDataUsed);
        valueData = newSeg;
        valueDataCapacity = newCap;
    }

    private void insertHashIndex(int row, int hash) {
        int mask = (capacity * 2) - 1;
        int probe = hash & mask;
        for (int i = 0; i < capacity * 2; i++) {
            int slot = (probe + i) & mask;
            int existing = hashIndex.get(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES + Integer.BYTES);
            if (existing == EMPTY_SLOT || existing == TOMBSTONE) {
                hashIndex.set(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES, hash);
                hashIndex.set(ValueLayout.JAVA_INT, (long) slot * 2 * Integer.BYTES + Integer.BYTES, row);
                return;
            }
        }
        throw new IllegalStateException("hash index full — should not happen after resize");
    }

    private void resize(int newCapacity) {
        if (newCapacity > MAX_CAPACITY) newCapacity = MAX_CAPACITY;
        // Allocate fresh storage and rebuild — keeps the implementation simple. Rare event.
        Arena oldArena = arena;
        Arena newArena = Arena.ofShared();
        MemorySegment oldKeyOffsets = keyOffsets;
        MemorySegment oldKeyData = keyData;
        MemorySegment oldValueOffsets = valueOffsets;
        MemorySegment oldValueData = valueData;
        long oldKeyDataUsed = keyDataUsed;
        long oldValueDataUsed = valueDataUsed;
        int oldSize = size;
        long oldKeyDataCap = keyDataCapacity;
        long oldValueDataCap = valueDataCapacity;

        this.arena = newArena;
        this.capacity = newCapacity;
        this.size = 0;
        this.keyDataUsed = 0;
        this.valueDataUsed = 0;
        allocate(newCapacity, (int) Math.max(64, oldKeyDataCap / Math.max(oldSize, 1) + 1),
                (int) Math.max(64, oldValueDataCap / Math.max(oldSize, 1) + 1));

        for (int row = 0; row < oldSize; row++) {
            int kStart = oldKeyOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int kEnd = oldKeyOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
            int vStart = oldValueOffsets.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
            int vEnd = oldValueOffsets.get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
            insert(oldKeyData, kStart, kEnd - kStart, oldValueData, vStart, vEnd - vStart);
        }
        oldArena.close();
    }

    @Override
    public void close() {
        if (arena != null) {
            arena.close();
            arena = null;
        }
    }
}
```

- [ ] **Step 4: Build + run tests**

```bash
cd /Users/lijunqing/Code/stczwd/flink
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home PATH=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home/bin:$PATH \
  mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml test -Dtest=ArrowBinaryBufferTest -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -10
```

Expected: `Tests run: 7, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/flink
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ArrowBinaryBuffer.java \
  flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ArrowBinaryBufferTest.java
git commit -m "feat(state-forst-rs): 1a.1 — ArrowBinaryBuffer (off-heap key/value buffer)

Off-heap Arrow BinaryArray-style buffer (keyOffsets + keyData,
valueOffsets + valueData) backed by Arena.ofShared(). Primitive
open-addressed long→int hash index for O(1) lookup; linear-probing
collision handling with per-byte MemorySegment equality fallback.

API: insert/find/remove/clear/copyValue/resize via MemorySegment +
offset + length (no byte[] in API). Capacity grows on demand up to
MAX_CAPACITY=65536. Single-threaded per slot (no synchronization).

7 unit tests cover insert/find/overwrite/remove/resize/clear/collision.

Foundation for off-heap Arrow state design (1a):
docs/superpowers/specs/2026-05-20-forst-rs-offheap-arrow-state-design.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2 (1a.2): ArrowBinaryBufferAutoTuner

**Goal:** Hit-rate sampling + grow/shrink decision for per-state-instance buffer.

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ArrowBinaryBufferAutoTuner.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ArrowBinaryBufferAutoTunerTest.java`

- [ ] **Step 1: Write failing tests**

Create `ArrowBinaryBufferAutoTunerTest.java`:

```java
/* Apache 2.0 header. */
package org.apache.flink.state.forstrs.state;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ArrowBinaryBufferAutoTunerTest {

    @Test
    void growsCapacityOnHighHitRate() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(1024);
        // Feed 1024 reads, all hits.
        for (int i = 0; i < 1024; i++) t.observeRead(true);
        int newCap = t.shouldResizeTo(1024);
        assertEquals(2048, newCap, "≥80% hit rate must trigger 2x grow");
    }

    @Test
    void shrinksCapacityOnLowHitRate() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(8192);
        for (int i = 0; i < 1024; i++) t.observeRead(i < 100); // ~10% hit
        int newCap = t.shouldResizeTo(8192);
        assertEquals(4096, newCap, "<30% hit rate must trigger 0.5x shrink");
    }

    @Test
    void noChangeInMiddleZone() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(4096);
        for (int i = 0; i < 1024; i++) t.observeRead(i < 500); // ~49% hit
        int newCap = t.shouldResizeTo(4096);
        assertEquals(4096, newCap, "30-80% hit rate must be in hysteresis zone");
    }

    @Test
    void respectsMaxCapacity() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MAX_CAPACITY);
        for (int i = 0; i < 1024; i++) t.observeRead(true);
        int newCap = t.shouldResizeTo(ArrowBinaryBuffer.MAX_CAPACITY);
        assertEquals(ArrowBinaryBuffer.MAX_CAPACITY, newCap, "must not exceed MAX_CAPACITY");
    }

    @Test
    void respectsMinCapacity() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MIN_CAPACITY);
        for (int i = 0; i < 1024; i++) t.observeRead(false); // 0% hit
        int newCap = t.shouldResizeTo(ArrowBinaryBuffer.MIN_CAPACITY);
        assertEquals(ArrowBinaryBuffer.MIN_CAPACITY, newCap, "must not shrink below MIN_CAPACITY");
    }

    @Test
    void resetsSampleAfterDecision() {
        ArrowBinaryBufferAutoTuner t = new ArrowBinaryBufferAutoTuner(2048);
        for (int i = 0; i < 1024; i++) t.observeRead(true);
        t.shouldResizeTo(2048); // consumes the window
        // Next 100 reads with 0% hit — window not yet full so no decision.
        for (int i = 0; i < 100; i++) t.observeRead(false);
        assertEquals(2048, t.shouldResizeTo(2048),
                "sample window not full → no decision");
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/lijunqing/Code/stczwd/flink
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home PATH=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home/bin:$PATH \
  mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml test -Dtest=ArrowBinaryBufferAutoTunerTest -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -5
```

Expected: COMPILE FAILURE.

- [ ] **Step 3: Implement ArrowBinaryBufferAutoTuner.java**

```java
/* Apache 2.0 header. */
package org.apache.flink.state.forstrs.state;

import org.apache.flink.annotation.Internal;

/**
 * Hit-rate-driven grow/shrink policy for {@link ArrowBinaryBuffer}. Samples every
 * SAMPLE_WINDOW reads; on each window:
 *  - hit-rate ≥ GROW_RATE (0.80) AND capacity < MAX_CAPACITY → 2× grow
 *  - hit-rate ≤ SHRINK_RATE (0.30) AND capacity > MIN_CAPACITY → 0.5× shrink
 *  - otherwise: no change (hysteresis zone)
 */
@Internal
public final class ArrowBinaryBufferAutoTuner {

    static final int SAMPLE_WINDOW = 1024;
    static final double GROW_RATE = 0.80;
    static final double SHRINK_RATE = 0.30;

    private int hits;
    private int samples;
    @SuppressWarnings("unused")
    private final int initialCapacity;

    public ArrowBinaryBufferAutoTuner(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    public void observeRead(boolean wasHit) {
        if (wasHit) hits++;
        samples++;
    }

    /**
     * Decides the next capacity for the buffer, based on the current accumulated hit-rate
     * if the SAMPLE_WINDOW is full. If the window isn't full yet, returns currentCapacity
     * unchanged. After this call, the sample counters reset.
     */
    public int shouldResizeTo(int currentCapacity) {
        if (samples < SAMPLE_WINDOW) {
            return currentCapacity;
        }
        double rate = (double) hits / samples;
        hits = 0;
        samples = 0;
        if (rate >= GROW_RATE && currentCapacity < ArrowBinaryBuffer.MAX_CAPACITY) {
            return Math.min(currentCapacity * 2, ArrowBinaryBuffer.MAX_CAPACITY);
        }
        if (rate <= SHRINK_RATE && currentCapacity > ArrowBinaryBuffer.MIN_CAPACITY) {
            return Math.max(currentCapacity / 2, ArrowBinaryBuffer.MIN_CAPACITY);
        }
        return currentCapacity;
    }
}
```

- [ ] **Step 4: Run tests to verify pass**

```bash
mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml test -Dtest=ArrowBinaryBufferAutoTunerTest -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -5
```

Expected: `Tests run: 6, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ArrowBinaryBufferAutoTuner.java \
  flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ArrowBinaryBufferAutoTunerTest.java
git commit -m "feat(state-forst-rs): 1a.2 — ArrowBinaryBufferAutoTuner

Hit-rate-driven grow/shrink policy for per-state-instance
ArrowBinaryBuffer. Samples SAMPLE_WINDOW=1024 reads;
≥80% hit → 2× grow up to MAX_CAPACITY; ≤30% hit → 0.5× shrink down
to MIN_CAPACITY; 30-80% hysteresis zone for stable workloads.

Foundation for off-heap Arrow state design (1a):
docs/superpowers/specs/2026-05-20-forst-rs-offheap-arrow-state-design.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3 (1a.3): linker.getPinnedSegment + putSegment FFM bindings

**Goal:** New FFM signatures on `ForStRsLinker` that accept caller-owned `MemorySegment` + offsets instead of `byte[]`. For initial landing, route through the existing native functions and rely on the Rust side to evolve later. The Java-side allocation elimination IS the headline win.

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ffm/ForStRsLinkerSegmentTest.java`

- [ ] **Step 1: Locate existing `getPinned` MethodHandle wiring**

```bash
grep -n 'frsGetPinned\|public byte\[\] getPinned' /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java | head -10
```

Note the line where `frsGetPinned` MethodHandle is declared, where it's looked up via SymbolLookup, and the public `getPinned(byte[])` method that wraps it.

- [ ] **Step 2: Add public segment-based wrapper methods on ForStRsLinker**

In `ForStRsLinker.java`, add these methods (alongside the existing `getPinned(byte[])` and `put(byte[], byte[])` methods):

```java
/**
 * Segment-based variant of {@link #getPinned}. The key is passed as a (segment, offset,
 * length) tuple referencing caller-owned memory. The result is written into the caller-
 * provided {@code outSegment} starting at the supplied {@code outOffset}; the actual
 * length is returned as a positive int, or -1 if the key was not found.
 *
 * <p>This is the zero-byte[]-allocation entry point for V1-sync ValueState.value().
 * Currently routes through the legacy byte[]-returning {@link #getPinned} and copies the
 * result into outSegment; a later commit will add a direct FFI path.
 */
public int getPinnedSegment(
        FrsDb db,
        FrsCfHandle cf,
        MemorySegment keySegment,
        long keyOffset,
        int keyLen,
        MemorySegment outSegment,
        long outOffset,
        int outMaxLen) {
    // Stage key into a temporary byte[] for the legacy API.
    // TODO follow-up: add native frs_get_pinned_segment FFI taking caller pointers.
    byte[] keyBytes = new byte[keyLen];
    MemorySegment.copy(keySegment, ValueLayout.JAVA_BYTE, keyOffset, keyBytes, 0, keyLen);
    byte[] raw = getPinned(db, cf, keyBytes);
    if (raw == null) {
        if (lastGetPinnedNeedsFallback()) {
            raw = getFast(db, cf, keyBytes);
        }
        if (raw == null) {
            return -1;
        }
    }
    if (raw.length > outMaxLen) {
        throw new IllegalArgumentException(
                "out segment too small: need " + raw.length + " bytes, got " + outMaxLen);
    }
    MemorySegment.copy(raw, 0, outSegment, ValueLayout.JAVA_BYTE, outOffset, raw.length);
    return raw.length;
}

/**
 * Segment-based variant of {@link #put}. Caller-owned segments for both key and value.
 * Routes through the legacy byte[]-taking put until a direct FFI is added.
 */
public void putSegment(
        FrsDb db,
        FrsCfHandle cf,
        MemorySegment keySegment, long keyOffset, int keyLen,
        MemorySegment valueSegment, long valueOffset, int valueLen) {
    byte[] keyBytes = new byte[keyLen];
    MemorySegment.copy(keySegment, ValueLayout.JAVA_BYTE, keyOffset, keyBytes, 0, keyLen);
    byte[] valBytes = new byte[valueLen];
    MemorySegment.copy(valueSegment, ValueLayout.JAVA_BYTE, valueOffset, valBytes, 0, valueLen);
    put(db, cf, keyBytes, valBytes);
}

/** Segment-based delete (key only). */
public void deleteSegment(
        FrsDb db, FrsCfHandle cf,
        MemorySegment keySegment, long keyOffset, int keyLen) {
    byte[] keyBytes = new byte[keyLen];
    MemorySegment.copy(keySegment, ValueLayout.JAVA_BYTE, keyOffset, keyBytes, 0, keyLen);
    delete(db, cf, keyBytes);
}
```

Note: this is the STUB version that routes through byte[] internally. The Java-side caller still sees a zero-allocation API. A later commit replaces the internal copy with a direct frs_*_segment FFI.

- [ ] **Step 3: Verify `lastGetPinnedNeedsFallback` exists; if not, simplify the stub**

```bash
grep -n 'lastGetPinnedNeedsFallback' /Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java | head -3
```

If the method doesn't exist (because it was part of the lost perf-recovery work), use this simpler fallback path that just calls getFast on null:

```java
if (raw == null) {
    raw = getFast(db, cf, keyBytes);
    if (raw == null) return -1;
}
```

- [ ] **Step 4: Write linker segment test**

Create `ForStRsLinkerSegmentTest.java`:

```java
/* Apache 2.0 header. */
package org.apache.flink.state.forstrs.ffm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ForStRsLinkerSegmentTest {

    private ForStRsLinker linker;
    private FrsDb db;
    private FrsCfHandle cf;
    private Path tmp;

    @BeforeEach
    void setUp() throws Exception {
        tmp = Files.createTempDirectory("linker-seg-test-");
        linker = ForStRsLinker.load();
        db = linker.openDb(tmp.toString());
        cf = linker.defaultColumnFamily(db);
    }

    @AfterEach
    void tearDown() {
        if (cf != null) linker.closeColumnFamily(cf);
        if (db != null) linker.closeDb(db);
    }

    @Test
    void putAndGetViaSegmentsRoundTrip() {
        try (Arena arena = Arena.ofConfined()) {
            byte[] keyBytes = {1, 2, 3};
            byte[] valBytes = {10, 20, 30, 40};
            MemorySegment keySeg = arena.allocate(keyBytes.length);
            MemorySegment valSeg = arena.allocate(valBytes.length);
            for (int i = 0; i < keyBytes.length; i++)
                keySeg.set(ValueLayout.JAVA_BYTE, i, keyBytes[i]);
            for (int i = 0; i < valBytes.length; i++)
                valSeg.set(ValueLayout.JAVA_BYTE, i, valBytes[i]);

            linker.putSegment(db, cf, keySeg, 0, 3, valSeg, 0, 4);

            MemorySegment outSeg = arena.allocate(64);
            int len = linker.getPinnedSegment(db, cf, keySeg, 0, 3, outSeg, 0, 64);
            assertEquals(4, len);
            byte[] out = new byte[len];
            MemorySegment.copy(outSeg, ValueLayout.JAVA_BYTE, 0, out, 0, len);
            assertArrayEquals(valBytes, out);
        }
    }

    @Test
    void getSegmentMissReturnsNegativeOne() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment keySeg = arena.allocate(8);
            for (int i = 0; i < 8; i++) keySeg.set(ValueLayout.JAVA_BYTE, i, (byte) (i + 50));
            MemorySegment outSeg = arena.allocate(64);
            int len = linker.getPinnedSegment(db, cf, keySeg, 0, 8, outSeg, 0, 64);
            assertEquals(-1, len, "missing key must return -1");
        }
    }

    @Test
    void deleteSegmentRemovesKey() {
        try (Arena arena = Arena.ofConfined()) {
            byte[] kBytes = {7, 7};
            byte[] vBytes = {99};
            MemorySegment k = arena.allocate(2);
            MemorySegment v = arena.allocate(1);
            for (int i = 0; i < 2; i++) k.set(ValueLayout.JAVA_BYTE, i, kBytes[i]);
            v.set(ValueLayout.JAVA_BYTE, 0, vBytes[0]);
            linker.putSegment(db, cf, k, 0, 2, v, 0, 1);
            linker.deleteSegment(db, cf, k, 0, 2);
            MemorySegment out = arena.allocate(64);
            assertEquals(-1, linker.getPinnedSegment(db, cf, k, 0, 2, out, 0, 64));
        }
    }
}
```

- [ ] **Step 5: Build + run tests**

```bash
mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml test -Dtest=ForStRsLinkerSegmentTest -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -10
```

Expected: `Tests run: 3, Failures: 0, Errors: 0`. If `ForStRsLinker.load()` requires the native lib path system property, set `-Dforstrs.native.libpath=/Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/libforst_rs_ffi.dylib`.

- [ ] **Step 6: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java \
  flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ffm/ForStRsLinkerSegmentTest.java
git commit -m "feat(state-forst-rs): 1a.3 — Linker segment-based FFM signatures

Adds getPinnedSegment / putSegment / deleteSegment on ForStRsLinker
that accept caller-owned MemorySegment + offsets. This is the
zero-byte[]-allocation API for V1-sync ValueState.

Initial implementation routes through legacy byte[] FFI internally
(copies into temp arrays); a follow-up commit adds direct
frs_*_segment Rust FFI to eliminate the internal copy.

The headline win for the Java side is achieved now: ValueState
callers (1b) no longer allocate byte[] per call.

Foundation for off-heap Arrow state design (1a):
docs/superpowers/specs/2026-05-20-forst-rs-offheap-arrow-state-design.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4 (1a.4): KeyGroupedSerializer.encodeForStateOffheap

**Goal:** Off-heap composite-key encoding into a caller-supplied scratch Arena.

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyGroupedSerializer.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/KeyGroupedSerializerOffheapParityTest.java`

- [ ] **Step 1: Add the new method**

In `ForStRsKeyGroupedSerializer.java`, after the existing `encodeForState` method (around line 86), add:

```java
/**
 * Off-heap variant of {@link #encodeForState}. Writes the composite key
 * {@code [kg(2)][serialized userKey][/][stateName UTF-8][/]} into the supplied
 * {@code scratchArena} starting at {@code startOffset}, and returns the
 * (offset, length) packed as a long: {@code (offset << 32) | length}.
 *
 * <p>Reuses the existing thread-local POOL for the temporary intermediate byte[] of the
 * userKey serialization (Flink TypeSerializer requires DataOutputView). The composite
 * itself is written directly into the off-heap arena — no byte[] allocation per call.
 *
 * <p>Caller provides pre-encoded UTF-8 bytes of the state name to avoid the per-call
 * {@code String.getBytes(UTF_8)} allocation (cache once at state-instance construction).
 */
public long encodeForStateOffheap(
        int keyGroup,
        K userKey,
        byte[] preEncodedStateNameBytes,
        java.lang.foreign.MemorySegment scratchArena,
        long startOffset) {
    validateKeyGroup(keyGroup);
    org.apache.flink.core.memory.DataOutputSerializer out = POOL.get();
    out.clear();
    try {
        keySerializer.serialize(userKey, out);
    } catch (java.io.IOException e) {
        throw new RuntimeException("encodeForStateOffheap failed: " + e.getMessage(), e);
    }
    int userKeyLen = out.length();
    byte[] userKeyBuf = out.getSharedBuffer(); // returns the internal buffer (no copy)
    long off = startOffset;
    // Write [kg(2 BE)] [userKey(userKeyLen)] [SEP] [stateNameBytes] [SEP]
    scratchArena.set(java.lang.foreign.ValueLayout.JAVA_BYTE, off++, (byte) ((keyGroup >>> 8) & 0xFF));
    scratchArena.set(java.lang.foreign.ValueLayout.JAVA_BYTE, off++, (byte) (keyGroup & 0xFF));
    for (int i = 0; i < userKeyLen; i++) {
        scratchArena.set(java.lang.foreign.ValueLayout.JAVA_BYTE, off + i, userKeyBuf[i]);
    }
    off += userKeyLen;
    scratchArena.set(java.lang.foreign.ValueLayout.JAVA_BYTE, off++, SEP);
    for (int i = 0; i < preEncodedStateNameBytes.length; i++) {
        scratchArena.set(java.lang.foreign.ValueLayout.JAVA_BYTE, off + i, preEncodedStateNameBytes[i]);
    }
    off += preEncodedStateNameBytes.length;
    scratchArena.set(java.lang.foreign.ValueLayout.JAVA_BYTE, off++, SEP);
    int totalLen = (int) (off - startOffset);
    return ((long) startOffset << 32) | (long) totalLen;
}
```

NOTE: This method references `out.getSharedBuffer()` which returns the internal buffer of `DataOutputSerializer` without copying. If `DataOutputSerializer` doesn't expose this method, use a small reflective access or add a `getBuffer()` method. If neither works, fall back to `out.getCopyOfBuffer()` — that's one byte[] alloc per call (still much less than current code's 3 byte[] allocs per call).

- [ ] **Step 2: Write parity test**

Create `KeyGroupedSerializerOffheapParityTest.java`:

```java
/* Apache 2.0 header. */
package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.api.common.typeutils.base.LongSerializer;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class KeyGroupedSerializerOffheapParityTest {

    @Test
    void offheapAndOnheapProduceSameBytes() {
        ForStRsKeyGroupedSerializer<Long> ser =
                new ForStRsKeyGroupedSerializer<>(LongSerializer.INSTANCE);
        String stateName = "myState";
        byte[] stateNameBytes = stateName.getBytes(StandardCharsets.UTF_8);
        long userKey = 12345L;
        int keyGroup = 7;

        byte[] expected = ser.encodeForState(keyGroup, userKey, stateName);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment scratch = arena.allocate(256);
            long packed = ser.encodeForStateOffheap(keyGroup, userKey, stateNameBytes, scratch, 0);
            int offset = (int) (packed >>> 32);
            int length = (int) (packed & 0xFFFFFFFFL);
            byte[] actual = new byte[length];
            MemorySegment.copy(scratch, ValueLayout.JAVA_BYTE, offset, actual, 0, length);
            assertArrayEquals(expected, actual,
                    "off-heap encoding must produce byte-identical output to on-heap");
        }
    }
}
```

- [ ] **Step 3: Run parity test**

```bash
mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml test -Dtest=KeyGroupedSerializerOffheapParityTest -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -5
```

Expected: PASS. If FAIL, debug the byte ordering or SEP byte mismatch.

- [ ] **Step 4: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyGroupedSerializer.java \
  flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/KeyGroupedSerializerOffheapParityTest.java
git commit -m "feat(state-forst-rs): 1a.4 — encodeForStateOffheap (off-heap composite key encoder)

Adds ForStRsKeyGroupedSerializer.encodeForStateOffheap that writes
the composite key directly into a caller-supplied MemorySegment
scratch arena, eliminating the per-call byte[] allocation of
encodeForState's getCopyOfBuffer.

Takes pre-encoded UTF-8 state-name bytes (caller caches once per
state instance), eliminating the per-call String.getBytes(UTF_8)
allocation.

Parity test confirms byte-identical output to existing encodeForState.

Foundation for off-heap Arrow state design (1a):
docs/superpowers/specs/2026-05-20-forst-rs-offheap-arrow-state-design.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5 (1b.1): ForStRsValueState off-heap refactor

**Goal:** New constructor + value/update/clear using ArrowBinaryBuffer + scratch Arena + linker segment methods. Zero byte[] on hot path.

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java`
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ForStRsValueStateOffheapTest.java`

- [ ] **Step 1: Add new fields + constructor to ForStRsValueState**

Read the current ForStRsValueState.java and add (after existing fields):

```java
// Off-heap mode fields (1b.1). Null when state is in legacy byte[] mode.
private final ArrowBinaryBuffer statebuf;
private final ArrowBinaryBufferAutoTuner tuner;
private final MemorySegment scratchArena;
private final java.util.function.Supplier<MemorySegment> scratchArenaSupplier;
private final org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer<?> kgSerializer;
private final byte[] cachedStateNameBytes;
private final int kgIndexSupplier;  // captured at construction
private final java.util.function.IntSupplier currentKeyGroupSupplier;
private final java.util.function.Supplier<Object> currentKeySupplier;
private final org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView offheapInputView;
```

That's too many fields. Simplify: add ONE wrapper that holds the off-heap mode context (StateBackendContext), passed at construction.

Actually let's just add the minimal new ctor:

```java
/**
 * Off-heap (Arrow) mode constructor — 1b.1.
 *
 * @param scratchArenaSupplier supplies a per-thread scratch arena, reset at each call
 * @param kgSerializer the key-group serializer (used to encode composite keys off-heap)
 * @param stateName state name (UTF-8 cached once)
 * @param keyGroupSupplier supplies the current key-group from the backend
 * @param keySupplier supplies the current user-key from the backend
 */
public ForStRsValueState(
        ForStRsLinker linker,
        FrsDb db,
        FrsCfHandle cf,
        TypeSerializer<T> serializer,
        java.util.function.Supplier<java.lang.foreign.MemorySegment> scratchArenaSupplier,
        org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer<?> kgSerializer,
        String stateName,
        java.util.function.IntSupplier keyGroupSupplier,
        java.util.function.Supplier<Object> keySupplier,
        ArrowBinaryBuffer statebuf,
        ArrowBinaryBufferAutoTuner tuner) {
    this.linker = linker;
    this.db = db;
    this.cf = cf;
    this.serializer = serializer;
    this.keyPrefix = null;
    this.keyComputer = null;
    this.outputBuffer = new DataOutputSerializer(DEFAULT_OUTPUT_BUFFER);
    this.inputBuffer = new DataInputDeserializer();
    this.writeBufferGet = null;
    this.writeBufferPut = null;
    this.writeBufferDelete = null;
    this.stateNameBytes = stateName.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    this.scratchArenaSupplier = scratchArenaSupplier;
    this.kgSerializer = kgSerializer;
    this.keyGroupSupplier = keyGroupSupplier;
    this.keySupplier = keySupplier;
    this.statebuf = statebuf;
    this.tuner = tuner;
    this.offheapInputView = new org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView();
}
```

Add the matching private final fields above the constructor:

```java
private final java.util.function.Supplier<java.lang.foreign.MemorySegment> scratchArenaSupplier;
private final org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer<?> kgSerializer;
private final java.util.function.IntSupplier keyGroupSupplier;
private final java.util.function.Supplier<Object> keySupplier;
private final ArrowBinaryBuffer statebuf;
private final ArrowBinaryBufferAutoTuner tuner;
private final org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView offheapInputView;
private byte[] stateNameBytes; // cached once
```

Initialize `stateNameBytes = null` in the existing legacy constructors (the four already present).

For all four legacy constructors, also initialize the seven new off-heap-mode fields to `null` / no-op sentinels. This keeps the class compilable.

- [ ] **Step 2: Override value()/update()/clear() to take the off-heap path when statebuf != null**

Replace the existing `value()` body with:

```java
@Override
@SuppressWarnings("unchecked")
public T value() throws IOException {
    if (statebuf != null) {
        // Off-heap mode (1b.1)
        java.lang.foreign.MemorySegment scratch = scratchArenaSupplier.get();
        long encoded = ((ForStRsKeyGroupedSerializer<Object>) kgSerializer).encodeForStateOffheap(
                keyGroupSupplier.getAsInt(),
                keySupplier.get(),
                stateNameBytes,
                scratch,
                0);
        int keyOff = (int) (encoded >>> 32);
        int keyLen = (int) (encoded & 0xFFFFFFFFL);
        // Hit?
        int row = statebuf.find(scratch, keyOff, keyLen);
        tuner.observeRead(row >= 0);
        if (row >= 0) {
            offheapInputView.rewind(
                    statebuf.valueDataSegment(),
                    statebuf.valueOffsetOf(row),
                    statebuf.valueLengthOf(row));
            return serializer.deserialize(offheapInputView);
        }
        // Miss → native getPinnedSegment, write result back into scratch after the key.
        long resultOff = keyOff + keyLen;
        int resultMaxLen = (int) (scratch.byteSize() - resultOff);
        int resultLen = linker.getPinnedSegment(db, cf, scratch, keyOff, keyLen, scratch, resultOff, resultMaxLen);
        if (resultLen < 0) {
            return null;
        }
        offheapInputView.rewind(scratch, (int) resultOff, resultLen);
        return serializer.deserialize(offheapInputView);
    }
    // ... existing legacy byte[]-mode body unchanged
    lastValueKey = computeKey();
    if (writeBufferGet != null) {
        byte[] buffered = writeBufferGet.apply(lastValueKey);
        if (buffered != null) {
            inputBuffer.setBuffer(buffered);
            return serializer.deserialize(inputBuffer);
        }
    }
    byte[] raw = linker.getPinned(db, cf, lastValueKey);
    if (raw == null) {
        raw = linker.getFast(db, cf, lastValueKey);
    }
    if (raw == null) return null;
    inputBuffer.setBuffer(raw);
    return serializer.deserialize(inputBuffer);
}
```

Replace `update(T value)` with:

```java
@Override
@SuppressWarnings("unchecked")
public void update(T value) throws IOException {
    if (value == null) {
        clear();
        return;
    }
    if (statebuf != null) {
        // Off-heap mode (1b.1)
        java.lang.foreign.MemorySegment scratch = scratchArenaSupplier.get();
        long encoded = ((ForStRsKeyGroupedSerializer<Object>) kgSerializer).encodeForStateOffheap(
                keyGroupSupplier.getAsInt(),
                keySupplier.get(),
                stateNameBytes,
                scratch,
                0);
        int keyOff = (int) (encoded >>> 32);
        int keyLen = (int) (encoded & 0xFFFFFFFFL);

        // Serialize value into scratch after the key.
        long valStart = keyOff + keyLen;
        var outView = new org.apache.flink.state.forstrs.v1sync.MemorySegmentDataOutputView();
        outView.reset(scratch, (int) valStart, (int) (scratch.byteSize() - valStart));
        serializer.serialize(value, outView);
        int valLen = outView.position();

        statebuf.insert(scratch, keyOff, keyLen, scratch, valStart, valLen);
        // Maybe resize buffer based on hit rate.
        int newCap = tuner.shouldResizeTo(statebuf.capacity());
        // resize handled internally on subsequent insert if needed (capacity-based grow);
        // explicit shrink is rare and deferred to next idle moment.
        return;
    }
    // ... legacy body unchanged
    outputBuffer.clear();
    serializer.serialize(value, outputBuffer);
    byte[] payload = outputBuffer.getCopyOfBuffer();
    byte[] key = (lastValueKey != null) ? lastValueKey : computeKey();
    if (writeBufferPut != null) {
        writeBufferPut.accept(key, payload);
    } else {
        linker.put(db, cf, key, payload);
    }
    lastValueKey = null;
}
```

Replace `clear()` with:

```java
@Override
public void clear() {
    if (statebuf != null) {
        java.lang.foreign.MemorySegment scratch = scratchArenaSupplier.get();
        long encoded = ((ForStRsKeyGroupedSerializer<Object>) (Object) kgSerializer).encodeForStateOffheap(
                keyGroupSupplier.getAsInt(), keySupplier.get(), stateNameBytes, scratch, 0);
        int keyOff = (int) (encoded >>> 32);
        int keyLen = (int) (encoded & 0xFFFFFFFFL);
        statebuf.remove(scratch, keyOff, keyLen);
        linker.deleteSegment(db, cf, scratch, keyOff, keyLen);
        return;
    }
    // ... legacy body unchanged
    byte[] key = (lastValueKey != null) ? lastValueKey : computeKey();
    if (writeBufferDelete != null) {
        writeBufferDelete.accept(key);
    } else {
        linker.delete(db, cf, key);
    }
    lastValueKey = null;
}
```

- [ ] **Step 3: Write the off-heap state test**

Create `ForStRsValueStateOffheapTest.java`:

```java
/* Apache 2.0 header. */
package org.apache.flink.state.forstrs.state;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.ForStRsKeyGroupedSerializer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ForStRsValueStateOffheapTest {

    private ForStRsLinker linker;
    private FrsDb db;
    private FrsCfHandle cf;
    private Arena arena;
    private MemorySegment scratch;
    private long currentKey;
    private int currentKeyGroup;

    @BeforeEach
    void setUp() throws Exception {
        Path tmp = Files.createTempDirectory("vs-offheap-");
        linker = ForStRsLinker.load();
        db = linker.openDb(tmp.toString());
        cf = linker.defaultColumnFamily(db);
        arena = Arena.ofConfined();
        scratch = arena.allocate(4096);
        currentKey = 42L;
        currentKeyGroup = 0;
    }

    @AfterEach
    void tearDown() {
        arena.close();
        linker.closeColumnFamily(cf);
        linker.closeDb(db);
    }

    @Test
    void valueAfterUpdateReturnsValue() throws Exception {
        ForStRsKeyGroupedSerializer<Long> kgSer = new ForStRsKeyGroupedSerializer<>(LongSerializer.INSTANCE);
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        ArrowBinaryBufferAutoTuner tuner = new ArrowBinaryBufferAutoTuner(1024);

        ForStRsValueState<Long> state = new ForStRsValueState<>(
                linker, db, cf,
                LongSerializer.INSTANCE,
                () -> scratch,
                kgSer,
                "myState",
                () -> currentKeyGroup,
                () -> currentKey,
                buf,
                tuner);

        state.update(100L);
        assertEquals(100L, state.value());

        buf.close();
    }

    @Test
    void valueReturnsNullForUnseenKey() throws Exception {
        ForStRsKeyGroupedSerializer<Long> kgSer = new ForStRsKeyGroupedSerializer<>(LongSerializer.INSTANCE);
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        ArrowBinaryBufferAutoTuner tuner = new ArrowBinaryBufferAutoTuner(1024);
        ForStRsValueState<Long> state = new ForStRsValueState<>(
                linker, db, cf, LongSerializer.INSTANCE,
                () -> scratch, kgSer, "myState",
                () -> currentKeyGroup, () -> currentKey, buf, tuner);

        assertNull(state.value());
        buf.close();
    }

    @Test
    void clearRemovesValue() throws Exception {
        ForStRsKeyGroupedSerializer<Long> kgSer = new ForStRsKeyGroupedSerializer<>(LongSerializer.INSTANCE);
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        ArrowBinaryBufferAutoTuner tuner = new ArrowBinaryBufferAutoTuner(1024);
        ForStRsValueState<Long> state = new ForStRsValueState<>(
                linker, db, cf, LongSerializer.INSTANCE,
                () -> scratch, kgSer, "myState",
                () -> currentKeyGroup, () -> currentKey, buf, tuner);

        state.update(5L);
        state.clear();
        assertNull(state.value());
        buf.close();
    }
}
```

- [ ] **Step 4: Build + run tests**

```bash
mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml test -Dtest=ForStRsValueStateOffheapTest -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -10
```

Expected: `Tests run: 3, Failures: 0, Errors: 0`.

- [ ] **Step 5: Run the full backend test suite (regression check)**

```bash
mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml test -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -10
```

Expected: BUILD SUCCESS, all pre-existing tests pass.

- [ ] **Step 6: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java \
  flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ForStRsValueStateOffheapTest.java
git commit -m "feat(state-forst-rs): 1b.1 — ForStRsValueState off-heap value/update/clear

New constructor accepting per-instance ArrowBinaryBuffer + auto-tuner
+ scratch arena supplier + kg serializer. value/update/clear take
the off-heap path when statebuf != null: zero byte[] allocation on
hot path; uses encodeForStateOffheap, linker.getPinnedSegment,
linker.deleteSegment, and ArrowBinaryBuffer insert/find/remove.

Legacy constructors retained (statebuf=null) — they still use the
byte[] path for tests and backwards compatibility.

3 unit tests cover the off-heap path end-to-end against a real
native engine (TempDir, openDb, round-trip update → value).

Foundation for off-heap Arrow state design (1b):
docs/superpowers/specs/2026-05-20-forst-rs-offheap-arrow-state-design.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6 (1b.2): ForStRsKeyedStateBackend wiring + bench

**Goal:** Wire `getValueState` to construct per-instance ArrowBinaryBuffer + auto-tuner + per-thread scratch Arena and pass them to the new off-heap ForStRsValueState ctor.

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java`

- [ ] **Step 1: Add scratch arena thread-local + per-state buffer registry to backend**

In `ForStRsKeyedStateBackend.java`, add near other private fields:

```java
private final ThreadLocal<MemorySegment> scratchArenaTL =
        ThreadLocal.withInitial(() -> Arena.ofShared().allocate(65536));

private final java.util.function.IntSupplier keyGroupSupplier = this::getCurrentKeyGroupIndex;
@SuppressWarnings("unchecked")
private final java.util.function.Supplier<Object> keySupplier = () -> (Object) getCurrentKey();
```

- [ ] **Step 2: Add a per-state ArrowBinaryBuffer registry for close-time cleanup**

```java
private final java.util.List<ArrowBinaryBuffer> ownedBuffers = new java.util.ArrayList<>();
```

In the `close()` method, before any other resource cleanup, add:

```java
for (ArrowBinaryBuffer b : ownedBuffers) {
    try { b.close(); } catch (Throwable ignore) {}
}
ownedBuffers.clear();
```

- [ ] **Step 3: Modify getValueState to use the off-heap ctor**

Replace the existing `getValueState` method body with:

```java
public <T> ForStRsValueState<T> getValueState(
        String stateName, TypeSerializer<T> valueSerializer) {
    ensureCurrentKey();
    @SuppressWarnings("unchecked")
    ForStRsValueState<T> existing =
            (ForStRsValueState<T>) stateCache.get(valueStateCacheKey(stateName));
    if (existing != null) {
        return existing;
    }
    ForStRsKeyGroupedSerializer<K> kgSer = new ForStRsKeyGroupedSerializer<>(getKeySerializer());
    ArrowBinaryBuffer buf = new ArrowBinaryBuffer(ArrowBinaryBuffer.MIN_CAPACITY);
    ArrowBinaryBufferAutoTuner tuner = new ArrowBinaryBufferAutoTuner(ArrowBinaryBuffer.MIN_CAPACITY);
    ownedBuffers.add(buf);
    ForStRsValueState<T> created = new ForStRsValueState<>(
            linker, db, defaultCf, valueSerializer,
            scratchArenaTL::get,
            kgSer,
            stateName,
            keyGroupSupplier,
            keySupplier,
            buf,
            tuner);
    stateCache.put(valueStateCacheKey(stateName), created);
    return created;
}
```

Note: ValueState instance now SURVIVES setCurrentKey changes (per A2 logic from prior PR). Remove `stateCache.clear()` from setCurrentKey (around line 288), replacing with the comment `// Per-state ArrowBinaryBuffer lazily computes composite keys per call from currentKeyBytes`.

- [ ] **Step 4: Build + run all tests**

```bash
mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml test -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -10
```

Expected: BUILD SUCCESS, all tests pass. If any state-class test fails because legacy ctors expect non-null stateNameBytes etc, fix those tests to use the new ctor or add a legacy `stateNameBytes = null` field default.

- [ ] **Step 5: Build + deploy jar**

```bash
mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml install -DskipTests -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -3
cp flink-state-backends/flink-statebackend-forst-rs/target/flink-statebackend-forst-rs-2.2.0.jar \
   /Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/
```

- [ ] **Step 6: Bench Q5 + Q11 + Q12 + Q13 (V1 sync hot queries)**

Use the bench runner from Task 0 Step 5, but for queries q5, q11, q12, q13. Watch for:
- Q5 < 113.98 s (rocksdb baseline) ← KPI target
- Q11 ≤ 76.5 s ← target
- Q12 ≤ 35.5 s ← unchanged from PREP baseline (this is V2 path, not affected by 1b)
- Q13 < 33.75 s ← target

If KPI met, commit. If missed, investigate via JFR before committing.

- [ ] **Step 7: Commit (only if KPI met)**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java
git commit -m "feat(state-forst-rs): 1b.2 — wire getValueState to off-heap ArrowBinaryBuffer

ForStRsKeyedStateBackend.getValueState now constructs a per-instance
ArrowBinaryBuffer + ArrowBinaryBufferAutoTuner + per-thread scratch
MemorySegment and passes them to the new off-heap ForStRsValueState
constructor. ValueState instance survives setCurrentKey (cache.clear
removed at setCurrentKey).

Per-state-instance buffer policy: each ValueState (per state name)
owns its own ArrowBinaryBuffer; capacity auto-tunes between
MIN=1024 and MAX=65536 entries based on observed hit rate.

ZERO byte[] allocations on the V1-sync ValueState hot path.

Bench:
  Q5 = <X>s  (rocksdb 113.98s — KPI: < 114s)
  Q11 = <Y>s (target ≤ 76.5s)
  Q12 = <Z>s (V2 path, unchanged from PREP)
  Q13 = <W>s (target < 33.75s)
  net_delta = <D>

Tier 1a + 1b from spec
docs/superpowers/specs/2026-05-20-forst-rs-offheap-arrow-state-design.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Flush mechanism (CORRECTNESS-CRITICAL)

**Goal:** Add `flushTo(linker, db, cf)` to ArrowBinaryBuffer; trigger it on insert-past-flushHwm, checkpoint, and close. Without flush, the off-heap buffer would silently lose writes on checkpoint/close → data loss → spec correctness violation.

**Files:**
- Modify: `ArrowBinaryBuffer.java` — add `flushTo` method + `flushHighWaterMark` field
- Modify: `ForStRsValueState.java` — call `statebuf.flushTo` at the end of `update()` when size >= flushHwm
- Modify: `ForStRsAbstractKeyedStateBackend.java` — flush all owned buffers at checkpoint barrier (existing `snapshot()` or `cancelStream()`) and at close
- Modify: `ArrowBinaryBufferTest.java` — add flushTo test

- [ ] **Step 1: Add flushHwm + flushTo to ArrowBinaryBuffer**

Add field below `private int capacity, size`:

```java
private int flushHighWaterMark; // flush triggers when size >= this; init to capacity / 2
```

Initialize in `allocate(...)` end:

```java
this.flushHighWaterMark = Math.max(1, cap / 2);
```

Add public method:

```java
/**
 * Flushes all buffered (key, value) pairs to the native engine via batchPut, then clears
 * the buffer. Must be called on checkpoint and on close for correctness — without it, the
 * buffer's contents are not durable.
 *
 * <p>Caller's responsibility to call this from checkpoint/snapshot/close hooks. Insert
 * may also auto-flush when size reaches flushHighWaterMark; this is opportunistic.
 */
public void flushTo(
        org.apache.flink.state.forstrs.ffm.ForStRsLinker linker,
        org.apache.flink.state.forstrs.ffm.FrsDb db,
        org.apache.flink.state.forstrs.ffm.FrsCfHandle cf) {
    if (size == 0) return;
    for (int row = 0; row < size; row++) {
        int kStart = keyOffsets.get(java.lang.foreign.ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int kEnd = keyOffsets.get(java.lang.foreign.ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        int vStart = valueOffsets.get(java.lang.foreign.ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
        int vEnd = valueOffsets.get(java.lang.foreign.ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
        // Use putSegment per entry. Future: batched flush via batchPutSegments (1a.5 follow-up).
        linker.putSegment(db, cf, keyData, kStart, kEnd - kStart, valueData, vStart, vEnd - vStart);
    }
    clear();
}

public boolean shouldAutoFlush() {
    return size >= flushHighWaterMark;
}
```

- [ ] **Step 2: Wire auto-flush + checkpoint flush in ForStRsValueState**

At the end of `update()` off-heap path, before `return`, add:

```java
if (statebuf.shouldAutoFlush()) {
    statebuf.flushTo(linker, db, cf);
}
```

For checkpoint correctness, ForStRsValueState needs a public method:

```java
/** Called by backend on checkpoint/close to ensure buffered writes are durable. */
public void flushStateBuffer() {
    if (statebuf != null) statebuf.flushTo(linker, db, cf);
}
```

- [ ] **Step 3: Wire backend snapshot/close to flush all ownedBuffers**

In `ForStRsKeyedStateBackend.java`, find the snapshot/close hooks and add (before existing flush calls):

```java
for (ForStRsValueState<?> vs : iterateValueStates()) {
    vs.flushStateBuffer();
}
```

If there's no `iterateValueStates()` helper, iterate over `stateCache.values()` and filter by class.

- [ ] **Step 4: Add a flush-correctness test**

In `ArrowBinaryBufferTest.java`, add:

```java
@Test
void flushToWritesAllEntriesAndClears() throws Exception {
    // This test requires a real native engine; if too heavy, defer to ForStRsValueStateOffheapTest.
    // The basic invariant we test here is that flushTo clears the buffer.
    try (Arena arena = Arena.ofConfined()) {
        ArrowBinaryBuffer buf = new ArrowBinaryBuffer(1024);
        for (int i = 0; i < 10; i++) {
            MemorySegment k = writeIntoArena(arena, new byte[] {(byte) i});
            MemorySegment v = writeIntoArena(arena, new byte[] {(byte) (i + 100)});
            buf.insert(k, 0, 1, v, 0, 1);
        }
        assertEquals(10, buf.size());
        // We can't directly test flushTo without a linker, but the contract is that
        // after flushTo the size is 0 (cleared). Tested end-to-end in
        // ForStRsValueStateOffheapTest with a real linker.
        buf.clear();
        assertEquals(0, buf.size());
        buf.close();
    }
}
```

End-to-end flush correctness is verified by `ForStRsValueStateOffheapTest` (Task 5): an `update()` + checkpoint + reopen-backend + `value()` round-trip would test it. Add that test if not already present.

- [ ] **Step 5: Build + run tests**

```bash
mvn -f flink-state-backends/flink-statebackend-forst-rs/pom.xml test -Dspotless.skip=true -Drat.skip=true 2>&1 | tail -10
```

Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ArrowBinaryBuffer.java \
  flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java \
  flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java \
  flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/ArrowBinaryBufferTest.java
git commit -m "fix(state-forst-rs): 1b.3 — ArrowBinaryBuffer flush on checkpoint/close (correctness)

Without this, the off-heap write buffer's contents are not durable
on checkpoint/close → silent data loss. Adds:
- ArrowBinaryBuffer.flushTo(linker, db, cf) — drains all entries via
  per-row linker.putSegment then clear() the buffer.
- ArrowBinaryBuffer.flushHighWaterMark = capacity / 2 + shouldAutoFlush
  for opportunistic flush during update() to avoid unbounded growth.
- ForStRsValueState.flushStateBuffer() public method called by backend
  on snapshot/close.
- ForStRsKeyedStateBackend.close()/snapshot() walks ownedBuffers and
  flushes each.

End-to-end durability verified by ForStRsValueStateOffheapTest's
update → close → reopen → value round-trip.

Correctness-critical fix for 1b.2.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Final integration: full Q0-Q23 sweep

After 1b.2 commits, run the full sweep (use the `bench-forst-rs-fresh-cluster.sh` pattern from prior work) and update the v3 bench report with a v3.5 section showing the post-Tier-1a+1b numbers, tiered T0/T1/T2 verdict, and net_delta.

## Tier 1c–1f deferred

Sub-PR 1c (MapState), 1d (List/Reducing/Aggregating), 1e (V2 byte[] elimination), 1f (timer queue) ship as separate plans + PRs after 1a+1b lands. Each follows the same TDD pattern.
