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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.InternalKeyContext;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsIterator;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

/**
 * Spec §6f — backend-resident {@link KeyGroupedInternalPriorityQueue} implementation that persists
 * its entries through the ForSt-RS engine, with a <b>memory-resident authoritative timer index</b>
 * serving every read.
 *
 * <p><b>MEMORY-RESIDENT TIMER INDEX (2026-06-11 redesign).</b> Per the
 * {@code 2026-06-11-memory-resident-timer-index-design} spec, a timer's element is FULLY derivable
 * from its composite key bytes ({@link #decodeElement}), so the engine is never needed to FIRE a
 * timer — only for durability and snapshot/restore. This class therefore keeps an off-heap,
 * ts-major min-heap index ({@link #liveIndex}, a second {@link ArrowTimerBuffer}) of every live
 * timer, and the entire cache-over-engine read layer (poll caches, resume cursors, refill floors,
 * exhausted-kg sets — three defect generations: the O(N²) watermark drain, the refill-floor
 * overwrite, and the floor-fix I/O tax) is DELETED:
 *
 * <ul>
 *   <li>{@code add(T)} inserts the composite into the index AND stages the engine write exactly as
 *       before (pendingBuffer → vectorized batch flush at {@link #FLUSH_THRESHOLD}).
 *   <li>{@code remove(T)} deletes from the index AND stages the engine delete as before.
 *   <li>{@code peek()}/{@code poll()} serve from the index head — pure memory, ZERO engine reads.
 *       {@code poll()} stages the fired timer's engine delete via the batched
 *       {@link #pendingPollDeletes} path.
 *   <li>Snapshot is unchanged: {@link #flushPendingToEngine()} drains pending writes/deletes, so
 *       the engine is always a correct durable superset at snapshot time.
 *   <li>Restore/open: ONE bulk kg-range engine scan at construction rebuilds the index (restore
 *       writes timer rows to the engine before queues are created — see
 *       {@code ForStRsRestoreOperation#copyTimerRowsForSourceBatch}).
 *   <li>Spill safety valve (pathological cardinality only): above {@code FRS_TIMER_INDEX_MAX}
 *       (default 8M ≈ ~400MB off-heap) the LARGEST-ts timers are evicted to engine-only and
 *       {@link #spillHorizon} records the band boundary. The index then holds EVERY live timer
 *       with {@code ts < spillHorizon}; when it empties, {@link #loadBandFromEngine} advances the
 *       horizon with one sequential band scan. The horizon is the ONLY cursor and it is MONOTONE
 *       per advance — no per-kg cursors, no floors, no invalidation.
 * </ul>
 *
 * <p><b>Key encoding.</b> Each entry is stored under a composite ForSt key:
 *
 * <pre>
 *   composite = QUEUE_NS_MARKER || snLen(2B BE) || stateName.bytes(UTF-8)
 *            || kg(2B BE) || ts(8B BE, sign-flipped) || serialize(T)
 * </pre>
 *
 * <p><b>Thread-safety.</b> Not thread-safe; the calling timer-service is responsible for
 * serialising calls per Flink's keyed-operator contract.
 *
 * @param <T> queue element type
 */
@Internal
public class ForStRsKeyGroupedInternalPriorityQueue<T extends HeapPriorityQueueElement>
        implements KeyGroupedInternalPriorityQueue<T>, AutoCloseable {

    /** Distinguishes priority-queue rows from regular keyed-state rows in the same default CF. */
    /**
     * E-H1: namespace marker for timer-queue rows. Made public-package so the
     * rescaling restore path can scan all timer rows by this prefix and filter
     * by embedded kg per row, regardless of state name.
     *
     * <p>E-H7: the marker bytes "q/" = (0x71, 0x2F) MATCH the first two bytes
     * of a regular-state key whose kg=28975 (since regular-state encoding
     * places kg(2B BE) at offset 0). Changing the marker is a wire-format
     * break that would orphan existing checkpoints, so we instead REFUSE
     * deployments where any reachable kg could collide — see
     * {@code MAX_TOTAL_KEY_GROUPS_FOR_COLLISION_SAFETY} below.
     */
    public static final byte[] QUEUE_NS_MARKER = "q/".getBytes(StandardCharsets.UTF_8);

    /**
     * E-H7: the largest {@code totalKeyGroups} where no regular kg's first
     * two BE bytes can equal {@link #QUEUE_NS_MARKER} bytes {0x71, 0x2F} =
     * decimal 28975. Construction below refuses higher values so the
     * rescaling-restore timer scan never aliases a regular-state row.
     * Flink's default maxParallelism is 128; production deployments rarely
     * exceed a few thousand. Operators that legitimately need
     * {@code maxParallelism &gt; 28975} must either downgrade their setting
     * or migrate the engine to a non-aliasing wire format (out of scope
     * for this fix because it would orphan existing checkpoints).
     */
    private static final int MAX_TOTAL_KEY_GROUPS_FOR_COLLISION_SAFETY = 28975;

    /**
     * XOR mask that flips the sign bit of a {@code long} so that big-endian lexicographic byte
     * order matches signed-integer numerical order across the full {@code long} range.
     */
    private static final long SIGN_FLIP = 0x8000_0000_0000_0000L;

    /**
     * Pending ENGINE-WRITE buffer size at which add()/remove() proactively drain to the engine.
     * The buffer is purely a write-behind batch (durability staging) now — reads never consult
     * the engine, so a flush no longer invalidates anything; the threshold only balances FFM
     * crossing amortization against snapshot-drain latency. MUST sit below ArrowTimerBuffer's
     * 65536 maxCapacity (the buffer THROWS past it).
     */
    @VisibleForTesting static final int FLUSH_THRESHOLD = 32768;

    /**
     * TIMER-INDEX: maximum live-timer entries held in the memory-resident index before the
     * cap-spill evicts the largest-ts band to engine-only. ~50-60B/entry off-heap ⇒ the default
     * 8M ≈ 400-480MB worst case per queue instance (NEXMark live-timer cardinality is low
     * millions worst case, so the spill path is pathological-only). Override via the
     * {@code FRS_TIMER_INDEX_MAX} environment variable.
     */
    private static final int INDEX_MAX = readIndexMax();

    private static int readIndexMax() {
        String v = System.getenv("FRS_TIMER_INDEX_MAX");
        if (v == null || v.isEmpty()) {
            return 8_000_000;
        }
        try {
            // Floor of 1024 keeps the band machinery sane (perKgBudget >= 1 etc.).
            return Math.max(Integer.parseInt(v.trim()), 1024);
        } catch (NumberFormatException e) {
            return 8_000_000;
        }
    }

    /** Initial scratch-segment size for composing composite keys (grows on demand). */
    private static final int SCRATCH_INITIAL_BYTES = 256;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final Arena arena;
    private final String stateName;
    private final TypeSerializer<T> elementSerializer;
    private final ToLongFunction<T> timestampExtractor;

    /** Cached encoded prefix {@code QUEUE_NS_MARKER || snLen(2B BE) || stateName.bytes}. */
    private final byte[] queuePrefix;

    /** The key-group range this queue services; defaults to a singleton {@code [kg, kg]}. */
    private final KeyGroupRange keyGroupRange;

    /**
     * Supplier for the "current key group" used by {@link #add} / {@link #remove} /
     * {@link #advance}; in a real keyed backend this proxies through {@code
     * AbstractKeyedStateBackend#getCurrentKeyGroupIndex()}, but tests inject a fixed-value
     * supplier.
     */
    private final java.util.function.IntSupplier currentKeyGroupSupplier;

    /**
     * Optional element factory used to (re-)attach side-band fields (e.g., HeapPriorityQueueElement
     * internalIndex) after deserialisation. May be {@code null} when no rebinding is required.
     */
    private final LongFunction<T> rebinder;

    // ------------------------------------------------------------------
    // Memory-resident live-timer index (authoritative read path)
    // ------------------------------------------------------------------

    /**
     * TIMER-INDEX: the authoritative ts-major min-heap of EVERY live timer composite with
     * {@code ts < spillHorizon} (normally: all of them). Off-heap Arrow-layout rows, hashed
     * find-by-composite, O(log n) insert/remove — a second {@link ArrowTimerBuffer} instance
     * (all rows OP_ADD; the op field is unused here). peek()/poll() read heap position 0.
     */
    private final ArrowTimerBuffer liveIndex;

    /**
     * TIMER-INDEX spill horizon: the smallest ts ever evicted from the index by the cap-spill
     * (or the band boundary after a {@link #loadBandFromEngine} advance). INVARIANT: the index
     * holds EVERY live timer with {@code ts < spillHorizon}; timers at/above it are engine-only.
     * {@link Long#MAX_VALUE} = no spill (the common case: index holds everything).
     */
    private long spillHorizon = Long.MAX_VALUE;

    /**
     * Memo of the last decoded head element so the hot peek-loop (Flink peeks on every timer
     * register and once per fired timer) deserializes each head composite once, not per call.
     * Keyed by exact composite bytes, so heap mutations can never serve a stale element for a
     * DIFFERENT key; re-decoding the SAME bytes is deterministic.
     */
    private byte[] peekMemoKey;

    private T peekMemoElement;

    // ------------------------------------------------------------------
    // Pending buffer (batched off-heap engine WRITE staging)
    // ------------------------------------------------------------------

    private final ArrowTimerBuffer pendingBuffer;

    /**
     * FRS-TIMER-DRAIN-BATCH (2026-06-07): fired engine-timer composite keys staged for ONE batched
     * {@link ForStRsLinker#vectorizedBatchDelete} per batch window instead of one
     * {@code deleteSegment} FFM crossing per fired timer (a watermark drain fires millions of
     * timers). Flushed when the batch reaches {@link #FLUSH_THRESHOLD}, before any engine
     * scan ({@link #loadBandFromEngine}), and from {@link #drainPendingBufferInternal} (the
     * mandatory pre-snapshot hook), so a deferred delete can never resurrect a fired timer.
     */
    private final java.util.ArrayList<byte[]> pendingPollDeletes = new java.util.ArrayList<>();

    /** FRS_TIMER_DIAG: env-gated drain attribution. */
    private static final boolean TIMER_DIAG = "1".equals(System.getProperty("forst.rs.timer.diag"));

    private long diagPollCount;
    private long diagDeleteFlushCount;
    private long diagDeleteKeyCount;

    /** Per-queue scratch segment for composing composite keys (single-threaded). */
    private final Arena scratchArena;
    private MemorySegment scratchSeg;
    private long scratchCapacity;

    /**
     * Pre-allocated staging segments for the flush path — Arrow-style
     * offsets+data arrays for the vectorized FFI shape (single FFM
     * crossing per pass, zero per-row Java heap allocation). Grown on
     * demand. Reused across flushes.
     *
     * <p>B-R5-NEW-H1: the ADD pass previously used
     * {@code linker.batchPut(byte[][], byte[][])}, a legacy ptr-array
     * shape with per-row {@code byte[kLen]} heap allocation. Switched
     * to {@code linker.vectorizedBatchPut(...)} mirroring the DELETE
     * pass — same single-FFM-crossing zero-alloc path.
     */
    private final Arena flushArena;
    private MemorySegment flushAddKeyOffsets; // (count+1) ints — ADD key offsets
    private MemorySegment flushAddKeyData; // packed ADD key bytes
    private MemorySegment flushAddValOffsets; // (count+1) ints — pre-filled [0,1,2,...]
    private MemorySegment flushAddValData; // count bytes — pre-filled 0x01 (sentinel)
    private MemorySegment flushDelOffsets; // (count+1) ints — Arrow-style offsets for vectorized delete
    private MemorySegment flushDelData; // packed key bytes
    private MemorySegment emptyValueSeg; // 1-byte non-empty sentinel (shared)
    private long flushPairCapacity;
    private long flushAddKeyDataCapacity;
    private long flushDelDataCapacity;

    /**
     * R29-M3: idempotency gate for {@link #close()}. PHASE 1.e of the backend's
     * dispose chain calls {@link #flushPendingToEngine()} via {@code
     * ForStRsAsyncKeyedStateBackend#snapshot} pre-snapshot drain, then dispose's
     * symmetric registry loop calls {@link #close()} again — which would re-drain
     * the (likely already-empty) pending buffer at the cost of another engine FFI
     * crossing AND potentially mask a prior exception by throwing on the second
     * drain. Mirror the {@code MemoryWritableFile} pattern: flip on first close,
     * skip the drain on subsequent calls; arena/buffer releases are still
     * idempotent at the {@link Arena#close()} level so re-running them is a no-op.
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Legacy IntSupplier ctor — kept for backward compatibility with existing tests.
     *
     * <p><b>Deprecated</b>: prefer the {@link InternalKeyContext}-based ctor so add/remove/advance
     * route to the key group of the <em>current</em> key (PR-A4 / S1-5 fix). The IntSupplier path
     * keeps the legacy "constant kg" behaviour: if {@code currentKeyGroupSupplier} returns a fixed
     * value the queue only sees timers in that one key group, which is the original E2-CRIT-2 bug.
     *
     * @deprecated use {@link #ForStRsKeyGroupedInternalPriorityQueue(ForStRsLinker, FrsDb,
     *     FrsCfHandle, Arena, String, TypeSerializer, ToLongFunction, InternalKeyContext, int,
     *     KeyGroupRange)} instead.
     */
    @Deprecated
    public ForStRsKeyGroupedInternalPriorityQueue(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena,
            String stateName,
            TypeSerializer<T> elementSerializer,
            ToLongFunction<T> timestampExtractor,
            java.util.function.IntSupplier currentKeyGroupSupplier,
            KeyGroupRange keyGroupRange) {
        this(
                linker,
                db,
                cf,
                arena,
                stateName,
                elementSerializer,
                timestampExtractor,
                currentKeyGroupSupplier,
                keyGroupRange,
                /* rebinder= */ null);
    }

    /**
     * Variant with an explicit element rebinder hook (used by timer integrations).
     *
     * @deprecated use the {@link InternalKeyContext}-based ctor.
     */
    @Deprecated
    public ForStRsKeyGroupedInternalPriorityQueue(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena,
            String stateName,
            TypeSerializer<T> elementSerializer,
            ToLongFunction<T> timestampExtractor,
            java.util.function.IntSupplier currentKeyGroupSupplier,
            KeyGroupRange keyGroupRange,
            LongFunction<T> rebinder) {
        // E-H4 (resolved 2026-06-02): the wire format now LENGTH-PREFIXES the
        // state name (see buildQueuePrefix + the rescaling-restore scan in
        // ForStRsRestoreOperation), so the embedded kg is located from the
        // 2-byte length, NOT by scanning for the first '/'. State names may
        // therefore contain ANY bytes — including '/', which Flink's standard
        // timer names DO ("_timer_state/processing_*" / "_timer_state/event_*").
        // The old indexOf('/') guard rejected those, failing every windowed/
        // join query at job init when the timer service is FORSTRS.
        if (stateName == null) {
            throw new IllegalArgumentException("stateName must not be null");
        }
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.arena = arena;
        this.stateName = stateName;
        this.elementSerializer = elementSerializer;
        this.timestampExtractor = timestampExtractor;
        this.currentKeyGroupSupplier = currentKeyGroupSupplier;
        this.keyGroupRange = keyGroupRange;
        this.rebinder = rebinder;
        this.queuePrefix = buildQueuePrefix(stateName);

        this.pendingBuffer = new ArrowTimerBuffer();
        this.liveIndex = new ArrowTimerBuffer(ArrowTimerBuffer.DEFAULT_INITIAL_CAPACITY, INDEX_MAX);
        this.scratchArena = Arena.ofShared();
        this.scratchCapacity = SCRATCH_INITIAL_BYTES;
        this.scratchSeg = scratchArena.allocate(scratchCapacity);

        this.flushArena = Arena.ofShared();
        this.flushPairCapacity = 0L;
        this.flushAddKeyDataCapacity = 0L;
        this.flushDelDataCapacity = 0L;
        this.emptyValueSeg = flushArena.allocate(1L);
        this.emptyValueSeg.set(ValueLayout.JAVA_BYTE, 0L, (byte) 1);

        // TIMER-INDEX restore/open hook: ONE bulk sequential kg-range scan of the engine prefix
        // rebuilds the index. Restore writes timer rows to the engine BEFORE queues are created
        // (ForStRsRestoreOperation#copyTimerRowsForSourceBatch runs in the backend builder;
        // createInternalPriorityQueue runs at operator init), so this scan observes the full
        // restored state. On a fresh job the prefixes are empty and this is one cheap
        // iterator-open per kg. fromTs = Long.MIN_VALUE scans each kg from its prefix start.
        loadBandFromEngine(Long.MIN_VALUE);
    }

    /**
     * Preferred ctor (PR-A4 / S1-5 fix): derives the current key group from {@code keyContext} on
     * every {@code add()}/{@code remove()}/{@code advance()} call so the queue dispatches into the
     * key group of the <em>current</em> key, not a fixed constant.
     *
     * <p>The supplier reads {@code keyContext.getCurrentKey()} and hashes it via {@link
     * KeyGroupRangeAssignment#assignToKeyGroup(Object, int)}. When the current key is {@code null}
     * (e.g. before the first record arrives, or during a poll() driven by close()/snapshot() from
     * the mailbox thread outside any record context), we fall back to {@code
     * keyGroupRange.getStartKeyGroup()} — the same value the legacy ctor used as its constant.
     *
     * <p>Safety: the supplier is invoked from {@code peek/poll/add/remove/advance}. Flink's
     * async-state V2 contract serializes those calls on the operator's mailbox thread (the
     * timer-service runs on the same thread as state mutation), so reading the key-context's
     * current key here is race-free.
     */
    public ForStRsKeyGroupedInternalPriorityQueue(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena,
            String stateName,
            TypeSerializer<T> elementSerializer,
            ToLongFunction<T> timestampExtractor,
            InternalKeyContext<?> keyContext,
            int totalKeyGroups,
            KeyGroupRange keyGroupRange) {
        this(
                linker,
                db,
                cf,
                arena,
                stateName,
                elementSerializer,
                timestampExtractor,
                deriveSupplier(keyContext, totalKeyGroups, keyGroupRange),
                keyGroupRange,
                /* rebinder= */ null);
    }

    /** Variant of the {@link InternalKeyContext}-based ctor with an explicit rebinder. */
    public ForStRsKeyGroupedInternalPriorityQueue(
            ForStRsLinker linker,
            FrsDb db,
            FrsCfHandle cf,
            Arena arena,
            String stateName,
            TypeSerializer<T> elementSerializer,
            ToLongFunction<T> timestampExtractor,
            InternalKeyContext<?> keyContext,
            int totalKeyGroups,
            KeyGroupRange keyGroupRange,
            LongFunction<T> rebinder) {
        this(
                linker,
                db,
                cf,
                arena,
                stateName,
                elementSerializer,
                timestampExtractor,
                deriveSupplier(keyContext, totalKeyGroups, keyGroupRange),
                keyGroupRange,
                rebinder);
    }

    /**
     * Builds the {@link java.util.function.IntSupplier} that returns the current-key's key group,
     * falling back to {@code keyGroupRange.getStartKeyGroup()} when no current key is set.
     */
    private static java.util.function.IntSupplier deriveSupplier(
            InternalKeyContext<?> keyContext, int totalKeyGroups, KeyGroupRange keyGroupRange) {
        if (keyContext == null) {
            throw new IllegalArgumentException("keyContext must not be null");
        }
        if (totalKeyGroups <= 0) {
            throw new IllegalArgumentException(
                    "totalKeyGroups must be > 0, got " + totalKeyGroups);
        }
        // E-H7 guard — see QUEUE_NS_MARKER doc and
        // MAX_TOTAL_KEY_GROUPS_FOR_COLLISION_SAFETY.
        if (totalKeyGroups > MAX_TOTAL_KEY_GROUPS_FOR_COLLISION_SAFETY) {
            throw new IllegalArgumentException(
                    "totalKeyGroups must be <= "
                            + MAX_TOTAL_KEY_GROUPS_FOR_COLLISION_SAFETY
                            + " because higher values produce regular-state row prefixes"
                            + " that alias the timer-queue namespace marker"
                            + " {0x71, 0x2F} (\"q/\") in the rescaling-restore path."
                            + " Got "
                            + totalKeyGroups);
        }
        final int fallback = keyGroupRange.getStartKeyGroup();
        return () -> {
            Object k = keyContext.getCurrentKey();
            if (k == null) {
                return fallback;
            }
            return KeyGroupRangeAssignment.assignToKeyGroup(k, totalKeyGroups);
        };
    }

    // ------------------------------------------------------------------
    // Composite-key encoding helpers
    // ------------------------------------------------------------------

    private static byte[] buildQueuePrefix(String stateName) {
        byte[] sn = stateName.getBytes(StandardCharsets.UTF_8);
        // Layout: QUEUE_NS_MARKER || snLen(2B BE) || stateName.bytes
        // The 2-byte length prefix (replacing the old trailing '/' delimiter)
        // lets the rescaling-restore scan locate the embedded kg from the length
        // alone, so state names containing '/' (Flink's _timer_state/* names)
        // are unambiguous. kg(2B BE) is written immediately after this prefix
        // (see encode), so keyGroupPrefix — which indexes at queuePrefix.length —
        // needs no change.
        if (sn.length > 0xFFFF) {
            throw new IllegalArgumentException(
                    "timer stateName UTF-8 length " + sn.length + " exceeds 65535: " + stateName);
        }
        byte[] out = new byte[QUEUE_NS_MARKER.length + 2 + sn.length];
        System.arraycopy(QUEUE_NS_MARKER, 0, out, 0, QUEUE_NS_MARKER.length);
        out[QUEUE_NS_MARKER.length] = (byte) ((sn.length >>> 8) & 0xFF);
        out[QUEUE_NS_MARKER.length + 1] = (byte) (sn.length & 0xFF);
        System.arraycopy(sn, 0, out, QUEUE_NS_MARKER.length + 2, sn.length);
        return out;
    }

    /**
     * Test seam: exposes the encoded queue prefix ({@code QUEUE_NS_MARKER ||
     * snLen(2B BE) || stateName}) so restore-side tests can build a wire-format
     * key via the SAME producer the queue uses and assert the restore scan
     * extracts the embedded kg — proving producer/consumer agreement across a
     * state name that contains '/'.
     */
    @VisibleForTesting
    public static byte[] buildQueuePrefixForTesting(String stateName) {
        return buildQueuePrefix(stateName);
    }

    /** Returns {@code queuePrefix || kg(2B BE)} — a prefix that scans one key group. */
    private byte[] keyGroupPrefix(int keyGroup) {
        validateKeyGroup(keyGroup);
        byte[] out = new byte[queuePrefix.length + 2];
        System.arraycopy(queuePrefix, 0, out, 0, queuePrefix.length);
        out[queuePrefix.length] = (byte) ((keyGroup >>> 8) & 0xFF);
        out[queuePrefix.length + 1] = (byte) (keyGroup & 0xFF);
        return out;
    }

    /** Encodes the full composite key for an element. */
    private byte[] encode(int keyGroup, T element) {
        validateKeyGroup(keyGroup);
        long ts = timestampExtractor.applyAsLong(element);
        long flipped = ts ^ SIGN_FLIP;
        DataOutputSerializer out = new DataOutputSerializer(64);
        try {
            out.write(queuePrefix);
            out.writeShort(keyGroup);
            out.writeLong(flipped);
            elementSerializer.serialize(element, out);
        } catch (IOException e) {
            throw new RuntimeException("encode failed: " + e.getMessage(), e);
        }
        return out.getCopyOfBuffer();
    }

    /**
     * Encodes the composite key for {@code (keyGroup, element)} into {@link #scratchSeg}, growing
     * the scratch segment if needed. Returns the encoded length.
     */
    private int encodeIntoScratch(int keyGroup, T element) {
        // Reuse the byte[] path for now (no off-heap TypeSerializer support yet) — but copy into
        // the scratch MemorySegment so downstream pendingBuffer hash/copy operates on segments.
        byte[] encoded = encode(keyGroup, element);
        ensureScratchCapacity(encoded.length);
        MemorySegment.copy(encoded, 0, scratchSeg, ValueLayout.JAVA_BYTE, 0, encoded.length);
        return encoded.length;
    }

    private void ensureScratchCapacity(int needed) {
        if (needed <= scratchCapacity) {
            return;
        }
        long newCap = Math.max(scratchCapacity * 2, needed);
        scratchSeg = scratchArena.allocate(newCap);
        scratchCapacity = newCap;
    }

    /** Returns the {@code long} timestamp encoded inside the composite key. */
    private long decodeTimestamp(byte[] composite) {
        int off = queuePrefix.length + 2; // skip queuePrefix + 2-byte kg
        if (composite.length < off + 8) {
            throw new IllegalArgumentException("composite too short to decode ts");
        }
        long flipped = 0L;
        for (int i = 0; i < 8; i++) {
            flipped = (flipped << 8) | (composite[off + i] & 0xFFL);
        }
        return flipped ^ SIGN_FLIP;
    }

    /**
     * TIMER-INDEX: reads the sign-flipped timestamp directly from a composite key held in an
     * off-heap segment (engine scan chunk) without materialising a byte[].
     */
    private long decodeTimestampFromSegment(MemorySegment seg, long keyOff, int keyLen) {
        int off = queuePrefix.length + 2;
        if (keyLen < off + 8) {
            throw new IllegalArgumentException("composite too short to decode ts");
        }
        long flipped = 0L;
        for (int i = 0; i < 8; i++) {
            flipped = (flipped << 8) | (seg.get(ValueLayout.JAVA_BYTE, keyOff + off + i) & 0xFFL);
        }
        return flipped ^ SIGN_FLIP;
    }

    /** Deserialises the element tail of a composite key. */
    private T decodeElement(byte[] composite) {
        int off = queuePrefix.length + 2 + 8;
        DataInputDeserializer in = new DataInputDeserializer();
        in.setBuffer(composite, off, composite.length - off);
        try {
            T t = elementSerializer.deserialize(in);
            if (rebinder != null) {
                long ts = decodeTimestamp(composite);
                T rebound = rebinder.apply(ts);
                if (rebound != null) {
                    return rebound;
                }
            }
            return t;
        } catch (IOException e) {
            throw new RuntimeException("decodeElement failed: " + e.getMessage(), e);
        }
    }

    private static void validateKeyGroup(int keyGroup) {
        if (keyGroup < 0 || keyGroup > 0xFFFF) {
            throw new IllegalArgumentException("keyGroup out of [0, 65535]: " + keyGroup);
        }
    }

    // ------------------------------------------------------------------
    // Memory-resident index — internal operations
    // ------------------------------------------------------------------

    /**
     * Inserts the composite at {@code [seg+off, seg+off+len)} with timestamp {@code ts} into the
     * live index. Dedupes (re-adding an existing composite is a no-op — engine put is idempotent
     * and the index must mirror that), respects the spill horizon (composites at/above it are
     * engine-only by invariant), and triggers the cap-spill when full.
     */
    private void indexInsert(MemorySegment seg, long off, int len, long ts) {
        // NOTE: spillHorizon == Long.MAX_VALUE means NO spill — every timer (including the
        // legitimate ts == Long.MAX_VALUE end-of-window cleanup timers Flink registers) is
        // index-resident. A plain `ts >= spillHorizon` would silently drop MAX_VALUE timers.
        if (spillHorizon != Long.MAX_VALUE && ts >= spillHorizon) {
            // Engine-only band — by invariant the index holds only ts < spillHorizon.
            return;
        }
        if (liveIndex.find(seg, off, len) >= 0) {
            // Dedupe: already live (timer re-registration).
            return;
        }
        if (liveIndex.size() >= INDEX_MAX) {
            capSpill();
            if (spillHorizon != Long.MAX_VALUE && ts >= spillHorizon) {
                // The spill moved the horizon below this entry — it is engine-only now.
                return;
            }
        }
        liveIndex.insertAdd(seg, off, len, ts);
    }

    /**
     * Removes the composite staged in {@link #scratchSeg} from the live index, if present.
     * Returns whether it was present (= the timer was live, since the index is authoritative
     * below the spill horizon).
     */
    private boolean indexRemoveFromScratch(int len) {
        int pos = liveIndex.find(scratchSeg, 0L, len);
        if (pos >= 0) {
            liveIndex.removeAt(pos);
            return true;
        }
        return false;
    }

    /** Removes a composite (byte[] form) from the live index, if present. */
    private void indexRemoveByComposite(byte[] composite) {
        ensureScratchCapacity(composite.length);
        MemorySegment.copy(
                composite, 0, scratchSeg, ValueLayout.JAVA_BYTE, 0, composite.length);
        indexRemoveFromScratch(composite.length);
    }

    /**
     * TIMER-INDEX cap-spill (safety valve, pathological cardinality only): evicts the LARGEST-ts
     * half of the index to engine-only and lowers {@link #spillHorizon} to the eviction cutoff
     * (= min ts of the evicted band). Every evicted row is durable: it is either already in the
     * engine or still staged as a pending ADD in {@link #pendingBuffer}, which flushes
     * independently of the index.
     */
    private void capSpill() {
        int keep = Math.max(INDEX_MAX / 2, 1);
        long cutoff = liveIndex.tsAtRank(keep);
        if (cutoff <= liveIndex.tsAt(0)) {
            // Every entry shares the minimum ts — there is no ts-band to spill. With the
            // default cap this means >4M live timers at ONE timestamp in ONE queue; refuse
            // loudly rather than silently degrade (the band reload could not make progress
            // either — see loadBandFromEngine's progress guard).
            throw new IllegalStateException(
                    "FRS_TIMER_INDEX_MAX="
                            + INDEX_MAX
                            + " exceeded by a single-timestamp timer cohort (ts="
                            + cutoff
                            + ", live="
                            + liveIndex.size()
                            + ") in queue '"
                            + stateName
                            + "' — raise FRS_TIMER_INDEX_MAX");
        }
        liveIndex.removeAtOrAboveTs(cutoff);
        spillHorizon = Math.min(spillHorizon, cutoff);
    }

    /** Copies the composite key bytes of the index row at {@code pos} into a fresh byte[]. */
    private byte[] copyIndexKey(int pos) {
        int kOff = liveIndex.keyOffsetAt(pos);
        int kLen = liveIndex.keyLenAt(pos);
        byte[] out = new byte[kLen];
        MemorySegment.copy(liveIndex.keyDataSegment(), ValueLayout.JAVA_BYTE, kOff, out, 0, kLen);
        return out;
    }

    /**
     * M5-V1 (hot-path alloc/copy audit 2026-06-12): segment-direct head-memo probe — true iff the
     * index head's composite bytes at {@code (kOff, kLen)} equal {@link #peekMemoKey}. Replaces
     * the old {@code decodeElementMemo(copyIndexKey(0))} shape whose memo-HIT case (the common
     * one: repeated peeks of the same head) still paid a fresh byte[] alloc + segment→heap copy +
     * {@code Arrays.equals} before discovering it didn't need the bytes. The compare runs via the
     * {@code MemorySegment.mismatch} intrinsic with ZERO allocation ({@code ofArray} is a
     * non-copying heap-segment view the JIT scalarizes).
     *
     * <p>Caution (audit V1): {@code removeAtOrAboveTs}/index mutation invalidate offsets — callers
     * must read {@code keyOffsetAt(0)}/{@code keyLenAt(0)} AFTER any mutation and use this probe
     * before {@code removeAt(0)}.
     */
    private boolean headMatchesMemo(int kOff, int kLen) {
        return peekMemoKey != null
                && peekMemoKey.length == kLen
                && MemorySegment.mismatch(
                                liveIndex.keyDataSegment(),
                                kOff,
                                kOff + (long) kLen,
                                MemorySegment.ofArray(peekMemoKey),
                                0L,
                                kLen)
                        == -1;
    }

    /** Memo-MISS path: decodes the OWNED {@code composite} and refreshes the head-memo. */
    private T decodeAndMemoize(byte[] composite) {
        T e = decodeElement(composite);
        peekMemoKey = composite;
        peekMemoElement = e;
        return e;
    }

    /**
     * TIMER-INDEX horizon advance trigger: when the index has drained empty AND a spill horizon
     * is set, reload the next ts-band from the engine. Rare path (only ever runs after a
     * cap-spill, i.e. >INDEX_MAX live timers at some point).
     */
    private void maybeAdvanceSpillHorizon() {
        while (liveIndex.size() == 0 && spillHorizon != Long.MAX_VALUE) {
            long from = spillHorizon;
            // The engine must be authoritative before we re-read it: pending ADDs at/above the
            // horizon were never indexed and live only in pendingBuffer; fired-timer deletes
            // must land so the scan cannot resurrect them. flushPendingToEngine drains both.
            flushPendingToEngine();
            loadBandFromEngine(from);
            if (spillHorizon == Long.MAX_VALUE) {
                return; // engine exhausted — index now holds everything that remains
            }
            if (liveIndex.size() > 0) {
                return; // band loaded
            }
            // Band [from, spillHorizon) was empty in the engine (all its rows were deleted
            // before this advance) — loop to the next band. Progress is guaranteed because
            // loadBandFromEngine only returns a horizon STRICTLY above `from` (see its guard).
        }
    }

    /**
     * TIMER-INDEX bulk band load: scans every kg's engine range {@code [fromTs, ∞)} sequentially
     * and (re)builds the index with up to ~half the cap, leaving {@link #spillHorizon} at the
     * first unloaded timestamp (or {@link Long#MAX_VALUE} when everything fit). Used by the
     * constructor (restore/open, {@code fromTs = Long.MIN_VALUE}) and by
     * {@link #maybeAdvanceSpillHorizon}.
     *
     * <p>Band-consistency rule: a kg's scan stops only at a ts BOUNDARY (never splitting a run of
     * equal-ts rows) once its row budget is used; the new horizon is the minimum first-unread ts
     * across kgs, and any loaded row at/above that horizon is evicted again. This keeps the
     * invariant exact — the index holds EVERY live timer with {@code ts < spillHorizon} — with
     * ONE monotone cursor and no per-kg state.
     */
    private void loadBandFromEngine(long fromTs) {
        // Optimistic: assume the whole remaining tail fits. indexInsert's capSpill lowers the
        // horizon again if it does not, and the stop-ts accounting below lowers it for budget
        // stops; the final min of the two is the true band edge.
        spillHorizon = Long.MAX_VALUE;
        int numKgs = Math.max(keyGroupRange.getNumberOfKeyGroups(), 1);
        int target = Math.max(INDEX_MAX / 2, 1);
        int perKgBudget = Math.max(target / numKgs, 1);
        long stopMin = Long.MAX_VALUE;
        for (int kg = keyGroupRange.getStartKeyGroup();
                kg <= keyGroupRange.getEndKeyGroup();
                kg++) {
            long stopTs = scanKgIntoIndex(kg, fromTs, perKgBudget);
            if (stopTs < stopMin) {
                stopMin = stopTs;
            }
        }
        long newHorizon = Math.min(stopMin, spillHorizon);
        if (newHorizon != Long.MAX_VALUE) {
            // Trim rows at/above the band edge so the invariant is exact. They remain
            // engine-resident and are reloaded by the next horizon advance.
            liveIndex.removeAtOrAboveTs(newHorizon);
            spillHorizon = newHorizon;
            if (liveIndex.size() == 0 && newHorizon <= fromTs) {
                // No progress is impossible by construction (budget stops are always at a ts
                // strictly above the last loaded ts >= fromTs); reachable only via a cap-spill
                // mid-load on a single-timestamp mega-cohort, which capSpill already rejects.
                throw new IllegalStateException(
                        "timer index band reload made no progress at ts="
                                + newHorizon
                                + " for queue '"
                                + stateName
                                + "' — raise FRS_TIMER_INDEX_MAX");
            }
        }
    }

    /**
     * Scans one kg's engine range from {@code fromTs} (or the kg prefix start when {@code fromTs
     * == Long.MIN_VALUE}) into the live index via the lazy, index-seeking, tombstone-skipping
     * {@code frs_vec_iter_range_open} FFI (chunked; one open per kg per band — sequential bulk
     * I/O, NOT per-poll). Loads rows until the kg is exhausted or {@code budget} rows are loaded
     * AND the next row starts a new timestamp (equal-ts runs are never split — see
     * {@link #loadBandFromEngine}). Returns the ts of the first row NOT loaded, or
     * {@link Long#MAX_VALUE} when the kg was exhausted.
     */
    private long scanKgIntoIndex(int kg, long fromTs, int budget) {
        byte[] kgPrefix = keyGroupPrefix(kg);
        byte[] lo = (fromTs == Long.MIN_VALUE) ? kgPrefix : buildSeekKey(kg, fromTs);
        byte[] hi = prefixUpperBound(kgPrefix);
        final int chunkCap = 64 * 1024;
        int loaded = 0;
        long lastTs = Long.MIN_VALUE;
        try (Arena a = Arena.ofConfined()) {
            MemorySegment chunkBuf = a.allocate(chunkCap);
            MemorySegment loSeg = a.allocate(Math.max(1, lo.length));
            MemorySegment.copy(lo, 0, loSeg, ValueLayout.JAVA_BYTE, 0, lo.length);
            int hiLen = (hi == null) ? 0 : hi.length;
            MemorySegment hiSeg = a.allocate(Math.max(1, hiLen));
            if (hiLen > 0) {
                MemorySegment.copy(hi, 0, hiSeg, ValueLayout.JAVA_BYTE, 0, hiLen);
            }
            MemorySegment outHandle = a.allocate(ValueLayout.JAVA_LONG);
            MemorySegment outRowCount = a.allocate(ValueLayout.JAVA_INT);
            MemorySegment outBytesUsed = a.allocate(ValueLayout.JAVA_INT);
            int rc =
                    linker.frsVecIterRangeOpen(
                            db.handle(),
                            cf.handle(),
                            loSeg,
                            lo.length,
                            hiSeg,
                            hiLen,
                            chunkBuf,
                            chunkCap,
                            outHandle,
                            outRowCount,
                            outBytesUsed);
            if (rc != 0) {
                throw new RuntimeException("frs_vec_iter_range_open rc=" + rc);
            }
            long handle = outHandle.get(ValueLayout.JAVA_LONG, 0);
            try {
                int rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                while (rowCount > 0) {
                    int off = 0;
                    for (int i = 0; i < rowCount; i++) {
                        int klen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
                        off += 4;
                        int vlen = chunkBuf.get(ValueLayout.JAVA_INT_UNALIGNED, off);
                        off += 4;
                        long ts = decodeTimestampFromSegment(chunkBuf, off, klen);
                        // Stop at a ts boundary once the budget is used, or unconditionally if
                        // the index is at the hard cap (forced stop; capSpill would otherwise
                        // fire on a load we are about to re-trim anyway).
                        if ((loaded >= budget && ts != lastTs)
                                || liveIndex.size() >= INDEX_MAX) {
                            return ts;
                        }
                        indexInsert(chunkBuf, off, klen, ts);
                        loaded++;
                        lastTs = ts;
                        off += klen + vlen; // skip the (sentinel) value payload
                    }
                    rc =
                            linker.frsVecIterRangeNext(
                                    handle, chunkBuf, chunkCap, outRowCount, outBytesUsed);
                    if (rc != 0) {
                        throw new RuntimeException("frs_vec_iter_range_next rc=" + rc);
                    }
                    rowCount = outRowCount.get(ValueLayout.JAVA_INT, 0);
                }
            } finally {
                linker.frsVecIterRangeClose(handle);
            }
        }
        return Long.MAX_VALUE;
    }

    /**
     * Build a composite lower-bound key {@code queuePrefix || kg(2B BE) || flipped_ts(8B BE)}
     * (no element tail) for {@code (kg, ts)}. Every real timer at {@code (kg, ts)} is
     * {@code seekKey || element}, which sorts at/after this key; every timer at {@code ts' > ts}
     * sorts after it (flipped-ts is ascending). So a range scan from this key yields exactly
     * the timers with {@code ts >= ts} in this kg, skipping earlier ones via the index.
     */
    private byte[] buildSeekKey(int kg, long ts) {
        long flipped = ts ^ SIGN_FLIP;
        byte[] out = new byte[queuePrefix.length + 2 + 8];
        System.arraycopy(queuePrefix, 0, out, 0, queuePrefix.length);
        out[queuePrefix.length] = (byte) ((kg >>> 8) & 0xFF);
        out[queuePrefix.length + 1] = (byte) (kg & 0xFF);
        for (int i = 0; i < 8; i++) {
            out[queuePrefix.length + 2 + i] = (byte) ((flipped >>> (56 - 8 * i)) & 0xFF);
        }
        return out;
    }

    /**
     * Smallest key strictly greater than EVERY key having {@code prefix} as a
     * prefix — i.e. the exclusive upper bound that scopes a range scan to one
     * key group. Increments the last byte {@code < 0xFF}, dropping trailing
     * 0xFF bytes. Returns {@code null} only if every byte is 0xFF (unbounded);
     * key-group prefixes never hit that (kg ≤ maxParallelism-1 ≤ 0x7FFF).
     */
    private static byte[] prefixUpperBound(byte[] prefix) {
        int i = prefix.length - 1;
        while (i >= 0 && (prefix[i] & 0xFF) == 0xFF) {
            i--;
        }
        if (i < 0) {
            return null;
        }
        byte[] out = new byte[i + 1];
        System.arraycopy(prefix, 0, out, 0, i + 1);
        out[i] = (byte) ((out[i] & 0xFF) + 1);
        return out;
    }

    // ------------------------------------------------------------------
    // KeyGroupedInternalPriorityQueue API
    // ------------------------------------------------------------------

    /** TEMP-DIAG: per-task timer counter tag from the task thread name. */
    private static String diagTag(String op) {
        String tn = Thread.currentThread().getName();
        if (tn.contains("GlobalWindowAggregate[7]")) {
            return "g7" + op;
        }
        if (tn.contains("GlobalWindowAggregate[14]")) {
            return "g14" + op;
        }
        if (tn.contains("WindowJoin")) {
            return "jn" + op;
        }
        return "ot" + op;
    }

    /**
     * Buffered add — composes the composite key off-heap, inserts it into the memory-resident
     * index (dedup-safe), then either cancels a matching pending REMOVE, no-ops on a matching
     * pending ADD, or stages a fresh engine ADD. Triggers an engine flush when the staging buffer
     * reaches {@link #FLUSH_THRESHOLD}.
     */
    @Override
    public boolean add(T element) {
        if (org.apache.flink.state.forstrs.DiagCompletionCounters.ENABLED) {
            org.apache.flink.state.forstrs.DiagCompletionCounters.named(diagTag("Add"));
        }
        // R38-H2: reject mutations on a closed queue. Without this gate a
        // late timer callback could call encodeIntoScratch (which writes via
        // MemorySegment.copy on the scratch arena) AFTER {@link #close()}
        // closed the arena, triggering an FFM IllegalStateException deep
        // inside the runtime. Fail loud here so the lifecycle bug surfaces
        // at the actual call site.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        int kg = currentKeyGroupSupplier.getAsInt();
        long ts = timestampExtractor.applyAsLong(element);
        int keyLen = encodeIntoScratch(kg, element);

        // TIMER-INDEX: the index is the authoritative read view — insert synchronously so
        // peek()/poll() observe this timer with zero engine traffic. Dedupe + spill-horizon
        // handling live inside indexInsert.
        indexInsert(scratchSeg, 0L, keyLen, ts);

        int existing = pendingBuffer.find(scratchSeg, 0L, keyLen);
        if (existing >= 0) {
            int op = pendingBuffer.opAt(existing);
            if (op == ArrowTimerBuffer.OP_ADD) {
                // Idempotent — already pending. No-op (the index insert above deduped too).
                return true;
            }
            // op == REMOVE.
            //
            // R0E-H3: the pre-fix code did `pendingBuffer.removeAt(existing)`
            // and then `insertAdd`. That trace correctly produced
            // {ADD} for the (absent engine + REMOVE + ADD) case but
            // SILENTLY LEAKED engine state for the (engine has X +
            // REMOVE + ADD + REMOVE) sequence: the second REMOVE in
            // step 5 found a pending ADD (created by the cancellation
            // in step 4), canceled it, and left an empty buffer — but
            // the engine still held X from before the first REMOVE,
            // so the user's last-op REMOVE was lost on flush.
            //
            // The buffer encodes one op per composite key and cannot
            // distinguish "ADD over a prior REMOVE" from "fresh ADD",
            // so we cannot defer the resolution to a follow-up
            // remove(). Force an immediate drain of the REMOVE to
            // engine before inserting the new ADD; the buffer becomes
            // empty, then insertAdd lands a clean ADD that subsequent
            // remove() correctly cancels (since engine has been
            // updated by our drain). The drain cost is one vectorized
            // FFI call, paid only on the rare REMOVE→ADD transition
            // path.
            flushPendingToEngine();
            // After flush, buffer is empty — fall through to insertAdd.
        }
        pendingBuffer.insertAdd(scratchSeg, 0L, keyLen, ts);
        if (pendingBuffer.size() >= FLUSH_THRESHOLD) {
            flushPendingToEngine();
        }
        return true;
    }

    /**
     * Buffered remove — composes the composite key off-heap, removes it from the memory-resident
     * index, then stages the engine delete: if a matching pending ADD exists, cancels it (no FFM);
     * else if a matching pending REMOVE exists, no-op; else inserts a REMOVE op into the pending
     * buffer.
     *
     * <p>Returns whether the timer was actually live: the index is authoritative below the spill
     * horizon, so presence-in-index = liveness. In the rare spilled regime a not-in-index timer
     * at/above the horizon may still be live engine-side — reported {@code true} conservatively
     * (the legacy approximation, now confined to that band).
     */
    @Override
    public boolean remove(T element) {
        // R38-H2: see add() — reject mutations after close().
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        int kg = currentKeyGroupSupplier.getAsInt();
        long ts = timestampExtractor.applyAsLong(element);
        int keyLen = encodeIntoScratch(kg, element);

        // TIMER-INDEX: synchronous index delete — peek()/poll() never see a removed timer, so
        // no pending-REMOVE masking (isRemovePendingFor) is needed on the read path anymore.
        boolean wasLive = indexRemoveFromScratch(keyLen);
        if (!wasLive && spillHorizon != Long.MAX_VALUE && ts >= spillHorizon) {
            // Spilled band — liveness unknown without an engine read; report true and let the
            // staged engine delete settle it (idempotent either way).
            wasLive = true;
        }

        int existing = pendingBuffer.find(scratchSeg, 0L, keyLen);
        if (existing >= 0) {
            int op = pendingBuffer.opAt(existing);
            if (op == ArrowTimerBuffer.OP_ADD) {
                // CANCEL — pure in-buffer cancellation, NEVER reaches engine.
                pendingBuffer.removeAt(existing);
                return true;
            }
            // op == REMOVE — already pending. No-op (idempotent).
            return wasLive;
        }
        // No pending entry. Insert a REMOVE op. The engine may or may not have the key — the
        // flush will issue a vectorized delete regardless (idempotent on the engine side).
        pendingBuffer.insertRemove(scratchSeg, 0L, keyLen, ts);
        if (pendingBuffer.size() >= FLUSH_THRESHOLD) {
            flushPendingToEngine();
        }
        return wasLive;
    }

    /**
     * Serves the GLOBAL minimum-ts timer across every key group in this subtask's range straight
     * from the memory-resident index — pure memory, ZERO engine reads (E-H2 contract preserved:
     * Flink's InternalTimerServiceImpl.advanceWatermark calls poll() BEFORE setCurrentKey(), and
     * the index is ts-ordered across the whole kg range by construction). The single-kg and
     * multi-kg cases share this implementation now.
     *
     * <p>The fired timer's engine delete is staged on the batched {@link #pendingPollDeletes}
     * path (one vectorized FFM crossing per batch) — unless the timer was never flushed to the
     * engine at all, in which case its pending ADD is cancelled in-buffer.
     */
    @Override
    public T poll() {
        if (org.apache.flink.state.forstrs.DiagCompletionCounters.ENABLED) {
            org.apache.flink.state.forstrs.DiagCompletionCounters.named(diagTag("Poll"));
        }
        // R38-H2: reject post-close access (FFM arenas are gone after close).
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        if (TIMER_DIAG && (++diagPollCount % 100_000L) == 0L) {
            System.err.println(
                    "[FRS_TIMER_DIAG] polls=" + diagPollCount
                            + " indexSize=" + liveIndex.size()
                            + " spillHorizon=" + spillHorizon
                            + " delFlushes=" + diagDeleteFlushCount
                            + " delKeys=" + diagDeleteKeyCount);
        }
        if (liveIndex.size() == 0) {
            maybeAdvanceSpillHorizon();
            if (liveIndex.size() == 0) {
                return null;
            }
        }
        // M5-V1 (hot-path alloc/copy audit 2026-06-12): the head's composite bytes already live
        // in liveIndex.keyDataSegment() — serve the memo compare AND the pendingBuffer probe
        // segment-direct. The old shape allocated a fresh byte[] copy per poll PLUS copied it
        // back into scratchSeg just to call find(); both copies (and, on the memo-HIT and
        // pending-ADD-cancel branches, the alloc itself) are gone. The composite is materialized
        // ONLY for the memo-miss decode and the pendingPollDeletes staging branch — the two
        // places that genuinely need owned bytes.
        int kOff = liveIndex.keyOffsetAt(0);
        int kLen = liveIndex.keyLenAt(0);
        T elem;
        byte[] owned; // owned composite bytes, or null until a branch needs them
        if (headMatchesMemo(kOff, kLen)) {
            elem = peekMemoElement;
            // peekMemoKey IS the head composite (exact bytes, owned, never mutated in place) —
            // reusable for the staging branch below without re-copying.
            owned = peekMemoKey;
        } else {
            owned = copyIndexKey(0);
            elem = decodeAndMemoize(owned);
        }
        // Probe the pending buffer segment-direct BEFORE removeAt(0): index mutation invalidates
        // (kOff, kLen). find() only reads pendingBuffer, removeAt(0) only mutates liveIndex, so
        // hoisting the probe above the removal is order-equivalent.
        int pending = pendingBuffer.find(liveIndex.keyDataSegment(), kOff, kLen);
        liveIndex.removeAt(0);
        // Engine-side delete. If the timer is still an UNFLUSHED pending ADD, cancel it
        // in-buffer — the engine never saw it, and staging a delete would race the later ADD
        // flush (delete-then-add would resurrect... actually add-after-delete: the orphan row
        // would re-appear). Otherwise stage the batched delete.
        if (pending >= 0 && pendingBuffer.opAt(pending) == ArrowTimerBuffer.OP_ADD) {
            pendingBuffer.removeAt(pending);
        } else if (pending < 0) {
            pendingPollDeletes.add(owned);
            if (pendingPollDeletes.size() >= FLUSH_THRESHOLD) {
                flushPollDeletes();
            }
        }
        // else: a pending REMOVE is already staged for this composite (defensive — remove()
        // also removes from the index synchronously, so this should be unreachable); the
        // staged REMOVE already deletes the engine row, nothing more to do.
        return elem;
    }

    /**
     * Returns (without removing) the GLOBAL minimum-ts timer across the kg range — index head,
     * pure memory, zero FFM. Flink calls this on every timer register and watermark advance.
     */
    @Override
    public T peek() {
        // R38-H2: reject post-close access (pendingBuffer/index arenas are closed).
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        if (liveIndex.size() == 0) {
            maybeAdvanceSpillHorizon();
            if (liveIndex.size() == 0) {
                return null;
            }
        }
        // M5-V1: memo-compare segment-direct BEFORE allocating — the common case (repeated peeks
        // of the same head) is now alloc- and copy-free; the byte[] composite is materialized
        // only on memo miss (head changed). Pure-read path: no index mutation between the
        // offset reads and the compare.
        int kOff = liveIndex.keyOffsetAt(0);
        int kLen = liveIndex.keyLenAt(0);
        if (headMatchesMemo(kOff, kLen)) {
            return peekMemoElement;
        }
        return decodeAndMemoize(copyIndexKey(0));
    }

    @Override
    public boolean isEmpty() {
        // R38-H2: reject post-close access.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        if (liveIndex.size() > 0) {
            return false;
        }
        // The index holds every live timer below the spill horizon; if a horizon is set the
        // engine may still hold spilled timers — advance to find out.
        maybeAdvanceSpillHorizon();
        return liveIndex.size() == 0;
    }

    @Override
    public int size() {
        // R39-H1: reject post-close access.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        if (spillHorizon == Long.MAX_VALUE) {
            // Common case: the index IS the live set — exact, zero engine reads.
            return liveIndex.size();
        }
        // Spilled: count engine rows (after a flush the engine row set == the live set:
        // pending ADDs/REMOVEs and fired-timer deletes have all landed).
        flushPendingToEngine();
        int n = 0;
        for (int kg = keyGroupRange.getStartKeyGroup();
                kg <= keyGroupRange.getEndKeyGroup();
                kg++) {
            n += sizeForKeyGroup(kg);
        }
        return n;
    }

    @Override
    public void addAll(Collection<? extends T> toAdd) {
        // R39-H1: reject post-close access. Delegates to add(); we gate at
        // the bulk entry point too so a null-vs-closed mix-up (toAdd == null
        // returning silently while a real call would throw) is impossible.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        if (toAdd == null) {
            return;
        }
        for (T t : toAdd) {
            add(t);
        }
    }

    @Override
    public CloseableIterator<T> iterator() {
        // R39-H1: reject post-close access. iterator() flushes then opens a
        // multi-kg iterator — both engine-touching paths require the arena
        // to still be alive.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        // After a flush the engine row set == the live set (correct superset semantics even
        // with a spill horizon), so the engine scan enumerates exactly the live timers.
        flushPendingToEngine();
        return new MultiKeyGroupIterator();
    }

    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroup) {
        // R39-H1: reject post-close access. Opens a prefix iterator on the
        // engine — fails opaquely against the closed arena otherwise.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        flushPendingToEngine();
        Set<T> out = new LinkedHashSet<>();
        // E-H8: per-call confined arena (same rationale as E-H6 fix).
        // The constructor-time shared arena cannot deallocate the iter
        // handle's 8 bytes, so repeated iterator opens against it
        // permanently leak memory.
        try (Arena perCallArena = Arena.ofConfined();
                FrsIterator iter =
                        linker.prefixLookupOpen(
                                db, cf, keyGroupPrefix(keyGroup), perCallArena)) {
            while (true) {
                ForStRsLinker.IteratorEntry e = linker.iteratorNext(iter);
                if (e == null) {
                    break;
                }
                out.add(decodeElement(e.key()));
            }
        }
        return out;
    }

    // ------------------------------------------------------------------
    // Bulk helpers — not part of the interface but useful to operators
    // ------------------------------------------------------------------

    /**
     * Removes every element in {@code toRemove} from the queue under the current key group. Returns
     * the count of elements actually deleted. Goes through the buffered cancellation path.
     */
    public int removeAll(Collection<? extends T> toRemove) {
        // R39-H1: reject post-close access. removeAll() delegates to
        // remove() + a trailing flushPendingToEngine; gate at the bulk
        // entry point so the empty-collection fast path can't bypass.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        if (toRemove == null || toRemove.isEmpty()) {
            return 0;
        }
        int removed = 0;
        for (T t : toRemove) {
            if (remove(t)) {
                removed++;
            }
        }
        // Flush so the count reflects post-engine state. We can't perfectly track which deletes
        // actually deleted something without an engine probe, so we approximate by the
        // call-count (matches legacy semantics: callers only need a non-negative best-effort).
        flushPendingToEngine();
        return removed;
    }

    // ------------------------------------------------------------------
    // Public batched advance — spec §"advance() strict order"
    // ------------------------------------------------------------------

    /**
     * Batched advance: drain pending buffer, then issue ONE prefix scan + ONE batch delete for
     * the timers due at or before {@code maxTimestamp} in the current key group. Returns the
     * number of timer entries returned to the visitor.
     *
     * <p>Strict ordering: (1) flush → (2) batch scan → (3) batch delete. Never overlaps. Spec
     * invariant #3. Fired timers are also removed from the memory-resident index so the read
     * path stays consistent with the engine delete.
     */
    public int advance(long maxTimestamp, java.util.function.Consumer<T> visitor) {
        // R38-H2: reject post-close access. advance() flushes then opens a
        // prefix iterator on the engine — every step requires the arena to
        // still be alive.
        if (closed.get()) {
            throw new IllegalStateException("timer queue closed");
        }
        // STEP 1 — flush pending mutations so the engine view is consistent.
        flushPendingToEngine();
        int kg = currentKeyGroupSupplier.getAsInt();
        // STEP 2 — open ONE prefix iterator on the engine kg-prefix. Single FFM crossing.
        // E-H8: per-call confined arena to reclaim the iter handle.
        java.util.ArrayList<byte[]> dueKeyList = new java.util.ArrayList<>();
        try (Arena perCallArena = Arena.ofConfined();
                FrsIterator iter =
                        linker.prefixLookupOpen(db, cf, keyGroupPrefix(kg), perCallArena)) {
            while (true) {
                ForStRsLinker.IteratorEntry e = linker.iteratorNext(iter);
                if (e == null) {
                    break;
                }
                long ts = decodeTimestamp(e.key());
                if (ts > maxTimestamp) {
                    // Keys are scanned in ascending-ts order (sign-flipped BE). Stop early.
                    break;
                }
                T element = decodeElement(e.key());
                visitor.accept(element);
                dueKeyList.add(e.key());
            }
        }
        if (dueKeyList.isEmpty()) {
            return 0;
        }
        // STEP 3 — single FFM batch delete + matching index deletes (fired timers must leave
        // the authoritative read view too; spilled rows simply miss the index lookup).
        byte[][] dueKeys = dueKeyList.toArray(new byte[0][]);
        vectorizedBatchDeleteKeys(dueKeys, dueKeys.length);
        for (byte[] k : dueKeys) {
            indexRemoveByComposite(k);
        }
        return dueKeys.length;
    }

    /**
     * Flushes the pending buffer to the engine: splits entries into ADD batch and REMOVE batch,
     * issuing one vectorized FFM call each. Clears the buffer at end.
     *
     * <p>Spec invariant #4 — must be called BEFORE any engine snapshot is captured (the keyed
     * backend's {@code snapshot()} drives this hook).
     */
    public void flushPendingToEngine() {
        // E4-R3-H3: gate on closed.get(). Every other public entry (add, remove,
        // poll, peek) already guards; the snapshot pre-hook (PHASE 1.e in
        // ForStRsAsyncKeyedStateBackend.snapshot) iterates registeredTimerQueues
        // unconditionally. Without this gate, a race between dispose and the
        // mailbox-thread snapshot can walk a closed flushArena and trigger FFM
        // IllegalStateException mid-snapshot — same UAF class as R38-H2 / R39-H1
        // but at the snapshot entry point.
        //
        // A-C4R4-H1: dispose-time drain {@link #close()} bypasses this gate via
        // {@link #drainPendingBufferInternal()} so the CAS-winner can flush
        // ADD/REMOVE entries that arrived between the prior snapshot and
        // close. Without the bypass the gate would silently swallow the
        // dispose drain.
        if (closed.get()) {
            return;
        }
        drainPendingBufferInternal();
    }

    /**
     * Gate-bypassing implementation shared by {@link #flushPendingToEngine()} and {@link
     * #close()}. The public entry checks {@code closed.get()} before delegating; {@code close()}
     * calls this directly to drain pending entries before flipping the {@code closed} flag.
     *
     * <p>Throw-safety (E-R23-H1, simplified by the memory-resident index): the read path never
     * consults the engine, so a mid-flush throw needs NO cache invalidation anymore. The pass
     * structure below (REMOVEs first, applied rows dropped from the buffer pass-atomically)
     * guarantees that on any throw the buffer still holds exactly the un-applied ops, so a retry
     * — or the pre-snapshot drain — re-issues them without loss or duplication.
     */
    private void drainPendingBufferInternal() {
        // FRS-TIMER-DRAIN-BATCH: persist staged fired-timer deletes before snapshot/close (this is
        // the mandatory pre-snapshot hook) — BEFORE the early-return, since pendingPollDeletes may
        // be non-empty even when the ADD/REMOVE pendingBuffer is empty.
        flushPollDeletes();
        int n = pendingBuffer.size();
        if (n == 0) {
            return;
        }
        // First pass: count adds + removes; size the staging segments.
        int addCount = 0;
        int delCount = 0;
        long totalDelBytes = 0L;
        for (int i = 0; i < n; i++) {
            int op = pendingBuffer.opAt(i);
            if (op == ArrowTimerBuffer.OP_ADD) {
                addCount++;
            } else if (op == ArrowTimerBuffer.OP_REMOVE) {
                delCount++;
                totalDelBytes += pendingBuffer.keyLenAt(i);
            }
        }
        ensureFlushPairCapacity(Math.max(addCount, delCount));

        // R38-H3: apply REMOVEs BEFORE ADDs so a mid-flush failure leaves
        // engine state ⊆ logical state (recoverable on retry — the
        // surviving ADDs are still pending in the buffer, so the next
        // flush re-applies them; in-buffer ADD/REMOVE cancellation in
        // {@link #add}/{@link #remove} ensures the buffer never carries a
        // contradictory pair so this ordering is safe).
        //
        // The original order (ADDs then DELETEs) allowed a DELETE-throw
        // mid-flush to leave engine ADDs orphaned while {@link
        // #pendingBuffer#clear} below was skipped, so the next retry
        // re-applied the ADDs but the originally-paired DELETEs were lost
        // from logical view. A pre-snapshot drain on that failure path
        // would have snapshotted ADDs the user logically removed.
        //
        // It does NOT break the timer-ordering invariant: timers are
        // ordered by ts on the engine side, not by the order in which
        // flush operations are issued.

        // Pass A — REMOVEs first (R38-H3).
        boolean removesApplied = false;
        if (delCount > 0) {
            ensureFlushDelDataCapacity(totalDelBytes);
            int outIdx = 0;
            long pos = 0;
            flushDelOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
            for (int i = 0; i < n; i++) {
                if (pendingBuffer.opAt(i) != ArrowTimerBuffer.OP_REMOVE) {
                    continue;
                }
                int kOff = pendingBuffer.keyOffsetAt(i);
                int kLen = pendingBuffer.keyLenAt(i);
                MemorySegment.copy(
                        pendingBuffer.keyDataSegment(),
                        kOff,
                        flushDelData,
                        pos,
                        kLen);
                pos += kLen;
                outIdx++;
                flushDelOffsets.set(
                        ValueLayout.JAVA_INT, (long) outIdx * Integer.BYTES, (int) pos);
            }
            linker.vectorizedBatchDelete(db, cf, flushDelOffsets, flushDelData, outIdx);
            removesApplied = true;
        } else {
            // No REMOVEs to apply — treat pass A as trivially succeeded so
            // the post-pass-A REMOVE-drop loop runs no rows but the flag
            // semantics stay simple for pass B.
            removesApplied = true;
        }

        // R39-M2: after pass A succeeds, drop the REMOVE rows from the
        // pending buffer so a subsequent ADD-throw in pass B does NOT
        // leave the buffer holding applied REMOVEs alongside un-flushed
        // ADDs (the prior shape risked a wedged buffer that never
        // shrank, because the trailing clear() was bypassed by the
        // throw and the next retry would re-issue idempotent REMOVEs
        // without ever cleaning them out). REMOVEs are idempotent on
        // the engine side so re-issue would have been safe; the wedge
        // here was about buffer-occupancy unboundedness, not engine
        // correctness. Walking high-to-low keeps removeAt indices
        // valid (removeAt shifts higher positions down).
        if (removesApplied && delCount > 0) {
            for (int i = n - 1; i >= 0; i--) {
                if (pendingBuffer.opAt(i) == ArrowTimerBuffer.OP_REMOVE) {
                    pendingBuffer.removeAt(i);
                }
            }
        }

        // Pass B — ADDs after REMOVEs have succeeded. After this call
        // returns successfully the remaining ADD rows are dropped; if it
        // throws the buffer still holds the un-applied ADDs and a retry
        // can reissue them (the dropped REMOVE rows from pass A are
        // already on the engine so the retry's pass-A no-ops harmlessly).
        if (addCount > 0) {
            // B-R5-NEW-H1: vectorized FFI path. Pre-fix the ADD pass
            // allocated `byte[addCount][]` outer arrays plus a
            // per-row `byte[kLen]` heap copy, then dispatched via the
            // legacy `linker.batchPut(byte[][], byte[][])` shape — every
            // entry crossed FFM as a separate pointer-array materialised
            // on the native side. For Q12-style workloads this drove
            // millions of `byte[]` allocations per second on the timer
            // hot path. Switched to `vectorizedBatchPut` (single FFM
            // crossing, off-heap packed key/val data) mirroring Pass A's
            // `vectorizedBatchDelete` route. Vals are the fixed 0x01
            // sentinel, pre-filled in `ensureFlushPairCapacity` so the
            // hot path only writes the key side.
            long totalAddBytes = 0L;
            int remaining = pendingBuffer.size();
            for (int i = 0; i < remaining; i++) {
                if (pendingBuffer.opAt(i) != ArrowTimerBuffer.OP_ADD) {
                    continue;
                }
                totalAddBytes += pendingBuffer.keyLenAt(i);
            }
            ensureFlushAddKeyDataCapacity(totalAddBytes);
            MemorySegment keyDataSeg = pendingBuffer.keyDataSegment();
            flushAddKeyOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
            long pos = 0L;
            int outIdx = 0;
            for (int i = 0; i < remaining; i++) {
                if (pendingBuffer.opAt(i) != ArrowTimerBuffer.OP_ADD) {
                    continue;
                }
                int kOff = pendingBuffer.keyOffsetAt(i);
                int kLen = pendingBuffer.keyLenAt(i);
                MemorySegment.copy(keyDataSeg, kOff, flushAddKeyData, pos, kLen);
                pos += kLen;
                outIdx++;
                flushAddKeyOffsets.set(
                        ValueLayout.JAVA_INT, (long) outIdx * Integer.BYTES, (int) pos);
            }
            // E-R13-H3: tie the in-buffer ADD-drop atomically to batchPut
            // success. Pre-fix the ADD rows lived in the pendingBuffer
            // until the trailing `clear()`; if anything interleaved
            // between batchPut's return and that clear() (an unchecked
            // Throwable, or a re-entrant listener), the ADDs were durable
            // on the engine AND still in the buffer. A subsequent remove()
            // observing the in-buffer ADD then took the "cancel pending
            // ADD" no-op branch and silently failed to issue the engine
            // REMOVE, leaving an orphan engine row — same leak class as
            // R0E-H3.
            boolean putSucceeded = false;
            try {
                linker.vectorizedBatchPut(
                        db,
                        cf,
                        flushAddKeyOffsets,
                        flushAddKeyData,
                        flushAddValOffsets,
                        flushAddValData,
                        outIdx);
                putSucceeded = true;
            } finally {
                if (putSucceeded) {
                    for (int i = pendingBuffer.size() - 1; i >= 0; i--) {
                        if (pendingBuffer.opAt(i) == ArrowTimerBuffer.OP_ADD) {
                            pendingBuffer.removeAt(i);
                        }
                    }
                }
            }
        }
        // Pass B succeeded — clear any residue (a no-op when the ADD
        // drop loop above already emptied the buffer; defensive against
        // future code that leaves non-ADD non-REMOVE rows).
        pendingBuffer.clear();
    }

    private void ensureFlushPairCapacity(int neededRows) {
        if (neededRows <= flushPairCapacity) {
            return;
        }
        long newCap = Math.max(flushPairCapacity == 0 ? 256 : flushPairCapacity * 2, neededRows);
        // B-R5-NEW-H1: Arrow-style offsets + packed val data for the
        // vectorized ADD FFI shape, mirroring the DEL side.
        flushAddKeyOffsets = flushArena.allocate((newCap + 1) * Integer.BYTES);
        flushAddValOffsets = flushArena.allocate((newCap + 1) * Integer.BYTES);
        flushAddValData = flushArena.allocate(newCap);
        // Each timer's value is a fixed 0x01 sentinel; pre-fill valData
        // and valOffsets with the constant pattern [0,1,2,...] so the
        // flush hot path only writes the key side.
        flushAddValData.fill((byte) 1);
        for (long i = 0; i <= newCap; i++) {
            flushAddValOffsets.set(ValueLayout.JAVA_INT, i * Integer.BYTES, (int) i);
        }
        flushDelOffsets = flushArena.allocate((newCap + 1) * Integer.BYTES);
        flushPairCapacity = newCap;
    }

    private void ensureFlushAddKeyDataCapacity(long neededBytes) {
        if (neededBytes <= flushAddKeyDataCapacity) {
            return;
        }
        long newCap = Math.max(
                flushAddKeyDataCapacity == 0 ? 4096 : flushAddKeyDataCapacity * 2,
                neededBytes);
        flushAddKeyData = flushArena.allocate(newCap);
        flushAddKeyDataCapacity = newCap;
    }

    private void ensureFlushDelDataCapacity(long neededBytes) {
        if (neededBytes <= flushDelDataCapacity) {
            return;
        }
        long newCap = Math.max(flushDelDataCapacity == 0 ? 4096 : flushDelDataCapacity * 2, neededBytes);
        flushDelData = flushArena.allocate(newCap);
        flushDelDataCapacity = newCap;
    }

    /**
     * FRS-TIMER-DRAIN-BATCH: flush all staged fired-timer composites in ONE batched
     * {@link ForStRsLinker#vectorizedBatchDelete} crossing. Idempotent / no-op when empty, so it is
     * safe to call from every engine-scan / snapshot entry point. MUST run before any engine
     * re-scan or snapshot so a deferred delete never resurrects a fired timer.
     */
    private void flushPollDeletes() {
        int n = pendingPollDeletes.size();
        if (n == 0) {
            return;
        }
        vectorizedBatchDeleteKeys(pendingPollDeletes.toArray(new byte[0][]), n);
        pendingPollDeletes.clear();
        if (TIMER_DIAG) {
            diagDeleteFlushCount++;
            diagDeleteKeyCount += n;
        }
    }

    /**
     * Issues a single vectorized batch delete for {@code count} keys via the linker. Stages keys
     * into the pre-allocated del staging segments.
     */
    private void vectorizedBatchDeleteKeys(byte[][] keys, int count) {
        if (count <= 0) {
            return;
        }
        long totalBytes = 0L;
        for (int i = 0; i < count; i++) {
            totalBytes += keys[i].length;
        }
        ensureFlushPairCapacity(count);
        ensureFlushDelDataCapacity(totalBytes);
        flushDelOffsets.set(ValueLayout.JAVA_INT, 0L, 0);
        long pos = 0L;
        for (int i = 0; i < count; i++) {
            byte[] k = keys[i];
            MemorySegment.copy(k, 0, flushDelData, ValueLayout.JAVA_BYTE, pos, k.length);
            pos += k.length;
            flushDelOffsets.set(
                    ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES, (int) pos);
        }
        linker.vectorizedBatchDelete(db, cf, flushDelOffsets, flushDelData, count);
    }

    // ------------------------------------------------------------------
    // Close — flush and release off-heap resources
    // ------------------------------------------------------------------

    @Override
    public void close() {
        close(false);
    }

    /**
     * E5-R5-H1: skip-drain variant. When the dispose path detects {@code awaitOutstandingSnapshots}
     * timed out (snapshot worker still in FFI), the timer-queue drain via
     * {@link #drainPendingBufferInternal()} would race the stuck worker's db/cf access — a real
     * UAF the V1-sync backend already guards via its {@code if (snapshotsTimedOut) continue;}
     * skip at {@code ForStRsAbstractKeyedStateBackend.java:1240}. {@code skipDrain=true} replays
     * the pre-A-C4R4-H1 behavior (release arenas only, drop any pending ADDs/REMOVEs that arrived
     * after the prior snapshot — same data-loss surface the original gate-blocked drain produced).
     */
    public void close(boolean skipDrain) {
        // R29-M3: idempotent close. PHASE 1.e of dispose already drained the
        // pending buffer via {@link #flushPendingToEngine()}; the symmetric
        // registry close() loop must NOT re-drain (extra FFI call) and must NOT
        // throw on the second invocation (which would mask the real dispose
        // cause). On second+ entry we skip the drain and proceed directly to
        // arena/buffer release (those releases are themselves idempotent at the
        // {@link Arena#close()} layer).
        //
        // A-C4R4-H1: the dispose drain MUST bypass the E4-R3-H3 gate in
        // {@link #flushPendingToEngine()}. The CAS-winner gets the single
        // dispose-time chance to flush ADD/REMOVE entries sitting in
        // `pendingBuffer`; the gate (which exists to guard a concurrent
        // snapshot+close UAF) would silently swallow that drain and the
        // entries would be lost. We CAS first (claims ownership of the
        // single drain + arena release), then call the gate-bypassing
        // {@link #drainPendingBufferInternal()} directly. Concurrent
        // close() losers see firstClose=false and skip drain — same
        // semantics as the pre-fix CAS-based idempotent close.
        boolean firstClose = closed.compareAndSet(false, true);
        try {
            if (firstClose && !skipDrain) {
                drainPendingBufferInternal();
            }
        } finally {
            try {
                pendingBuffer.close();
            } catch (Exception ignored) {
            }
            try {
                liveIndex.close();
            } catch (Exception ignored) {
            }
            try {
                scratchArena.close();
            } catch (Exception ignored) {
            }
            try {
                flushArena.close();
            } catch (Exception ignored) {
            }
        }
    }

    // ------------------------------------------------------------------
    // Test/observability hooks
    // ------------------------------------------------------------------

    @VisibleForTesting
    public int pendingBufferSize() {
        return pendingBuffer.size();
    }

    /** TIMER-INDEX test seam: current live-index entry count. */
    @VisibleForTesting
    public int liveIndexSize() {
        return liveIndex.size();
    }

    /** TIMER-INDEX test seam: current spill horizon ({@link Long#MAX_VALUE} = no spill). */
    @VisibleForTesting
    public long spillHorizonForTesting() {
        return spillHorizon;
    }

    // ------------------------------------------------------------------
    // Implementation helpers
    // ------------------------------------------------------------------

    private int sizeForKeyGroup(int kg) {
        int n = 0;
        // E-H8: per-call confined arena to reclaim the iter handle.
        try (Arena perCallArena = Arena.ofConfined();
                FrsIterator iter =
                        linker.prefixLookupOpen(db, cf, keyGroupPrefix(kg), perCallArena)) {
            while (linker.iteratorNext(iter) != null) {
                n++;
            }
        }
        return n;
    }

    /** Iterator that walks every covered key group end-to-end, decoding elements lazily. */
    private final class MultiKeyGroupIterator implements CloseableIterator<T> {
        private int kg = keyGroupRange.getStartKeyGroup();
        private FrsIterator current;
        // E-H8: per-kg arena lives for one iter handle's lifetime. We
        // close it in `closeCurrent` so the handle's 8 bytes are
        // reclaimed before opening the next kg's iter, mirroring the
        // E-H6 pattern but adapted to a stateful iterator.
        private Arena currentArena;
        private T next;
        private boolean closed;

        MultiKeyGroupIterator() {
            advance();
        }

        private void advance() {
            while (!closed) {
                if (current == null) {
                    if (kg > keyGroupRange.getEndKeyGroup()) {
                        next = null;
                        return;
                    }
                    currentArena = Arena.ofConfined();
                    current = linker.prefixLookupOpen(
                            db, cf, keyGroupPrefix(kg), currentArena);
                }
                ForStRsLinker.IteratorEntry e = linker.iteratorNext(current);
                if (e == null) {
                    closeCurrent();
                    kg++;
                    continue;
                }
                next = decodeElement(e.key());
                return;
            }
            next = null;
        }

        private void closeCurrent() {
            if (current != null) {
                current.close();
                current = null;
            }
            if (currentArena != null) {
                currentArena.close();
                currentArena = null;
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public T next() {
            if (next == null) {
                throw new java.util.NoSuchElementException();
            }
            T out = next;
            advance();
            return out;
        }

        @Override
        public void close() {
            closed = true;
            closeCurrent();
        }
    }
}
