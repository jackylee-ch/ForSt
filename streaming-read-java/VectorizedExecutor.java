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

package org.apache.flink.state.forstrs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.state.forstrs.exec.FrsIterHandle;
import org.apache.flink.state.forstrs.exec.SlotArenaScope;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.ffm.FrsEnginePanicError;
import org.apache.flink.state.forstrs.ffm.FrsErrorCode;
import org.apache.flink.state.forstrs.ffm.FrsException;
import org.apache.flink.state.forstrs.metrics.DispatchMetrics;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Vectorized {@link StateExecutor} that dispatches an entire batch of state requests via a single
 * FFM call per op type, using caller-owned Arrow {@link ColumnarBatchBuffer}s. Replaces {@code
 * ForStRsStateExecutor}'s per-request {@code byte[]} + {@code byte[][]} allocations on the hot
 * path.
 *
 * <p>See spec {@code docs/superpowers/specs/2026-05-15-forst-rs-vectorized-executor-design.md} §C3.
 *
 * <p><b>D-R4-2 (JAVA_INT alignment audit).</b> Every {@link ValueLayout#JAVA_INT} site in this
 * class — {@code outOffsets}, {@code opsOffSeg}, {@code keysOffSeg}, {@code prefixesOff},
 * {@code lens}, {@code outRowCount}, {@code outBytesUsed} — is an arena-allocated segment
 * ({@code arena.allocate(JAVA_INT, ...)} or {@code scratch.allocate(JAVA_INT, ...)}) which the
 * FFM API guarantees to be at least 4-byte aligned. Indexed access uses either {@code (long) i *
 * Integer.BYTES} or {@code setAtIndex(JAVA_INT, i, ...)} — both always produce 4-byte-aligned
 * offsets. None of these segments is a heap-backed slice of a Rust-side struct with non-
 * multiple-of-4 fields, so {@code JAVA_INT_UNALIGNED} is not required. The unaligned variant is
 * reserved for cases like the {@code FrsBytes} struct that {@link
 * org.apache.flink.state.forstrs.ffm.ForStRsLinker} reads from a heap {@code byte[24]}; see the
 * {@code FRS_BYTES_LAYOUT_UNALIGNED} layout there.
 */
@Internal
public class VectorizedExecutor implements StateExecutor {

    // FRS-OPCOUNT-DISPATCH (2026-06-07): is q19/q9's batch collapsing to per-row sync
    // (executeBatchInOfferOrder) due to requiresOrderedDispatch? That path is the documented
    // 100×-1000× amplification. -Dforst.rs.opcount=1 enables; prints ordered vs vectorized
    // batch+row counts so we can see the ordered/vectorized ratio per query.
    private static final boolean OC_DISPATCH = "1".equals(System.getProperty("forst.rs.opcount"));
    private static final AtomicLong OC_ORDERED_BATCHES = new AtomicLong();
    private static final AtomicLong OC_ORDERED_ROWS = new AtomicLong();
    private static final AtomicLong OC_VEC_BATCHES = new AtomicLong();
    private static final AtomicLong OC_VEC_ROWS = new AtomicLong();
    private static final AtomicLong OC_DISPATCH_NEXT_DUMP = new AtomicLong(100_000L);
    // FRS-OPCOUNT-ITERPREFIX: per-iteration cost breakdown for q9/q19 (the queries that lose to
    // BOTH C++ engines). opens = #prefix opens; openNs = time in frsVecIterPrefixOpenBatch (FFI +
    // engine build); totalNs = whole dispatchIterPrefix (open + per-row handle/register/future).
    // Pinpoints the architectural target: if openNs dominates → engine/FFI; if (total-open)
    // dominates → the Java handle/future machinery.
    private static final AtomicLong OC_IP_OPENS = new AtomicLong();
    private static final AtomicLong OC_IP_OPEN_NS = new AtomicLong();
    private static final AtomicLong OC_IP_TOTAL_NS = new AtomicLong();
    private static final AtomicLong OC_IP_NEXT_DUMP = new AtomicLong(1_000_000L);

    private static void ocIterPrefixMaybeDump() {
        long opens = OC_IP_OPENS.get();
        long threshold = OC_IP_NEXT_DUMP.get();
        if (opens >= threshold
                && OC_IP_NEXT_DUMP.compareAndSet(threshold, threshold + 2_000_000L)) {
            long o = OC_IP_OPENS.get();
            long openMs = OC_IP_OPEN_NS.get() / 1_000_000L;
            long totMs = OC_IP_TOTAL_NS.get() / 1_000_000L;
            System.err.println(
                    "[FRS_OPCOUNT_ITERPREFIX] opens=" + o
                            + " openMs=" + openMs
                            + " totalMs=" + totMs
                            + " openNsPerOpen=" + (o > 0 ? OC_IP_OPEN_NS.get() / o : 0)
                            + " totalNsPerOpen=" + (o > 0 ? OC_IP_TOTAL_NS.get() / o : 0));
        }
    }

    private static void ocDispatchMaybeDump() {
        long total = OC_ORDERED_ROWS.get() + OC_VEC_ROWS.get();
        long threshold = OC_DISPATCH_NEXT_DUMP.get();
        if (total >= threshold
                && OC_DISPATCH_NEXT_DUMP.compareAndSet(threshold, threshold + 2_000_000L)) {
            System.err.println(
                    "[FRS_OPCOUNT_DISPATCH] orderedBatches=" + OC_ORDERED_BATCHES.get()
                            + " orderedRows=" + OC_ORDERED_ROWS.get()
                            + " vecBatches=" + OC_VEC_BATCHES.get()
                            + " vecRows=" + OC_VEC_ROWS.get());
        }
    }

    /** Initial output buffer capacity for GET values. Grows on BUFFER_TOO_SMALL. */
    private static final int INITIAL_OUT_DATA_CAP = 64 * 1024;

    private static final int FRS_STATUS_OK = 0;
    private static final int FRS_STATUS_BUFFER_TOO_SMALL = 17;

    /**
     * Stage-3 Unit-2 (two-regime design §3.4, gated per §4 Stage 3 / §8): env gate {@code
     * FRS_RS_MIXED_BATCH=true|false} for the MIXED write batch. When true, a classified batch's
     * puts + deletes + heap-path appendMerges against this executor's CF commit in ONE {@code
     * frs_vectorized_batch_mixed} crossing (one atomic engine sequence allocation) instead of up
     * to three separate crossings. DEFAULT FALSE until the Stage-3 gates pass (engine suite,
     * q20@100M, q9 re-run, q17/q3/q8 no-regress); the multi-call path stays the default and is
     * byte-for-byte untouched when the gate is off.
     */
    static final boolean MIXED_BATCH_ENV =
            Boolean.parseBoolean(System.getenv("FRS_RS_MIXED_BATCH"));

    /**
     * Per-executor effective gate. Initialized from {@link #MIXED_BATCH_ENV}; package-private
     * setter below lets unit tests exercise both paths in one JVM (env vars are immutable
     * in-process). Read once per batch at container creation — the per-batch mode is captured on
     * the classifier ({@link VectorizedClassifier#setMixedStaging}) so a mid-batch flip can never
     * split one batch across staging models.
     */
    private volatile boolean mixedBatchEnabled = MIXED_BATCH_ENV;

    /** Test seam: arms/disarms the mixed write batch for containers created AFTER the call. */
    void setMixedBatchEnabledForTests(boolean enabled) {
        this.mixedBatchEnabled = enabled;
    }

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final FrsCfHandle cf;
    private final Arena arena;

    /**
     * FRS-REUSE-CHUNKBUF (2026-06-09): per-executor REUSED iterator chunk buffer (CHUNK_BUF_CAP, 64 KiB),
     * lazily allocated ONCE from {@link #arena} and handed to every {@link ForStRsDBIterRequest#process}.
     * Eliminates the per-probe 64 KiB native allocation + zero-fill (the JFR-pinned {@code initNativeMemory}
     * hotspot on the q9/q7/q20 join drain — a per-record-alloc mandate violation). Single-threaded per
     * executor (the parallel RoutingStateExecutor gives each worker its own executor, hence its own buffer).
     */
    private MemorySegment reusableIterChunkBuf;

    // Optional metrics + fatal-handler — set via setters after construction so that
    // backends that don't yet have a MetricGroup can still instantiate the executor.
    private DispatchMetrics metrics;
    private FatalErrorHandler fatalHandler;

    // Optional SlotArenaScope + monotonic handle ID counter for ITER_PREFIX dispatch.
    // Set via setSlotScope() before any submitVectorized(IterPrefixRequest) calls.
    private SlotArenaScope slotScope;
    private final AtomicLong nextIterHandleId = new AtomicLong(0);

    /**
     * R29-M2: post-shutdown gate (mirrors {@link ForStRsStateExecutor}'s R28-M2 design).
     * {@link #shutdown()} previously was a no-op so the dispose chain that calls
     * {@code managedExecutors.forEach(VectorizedExecutor::shutdown)} did not actually
     * prevent in-flight {@link #executeBatchRequests}/{@link #executeRequestSync} calls
     * from touching the soon-to-be-closed slot {@link Arena}. Flip this flag in
     * {@link #shutdown()} and check at the entry points so racing virtual threads
     * (the async-state controller's batch dispatch + sync-savepoint sync path)
     * fail-fast with {@link IllegalStateException} instead of reading torn-down
     * memory.
     */
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    // Long-lived classifier-side buffers (reused across batches via reset()).
    private final ColumnarBatchBuffer getKeys;
    private final ColumnarBatchBuffer putKeys;
    private final ColumnarBatchBuffer putValues;
    private final ColumnarBatchBuffer deleteKeys;

    // V3.1 (V20 sub-spec §5): long-lived registry of ListState names. A fresh classifier is
    // created per batch by createRequestContainer(); the registry is passed in so APPEND_MERGE
    // routing persists across batches. Backend calls registerListState() once per state primitive
    // at creation time.
    private final java.util.Set<String> listStateNames =
            java.util.concurrent.ConcurrentHashMap.newKeySet();

    // Reusable output buffers for the GET path.
    private MemorySegment outOffsets;
    private MemorySegment outValidity;
    private MemorySegment outData;
    private long outDataCap;
    private int outSlotsCap;
    private final MemorySegment outDataLenSeg; // *mut usize scratch

    // B5-H7: scratch int[] for per-row ops offsets in dispatchAppendMergeBatch. Grown on
    // demand with the same exponential pattern as outSlotsCap; reused across batches.
    // Sized to count+1 entries.
    private int[] scratchOpsOffsets = new int[0];

    // B5-H7: scratch flat array for heap-path APPEND_MERGE futures so the per-row drain in
    // executeBatchRequests / executeRequestSyncInner avoids the per-row List.get() interface
    // dispatch. Grown on demand; only the first `count` entries are valid per dispatch.
    @SuppressWarnings("unchecked")
    private CompletableFuture<Void>[] scratchHeapFutures = (CompletableFuture<Void>[]) new CompletableFuture<?>[0];

    // R21-M2: largest valid prefix written into {@link #scratchHeapFutures} across batches.
    // Used by {@link #flattenHeapFutures} to null out trailing slots from a prior larger
    // dispatch so the JVM does not retain references to stale {@link CompletableFuture}s
    // (and the StateRequests they transitively pin). Reset to {@code n} on each call.
    private int scratchHeapFuturesPrevSize = 0;

    // B6-H4: scratch flat array of per-row first-operand MemorySegment for APPEND_MERGE batch
    // dispatch. For B6-H1 heap-path rows the slice is `null` (value lives in
    // {@link AppendMergeBatchBuffer#valueBuffer()} at the same index); otherwise the slice
    // came from a pre-built {@link AppendMergeRequest}. Lifted out of the per-row body so the
    // dispatchAppendMergeBatch INNER loop avoids two List.get(row) interface dispatches per row.
    private MemorySegment[] scratchValueSlices = new MemorySegment[0];

    // R21-M2: mirror of {@link #scratchHeapFuturesPrevSize} for {@link #scratchValueSlices}.
    // Trailing slots can pin {@link MemorySegment} views over native arena memory; clearing them
    // is needed to release those views for GC even though the underlying executor arena
    // outlives them.
    private int scratchValueSlicesPrevSize = 0;

    // A6-H3 / B6-H2 / D6-H1: persistent executor-arena scratch for dispatchIterRange.
    // The open call needs 8+4+4=16 bytes of out-params and the FrsIterHandle borrows
    // the executor arena later (ownsArena=false). Allocating these segments ONCE at
    // construction (rather than per-iter-open) eliminates monotonic executor-arena
    // growth on the iter-range path. Safe to reuse across calls because the open
    // FFI completes synchronously and the values are copied out before the handle
    // is registered.
    private final MemorySegment scratchIterRangeHandle;
    private final MemorySegment scratchIterRangeRowCount;
    private final MemorySegment scratchIterRangeBytesUsed;

    // A6-H3 / B6-H2 / D6-H1: persistent executor-arena scratch for dispatchIterPrefix.
    // Capacities are tracked per-segment and grown on demand via the
    // ensure*Capacity helpers. The FrsIterHandle borrows the executor arena
    // (ownsArena=false) so these segments must outlive the open call but are
    // safe to overwrite on the next dispatch (open is synchronous, the engine
    // copies handle/chunk metadata out before returning).
    private MemorySegment scratchPrefixesOff;
    private long scratchPrefixesOffCap; // in entries (u32)
    private MemorySegment scratchPrefixesData;
    private long scratchPrefixesDataCap; // in bytes
    private MemorySegment scratchOutHandles;
    private long scratchOutHandlesCap; // in entries (u64)
    private MemorySegment scratchOutChunks;
    private long scratchOutChunksCap; // in entries (FrsChunk stride)

    public VectorizedExecutor(ForStRsLinker linker, FrsDb db, FrsCfHandle cf, Arena arena) {
        this.linker = linker;
        this.db = db;
        this.cf = cf;
        this.arena = arena;
        this.getKeys = new ColumnarBatchBuffer(arena);
        this.putKeys = new ColumnarBatchBuffer(arena);
        this.putValues = new ColumnarBatchBuffer(arena);
        this.deleteKeys = new ColumnarBatchBuffer(arena);
        this.outSlotsCap = 4096;
        this.outOffsets = arena.allocate(ValueLayout.JAVA_INT, (long) outSlotsCap + 1);
        this.outValidity = arena.allocate(outSlotsCap);
        this.outDataCap = INITIAL_OUT_DATA_CAP;
        this.outData = arena.allocate(outDataCap);
        this.outDataLenSeg = arena.allocate(ValueLayout.JAVA_LONG);

        // A6-H3 / B6-H2 / D6-H1: pre-allocate per-iter-range out-param scratch (16 bytes
        // total) and start the iter-prefix scratch with a modest capacity that grows
        // on demand. All four iter-prefix segments resize together via the helpers
        // below; the executor-arena cost grows monotonically with the LARGEST batch
        // observed (not every batch), so 24h streaming jobs no longer leak ~MB/s.
        this.scratchIterRangeHandle = arena.allocate(ValueLayout.JAVA_LONG);
        this.scratchIterRangeRowCount = arena.allocate(ValueLayout.JAVA_INT);
        this.scratchIterRangeBytesUsed = arena.allocate(ValueLayout.JAVA_INT);
        this.scratchPrefixesOffCap = 0L;
        this.scratchPrefixesOff = MemorySegment.NULL;
        this.scratchPrefixesDataCap = 0L;
        this.scratchPrefixesData = MemorySegment.NULL;
        this.scratchOutHandlesCap = 0L;
        this.scratchOutHandles = MemorySegment.NULL;
        this.scratchOutChunksCap = 0L;
        this.scratchOutChunks = MemorySegment.NULL;
    }

    // -----------------------------------------------------------------
    // Optional wiring (P4)
    // -----------------------------------------------------------------

    /**
     * Attach dispatch metrics. Call immediately after construction; thread-safe for single-writer
     * scenarios (the backend thread that owns this executor).
     */
    public void setDispatchMetrics(DispatchMetrics m) {
        this.metrics = m;
    }

    /**
     * Attach a FatalErrorHandler for fail-process error escalation (PANIC_CAUGHT / UNKNOWN). If not
     * set, fail-process errors are still thrown as {@link FrsEnginePanicError} — Flink's default
     * uncaught-exception handler will catch them at the task level.
     */
    public void setFatalHandler(FatalErrorHandler fh) {
        this.fatalHandler = fh;
    }

    /**
     * Attach a {@link SlotArenaScope} for ITER_PREFIX dispatch. Must be set before any {@link
     * IterPrefixRequest} is dispatched; the scope is used to allocate per-iterator Arenas and
     * register handles for turn-boundary lifetime management.
     */
    public void setSlotScope(SlotArenaScope scope) {
        this.slotScope = scope;
    }

    @Override
    public AsyncRequestContainer<StateRequest<?, ?, ?, ?>> createRequestContainer() {
        // 2026-05-29 PERF-RESTORE-#0 (PROFILED HOT FRAME): jstack on q4 RUNNABLE
        // Join threads showed every batch ~50% time in
        //   Unsafe.allocateMemory0 ← Arena.allocate
        //   ← ColumnarBatchBuffer.<init> (line 73-74)
        //   ← VectorizedClassifier.initNewKindBuffers (line 197)
        //   ← VectorizedExecutor.createRequestContainer
        // because a NEW classifier + NEW AppendMergeBatchBuffer/ColumnarBatchBuffer
        // were allocated per batch. The big synchronous-FFI comment block below
        // explicitly documents that buffers can be safely SHARED — so pool a
        // single classifier at executor level and just reset() it per batch.
        // Saves one MemorySegment alloc + memset(0) per batch on the hot loop.
        // PR-1 (coordinated executor): a POOL of classifiers, each owning a PRIVATE buffer
        // quartet, so two outstanding containers never share fill-side state — the mailbox can
        // fill batch N+1 while batch N executes on a worker thread (the depth>1 blocker in the
        // PR-E2 comment below). Inline mode never calls releaseRequestContainer, so the pool
        // grows to exactly one entry there — identical allocation behavior to the prior single
        // pooledClassifier (PERF-RESTORE-#0 preserved). Under CoordinatedStateExecutor the pool
        // is bounded by AEC admission (fullyLoaded) — at most a few classifiers per executor.
        VectorizedClassifier classifier = classifierPool.pollFirst();
        if (classifier == null) {
            classifier =
                    new VectorizedClassifier(
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena),
                            new ColumnarBatchBuffer(arena));
            classifier.initNewKindBuffers(arena);
        }
        classifier.reset();
        // Stage-3 Unit-2: arm mixed write staging for THIS batch (must precede
        // the first offer()). reset() above disarmed any prior-batch mode, so
        // pooled classifiers re-capture the gate value each batch. The
        // executeRequestSync per-call classifier intentionally never arms it
        // (single-row batches gain nothing and the sync path stays untouched).
        if (mixedBatchEnabled) {
            classifier.setMixedStaging(true);
        }
        // V3.1: propagate the executor-level listStateNames registry into the
        // classifier so APPEND_MERGE routing survives the per-batch reset.
        for (String name : listStateNames) {
            classifier.registerListState(name);
        }
        return classifier;
    }

    /**
     * Returns a batch's classifier to the pool once its execution has fully completed. Thread
     * model: under {@code CoordinatedStateExecutor}, create runs on the mailbox thread (container
     * fill for batch N+1) CONCURRENTLY with a worker thread releasing batch N's classifier — so
     * the pool is a lock-free concurrent deque. A classifier instance itself is never touched by
     * two threads at once: it is exclusively the mailbox's during fill, handed off to exactly one
     * worker for execution (happens-before via the executor submit), and only re-enters the pool
     * after execution finishes.
     */
    public void releaseRequestContainer(
            org.apache.flink.runtime.asyncprocessing.AsyncRequestContainer<
                            org.apache.flink.runtime.asyncprocessing.StateRequest<?, ?, ?, ?>>
                    container) {
        if (container instanceof VectorizedClassifier vc) {
            classifierPool.addLast(vc);
        }
    }

    /** PR-1: pooled classifiers, each with a private buffer quartet; owned by this executor. */
    private final java.util.concurrent.ConcurrentLinkedDeque<VectorizedClassifier> classifierPool =
            new java.util.concurrent.ConcurrentLinkedDeque<>();

    /** FRS_REENTRY_DIAG: per-thread executeBatchRequests depth on THIS executor instance. */
    private final ThreadLocal<Integer> reentryDepth = ThreadLocal.withInitial(() -> 0);

    private static final java.util.concurrent.atomic.AtomicLong REENTRY_COUNT =
            new java.util.concurrent.atomic.AtomicLong();
    private static final boolean REENTRY_DIAG = "1".equals(System.getenv("FRS_REENTRY_DIAG"));

    /**
     * V3.1 (V20 sub-spec §5): register a ListState name so the per-batch classifier's
     * APPEND_MERGE routing recognizes LIST_ADD requests on this state. Idempotent.
     */
    public void registerListState(String stateName) {
        listStateNames.add(stateName);
    }

    /**
     * Execute a classified batch end-to-end and return a container future that reflects the
     * outcome.
     *
     * <p><b>PR-E2 / F5-3 design rationale (in-flight parallelism).</b> The spec asked whether we
     * could pipeline dispatch by:
     *
     * <ol>
     *   <li>Submitting the FFM batch synchronously on the mailbox thread,
     *   <li>Returning an INCOMPLETE container future immediately, and
     *   <li>Completing per-row futures from a worker thread on the next mailbox tick.
     * </ol>
     *
     * <p>The framework <em>does</em> permit this contract — the {@link
     * org.apache.flink.runtime.asyncprocessing.AsyncExecutionController#triggerIfNeeded} caller
     * ignores the container future entirely; per-row {@link
     * org.apache.flink.core.asyncprocessing.InternalAsyncFuture#complete} already queues user
     * callbacks back onto the mailbox via {@link
     * org.apache.flink.runtime.asyncprocessing.CallbackRunnerWrapper}. The community ForSt
     * {@code ForStStateExecutor} exploits this by offloading the engine call to a dedicated
     * {@code coordinatorThread} and returning early.
     *
     * <p><b>Why forst-rs cannot adopt that pattern in PR-E2's 2.5-day scope:</b>
     *
     * <ol>
     *   <li><b>FFI is synchronous (V1 contract).</b> {@code frs_vectorized_batch_get/put/delete}
     *       block until the engine has applied the batch op. So the only thread-side parallelism
     *       to win is interleaving Java-side decode + future-completion with the NEXT batch's
     *       classifier offer-phase.
     *   <li><b>Shared executor buffers prevent offload.</b> The Arrow {@link
     *       ColumnarBatchBuffer}s ({@code getKeys}, {@code putKeys}, {@code putValues}, {@code
     *       deleteKeys}) and the GET out-segments ({@code outOffsets}, {@code outValidity},
     *       {@code outData}) are <em>long-lived, executor-owned</em>. {@link
     *       #createRequestContainer()} hands the same buffer instances to every per-batch
     *       classifier and calls {@code reset()} on them. If we offloaded batch N to a worker
     *       and let the mailbox thread call {@code createRequestContainer()} for batch N+1, the
     *       reset would clobber buffer state that batch N's worker is still reading/writing →
     *       data race.
     *   <li><b>No MailboxExecutor injection.</b> The "defer per-row completion to next mailbox
     *       tick" alternative requires plumbing a {@link
     *       org.apache.flink.api.common.operators.MailboxExecutor} reference into this class.
     *       That plumbing reaches up through {@code ForStRsAsyncKeyedStateBackend.create(...)}
     *       and the keyed-state-backend factory and is non-trivially out of PR-E2's scope.
     *   <li><b>{@link #fullyLoaded()} always returns {@code false}.</b> Without a real in-flight
     *       accounting (counting outstanding batches in flight on a worker), the runtime would
     *       trigger an unbounded number of pipelined batches and overflow the shared buffers
     *       within milliseconds.
     * </ol>
     *
     * <p>The proper fix is structural: per-batch buffer ownership (refactor of the C1 design),
     * MailboxExecutor injection, a coordinator-thread mirror of community ForSt, and real
     * {@code fullyLoaded()} accounting. That is multiple PRs of work and was explicitly flagged
     * by the spec as "deferred to a Flink-runtime change" when the buffer model blocks the
     * surgical fix.
     *
     * <p><b>Pragmatic fallback chosen here.</b> We instrument the synchronous path with {@link
     * DispatchMetrics#recordBatchStart()} / {@link DispatchMetrics#recordBatchEnd()} so the
     * in-flight depth is observable. In the current contract the depth histogram is a dirac at
     * 1; the test {@code AsyncDispatchInFlightParallelismTest} asserts this invariant. When the
     * proper structural fix lands, the same histogram will show depths &gt; 1, and the test
     * becomes a regression gate for the buffer-ownership refactor.
     *
     * <p>This is NOT a forst-rs bug — the V1 FFI is sync by design, and the framework's caller
     * doesn't gate on the container future. The depth-=-1 invariant is a buffer-ownership
     * constraint enforced by the current shared-buffer design.
     */
    @Override
    public CompletableFuture<Void> executeBatchRequests(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> container) {
        // R29-M2: refuse new work after shutdown so the dispose chain can safely
        // proceed to {@code arena.close()} without racing in-flight FFI calls.
        // Mirrors {@code ForStRsStateExecutor}'s R28-M2 gate (the unwired one
        // R29-M2 closes); failing the container future is the same shape the
        // AsyncExecutionController already handles for batch-dispatch failures.
        if (shutdown.get()) {
            IllegalStateException err =
                    new IllegalStateException(
                            "VectorizedExecutor shutdown: rejecting batch request"
                                    + " (backend dispose() is in progress)");
            if (container instanceof VectorizedClassifier classifier) {
                drainPendingFuturesExceptionally(classifier, err);
            }
            return CompletableFuture.failedFuture(err);
        }
        VectorizedClassifier classifier = (VectorizedClassifier) container;
        // FRS_REENTRY_DIAG (2026-06-10, q8-race probe): detect RE-ENTRANT execution on the SAME
        // executor instance — a recursive trigger (per-row completion callback running inline on
        // the mailbox → state op → AEC trigger) would overwrite the executor-owned out-segments
        // mid-decode of the outer batch = the corruption mechanism candidate.
        int depth = reentryDepth.get();
        if (depth > 0 && REENTRY_DIAG) {
            long n = REENTRY_COUNT.incrementAndGet();
            if (n <= 5 || n % 1000 == 0) {
                System.err.println(
                        "[REENTRY_DIAG] executeBatchRequests re-entered depth=" + (depth + 1)
                                + " count=" + n + " thread=" + Thread.currentThread().getName());
            }
        }
        reentryDepth.set(depth + 1);
        if (metrics != null) {
            metrics.recordBatchStart();
        }
        try {
            // A6-H4: abort the batch BEFORE any dispatch if a prior {@code recordDelete} call
            // saw its {@code onClear} hook throw (typically an FFI {@code linker.batchPut}
            // failure from a per-state list-buffer drain). Without this short-circuit, the
            // executor would proceed to PUT/DELETE the partially-built batch and then
            // {@code reset()} would clear all in-progress rows — silently dropping the
            // pending writes the failed {@code onClear} drain was meant to flush. Surfacing
            // the cause as a failed container future causes the runtime to fail the task
            // (matching the FrsException path on direct dispatch failures).
            Throwable poison = classifier.batchPoisonCause();
            if (poison != null) {
                return CompletableFuture.failedFuture(poison);
            }
            if (DispatchOrderingHazards.requiresOrderedDispatch(classifier)) {
                if (OC_DISPATCH) {
                    OC_ORDERED_BATCHES.incrementAndGet();
                    OC_ORDERED_ROWS.addAndGet(classifier.orderedCount());
                    ocDispatchMaybeDump();
                }
                return executeBatchInOfferOrder(classifier);
            }
            if (OC_DISPATCH) {
                OC_VEC_BATCHES.incrementAndGet();
                OC_VEC_ROWS.addAndGet(
                        classifier.getCount() + classifier.putCount() + classifier.deleteCount());
                ocDispatchMaybeDump();
            }
            // Spec §Correctness Invariant 2: any deferred / cached writes must be
            // flushed BEFORE iterator ops. Within a single batch the natural
            // ordering of PUT/DELETE before ITER guarantees that.
            //
            // Stage-3 Unit-2: under mixed staging, puts + deletes + heap-path
            // appendMerges commit in ONE frs_vectorized_batch_mixed crossing
            // HERE (write-before-read per batch, two-regime design §3.2/§8).
            // Ordering safety: requiresOrderedDispatch above already routed
            // every batch with a same-key/prefix conflict across write kinds
            // (or get-vs-write) to offer-order sync dispatch, so the rows
            // committed here are cross-kind conflict-free; within the mixed
            // column rows apply in offer order (engine row order = seq order).
            // The ONE semantic delta vs the multi-call path: heap appendMerge
            // rows are engine-visible to SAME-BATCH iterators (multi-call
            // dispatched merges after iters) — which is what Invariant 2 asks
            // for; get-vs-merge overlap still routes ordered, unchanged.
            if (classifier.isMixedStaging()) {
                executeMixedWrites(classifier);
            } else {
                executePuts(classifier);
                executeDeletes(classifier);
            }
            executeGets(classifier);
            executeIters(classifier);
            // Dispatch vectorized APPEND_MERGE requests (P6-B, ListState path).
            //
            // PR-C2 split: each row went via either the heap path (appendMergeBuffer) or the
            // off-heap path (per-state ListStateArrowBuffer). The classifier's parallel arrays
            // {@code offHeapAppendMergeFutures} / {@code offHeapAppendMergeStates} mark which
            // path each row took.
            //   - Heap path: dispatchAppendMerge(amBuf) drives futures via amBuf.futures();
            //   - Off-heap path: drain unique state buffers via flushOffHeapListBuffersIfDirty()
            //     and the per-row off-heap futures resolve when their state's buffer flushes.
            AppendMergeBatchBuffer amBuf = classifier.appendMergeBuffer();
            Throwable firstRowFailure = null;
            int amCount = classifier.appendMergeCount();
            if (amCount > 0) {
                // 1) Heap path: dispatch and propagate any heap-row futures.
                if (amBuf != null && !amBuf.isEmpty()) {
                    dispatchAppendMerge(amBuf);
                }
                // 2) Off-heap path: drain unique state-instance buffers exactly once.
                classifier.flushOffHeapListBuffersIfDirty();
                // 3) Plumb completion to the StateRequest's runtime future on a per-row basis.
                StateRequest<?, ?, ?, ?>[] amReqs = classifier.appendMergeRequests();
                CompletableFuture<Void>[] offFutures = classifier.offHeapAppendMergeFutures();
                List<CompletableFuture<Void>> heapFutures =
                        amBuf != null ? amBuf.futures() : null;
                // B5-H7: flatten the heap-path futures into a primitive array so the per-row
                // dispatch below avoids the per-row List.get() interface call.
                CompletableFuture<Void>[] heapFuturesArr = null;
                int heapFuturesSize = 0;
                if (heapFutures != null && !heapFutures.isEmpty()) {
                    heapFuturesArr = flattenHeapFutures(heapFutures);
                    heapFuturesSize = heapFutures.size();
                }
                // Track per-path indices: heap rows index into heapFuturesArr in heap-append order.
                int heapIdx = 0;
                boolean mixedStaged = classifier.isMixedStaging();
                for (int i = 0; i < amCount; i++) {
                    if (mixedStaged && offFutures[i] == null) {
                        // Stage-3 Unit-2: heap-path rows were committed AND
                        // completed by executeMixedWrites (success: completePut;
                        // failure: the batch never reaches this loop). They have
                        // no amBuf tracking future — do not consume one.
                        continue;
                    }
                    CompletableFuture<Void> amFut =
                            offFutures[i] != null
                                    ? offFutures[i]
                                    : (heapFuturesArr != null && heapIdx < heapFuturesSize
                                            ? heapFuturesArr[heapIdx++]
                                            : null);
                    if (amFut != null && amFut.isCompletedExceptionally()) {
                        Throwable cause;
                        try {
                            amFut.getNow(null);
                            cause = new RuntimeException(
                                    "AppendMergeRequest future completed exceptionally"
                                            + " but cause unavailable");
                        } catch (Throwable t) {
                            cause = t.getCause() != null ? t.getCause() : t;
                        }
                        // R22-L2: mark BEFORE per-row completion for pattern consistency with the
                        // single-request path at line ~533. The current batched outer catch does
                        // NOT re-complete these rows, so this is defense-in-depth — a future
                        // refactor that adds a re-completion fallback would see the marker and
                        // skip the duplicate. Matches the R21-H1 contract: outer catches always
                        // observe a marker before they can race a per-row failPath.
                        classifier.markCompletedExceptionally(amReqs[i]);
                        completePutExceptionally(amReqs[i], cause);
                        if (firstRowFailure == null) {
                            firstRowFailure = cause;
                        }
                    } else {
                        completePut(amReqs[i]);
                    }
                }
            }
            // Dispatch vectorized ITER_PREFIX requests if the classifier's buffer is
            // non-null/non-empty.
            IterPrefixBatchBuffer ipBuf = classifier.iterPrefixBuffer();
            if (ipBuf != null && !ipBuf.isEmpty()) {
                dispatchIterPrefix(ipBuf);
            }
            // B-R5-NEW-H2: also dispatch ITER_RANGE. Pre-fix the buffer was filled
            // by VectorizedClassifier.submitVectorized but never drained — any
            // IterRangeRequest's per-row future was silently dropped. Matches the
            // dispatchIterPrefix wiring above; the outer try/catch's
            // drainPendingFuturesExceptionally also walks iterRangeBuffer so a
            // dispatch throw doesn't leak futures (see #drainPendingFuturesExceptionally).
            IterRangeBatchBuffer irBuf = classifier.iterRangeBuffer();
            if (irBuf != null && !irBuf.isEmpty()) {
                dispatchIterRange(irBuf);
            }
            // Round-2 fix A2-H3: container future should reflect row failures so the
            // runtime does not schedule the next batch into the failing engine.
            if (firstRowFailure != null) {
                return CompletableFuture.failedFuture(firstRowFailure);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Throwable t) {
            // Round-3 fix A3-H2: widen from `catch (Exception)` to `catch (Throwable)`.
            // FrsEnginePanicError extends Error, so a panic in executeGets/Puts/etc. would
            // previously escape the outer catch and the container future would never be
            // returned (operator hangs forever on the unresolved CompletableFuture).
            //
            // R99-H1: each `executeXxx` method drains ONLY its own kind's per-row
            // futures on throw. A throw mid-pipeline (e.g., in `executeGets`) leaves
            // pending iter / append-merge per-row futures unresolved because their
            // dispatch methods never ran. Per the comment block at lines 282-290,
            // the framework caller ignores the container future entirely; per-row
            // InternalAsyncFuture#complete drives mailbox callbacks. So an
            // unresolved per-row future indefinitely blocks the next op on that key
            // → operator hang. Drain the remaining queued kinds here before
            // returning the failed container.
            try {
                drainPendingFuturesExceptionally(classifier, t);
            } catch (Throwable drainErr) {
                // Drain must never mask the original failure. Attach as suppressed
                // so the diagnostic chain is preserved.
                t.addSuppressed(drainErr);
            }
            return CompletableFuture.failedFuture(t);
        } finally {
            // PR-E2: end-of-batch hook for in-flight depth tracking. Runs even on the
            // failure path so the depth gauge accurately reflects batch lifecycle.
            if (metrics != null) {
                metrics.recordBatchEnd();
            }
            reentryDepth.set(reentryDepth.get() - 1);
            // PR-1 leak fix: SELF-RELEASE the classifier back to the pool. Every container
            // passes through executeBatchRequests exactly once, in EVERY executor mode
            // (inline/routing/adaptive/coordinated) — without this, each batch leaked a fresh
            // classifier + 4 arena-allocated ColumnarBatchBuffers (arena memory frees only at
            // close) → multi-GB/min native growth on high-batch-rate queries (q17 adaptive:
            // TM cgroup-OOM-killed within seconds = the "wedge"). Safe to pool here: per-row
            // completions were issued with detached values during execution, and the next
            // createRequestContainer() resets before reuse.
            releaseRequestContainer(container);
        }
    }

    private CompletableFuture<Void> executeBatchInOfferOrder(VectorizedClassifier classifier) {
        if (classifier.hasOffHeapAppendMergeRows()) {
            IllegalStateException err =
                    new IllegalStateException(
                            "ForSt-RS batch contains same-key ordering hazards involving "
                                    + "off-heap APPEND_MERGE rows; fail fast to avoid replaying "
                                    + "already-staged list operands out of order");
            drainPendingFuturesExceptionally(classifier, err);
            return CompletableFuture.failedFuture(err);
        }
        StateRequest<?, ?, ?, ?>[] reqs = classifier.orderedRequests();
        int n = classifier.orderedCount();
        Throwable firstFailure = null;
        for (int i = 0; i < n; i++) {
            try {
                executeRequestSync(reqs[i], true);
            } catch (Throwable t) {
                if (firstFailure == null) {
                    firstFailure = t;
                    for (int j = i + 1; j < n; j++) {
                        try {
                            completePutExceptionally(reqs[j], t);
                        } catch (Throwable ignored) {
                            // Continue draining the unexecuted tail so ordered replay is fail-stop.
                        }
                    }
                    break;
                } else {
                    firstFailure.addSuppressed(t);
                }
            }
        }
        return firstFailure == null
                ? CompletableFuture.completedFuture(null)
                : CompletableFuture.failedFuture(firstFailure);
    }

    @Override
    public void executeRequestSync(StateRequest<?, ?, ?, ?> request) {
        executeRequestSync(request, false);
    }

    private void executeRequestSync(StateRequest<?, ?, ?, ?> request, boolean rethrowFailure) {
        // R29-M2: same post-shutdown gate as {@link #executeBatchRequests}. The sync
        // path is used by SYNC_SAVEPOINT and tests; refusing new work post-shutdown
        // prevents UAF on the slot {@link Arena} that dispose() is about to close.
        if (shutdown.get()) {
            IllegalStateException rej =
                    new IllegalStateException(
                            "VectorizedExecutor shutdown: rejecting sync request"
                                    + " (backend dispose() is in progress)");
            try {
                request.getFuture().completeExceptionally(rej.getMessage(), rej);
            } catch (RuntimeException ignored) {
                // exceptionHandler may itself throw; the throw below still surfaces
                // the rejection through the regular exception channel.
            }
            throw rej;
        }
        VectorizedClassifier single =
                new VectorizedClassifier(getKeys, putKeys, putValues, deleteKeys);
        // R18-H2: widen the try/catch to cover registerListState / initNewKindBuffers /
        // single.offer(request). Pre-fix the try block started at executeRequestSyncInner;
        // {@link VectorizedClassifier#recordDelete} (invoked transitively from
        // {@link VectorizedClassifier#offer} for null-payload write paths) rethrows the FFI
        // batchPut error when an {@code onClear} hook on a list/reducing/aggregating state
        // fails. That throw escaped executeRequestSync without completing the StateRequest's
        // future, leaving the operator wedged on an unresolvable CompletableFuture.
        //
        // Also covers initNewKindBuffers (arena allocation can OOM) and registerListState
        // (HashMap entry init) — every operation that mutates the per-call classifier before
        // dispatch must propagate to completePutExceptionally so the runtime observes the
        // failure on the request's future.
        try {
            single.reset();
            // Lazy-init the new-kind buffers (matches createRequestContainer behavior).
            for (String name : listStateNames) {
                single.registerListState(name);
            }
            single.initNewKindBuffers(arena);
            single.offer(request);
            // Round-3 fix A3-H1: wrap dispatch in try/catch so an FFI/engine throw on the sync
            // path doesn't leak the StateRequest's future (operator would hang).
            executeRequestSyncInner(single);
        } catch (Throwable t) {
            // R20-H1: explicit tracking replaces the R19-H1 isDone()-based double-completion
            // guard. The flink-core {@code AsyncFutureImpl.completeExceptionally(msg, ex)} in
            // production delegates ONLY to {@code AsyncFrameworkExceptionHandler.handleException}
            // and does NOT mutate {@code completableFuture} — so {@code getFuture().isDone()}
            // keeps returning {@code false} even after exceptional completion fired. The
            // R19-H1 guard was therefore a no-op in production (the test passed only because
            // the {@code RecordingFuture} mock's {@code isDone()} was wired to a counter,
            // inverted from production semantics).
            //
            // The classifier records every request it already pre-completed exceptionally
            // inside {@code recordDelete}'s onClear-throw handler. We consult that set here
            // ({@link VectorizedClassifier#takeClassifierCompletedExceptionally}) instead of
            // {@code isDone()} so the guard works regardless of {@code AsyncFutureImpl}'s
            // delegate-only semantics.
            if (!single.takeClassifierCompletedExceptionally(request)) {
                completePutExceptionally(request, t);
            }
            if (rethrowFailure) {
                throw asRuntimeException(t);
            }
        }
    }

    private static RuntimeException asRuntimeException(Throwable t) {
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        return new RuntimeException(t);
    }

    private void executeRequestSyncInner(VectorizedClassifier single) {
        executePuts(single);
        executeDeletes(single);
        executeGets(single);
        executeIters(single);
        AppendMergeBatchBuffer amBuf = single.appendMergeBuffer();
        int amCount = single.appendMergeCount();
        if (amCount > 0) {
            // PR-C2: mirror the executeBatchRequests split — drive both heap (amBuf) and
            // off-heap (per-state ListStateArrowBuffer) paths and complete per-row futures
            // in row order.
            if (amBuf != null && !amBuf.isEmpty()) {
                dispatchAppendMerge(amBuf);
            }
            single.flushOffHeapListBuffersIfDirty();
            StateRequest<?, ?, ?, ?>[] amReqs = single.appendMergeRequests();
            CompletableFuture<Void>[] offFutures = single.offHeapAppendMergeFutures();
            List<CompletableFuture<Void>> heapFutures =
                    amBuf != null ? amBuf.futures() : null;
            // B5-H7: flatten heap-path futures to avoid per-row List.get() interface dispatch.
            CompletableFuture<Void>[] heapFuturesArr = null;
            int heapFuturesSize = 0;
            Throwable firstAppendFailure = null;
            if (heapFutures != null && !heapFutures.isEmpty()) {
                heapFuturesArr = flattenHeapFutures(heapFutures);
                heapFuturesSize = heapFutures.size();
            }
            int heapIdx = 0;
            for (int i = 0; i < amCount; i++) {
                CompletableFuture<Void> amFut =
                        offFutures[i] != null
                                ? offFutures[i]
                                : (heapFuturesArr != null && heapIdx < heapFuturesSize
                                        ? heapFuturesArr[heapIdx++]
                                        : null);
                if (amFut != null && amFut.isCompletedExceptionally()) {
                    Throwable cause;
                    try {
                        amFut.getNow(null);
                        cause = new RuntimeException(
                                "AppendMergeRequest future completed exceptionally"
                                        + " but cause unavailable");
                    } catch (Throwable t) {
                        cause = t.getCause() != null ? t.getCause() : t;
                    }
                    // R21-H1: mark BEFORE completion so that if a later step in
                    // executeRequestSyncInner throws (e.g. dispatchIterPrefix below),
                    // the outer catch in {@link #executeRequestSync} skips the duplicate
                    // completion attempt for this same single request.
                    single.markCompletedExceptionally(amReqs[i]);
                    completePutExceptionally(amReqs[i], cause);
                    if (firstAppendFailure == null) {
                        firstAppendFailure = cause;
                    }
                } else {
                    completePut(amReqs[i]);
                }
            }
            if (firstAppendFailure != null) {
                throw asRuntimeException(firstAppendFailure);
            }
        }
        IterPrefixBatchBuffer ipBuf = single.iterPrefixBuffer();
        if (ipBuf != null && !ipBuf.isEmpty()) {
            dispatchIterPrefix(ipBuf);
        }
        // B-R6-NEW-H1: sync-path symmetric dispatch for ITER_RANGE. Mirrors the
        // async path (executeBatchRequests line ~460); without this an
        // IterRangeRequest queued via VectorizedClassifier.submitVectorized
        // from the sync entry would silently leak its per-row future on the
        // happy path (drainPendingFuturesExceptionally only fires on throw).
        IterRangeBatchBuffer irBuf = single.iterRangeBuffer();
        if (irBuf != null && !irBuf.isEmpty()) {
            dispatchIterRange(irBuf);
        }
    }

    @Override
    public boolean fullyLoaded() {
        return false;
    }

    @Override
    public void shutdown() {
        // R29-M2: idempotent set so the close()/dispose() chain in
        // {@link
        // org.apache.flink.state.forstrs.keyed.ForStRsAsyncKeyedStateBackend#dispose}
        // — which calls {@code managedExecutors.forEach(VectorizedExecutor::shutdown)}
        // BEFORE {@code arena.close} via {@link
        // org.apache.flink.state.forstrs.exec.SlotArenaScope#closeSlot} — actually
        // gates concurrent {@link #executeBatchRequests}/{@link #executeRequestSync}
        // calls from in-flight async-state controller batches. Best-effort: the
        // executor does NOT own the {@link Arena} / {@link FrsDb} / {@link FrsCfHandle}
        // (those are released by {@link
        // org.apache.flink.state.forstrs.keyed.ForStRsAsyncKeyedStateBackend#releaseNativeResources});
        // shutdown's only job is to flip the rejection gate.
        shutdown.set(true);
    }

    /**
     * Pre-snapshot drain hook (PR-A1 wiring).
     *
     * <p>The async-state V2 dispatch loop in {@link #executeBatchRequests} hands every classifier
     * row to the engine within a single mailbox turn: by the time {@code executeBatchRequests}
     * returns, every PUT / DELETE / GET / ITER / APPEND_MERGE row has been submitted via FFM, the
     * engine memtable has applied the mutation, and the per-row futures are resolved. There is no
     * deferred work owned by this {@link VectorizedExecutor} that {@code flushDirty} needs to
     * drain — the work is "dirty" in the engine memtable, not on the Java side.
     *
     * <p>What we still need to do here, however, is force the engine to fold its in-memory
     * memtable down to L0 SSTs <em>before</em> the snapshot strategy enumerates files. The
     * engine's normal flush cadence is asynchronous and L0-rotation-driven; on a checkpoint
     * barrier the strategy expects every committed write to be reachable as an SST file. Calling
     * {@link ForStRsLinker#flush} forces a synchronous memtable → L0 conversion so the snapshot's
     * file enumeration is complete.
     *
     * <p>Called from {@code ForStRsAsyncKeyedStateBackend.snapshot()} (PR-A1) ONLY.
     *
     * <p>R38-M2: this method is a snapshot-pre-hook. Do NOT call it after
     * {@code notifyCheckpointComplete} — by then the manifest is already on
     * durable storage and any new L0 SSTs produced here are orphaned w.r.t.
     * the just-completed checkpoint (they can only be referenced by the
     * next snapshot, and until then inflate local-disk footprint with no
     * caller benefit).
     */
    public void flushDirty() {
        // FFI flush: synchronously fold memtable → L0 SST. Idempotent; engine returns OK if the
        // memtable is empty. Throws on engine-side error (caller is the snapshot path which will
        // surface the failure as a failed RunnableFuture).
        //
        // R24-M1: the catch (RuntimeException) wrapper here was a no-op (just re-threw). It
        // also let Error subclasses (StackOverflowError, OutOfMemoryError) skip the
        // snapshot-bookkeeping path — the caller's failure-marker logic only runs if the throw
        // propagates, which it does directly without the catch. Widen to Throwable and convert
        // to FrsBackendException so the snapshot path observes a uniform exception shape and
        // surfaces a clean error message; rethrow Errors after wrapping is not appropriate
        // because Errors typically indicate VM-level conditions that should not be wrapped.
        // Strategy: let RuntimeException propagate unchanged; wrap checked-style failures from
        // the FFI into FrsBackendException with status=INTERNAL_ERROR; rethrow Error subclasses
        // as-is so the JVM-level signal is not laundered.
        if (linker != null && db != null) {
            try {
                linker.flush(db);
            } catch (Error err) {
                // VM-level: rethrow without wrapping so JVM-level signals (OOM, stack overflow)
                // reach the snapshot path's outer error handler in their original form.
                throw err;
            } catch (RuntimeException re) {
                // Already RuntimeException — propagate unchanged; the snapshot path observes
                // this and marks the future failed.
                throw re;
            } catch (Throwable t) {
                // Anything else (checked-style FFI surprises): wrap in the project's uniform
                // backend-exception shape so the snapshot path's failure marker is consistent
                // across flush failure modes.
                throw new FrsBackendException(
                        org.apache.flink.state.forstrs.FrsStatus.INTERNAL,
                        "flushDirty: engine flush failed: " + t.getMessage());
            }
        }
    }

    // -----------------------------------------------------------------
    // Op-type executors
    // -----------------------------------------------------------------

    /**
     * Aggregate stateName for old-style Flink-runtime batches that mix multiple state names. The
     * new-style VectorizedStateRequest path (P5+) will provide per-state attribution.
     */
    private static final String MIXED_STATE = "_mixed";

    private void executePuts(VectorizedClassifier c) {
        int n = c.putCount();
        if (n == 0) {
            return;
        }
        StateRequest<?, ?, ?, ?>[] reqs = c.putRequests();
        long t0 = System.nanoTime();
        try {
            invokeVectorizedBatchPut(
                    c.putKeys().offsetsSegment(),
                    c.putKeys().dataSegment(),
                    c.putValues().offsetsSegment(),
                    c.putValues().dataSegment(),
                    n);
            long latencyNs = System.nanoTime() - t0;
            if (metrics != null) {
                metrics.recordDispatch(
                        VectorizedStateRequest.Kind.PUT, MIXED_STATE, n, 0L, latencyNs);
            }
            for (int i = 0; i < n; i++) {
                completePut(reqs[i]);
            }
        } catch (Throwable t) {
            // PR-A10 / S1-9: an FFI throw here previously left every per-row
            // StateRequest future unresolved — the outer catch in
            // executeBatchRequests returned a failed container future but the
            // runtime would still wait on each row's future forever. Drain them
            // first so the async-state runtime sees them all resolve, then
            // re-throw so the outer catch can return a failed container future.
            //
            // R21-H1: BEFORE per-row exceptional completion, populate
            // {@code classifierCompletedExceptionally} so that {@link
            // #executeRequestSync}'s outer catch sees the marker via {@link
            // VectorizedClassifier#takeClassifierCompletedExceptionally} and
            // skips its own (duplicate) {@code completePutExceptionally} call.
            // Without this, on the sync path the framework
            // {@code AsyncFrameworkExceptionHandler.handleException} fires
            // twice (once per per-op catch row, once from outer catch) and
            // produces a double task-failure log.
            // R93-H1: escalate FrsEnginePanicError to fatalHandler.
                // `ForStRsLinker.checkVectorized` (called by
                // `frsVectorizedBatchPut` below) throws
                // `FrsEnginePanicError` on fail-process FFI rc, but never
                // invokes the fatal handler. Pre-fix the engine kept
                // running on poisoned state after a PUT-path panic.
                // Mirrors the GET path at lines 828-836.
                if (t instanceof FrsEnginePanicError panicErr && fatalHandler != null) {
                    fatalHandler.onFatalError(panicErr);
                }
            for (int i = 0; i < n; i++) {
                try {
                    c.markCompletedExceptionally(reqs[i]);
                    completePutExceptionally(reqs[i], t);
                } catch (Throwable ignore) {
                    // continue draining the rest
                }
            }
            throw t;
        }
    }

    private void executeDeletes(VectorizedClassifier c) {
        int n = c.deleteCount();
        if (n == 0) {
            return;
        }
        StateRequest<?, ?, ?, ?>[] reqs = c.deleteRequests();
        try {
            invokeVectorizedBatchDelete(
                    c.deleteKeys().offsetsSegment(), c.deleteKeys().dataSegment(), n);
            for (int i = 0; i < n; i++) {
                completeDelete(reqs[i]);
            }
        } catch (Throwable t) {
            // PR-A10 / S1-9: drain per-row futures so the runtime does not hang
            // on the failing batch's individual StateRequest futures.
            // R21-H1: populate the classifier marker set BEFORE per-row
            // completion so {@link #executeRequestSync}'s outer catch skips
            // a duplicate completion (see executePuts catch for rationale).
            // R93-H1: escalate FrsEnginePanicError — sister to executePuts.
            if (t instanceof FrsEnginePanicError panicErr && fatalHandler != null) {
                fatalHandler.onFatalError(panicErr);
            }
            for (int i = 0; i < n; i++) {
                try {
                    c.markCompletedExceptionally(reqs[i]);
                    completeDeleteExceptionally(reqs[i], t);
                } catch (Throwable ignore) {
                    // continue draining the rest
                }
            }
            throw t;
        }
    }

    /**
     * Stage-3 Unit-2: commits a mixed-staged batch's puts + deletes + heap-path appendMerges in
     * ONE {@code frs_vectorized_batch_mixed} crossing (one atomic engine sequence allocation).
     *
     * <p>ZERO-COPY: the classifier's unified off-heap columns ({@code mixedKeys} /
     * {@code mixedValues} / {@code kinds}) are handed to the FFI verbatim — rows were serialized
     * exactly once at classification time, in offer order; the kinds byte per row was written
     * during per-op classification (no boxing, no dispatch-time column rebuild).
     *
     * <p>Engine ordering contract (frs_vectorized_batch_mixed, lib.rs): rows apply in array
     * order under one seq range — a later same-key row wins; merge operands concatenate in row
     * order. Batches with cross-kind same-key/prefix conflicts never reach this method
     * ({@link DispatchOrderingHazards#requiresOrderedDispatchMixed} routes them to offer-order sync dispatch), so
     * offer-order commit here is observationally identical to the multi-call path's
     * put→delete→merge grouped order.
     *
     * <p>Completion: put / delete rows complete here (mirrors {@link #executePuts} /
     * {@link #executeDeletes}); heap-path APPEND_MERGE rows complete in the shared
     * post-dispatch loop in {@link #executeBatchRequests} via the {@code offFutures[i] == null →
     * completePut(amReqs[i])} arm being SKIPPED and completed here instead (they carry no
     * tracking future). On throw, ALL three kinds drain exceptionally before the rethrow —
     * sister pattern to the executePuts/executeDeletes catch blocks (R21-H1 markers + R93-H1
     * panic escalation).
     */
    private void executeMixedWrites(VectorizedClassifier c) {
        int n = c.mixedRowCount();
        if (n == 0) {
            return;
        }
        StateRequest<?, ?, ?, ?>[] putReqs = c.putRequests();
        int putCount = c.putCount();
        StateRequest<?, ?, ?, ?>[] delReqs = c.deleteRequests();
        int delCount = c.deleteCount();
        StateRequest<?, ?, ?, ?>[] amReqs = c.appendMergeRequests();
        CompletableFuture<Void>[] offFutures = c.offHeapAppendMergeFutures();
        int amCount = c.appendMergeCount();
        long t0 = System.nanoTime();
        try {
            invokeVectorizedBatchMixed(
                    c.mixedKindsSegment(),
                    c.mixedKeys().offsetsSegment(),
                    c.mixedKeys().dataSegment(),
                    c.mixedValues().offsetsSegment(),
                    c.mixedValues().dataSegment(),
                    n);
            if (DiagCompletionCounters.ENABLED) {
                DiagCompletionCounters.named("mixedBatchCalls");
                DiagCompletionCounters.namedAdd("mixedBatchOps", n);
            }
            long latencyNs = System.nanoTime() - t0;
            if (metrics != null) {
                metrics.recordDispatch(
                        VectorizedStateRequest.Kind.PUT, MIXED_STATE, n, 0L, latencyNs);
            }
            for (int i = 0; i < putCount; i++) {
                completePut(putReqs[i]);
            }
            for (int i = 0; i < delCount; i++) {
                completeDelete(delReqs[i]);
            }
            // Heap-path APPEND_MERGE rows (offFutures[i] == null) committed in
            // this same crossing; complete them now. Off-heap rows resolve via
            // their per-state buffer flush, untouched by the mixed path.
            for (int i = 0; i < amCount; i++) {
                if (offFutures[i] == null) {
                    completePut(amReqs[i]);
                }
            }
        } catch (Throwable t) {
            // R93-H1 sister: escalate engine panics to the fatal handler so a
            // poisoned engine never keeps serving writes.
            if (t instanceof FrsEnginePanicError panicErr && fatalHandler != null) {
                fatalHandler.onFatalError(panicErr);
            }
            // PR-A10 / S1-9 + R21-H1 sister: drain EVERY row this crossing
            // covered (put + delete + heap-merge) so no per-row future hangs,
            // marking each first so outer catches skip duplicate completion.
            for (int i = 0; i < putCount; i++) {
                try {
                    c.markCompletedExceptionally(putReqs[i]);
                    completePutExceptionally(putReqs[i], t);
                } catch (Throwable ignore) {
                    // continue draining the rest
                }
            }
            for (int i = 0; i < delCount; i++) {
                try {
                    c.markCompletedExceptionally(delReqs[i]);
                    completeDeleteExceptionally(delReqs[i], t);
                } catch (Throwable ignore) {
                    // continue draining the rest
                }
            }
            for (int i = 0; i < amCount; i++) {
                if (offFutures[i] != null) {
                    continue;
                }
                try {
                    c.markCompletedExceptionally(amReqs[i]);
                    completePutExceptionally(amReqs[i], t);
                } catch (Throwable ignore) {
                    // continue draining the rest
                }
            }
            throw t;
        }
    }

    private void executeGets(VectorizedClassifier c) {
        int n = c.getCount();
        if (n == 0) {
            return;
        }
        StateRequest<?, ?, ?, ?>[] reqs = c.getRequests();
        ForStRsInnerTable<?, ?, ?>[] tables = c.getTables();
        // R38-M3: per-GET confined arena for the oversize-retry path. The
        // steady-state buffer ({@link #outData} at {@link #INITIAL_OUT_DATA_CAP})
        // is preserved across calls; only the oversize growth lives in this
        // arena and is released at the end of the GET. Eliminates monotonic
        // executor-arena growth when a periodic single-large value triggers
        // BUFFER_TOO_SMALL.
        Arena perGetArena = null;
        MemorySegment savedOutData = outData;
        long savedOutDataCap = outDataCap;
        try {
            ensureOutCapacity(n);

            long t0 = System.nanoTime();
            // Retry-with-growth loop: if out_data buffer is too small, grow and retry.
            while (true) {
                int rc =
                        invokeVectorizedBatchGet(
                                c.getKeys().offsetsSegment(),
                                c.getKeys().dataSegment(),
                                n,
                                outOffsets,
                                outData,
                                outValidity,
                                outDataCap,
                                outDataLenSeg);
                if (rc == FRS_STATUS_OK) {
                    long latencyNs = System.nanoTime() - t0;
                    if (metrics != null) {
                        metrics.recordDispatch(
                                VectorizedStateRequest.Kind.GET, MIXED_STATE, n, 0L, latencyNs);
                    }
                    break;
                }
                if (rc == FRS_STATUS_BUFFER_TOO_SMALL) {
                    long needed = outDataLenSeg.get(ValueLayout.JAVA_LONG, 0L);
                    long newCap = Math.max(outDataCap * 2L, needed);
                    // R38-M3: allocate in a per-GET confined arena that is
                    // closed in finally — the executor arena no longer
                    // grows on a periodic large value.
                    if (perGetArena == null) {
                        perGetArena = Arena.ofConfined();
                    }
                    outData = perGetArena.allocate(newCap);
                    outDataCap = newCap;
                    continue;
                }
                // Non-OK, non-BUFFER_TOO_SMALL: classify via FrsErrorCode.
                FrsErrorCode errCode = FrsErrorCode.fromU32(rc);
                if (metrics != null) {
                    metrics.recordFfiError(VectorizedStateRequest.Kind.GET, MIXED_STATE, errCode);
                }
                if (errCode.isFailProcess()) {
                    FrsEnginePanicError panicErr =
                            new FrsEnginePanicError(
                                    errCode, "kind=GET state=" + MIXED_STATE + " rc=" + rc);
                    if (fatalHandler != null) {
                        fatalHandler.onFatalError(panicErr);
                    }
                    throw panicErr;
                }
                // R88-H1: the vectorized FFI returns codes from
                // `FrsErrorCode` (100, 101, 110, 200, 201, 300, 301, 302,
                // 303, 999) which are NOT in the legacy `FrsStatus` enum
                // (0..17). For any fail-row or fail-batch result that
                // survives the `isFailProcess()` guard above (e.g.
                // BATCH_HEADER_MALFORMED=110, KEY_TOO_LARGE=100,
                // ITER_EXPIRED=200, ENGINE_IO=300), the bare
                // `FrsStatus.fromCode(rc)` throws `IllegalArgumentException`
                // — turning a typed error into an unchecked crash that
                // bypasses the per-row exceptional-completion logic
                // downstream. Catch the IAE and fall back to PANIC so the
                // wrapped FrsBackendException always carries the original
                // `rc` value in its message.
                FrsStatus status;
                try {
                    status = FrsStatus.fromCode(rc);
                } catch (IllegalArgumentException ignored) {
                    status = FrsStatus.PANIC;
                }
                throw new FrsBackendException(
                        status, "frs_vectorized_batch_get rc=" + rc + " errCode=" + errCode);
            }

            // Decode results: for each slot, read validity byte and (offsets, data) range.
            // PR-B1 (V2-6, C-H1, C-H6): pass the native segment slice directly into the
            // table's MemorySegment overload — eliminates the per-row `new byte[len]`
            // that used to happen here before deserialisation.
            for (int i = 0; i < n; i++) {
                byte vld = outValidity.get(ValueLayout.JAVA_BYTE, i);
                boolean present = vld != 0;
                int len = 0;
                long start = 0L;
                if (present) {
                    int s = outOffsets.get(ValueLayout.JAVA_INT, (long) i * Integer.BYTES);
                    int e =
                            outOffsets.get(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES);
                    start = s;
                    len = e - s;
                }
                completeGet(reqs[i], tables[i], present, outData, start, len);
            }
        } catch (Throwable t) {
            // PR-A10 / S1-9: drain every pending GET future on FFI / decode
            // failure. Without this, the outer catch in executeBatchRequests
            // returns a failed container future but the per-row StateRequest
            // futures stay unresolved — the operator hangs on the first
            // transient engine error in the GET path.
            // R21-H1: populate the classifier marker set BEFORE per-row
            // completion so {@link #executeRequestSync}'s outer catch skips
            // a duplicate completion (see executePuts catch for rationale).
            for (int i = 0; i < n; i++) {
                try {
                    c.markCompletedExceptionally(reqs[i]);
                    completeGetExceptionally(reqs[i], tables[i], t);
                } catch (Throwable ignore) {
                    // continue draining the rest
                }
            }
            throw t;
        } finally {
            // R38-M3: release the per-GET oversize buffer and restore the
            // executor's steady-state outData segment so the next call
            // starts from {@link #INITIAL_OUT_DATA_CAP} again. This runs
            // for both the OK path and the error path; the field swap is
            // safe because completeGet has already consumed outData by
            // here (it was the same segment that perGetArena allocated).
            if (perGetArena != null) {
                outData = savedOutData;
                outDataCap = savedOutDataCap;
                try {
                    perGetArena.close();
                } catch (Throwable ignored) {
                    // R39-M1: best-effort silent close. Finally restores
                    // outData fields THEN closes the per-GET arena; if
                    // close() throws (an in-flight borrowed segment from
                    // a use-after-free) the prior R38-M3 comment claimed
                    // we "would surface" it, but the catch-Throwable here
                    // actively swallows it. The simplest correct contract
                    // is to acknowledge that — keep the close path simple
                    // and do not rethrow. Any use-after-free would also
                    // surface through the next perGetArena allocation or
                    // through asan/valgrind in stress, so the loss of
                    // signal here is bounded.
                }
            }
        }
    }

    /**
     * FRS_RS_PARALLEL_ITER (default OFF): route the batch's iterator probes through the
     * coalesced + parallel engine open ({@code frs_vec_iter_prefix_open_batch_parallel}) instead of
     * the serial per-request loop — the join read-path lever (q7/q9/q20). Default OFF so the passing
     * set (q3/q11/q12/q15/q16/q19 + light) keeps the proven serial path until the parallel path is
     * e2e-validated; flip the default once verified.
     */
    private static final boolean PARALLEL_ITER = "1".equals(System.getenv("FRS_RS_PARALLEL_ITER"));
    private static final boolean ITER_DISPATCH_DIAG =
            "1".equals(System.getenv("FRS_ITER_DISPATCH_DIAG"));
    private static final java.util.concurrent.atomic.AtomicLong DIAG_BATCHES =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong DIAG_FRESH =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong DIAG_CONT =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong DIAG_PAR_DISPATCHES =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong DIAG_NEXT_DUMP =
            new java.util.concurrent.atomic.AtomicLong(200_000L);

    private void executeIters(VectorizedClassifier c) {
        if (c.iterRequests().isEmpty()) {
            return;
        }
        // FRS-PARALLEL-ITER: when enabled, PARTITION the batch — fresh-open MAP_ITER probes go
        // through ONE batched-parallel FFI crossing (engine builds+drains the K probes across its
        // read pool); continuations (existingVecHandle != 0) and MAP_IS_EMPTY stay on the serial
        // path. A MIXED batch still parallelizes its fresh probes (the prior all-or-nothing gate
        // fell back to serial whenever a single continuation was present — the common case for
        // multi-chunk join iterations, which is why q9/q7/q20 saw no speedup).
        if (PARALLEL_ITER) {
            java.util.List<ForStRsDBIterRequest<?, ?, ?, ?>> all = c.iterRequests();
            java.util.List<ForStRsDBIterRequest<?, ?, ?, ?>> fresh = new java.util.ArrayList<>();
            java.util.List<ForStRsDBIterRequest<?, ?, ?, ?>> rest = new java.util.ArrayList<>();
            for (ForStRsDBIterRequest<?, ?, ?, ?> it : all) {
                if (!it.isMapIsEmpty() && !it.hasExistingVecHandle()) {
                    fresh.add(it);
                } else {
                    rest.add(it);
                }
            }
            if (ITER_DISPATCH_DIAG) {
                DIAG_BATCHES.incrementAndGet();
                DIAG_FRESH.addAndGet(fresh.size());
                DIAG_CONT.addAndGet(rest.size());
                long total = DIAG_FRESH.get() + DIAG_CONT.get();
                long threshold = DIAG_NEXT_DUMP.get();
                if (total >= threshold && DIAG_NEXT_DUMP.compareAndSet(threshold, threshold + 200_000L)) {
                    System.out.println(
                            "[ITER_DISPATCH_DIAG] batches="
                                    + DIAG_BATCHES.get()
                                    + " fresh="
                                    + DIAG_FRESH.get()
                                    + " continuation="
                                    + DIAG_CONT.get()
                                    + " parDispatches="
                                    + DIAG_PAR_DISPATCHES.get()
                                    + " fresh%="
                                    + (total == 0 ? 0 : (100 * DIAG_FRESH.get() / total)));
                }
            }
            if (fresh.size() > 1) {
                DIAG_PAR_DISPATCHES.incrementAndGet();
                executeItersBatchedParallel(fresh, c);
                // Serial-drive the remainder (continuations + IS_EMPTY) below.
                if (rest.isEmpty()) {
                    return;
                }
                for (ForStRsDBIterRequest<?, ?, ?, ?> iter : rest) {
                    drainIterSerial(iter, c);
                }
                return;
            }
        }
        // R22-M2: per-op try/catch establishes the R21-H1 invariant for iter requests too. Pre-fix,
        // an iter.process(...) throw fell through to the outer catch in {@link
        // #executeRequestSync}, which labels failure as PUT — and because the iter's StateRequest
        // is not in the marker set (no markCompletedExceptionally call here), the outer catch did
        // NOT skip re-completion, so the iter row's future could be completed twice or routed
        // through the PUT failure path. Marking + completing-exceptionally on the iter request
        // before rethrowing matches the executePuts / executeGets contract.
        for (ForStRsDBIterRequest<?, ?, ?, ?> iter : c.iterRequests()) {
            drainIterSerial(iter, c);
        }
    }

    /** Serial drain of one iterator request with the R22-M2 per-op fatal/mark/complete contract. */
    private void drainIterSerial(ForStRsDBIterRequest<?, ?, ?, ?> iter, VectorizedClassifier c) {
        try {
            if (reusableIterChunkBuf == null) {
                reusableIterChunkBuf = arena.allocate(ForStRsDBIterRequest.chunkBufCap());
            }
            iter.process(linker, db, cf, arena, reusableIterChunkBuf);
        } catch (Throwable t) {
            // R94-H1 / R93-H2: throwIfFatal() throws FrsEnginePanicError for fail-process FFI rc;
            // escalate to the fatal handler so the engine doesn't keep running on poisoned state.
            if (t instanceof FrsEnginePanicError panicErr && fatalHandler != null) {
                fatalHandler.onFatalError(panicErr);
            }
            StateRequest<?, ?, ?, ?> sr = iter.getStateRequest();
            if (sr != null) {
                try {
                    c.markCompletedExceptionally(sr);
                } catch (Throwable ignore) {
                    // Best-effort marker placement — never swallow the original failure.
                }
            }
            try {
                iter.completeExceptionally(t);
            } catch (Throwable ignore) {
                // Best-effort per-row completion — never swallow the original failure.
            }
            throw t;
        }
    }

    /**
     * Coalesced + PARALLEL iterator dispatch (the join read-path lever, q7/q9/q20). Packs the K
     * probe prefixes SoA, opens them all in ONE {@code frs_vec_iter_prefix_open_batch_parallel}
     * crossing (the engine builds+drains the K probes across its read pool), then drives each
     * request's drain from its handle + first chunk via
     * {@link ForStRsDBIterRequest#processFromBatchedOpen}. Replaces the serial per-record loop that
     * crossed FFI N times + built N prefix-scans on the single coordinator thread. Decode is the
     * SAME zero-copy path as the serial loop ({@code completeWithEntries}/{@code VIEW_TL}). Only
     * reached when {@link #PARALLEL_ITER} is on AND every probe is a fresh-open MAP_ITER.
     */
    private void executeItersBatchedParallel(
            java.util.List<ForStRsDBIterRequest<?, ?, ?, ?>> reqs, VectorizedClassifier c) {
        int n = reqs.size();
        final int chunkCap = ForStRsDBIterRequest.chunkBufCap();
        try (Arena scratch = Arena.ofConfined()) {
            // 1+2: pack prefixes SoA (offsets[n+1] + contiguous bytes).
            int total = 0;
            for (ForStRsDBIterRequest<?, ?, ?, ?> r : reqs) {
                byte[] p = r.prefix();
                total += (p == null ? 0 : p.length);
            }
            MemorySegment prefixesOff = scratch.allocate((long) (n + 1) * Integer.BYTES);
            MemorySegment prefixesData = scratch.allocate(Math.max(total, 1));
            prefixesOff.set(ValueLayout.JAVA_INT, 0L, 0);
            int off = 0;
            for (int i = 0; i < n; i++) {
                byte[] p = reqs.get(i).prefix();
                int len = (p == null ? 0 : p.length);
                if (len > 0) {
                    MemorySegment.copy(p, 0, prefixesData, ValueLayout.JAVA_BYTE, off, len);
                }
                off += len;
                prefixesOff.set(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES, off);
            }

            // 3: K uniform chunk buffers (the engine fills each probe's first chunk here) + the
            // u64 handle array + the AoS FrsChunk descriptors. Confined to this call: the first
            // chunks are consumed by processFromBatchedOpen (which deserializes to detached on-heap
            // UK/UV) before this try-block closes, so nothing here outlives the scratch.
            MemorySegment chunkData = scratch.allocate((long) n * chunkCap);
            MemorySegment outHandles = scratch.allocate((long) n * Long.BYTES);
            long chunkStride = ForStRsLinker.frsChunkLayoutByteSize();
            MemorySegment outChunks = scratch.allocate((long) n * chunkStride);
            for (int i = 0; i < n; i++) {
                MemorySegment cb = chunkData.asSlice((long) i * chunkCap, chunkCap);
                ForStRsLinker.setFrsChunkBufPtr(outChunks, i, cb);
                ForStRsLinker.setFrsChunkBufCap(outChunks, i, chunkCap);
            }

            // Single FFI crossing for N parallel opens.
            int rcBatch =
                    linker.frsVecIterPrefixOpenBatchParallel(
                            db.handle(),
                            cf.handle(),
                            prefixesOff,
                            prefixesData,
                            n,
                            outHandles,
                            outChunks,
                            chunkCap);

            // Drive each probe's drain. Per-row handles are populated even on batch-level non-Ok
            // (the Rust impl writes 0 to a failed row's handle); a failed probe aborts the batch
            // the same way the serial loop's per-iter catch rethrows.
            for (int i = 0; i < n; i++) {
                ForStRsDBIterRequest<?, ?, ?, ?> req = reqs.get(i);
                long handle = outHandles.get(ValueLayout.JAVA_LONG, (long) i * Long.BYTES);
                int rows = ForStRsLinker.getFrsChunkRowCount(outChunks, i);
                int bytes = ForStRsLinker.getFrsChunkBytesUsed(outChunks, i);
                // P0: FRS_CHUNK_EOF (bit 0 of FrsChunk._reserved) — the engine exhausted
                // this probe in its first chunk and AUTO-CLOSED the iterator; the drain
                // skips the trailing next() + close() crossings for it.
                boolean eofAtOpen = ForStRsLinker.isFrsChunkEof(outChunks, i);
                MemorySegment cb = chunkData.asSlice((long) i * chunkCap, chunkCap);
                try {
                    if (handle == 0L) {
                        FrsErrorCode code = FrsErrorCode.fromU32(rcBatch);
                        if (code.isFailProcess() && fatalHandler != null) {
                            FrsEnginePanicError panicErr =
                                    new FrsEnginePanicError(code, "kind=ITER_BATCH_PARALLEL row=" + i);
                            fatalHandler.onFatalError(panicErr);
                            throw panicErr;
                        }
                        throw new FrsException(code, i, new byte[0]);
                    }
                    req.processFromBatchedOpen(
                            linker, db, cf, handle, cb, rows, bytes, eofAtOpen);
                } catch (Throwable t) {
                    if (t instanceof FrsEnginePanicError panicErr && fatalHandler != null) {
                        fatalHandler.onFatalError(panicErr);
                    }
                    StateRequest<?, ?, ?, ?> sr = req.getStateRequest();
                    if (sr != null) {
                        try {
                            c.markCompletedExceptionally(sr);
                        } catch (Throwable ignore) {
                            // Best-effort marker placement.
                        }
                    }
                    try {
                        req.completeExceptionally(t);
                    } catch (Throwable ignore) {
                        // Best-effort per-row completion.
                    }
                    throw t;
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // New-kind dispatch stubs (P2 Batch C) — real FFI wiring in later PRs
    // -----------------------------------------------------------------

    /**
     * Dispatches an APPEND_MERGE batch via {@code frs_vec_merge_append} FFI (P6-B).
     *
     * <p><b>Phase A.1 update (audit-design §3 V4):</b> when each request carries exactly 1 operand
     * slice, this method delegates to {@link #dispatchAppendMergeBatch} for a single batched FFM
     * crossing. Multi-operand-per-row requests still go through the legacy per-row path below
     * (the batched FFI's wire layout is 1 operand per row).
     *
     * <p>Legacy per-request dispatch: for each request in the buffer, allocates a small scratch
     * {@link Arena} to hold the {@code operand_ptrs} and {@code operand_lens} arrays, then calls
     * {@code frs_vec_merge_append} with the key and N operand slices. Each request carries N
     * value slices (one per list element). For {@link
     * org.apache.flink.state.forstrs.state.ForStRsListStateV2#addAll(java.util.List)}, N &gt; 1 so
     * the call is effectively batched at the element level even in this per-request form.
     *
     * @param buffer the APPEND_MERGE batch buffer populated by the classifier
     */
    public void dispatchAppendMerge(AppendMergeBatchBuffer buffer) {
        // Phase A.1 fast path: if every row has exactly 1 operand, use the batched FFI.
        // This is the common case for ListState.asyncAdd (one element per call).
        int count = buffer.count();
        if (count == 0) {
            return;
        }
        // B6-H1: a null entry in valueSliceLists signals "value lives in valueBuffer at this row"
        // (always single-operand by construction). Treat as 1-operand for the fast-path check.
        boolean allSingleOperand = true;
        List<MemorySegment[]> slices = buffer.valueSliceLists();
        for (int row = 0; row < count; row++) {
            MemorySegment[] vs = slices.get(row);
            if (vs != null && vs.length != 1) {
                allSingleOperand = false;
                break;
            }
        }
        if (allSingleOperand) {
            dispatchAppendMergeBatch(buffer);
            return;
        }
        // B-NEW-H3: multi-operand-per-row was previously dispatched one FFI
        // call per row by dispatchAppendMergePerRow. The new batched path
        // flattens N rows × M_i operands into one frsVecMergeAppendBatch
        // call by emitting M_i Merge entries per row with the SAME key
        // repeated (the engine's merge operator concatenates operands per
        // key in time-seq order — semantically identical to one merge
        // call with M operands). Single FFM crossing replaces N.
        dispatchAppendMergePerRowBatched(buffer);
    }

    /**
     * B-NEW-H3: batched multi-operand append-merge path. Flattens N rows × M_i operands into
     * one {@link ForStRsLinker#frsVecMergeAppendBatch} call by repeating each row's key for
     * every operand. Engine merge-operator semantics are preserved because Merge entries get
     * monotonically increasing seq numbers in submission order and the operator concatenates in
     * that order.
     */
    private void dispatchAppendMergePerRowBatched(AppendMergeBatchBuffer buffer) {
        int count = buffer.count();
        if (count == 0) {
            return;
        }
        long t0 = System.nanoTime();
        int rowsProcessed = 0;
        long bytesIn = 0;
        ColumnarBatchBuffer keyBuf = buffer.keyBuffer();
        ColumnarBatchBuffer valBuf = buffer.valueBuffer();
        List<MemorySegment[]> valueSliceLists = buffer.valueSliceLists();
        List<CompletableFuture<Void>> futures = buffer.futures();

        try (Arena scratch = Arena.ofConfined()) {
            try {
                // First pass: count total entries (sum of M_i) + compute total
                // key bytes + total op bytes.
                int totalEntries = 0;
                long totalKeyBytes = 0;
                long totalOpBytes = 0;
                for (int row = 0; row < count; row++) {
                    MemorySegment[] vsOrig = valueSliceLists.get(row);
                    int keyStart =
                            keyBuf.offsetsSegment()
                                    .get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                    int keyEnd =
                            keyBuf.offsetsSegment()
                                    .get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
                    int keyLen = keyEnd - keyStart;
                    int operandCount;
                    long rowOpBytes;
                    if (vsOrig != null) {
                        operandCount = vsOrig.length;
                        long sum = 0;
                        for (MemorySegment v : vsOrig) {
                            sum += v.byteSize();
                        }
                        rowOpBytes = sum;
                    } else {
                        // Heap-path: single operand from valBuf at this row.
                        int vStart =
                                valBuf.offsetsSegment()
                                        .get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                        int vEnd =
                                valBuf.offsetsSegment()
                                        .get(
                                                ValueLayout.JAVA_INT,
                                                (long) (row + 1) * Integer.BYTES);
                        operandCount = 1;
                        rowOpBytes = vEnd - vStart;
                    }
                    totalEntries += operandCount;
                    totalKeyBytes += (long) keyLen * operandCount;
                    totalOpBytes += rowOpBytes;
                    // B-NEW-H3 metrics: bytesIn = sum of (per-emission key + per-operand value).
                    bytesIn += (long) keyLen * operandCount + rowOpBytes;
                }

                // Allocate offsets+data segments.
                MemorySegment keysOff =
                        scratch.allocate((long) (totalEntries + 1) * Integer.BYTES);
                MemorySegment keysData = scratch.allocate(totalKeyBytes);
                MemorySegment opsOff =
                        scratch.allocate((long) (totalEntries + 1) * Integer.BYTES);
                MemorySegment opsData = scratch.allocate(totalOpBytes);

                // Second pass: populate.
                int entryIdx = 0;
                int kPos = 0;
                int oPos = 0;
                keysOff.set(ValueLayout.JAVA_INT, 0, 0);
                opsOff.set(ValueLayout.JAVA_INT, 0, 0);
                for (int row = 0; row < count; row++) {
                    int keyStart =
                            keyBuf.offsetsSegment()
                                    .get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                    int keyEnd =
                            keyBuf.offsetsSegment()
                                    .get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
                    int keyLen = keyEnd - keyStart;
                    MemorySegment keySlice = keyBuf.dataSegment().asSlice(keyStart, keyLen);

                    MemorySegment[] vsOrig = valueSliceLists.get(row);
                    if (vsOrig != null) {
                        for (MemorySegment v : vsOrig) {
                            int vLen = (int) v.byteSize();
                            MemorySegment.copy(keySlice, 0L, keysData, kPos, keyLen);
                            kPos += keyLen;
                            MemorySegment.copy(v, 0L, opsData, oPos, vLen);
                            oPos += vLen;
                            entryIdx++;
                            keysOff.set(
                                    ValueLayout.JAVA_INT,
                                    (long) entryIdx * Integer.BYTES,
                                    kPos);
                            opsOff.set(
                                    ValueLayout.JAVA_INT,
                                    (long) entryIdx * Integer.BYTES,
                                    oPos);
                        }
                    } else {
                        int vStart =
                                valBuf.offsetsSegment()
                                        .get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                        int vEnd =
                                valBuf.offsetsSegment()
                                        .get(
                                                ValueLayout.JAVA_INT,
                                                (long) (row + 1) * Integer.BYTES);
                        int vLen = vEnd - vStart;
                        MemorySegment.copy(keySlice, 0L, keysData, kPos, keyLen);
                        kPos += keyLen;
                        MemorySegment.copy(
                                valBuf.dataSegment(), vStart, opsData, oPos, vLen);
                        oPos += vLen;
                        entryIdx++;
                        keysOff.set(
                                ValueLayout.JAVA_INT,
                                (long) entryIdx * Integer.BYTES,
                                kPos);
                        opsOff.set(ValueLayout.JAVA_INT, (long) entryIdx * Integer.BYTES, oPos);
                    }
                }

                // Single FFM crossing for all rows' operands.
                int rc =
                        invokeVecMergeAppendBatch(
                                keysOff, keysData, opsOff, opsData, totalEntries);
                FrsErrorCode code = FrsErrorCode.fromU32(rc);
                if (code == FrsErrorCode.OK) {
                    for (CompletableFuture<Void> f : futures) {
                        f.complete(null);
                    }
                    rowsProcessed = totalEntries;
                } else if (code.isFailProcess()) {
                    if (metrics != null) {
                        metrics.recordFfiError(
                                VectorizedStateRequest.Kind.APPEND_MERGE, MIXED_STATE, code);
                    }
                    FrsEnginePanicError panicErr =
                            new FrsEnginePanicError(
                                    code, "kind=APPEND_MERGE (batched multi-operand)");
                    if (fatalHandler != null) {
                        fatalHandler.onFatalError(panicErr);
                    }
                    for (CompletableFuture<Void> f : futures) {
                        f.completeExceptionally(panicErr);
                    }
                } else {
                    if (metrics != null) {
                        metrics.recordFfiError(
                                VectorizedStateRequest.Kind.APPEND_MERGE, MIXED_STATE, code);
                    }
                    FrsException ex = new FrsException(code, 0, new byte[0]);
                    for (CompletableFuture<Void> f : futures) {
                        f.completeExceptionally(ex);
                    }
                }
            } catch (Throwable t) {
                for (CompletableFuture<Void> f : futures) {
                    if (!f.isDone()) {
                        f.completeExceptionally(t);
                    }
                }
                throw t;
            }
        }
        // B-NEW-H3 metrics: parity with legacy dispatchAppendMergePerRow at line ~1660.
        if (metrics != null) {
            metrics.recordDispatch(
                    VectorizedStateRequest.Kind.APPEND_MERGE,
                    MIXED_STATE,
                    rowsProcessed,
                    bytesIn,
                    System.nanoTime() - t0);
        }
    }

    /** Legacy per-row dispatch path. Kept for multi-operand-per-row requests until V20 closes. */
    private void dispatchAppendMergePerRow(AppendMergeBatchBuffer buffer) {
        int count = buffer.count();
        if (count == 0) {
            return;
        }
        long t0 = System.nanoTime();
        int rowsProcessed = 0;
        long bytesIn = 0;

        ColumnarBatchBuffer keyBuf = buffer.keyBuffer();
        ColumnarBatchBuffer valBuf = buffer.valueBuffer();
        List<MemorySegment[]> valueSliceLists = buffer.valueSliceLists();
        List<CompletableFuture<Void>> futures = buffer.futures();

        // A6-H3 / B6-H2 / D6-H1: outer per-batch Arena.ofConfined wraps the entire row
        // loop. All per-row scratch (operand_ptrs, operand_lens, copied operand bytes)
        // is reclaimed deterministically when the dispatch returns. The previous
        // implementation allocated from the executor's long-lived arena (B5-H4) which
        // leaked O(operand_bytes * batches) until backend dispose — auditors flagged
        // ~MB/s on 24h streaming. frsVecMergeAppend is synchronous so the scratch is
        // safe to close as soon as the loop returns.
        try (Arena scratch = Arena.ofConfined()) {
            for (int row = 0; row < count; row++) {
                MemorySegment[] vsOrig = valueSliceLists.get(row);
                CompletableFuture<Void> future = futures.get(row);

                // Extract key slice from the columnar key buffer.
                int keyStart =
                        keyBuf.offsetsSegment().get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                int keyEnd =
                        keyBuf.offsetsSegment()
                                .get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
                int keyLen = keyEnd - keyStart;
                MemorySegment keyPtr = keyBuf.dataSegment().asSlice(keyStart, keyLen);

                // R70-H1: handle heap-path rows where `valueSliceLists.get(row) == null`
                // signals "value lives in valueBuffer at this row index" (see
                // AppendMergeBatchBuffer.appendHeapRow). Pre-fix the per-row dispatcher
                // NPE'd on the `for (MemorySegment v : vs)` loop whenever a single
                // classifier drain interleaved `addAll` (multi-operand) and `add`
                // (heap-path) rows from different async-state instances. The NPE
                // escaped the inner future-drain catch, leaving subsequent futures
                // unresolved and wedging upstream callers.
                //
                // Resolution: synthesize a single-element MemorySegment[] from the
                // valBuf slice when vs is null. The frsVecMergeAppend FFI is called
                // identically — one operand with bytes from valBuf instead of vs[0].
                MemorySegment[] vs;
                if (vsOrig != null) {
                    vs = vsOrig;
                } else {
                    int vStart =
                            valBuf.offsetsSegment()
                                    .get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                    int vEnd =
                            valBuf.offsetsSegment()
                                    .get(ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
                    int vLen = vEnd - vStart;
                    vs = new MemorySegment[] {valBuf.dataSegment().asSlice(vStart, vLen)};
                }

                bytesIn += keyLen;
                for (MemorySegment v : vs) {
                    bytesIn += v.byteSize();
                }

                // A5-H3: the scratch allocations themselves can throw (OOM, IllegalStateException
                // if the arena is closed). The INNER try/catch wraps BOTH allocation AND the FFI
                // dispatch so a throw at any point drains every pending future in
                // futures[row..count-1] (not just the tail past a successful alloc). Without this
                // an OOM in `scratch.allocate(vLen)` would escape before completing any future,
                // wedging the operator on `futures[row..count-1]`. The outer try-with-resources
                // still closes the per-batch scratch arena via finally.
                try {
                    MemorySegment ptrs = scratch.allocate(ValueLayout.ADDRESS, vs.length);
                    MemorySegment lens = scratch.allocate(ValueLayout.JAVA_INT, vs.length);
                    for (int i = 0; i < vs.length; i++) {
                        long vLen = vs[i].byteSize();
                        MemorySegment nativeV = scratch.allocate(vLen);
                        MemorySegment.copy(vs[i], 0L, nativeV, 0L, vLen);
                        ptrs.setAtIndex(ValueLayout.ADDRESS, i, nativeV);
                        lens.setAtIndex(ValueLayout.JAVA_INT, i, (int) vLen);
                    }
                    int rc =
                            linker.frsVecMergeAppend(
                                    db.handle(),
                                    cf.handle(),
                                    keyPtr,
                                    keyLen,
                                    ptrs,
                                    lens,
                                    vs.length);
                    FrsErrorCode code = FrsErrorCode.fromU32(rc);
                    if (code == FrsErrorCode.OK) {
                        future.complete(null);
                        rowsProcessed += vs.length;
                    } else if (code.isFailProcess()) {
                        if (metrics != null) {
                            metrics.recordFfiError(
                                    VectorizedStateRequest.Kind.APPEND_MERGE, "_mixed", code);
                        }
                        FrsEnginePanicError panicErr =
                                new FrsEnginePanicError(code, "kind=APPEND_MERGE row=" + row);
                        if (fatalHandler != null) {
                            fatalHandler.onFatalError(panicErr);
                        }
                        future.completeExceptionally(panicErr);
                    } else {
                        if (metrics != null) {
                            metrics.recordFfiError(
                                    VectorizedStateRequest.Kind.APPEND_MERGE, "_mixed", code);
                        }
                        future.completeExceptionally(new FrsException(code, row, new byte[0]));
                    }
                } catch (Throwable t) {
                    // S1-9 + A5-H3: drain ALL pending futures (this row + the unprocessed tail)
                    // so no caller is left waiting. Order matters: complete `future` first (matches
                    // the request that actually triggered the throw), then the tail. Covers BOTH
                    // allocation failures (OOM, arena closed) AND FFI dispatch failures (panic
                    // upcall, linker RuntimeException). The outer try-with-resources will close
                    // `scratch` in its finally before the throw escapes.
                    if (!future.isDone()) {
                        future.completeExceptionally(t);
                    }
                    for (int r = row + 1; r < count; r++) {
                        CompletableFuture<Void> tail = futures.get(r);
                        if (!tail.isDone()) {
                            tail.completeExceptionally(
                                    new RuntimeException(
                                            "dispatchAppendMergePerRow aborted at row=" + row, t));
                        }
                    }
                    // Re-throw so the executor surfaces the fatal condition through the same
                    // path as a non-batched FFI failure (mirrors how `frsVecMergeAppend`
                    // non-OK return codes propagate to the upstream snapshot/close handler).
                    throw t;
                }
            }
        }

        if (metrics != null) {
            metrics.recordDispatch(
                    VectorizedStateRequest.Kind.APPEND_MERGE,
                    MIXED_STATE,
                    rowsProcessed,
                    bytesIn,
                    System.nanoTime() - t0);
        }
    }

    /**
     * Phase A.1 (audit-design §3 V4) — batched APPEND_MERGE dispatch.
     *
     * <p>Single FFI crossing for {@code count} rows. Each row's operand is the
     * caller-pre-encoded payload bytes (typically {@code [count=u32 LE][elem_bytes*]}
     * for ListState semantics — see {@link
     * org.apache.flink.state.forstrs.state.ForStRsAsyncListStateV2#asyncAdd}).
     *
     * <p>Per-row layout in {@code buffer}: each {@link AppendMergeBatchBuffer#valueSliceLists()}
     * entry must contain exactly ONE {@link MemorySegment} (the pre-encoded operand). The
     * batched FFI expects one operand per row — for multi-element {@code asyncAddAll}, the
     * caller should pre-concatenate elements into a single operand with {@code count=N}.
     *
     * <p>Called by {@link #dispatchAppendMerge} for the single-operand batch path; legacy
     * multi-operand rows still use the per-row dispatcher.
     *
     * @param buffer the APPEND_MERGE batch buffer populated by the classifier
     * @return native error code (0 = OK)
     */
    public int dispatchAppendMergeBatch(AppendMergeBatchBuffer buffer) {
        int count = buffer.count();
        if (count == 0) {
            return 0;
        }
        long t0 = System.nanoTime();
        long bytesIn = 0;

        ColumnarBatchBuffer keyBuf = buffer.keyBuffer();
        ColumnarBatchBuffer valBuf = buffer.valueBuffer(); // B6-H1: heap-path value column
        List<MemorySegment[]> valueSliceLists = buffer.valueSliceLists();
        List<CompletableFuture<Void>> futures = buffer.futures();

        // A8-M2: widen the try/catch added by A7-H1 to cover ALL allocation + dispatch work,
        // including {@link #flattenValueSlices}, {@link #flattenHeapFutures}, {@link
        // #ensureScratchOpsOffsets}, and the per-row opsTotal sizing loop. Any of these can
        // throw (OOM on the exponential-grow scratch arrays, panic upcalls from native code
        // touching valBuf, IndexOutOfBoundsException on malformed offsets). If they throw
        // BEFORE the inner try fires, the row futures held in {@code futures} are NEVER
        // completed and AsyncExecutionController hangs. Drain from the original {@code
        // futures} List on the early-throw path (futuresArr may not have been built yet);
        // once futuresArr is populated, prefer it because it's already a flat array.
        CompletableFuture<Void>[] futuresArr = null;
        int rc;
        FrsErrorCode code;
        try (Arena scratch = Arena.ofConfined()) {
            try {
                // B6-H5: index-based completion loop — flatten the per-row futures list into
                // a primitive array so the OK / error completion loops below don't allocate
                // an ArrayList.Itr.
                futuresArr = flattenHeapFutures(futures);

                MemorySegment valOffSeg = valBuf.offsetsSegment();
                MemorySegment opsOffSeg;
                MemorySegment opsDataSeg;
                if (allValuesInValueBuffer(valueSliceLists)) {
                    // The common Async ListState path already stores operands as an Arrow Binary
                    // column. Hand that column directly to native merge-append instead of copying
                    // it into a scratch ops buffer.
                    opsOffSeg = valOffSeg;
                    opsDataSeg = valBuf.dataSegment();
                    for (int row = 0; row < count; row++) {
                        int vStart =
                                valOffSeg.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                        int vEnd =
                                valOffSeg.get(
                                        ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
                        bytesIn += vEnd - vStart;
                    }
                } else {
                    // B6-H4: lift the per-row List.get(row) interface dispatch onto a flattened
                    // scratch array via the same exponential-grow pattern as flattenHeapFutures.
                    // Reused across batches.
                    MemorySegment[] scratchSlices = flattenValueSlices(valueSliceLists);

                    // A6-H3 / B6-H2 / D6-H1: per-batch Arena.ofConfined so opsOffSeg + opsDataSeg
                    // segments are reclaimed deterministically when the dispatch returns.
                    // B5-H7: opsOffsets uses the reusable scratchOpsOffsets field (grows on
                    // demand) so we don't `new int[count+1]` per dispatch.
                    int[] opsOffsets = ensureScratchOpsOffsets(count + 1);
                    int opsTotal = 0;
                    for (int row = 0; row < count; row++) {
                        MemorySegment vs = scratchSlices[row];
                        int opLen;
                        if (vs == null) {
                            int vStart =
                                    valOffSeg.get(
                                            ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                            int vEnd =
                                    valOffSeg.get(
                                            ValueLayout.JAVA_INT,
                                            (long) (row + 1) * Integer.BYTES);
                            opLen = vEnd - vStart;
                        } else {
                            opLen = (int) vs.byteSize();
                        }
                        opsOffsets[row] = opsTotal;
                        opsTotal += opLen;
                    }
                    opsOffsets[count] = opsTotal;

                    opsOffSeg = scratch.allocate(ValueLayout.JAVA_INT, count + 1L);
                    opsDataSeg = opsTotal == 0 ? MemorySegment.NULL : scratch.allocate(opsTotal);
                    int writeOff = 0;
                    MemorySegment valDataSeg = valBuf.dataSegment();
                    for (int row = 0; row < count; row++) {
                        opsOffSeg.set(
                                ValueLayout.JAVA_INT,
                                (long) row * Integer.BYTES,
                                opsOffsets[row]);
                        MemorySegment vs = scratchSlices[row];
                        long opLen;
                        if (vs == null) {
                            int vStart =
                                    valOffSeg.get(
                                            ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                            int vEnd =
                                    valOffSeg.get(
                                            ValueLayout.JAVA_INT,
                                            (long) (row + 1) * Integer.BYTES);
                            opLen = vEnd - vStart;
                            if (opLen > 0) {
                                MemorySegment.copy(
                                        valDataSeg, vStart, opsDataSeg, writeOff, opLen);
                            }
                        } else {
                            opLen = vs.byteSize();
                            if (opLen > 0) {
                                MemorySegment.copy(vs, 0L, opsDataSeg, writeOff, opLen);
                            }
                        }
                        writeOff += (int) opLen;
                        bytesIn += opLen;
                    }
                    opsOffSeg.set(ValueLayout.JAVA_INT, (long) count * Integer.BYTES, opsTotal);
                }

                // Keys are already in the columnar layout of keyBuf — no copy.
                MemorySegment keysOffSeg = keyBuf.offsetsSegment();
                MemorySegment keysDataSeg = keyBuf.dataSegment();
                for (int row = 0; row < count; row++) {
                    int kStart =
                            keysOffSeg.get(ValueLayout.JAVA_INT, (long) row * Integer.BYTES);
                    int kEnd =
                            keysOffSeg.get(
                                    ValueLayout.JAVA_INT, (long) (row + 1) * Integer.BYTES);
                    bytesIn += kEnd - kStart;
                }

                rc = invokeVecMergeAppendBatch(keysOffSeg, keysDataSeg, opsOffSeg, opsDataSeg, count);
                code = FrsErrorCode.fromU32(rc);
            } catch (Throwable t) {
                // A7-H1: B12-C consolidation introduced this per-batch Arena.ofConfined
                // block but lost the per-row try/catch that the legacy
                // dispatchAppendMergePerRow keeps (lines 856-878). If the FFI call
                // (linker.frsVecMergeAppendBatch) or any of the preceding scratch
                // allocations / MemorySegment.copy ops throw (RuntimeException, Error,
                // scratch OOM, panic upcall), the futures in futuresArr[*] would
                // NEVER be completed and callers would hang forever. Drain ALL
                // pending futures exceptionally before re-raising, mirroring the
                // per-row path's pattern. The outer try-with-resources closes
                // `scratch` in its finally before the throw escapes.
                //
                // A8-M2 widening: futuresArr may still be {@code null} if the throw came from
                // {@link #flattenValueSlices} or {@link #flattenHeapFutures} itself (the
                // latter constructs and returns futuresArr — a partial assignment is not
                // possible at the source level). In that case, drain from the original
                // List<CompletableFuture> the caller handed us.
                if (futuresArr != null) {
                    for (int r = 0; r < count; r++) {
                        CompletableFuture<Void> f = futuresArr[r];
                        if (f != null && !f.isDone()) {
                            f.completeExceptionally(t);
                        }
                    }
                } else {
                    int sz = futures.size();
                    for (int r = 0; r < sz; r++) {
                        CompletableFuture<Void> f = futures.get(r);
                        if (f != null && !f.isDone()) {
                            f.completeExceptionally(t);
                        }
                    }
                }
                if (metrics != null) {
                    metrics.recordFfiError(
                            VectorizedStateRequest.Kind.APPEND_MERGE,
                            "_batched",
                            FrsErrorCode.PANIC_CAUGHT);
                }
                throw t;
            }
        }

        if (code == FrsErrorCode.OK) {
            // B6-H5: index-based loop instead of ArrayList.Itr allocation.
            for (int i = 0; i < count; i++) {
                futuresArr[i].complete(null);
            }
        } else {
            Throwable err =
                    code.isFailProcess()
                            ? new FrsEnginePanicError(code, "kind=APPEND_MERGE_BATCH")
                            : new FrsException(code, -1, new byte[0]);
            if (code.isFailProcess() && fatalHandler != null) {
                fatalHandler.onFatalError((FrsEnginePanicError) err);
            }
            if (metrics != null) {
                metrics.recordFfiError(
                        VectorizedStateRequest.Kind.APPEND_MERGE, "_batched", code);
            }
            // B6-H5: index-based loop instead of ArrayList.Itr allocation.
            for (int i = 0; i < count; i++) {
                futuresArr[i].completeExceptionally(err);
            }
        }

        if (metrics != null) {
            metrics.recordDispatch(
                    VectorizedStateRequest.Kind.APPEND_MERGE,
                    MIXED_STATE,
                    count,
                    bytesIn,
                    System.nanoTime() - t0);
        }

        return rc;
    }

    /**
     * Dispatches an ITER_PREFIX batch via {@code frs_vec_iter_prefix_open} FFI (P5).
     *
     * <p>For each request in the buffer, opens a native prefix-bounded iterator, wraps it in an
     * {@link FrsIterHandle}, registers it with the {@link SlotArenaScope}, and completes the
     * request's future with an {@link IterPrefixRequest.IterFirstChunk} carrying the handle and
     * first-chunk row count.
     *
     * <p>Requires {@link #setSlotScope(SlotArenaScope)} to have been called beforehand; throws
     * {@link IllegalStateException} if the scope is not set.
     *
     * @param buffer the ITER_PREFIX batch buffer populated by the classifier
     */
    public void dispatchIterPrefix(IterPrefixBatchBuffer buffer) {
        if (slotScope == null) {
            throw new IllegalStateException(
                    "SlotArenaScope not set on VectorizedExecutor — call setSlotScope() "
                            + "before dispatching ITER_PREFIX requests");
        }
        long t0 = System.nanoTime();
        int rowsTotal = 0;
        long bytesIn = 0;

        List<MemorySegment> prefixSlices = buffer.prefixSlices();
        List<MemorySegment> chunkBufSlices = buffer.chunkBufSlices();
        List<CompletableFuture<IterPrefixRequest.IterFirstChunk>> futures = buffer.futures();
        int n = buffer.count();
        if (n == 0) {
            return;
        }

        // A6-H3 / B6-H2 / D6-H1 (revises PR-E3 / E-HIGH-5 / F5-4): the four buffer-plan
        // segments live as PERSISTENT executor-arena fields, grown on demand to the
        // largest batch ever observed and reused thereafter. This stops the monotonic
        // executor-arena growth that PR-E3 introduced when it lifted the per-request
        // Arena.ofShared() — now per-batch growth is amortized to O(log batch_max).
        //
        // Buffer plan (all reused from executor-arena fields):
        //   1. scratchPrefixesOff:  u32[n+1] packed offsets (grown via ensurePrefixesOff)
        //   2. scratchPrefixesData: u8[total_prefix_bytes]  (grown via ensurePrefixesData)
        //   3. scratchOutHandles:   u64[n] handle output    (grown via ensureOutHandles)
        //   4. scratchOutChunks:    FrsChunk[n] AoS         (grown via ensureOutChunks)
        //
        // Safe to reuse: the FrsIterHandle that borrows `arena` (ownsArena=false) does NOT
        // retain references to any of these scratch segments — it only stores the native
        // handle (long) read out of outHandles, and the chunk-buf reference comes from
        // chunkBufSlices (caller-owned). The engine copies handle/chunk metadata into
        // the segments synchronously during the open call and never touches them again.
        //
        // Chunk capacity policy: chunkBufSlices already come from a uniform 64KiB pool
        // (ForStRsDBIterRequest.CHUNK_BUF_CAP). We pick the first row's chunk size as the
        // uniform `chunkCap` parameter; the Rust side validates that every per-row
        // buf_cap matches and rejects malformed rows individually without aborting the
        // whole batch.
        int chunkCap = (int) chunkBufSlices.get(0).byteSize();

        // 1+2: pack the prefixes into SoA layout in the persistent executor-arena scratch.
        int totalPrefixBytes = 0;
        for (int i = 0; i < n; i++) {
            totalPrefixBytes += (int) prefixSlices.get(i).byteSize();
        }
        MemorySegment prefixesOff = ensurePrefixesOff((long) n + 1);
        MemorySegment prefixesData = ensurePrefixesData(totalPrefixBytes);
        int off = 0;
        prefixesOff.set(ValueLayout.JAVA_INT, 0L, 0);
        for (int i = 0; i < n; i++) {
            MemorySegment p = prefixSlices.get(i);
            int len = (int) p.byteSize();
            if (len > 0) {
                MemorySegment.copy(p, 0L, prefixesData, off, len);
            }
            off += len;
            prefixesOff.set(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES, off);
        }

        // 3: handles output array (n × u64) — persistent, grown on demand.
        MemorySegment outHandles = ensureOutHandles(n);

        // 4: FrsChunk AoS array — persistent, grown on demand; pre-fill buf_ptr + buf_cap
        // from per-row chunk buffers.
        long chunkStride = ForStRsLinker.frsChunkLayoutByteSize();
        MemorySegment outChunks = ensureOutChunks(n, chunkStride);
        for (int i = 0; i < n; i++) {
            MemorySegment chunkBuf = chunkBufSlices.get(i);
            ForStRsLinker.setFrsChunkBufPtr(outChunks, i, chunkBuf);
            ForStRsLinker.setFrsChunkBufCap(outChunks, i, (int) chunkBuf.byteSize());
        }

        // Single FFI crossing for N opens.  Critical mode: see linker bind comment.
        long openT0 = OC_DISPATCH ? System.nanoTime() : 0L;
        int rcBatch =
                linker.frsVecIterPrefixOpenBatch(
                        db.handle(),
                        cf.handle(),
                        prefixesOff,
                        prefixesData,
                        n,
                        outHandles,
                        outChunks,
                        chunkCap);
        if (OC_DISPATCH) {
            OC_IP_OPEN_NS.addAndGet(System.nanoTime() - openT0);
            OC_IP_OPENS.addAndGet(n);
        }

        FrsErrorCode batchCode = FrsErrorCode.fromU32(rcBatch);
        // Even on batch-level non-Ok, per-row handles may be populated for the rows that
        // did succeed — Rust impl writes 0 to outHandles[i] for failed rows. We must
        // process each row independently.

        for (int row = 0; row < n; row++) {
            CompletableFuture<IterPrefixRequest.IterFirstChunk> future = futures.get(row);
            long nativeHandle = outHandles.get(ValueLayout.JAVA_LONG, (long) row * Long.BYTES);
            int firstChunkRows = ForStRsLinker.getFrsChunkRowCount(outChunks, row);
            int firstChunkBytes = ForStRsLinker.getFrsChunkBytesUsed(outChunks, row);

            if (nativeHandle == 0L) {
                // Per-row failure — propagate the batch-level code (best-effort
                // attribution; the Rust impl returns the FIRST per-row error code).
                FrsErrorCode rowCode = batchCode != FrsErrorCode.OK ? batchCode : FrsErrorCode.UNKNOWN;
                // R92-H1: escalate fail-process codes (e.g. PANIC_CAUGHT=900)
                // via the fatal handler — sister to the GET path at line
                // 828-836 and APPEND_MERGE at 1102-1111. Pre-fix the iter
                // path completed the row future with a plain FrsException
                // and the engine kept running on potentially poisoned state.
                //
                // R93-L1: only escalate when batchCode was actually
                // fail-process. The UNKNOWN fallback (when batchCode==OK
                // but a per-row handle is unexpectedly null — a defensive
                // path that should never fire per the Rust impl) returns
                // `UNKNOWN(999)` which has `isFailProcess() == true`; we
                // must NOT escalate that case to fatalHandler because
                // it's a per-row anomaly, not an engine panic.
                if (rowCode.isFailProcess() && batchCode != FrsErrorCode.OK) {
                    FrsEnginePanicError panicErr =
                            new FrsEnginePanicError(
                                    rowCode, "kind=ITER_PREFIX row=" + row);
                    if (fatalHandler != null) {
                        fatalHandler.onFatalError(panicErr);
                    }
                    future.completeExceptionally(panicErr);
                } else {
                    future.completeExceptionally(new FrsException(rowCode, row, new byte[0]));
                }
                if (metrics != null) {
                    metrics.recordFfiError(
                            VectorizedStateRequest.Kind.ITER_PREFIX, "_mixed", rowCode);
                }
                continue;
            }

            rowsTotal += firstChunkRows;
            bytesIn += firstChunkBytes;

            // P0 NOTE (integration follow-up, not wired here): when
            // ForStRsLinker.isFrsChunkEof(outChunks, row) is true the engine auto-closed
            // this row's iterator — the FrsIterHandle path below stays CORRECT unchanged
            // (its eventual next() reports clean EOF and close() is a safe no-op), but a
            // follow-up could skip the FrsIterHandle/slotScope registration and complete
            // the future as already-exhausted to save the trailing next()+close()
            // crossings on this legacy batched-open path too.
            long jHandleId = nextIterHandleId.incrementAndGet();
            // PR-E3 zero-memory-copy gate: the FrsIterHandle borrows the executor's
            // long-lived arena (ownsArena=false) so close() does NOT close it.  The
            // next() scratch (outRc, outBu — 8 bytes per next call) is allocated from
            // the same executor arena via the borrowed reference.
            FrsIterHandle fh =
                    new FrsIterHandle(
                            jHandleId,
                            nativeHandle,
                            linker,
                            arena,
                            slotScope,
                            /* ownsArena= */ false);
            slotScope.registerIter(fh);
            if (metrics != null) {
                metrics.recordIterHandlesOpened();
            }
            future.complete(new IterPrefixRequest.IterFirstChunk(fh, firstChunkRows));
        }

        if (metrics != null) {
            metrics.recordDispatch(
                    VectorizedStateRequest.Kind.ITER_PREFIX,
                    MIXED_STATE,
                    rowsTotal,
                    bytesIn,
                    System.nanoTime() - t0);
        }
        if (OC_DISPATCH) {
            OC_IP_TOTAL_NS.addAndGet(System.nanoTime() - t0);
            ocIterPrefixMaybeDump();
        }
    }

    /**
     * Dispatches an ITER_RANGE batch via {@code frs_vec_iter_range_open} FFI (P9).
     *
     * <p>For each request in the buffer, opens a native range-bounded iterator over [lo, hi), wraps
     * it in an {@link FrsIterHandle}, registers it with the {@link SlotArenaScope}, and completes
     * the request's future with an {@link IterRangeRequest.IterFirstChunk} carrying the handle and
     * first-chunk row count.
     *
     * <p>Requires {@link #setSlotScope(SlotArenaScope)} to have been called beforehand; throws
     * {@link IllegalStateException} if the scope is not set.
     *
     * @param buffer the ITER_RANGE batch buffer populated by the classifier
     */
    public void dispatchIterRange(IterRangeBatchBuffer buffer) {
        if (slotScope == null) {
            throw new IllegalStateException(
                    "SlotArenaScope not set on VectorizedExecutor — call setSlotScope() "
                            + "before dispatching ITER_RANGE requests");
        }
        long t0 = System.nanoTime();
        int rowsTotal = 0;
        long bytesIn = 0;

        List<MemorySegment> loSlices = buffer.loSlices();
        List<MemorySegment> hiSlices = buffer.hiSlices();
        List<MemorySegment> chunkBufSlices = buffer.chunkBufSlices();
        List<CompletableFuture<IterRangeRequest.IterFirstChunk>> futures = buffer.futures();

        for (int row = 0; row < buffer.count(); row++) {
            MemorySegment lo = loSlices.get(row);
            MemorySegment hi = hiSlices.get(row);
            MemorySegment chunkBuf = chunkBufSlices.get(row);
            CompletableFuture<IterRangeRequest.IterFirstChunk> future = futures.get(row);

            // A6-H3 / B6-H2 / D6-H1 (revises D5-H3 + B5-H4): the 3 out-param scratch
            // segments are PERSISTENT executor-arena fields allocated once at construction.
            // Previously each iter-range open allocated 16 bytes of fresh executor-arena
            // scratch — monotonic growth that auditors flagged on 24h streaming jobs.
            // Safe to reuse: open is synchronous (engine writes the handle/row-count/
            // bytes-used BEFORE returning) and the FrsIterHandle does NOT retain a
            // reference to these segments — the row-count and bytes-used are read out
            // immediately below and copied to local ints. ownsArena=false on the handle
            // ensures handle.close() does NOT touch the executor arena.
            MemorySegment outHandle = scratchIterRangeHandle;
            MemorySegment outRowCount = scratchIterRangeRowCount;
            MemorySegment outBytesUsed = scratchIterRangeBytesUsed;

            int rc =
                    linker.frsVecIterRangeOpen(
                            db.handle(),
                            cf.handle(),
                            lo,
                            (int) lo.byteSize(),
                            hi,
                            (int) hi.byteSize(),
                            chunkBuf,
                            (int) chunkBuf.byteSize(),
                            outHandle,
                            outRowCount,
                            outBytesUsed);

            FrsErrorCode code = FrsErrorCode.fromU32(rc);
            if (code != FrsErrorCode.OK) {
                // R92-H1 (companion): escalate fail-process codes via
                // fatalHandler — sister to dispatchIterPrefix above and
                // GET / APPEND_MERGE paths.
                if (code.isFailProcess()) {
                    FrsEnginePanicError panicErr =
                            new FrsEnginePanicError(
                                    code, "kind=ITER_RANGE row=" + row);
                    if (fatalHandler != null) {
                        fatalHandler.onFatalError(panicErr);
                    }
                    future.completeExceptionally(panicErr);
                } else {
                    future.completeExceptionally(new FrsException(code, row, new byte[0]));
                }
                if (metrics != null) {
                    metrics.recordFfiError(
                            VectorizedStateRequest.Kind.ITER_RANGE, MIXED_STATE, code);
                }
                continue;
            }

            long nativeHandle = outHandle.get(ValueLayout.JAVA_LONG, 0);
            int firstChunkRows = outRowCount.get(ValueLayout.JAVA_INT, 0);
            int firstChunkBytes = outBytesUsed.get(ValueLayout.JAVA_INT, 0);
            rowsTotal += firstChunkRows;
            bytesIn += firstChunkBytes;

            // P0 NOTE: nativeHandle may now be 0 — the engine auto-closed an
            // exhausted-at-open range iterator (rows are already in chunkBuf). The
            // FrsIterHandle below remains CORRECT with nativeHandle == 0: next(0)
            // reports clean EOF and close(0) is a no-op. A follow-up can complete the
            // future as already-exhausted to skip those two crossings.
            long jHandleId = nextIterHandleId.incrementAndGet();
            // ownsArena=false: arena is the executor-owned long-lived arena and must NOT
            // be closed when this handle closes (mirrors PR-E3 dispatchIterPrefix).
            FrsIterHandle fh =
                    new FrsIterHandle(
                            jHandleId,
                            nativeHandle,
                            linker,
                            arena,
                            slotScope,
                            /* ownsArena= */ false);
            slotScope.registerIter(fh);
            if (metrics != null) {
                metrics.recordIterHandlesOpened();
            }
            future.complete(new IterRangeRequest.IterFirstChunk(fh, firstChunkRows));
        }

        if (metrics != null) {
            metrics.recordDispatch(
                    VectorizedStateRequest.Kind.ITER_RANGE,
                    MIXED_STATE,
                    rowsTotal,
                    bytesIn,
                    System.nanoTime() - t0);
        }
    }

    // -----------------------------------------------------------------
    // FFI invocation seams — overridable for tests so we can stub the
    // linker's batch entry points without subclassing the final
    // ForStRsLinker class. Production code always calls the real linker.
    // -----------------------------------------------------------------

    /**
     * PR-A10 / S1-9 test seam: invoked from {@link #executePuts(VectorizedClassifier)}. Production
     * impl forwards to {@link ForStRsLinker#vectorizedBatchPut}; tests may override to throw a
     * deterministic exception and verify per-row future propagation.
     */
    protected void invokeVectorizedBatchPut(
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            MemorySegment valOffsetsSeg,
            MemorySegment valDataSeg,
            long count) {
        linker.vectorizedBatchPut(
                db, cf, keyOffsetsSeg, keyDataSeg, valOffsetsSeg, valDataSeg, count);
    }

    /**
     * PR-A10 / S1-9 test seam: invoked from {@link #executeDeletes(VectorizedClassifier)}.
     * Production impl forwards to {@link ForStRsLinker#vectorizedBatchDelete}; tests may override
     * to throw a deterministic exception and verify per-row future propagation.
     */
    protected void invokeVectorizedBatchDelete(
            MemorySegment keyOffsetsSeg, MemorySegment keyDataSeg, long count) {
        linker.vectorizedBatchDelete(db, cf, keyOffsetsSeg, keyDataSeg, count);
    }

    /**
     * Stage-3 Unit-2 test seam: invoked from {@link #executeMixedWrites(VectorizedClassifier)}.
     * Production impl forwards to {@link ForStRsLinker#vectorizedBatchMixed}; tests may override
     * to capture segments (zero-copy assertion) or throw (per-row failure propagation).
     */
    protected void invokeVectorizedBatchMixed(
            MemorySegment kindsSeg,
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            MemorySegment valOffsetsSeg,
            MemorySegment valDataSeg,
            long count) {
        linker.vectorizedBatchMixed(
                db, cf, kindsSeg, keyOffsetsSeg, keyDataSeg, valOffsetsSeg, valDataSeg, count);
    }

    /**
     * PR-A10 / S1-9 test seam: invoked from {@link #executeGets(VectorizedClassifier)}. Production
     * impl forwards to {@link ForStRsLinker#vectorizedBatchGet}; tests may override to throw a
     * deterministic exception and verify per-row future propagation.
     */
    protected int invokeVectorizedBatchGet(
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            long count,
            MemorySegment outOffsetsSeg,
            MemorySegment outDataSeg,
            MemorySegment outValiditySeg,
            long outDataCapArg,
            MemorySegment outDataLenSegArg) {
        return linker.vectorizedBatchGet(
                db,
                cf,
                keyOffsetsSeg,
                keyDataSeg,
                count,
                outOffsetsSeg,
                outDataSeg,
                outValiditySeg,
                outDataCapArg,
                outDataLenSegArg);
    }

    /** Test seam for batched APPEND_MERGE dispatch. */
    protected int invokeVecMergeAppendBatch(
            MemorySegment keysOffSeg,
            MemorySegment keysDataSeg,
            MemorySegment opsOffSeg,
            MemorySegment opsDataSeg,
            int count) {
        return linker.frsVecMergeAppendBatch(
                db.handle(), cf.handle(), keysOffSeg, keysDataSeg, opsOffSeg, opsDataSeg, count);
    }

    // -----------------------------------------------------------------
    // Future completion
    // -----------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static void completePut(StateRequest<?, ?, ?, ?> request) {
        if (DiagCompletionCounters.ENABLED) {
            DiagCompletionCounters.completed(request.getRequestType());
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).complete(null);
    }

    /**
     * Round-1 fix A1-H5 + Round-2 fix A2-H2: propagate FFI / engine failures to the
     * Flink-runtime async future. Preserves the original cause type (FrsException /
     * FrsEnginePanicError) so per-row diagnostics — including {@code FrsErrorCode} and
     * row index — are not lost. Does NOT re-invoke the fatal handler when the cause
     * is already a {@link FrsEnginePanicError}: the dispatcher fired it once and
     * downstream layers must not double-escalate.
     */
    /**
     * R99-H1 + R100-H1 + R100-M1 + R100-L1: drain ALL remaining per-row
     * futures in a classifier after the outer pipeline catch fires. The
     * executor runs `executeXxx` methods sequentially (PUT, DEL, GET,
     * ITER, APPEND_MERGE); each only drains its own kind on throw, so a
     * throw mid-pipeline leaves un-dispatched kinds' futures unresolved.
     * The framework caller ignores the container future and drives
     * next-op scheduling off per-row {@link InternalAsyncFuture}
     * completion — any unresolved future indefinitely blocks the next
     * op on its key, causing operator hang.
     *
     * <p>R100-M1: per-row exceptional completion is gated behind {@link
     * VectorizedClassifier#takeClassifierCompletedExceptionally}, which
     * returns {@code true} when the request was ALREADY drained by a
     * per-method catch (executePuts/executeDeletes/executeIters' inner
     * `markCompletedExceptionally` set the marker). Skipping those
     * preserves the R21-H1 anti-double-fire contract that
     * `AsyncFutureImpl.completeExceptionally` would otherwise violate
     * by re-firing the framework exception handler.
     *
     * <p>R100-H1: include APPEND_MERGE per-row `StateRequest`s
     * (`appendMergeRequests` array) in the drain, not just the buffer's
     * heap-path tracking futures — those are different objects from
     * the row's `StateRequest.getFuture()`.
     */
    private static void drainPendingFuturesExceptionally(
            VectorizedClassifier classifier, Throwable cause) {
        if (classifier == null) {
            return;
        }
        classifier.discardOffHeapAppendMergeRows(cause);
        // GET rows
        StateRequest<?, ?, ?, ?>[] getReqs = classifier.getRequests();
        ForStRsInnerTable<?, ?, ?>[] getTables = classifier.getTables();
        int getCount = classifier.getCount();
        for (int i = 0; i < getCount && getReqs != null && i < getReqs.length; i++) {
            try {
                if (classifier.takeClassifierCompletedExceptionally(getReqs[i])) {
                    continue;
                }
                classifier.markCompletedExceptionally(getReqs[i]);
                completeGetExceptionally(getReqs[i], getTables[i], cause);
            } catch (Throwable ignore) {
                // best-effort drain — never mask the primary cause
            }
        }
        // PUT rows
        StateRequest<?, ?, ?, ?>[] putReqs = classifier.putRequests();
        int putCount = classifier.putCount();
        for (int i = 0; i < putCount && putReqs != null && i < putReqs.length; i++) {
            try {
                // R100-M1: skip rows already drained by `executePuts` catch.
                if (classifier.takeClassifierCompletedExceptionally(putReqs[i])) {
                    continue;
                }
                classifier.markCompletedExceptionally(putReqs[i]);
                completePutExceptionally(putReqs[i], cause);
            } catch (Throwable ignore) {
                // best-effort drain — never mask the primary cause
            }
        }
        // DELETE rows
        StateRequest<?, ?, ?, ?>[] delReqs = classifier.deleteRequests();
        int delCount = classifier.deleteCount();
        for (int i = 0; i < delCount && delReqs != null && i < delReqs.length; i++) {
            try {
                if (classifier.takeClassifierCompletedExceptionally(delReqs[i])) {
                    continue;
                }
                classifier.markCompletedExceptionally(delReqs[i]);
                completeDeleteExceptionally(delReqs[i], cause);
            } catch (Throwable ignore) {
                // best-effort drain
            }
        }
        // ITER rows
        for (ForStRsDBIterRequest<?, ?, ?, ?> iter : classifier.iterRequests()) {
            try {
                StateRequest<?, ?, ?, ?> sr = iter.getStateRequest();
                if (sr != null
                        && classifier.takeClassifierCompletedExceptionally(sr)) {
                    continue;
                }
                if (sr != null) {
                    classifier.markCompletedExceptionally(sr);
                }
                iter.completeExceptionally(cause);
            } catch (Throwable ignore) {
                // best-effort drain
            }
        }
        // R100-H1: APPEND_MERGE row StateRequests. These are tracked
        // separately from the buffer's heap-path `futures()` list —
        // the buffer futures fire when the batched FFI flush completes
        // (drained by dispatchAppendMerge's internal catch), whereas
        // the row StateRequest.getFuture() resolves only in the
        // post-dispatch loop at executeBatchRequests:432-438. A throw
        // INSIDE dispatchAppendMerge skips that loop and leaves the
        // row StateRequests unresolved → operator hang.
        StateRequest<?, ?, ?, ?>[] amReqs = classifier.appendMergeRequests();
        int amCount = classifier.appendMergeCount();
        for (int i = 0; i < amCount && amReqs != null && i < amReqs.length; i++) {
            try {
                if (classifier.takeClassifierCompletedExceptionally(amReqs[i])) {
                    continue;
                }
                classifier.markCompletedExceptionally(amReqs[i]);
                // AM rows complete via the PUT-shaped path (the API
                // returns Void to the caller, mirroring add/addAll).
                completePutExceptionally(amReqs[i], cause);
            } catch (Throwable ignore) {
                // best-effort drain
            }
        }
        // APPEND_MERGE buffer heap-path tracking futures. Idempotent:
        // `isDone()` guard skips entries already drained by
        // dispatchAppendMerge's internal catch.
        AppendMergeBatchBuffer amBuf = classifier.appendMergeBuffer();
        if (amBuf != null) {
            for (CompletableFuture<Void> f : amBuf.futures()) {
                try {
                    if (f != null && !f.isDone()) {
                        f.completeExceptionally(cause);
                    }
                } catch (Throwable ignore) {
                    // best-effort drain
                }
            }
        }
        // B-R5-NEW-H1: ITER_PREFIX + ITER_RANGE buffer per-row futures.
        // Pre-fix dispatchIterPrefix / dispatchIterRange had no surrounding
        // try/catch and the outer drain walked only iterRequests() (legacy
        // ForStRsDBIterRequest), missing the off-heap-buffer per-row futures.
        // A panic-upcall before per-row completion would leave them
        // unresolved → operator hang. Drain is idempotent via isDone().
        IterPrefixBatchBuffer ipDrainBuf = classifier.iterPrefixBuffer();
        if (ipDrainBuf != null) {
            for (CompletableFuture<IterPrefixRequest.IterFirstChunk> f : ipDrainBuf.futures()) {
                try {
                    if (f != null && !f.isDone()) {
                        f.completeExceptionally(cause);
                    }
                } catch (Throwable ignore) {
                    // best-effort drain
                }
            }
        }
        IterRangeBatchBuffer irDrainBuf = classifier.iterRangeBuffer();
        if (irDrainBuf != null) {
            for (CompletableFuture<IterRangeRequest.IterFirstChunk> f : irDrainBuf.futures()) {
                try {
                    if (f != null && !f.isDone()) {
                        f.completeExceptionally(cause);
                    }
                } catch (Throwable ignore) {
                    // best-effort drain
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void completePutExceptionally(
            StateRequest<?, ?, ?, ?> request, Throwable cause) {
        if (DiagCompletionCounters.ENABLED) {
            DiagCompletionCounters.failed(request.getRequestType());
        }
        // Use the cause's own message if specific (FrsException/FrsEnginePanicError
        // include rc + row index); fall back to a generic label only when message is empty.
        String msg = cause.getMessage();
        if (msg == null || msg.isEmpty()) {
            msg = "ForSt-RS dispatch failed: " + cause.getClass().getSimpleName();
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).completeExceptionally(msg, cause);
    }

    /**
     * PR-B1 (V2-6, C-H1, C-H6): zero-copy GET-result completion. The decoded value is read
     * directly off the native {@code outData} segment via the table's {@link
     * ForStRsInnerTable#deserializeValue(MemorySegment, long, int)} overload — no per-row
     * {@code byte[]} allocation. {@code present} encodes the validity bit (null result vs
     * empty value); MAP_CONTAINS resolves to a boolean derived from {@code present} without
     * touching the bytes.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void completeGet(
            StateRequest<?, ?, ?, ?> request,
            ForStRsInnerTable table,
            boolean present,
            MemorySegment data,
            long offset,
            int len) {
        if (DiagCompletionCounters.ENABLED) {
            DiagCompletionCounters.completed(request.getRequestType());
        }
        Object result;
        StateRequestType type = request.getRequestType();
        if (type == StateRequestType.MAP_CONTAINS) {
            result = present;
        } else if (!present) {
            result = null;
        } else {
            result = table.deserializeValue(data, offset, len);
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).complete(result);
    }

    /**
     * PR-A10 / S1-9: propagate a GET-path FFI / decode failure to the per-row StateRequest future.
     * Mirrors {@link #completeGet} but completes the future exceptionally. The {@code table}
     * argument is accepted for symmetry with {@link #completeGet} (and to mirror the call sites)
     * but is currently unused — completion does not need to materialize a value on the failure
     * path.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void completeGetExceptionally(
            StateRequest<?, ?, ?, ?> request, ForStRsInnerTable table, Throwable cause) {
        if (DiagCompletionCounters.ENABLED) {
            DiagCompletionCounters.failed(request.getRequestType());
        }
        String msg = cause.getMessage();
        if (msg == null || msg.isEmpty()) {
            msg = "ForSt-RS GET dispatch failed: " + cause.getClass().getSimpleName();
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).completeExceptionally(msg, cause);
    }

    /**
     * PR-A10 / S1-9: DELETE futures are {@code <Void>} so completion is a null-result. Distinct
     * from {@link #completePut} only for call-site clarity at the dispatcher.
     */
    @SuppressWarnings("unchecked")
    private static void completeDelete(StateRequest<?, ?, ?, ?> request) {
        if (DiagCompletionCounters.ENABLED) {
            DiagCompletionCounters.completed(request.getRequestType());
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).complete(null);
    }

    /**
     * PR-A10 / S1-9: propagate a DELETE-path FFI failure to the per-row StateRequest future. Same
     * shape as {@link #completePutExceptionally} but distinct for call-site clarity.
     */
    @SuppressWarnings("unchecked")
    private static void completeDeleteExceptionally(
            StateRequest<?, ?, ?, ?> request, Throwable cause) {
        if (DiagCompletionCounters.ENABLED) {
            DiagCompletionCounters.failed(request.getRequestType());
        }
        String msg = cause.getMessage();
        if (msg == null || msg.isEmpty()) {
            msg = "ForSt-RS DELETE dispatch failed: " + cause.getClass().getSimpleName();
        }
        ((InternalAsyncFuture<Object>) request.getFuture()).completeExceptionally(msg, cause);
    }

    // -----------------------------------------------------------------
    // Scratch-array sizing (B5-H7)
    // -----------------------------------------------------------------

    /**
     * B5-H7: flatten the heap-path APPEND_MERGE futures into a primitive array, sized at least
     * {@code count}. Reuses {@link #scratchHeapFutures} across batches via an exponential
     * grow-on-demand pattern. The caller indexes the returned array directly to avoid the
     * per-row {@code List.get()} interface dispatch.
     *
     * <p>Returns the (possibly grown) scratch array. Callers must only read indices
     * {@code [0, count)}; trailing entries are stale from prior dispatches.
     */
    private CompletableFuture<Void>[] flattenHeapFutures(
            List<CompletableFuture<Void>> heapFutures) {
        int n = heapFutures.size();
        if (scratchHeapFutures.length < n) {
            int newCap = Math.max(16, scratchHeapFutures.length);
            while (newCap < n) {
                newCap <<= 1;
            }
            @SuppressWarnings("unchecked")
            CompletableFuture<Void>[] grown =
                    (CompletableFuture<Void>[]) new CompletableFuture<?>[newCap];
            scratchHeapFutures = grown;
            // R21-M2: grown array starts entirely null — no stale refs to clear.
            scratchHeapFuturesPrevSize = 0;
        }
        for (int i = 0; i < n; i++) {
            scratchHeapFutures[i] = heapFutures.get(i);
        }
        // R21-M2: null out trailing slots from a prior LARGER dispatch so the JVM doesn't
        // pin CompletableFuture (and the chained StateRequest / payload they reference)
        // beyond their useful lifetime. Bound the fill to {@code [n, prevSize)} so we
        // don't redundantly walk slots that were never written.
        if (scratchHeapFuturesPrevSize > n) {
            java.util.Arrays.fill(scratchHeapFutures, n, scratchHeapFuturesPrevSize, null);
        }
        scratchHeapFuturesPrevSize = n;
        return scratchHeapFutures;
    }

    /**
     * B6-H4: flatten the per-row first-operand {@link MemorySegment} into a primitive array,
     * sized at least {@code count}. For B6-H1 heap-path rows (entry == null) the slot is left
     * null and {@link #dispatchAppendMergeBatch} reads the bytes from {@link
     * AppendMergeBatchBuffer#valueBuffer()} at the same row index. Reuses
     * {@link #scratchValueSlices} across batches via exponential grow-on-demand.
     *
     * <p>Returns the (possibly grown) scratch array. Callers must only read indices
     * {@code [0, count)}; trailing entries are stale from prior dispatches.
     */
    private MemorySegment[] flattenValueSlices(List<MemorySegment[]> valueSliceLists) {
        int n = valueSliceLists.size();
        if (scratchValueSlices.length < n) {
            int newCap = Math.max(16, scratchValueSlices.length);
            while (newCap < n) {
                newCap <<= 1;
            }
            scratchValueSlices = new MemorySegment[newCap];
            // R21-M2: grown array starts entirely null — no stale refs to clear.
            scratchValueSlicesPrevSize = 0;
        }
        for (int i = 0; i < n; i++) {
            MemorySegment[] vs = valueSliceLists.get(i);
            // Single-operand contract: null marker (B6-H1) or 1-element array. Multi-operand
            // requests are routed to dispatchAppendMergePerRow upstream by dispatchAppendMerge.
            scratchValueSlices[i] = vs == null ? null : vs[0];
        }
        // R21-M2: null out trailing slots from a prior LARGER dispatch so MemorySegment views
        // over executor-arena memory don't linger as GC roots past their useful lifetime.
        if (scratchValueSlicesPrevSize > n) {
            java.util.Arrays.fill(scratchValueSlices, n, scratchValueSlicesPrevSize, null);
        }
        scratchValueSlicesPrevSize = n;
        return scratchValueSlices;
    }

    private static boolean allValuesInValueBuffer(List<MemorySegment[]> valueSliceLists) {
        int n = valueSliceLists.size();
        for (int i = 0; i < n; i++) {
            if (valueSliceLists.get(i) != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * B5-H7: ensure {@link #scratchOpsOffsets} holds at least {@code needed} ints. Grown with
     * the same exponential strategy as {@link #ensureOutCapacity}.
     */
    private int[] ensureScratchOpsOffsets(int needed) {
        if (scratchOpsOffsets.length >= needed) {
            return scratchOpsOffsets;
        }
        int newCap = Math.max(16, scratchOpsOffsets.length);
        while (newCap < needed) {
            newCap <<= 1;
        }
        scratchOpsOffsets = new int[newCap];
        return scratchOpsOffsets;
    }

    // -----------------------------------------------------------------
    // A6-H3 / B6-H2 / D6-H1: persistent iter-prefix scratch growth helpers.
    // Each segment is reused across dispatches; growth is exponential so the
    // executor arena's bump-allocation cost is amortized to O(log batch_max).
    // -----------------------------------------------------------------

    private MemorySegment ensurePrefixesOff(long neededEntries) {
        if (scratchPrefixesOffCap >= neededEntries) {
            return scratchPrefixesOff;
        }
        long newCap = Math.max(16L, scratchPrefixesOffCap);
        while (newCap < neededEntries) {
            newCap <<= 1;
        }
        scratchPrefixesOff = arena.allocate(ValueLayout.JAVA_INT, newCap);
        scratchPrefixesOffCap = newCap;
        return scratchPrefixesOff;
    }

    private MemorySegment ensurePrefixesData(long neededBytes) {
        if (neededBytes == 0L) {
            return MemorySegment.NULL;
        }
        if (scratchPrefixesDataCap >= neededBytes) {
            return scratchPrefixesData;
        }
        long newCap = Math.max(64L, scratchPrefixesDataCap);
        while (newCap < neededBytes) {
            newCap <<= 1;
        }
        scratchPrefixesData = arena.allocate(newCap);
        scratchPrefixesDataCap = newCap;
        return scratchPrefixesData;
    }

    private MemorySegment ensureOutHandles(long neededEntries) {
        if (scratchOutHandlesCap >= neededEntries) {
            return scratchOutHandles;
        }
        long newCap = Math.max(16L, scratchOutHandlesCap);
        while (newCap < neededEntries) {
            newCap <<= 1;
        }
        scratchOutHandles = arena.allocate(ValueLayout.JAVA_LONG, newCap);
        scratchOutHandlesCap = newCap;
        return scratchOutHandles;
    }

    private MemorySegment ensureOutChunks(long neededEntries, long chunkStride) {
        if (scratchOutChunksCap >= neededEntries) {
            return scratchOutChunks;
        }
        long newCap = Math.max(16L, scratchOutChunksCap);
        while (newCap < neededEntries) {
            newCap <<= 1;
        }
        scratchOutChunks = arena.allocate(chunkStride * newCap);
        scratchOutChunksCap = newCap;
        return scratchOutChunks;
    }

    // -----------------------------------------------------------------
    // Output-buffer sizing
    // -----------------------------------------------------------------

    private void ensureOutCapacity(int slots) {
        if (slots <= outSlotsCap) {
            return;
        }
        int newCap = outSlotsCap;
        while (newCap < slots) {
            newCap <<= 1;
        }
        outOffsets = arena.allocate(ValueLayout.JAVA_INT, (long) newCap + 1);
        outValidity = arena.allocate(newCap);
        outSlotsCap = newCap;
    }
}
