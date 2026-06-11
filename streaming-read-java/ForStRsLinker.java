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

package org.apache.flink.state.forstrs.ffm;

import org.apache.flink.state.forstrs.FrsBackendException;
import org.apache.flink.state.forstrs.FrsStatus;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * JDK 25 FFM bridge to libforst_rs_ffi.{dylib,so,dll}.
 *
 * <p>Loads the cdylib and binds downcall MethodHandles for the lifecycle, point ops, flush /
 * checkpoint / metadata, and Delta-Join lookup-iterator surface needed by the embedded local-lookup
 * function.
 *
 * <p>Library lookup order:
 *
 * <ol>
 *   <li>System property {@code forstrs.native.libpath} (absolute path)
 *   <li>{@code System.loadLibrary("forst_rs_ffi")} fallback
 * </ol>
 *
 * <p>Lifetime: native libraries are loaded through a JVM-global lookup cache. The {@link Arena}
 * passed to the constructor still owns call-local/native object memory, but closing it must not
 * unload the process-wide ForSt-RS cdylib while Rust background threads or TLS destructors may
 * still run.
 *
 * <p>Reference: docs/design/2.5_ffm_bridge_design.md §3 and
 * docs/design/2.13_deltajoin_localization.md.
 */
public final class ForStRsLinker {

    /** {@code FrsBytes} struct layout: data ptr, len, capacity (24 bytes on 64-bit). */
    public static final StructLayout FRS_BYTES_LAYOUT =
            MemoryLayout.structLayout(
                    ValueLayout.ADDRESS.withName("data"),
                    ValueLayout.JAVA_LONG.withName("len"),
                    ValueLayout.JAVA_LONG.withName("capacity"));

    /**
     * Typed VarHandles for {@code FrsBytes} access on native-allocated (8-byte aligned) segments
     * such as {@code Arena.allocate(FRS_BYTES_LAYOUT.byteSize() * count)} in {@link #batchGet}.
     *
     * <p>Replaces magic-offset reads like {@code seg.get(ValueLayout.ADDRESS, 0)} +
     * {@code seg.get(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS.byteSize())} (PR-F2 / D-R2-2).
     */
    private static final VarHandle FRS_BYTES_DATA =
            FRS_BYTES_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("data"));

    private static final VarHandle FRS_BYTES_LEN =
            FRS_BYTES_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("len"));

    /**
     * Unaligned (alignment=1) {@code FrsBytes} layout — required when the struct lives in a heap
     * {@code byte[24]} view (e.g. via {@code MemorySegment.ofArray(byte[])} which only guarantees
     * 1-byte alignment). The Rust ABI is the same; only the Java-side alignment contract changes.
     */
    private static final StructLayout FRS_BYTES_LAYOUT_UNALIGNED =
            MemoryLayout.structLayout(
                    ValueLayout.ADDRESS_UNALIGNED.withName("data"),
                    ValueLayout.JAVA_LONG_UNALIGNED.withName("len"),
                    ValueLayout.JAVA_LONG_UNALIGNED.withName("capacity"));

    private static final VarHandle FRS_BYTES_DATA_U =
            FRS_BYTES_LAYOUT_UNALIGNED.varHandle(MemoryLayout.PathElement.groupElement("data"));

    private static final VarHandle FRS_BYTES_LEN_U =
            FRS_BYTES_LAYOUT_UNALIGNED.varHandle(MemoryLayout.PathElement.groupElement("len"));

    /**
     * {@code FFI_ArrowArray} struct layout (Arrow C Data Interface, 80 bytes). Used by {@link
     * #batchGetArrow} to readback typed children/buffers pointers instead of magic-offset reads.
     */
    private static final StructLayout FFI_ARROW_ARRAY_LAYOUT =
            MemoryLayout.structLayout(
                    ValueLayout.JAVA_LONG.withName("length"),
                    ValueLayout.JAVA_LONG.withName("null_count"),
                    ValueLayout.JAVA_LONG.withName("offset"),
                    ValueLayout.JAVA_LONG.withName("n_buffers"),
                    ValueLayout.JAVA_LONG.withName("n_children"),
                    ValueLayout.ADDRESS.withName("buffers"),
                    ValueLayout.ADDRESS.withName("children"),
                    ValueLayout.ADDRESS.withName("dictionary"),
                    ValueLayout.ADDRESS.withName("release"),
                    ValueLayout.ADDRESS.withName("private_data"));

    private static final VarHandle ARROW_ARRAY_N_BUFFERS =
            FFI_ARROW_ARRAY_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("n_buffers"));
    private static final VarHandle ARROW_ARRAY_N_CHILDREN =
            FFI_ARROW_ARRAY_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("n_children"));
    private static final VarHandle ARROW_ARRAY_BUFFERS =
            FFI_ARROW_ARRAY_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("buffers"));
    private static final VarHandle ARROW_ARRAY_CHILDREN =
            FFI_ARROW_ARRAY_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("children"));
    private static final VarHandle ARROW_ARRAY_RELEASE =
            FFI_ARROW_ARRAY_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("release"));

    /**
     * {@code FrsChunk} struct layout (PR-E3 / E-HIGH-5 / F5-4). Mirrors the {@code #[repr(C)]} struct
     * in {@code crates/forst-rs-ffi/src/lib.rs §12.b}.
     *
     * <p>Layout (24 bytes on 64-bit, u64-aligned):
     *
     * <pre>
     * +0   ADDRESS   (8B)  buf_ptr     — caller-owned chunk buffer (input)
     * +8   JAVA_INT  (4B)  buf_cap     — buffer capacity in bytes (input)
     * +12  JAVA_INT  (4B)  row_count   — rows written by engine (output)
     * +16  JAVA_INT  (4B)  bytes_used  — bytes written by engine (output)
     * +20  JAVA_INT  (4B)  _reserved   — engine flag word (output); bit 0 = {@link #FRS_CHUNK_EOF}
     * </pre>
     */
    public static final StructLayout FRS_CHUNK_LAYOUT =
            MemoryLayout.structLayout(
                    ValueLayout.ADDRESS.withName("buf_ptr"),
                    ValueLayout.JAVA_INT.withName("buf_cap"),
                    ValueLayout.JAVA_INT.withName("row_count"),
                    ValueLayout.JAVA_INT.withName("bytes_used"),
                    ValueLayout.JAVA_INT.withName("_reserved"));

    /**
     * P0 (streaming-read redesign §2.3): bit 0 of {@code FrsChunk._reserved}. Set by the batched
     * open paths when the FIRST chunk already exhausted the iterator and no error is pending. The
     * engine has AUTO-CLOSED the iterator (the returned non-zero handle was never registered), so
     * the caller can — and should — skip both the mandatory trailing {@code
     * frs_vec_iter_prefix_next} and the {@code frs_vec_iter_prefix_close} crossings. Advisory:
     * calling them anyway yields clean EOF / a no-op respectively.
     */
    public static final int FRS_CHUNK_EOF = 1;

    private static final VarHandle FRS_CHUNK_BUF_PTR =
            FRS_CHUNK_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("buf_ptr"));
    private static final VarHandle FRS_CHUNK_BUF_CAP =
            FRS_CHUNK_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("buf_cap"));
    private static final VarHandle FRS_CHUNK_ROW_COUNT =
            FRS_CHUNK_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("row_count"));
    private static final VarHandle FRS_CHUNK_BYTES_USED =
            FRS_CHUNK_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("bytes_used"));
    private static final VarHandle FRS_CHUNK_RESERVED =
            FRS_CHUNK_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("_reserved"));

    /**
     * {@code FrsEngineOptions} struct layout (B-Prod-P7, spec §6d). Mirrors the {@code #[repr(C)]}
     * struct in {@code crates/forst-rs-ffi/src/lib.rs}.
     *
     * <p>Layout (48 bytes on 64-bit):
     *
     * <pre>
     * +0   ADDRESS (8B)   db_path
     * +8   JAVA_LONG (8B) write_buffer_size
     * +16  JAVA_INT  (4B) max_write_buffer_number
     * +20  JAVA_INT  (4B) max_background_compactions
     * +24  JAVA_INT  (4B) max_background_flushes
     * +28  4B padding (alignment for following u64)
     * +32  JAVA_LONG (8B) block_cache_capacity_bytes
     * +40  JAVA_LONG (8B) write_buffer_manager_capacity_bytes
     * </pre>
     *
     * <p>The padding is added explicitly via {@link MemoryLayout#paddingLayout(long)} so that the
     * Java mirror produces the exact 48-byte struct that Rust's {@code #[repr(C)]} layout generates
     * on the same target. Future fields land at +48 onwards (append-only).
     */
    public static final StructLayout FRS_ENGINE_OPTIONS_LAYOUT =
            MemoryLayout.structLayout(
                    ValueLayout.ADDRESS.withName("db_path"),
                    ValueLayout.JAVA_LONG.withName("write_buffer_size"),
                    ValueLayout.JAVA_INT.withName("max_write_buffer_number"),
                    ValueLayout.JAVA_INT.withName("max_background_compactions"),
                    ValueLayout.JAVA_INT.withName("max_background_flushes"),
                    MemoryLayout.paddingLayout(4),
                    ValueLayout.JAVA_LONG.withName("block_cache_capacity_bytes"),
                    ValueLayout.JAVA_LONG.withName("write_buffer_manager_capacity_bytes"));

    // ValueLayout.ADDRESS_UNALIGNED and ValueLayout.JAVA_LONG_UNALIGNED are
    // used to read the FrsBytes out struct when it lives in a heap byte[24]
    // segment (which only guarantees 1-byte alignment). Strict-alignment reads
    // through ValueLayout.ADDRESS would throw on a heap-byte-array view.

    private static final ConcurrentHashMap<String, SymbolLookup> LIBRARY_LOOKUPS =
            new ConcurrentHashMap<>();

    private final Linker linker;
    private final SymbolLookup lookup;

    /**
     * D8-H2 cache for Arrow C Data Interface release-callback {@link MethodHandle}s, keyed by the
     * release fn pointer address. Each call into {@code batchGetArrow} historically spun a fresh
     * {@code Linker.nativeLinker().downcallHandle(...)} per release call (2× per call) which pins
     * JIT metadata and spins LambdaForm bytecode — wasteful on the high-throughput vectorized read
     * path. Strategy (a): per-address cache. Robust to release-fn pointer variation across
     * producers (Arrow C-Data spec allows the producer to choose its release fn; arrow-rs uses a
     * single static fn but other producers may not).
     *
     * <p>B9-H2: replaced the previous {@code ConcurrentHashMap<Long, MethodHandle>} hot path with
     * a single-slot volatile fast path. The fast path serves 100% of arrow-rs traffic (single
     * static release fn) with zero allocations — no {@code Long} autobox per
     * {@code invokeArrowRelease} call. The {@link ConcurrentHashMap} fallback is consulted only
     * when the address misses the fast slot — rare in steady state, kept for mixed-producer
     * pipelines that legitimately emit multiple distinct release-fn pointers.
     *
     * <p>A10-H2: the prior two-field fast slot ({@code arrowReleaseFnAddr} and
     * {@code arrowReleaseHandleFast}) had a publication ordering race on mixed-producer pipelines.
     * Writer order was {@code handle = h; addr = a;} (handle first, address second). Reader order
     * was {@code h = handle; a = addr;} (handle first, address second), then verified
     * {@code a == releaseFn.address()}. A concurrent producer switch could interleave so that the
     * reader observed the NEW handle but the OLD address — the address check then matched the OLD
     * address against the OLD {@code releaseFn} (same producer's frame), accepting a handle bound
     * to a different release fn. The result: {@code invokeExact(target)} dispatched the wrong
     * native callback → JVM crash or silent payload corruption.
     *
     * <p>Fix: single-reference publication via {@link AtomicReference}. A {@link Slot} carries both
     * fields together; readers do one atomic-reference load (observing both fields as a unit) and
     * writers do one atomic-reference set. The race is eliminated because there is no longer a
     * window where one field is updated and the other is stale. Cost is one {@code Slot}
     * allocation per producer change — a rare event (the fast slot is only repopulated when the
     * incoming release-fn address differs from the cached one, which on steady-state arrow-rs
     * traffic never happens after the first call).
     */
    private static final class ArrowReleaseSlot {
        final long addr;
        final MethodHandle handle;

        ArrowReleaseSlot(long addr, MethodHandle handle) {
            this.addr = addr;
            this.handle = handle;
        }
    }

    private final AtomicReference<ArrowReleaseSlot> arrowReleaseSlot = new AtomicReference<>();

    private final ConcurrentHashMap<Long, MethodHandle> arrowReleaseHandleCache =
            new ConcurrentHashMap<>();

    private static final FunctionDescriptor ARROW_RELEASE_DESCRIPTOR =
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS);

    // --- 0. ABI version negotiation ---
    private final MethodHandle frsAbiVersion;

    // --- 1. Lifecycle ---
    private final MethodHandle frsDbOpen;
    private final MethodHandle frsDbOpenMemory;
    private final MethodHandle frsDbOpenMemoryTuned;
    private final MethodHandle frsDbOpenFromCheckpoint;
    private final MethodHandle frsDbOpenRemote;
    private final MethodHandle frsDbOpenRemoteWithOptions;
    private final MethodHandle frsDbClose;
    // B-Prod-P7 §6d: structured open + WriteBufferManager diagnostics.
    private final MethodHandle frsDbOpenWithOptions;
    private final MethodHandle frsDbWbmCapacity;
    private final MethodHandle frsDbWbmCurrentBytes;

    // --- 2. CF management ---
    private final MethodHandle frsDbDefaultCf;
    private final MethodHandle frsDbCreateCf;
    private final MethodHandle frsDbOpenCf;
    private final MethodHandle frsCfClose;

    // --- 3. Point ops ---
    private final MethodHandle frsPut;
    private final MethodHandle frsGet;
    private final MethodHandle frsGetPinned;
    private final MethodHandle frsGetAndPut;
    private final MethodHandle frsDelete;

    // --- 3b. Batch ops ---
    private final MethodHandle frsBatchPut;
    private final MethodHandle frsBatchGet;

    // C1: zero-copy batch_put via Arrow C Data Interface. The native side
    // takes an FFI_ArrowArray + FFI_ArrowSchema pair (key | value | op_type)
    // and dispatches DIRECTLY into the memtable's columnar storage with no
    // intermediate WriteBatch allocation. See `frs_batch_put_arrow` in
    // `crates/forst-rs-ffi/src/lib.rs` and `DbImpl::batch_put_arrow` in
    // `crates/forst-rs-engine/src/db.rs`.
    private final MethodHandle frsBatchPutArrow;
    private final MethodHandle frsBatchGetArrow;

    // --- 4. Memory management ---
    private final MethodHandle frsBytesFree;

    // --- 5. Flush / checkpoint / metadata ---
    private final MethodHandle frsFlush;
    private final MethodHandle frsCreateCheckpoint;
    // FRS-CKPT-NOFLUSH (2026-06-01): snapshot/restore the live memtable as an
    // Arrow-IPC artifact instead of flushing it to an L0 SST.
    private final MethodHandle frsSnapshotMemtablesToDir;
    private final MethodHandle frsReplayMemtableArtifacts;
    private final MethodHandle frsSequenceNumber;

    // --- 6. Delta-Join lookup + iterator ---
    private final MethodHandle frsLookupKv;
    private final MethodHandle frsGetIntoBuf;
    private final MethodHandle frsGetFast;
    private final MethodHandle frsIteratorOpen;
    private final MethodHandle frsIteratorSeek;
    private final MethodHandle frsIteratorNext;
    private final MethodHandle frsIteratorClose;
    private final MethodHandle frsPrefixLookupOpen;
    private final MethodHandle frsPrefixLookupClose;
    private final MethodHandle frsPrefixGetAll;
    private final MethodHandle frsBatchPrefixScan;

    // --- 6b. Vectorized batch ops (caller-owned Arrow BinaryArray buffers) ---
    // See spec docs/superpowers/specs/2026-05-15-forst-rs-vectorized-executor-design.md §C4.
    // All inputs are (offsets:i32[count+1], data:u8[]) plus count. No allocation
    // crosses the FFM boundary — the caller owns every buffer.
    private final MethodHandle frsVectorizedBatchGet;
    private final MethodHandle frsVectorizedBatchPut;
    private final MethodHandle frsVectorizedBatchDelete;
    // Stage-3 Unit-2 (two-regime design §3.4): MIXED put/delete/merge batch —
    // one FFI crossing + ONE atomic engine sequence allocation for an
    // interleaved write batch. Requires a dylib built from engine commit
    // b5b85a9b9 or later (frs_vectorized_batch_mixed).
    private final MethodHandle frsVectorizedBatchMixed;

    // --- 6c. Explicit WriteBatch (SP4) — Java-held handle for atomic batches.
    private final MethodHandle frsWritebatchOpen;
    private final MethodHandle frsWritebatchPut;
    private final MethodHandle frsWritebatchDelete;
    private final MethodHandle frsWritebatchCommit;
    private final MethodHandle frsWritebatchClose;

    // --- 7. TTL compaction filter ---
    private final MethodHandle frsCfSetCompactionFilterTtl;

    // --- 8. MVCC snapshot + versioned reads + incremental checkpoint (B-Prod-P2) ---
    private final MethodHandle frsDbSnapshot;
    private final MethodHandle frsDbReleaseSnapshot;
    private final MethodHandle frsGetAt;
    private final MethodHandle frsIteratorOpenAt;
    private final MethodHandle frsCreateIncrementalCheckpointAt;
    // FRS-CKPT-NOFLUSH (2026-06-01): incremental checkpoint that skips the
    // memtable→L0-SST flush (memtable captured as Arrow-IPC artifact instead).
    private final MethodHandle frsCreateIncrementalCheckpointAtNoflush;
    private final MethodHandle frsDbOpenFromIncremental;
    private final MethodHandle frsDbIncrementalCheckpointResultFree;

    // --- 9. State import / export migration (B-Prod-P10, spec §6g) ---
    private final MethodHandle frsCfExport;
    private final MethodHandle frsDbCreateCfFromImport;

    // --- 9b. drop_cf + ingest_external_sst (B-Prod-followup-5, spec §6g) ---
    // Native fast-path import: hardlink pre-built SSTs into L0 in
    // O(file-count) rather than scan + replay-put in O(key-count). The
    // drop_cf companion lets callers reclaim a CF name before re-importing
    // under it (the legacy migration path required a fresh name because
    // `drop_cf` did not exist).
    private final MethodHandle frsDbDropCf;
    private final MethodHandle frsDbIngestExternalSst;

    // --- 10. Vectorized chunked iterator (P3-A/P3-B, spec §1 §b + §2 component E) ---
    // FrsDb / FrsCfHandle are *mut c_void → ADDRESS; iterator handle is u64 scalar → JAVA_LONG.
    private final MethodHandle frsVecIterPrefixOpen;
    private final MethodHandle frsVecIterPrefixNext;
    private final MethodHandle frsVecIterPrefixClose;
    private final MethodHandle frsVecIterPrefixAbort;
    /**
     * PR-E3 / E-HIGH-5 / F5-4: batched prefix-iter open — single FFI crossing
     * for N prefix opens (SoA prefixes_off + prefixes_data, AoS FrsChunk
     * output array).  Replaces the per-request loop that crossed FFI N times.
     */
    private final MethodHandle frsVecIterPrefixOpenBatch;

    /**
     * PARALLEL variant of {@link #frsVecIterPrefixOpenBatch} — identical ABI, but the engine builds +
     * drains the K probes across its read pool (FRS_RS_READ_IO_PARALLELISM) instead of a serial loop.
     * The join read-path lever (q7/q9/q20): the K iterator probes in an async-state batch run
     * concurrently. Results are byte-identical to the serial open (proven by the engine + FFI tests).
     */
    private final MethodHandle frsVecIterPrefixOpenBatchParallel;

    private final MethodHandle frsVecMergeAppend;
    private final MethodHandle frsVecMergeAppendBatch; // Phase A.1 (audit-design §3 V4)

    // 13. Range iterator (P9, §2-D): mirrors prefix handles with extra hi-bound argument.
    private final MethodHandle frsVecIterRangeOpen;
    private final MethodHandle frsVecIterRangeNext;
    private final MethodHandle frsVecIterRangeClose;
    private final MethodHandle frsVecIterRangeAbort;

    private static SymbolLookup explicitLibraryLookup(String explicit) {
        String absolutePath = Path.of(explicit).toAbsolutePath().toString();
        return LIBRARY_LOOKUPS.computeIfAbsent(
                absolutePath,
                path -> SymbolLookup.libraryLookup(Path.of(path), Arena.global()));
    }

    private static SymbolLookup defaultLibraryLookup() {
        return LIBRARY_LOOKUPS.computeIfAbsent(
                "loader:forst_rs_ffi",
                ignored -> {
                    System.loadLibrary("forst_rs_ffi");
                    return SymbolLookup.loaderLookup();
                });
    }

    public ForStRsLinker(Arena arena) {
        this.linker = Linker.nativeLinker();

        String explicit = System.getProperty("forstrs.native.libpath");
        this.lookup =
                explicit != null && !explicit.isBlank()
                        ? explicitLibraryLookup(explicit)
                        : defaultLibraryLookup();

        // 0. ABI version negotiation
        this.frsAbiVersion = bind("frs_abi_version", FunctionDescriptor.of(ValueLayout.JAVA_INT));

        // 1. Lifecycle
        this.frsDbOpen =
                bind(
                        "frs_db_open",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db_path (c_char*)
                                ValueLayout.ADDRESS)); // out_handle

        this.frsDbOpenMemory =
                bind(
                        "frs_db_open_memory",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // Performance-tuning sibling of frs_db_open_memory: 4 size_t knobs +
        // out_handle. Each knob may be 0, in which case the engine keeps its
        // built-in default for that field (see frs_db_open_memory_tuned doc
        // in lib.rs); out-of-range values flow through EngineOptionsBuilder
        // .try_build() and surface as FRS_STATUS_INVALID_ARGUMENT rather
        // than panicking.
        this.frsDbOpenMemoryTuned =
                bind(
                        "frs_db_open_memory_tuned",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.JAVA_LONG, // write_buffer_size
                                ValueLayout.JAVA_LONG, // max_write_buffer_number
                                ValueLayout.JAVA_LONG, // max_background_compactions
                                ValueLayout.JAVA_LONG, // max_background_flushes
                                ValueLayout.ADDRESS)); // out_handle

        this.frsDbOpenFromCheckpoint =
                bind(
                        "frs_db_open_from_checkpoint",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // target_dir (c_char*)
                                ValueLayout.ADDRESS)); // out_handle

        // B-Prod-P6: open the engine on top of an OpenDAL URI with a local
        // LRU cache. JSON config is a flat string-to-string object; pass
        // "{}" or NULL when the URI scheme needs no extra config.
        this.frsDbOpenRemote =
                bind(
                        "frs_db_open_remote",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // uri (c_char*)
                                ValueLayout.ADDRESS, // opendal_config_json (c_char*)
                                ValueLayout.ADDRESS, // cache_dir (c_char*)
                                ValueLayout.JAVA_LONG, // cache_capacity_bytes (u64)
                                ValueLayout.ADDRESS)); // out_handle
        this.frsDbOpenRemoteWithOptions =
                bind(
                        "frs_db_open_remote_with_options",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // const FrsEngineOptions*
                                ValueLayout.ADDRESS, // uri (c_char*)
                                ValueLayout.ADDRESS, // opendal_config_json (c_char*)
                                ValueLayout.ADDRESS, // cache_dir (c_char*)
                                ValueLayout.JAVA_LONG, // cache_capacity_bytes (u64)
                                ValueLayout.ADDRESS)); // out_handle

        this.frsDbClose =
                bind(
                        "frs_db_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // B-Prod-P7 §6d: structured open + WBM diagnostics. The opts arg
        // is a pointer to FrsEngineOptions, allocated in the caller's Arena
        // and populated field-by-field (see #dbOpenWithOptions).
        this.frsDbOpenWithOptions =
                bind(
                        "frs_db_open_with_options",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // const FrsEngineOptions*
                                ValueLayout.ADDRESS)); // out_handle (FrsDb*)
        this.frsDbWbmCapacity =
                bind(
                        "frs_db_write_buffer_manager_capacity",
                        FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
        this.frsDbWbmCurrentBytes =
                bind(
                        "frs_db_write_buffer_manager_current_bytes",
                        FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        // 2. CF management
        this.frsDbDefaultCf =
                bind(
                        "frs_db_default_cf",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db handle
                                ValueLayout.ADDRESS)); // out_cf

        this.frsDbCreateCf =
                bind(
                        "frs_db_create_cf",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // name (c_char*)
                                ValueLayout.ADDRESS)); // out_cf

        this.frsDbOpenCf =
                bind(
                        "frs_db_open_cf",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // name (c_char*)
                                ValueLayout.ADDRESS)); // out_cf

        this.frsCfClose =
                bind(
                        "frs_cf_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // 3. Point ops — bound with Linker.Option.critical(true) so that we can
        // pass MemorySegment.ofArray(byte[]) heap segments directly. The Linker
        // pins the underlying byte[] for the duration of the call (no copy)
        // instead of allocating a per-call native staging buffer; this matches
        // the JNI GetByteArrayElements pin semantics and eliminates the
        // dominant per-op overhead the original confined-Arena path imposed.
        // D-R4-NEW-H1: do NOT bind frs_put critical. The Rust impl
        // routes through `db.put` → `write_single` →
        // `write_controller.may_throttle()` which can `std::thread::sleep`
        // (Slowdown) or `condvar.wait_timeout` (Stall) for ms- to seconds-
        // class durations. Critical-mode FFI suspends JVM safepoints for
        // the duration — every sibling thread stalls at every safepoint
        // poll, GC cannot run. The sister batched paths
        // (frs_vectorized_batch_put / frs_vectorized_batch_delete) were
        // already moved to plain `bind` for the same reason; we extend
        // that fix to single-key writers.
        this.frsPut =
                bind(
                        "frs_put",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS, // value ptr
                                ValueLayout.JAVA_LONG)); // value_len

        this.frsGet =
                bind(
                        "frs_get",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS)); // out FrsBytes*

        // Zero-copy pinned get from memtable inline storage. Returns a direct
        // pointer into the memtable's HashEntry.inline_value without any Rust-side
        // Vec allocation. Critical mode: the key byte[] is pinned for the call.
        this.frsGetPinned =
                bindCritical(
                        "frs_get_pinned",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS, // out_ptr (*const u8*)
                                ValueLayout.ADDRESS)); // out_len (usize*)

        // D-R4-NEW-H1: same rationale as frs_put — get_and_put + delete
        // also call into write_single → may_throttle() and must not
        // suspend safepoints. Plain `bind`.
        this.frsGetAndPut =
                bind(
                        "frs_get_and_put",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS, // new_value ptr
                                ValueLayout.JAVA_LONG, // new_value_len
                                ValueLayout.ADDRESS)); // out FrsBytes* (old value)

        this.frsDelete =
                bind(
                        "frs_delete",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG)); // key_len

        // 3b. Batch ops — frs_batch_put takes parallel arrays of native
        // pointers (uint8_t* const*) and sizes (size_t*). Critical mode is NOT
        // applicable here because we pass arrays-of-pointers into-native, which
        // must live in native memory anyway (the byte[] addresses inside a
        // Java [B[] array can't be pinned simultaneously). Caller stages the
        // four arrays via the {@link #batchPut(FrsDb, FrsCfHandle, MemorySegment,
        // MemorySegment, MemorySegment, MemorySegment, long)} overload.
        this.frsBatchPut =
                bind(
                        "frs_batch_put",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // keys (uint8_t* const*)
                                ValueLayout.ADDRESS, // key_lens (size_t*)
                                ValueLayout.ADDRESS, // values (uint8_t* const*)
                                ValueLayout.ADDRESS, // value_lens (size_t*)
                                ValueLayout.JAVA_LONG)); // count (size_t)

        // frs_batch_get: reads N keys in one call, returning N FrsBytes slots.
        // Native signature:
        //   int frs_batch_get(FrsDb, FrsCfHandle, *const *const u8 keys,
        //                     *const usize key_lens, usize count, *mut FrsBytes out_values)
        this.frsBatchGet =
                bind(
                        "frs_batch_get",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // keys (uint8_t* const*)
                                ValueLayout.ADDRESS, // key_lens (size_t*)
                                ValueLayout.JAVA_LONG, // count (size_t)
                                ValueLayout.ADDRESS)); // out_values (FrsBytes*)

        // C1: zero-copy batch_put via Arrow C Data Interface.
        // Native signature:
        //   int frs_batch_put_arrow(
        //       FrsDb handle, FrsCfHandle cf,
        //       FFI_ArrowArray*  array,
        //       FFI_ArrowSchema* schema);
        // The native side takes ownership of *array / *schema (and zeroes the
        // originals per the Arrow C Data Interface contract), so the caller
        // does not need to free them after the call returns.
        this.frsBatchPutArrow =
                bind(
                        "frs_batch_put_arrow",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // array (FFI_ArrowArray*)
                                ValueLayout.ADDRESS)); // schema (FFI_ArrowSchema*)

        this.frsBatchGetArrow =
                bind(
                        "frs_batch_get_arrow",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // keys_array (FFI_ArrowArray*)
                                ValueLayout.ADDRESS, // keys_schema (FFI_ArrowSchema*)
                                ValueLayout.ADDRESS, // out_array (FFI_ArrowArray*)
                                ValueLayout.ADDRESS)); // out_schema (FFI_ArrowSchema*)

        // 4. Memory management — bound critical because every get/lookup_kv path
        // calls frs_bytes_free on its 24-byte FrsBytes out struct, which is now
        // a heap segment passed via MemorySegment.ofArray(byte[24]).
        this.frsBytesFree =
                bindCritical(
                        "frs_bytes_free",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // 5. Flush / checkpoint / metadata
        this.frsFlush =
                bind("frs_flush", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsCreateCheckpoint =
                bind(
                        "frs_create_checkpoint",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS)); // target_dir (c_char*)

        // FRS-CKPT-NOFLUSH: (db, target_dir c_char*, out_count u64*) -> int
        this.frsSnapshotMemtablesToDir =
                bind(
                        "frs_snapshot_memtables_to_dir",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // snapshot (seq-bounded cut)
                                ValueLayout.ADDRESS, // target_dir
                                ValueLayout.ADDRESS)); // out_count (u64*)
        this.frsReplayMemtableArtifacts =
                bind(
                        "frs_replay_memtable_artifacts",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // dir
                                ValueLayout.ADDRESS)); // out_rows (u64*)

        this.frsSequenceNumber =
                bind(
                        "frs_sequence_number",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS)); // out_seq (u64*)

        // 6. Delta-Join lookup + iterator
        this.frsLookupKv =
                bind(
                        "frs_lookup_kv",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS)); // out FrsBytes*

        this.frsGetIntoBuf =
                bind(
                        "frs_get_into_buf",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS, // out_buf ptr
                                ValueLayout.JAVA_LONG, // out_buf_cap
                                ValueLayout.ADDRESS)); // out_val_len ptr

        // frs_get_fast: same signature as frs_get_into_buf but skips catch_unwind
        // and Arc::clone on the FFI hot path. Keep it safepoint-friendly because
        // full LSM reads may still touch blocking storage.
        this.frsGetFast =
                bind(
                        "frs_get_fast",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS, // out_buf ptr
                                ValueLayout.JAVA_LONG, // out_buf_cap
                                ValueLayout.ADDRESS)); // out_val_len ptr

        this.frsIteratorOpen =
                bind(
                        "frs_iterator_open",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS)); // out_iter

        this.frsIteratorSeek =
                bind(
                        "frs_iterator_seek",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // iter
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG)); // key_len

        this.frsIteratorNext =
                bind(
                        "frs_iterator_next",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // iter
                                ValueLayout.ADDRESS, // out_key (FrsBytes*)
                                ValueLayout.ADDRESS, // out_value (FrsBytes*)
                                ValueLayout.ADDRESS)); // out_valid (bool*)

        this.frsIteratorClose =
                bind(
                        "frs_iterator_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsPrefixLookupOpen =
                bind(
                        "frs_prefix_lookup_open",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // prefix ptr
                                ValueLayout.JAVA_LONG, // prefix_len
                                ValueLayout.ADDRESS)); // out_iter

        this.frsPrefixLookupClose =
                bind(
                        "frs_prefix_lookup_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        this.frsPrefixGetAll =
                bind(
                        "frs_prefix_get_all",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // prefix ptr
                                ValueLayout.JAVA_LONG, // prefix_len (usize)
                                ValueLayout.JAVA_LONG, // max_count (usize)
                                ValueLayout.ADDRESS, // out_keys (FrsBytes[])
                                ValueLayout.ADDRESS, // out_values (FrsBytes[])
                                ValueLayout.ADDRESS)); // out_count (*mut usize)

        this.frsBatchPrefixScan =
                bind(
                        "frs_batch_prefix_scan",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // prefixes (*const *const u8)
                                ValueLayout.ADDRESS, // prefix_lens (*const usize)
                                ValueLayout.JAVA_LONG, // prefix_count (usize)
                                ValueLayout.JAVA_LONG, // max_per_prefix (usize)
                                ValueLayout.ADDRESS, // out_keys (FrsBytes[])
                                ValueLayout.ADDRESS, // out_values (FrsBytes[])
                                ValueLayout.ADDRESS, // out_counts (*mut usize)
                                ValueLayout.ADDRESS)); // out_total (*mut usize)

        // 6b. Vectorized batch ops — caller-owned Arrow BinaryArray buffers.
        // Native signature (frs_vectorized_batch_get):
        //   int frs_vectorized_batch_get(
        //     FrsDb, FrsCfHandle,
        //     *const i32 key_offsets, *const u8 key_data, usize key_data_len, usize count,
        //     *mut i32 out_offsets, *mut u8 out_data, *mut u8 out_validity,
        //     usize out_data_cap, *mut usize out_data_len);
        this.frsVectorizedBatchGet =
                bind(
                        "frs_vectorized_batch_get",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key_offsets (*const i32)
                                ValueLayout.ADDRESS, // key_data    (*const u8)
                                ValueLayout.JAVA_LONG, // key_data_len (usize)
                                ValueLayout.JAVA_LONG, // count     (usize)
                                ValueLayout.ADDRESS, // out_offsets (*mut i32)
                                ValueLayout.ADDRESS, // out_data    (*mut u8)
                                ValueLayout.ADDRESS, // out_validity(*mut u8)
                                ValueLayout.JAVA_LONG, // out_data_cap (usize)
                                ValueLayout.ADDRESS)); // out_data_len (*mut usize)

        // Native signature (frs_vectorized_batch_put):
        //   int frs_vectorized_batch_put(
        //     FrsDb, FrsCfHandle,
        //     *const i32 key_offsets, *const u8 key_data, usize key_data_len,
        //     *const i32 val_offsets, *const u8 val_data, usize val_data_len,
        //     usize count);
        // D6-H2: REMOVED critical mode (was PR-B2 / D-R3-1). The batched put can stall
        // for tens-of-ms to multi-second on WAL fsync, memtable-full waits, or
        // rate-limited flushes inside the engine's commit path. Holding critical mode
        // across that pins the JVM safepoint — sibling tasks stall and GC starves.
        // The MemorySegment args are already implicitly pinned for the duration of the
        // FFI call when they come from a confined or shared arena (which is the only
        // way VectorizedExecutor allocates them); critical mode bought us heap-byte[]
        // acceptance which the batched path never needed (keys/vals are always native
        // segments from ColumnarBatchBuffer).
        this.frsVectorizedBatchPut =
                bind(
                        "frs_vectorized_batch_put",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key_offsets (*const i32)
                                ValueLayout.ADDRESS, // key_data    (*const u8)
                                ValueLayout.JAVA_LONG, // key_data_len (usize)
                                ValueLayout.ADDRESS, // val_offsets (*const i32)
                                ValueLayout.ADDRESS, // val_data    (*const u8)
                                ValueLayout.JAVA_LONG, // val_data_len (usize)
                                ValueLayout.JAVA_LONG)); // count   (usize)

        // Native signature (frs_vectorized_batch_delete):
        //   int frs_vectorized_batch_delete(
        //     FrsDb, FrsCfHandle,
        //     *const i32 key_offsets, *const u8 key_data, usize key_data_len, usize count);
        // D6-H2: REMOVED critical mode — same rationale as the batch_put binding above.
        // Synchronous WriteBatch commit, but the commit itself can block on WAL fsync /
        // memtable-full / rate-limited flush. Critical mode is reserved for truly
        // bounded ops (single-key get/put/delete/lookup_kv/get_at).
        this.frsVectorizedBatchDelete =
                bind(
                        "frs_vectorized_batch_delete",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key_offsets
                                ValueLayout.ADDRESS, // key_data
                                ValueLayout.JAVA_LONG, // key_data_len
                                ValueLayout.JAVA_LONG)); // count

        // Native signature (frs_vectorized_batch_mixed) — Stage-3 Unit-2:
        //   int frs_vectorized_batch_mixed(
        //     FrsDb, FrsCfHandle,
        //     *const u8 kinds, usize count,
        //     *const i32 key_offsets, *const u8 key_data, usize key_data_len,
        //     *const i32 value_offsets, *const u8 value_data, usize value_data_len);
        // kinds[i]: 0 = Delete, 1 = Put, 2 = Merge (engine OpType discriminants).
        // Delete rows MUST carry an empty value slice (value_offsets[i] ==
        // value_offsets[i+1]); Merge rows require the CF to have a merge
        // operator. The whole batch lands atomically with ONE sequence-range
        // allocation; rows apply in array order (later same-key row wins).
        // D6-H2 rationale applies verbatim: plain bind (NOT critical) — the
        // commit path can stall on memtable-full / rate-limited flush and
        // must not pin JVM safepoints.
        this.frsVectorizedBatchMixed =
                bind(
                        "frs_vectorized_batch_mixed",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // kinds (*const u8)
                                ValueLayout.JAVA_LONG, // count (usize)
                                ValueLayout.ADDRESS, // key_offsets (*const i32)
                                ValueLayout.ADDRESS, // key_data    (*const u8)
                                ValueLayout.JAVA_LONG, // key_data_len (usize)
                                ValueLayout.ADDRESS, // value_offsets (*const i32)
                                ValueLayout.ADDRESS, // value_data    (*const u8)
                                ValueLayout.JAVA_LONG)); // value_data_len (usize)

        // 6c. Explicit WriteBatch — Java holds a handle so it can stage cross-CF
        // puts/deletes and commit atomically.
        this.frsWritebatchOpen =
                bind(
                        "frs_writebatch_open",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS)); // out_handle (FrsWriteBatch*)

        this.frsWritebatchPut =
                bind(
                        "frs_writebatch_put",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // wb handle
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key_offsets
                                ValueLayout.ADDRESS, // key_data
                                ValueLayout.ADDRESS, // val_offsets
                                ValueLayout.ADDRESS, // val_data
                                ValueLayout.JAVA_LONG)); // count

        this.frsWritebatchDelete =
                bind(
                        "frs_writebatch_delete",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // wb handle
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // key_offsets
                                ValueLayout.ADDRESS, // key_data
                                ValueLayout.JAVA_LONG)); // count

        this.frsWritebatchCommit =
                bind(
                        "frs_writebatch_commit",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // wb handle
                                ValueLayout.ADDRESS)); // db

        this.frsWritebatchClose =
                bind(
                        "frs_writebatch_close",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT, ValueLayout.ADDRESS)); // wb handle

        // 7. TTL compaction filter — Flink-shaped per-CF filter that drops entries
        // older than ttl_ms. Engine-side enforcement runs at flush + L0→L1 compaction;
        // see crates/forst-rs-engine/src/compaction_filter.rs.
        this.frsCfSetCompactionFilterTtl =
                bind(
                        "frs_cf_set_compaction_filter_ttl",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.JAVA_LONG, // ttl_ms (u64)
                                ValueLayout.JAVA_INT, // state_type (i32)
                                ValueLayout.JAVA_LONG)); // timestamp_offset (usize)

        // 8. MVCC snapshot + versioned reads + incremental checkpoint (B-Prod-P2)
        this.frsDbSnapshot =
                bind(
                        "frs_db_snapshot",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS)); // out_snapshot (FrsSnapshot*)

        this.frsDbReleaseSnapshot =
                bind(
                        "frs_db_release_snapshot",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS)); // snapshot (FrsSnapshot)

        this.frsGetAt =
                bind(
                        "frs_get_at",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // snapshot
                                ValueLayout.ADDRESS, // key ptr
                                ValueLayout.JAVA_LONG, // key_len
                                ValueLayout.ADDRESS)); // out FrsBytes*

        this.frsIteratorOpenAt =
                bind(
                        "frs_iterator_open_at",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // snapshot
                                ValueLayout.ADDRESS)); // out_iter

        this.frsCreateIncrementalCheckpointAt =
                bind(
                        "frs_create_incremental_checkpoint_at",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // snapshot
                                ValueLayout.JAVA_LONG, // checkpoint_id (u64)
                                ValueLayout.JAVA_LONG, // base_checkpoint_id (u64)
                                ValueLayout.ADDRESS)); // out (FrsIncrementalCheckpointResult*)

        // FRS-CKPT-NOFLUSH: same ABI as frs_create_incremental_checkpoint_at,
        // but does not flush the memtable to an L0 SST.
        this.frsCreateIncrementalCheckpointAtNoflush =
                bind(
                        "frs_create_incremental_checkpoint_at_noflush",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // snapshot
                                ValueLayout.JAVA_LONG, // checkpoint_id (u64)
                                ValueLayout.JAVA_LONG, // base_checkpoint_id (u64)
                                ValueLayout.ADDRESS)); // out (FrsIncrementalCheckpointResult*)

        this.frsDbOpenFromIncremental =
                bind(
                        "frs_db_open_from_incremental",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // target_dir (c_char*)
                                ValueLayout.ADDRESS, // base_manifest (c_char*)
                                ValueLayout.ADDRESS, // sst_files (c_char* const*)
                                ValueLayout.JAVA_LONG, // sst_file_count (size_t)
                                ValueLayout.ADDRESS)); // out_handle

        this.frsDbIncrementalCheckpointResultFree =
                bind(
                        "frs_db_incremental_checkpoint_result_free",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

        // 9. State import / export migration (B-Prod-P10, spec §6g).
        //
        // The native side writes a single self-describing blob
        // (`EXPORT.frsblob`) under `export_dir` — see
        // crates/forst-rs-ffi/src/lib.rs::frs_cf_export and
        // crates/forst-rs-engine/src/db.rs::cf_export for the on-disk
        // format. The import side creates a fresh CF and replays the blob.
        this.frsCfExport =
                bind(
                        "frs_cf_export",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS)); // export_dir (c_char*)

        this.frsDbCreateCfFromImport =
                bind(
                        "frs_db_create_cf_from_import",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // name (c_char*)
                                ValueLayout.ADDRESS, // import_dir (c_char*)
                                ValueLayout.ADDRESS)); // out_cf

        // 9b. drop_cf + ingest_external_sst (B-Prod-followup-5).
        // See crates/forst-rs-ffi/src/lib.rs::frs_db_drop_cf and
        // crates/forst-rs-ffi/src/lib.rs::frs_db_ingest_external_sst.
        this.frsDbDropCf =
                bind(
                        "frs_db_drop_cf",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS)); // cf

        this.frsDbIngestExternalSst =
                bind(
                        "frs_db_ingest_external_sst",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // sst_paths (const char**)
                                ValueLayout.JAVA_LONG)); // count (usize → JAVA_LONG)

        // 10. Vectorized chunked iterator (P3-A/P3-B, spec §1 §b + §2 component E)
        this.frsVecIterPrefixOpen =
                bind(
                        "frs_vec_iter_prefix_open",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // prefix_ptr
                                ValueLayout.JAVA_INT, // prefix_len (u32)
                                ValueLayout.ADDRESS, // chunk_buf_ptr
                                ValueLayout.JAVA_INT, // chunk_buf_cap (u32)
                                ValueLayout.ADDRESS, // out_handle (*mut u64)
                                ValueLayout.ADDRESS, // out_row_count (*mut u32)
                                ValueLayout.ADDRESS)); // out_bytes_used (*mut u32)
        this.frsVecIterPrefixNext =
                bind(
                        "frs_vec_iter_prefix_next",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.JAVA_LONG, // handle (u64 scalar)
                                ValueLayout.ADDRESS, // chunk_buf_ptr
                                ValueLayout.JAVA_INT, // chunk_buf_cap (u32)
                                ValueLayout.ADDRESS, // out_row_count (*mut u32)
                                ValueLayout.ADDRESS)); // out_bytes_used (*mut u32)
        this.frsVecIterPrefixClose =
                bind(
                        "frs_vec_iter_prefix_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
        this.frsVecIterPrefixAbort =
                bind(
                        "frs_vec_iter_prefix_abort",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
        // D9-H2: REMOVED critical mode (was PR-E3 / E-HIGH-5 / F5-4). Same rationale as D6-H2
        // (vectorized_batch_put/_delete) and D8-H1 (merge_append_batch): the Rust impl performs N
        // synchronous engine prefix_scan + fill_chunk operations inside one FFI call. While each
        // individual prefix_scan is bounded, the aggregate wall-clock under back-pressure (LSM
        // compaction stalls, buffer-pool contention) scales linearly with N and can hold the JVM
        // safepoint window for ms-scale latency — sibling tasks stall, GC starves. The args from
        // VectorizedExecutor (prefixes_off / prefixes_data / out_handles / out_first_chunks) are
        // already confined-arena MemorySegments, so non-critical mode is sufficient — critical-
        // mode heap byte[] acceptance is not needed here.
        this.frsVecIterPrefixOpenBatch =
                bind(
                        "frs_vec_iter_prefix_open_batch",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT, // return rc
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // prefixes_off (u32*)
                                ValueLayout.ADDRESS, // prefixes_data (u8*)
                                ValueLayout.JAVA_INT, // n (u32)
                                ValueLayout.ADDRESS, // out_handles (u64*)
                                ValueLayout.ADDRESS, // out_first_chunks (FrsChunk*)
                                ValueLayout.JAVA_INT)); // chunk_cap (u32)
        this.frsVecIterPrefixOpenBatchParallel =
                bind(
                        "frs_vec_iter_prefix_open_batch_parallel",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT, // return rc
                                ValueLayout.ADDRESS, // db
                                ValueLayout.ADDRESS, // cf
                                ValueLayout.ADDRESS, // prefixes_off (u32*)
                                ValueLayout.ADDRESS, // prefixes_data (u8*)
                                ValueLayout.JAVA_INT, // n (u32)
                                ValueLayout.ADDRESS, // out_handles (u64*)
                                ValueLayout.ADDRESS, // out_first_chunks (FrsChunk*)
                                ValueLayout.JAVA_INT)); // chunk_cap (u32)
        this.frsVecMergeAppend =
                bind(
                        "frs_vec_merge_append",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS,
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS,
                                ValueLayout.JAVA_INT)); // P6-B
        // Phase A.1 (audit-design §3 V4): batched merge-append, N rows in 1 FFI call.
        // D8-H1: REMOVED critical mode (was PR-B2 / D-R3-3). The Rust impl performs N
        // synchronous put() calls (one per distinct key), each of which can stall on
        // WAL fsync, memtable-full waits, or rate-limited flushes inside the engine's
        // commit path. Holding critical mode across that pins the JVM safepoint —
        // sibling tasks stall and GC starves. Same rationale as D6-H2 for
        // frs_vectorized_batch_put/_delete. The MemorySegment args (keys/ops off+data)
        // are already from VectorizedExecutor's confined arena, so non-critical mode
        // is sufficient — no heap byte[] acceptance is needed.
        this.frsVecMergeAppendBatch =
                bind(
                        "frs_vec_merge_append_batch",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT, // return rc
                                ValueLayout.ADDRESS,   // db
                                ValueLayout.ADDRESS,   // cf
                                ValueLayout.ADDRESS,   // keys_off (u32*)
                                ValueLayout.ADDRESS,   // keys_data (u8*)
                                ValueLayout.JAVA_LONG, // keys_data_len (usize)
                                ValueLayout.ADDRESS,   // ops_off (u32*)
                                ValueLayout.ADDRESS,   // ops_data (u8*)
                                ValueLayout.JAVA_LONG, // ops_data_len (usize)
                                ValueLayout.JAVA_INT));// n

        // 13. Vectorized chunked range iterator (P9, spec §2 component D)
        this.frsVecIterRangeOpen =
                bind(
                        "frs_vec_iter_range_open",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS,
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS,
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS,
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS));
        this.frsVecIterRangeNext =
                bind(
                        "frs_vec_iter_range_next",
                        FunctionDescriptor.of(
                                ValueLayout.JAVA_INT,
                                ValueLayout.JAVA_LONG,
                                ValueLayout.ADDRESS,
                                ValueLayout.JAVA_INT,
                                ValueLayout.ADDRESS,
                                ValueLayout.ADDRESS));
        this.frsVecIterRangeClose =
                bind(
                        "frs_vec_iter_range_close",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
        this.frsVecIterRangeAbort =
                bind(
                        "frs_vec_iter_range_abort",
                        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
    }

    private MethodHandle bind(String name, FunctionDescriptor descriptor) {
        MemorySegment sym =
                lookup.find(name)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "symbol not found in cdylib: " + name));
        return linker.downcallHandle(sym, descriptor);
    }

    /**
     * Binds {@code name} as a downcall handle in <em>critical</em> mode (JDK 22+: {@link
     * Linker.Option#critical(boolean) critical(true)}). Critical-mode handles can accept heap
     * {@link MemorySegment}s such as {@link MemorySegment#ofArray(byte[])}; the linker pins the
     * underlying primitive array for the duration of the native call instead of forcing the caller
     * to stage bytes through a per-call native arena.
     *
     * <p>Use only on tiny non-blocking symbols such as {@code frs_get_pinned} and
     * {@code frs_bytes_free}. Full LSM reads and all writers stay on plain downcalls because they
     * can touch storage or throttling paths and must remain JVM-safepoint-friendly.
     */
    private MethodHandle bindCritical(String name, FunctionDescriptor descriptor) {
        MemorySegment sym =
                lookup.find(name)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "symbol not found in cdylib: " + name));
        // allowHeapAccess=true so MemorySegment.ofArray(byte[]) is acceptable.
        return linker.downcallHandle(sym, descriptor, Linker.Option.critical(true));
    }

    /**
     * Invokes the Arrow C Data Interface release callback {@code releaseFn} on {@code target},
     * caching the {@link MethodHandle} per release-fn pointer address.
     *
     * <p>D8-H2 fix: previously each call into {@link #batchGetArrow} spun up a fresh
     * {@code Linker.nativeLinker().downcallHandle(...)} for the array's release callback and another
     * for the schema's release callback — two per call. This pinned JIT metadata and spun
     * LambdaForm bytecode on the high-throughput vectorized read path. Caching by address handles
     * the common case (single static fn from arrow-rs) optimally while remaining safe to mixed
     * producers that emit distinct release fns.
     *
     * <p>If {@code releaseFn} is the null pointer, this method is a no-op (Arrow C-Data
     * convention: released arrays/schemas have their release fn cleared to null).
     */
    private void invokeArrowRelease(MemorySegment releaseFn, MemorySegment target) {
        long addr = releaseFn.address();
        if (addr == 0L) {
            return;
        }
        // A10-H2 fast path: single-reference atomic publication. One atomic-reference load sees
        // both fields as a unit, so there is no window where (handle, addr) can be observed
        // half-updated. On the steady-state arrow-rs traffic this is one ARef read + one
        // compare → no allocations, no autobox, no race.
        ArrowReleaseSlot snap = arrowReleaseSlot.get();
        MethodHandle handle;
        if (snap != null && snap.addr == addr) {
            handle = snap.handle;
        } else {
            // Slow path: miss in the fast slot (first call or producer switch). Resolve via the
            // ConcurrentHashMap (canonical owner) and publish a new Slot. computeIfAbsent
            // guarantees one downcallHandle build per distinct address across all threads.
            handle =
                    arrowReleaseHandleCache.computeIfAbsent(
                            addr,
                            a ->
                                    Linker.nativeLinker()
                                            .downcallHandle(
                                                    releaseFn, ARROW_RELEASE_DESCRIPTOR));
            // Single atomic publication of (addr, handle). A racing producer switch may CAS in
            // a different Slot; we accept the loss — the next call will observe the winner and
            // either match or repeat the slow path. Both Slots reference the SAME handle for
            // their own address (because computeIfAbsent is the canonical builder), so the only
            // observable effect of a CAS loss is a redundant Slot allocation on the loser's
            // thread. Correctness-preserving.
            arrowReleaseSlot.set(new ArrowReleaseSlot(addr, handle));
        }
        try {
            handle.invokeExact(target);
        } catch (Throwable ignored) {
            // Swallow per existing batchGetArrow contract — release callbacks must not propagate
            // exceptions to the caller; the caller's results are still valid.
        }
    }

    // ------------------------------------------------------------------
    // 1. Lifecycle
    // ------------------------------------------------------------------

    /** Opens an in-memory ForSt-RS engine. Caller closes via {@link FrsDb#close()}. */
    public FrsDb dbOpenMemory(Arena arena) {
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpenMemory.invokeExact(outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_memory threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_memory");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /**
     * Opens an in-memory ForSt-RS engine with caller-supplied write-path tuning knobs.
     *
     * <p>This is the JMH-bench / write-pressure-test counterpart to {@link #dbOpenMemory(Arena)}:
     * the four parameters map 1:1 onto {@code EngineOptions.write_buffer_size}, {@code
     * max_write_buffer_number}, {@code max_background_compactions} and {@code
     * max_background_flushes}. Any parameter passed as {@code 0} keeps the engine's built-in
     * default for that field; out-of-range values flow through {@code
     * EngineOptionsBuilder::try_build} on the Rust side and surface as {@link
     * FrsStatus#INVALID_ARGUMENT}, not a JVM panic.
     *
     * <p>Recommended sustained-write presets (R-loop write-path tuning, 2026-05-10):
     *
     * <ul>
     *   <li>Preset A: {@code (256 MiB, 8, 4, 4)} — 2 GiB total memtable budget, default-ish bg
     *   <li>Preset B: {@code (512 MiB, 4, 8, 8)} — same total memtable, more bg parallelism
     * </ul>
     */
    public FrsDb dbOpenMemoryTuned(
            Arena arena,
            long writeBufferSize,
            long maxWriteBufferNumber,
            long maxBackgroundCompactions,
            long maxBackgroundFlushes) {
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc =
                    (int)
                            frsDbOpenMemoryTuned.invokeExact(
                                    writeBufferSize,
                                    maxWriteBufferNumber,
                                    maxBackgroundCompactions,
                                    maxBackgroundFlushes,
                                    outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_memory_tuned threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_memory_tuned");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /**
     * Opens an in-memory or filesystem-backed engine using a structured {@link
     * org.apache.flink.state.forstrs.config.ForStRsOptions} (B-Prod-P7, spec §6d).
     *
     * <p>Builds a native {@code FrsEngineOptions} struct in {@code arena}, sets the cache / WBM /
     * write-buffer / background-thread fields, then dispatches to {@code frs_db_open_with_options}.
     * {@code dbPath} of {@code null} or empty opens an in-memory engine at {@code /db}; non-null
     * opens an on-disk engine at the given path. Each numeric field of {@code 0} keeps the engine's
     * built-in default for that field.
     *
     * <p>The struct is allocated in {@code arena}; the caller may free it as soon as this method
     * returns. Failure modes:
     *
     * <ul>
     *   <li>{@link FrsStatus#INVALID_ARGUMENT} when any field violates the engine's validation caps
     *       (e.g. cache > 1 PiB).
     *   <li>{@link FrsStatus#PANIC} when the JVM cannot reach the cdylib (lookup failure).
     * </ul>
     */
    public FrsDb dbOpenWithOptions(
            Arena arena,
            String dbPath,
            long writeBufferSize,
            int maxWriteBufferNumber,
            int maxBackgroundCompactions,
            int maxBackgroundFlushes,
            long blockCacheCapacityBytes,
            long writeBufferManagerCapacityBytes) {
        MemorySegment optsSeg =
                allocateEngineOptions(
                        arena,
                        dbPath,
                        writeBufferSize,
                        maxWriteBufferNumber,
                        maxBackgroundCompactions,
                        maxBackgroundFlushes,
                        blockCacheCapacityBytes,
                        writeBufferManagerCapacityBytes);

        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpenWithOptions.invokeExact(optsSeg, outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_with_options threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_with_options");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    private MemorySegment allocateEngineOptions(
            Arena arena,
            String dbPath,
            long writeBufferSize,
            int maxWriteBufferNumber,
            int maxBackgroundCompactions,
            int maxBackgroundFlushes,
            long blockCacheCapacityBytes,
            long writeBufferManagerCapacityBytes) {
        MemorySegment optsSeg = arena.allocate(FRS_ENGINE_OPTIONS_LAYOUT);
        MemorySegment pathSeg =
                (dbPath == null || dbPath.isEmpty())
                        ? MemorySegment.NULL
                        : allocateCString(arena, dbPath);
        // Field offsets: 0, 8, 16, 20, 24, 28(pad), 32, 40 — see
        // FRS_ENGINE_OPTIONS_LAYOUT docstring.
        optsSeg.set(ValueLayout.ADDRESS, 0, pathSeg);
        optsSeg.set(ValueLayout.JAVA_LONG, 8, writeBufferSize);
        optsSeg.set(ValueLayout.JAVA_INT, 16, maxWriteBufferNumber);
        optsSeg.set(ValueLayout.JAVA_INT, 20, maxBackgroundCompactions);
        optsSeg.set(ValueLayout.JAVA_INT, 24, maxBackgroundFlushes);
        // 4 bytes padding at offset 28
        optsSeg.set(ValueLayout.JAVA_LONG, 32, blockCacheCapacityBytes);
        optsSeg.set(ValueLayout.JAVA_LONG, 40, writeBufferManagerCapacityBytes);
        return optsSeg;
    }

    /**
     * Returns the configured WriteBufferManager capacity in bytes (B-Prod-P7, spec §6d). {@code 0}
     * means unbounded (the engine has no cross-CF cap). Useful for tuning ITs that want to verify
     * their requested cap round-tripped through FFI.
     */
    public long dbWriteBufferManagerCapacity(FrsDb db) {
        try {
            return (long) frsDbWbmCapacity.invokeExact(db.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC,
                    "frs_db_write_buffer_manager_capacity threw: " + t.getMessage());
        }
    }

    /**
     * Returns the running cross-CF memtable bytes tracked by the WriteBufferManager (B-Prod-P7,
     * spec §6d). Used by ITs to assert the cap actually fires under load.
     */
    public long dbWriteBufferManagerCurrentBytes(FrsDb db) {
        try {
            return (long) frsDbWbmCurrentBytes.invokeExact(db.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC,
                    "frs_db_write_buffer_manager_current_bytes threw: " + t.getMessage());
        }
    }

    /**
     * Returns the FRS_ABI_VERSION from the loaded native lib. Compare against {@link
     * FrsAbi#EXPECTED_ABI_VERSION} at backend init.
     */
    public int frsAbiVersion() {
        try {
            return (int) frsAbiVersion.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException("frs_abi_version FFI call failed", t);
        }
    }

    /**
     * Opens a filesystem-backed ForSt-RS engine at {@code path}. Caller closes via {@link
     * FrsDb#close()}.
     */
    public FrsDb dbOpen(Arena arena, String path) {
        MemorySegment pathSeg = allocateCString(arena, path);
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpen.invokeExact(pathSeg, outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_db_open threw: " + t.getMessage());
        }
        check(rc, "frs_db_open");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /**
     * Opens a remote-storage-backed engine with a local LRU SST cache (B-Prod-P6).
     *
     * <p>{@code uri} is an OpenDAL URI such as {@code memory://}, {@code file:///abs/path}, or
     * {@code s3://bucket/}. {@code opendalConfigJson} carries any scheme-specific knobs (e.g.
     * {@code {"region":"us-east-1","endpoint":"https://minio.example.com"}}); pass {@code "{}"} or
     * {@code null} when none are needed. {@code cacheDir} is the local directory used for the LRU
     * SST cache; {@code cacheCapacityBytes} caps its on-disk footprint.
     *
     * <p>Caller closes via {@link FrsDb#close()}.
     */
    public FrsDb dbOpenRemote(
            Arena arena,
            String uri,
            String opendalConfigJson,
            String cacheDir,
            long cacheCapacityBytes) {
        MemorySegment uriSeg = allocateCString(arena, uri);
        MemorySegment cfgSeg =
                opendalConfigJson == null
                        ? MemorySegment.NULL
                        : allocateCString(arena, opendalConfigJson);
        MemorySegment cacheDirSeg = allocateCString(arena, cacheDir);
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc =
                    (int)
                            frsDbOpenRemote.invokeExact(
                                    uriSeg, cfgSeg, cacheDirSeg, cacheCapacityBytes, outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_remote threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_remote");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /**
     * Opens a remote-storage-backed engine while also applying the structured engine tuning knobs
     * used by {@link #dbOpenWithOptions}. The native side ignores {@code dbPath} and derives a
     * stable logical remote DB path from {@code uri}; every other field is honoured exactly as in
     * the local open path.
     */
    public FrsDb dbOpenRemoteWithOptions(
            Arena arena,
            String uri,
            String opendalConfigJson,
            String cacheDir,
            long cacheCapacityBytes,
            long writeBufferSize,
            int maxWriteBufferNumber,
            int maxBackgroundCompactions,
            int maxBackgroundFlushes,
            long blockCacheCapacityBytes,
            long writeBufferManagerCapacityBytes) {
        MemorySegment optsSeg =
                allocateEngineOptions(
                        arena,
                        null,
                        writeBufferSize,
                        maxWriteBufferNumber,
                        maxBackgroundCompactions,
                        maxBackgroundFlushes,
                        blockCacheCapacityBytes,
                        writeBufferManagerCapacityBytes);
        MemorySegment uriSeg = allocateCString(arena, uri);
        MemorySegment cfgSeg =
                opendalConfigJson == null
                        ? MemorySegment.NULL
                        : allocateCString(arena, opendalConfigJson);
        MemorySegment cacheDirSeg = allocateCString(arena, cacheDir);
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc =
                    (int)
                            frsDbOpenRemoteWithOptions.invokeExact(
                                    optsSeg,
                                    uriSeg,
                                    cfgSeg,
                                    cacheDirSeg,
                                    cacheCapacityBytes,
                                    outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC,
                    "frs_db_open_remote_with_options threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_remote_with_options");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    /**
     * Opens an engine by restoring its state from a checkpoint directory previously produced by
     * {@link #createCheckpoint(FrsDb, String)}. The checkpoint directory must contain {@code
     * CHECKPOINT.blob} and every SST file the manifest references. The engine is opened with {@code
     * db_path = targetDir}: subsequent reads/writes operate directly on the checkpoint files.
     * Caller closes via {@link FrsDb#close()}.
     */
    public FrsDb dbOpenFromCheckpoint(Arena arena, String targetDir) {
        MemorySegment dirSeg = allocateCString(arena, targetDir);
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpenFromCheckpoint.invokeExact(dirSeg, outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_from_checkpoint threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_from_checkpoint");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    // ------------------------------------------------------------------
    // 2. Column family management
    // ------------------------------------------------------------------

    /** Returns the default column family. Caller closes via {@link FrsCfHandle#close()}. */
    public FrsCfHandle dbDefaultCf(FrsDb db, Arena arena) {
        MemorySegment outCf = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbDefaultCf.invokeExact(db.handle(), outCf);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_default_cf threw: " + t.getMessage());
        }
        check(rc, "frs_db_default_cf");
        MemorySegment cfHandle = outCf.get(ValueLayout.ADDRESS, 0);
        return new FrsCfHandle(this, cfHandle);
    }

    /** Creates a new named column family. Caller closes via {@link FrsCfHandle#close()}. */
    public FrsCfHandle dbCreateCf(FrsDb db, Arena arena, String name) {
        MemorySegment nameSeg = allocateCString(arena, name);
        MemorySegment outCf = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbCreateCf.invokeExact(db.handle(), nameSeg, outCf);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_create_cf threw: " + t.getMessage());
        }
        check(rc, "frs_db_create_cf");
        MemorySegment cfHandle = outCf.get(ValueLayout.ADDRESS, 0);
        return new FrsCfHandle(this, cfHandle);
    }

    /** Opens an existing named column family. Caller closes via {@link FrsCfHandle#close()}. */
    public FrsCfHandle dbOpenCf(FrsDb db, Arena arena, String name) {
        MemorySegment nameSeg = allocateCString(arena, name);
        MemorySegment outCf = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbOpenCf.invokeExact(db.handle(), nameSeg, outCf);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_open_cf threw: " + t.getMessage());
        }
        check(rc, "frs_db_open_cf");
        MemorySegment cfHandle = outCf.get(ValueLayout.ADDRESS, 0);
        return new FrsCfHandle(this, cfHandle);
    }

    /**
     * Returns the handle for the named column family, creating it if it does not yet exist.
     *
     * <p>This is the checkpoint-safe path for CFs that must survive a job restart. On a fresh DB the
     * CF does not exist yet, so it is created ({@code frs_db_create_cf}). On a restore the engine's
     * {@code open_from_checkpoint} has already re-registered every non-default CF from the manifest
     * descriptors (preserving its {@code cf_id}, merge operator, and replayed memtable/SST state),
     * so the CF is opened by name ({@code frs_db_open_cf}) instead — creating it again would fail
     * with "column family '…' already exists". Idempotent within a single DB instance.
     *
     * <p>Distinguishing the two cases on the engine's {@link FrsStatus#NOT_FOUND} status (rather
     * than racing a create) keeps the contract explicit; only a genuine "absent" is treated as
     * fresh, any other failure propagates. Caller closes via {@link FrsCfHandle#close()}.
     */
    public FrsCfHandle dbOpenOrCreateCf(FrsDb db, Arena arena, String name) {
        try {
            return dbOpenCf(db, arena, name);
        } catch (FrsBackendException e) {
            if (e.status() == FrsStatus.NOT_FOUND) {
                return dbCreateCf(db, arena, name);
            }
            throw e;
        }
    }

    // ------------------------------------------------------------------
    // 3. Point ops
    // ------------------------------------------------------------------

    /**
     * Writes a key/value pair.
     *
     * <p>The downcall remains non-critical because the Rust write path can throttle or stall, so
     * the key/value arrays are staged into native memory before crossing FFM.
     */
    public void put(FrsDb db, FrsCfHandle cf, byte[] key, byte[] value) {
        put(db, cf, key, value, 0, value.length);
    }

    /**
     * Writes a key/value pair using a sub-range of the supplied {@code value} byte[]. Allows
     * callers (e.g. V1-sync state classes) to pass the shared internal buffer of a
     * {@link org.apache.flink.core.memory.DataOutputSerializer} without first calling
     * {@code getCopyOfBuffer()} — eliminates one per-event byte[] allocation on the value-state
     * update hot path (PR-B3).
     *
     * @param value the value-bearing buffer (only bytes {@code [valueOffset, valueOffset +
     *     valueLength)} are read by the engine)
     * @param valueOffset offset into {@code value} where the payload starts (must be {@code >= 0})
     * @param valueLength number of payload bytes (must be {@code >= 0} and {@code valueOffset +
     *     valueLength <= value.length})
     */
    public void put(
            FrsDb db,
            FrsCfHandle cf,
            byte[] key,
            byte[] value,
            int valueOffset,
            int valueLength) {
        if (valueOffset < 0
                || valueLength < 0
                || (long) valueOffset + (long) valueLength > value.length) {
            throw new IllegalArgumentException(
                    "value range out of bounds: offset="
                            + valueOffset
                            + " length="
                            + valueLength
                            + " value.length="
                            + value.length);
        }
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg = copyBytesToNative(local, key);
            MemorySegment valSeg = copyBytesRangeToNative(local, value, valueOffset, valueLength);
            int rc;
            try {
                rc =
                        (int)
                                frsPut.invokeExact(
                                        db.handle(),
                                        cf.handle(),
                                        keySeg,
                                        (long) key.length,
                                        valSeg,
                                        (long) valueLength);
            } catch (Throwable t) {
                throw new FrsBackendException(FrsStatus.PANIC, "frs_put threw: " + t.getMessage());
            }
            check(rc, "frs_put");
        }
    }

    /** Returns the value for {@code key} or {@code null} if absent. */
    public byte[] get(FrsDb db, FrsCfHandle cf, byte[] key) {
        return getInternal(frsGet, "frs_get", db, cf, key);
    }

    /** Native status code returned when the value is not inline (too large or in SST). */
    private static final int FRS_STATUS_FALLBACK = 16;

    /**
     * ThreadLocal buffer for the 16-byte out struct used by {@link #getPinned}: 8 bytes for the
     * pointer + 8 bytes for the length. Avoids per-call allocation on the hot path.
     */
    private static final ThreadLocal<byte[]> PINNED_OUT_BUF =
            ThreadLocal.withInitial(() -> new byte[16]);

    /**
     * Zero-copy get from memtable inline storage. Returns the value bytes directly (copied from
     * native pointer) without Rust-side Vec allocation. Returns {@code null} if the value is not
     * inline (caller should fall back to {@link #get}).
     *
     * <p>The returned pointer points directly into the memtable's {@code HashEntry.inline_value:
     * Box<[u8]>}. It is valid until the memtable is flushed (which cannot happen mid-record in
     * Flink's single-threaded-per-slot model).
     *
     * <h3>R44-M1 — Pinned-pointer use-after-flush safety invariant</h3>
     *
     * <p><b>CRITICAL:</b> the pointer returned from {@code frs_get_pinned} (and stored
     * transiently in {@code outBuf}) is a raw pointer into the engine's active memtable inline
     * storage. It remains valid <em>only</em> for as long as that memtable is the active one
     * for this column family. If a memtable flush switches the active memtable between the
     * {@code frs_get_pinned} return and the {@code toArray} copy below, the read on line {@code
     * MemorySegment.ofAddress(ptr).reinterpret(len)} would dereference freed memory — a true
     * use-after-free with all the usual undefined-behavior implications (silent data corruption,
     * SIGSEGV, or arbitrary value bytes).
     *
     * <p>This call site is currently safe <b>solely</b> because of the Flink runtime
     * single-threaded-per-slot invariant: the same task thread that issued {@code frs_get_pinned}
     * also drives any flush trigger (write → switch → flush), so no flush can land between
     * the pin-issue and the copy below within a single record's processing. If a future change
     * ever introduces concurrent flush from a background worker that can race with the slot
     * thread's read path — or if this method is called from a thread other than the slot
     * thread — the invariant breaks and this code becomes unsound.
     *
     * <p>The proper fix (deferred to a follow-up spec) is an engine-side epoch counter:
     * {@code frs_get_pinned} would return both the pointer and the issuing memtable's epoch;
     * a subsequent helper would re-validate the epoch and return {@code FRS_STATUS_FALLBACK}
     * if the memtable has been switched. Until that lands, do NOT call this method from any
     * thread other than the owning slot thread, and do NOT introduce concurrent flush paths.
     */
    public byte[] getPinned(FrsDb db, FrsCfHandle cf, byte[] key) {
        if (OPCOUNT) {

            oc(0);

        }
        MemorySegment keySeg = MemorySegment.ofArray(key);
        byte[] outBuf = PINNED_OUT_BUF.get();
        MemorySegment outSeg = MemorySegment.ofArray(outBuf);
        int rc;
        try {
            rc =
                    (int)
                            frsGetPinned.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keySeg,
                                    (long) key.length,
                                    outSeg.asSlice(0, 8), // out_ptr
                                    outSeg.asSlice(8, 8)); // out_len
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_get_pinned threw: " + t.getMessage());
        }
        if (rc == FRS_STATUS_FALLBACK) {
            return null; // not inline — caller should fall back to get()
        }
        check(rc, "frs_get_pinned");
        // Read ptr + len from the out buffer (heap byte[] — use unaligned reads)
        long ptr = MemorySegment.ofArray(outBuf).get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        long len = MemorySegment.ofArray(outBuf).get(ValueLayout.JAVA_LONG_UNALIGNED, 8);
        if (ptr == 0 || len == 0) {
            return null;
        }
        // R44-M1: see the class-level Javadoc on this method. The single-threaded-per-slot
        // invariant is what makes this read-after-pin safe today; an engine-side epoch
        // counter is the planned fix (deferred to follow-up).
        // Copy from native pointer to byte[] (one copy, no Rust allocation)
        return MemorySegment.ofAddress(ptr).reinterpret(len).toArray(ValueLayout.JAVA_BYTE);
    }

    /**
     * Segment-based variant of {@link #getPinned}. Key passed as caller-owned (segment, offset,
     * length) tuple. Result is written into the caller-provided {@code outSegment} starting at
     * {@code outOffset}; the actual length is returned as a positive int, or -1 if the key was not
     * found.
     *
     * <p>PR-B1 (V2-10): the key slice is passed directly to {@code frs_get_pinned} (no
     * intermediate {@code byte[]}). On a hit, the value is copied native→native from the pinned
     * memtable pointer into {@code outSegment} — also no {@code byte[]} hop. Falls back to a
     * native-{@code outSegment}-based {@code frs_get_fast} path on non-inline values, again
     * with no Java heap allocation. Only the cold-path {@code get()} fallback (for values
     * exceeding the GET_INTO_BUF capacity, which is rare) returns through the legacy
     * {@code byte[]} entry point.
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
        if (OPCOUNT) {

            oc(0);

        }
        // ----- 1. frs_get_pinned: pass key slice directly, no byte[] copy.
        MemorySegment keySlice = keySegment.asSlice(keyOffset, keyLen);
        byte[] outBuf = PINNED_OUT_BUF.get();
        MemorySegment pinnedOutSeg = MemorySegment.ofArray(outBuf);
        int rc;
        try {
            rc =
                    (int)
                            frsGetPinned.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keySlice,
                                    (long) keyLen,
                                    pinnedOutSeg.asSlice(0, 8), // out_ptr
                                    pinnedOutSeg.asSlice(8, 8)); // out_len
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_get_pinned (segment) threw: " + t.getMessage());
        }
        if (rc != FRS_STATUS_FALLBACK) {
            check(rc, "frs_get_pinned");
            long ptr =
                    MemorySegment.ofArray(outBuf).get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            long len =
                    MemorySegment.ofArray(outBuf).get(ValueLayout.JAVA_LONG_UNALIGNED, 8);
            if (ptr == 0 || len == 0) {
                return -1;
            }
            if (len > outMaxLen) {
                throw new IllegalArgumentException(
                        "out segment too small: need " + len + " bytes, got " + outMaxLen);
            }
            // Native → native copy: no byte[] hop.
            MemorySegment nativeSrc = MemorySegment.ofAddress(ptr).reinterpret(len);
            MemorySegment.copy(
                    nativeSrc, 0L, outSegment, outOffset, len);
            return (int) len;
        }

        // ----- 2. Not inline: try frs_get_fast through native staging. The downcall is plain
        // (safepoint-friendly), so heap segments must not cross the FFM boundary.
        try (Arena local = Arena.ofConfined()) {
            MemorySegment nativeKey = copySegmentToNative(local, keySlice, 0L, keyLen);
            MemorySegment fastOutSeg = local.allocate(GET_INTO_BUF_CAP);
            MemorySegment lenSeg = local.allocate(Long.BYTES);
            int fastRc;
            try {
                fastRc =
                        (int)
                                frsGetFast.invokeExact(
                                        db.handle(),
                                        cf.handle(),
                                        nativeKey,
                                        (long) keyLen,
                                        fastOutSeg,
                                        (long) GET_INTO_BUF_CAP,
                                        lenSeg);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_get_fast (segment) threw: " + t.getMessage());
            }
            if (fastRc == 0) {
                long valLen = lenSeg.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
                if (valLen == 0) {
                    return -1;
                }
                if (valLen > outMaxLen) {
                    throw new IllegalArgumentException(
                            "out segment too small: need " + valLen + " bytes, got " + outMaxLen);
                }
                MemorySegment.copy(fastOutSeg, 0L, outSegment, outOffset, valLen);
                return (int) valLen;
            }
        }

        // ----- 3. Cold path: value > GET_INTO_BUF_CAP — fall back to legacy byte[] entry.
        // Rare in practice (state values are typically a few KB; GET_INTO_BUF is 64 KB).
        byte[] keyBytes = new byte[keyLen];
        MemorySegment.copy(keySegment, ValueLayout.JAVA_BYTE, keyOffset, keyBytes, 0, keyLen);
        byte[] raw = get(db, cf, keyBytes);
        if (raw == null) {
            return -1;
        }
        if (raw.length > outMaxLen) {
            throw new IllegalArgumentException(
                    "out segment too small: need " + raw.length + " bytes, got " + outMaxLen);
        }
        MemorySegment.copy(raw, 0, outSegment, ValueLayout.JAVA_BYTE, outOffset, raw.length);
        return raw.length;
    }

    /**
     * Segment-based variant of {@link #put}. Caller-owned segments for both key and value. The
     * key/value slices are passed directly to {@code frs_put} (no per-call {@code byte[]}
     * allocation) — PR-B1 (V2-10).
     */
    public void putSegment(
            FrsDb db,
            FrsCfHandle cf,
            MemorySegment keySegment,
            long keyOffset,
            int keyLen,
            MemorySegment valueSegment,
            long valueOffset,
            int valueLen) {
        if (OPCOUNT) {

            oc(1);

        }
        // D-C5R1-NEW-H1: frs_put is plain-bound (not critical) per D-R4-NEW-H1
        // (write_controller.may_throttle can sleep — critical mode would suspend
        // safepoints). Plain downcall handles REJECT heap MemorySegments at
        // invocation with IllegalArgumentException; only native segments pass.
        // If either input is heap-backed (MemorySegment.ofArray(byte[])), stage
        // both into a per-call confined Arena. The implicit alloc cost is the
        // price for never blocking GC during throttle stalls.
        boolean keyHeap = !keySegment.isNative();
        boolean valHeap = !valueSegment.isNative();
        if (keyHeap || valHeap) {
            try (java.lang.foreign.Arena local = java.lang.foreign.Arena.ofConfined()) {
                MemorySegment keyNative =
                        keyHeap
                                ? copyToNative(local, keySegment, keyOffset, keyLen)
                                : keySegment.asSlice(keyOffset, keyLen);
                MemorySegment valNative =
                        valHeap
                                ? copyToNative(local, valueSegment, valueOffset, valueLen)
                                : valueSegment.asSlice(valueOffset, valueLen);
                int rcLocal;
                try {
                    rcLocal =
                            (int)
                                    frsPut.invokeExact(
                                            db.handle(),
                                            cf.handle(),
                                            keyNative,
                                            (long) keyLen,
                                            valNative,
                                            (long) valueLen);
                } catch (Throwable t) {
                    throw new FrsBackendException(
                            FrsStatus.PANIC, "frs_put (segment) threw: " + t.getMessage());
                }
                check(rcLocal, "frs_put");
                return;
            }
        }
        MemorySegment keySlice = keySegment.asSlice(keyOffset, keyLen);
        MemorySegment valSlice = valueSegment.asSlice(valueOffset, valueLen);
        int rc;
        try {
            rc =
                    (int)
                            frsPut.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keySlice,
                                    (long) keyLen,
                                    valSlice,
                                    (long) valueLen);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_put (segment) threw: " + t.getMessage());
        }
        check(rc, "frs_put");
    }

    /**
     * D-C5R1-NEW-H1 helper: stage a heap-backed MemorySegment slice into a native segment in
     * the given arena. Cheap allocation + single bulk copy; called only when the caller passes
     * a heap segment (e.g., {@code MemorySegment.ofArray(byte[])}).
     */
    private static MemorySegment copyToNative(
            java.lang.foreign.Arena arena,
            MemorySegment src,
            long srcOffset,
            int len) {
        MemorySegment dst = arena.allocate(len);
        MemorySegment.copy(src, srcOffset, dst, 0L, len);
        return dst;
    }

    /**
     * Segment-based delete (key only). The key slice is passed directly to {@code frs_delete} — no
     * {@code byte[]} allocation (PR-B1 / V2-10).
     */
    public void deleteSegment(
            FrsDb db,
            FrsCfHandle cf,
            MemorySegment keySegment,
            long keyOffset,
            int keyLen) {
        if (OPCOUNT) {

            oc(1);

        }
        // D-C5R1-NEW-H1 (sister): frs_delete is plain-bound; heap segments
        // must be staged to native. See putSegment for full rationale.
        if (!keySegment.isNative()) {
            try (java.lang.foreign.Arena local = java.lang.foreign.Arena.ofConfined()) {
                MemorySegment keyNative = copyToNative(local, keySegment, keyOffset, keyLen);
                int rcLocal;
                try {
                    rcLocal =
                            (int)
                                    frsDelete.invokeExact(
                                            db.handle(),
                                            cf.handle(),
                                            keyNative,
                                            (long) keyLen);
                } catch (Throwable t) {
                    throw new FrsBackendException(
                            FrsStatus.PANIC, "frs_delete (segment) threw: " + t.getMessage());
                }
                check(rcLocal, "frs_delete");
                return;
            }
        }
        MemorySegment keySlice = keySegment.asSlice(keyOffset, keyLen);
        int rc;
        try {
            rc =
                    (int)
                            frsDelete.invokeExact(
                                    db.handle(), cf.handle(), keySlice, (long) keyLen);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_delete (segment) threw: " + t.getMessage());
        }
        check(rc, "frs_delete");
    }

    /**
     * Combined get + put in one FFM call. Returns the old value ({@code null} if the key did not
     * exist before the put). The put always succeeds regardless of whether the key existed.
     *
     * <p>Saves one FFM boundary crossing vs separate {@link #get} + {@link #put} for the dominant
     * ValueState read-modify-write pattern.
     */
    public byte[] getAndPut(FrsDb db, FrsCfHandle cf, byte[] key, byte[] newValue) {
        return getAndPut(db, cf, key, newValue, 0, newValue.length);
    }

    /**
     * Sub-range variant of {@link #getAndPut(FrsDb, FrsCfHandle, byte[], byte[])}. Lets V1-sync
     * state classes pass the shared internal buffer of a
     * {@link org.apache.flink.core.memory.DataOutputSerializer} directly, avoiding the per-event
     * {@code getCopyOfBuffer()} allocation on the read-modify-write hot path (PR-B3).
     */
    public byte[] getAndPut(
            FrsDb db,
            FrsCfHandle cf,
            byte[] key,
            byte[] newValue,
            int newValueOffset,
            int newValueLength) {
        if (OPCOUNT) {

            oc(0);

        } // RMW: one fused get+put FFM crossing
        if (newValueOffset < 0
                || newValueLength < 0
                || (long) newValueOffset + (long) newValueLength > newValue.length) {
            throw new IllegalArgumentException(
                    "newValue range out of bounds: offset="
                            + newValueOffset
                            + " length="
                            + newValueLength
                            + " newValue.length="
                            + newValue.length);
        }
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg = copyBytesToNative(local, key);
            MemorySegment valSeg =
                    copyBytesRangeToNative(local, newValue, newValueOffset, newValueLength);
            MemorySegment outBytes = local.allocate(24);
            int rc;
            try {
                rc =
                        (int)
                                frsGetAndPut.invokeExact(
                                        db.handle(),
                                        cf.handle(),
                                        keySeg,
                                        (long) key.length,
                                        valSeg,
                                        (long) newValueLength,
                                        outBytes);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_get_and_put threw: " + t.getMessage());
            }
            if (rc == FrsStatus.NOT_FOUND.code()) {
                return null; // key didn't exist before the put (put still succeeded)
            }
            check(rc, "frs_get_and_put");

            long dataAddr = ((MemorySegment) FRS_BYTES_DATA_U.get(outBytes, 0L)).address();
            long len = (long) FRS_BYTES_LEN_U.get(outBytes, 0L);
            if (dataAddr == 0L) {
                return null;
            }
            return copyAndFreeRaw(outBytes, dataAddr, len, "frs_get_and_put/free");
        }
    }

    /**
     * Batched put — writes {@code count} key/value pairs in one engine call.
     *
     * <p>The four "parallel array" segments must be laid out in native memory:
     *
     * <ul>
     *   <li>{@code keyPtrs}: {@code count} {@code uint8_t*} entries — each pointer targets a key
     *       buffer whose size is the matching {@code keyLens[i]}.
     *   <li>{@code keyLens}: {@code count} {@code size_t} entries (8 bytes each).
     *   <li>{@code valuePtrs}: {@code count} {@code uint8_t*} entries.
     *   <li>{@code valueLens}: {@code count} {@code size_t} entries.
     * </ul>
     *
     * <p>The key/value buffers themselves can live anywhere (native or pinned heap) as long as the
     * pointers inside {@code keyPtrs} / {@code valuePtrs} remain valid for the duration of this
     * call. Hot path callers (e.g. JMH bench) stage everything once into a long-lived native arena
     * and never copy.
     */
    public void batchPut(
            FrsDb db,
            FrsCfHandle cf,
            MemorySegment keyPtrs,
            MemorySegment keyLens,
            MemorySegment valuePtrs,
            MemorySegment valueLens,
            long count) {
        int rc;
        try {
            rc =
                    (int)
                            frsBatchPut.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keyPtrs,
                                    keyLens,
                                    valuePtrs,
                                    valueLens,
                                    count);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_batch_put threw: " + t.getMessage());
        }
        check(rc, "frs_batch_put");
    }

    /**
     * Convenience overload — stages {@code keys} / {@code values} into a fresh confined arena and
     * forwards to {@link #batchPut(FrsDb, FrsCfHandle, MemorySegment, MemorySegment, MemorySegment,
     * MemorySegment, long)}. The staging cost (one alloc + N+N copies) makes this UNSUITABLE for
     * benchmarking; use the segment overload with pre-staged buffers for the hot path.
     */
    public void batchPut(FrsDb db, FrsCfHandle cf, byte[][] keys, byte[][] values) {
        if (keys.length != values.length) {
            throw new IllegalArgumentException(
                    "keys.length (" + keys.length + ") != values.length (" + values.length + ")");
        }
        int count = keys.length;
        if (count == 0) {
            return;
        }
        try (Arena local = Arena.ofConfined()) {
            // Stage keys + values payloads into native memory so that their pointers
            // remain stable for the duration of the downcall.
            MemorySegment keyPtrs = local.allocate((long) count * ValueLayout.ADDRESS.byteSize());
            MemorySegment keyLens = local.allocate((long) count * ValueLayout.JAVA_LONG.byteSize());
            MemorySegment valPtrs = local.allocate((long) count * ValueLayout.ADDRESS.byteSize());
            MemorySegment valLens = local.allocate((long) count * ValueLayout.JAVA_LONG.byteSize());
            for (int i = 0; i < count; i++) {
                byte[] k = keys[i];
                byte[] v = values[i];
                MemorySegment ks = local.allocate(k.length == 0 ? 1 : k.length);
                MemorySegment vs = local.allocate(v.length == 0 ? 1 : v.length);
                if (k.length > 0) {
                    MemorySegment.copy(k, 0, ks, ValueLayout.JAVA_BYTE, 0, k.length);
                }
                if (v.length > 0) {
                    MemorySegment.copy(v, 0, vs, ValueLayout.JAVA_BYTE, 0, v.length);
                }
                keyPtrs.set(ValueLayout.ADDRESS, (long) i * ValueLayout.ADDRESS.byteSize(), ks);
                valPtrs.set(ValueLayout.ADDRESS, (long) i * ValueLayout.ADDRESS.byteSize(), vs);
                keyLens.set(
                        ValueLayout.JAVA_LONG,
                        (long) i * ValueLayout.JAVA_LONG.byteSize(),
                        (long) k.length);
                valLens.set(
                        ValueLayout.JAVA_LONG,
                        (long) i * ValueLayout.JAVA_LONG.byteSize(),
                        (long) v.length);
            }
            batchPut(db, cf, keyPtrs, keyLens, valPtrs, valLens, count);
        }
    }

    /**
     * Batch point-lookup: reads {@code count} keys in one FFM call, amortizing the boundary
     * crossing cost across N lookups.
     *
     * <p>The caller supplies pre-staged native segments for key pointers and key lengths. The
     * method allocates the output FrsBytes array internally and reads/frees each result.
     *
     * @return array of length {@code count}; null entries mean "not found".
     */
    // R0C-NEW-H1 Tier-1: legacy `batchGet(byte[][])` and segment-ptr-array variant
    // removed under the byte[]/byte[][] ban. Callers use `vectorizedBatchGet` (Arrow
    // off-heap output) or `batchGetArrow` (Arrow C Data Interface output) instead.

    /**
     * C1 zero-copy batch_put via the Arrow C Data Interface.
     *
     * <p>The caller pre-builds an Arrow {@code RecordBatch} with the canonical schema {@code key:
     * Binary, value: Binary nullable, op_type: UInt8} and exports it to two native segments via
     * Arrow-Java's {@code Data.exportArray} (or any equivalent Arrow C Data Interface producer).
     * Both pointers MUST point to the standard {@code FFI_ArrowArray} / {@code FFI_ArrowSchema}
     * layouts.
     *
     * <p>The native side takes ownership of {@code *array} and {@code *schema} per the Arrow C Data
     * Interface contract: it reads them, then zeroes the originals so the caller does NOT have to
     * free them — release callbacks are invalidated in-place.
     *
     * <p>This bypasses the legacy {@link #batchPut} path's per-row {@code Vec<u8>} allocation in
     * WriteBatch + the subsequent {@code Vec<&[u8]>} re-borrow in {@code db.batch_write}.
     * Engine-level micro-bench shows ~1.8× speedup vs the WriteBatch path on the 1000-row Put-only
     * workload (see {@code crates/forst-rs-bench/benches/batch_put_arrow_vs_write_batch.rs}).
     *
     * @param db database handle
     * @param cf column family handle
     * @param arrayPtr pointer to a populated {@code FFI_ArrowArray} (caller-owned alloc; contents
     *     are consumed by this call)
     * @param schemaPtr pointer to a populated {@code FFI_ArrowSchema} (caller-owned alloc; contents
     *     are consumed by this call)
     */
    public void batchPutArrow(
            FrsDb db, FrsCfHandle cf, MemorySegment arrayPtr, MemorySegment schemaPtr) {
        int rc;
        try {
            rc = (int) frsBatchPutArrow.invokeExact(db.handle(), cf.handle(), arrayPtr, schemaPtr);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_batch_put_arrow threw: " + t.getMessage());
        }
        check(rc, "frs_batch_put_arrow");
    }

    // R0C-NEW-H1: `batchGetReuse(byte[][])` removed — superseded by `vectorizedBatchGet`
    // (off-heap segment-based) under the byte[]/byte[][] ban. Zero live or test callers
    // when removed.

    private static final long ARROW_SCHEMA_BYTES = 72L;
    private static final long ARROW_ARRAY_BYTES = FFI_ARROW_ARRAY_LAYOUT.byteSize();

    /**
     * Arrow-based batch get: stages keys as Arrow BinaryArray, calls frs_batch_get_arrow, reads
     * results from the returned Arrow StructArray. Zero-copy on the return path — values are read
     * directly from the contiguous Arrow buffer via offset arithmetic.
     */
    public byte[][] batchGetArrow(FrsDb db, FrsCfHandle cf, byte[][] keys) {
        int count = keys.length;
        if (count == 0) {
            return new byte[0][];
        }
        try (Arena local = Arena.ofConfined()) {
            long ptrSz = ValueLayout.ADDRESS.byteSize();

            // Build release stub (no-op — we own all memory in the local arena)
            MemorySegment releaseStub = buildArrowReleaseStub(local);

            // Stage keys as Arrow BinaryArray: offsets[count+1] + data
            int totalKeyBytes = 0;
            for (byte[] k : keys) {
                totalKeyBytes += k.length;
            }
            MemorySegment keyOffsets = local.allocate((count + 1) * 4L, 4L);
            MemorySegment keyData = local.allocate(totalKeyBytes);
            int off = 0;
            for (int i = 0; i < count; i++) {
                keyOffsets.set(ValueLayout.JAVA_INT, 4L * i, off);
                MemorySegment.copy(keys[i], 0, keyData, ValueLayout.JAVA_BYTE, off, keys[i].length);
                off += keys[i].length;
            }
            keyOffsets.set(ValueLayout.JAVA_INT, 4L * count, off);

            // BinaryArray buffers: [validity(NULL), offsets, data]
            MemorySegment keyBufs = local.allocate(3 * ptrSz);
            keyBufs.set(ValueLayout.ADDRESS, 0, MemorySegment.NULL);
            keyBufs.set(ValueLayout.ADDRESS, ptrSz, keyOffsets);
            keyBufs.set(ValueLayout.ADDRESS, 2 * ptrSz, keyData);

            // Build FFI_ArrowArray for keys
            MemorySegment keysArray = local.allocate(ARROW_ARRAY_BYTES);
            writeArrowArray(keysArray, count, 0, 0, 3, 0, keyBufs, MemorySegment.NULL, releaseStub);

            // Build FFI_ArrowSchema for keys (format "z" = Binary)
            MemorySegment fmtBinary = cstring(local, "z");
            MemorySegment keysSchema = local.allocate(ARROW_SCHEMA_BYTES);
            writeArrowSchema(
                    keysSchema,
                    fmtBinary,
                    MemorySegment.NULL,
                    0L,
                    0,
                    MemorySegment.NULL,
                    releaseStub);

            // Allocate output structs
            MemorySegment outArray = local.allocate(ARROW_ARRAY_BYTES);
            MemorySegment outSchema = local.allocate(ARROW_SCHEMA_BYTES);

            // Call frs_batch_get_arrow
            int rc;
            try {
                rc =
                        (int)
                                frsBatchGetArrow.invokeExact(
                                        db.handle(),
                                        cf.handle(),
                                        keysArray,
                                        keysSchema,
                                        outArray,
                                        outSchema);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_batch_get_arrow threw: " + t.getMessage());
            }
            check(rc, "frs_batch_get_arrow");

            // Parse returned StructArray: children[0] = value (Binary nullable), children[1] =
            // found (Boolean)
            // StructArray layout: n_children=2, children points to [valueArray, foundArray]
            // Typed-layout access via FFI_ARROW_ARRAY_LAYOUT VarHandles (PR-F2 / D-R2-2).
            long nChildren = (long) ARROW_ARRAY_N_CHILDREN.get(outArray, 0L);
            MemorySegment childrenPtr =
                    ((MemorySegment) ARROW_ARRAY_CHILDREN.get(outArray, 0L))
                            .reinterpret(nChildren * ptrSz);

            // Value child (BinaryArray): buffers = [validity, offsets, data]
            MemorySegment valueArrayPtr =
                    childrenPtr.get(ValueLayout.ADDRESS, 0).reinterpret(ARROW_ARRAY_BYTES);
            long valueNBuffers = (long) ARROW_ARRAY_N_BUFFERS.get(valueArrayPtr, 0L);
            MemorySegment valueBufsPtr =
                    ((MemorySegment) ARROW_ARRAY_BUFFERS.get(valueArrayPtr, 0L))
                            .reinterpret(valueNBuffers * ptrSz);

            MemorySegment validityPtr = valueBufsPtr.get(ValueLayout.ADDRESS, 0);
            MemorySegment valueOffsetsPtr =
                    valueBufsPtr.get(ValueLayout.ADDRESS, ptrSz).reinterpret((count + 1) * 4L);
            MemorySegment valueDataPtr = valueBufsPtr.get(ValueLayout.ADDRESS, 2 * ptrSz);

            // R67-H2: the value-decode loop's `MemorySegment.copy` and
            // `new byte[len]` can throw on OOM or malformed offsets.
            // Pre-fix the Arrow release callbacks at the bottom of the
            // function sat AFTER the loop with NO try/finally guard —
            // any throw from the loop body bypassed the release calls
            // and leaked the engine-owned Arrow output struct (children
            // buffers, dictionaries, and any release-callback-owned
            // payloads). The local Arena reclaimed only the
            // outArray/outSchema struct slots, not the engine heap the
            // pointers reference.
            //
            // Wrap the loop in try/finally so the release callbacks
            // ALWAYS fire. If a release callback itself throws, we
            // capture it and rethrow after invoking the sister release
            // — guaranteeing no native heap leak regardless of which
            // step fails.
            byte[][] results = new byte[count][];
            Throwable loopFailure = null;
            try {
                for (int i = 0; i < count; i++) {
                    // Check validity bitmap
                    boolean isNull;
                    if (validityPtr.address() == 0L) {
                        isNull = false; // no validity bitmap = all valid
                    } else {
                        MemorySegment validitySeg = validityPtr.reinterpret((count + 7) / 8);
                        int byteIdx = i / 8;
                        int bitIdx = i % 8;
                        isNull =
                                (validitySeg.get(ValueLayout.JAVA_BYTE, byteIdx) & (1 << bitIdx))
                                        == 0;
                    }
                    if (isNull) {
                        results[i] = null;
                    } else {
                        int start = valueOffsetsPtr.get(ValueLayout.JAVA_INT, 4L * i);
                        int end = valueOffsetsPtr.get(ValueLayout.JAVA_INT, 4L * (i + 1));
                        int len = end - start;
                        results[i] = new byte[len];
                        MemorySegment dataSeg = valueDataPtr.reinterpret((long) end);
                        MemorySegment.copy(
                                dataSeg, ValueLayout.JAVA_BYTE, start, results[i], 0, len);
                    }
                }
            } catch (Throwable t) {
                loopFailure = t;
            } finally {
                // Release the output Arrow structs (call the release callback).
                // D8-H2: use the cached release-handle lookup; was spinning a fresh
                // downcallHandle per call which pinned JIT metadata on the hot read path.
                Throwable releaseFailure = null;
                try {
                    MemorySegment outRelease =
                            (MemorySegment) ARROW_ARRAY_RELEASE.get(outArray, 0L);
                    invokeArrowRelease(outRelease, outArray);
                } catch (Throwable t) {
                    releaseFailure = t;
                }
                try {
                    MemorySegment outSchemaRelease = outSchema.get(ValueLayout.ADDRESS, 56);
                    invokeArrowRelease(outSchemaRelease, outSchema);
                } catch (Throwable t) {
                    if (releaseFailure == null) {
                        releaseFailure = t;
                    } else {
                        releaseFailure.addSuppressed(t);
                    }
                }
                if (loopFailure != null) {
                    if (releaseFailure != null) {
                        loopFailure.addSuppressed(releaseFailure);
                    }
                } else if (releaseFailure != null) {
                    loopFailure = releaseFailure;
                }
            }
            if (loopFailure != null) {
                if (loopFailure instanceof RuntimeException) {
                    throw (RuntimeException) loopFailure;
                }
                if (loopFailure instanceof Error) {
                    throw (Error) loopFailure;
                }
                throw new FrsBackendException(
                        FrsStatus.PANIC, "batchGetArrow: " + loopFailure.getMessage());
            }

            return results;
        }
    }

    // -----------------------------------------------------------------
    // Vectorized batch ops (caller-owned Arrow BinaryArray buffers)
    //
    // These accept pre-staged off-heap (offsets, data) segments built by
    // the Java-side {@code ColumnarBatchBuffer} and dispatch the entire
    // batch via a single FFM downcall — see spec
    // 2026-05-15-forst-rs-vectorized-executor-design.md §C4.
    //
    // Status:
    //   OK                  — success; outputs valid
    //   BUFFER_TOO_SMALL    — out_data_cap insufficient; caller may grow
    //                         to *outDataLenSeg(asLong) bytes and retry
    //   any other negative  — engine error (mapped via {@link #check})
    // -----------------------------------------------------------------

    /**
     * Vectorized batch GET. Pass pre-staged off-heap {@link MemorySegment}s for input
     * key_offsets/key_data and caller-allocated output buffers (offsets/data/validity). On success
     * the caller should consult {@code outDataLenSeg.get(JAVA_LONG, 0)} for the total bytes
     * actually written into {@code outDataSeg}.
     *
     * <p>Returns the raw FFI status code so the caller can distinguish OK vs BUFFER_TOO_SMALL
     * without an exception. Callers that don't need that distinction can use {@link
     * #vectorizedBatchGetChecked} instead.
     */
    public int vectorizedBatchGet(
            FrsDb db,
            FrsCfHandle cf,
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            long count,
            MemorySegment outOffsetsSeg,
            MemorySegment outDataSeg,
            MemorySegment outValiditySeg,
            long outDataCap,
            MemorySegment outDataLenSeg) {
        try {
            return (int)
                    frsVectorizedBatchGet.invokeExact(
                            db.handle(),
                            cf.handle(),
                            keyOffsetsSeg,
                            keyDataSeg,
                            keyDataSeg.byteSize(),
                            count,
                            outOffsetsSeg,
                            outDataSeg,
                            outValiditySeg,
                            outDataCap,
                            outDataLenSeg);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_vectorized_batch_get threw: " + t.getMessage());
        }
    }

    /**
     * Vectorized batch GET that throws on any non-OK status. The {@code BUFFER_TOO_SMALL} case is
     * surfaced as a regular {@link FrsBackendException}; callers that want a retry loop should use
     * {@link #vectorizedBatchGet} and check the status directly.
     */
    public void vectorizedBatchGetChecked(
            FrsDb db,
            FrsCfHandle cf,
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            long count,
            MemorySegment outOffsetsSeg,
            MemorySegment outDataSeg,
            MemorySegment outValiditySeg,
            long outDataCap,
            MemorySegment outDataLenSeg) {
        int rc =
                vectorizedBatchGet(
                        db,
                        cf,
                        keyOffsetsSeg,
                        keyDataSeg,
                        count,
                        outOffsetsSeg,
                        outDataSeg,
                        outValiditySeg,
                        outDataCap,
                        outDataLenSeg);
        check(rc, "frs_vectorized_batch_get");
    }

    /** Vectorized batch PUT. */
    public void vectorizedBatchPut(
            FrsDb db,
            FrsCfHandle cf,
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            MemorySegment valOffsetsSeg,
            MemorySegment valDataSeg,
            long count) {
        if (OPCOUNT) {

            oc(1);

        }
        int rc;
        try {
            rc =
                    (int)
                            frsVectorizedBatchPut.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keyOffsetsSeg,
                                    keyDataSeg,
                                    keyDataSeg.byteSize(),
                                    valOffsetsSeg,
                                    valDataSeg,
                                    valDataSeg.byteSize(),
                                    count);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_vectorized_batch_put threw: " + t.getMessage());
        }
        checkVectorized(rc, "frs_vectorized_batch_put");
    }

    /**
     * R91-H1: defensive rc check for vectorized FFI paths that may return
     * {@link FrsErrorCode} values outside the legacy {@link FrsStatus}
     * enum (codes 100/101/110/200/201/300/301/302/303/900/999). Bare
     * {@link #check(int, String)} would throw {@code IllegalArgumentException}
     * from {@link FrsStatus#fromCode} on those codes — bypassing typed
     * error handling and, more importantly, NOT escalating
     * {@code PANIC_CAUGHT=900} to {@code FrsEnginePanicError} the way the
     * GET path does inline (VectorizedExecutor.java:828-836). Without
     * fatal escalation a panicked engine state on the WRITE path is
     * silently swallowed and subsequent writes operate on poisoned state.
     *
     * <p>This helper decodes via {@link FrsErrorCode#fromU32} and throws
     * {@link FrsBackendException} carrying the typed code; the caller's
     * downstream {@code fatalHandler} (set on the executor) is invoked
     * directly here when the code is fail-process.
     */
    private static void checkVectorized(int rc, String fn) {
        if (rc == FrsStatus.OK.code()) {
            return;
        }
        FrsErrorCode err = FrsErrorCode.fromU32(rc);
        if (err.isFailProcess()) {
            throw new FrsEnginePanicError(err, fn + " rc=" + rc);
        }
        // Map back into FrsStatus for backwards-compat callers; fall
        // back to PANIC on unknown codes so an unmatched rc surfaces as
        // a typed FrsBackendException rather than an IAE.
        FrsStatus status;
        try {
            status = FrsStatus.fromCode(rc);
        } catch (IllegalArgumentException ignored) {
            status = FrsStatus.PANIC;
        }
        throw new FrsBackendException(status, fn + " rc=" + rc + " errCode=" + err);
    }

    /** Vectorized batch DELETE. */
    public void vectorizedBatchDelete(
            FrsDb db,
            FrsCfHandle cf,
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            long count) {
        int rc;
        try {
            rc =
                    (int)
                            frsVectorizedBatchDelete.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    keyOffsetsSeg,
                                    keyDataSeg,
                                    keyDataSeg.byteSize(),
                                    count);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_vectorized_batch_delete threw: " + t.getMessage());
        }
        checkVectorized(rc, "frs_vectorized_batch_delete");
    }

    /**
     * Stage-3 Unit-2: vectorized MIXED write batch — puts, deletes, and merge-appends in ONE FFI
     * crossing with ONE atomic engine sequence allocation.
     *
     * <p>Layout contract (mirrors {@code frs_vectorized_batch_mixed} in {@code
     * crates/forst-rs-ffi/src/lib.rs}):
     *
     * <ul>
     *   <li>{@code kindsSeg}: {@code count} bytes — {@code 0} = Delete, {@code 1} = Put, {@code 2}
     *       = Merge (engine {@code OpType} discriminants, forwarded verbatim).
     *   <li>{@code keyOffsetsSeg} / {@code keyDataSeg} and {@code valOffsetsSeg} / {@code
     *       valDataSeg}: Arrow BinaryArray columns ({@code count + 1} i32 offsets + flat data).
     *   <li>Delete rows MUST have an empty value slice ({@code valOffsets[i] == valOffsets[i+1]});
     *       a non-empty delete value is rejected as {@code BatchHeaderMalformed}.
     *   <li>Merge rows require the CF to have a merge operator (checked engine-side, lazily).
     *   <li>Rows apply in array order under a single seq-range: a later row for the same key wins
     *       (delete-then-put yields the put); Merge operands concatenate in row order.
     * </ul>
     *
     * <p>Same non-critical bind + {@link #checkVectorized} escalation as the put/delete sisters.
     */
    public void vectorizedBatchMixed(
            FrsDb db,
            FrsCfHandle cf,
            MemorySegment kindsSeg,
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            MemorySegment valOffsetsSeg,
            MemorySegment valDataSeg,
            long count) {
        if (OPCOUNT) {

            oc(1);

        }
        int rc;
        try {
            rc =
                    (int)
                            frsVectorizedBatchMixed.invokeExact(
                                    db.handle(),
                                    cf.handle(),
                                    kindsSeg,
                                    count,
                                    keyOffsetsSeg,
                                    keyDataSeg,
                                    keyDataSeg.byteSize(),
                                    valOffsetsSeg,
                                    valDataSeg,
                                    valDataSeg.byteSize());
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_vectorized_batch_mixed threw: " + t.getMessage());
        }
        checkVectorized(rc, "frs_vectorized_batch_mixed");
    }

    // -----------------------------------------------------------------
    // Explicit WriteBatch (SP4)
    //
    // Pattern:
    //   long h = linker.writebatchOpen(arena);
    //   linker.writebatchPut(h, cf, kOffs, kData, vOffs, vData, n);
    //   linker.writebatchDelete(h, cf, dOffs, dData, m);
    //   linker.writebatchCommit(h, db);  // h is invalid after this returns
    // -----------------------------------------------------------------

    /** Opens a fresh WriteBatch handle. Returns the raw native pointer as a long. */
    public long writebatchOpen(Arena arena) {
        MemorySegment out = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsWritebatchOpen.invokeExact(out);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_writebatch_open threw: " + t.getMessage());
        }
        check(rc, "frs_writebatch_open");
        return out.get(ValueLayout.ADDRESS, 0).address();
    }

    /** Appends a vectorized put batch to the WriteBatch. */
    public void writebatchPut(
            long handle,
            FrsCfHandle cf,
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            MemorySegment valOffsetsSeg,
            MemorySegment valDataSeg,
            long count) {
        int rc;
        try {
            rc =
                    (int)
                            frsWritebatchPut.invokeExact(
                                    MemorySegment.ofAddress(handle),
                                    cf.handle(),
                                    keyOffsetsSeg,
                                    keyDataSeg,
                                    valOffsetsSeg,
                                    valDataSeg,
                                    count);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_writebatch_put threw: " + t.getMessage());
        }
        check(rc, "frs_writebatch_put");
    }

    /** Appends a vectorized delete batch to the WriteBatch. */
    public void writebatchDelete(
            long handle,
            FrsCfHandle cf,
            MemorySegment keyOffsetsSeg,
            MemorySegment keyDataSeg,
            long count) {
        int rc;
        try {
            rc =
                    (int)
                            frsWritebatchDelete.invokeExact(
                                    MemorySegment.ofAddress(handle),
                                    cf.handle(),
                                    keyOffsetsSeg,
                                    keyDataSeg,
                                    count);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_writebatch_delete threw: " + t.getMessage());
        }
        check(rc, "frs_writebatch_delete");
    }

    /** Commits the WriteBatch atomically. The handle is invalid after this returns. */
    public void writebatchCommit(long handle, FrsDb db) {
        int rc;
        try {
            rc =
                    (int)
                            frsWritebatchCommit.invokeExact(
                                    MemorySegment.ofAddress(handle), db.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_writebatch_commit threw: " + t.getMessage());
        }
        check(rc, "frs_writebatch_commit");
    }

    /** Drops the WriteBatch without committing. Safe on a 0/invalid handle. */
    public void writebatchClose(long handle) {
        int rc;
        try {
            rc = (int) frsWritebatchClose.invokeExact(MemorySegment.ofAddress(handle));
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_writebatch_close threw: " + t.getMessage());
        }
        check(rc, "frs_writebatch_close");
    }

    private static void writeArrowArray(
            MemorySegment seg,
            long length,
            long nullCount,
            long offset,
            long nBuffers,
            long nChildren,
            MemorySegment buffers,
            MemorySegment children,
            MemorySegment release) {
        seg.set(ValueLayout.JAVA_LONG, 0, length);
        seg.set(ValueLayout.JAVA_LONG, 8, nullCount);
        seg.set(ValueLayout.JAVA_LONG, 16, offset);
        seg.set(ValueLayout.JAVA_LONG, 24, nBuffers);
        seg.set(ValueLayout.JAVA_LONG, 32, nChildren);
        seg.set(ValueLayout.ADDRESS, 40, buffers);
        seg.set(ValueLayout.ADDRESS, 48, children);
        seg.set(ValueLayout.ADDRESS, 56, MemorySegment.NULL);
        seg.set(ValueLayout.ADDRESS, 64, release);
        seg.set(ValueLayout.ADDRESS, 72, MemorySegment.NULL);
    }

    private static void writeArrowSchema(
            MemorySegment seg,
            MemorySegment format,
            MemorySegment name,
            long flags,
            long nChildren,
            MemorySegment children,
            MemorySegment release) {
        seg.set(ValueLayout.ADDRESS, 0, format);
        seg.set(ValueLayout.ADDRESS, 8, name != null ? name : MemorySegment.NULL);
        seg.set(ValueLayout.ADDRESS, 16, MemorySegment.NULL);
        seg.set(ValueLayout.JAVA_LONG, 24, flags);
        seg.set(ValueLayout.JAVA_LONG, 32, nChildren);
        seg.set(ValueLayout.ADDRESS, 40, children != null ? children : MemorySegment.NULL);
        seg.set(ValueLayout.ADDRESS, 48, MemorySegment.NULL);
        seg.set(ValueLayout.ADDRESS, 56, release);
        seg.set(ValueLayout.ADDRESS, 64, MemorySegment.NULL);
    }

    private static MemorySegment cstring(Arena arena, String s) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        MemorySegment seg = arena.allocate(bytes.length + 1L);
        MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
        seg.set(ValueLayout.JAVA_BYTE, bytes.length, (byte) 0);
        return seg;
    }

    private MemorySegment buildArrowReleaseStub(Arena arena) {
        try {
            java.lang.invoke.MethodHandle target =
                    java.lang.invoke.MethodHandles.lookup()
                            .findStatic(
                                    ForStRsLinker.class,
                                    "arrowNoopRelease",
                                    java.lang.invoke.MethodType.methodType(
                                            void.class, MemorySegment.class));
            return Linker.nativeLinker()
                    .upcallStub(target, FunctionDescriptor.ofVoid(ValueLayout.ADDRESS), arena);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build Arrow release stub", e);
        }
    }

    @SuppressWarnings("unused")
    private static void arrowNoopRelease(MemorySegment self) {
        if (self.address() != 0L) {
            MemorySegment view = self.reinterpret(ARROW_ARRAY_BYTES);
            ARROW_ARRAY_RELEASE.set(view, 0L, MemorySegment.NULL);
        }
    }

    /** Deletes {@code key} from the column family. No-op if absent. */
    public void delete(FrsDb db, FrsCfHandle cf, byte[] key) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg = copyBytesToNative(local, key);
            int rc;
            try {
                rc =
                        (int)
                                frsDelete.invokeExact(
                                        db.handle(), cf.handle(), keySeg, (long) key.length);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_delete threw: " + t.getMessage());
            }
            check(rc, "frs_delete");
        }
    }

    // ------------------------------------------------------------------
    // 4. Flush / checkpoint / metadata
    // ------------------------------------------------------------------

    /** Flushes all pending memtables to L0. */
    public void flush(FrsDb db) {
        int rc;
        try {
            rc = (int) frsFlush.invokeExact(db.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(FrsStatus.PANIC, "frs_flush threw: " + t.getMessage());
        }
        check(rc, "frs_flush");
    }

    /** Creates a checkpoint at {@code targetDir} (must not yet exist). */
    public void createCheckpoint(FrsDb db, String targetDir) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment dirSeg = allocateCString(local, targetDir);
            int rc;
            try {
                rc = (int) frsCreateCheckpoint.invokeExact(db.handle(), dirSeg);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_create_checkpoint threw: " + t.getMessage());
            }
            check(rc, "frs_create_checkpoint");
        }
    }

    /**
     * FRS-CKPT-NOFLUSH (2026-06-01): serialise every CF's LIVE memtables to
     * per-CF Arrow-IPC artifacts under {@code dir} WITHOUT flushing them to L0
     * SSTs (the memtable stays RAM-resident + writable). Returns the number of
     * artifacts written. The snapshot strategy includes these as private-state
     * files in the incremental keyed-state handle.
     */
    public long snapshotMemtablesToDir(FrsDb db, FrsSnapshot snapshot, String dir) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment dirSeg = allocateCString(local, dir);
            MemorySegment outCount = local.allocate(ValueLayout.JAVA_LONG);
            int rc;
            try {
                rc =
                        (int)
                                frsSnapshotMemtablesToDir.invokeExact(
                                        db.handle(), snapshot.handle(), dirSeg, outCount);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC,
                        "frs_snapshot_memtables_to_dir threw: " + t.getMessage());
            }
            check(rc, "frs_snapshot_memtables_to_dir");
            return outCount.get(ValueLayout.JAVA_LONG, 0);
        }
    }

    /**
     * FRS-CKPT-NOFLUSH: restore counterpart — replay every memtable artifact
     * found under {@code dir} into its CF (preserving sequence + op_type). Call
     * AFTER the engine has opened the checkpoint's SST set. Returns total rows
     * replayed.
     */
    public long replayMemtableArtifacts(FrsDb db, String dir) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment dirSeg = allocateCString(local, dir);
            MemorySegment outRows = local.allocate(ValueLayout.JAVA_LONG);
            int rc;
            try {
                rc =
                        (int)
                                frsReplayMemtableArtifacts.invokeExact(
                                        db.handle(), dirSeg, outRows);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC,
                        "frs_replay_memtable_artifacts threw: " + t.getMessage());
            }
            check(rc, "frs_replay_memtable_artifacts");
            return outRows.get(ValueLayout.JAVA_LONG, 0);
        }
    }

    /** Returns the current global sequence number. */
    public long sequenceNumber(FrsDb db) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment outSeq = local.allocate(ValueLayout.JAVA_LONG);
            int rc;
            try {
                rc = (int) frsSequenceNumber.invokeExact(db.handle(), outSeq);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_sequence_number threw: " + t.getMessage());
            }
            check(rc, "frs_sequence_number");
            return outSeq.get(ValueLayout.JAVA_LONG, 0);
        }
    }

    // ------------------------------------------------------------------
    // 5. Delta-Join lookup + iterator
    // ------------------------------------------------------------------

    /**
     * Single-key exact-match lookup, semantically equivalent to {@link #get} but routed through the
     * dedicated Delta-Join path.
     */
    public byte[] lookupKv(FrsDb db, FrsCfHandle cf, byte[] key) {
        return getInternal(frsLookupKv, "frs_lookup_kv", db, cf, key);
    }

    private static final int GET_INTO_BUF_CAP = 4096;
    public byte[] getIntoBuf(FrsDb db, FrsCfHandle cf, byte[] key) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg = copyBytesToNative(local, key);
            MemorySegment outBufSeg = local.allocate(GET_INTO_BUF_CAP);
            MemorySegment lenSeg = local.allocate(Long.BYTES);
            int rc;
            try {
                rc =
                        (int)
                                frsGetIntoBuf.invokeExact(
                                        db.handle(),
                                        cf.handle(),
                                        keySeg,
                                        (long) key.length,
                                        outBufSeg,
                                        (long) GET_INTO_BUF_CAP,
                                        lenSeg);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_get_into_buf threw: " + t.getMessage());
            }
            if (rc == 17) { // FRS_STATUS_BUFFER_TOO_SMALL
                return lookupKv(db, cf, key);
            }
            check(rc, "frs_get_into_buf");
            long valLen = lenSeg.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            if (valLen == 0) {
                return null;
            }
            byte[] result = new byte[(int) valLen];
            MemorySegment.copy(outBufSeg, ValueLayout.JAVA_BYTE, 0, result, 0, (int) valLen);
            return result;
        }
    }

    /**
     * Fast-path equivalent of {@link #getIntoBuf}: same wire shape, but the Rust side skips {@code
     * catch_unwind} + {@code Arc::clone}, saving ~1.5µs per call. Safe when the caller guarantees
     * the {@code db} handle outlives the call — which is true for the lifetime of a Flink
     * TaskExecutor (the backend holds the db open until close).
     */
    public byte[] getFast(FrsDb db, FrsCfHandle cf, byte[] key) {
        if (OPCOUNT) {

            oc(0);

        }
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg = copyBytesToNative(local, key);
            MemorySegment outBufSeg = local.allocate(GET_INTO_BUF_CAP);
            MemorySegment lenSeg = local.allocate(Long.BYTES);
            int rc;
            try {
                rc =
                        (int)
                                frsGetFast.invokeExact(
                                        db.handle(),
                                        cf.handle(),
                                        keySeg,
                                        (long) key.length,
                                        outBufSeg,
                                        (long) GET_INTO_BUF_CAP,
                                        lenSeg);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_get_fast threw: " + t.getMessage());
            }
            if (rc == 17) { // FRS_STATUS_BUFFER_TOO_SMALL → fall back to general get
                return get(db, cf, key);
            }
            if (rc != 0) {
                // Caller-side fallback (no exception): get_fast trades robustness for speed.
                return get(db, cf, key);
            }
            long valLen = lenSeg.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            if (valLen == 0) {
                return null;
            }
            byte[] result = new byte[(int) valLen];
            MemorySegment.copy(outBufSeg, ValueLayout.JAVA_BYTE, 0, result, 0, (int) valLen);
            return result;
        }
    }

    /** Opens a forward iterator over the entire column family. */
    public FrsIterator iteratorOpen(FrsDb db, FrsCfHandle cf, Arena arena) {
        MemorySegment outIter = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsIteratorOpen.invokeExact(db.handle(), cf.handle(), outIter);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_iterator_open threw: " + t.getMessage());
        }
        check(rc, "frs_iterator_open");
        MemorySegment handle = outIter.get(ValueLayout.ADDRESS, 0);
        return new FrsIterator(this, handle, false);
    }

    /**
     * Opens a forward iterator bounded to keys with {@code prefix}. Returns an iterator backed by
     * {@code frs_prefix_lookup_close} on {@link FrsIterator#close()}.
     */
    public FrsIterator prefixLookupOpen(FrsDb db, FrsCfHandle cf, byte[] prefix, Arena arena) {
        if (OPCOUNT) {

            oc(2);

        }
        MemorySegment outIter = arena.allocate(ValueLayout.ADDRESS);
        try (Arena local = Arena.ofConfined()) {
            MemorySegment prefixSeg;
            long prefixLen;
            if (prefix == null || prefix.length == 0) {
                prefixSeg = MemorySegment.NULL;
                prefixLen = 0L;
            } else {
                prefixSeg = local.allocate(prefix.length);
                MemorySegment.copy(prefix, 0, prefixSeg, ValueLayout.JAVA_BYTE, 0, prefix.length);
                prefixLen = prefix.length;
            }
            int rc;
            try {
                rc =
                        (int)
                                frsPrefixLookupOpen.invokeExact(
                                        db.handle(), cf.handle(), prefixSeg, prefixLen, outIter);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_prefix_lookup_open threw: " + t.getMessage());
            }
            check(rc, "frs_prefix_lookup_open");
        }
        MemorySegment handle = outIter.get(ValueLayout.ADDRESS, 0);
        return new FrsIterator(this, handle, true);
    }

    // R0C-NEW-H1: `prefixGetAll(byte[])` and `batchPrefixScan(byte[][])` removed —
    // superseded by `frs_vec_iter_prefix_open` + `frs_vec_iter_prefix_next` (Arrow
    // chunked iterator) under the byte[]/byte[][] ban. Zero live or test callers
    // when removed. The IteratorEntry-returning shape allocated per-row `byte[]`
    // pairs which defeated end-to-end Arrow throughput.

    // R0C-NEW-H1 Tier-1: legacy `iteratorSeek(byte[])` removed under the byte[]/byte[][]
    // ban. Iterator-based prefix scans go through `iteratorOpenAt` / `vecIterPrefixOpen`
    // which expose segment-shaped key positioning at iterator construction time.

    /**
     * Advances {@code iter} and returns the next entry, or {@code null} when the iterator is
     * exhausted. Each returned key/value is heap-owned by Rust; this method copies the bytes into
     * Java heap and frees the native buffers via {@code frs_bytes_free}.
     */
    public IteratorEntry iteratorNext(FrsIterator iter) {
        if (OPCOUNT) {

            oc(3);

        }
        // D-R3-H4: reuse the iterator's pre-allocated scratch segments
        // instead of opening a fresh Arena.ofConfined() per call. Pre-fix
        // a 1000-row scan paid 1000 arena lifecycles + 3000 native allocs;
        // now those allocations happen ONCE at iter open and amortize
        // across the entire scan.
        MemorySegment outKey = iter.scratchKey;
        MemorySegment outValue = iter.scratchValue;
        MemorySegment outValid = iter.scratchValid;
        int rc;
        try {
            rc = (int) frsIteratorNext.invokeExact(iter.handle(), outKey, outValue, outValid);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_iterator_next threw: " + t.getMessage());
        }
        check(rc, "frs_iterator_next");

        boolean valid = outValid.get(ValueLayout.JAVA_BOOLEAN, 0);
        if (!valid) {
            // Rust set both FrsBytes to NULL in this case — nothing to free.
            return null;
        }

        // R65-M2: drain BOTH key and value even if the key's free
        // returns non-OK or throws — pre-fix, a key-free failure
        // leaked the value's native heap. Collect failures and
        // rethrow after both calls so neither buffer leaks.
        java.util.List<Throwable> pending = new java.util.ArrayList<>();
        byte[] keyCopy = copyAndFreeCollect(outKey, "frs_iterator_next/key", pending);
        byte[] valueCopy =
                copyAndFreeCollect(outValue, "frs_iterator_next/value", pending);
        rethrowIfAny(pending);
        return new IteratorEntry(keyCopy, valueCopy);
    }

    // ------------------------------------------------------------------
    // Internal close hooks
    // ------------------------------------------------------------------

    /** Internal: invoked by {@link FrsDb#close()}. */
    void dbClose(MemorySegment handle) {
        try {
            int rc = (int) frsDbClose.invokeExact(handle);
            check(rc, "frs_db_close");
        } catch (Throwable t) {
            if (t instanceof FrsBackendException fbe) {
                throw fbe;
            }
            throw new FrsBackendException(FrsStatus.PANIC, "frs_db_close threw: " + t.getMessage());
        }
    }

    /** Internal: invoked by {@link FrsCfHandle#close()}. */
    void cfClose(MemorySegment handle) {
        try {
            int rc = (int) frsCfClose.invokeExact(handle);
            check(rc, "frs_cf_close");
        } catch (Throwable t) {
            if (t instanceof FrsBackendException fbe) {
                throw fbe;
            }
            throw new FrsBackendException(FrsStatus.PANIC, "frs_cf_close threw: " + t.getMessage());
        }
    }

    /** Internal: invoked by {@link FrsIterator#close()}. */
    void iteratorClose(MemorySegment handle, boolean prefix) {
        MethodHandle mh = prefix ? frsPrefixLookupClose : frsIteratorClose;
        String fn = prefix ? "frs_prefix_lookup_close" : "frs_iterator_close";
        try {
            int rc = (int) mh.invokeExact(handle);
            check(rc, fn);
        } catch (Throwable t) {
            if (t instanceof FrsBackendException fbe) {
                throw fbe;
            }
            throw new FrsBackendException(FrsStatus.PANIC, fn + " threw: " + t.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * ThreadLocal pool for the 24-byte FrsBytes out-struct used by {@link #getInternal}. Avoids
     * allocating a fresh {@code byte[24]} on every point-lookup call. Thread-safe because Flink
     * task slots are single-threaded per operator chain.
     */
    private static final ThreadLocal<byte[]> FRS_BYTES_BUF =
            ThreadLocal.withInitial(() -> new byte[24]);

    /**
     * Shared Get/LookupKv helper: marshals the key, reads the FrsBytes, frees the buffer.
     *
     * <p>Plain downcalls are JVM-safepoint-friendly but cannot rely on critical-mode heap segment
     * pinning, so the key and 24-byte {@code FrsBytes} out struct are staged in a confined native
     * arena for the duration of the call.
     */
    private byte[] getInternal(MethodHandle mh, String fn, FrsDb db, FrsCfHandle cf, byte[] key) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment keySeg = copyBytesToNative(local, key);
            // FrsBytes layout: data ptr (8) + len (8) + capacity (8) = 24 bytes.
            MemorySegment outBytes = local.allocate(24);
            int rc;
            try {
                rc =
                        (int)
                                mh.invokeExact(
                                        db.handle(),
                                        cf.handle(),
                                        keySeg,
                                        (long) key.length,
                                        outBytes);
            } catch (Throwable t) {
                throw new FrsBackendException(FrsStatus.PANIC, fn + " threw: " + t.getMessage());
            }
            check(rc, fn);

            long dataAddr = ((MemorySegment) FRS_BYTES_DATA_U.get(outBytes, 0L)).address();
            long len = (long) FRS_BYTES_LEN_U.get(outBytes, 0L);
            if (dataAddr == 0L) {
                return null; // not found
            }
            return copyAndFreeRaw(outBytes, dataAddr, len, fn + "/free");
        }
    }

    private static MemorySegment copyBytesToNative(Arena arena, byte[] bytes) {
        return copyBytesRangeToNative(arena, bytes, 0, bytes.length);
    }

    private static MemorySegment copyBytesRangeToNative(
            Arena arena, byte[] bytes, int offset, int len) {
        MemorySegment dst = arena.allocate(Math.max(1L, (long) len));
        if (len > 0) {
            MemorySegment.copy(bytes, offset, dst, ValueLayout.JAVA_BYTE, 0, len);
        }
        return dst;
    }

    private static MemorySegment copySegmentToNative(
            Arena arena, MemorySegment src, long offset, long len) {
        MemorySegment dst = arena.allocate(Math.max(1L, len));
        if (len > 0) {
            MemorySegment.copy(src, offset, dst, 0L, len);
        }
        return dst;
    }

    /** Reads a non-null FrsBytes payload, copies to byte[], and frees the native buffer. */
    private byte[] copyAndFree(MemorySegment frsBytes, String fn) {
        long dataAddr = ((MemorySegment) FRS_BYTES_DATA.get(frsBytes, 0L)).address();
        long len = (long) FRS_BYTES_LEN.get(frsBytes, 0L);
        if (dataAddr == 0L) {
            // No data — nothing to free, return empty byte[].
            return new byte[0];
        }
        return copyAndFreeRaw(frsBytes, dataAddr, len, fn);
    }

    private byte[] copyAndFreeRaw(MemorySegment frsBytes, long dataAddr, long len, String fn) {
        try {
            MemorySegment dataSeg = MemorySegment.ofAddress(dataAddr).reinterpret(len);
            byte[] copy = new byte[(int) len];
            MemorySegment.copy(dataSeg, ValueLayout.JAVA_BYTE, 0, copy, 0, (int) len);
            return copy;
        } finally {
            int freeRc;
            try {
                freeRc = (int) frsBytesFree.invokeExact(frsBytes);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_bytes_free threw: " + t.getMessage());
            }
            check(freeRc, fn);
        }
    }

    /**
     * R65-H1/H2/H3 + R65-M1 drain-tolerant copy-and-free.
     *
     * <p>Reads a {@code FrsBytes} payload, copies to a fresh {@code byte[]}, and frees the native
     * buffer. Unlike {@link #copyAndFree}, this variant NEVER throws on per-entry failure —
     * pre-free copy failures (memcpy OOM, malformed addr/len, reinterpret violation) AND free
     * failures (non-OK status, panicking method handle) are appended to {@code pending} so the
     * caller's loop continues draining the rest of the batch. The free is attempted even on
     * pre-free failure, so no entry's native heap is leaked. Returns {@code null} on copy
     * failure, empty {@code byte[0]} on {@code dataAddr == 0} (no data), or the copy on success.
     *
     * <p>The caller drives a loop, accumulates failures, then throws after iteration:
     *
     * <pre>{@code
     * List<Throwable> pending = new ArrayList<>();
     * byte[][] results = new byte[count][];
     * for (int i = 0; i < count; i++) {
     *     results[i] = copyAndFreeCollect(entry(i), "fn", pending);
     * }
     * rethrowIfAny(pending);
     * }</pre>
     */
    private byte[] copyAndFreeCollect(
            MemorySegment frsBytes, String fn, java.util.List<Throwable> pending) {
        long dataAddr = ((MemorySegment) FRS_BYTES_DATA.get(frsBytes, 0L)).address();
        long len = (long) FRS_BYTES_LEN.get(frsBytes, 0L);
        if (dataAddr == 0L) {
            return new byte[0];
        }
        byte[] copy = null;
        try {
            MemorySegment dataSeg = MemorySegment.ofAddress(dataAddr).reinterpret(len);
            copy = new byte[(int) len];
            MemorySegment.copy(dataSeg, ValueLayout.JAVA_BYTE, 0, copy, 0, (int) len);
        } catch (Throwable t) {
            pending.add(
                    new FrsBackendException(
                            FrsStatus.PANIC, fn + "/copy threw: " + t.getMessage()));
            // Fall through to free — we still own the native buffer.
        }
        try {
            int freeRc = (int) frsBytesFree.invokeExact(frsBytes);
            if (freeRc != FrsStatus.OK.code()) {
                FrsStatus s;
                try {
                    s = FrsStatus.fromCode(freeRc);
                } catch (Throwable ignored) {
                    s = FrsStatus.PANIC;
                }
                pending.add(new FrsBackendException(s, fn + "/free: status=" + freeRc));
            }
        } catch (Throwable t) {
            pending.add(
                    new FrsBackendException(
                            FrsStatus.PANIC, fn + "/free threw: " + t.getMessage()));
        }
        return copy;
    }

    // FRS-OPCOUNT (2026-06-07): universal op counter at the linker chokepoint (captures BOTH
    // V1-sync and V2-async paths, since both funnel through ForStRsLinker). -Dforst.rs.opcount=1.
    // Measures engine ops/record for the backend-layer gap investigation (q9/q19/q20). Kinds:
    // 0=read 1=write/delete 2=iter-open 3=iter-next.
    static final boolean OPCOUNT = "1".equals(System.getProperty("forst.rs.opcount"));
    private static final java.util.concurrent.atomic.AtomicLong OC_READ =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong OC_WRITE =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong OC_ITEROPEN =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong OC_ITERNEXT =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong OC_NEXT_DUMP =
            new java.util.concurrent.atomic.AtomicLong(50_000L);
    private static final long OC_DUMP_STEP = 2_000_000L;

    static void oc(int kind) {
        switch (kind) {
            case 0 -> OC_READ.incrementAndGet();
            case 1 -> OC_WRITE.incrementAndGet();
            case 2 -> OC_ITEROPEN.incrementAndGet();
            default -> OC_ITERNEXT.incrementAndGet();
        }
        long total = OC_READ.get() + OC_WRITE.get() + OC_ITEROPEN.get() + OC_ITERNEXT.get();
        long threshold = OC_NEXT_DUMP.get();
        if (total >= threshold && OC_NEXT_DUMP.compareAndSet(threshold, threshold + OC_DUMP_STEP)) {
            System.err.println(
                    "[FRS_OPCOUNT] reads=" + OC_READ.get()
                            + " writes=" + OC_WRITE.get()
                            + " iterOpens=" + OC_ITEROPEN.get()
                            + " iterNexts=" + OC_ITERNEXT.get());
        }
    }

    /** Rethrows the first failure with the rest attached as suppressed. */
    private static void rethrowIfAny(java.util.List<Throwable> pending) {
        if (pending.isEmpty()) {
            return;
        }
        Throwable first = pending.get(0);
        for (int i = 1; i < pending.size(); i++) {
            first.addSuppressed(pending.get(i));
        }
        if (first instanceof FrsBackendException) {
            throw (FrsBackendException) first;
        }
        throw new FrsBackendException(FrsStatus.PANIC, "drain failed: " + first.getMessage());
    }

    /** Allocates a NUL-terminated UTF-8 C string in {@code arena}. */
    private static MemorySegment allocateCString(Arena arena, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        MemorySegment seg = arena.allocate(bytes.length + 1L);
        MemorySegment.copy(bytes, 0, seg, ValueLayout.JAVA_BYTE, 0, bytes.length);
        seg.set(ValueLayout.JAVA_BYTE, bytes.length, (byte) 0);
        return seg;
    }

    private static void check(int rc, String fn) {
        if (rc != FrsStatus.OK.code()) {
            throw new FrsBackendException(FrsStatus.fromCode(rc), fn);
        }
    }

    /** Iterator entry returned by {@link #iteratorNext(FrsIterator)}. */
    public record IteratorEntry(byte[] key, byte[] value) {}

    // ------------------------------------------------------------------
    // 7. TTL compaction filter — Flink-shaped per-CF filter
    //
    // The engine binds a per-CF FlinkTtlCompactionFilter (Disabled/Value/List)
    // that drops expired entries at flush + L0→L1 compaction time. Apply it
    // immediately after CF creation by calling
    // {@link #setCompactionFilterTtl(FrsDb, FrsCfHandle, long, int, long)}.
    // Once set, the filter cannot be removed without dropping/recreating the CF.
    //
    // The JNI compat shim (libforstjni surface) carries
    // {@code Java_org_forstdb_FlinkCompactionFilter_*} symbols that record
    // configure() calls but cannot reliably wire factory→holder→CF without
    // Java-side cooperation. Production paths must use this FFM API instead.
    // ------------------------------------------------------------------

    /** State-type ordinals; mirror {@code FlinkCompactionFilter.StateType} (and Rust). */
    public static final int STATE_TYPE_DISABLED = 0;

    public static final int STATE_TYPE_VALUE = 1;
    public static final int STATE_TYPE_LIST = 2;

    /**
     * Installs an engine-side TTL compaction filter on {@code cf}. Subsequent flushes and
     * compactions on this CF will drop entries whose recorded timestamp + {@code ttlMs} is older
     * than the current wall clock. The filter is engine-managed; releasing the CF (via {@link
     * FrsCfHandle#close()}) tears it down.
     *
     * <p>Value semantics ({@code stateType == STATE_TYPE_VALUE}): a single timestamp prefix on the
     * value bytes; if expired the entry is dropped. List semantics ({@code STATE_TYPE_LIST}): the
     * filter inspects each list element. {@code STATE_TYPE_DISABLED} is a no-op (kept for API
     * symmetry so callers can pass through whatever ordinal Flink configured without branching).
     *
     * <p>The {@code timestampOffset} parameter is the byte offset within the encoded value where
     * the 8-byte big-endian millisecond timestamp lives; pass {@code 0} unless your serializer
     * pads.
     *
     * @throws FrsBackendException if the native call returns a non-OK status (e.g. closed db or
     *     unknown {@code stateType})
     */
    public void setCompactionFilterTtl(
            FrsDb db, FrsCfHandle cf, long ttlMs, int stateType, long timestampOffset) {
        if (stateType < STATE_TYPE_DISABLED || stateType > STATE_TYPE_LIST) {
            throw new IllegalArgumentException(
                    "stateType must be 0/1/2 (Disabled/Value/List); got " + stateType);
        }
        if (ttlMs < 0L) {
            throw new IllegalArgumentException("ttlMs must be non-negative; got " + ttlMs);
        }
        if (timestampOffset < 0L) {
            throw new IllegalArgumentException(
                    "timestampOffset must be non-negative; got " + timestampOffset);
        }
        int rc;
        try {
            rc =
                    (int)
                            frsCfSetCompactionFilterTtl.invokeExact(
                                    db.handle(), cf.handle(), ttlMs, stateType, timestampOffset);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_cf_set_compaction_filter_ttl threw: " + t.getMessage());
        }
        check(rc, "frs_cf_set_compaction_filter_ttl");
    }

    // ------------------------------------------------------------------
    // 8. MVCC snapshot + versioned reads + incremental checkpoint (B-Prod-P2)
    //
    // These map onto the engine surface added in B-Prod-P2 Tasks 2.1-2.4 and
    // give Flink's snapshot strategy the FFM API it needs:
    //
    //   * dbSnapshot / dbReleaseSnapshot — capture / release a snapshot
    //     pinned at the current sequence number (per spec §10.0 ABI lifetime
    //     contract).
    //   * getAt — versioned point-lookup at the snapshot.
    //   * iteratorOpenAt — versioned forward iterator at the snapshot.
    //   * createIncrementalCheckpointAt + dbOpenFromIncremental — capture +
    //     restore an incremental checkpoint pinned at the snapshot.
    // ------------------------------------------------------------------

    /**
     * Captures a snapshot at the engine's current sequence number. Caller MUST close the returned
     * {@link FrsSnapshot} (try-with-resources is the canonical pattern) so the engine's compaction
     * can advance past the snapshot's seq.
     */
    public FrsSnapshot dbSnapshot(FrsDb db, Arena arena) {
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc = (int) frsDbSnapshot.invokeExact(db.handle(), outHandle);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_snapshot threw: " + t.getMessage());
        }
        check(rc, "frs_db_snapshot");
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsSnapshot(this, db, handle);
    }

    /**
     * Internal: invoked by {@link FrsSnapshot#close()}. Public callers should close the snapshot
     * directly; this entry point exists so {@code FrsSnapshot} can route the close through the
     * linker without exposing the method handle.
     *
     * <p>Idempotency is enforced by {@code FrsSnapshot} on the Java side; this method assumes the
     * snapshot is non-null and not yet closed (the wrapper checks before invoking).
     */
    void dbReleaseSnapshot(FrsDb db, FrsSnapshot snapshot) {
        if (snapshot == null || snapshot.isClosed()) {
            return;
        }
        int rc;
        try {
            rc = (int) frsDbReleaseSnapshot.invokeExact(db.handle(), snapshot.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_release_snapshot threw: " + t.getMessage());
        }
        check(rc, "frs_db_release_snapshot");
    }

    // R0C-NEW-H1 Tier-1: legacy `getAt(byte[]) → byte[]` removed under the byte[]/
    // byte[][] ban. Snapshot point-lookups now go via `iteratorOpenAt` + segment-
    // shaped iteration, which exposes the same MVCC semantics with off-heap output.

    /**
     * Opens a forward iterator that yields the latest version of each user-key with {@code seq
     * &lt;= snapshot.seq}. Drive it with {@link #iteratorNext(FrsIterator)} and release with {@link
     * FrsIterator#close()} (uses the standard non-prefix close path).
     */
    public FrsIterator iteratorOpenAt(FrsDb db, FrsCfHandle cf, FrsSnapshot snapshot, Arena arena) {
        MemorySegment outIter = arena.allocate(ValueLayout.ADDRESS);
        int rc;
        try {
            rc =
                    (int)
                            frsIteratorOpenAt.invokeExact(
                                    db.handle(), cf.handle(), snapshot.handle(), outIter);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_iterator_open_at threw: " + t.getMessage());
        }
        check(rc, "frs_iterator_open_at");
        MemorySegment handle = outIter.get(ValueLayout.ADDRESS, 0);
        return new FrsIterator(this, handle, false);
    }

    /**
     * Captures an incremental checkpoint at {@code snapshot}, returning the manifest path + new vs
     * shared SST file lists. Caller MUST eventually call {@link
     * #dbIncrementalCheckpointResultFree(MemorySegment)} on {@code resultPtr} to reclaim the inner
     * allocations (manifest_path C string + the two FrsLiveFileList boxes).
     *
     * <p>{@code resultPtr} must point to a 32-byte caller-allocated buffer matching the C layout:
     *
     * <pre>
     * struct FrsIncrementalCheckpointResult {
     *     char*               manifest_path;        // 8
     *     FrsLiveFileList*    new_ssts;             // 8
     *     FrsLiveFileList*    shared_ssts;          // 8
     *     int                 flush_done_eventfd;   // 4 + 4 padding
     * };
     * </pre>
     */
    public void createIncrementalCheckpointAt(
            FrsDb db,
            FrsSnapshot snapshot,
            long checkpointId,
            long baseCheckpointId,
            MemorySegment resultPtr) {
        int rc;
        try {
            rc =
                    (int)
                            frsCreateIncrementalCheckpointAt.invokeExact(
                                    db.handle(),
                                    snapshot.handle(),
                                    checkpointId,
                                    baseCheckpointId,
                                    resultPtr);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC,
                    "frs_create_incremental_checkpoint_at threw: " + t.getMessage());
        }
        check(rc, "frs_create_incremental_checkpoint_at");
    }

    /**
     * FRS-CKPT-NOFLUSH: incremental checkpoint that enumerates only the
     * already-flushed SST set WITHOUT flushing the memtable. The caller captures
     * the live memtable via {@link #snapshotMemtablesToDir} and uploads those
     * artifacts as private checkpoint state. Same result-struct contract as
     * {@link #createIncrementalCheckpointAt}.
     */
    public void createIncrementalCheckpointAtNoflush(
            FrsDb db,
            FrsSnapshot snapshot,
            long checkpointId,
            long baseCheckpointId,
            MemorySegment resultPtr) {
        int rc;
        try {
            rc =
                    (int)
                            frsCreateIncrementalCheckpointAtNoflush.invokeExact(
                                    db.handle(),
                                    snapshot.handle(),
                                    checkpointId,
                                    baseCheckpointId,
                                    resultPtr);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC,
                    "frs_create_incremental_checkpoint_at_noflush threw: " + t.getMessage());
        }
        check(rc, "frs_create_incremental_checkpoint_at_noflush");
    }

    /**
     * Releases the inner allocations of an {@link FrsIncrementalCheckpointResult}-shaped struct
     * previously populated by {@link #createIncrementalCheckpointAt}. Idempotent on the native
     * side; no-op for a NULL pointer.
     */
    public void dbIncrementalCheckpointResultFree(MemorySegment resultPtr) {
        int rc;
        try {
            rc = (int) frsDbIncrementalCheckpointResultFree.invokeExact(resultPtr);
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC,
                    "frs_db_incremental_checkpoint_result_free threw: " + t.getMessage());
        }
        check(rc, "frs_db_incremental_checkpoint_result_free");
    }

    /**
     * Opens a fresh DB whose state is reconstructed from a manifest blob and a list of SST file
     * paths. The native side hardlinks (or copies) each {@code sstFiles} entry into {@code
     * targetDir}, then opens the DB from the persisted manifest. Caller closes via {@link
     * FrsDb#close()}.
     */
    public FrsDb dbOpenFromIncremental(
            Arena arena, String targetDir, String baseManifest, java.util.List<String> sstFiles) {
        MemorySegment targetSeg = allocateCString(arena, targetDir);
        MemorySegment manifestSeg = allocateCString(arena, baseManifest);
        MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
        long count = sstFiles.size();
        try (Arena local = Arena.ofConfined()) {
            // Allocate `count` pointer slots and a per-path C string in `local`.
            MemorySegment paths = local.allocate(count * ValueLayout.ADDRESS.byteSize());
            for (int i = 0; i < count; i++) {
                MemorySegment p = allocateCString(local, sstFiles.get(i));
                paths.set(ValueLayout.ADDRESS, (long) i * ValueLayout.ADDRESS.byteSize(), p);
            }
            int rc;
            try {
                rc =
                        (int)
                                frsDbOpenFromIncremental.invokeExact(
                                        targetSeg, manifestSeg, paths, count, outHandle);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_db_open_from_incremental threw: " + t.getMessage());
            }
            check(rc, "frs_db_open_from_incremental");
        }
        MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
        return new FrsDb(this, handle);
    }

    // ------------------------------------------------------------------
    // 9. State import / export migration (B-Prod-P10, spec §6g)
    //
    // Cross-job state transfer: dump every (key, value) row in a CF to
    // a self-describing blob under `exportDir`, ship the directory to
    // another job, and reconstitute it as a brand-new CF on the consumer
    // side. The native blob format and atomicity contract live in
    // crates/forst-rs-engine/src/db.rs (search for FRSEXP01 / cf_export
    // / create_cf_from_import).
    //
    // The Java surface is tiny on purpose: two methods that thread
    // strings through the standard `allocateCString` helper and box the
    // returned CF handle. Higher-level orchestration (e.g. shipping the
    // export directory across nodes) lives in
    // {@code org.apache.flink.state.forstrs.migration.ForStRsStateMigration}.
    // ------------------------------------------------------------------

    /**
     * Exports every live (key, value) row from {@code cf} to a single self-describing blob ({@code
     * EXPORT.frsblob}) under {@code exportDir}. The directory is created if it does not exist.
     * Concurrent writers to {@code cf} are not blocked, but only writes committed before this call
     * took the underlying scan are guaranteed to appear in the export (snapshot-at-call-time
     * semantics, mirroring RocksDB's {@code ExportColumnFamily}).
     *
     * @throws FrsBackendException if the native call returns a non-OK status
     */
    public void cfExport(FrsDb db, FrsCfHandle cf, String exportDir) {
        try (Arena local = Arena.ofConfined()) {
            MemorySegment dirSeg = allocateCString(local, exportDir);
            int rc;
            try {
                rc = (int) frsCfExport.invokeExact(db.handle(), cf.handle(), dirSeg);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_cf_export threw: " + t.getMessage());
            }
            check(rc, "frs_cf_export");
        }
    }

    /**
     * Creates a brand-new column family with name {@code name} and seeds it with every row from
     * {@code importDir/EXPORT.frsblob}. Returns a {@link FrsCfHandle} the caller MUST close.
     *
     * <p>The original CF name embedded in the blob is intentionally ignored — Flink callers
     * commonly re-namespace state when migrating jobs, and the explicit-name signature mirrors
     * RocksDB's {@code ImportColumnFamily(new_name, metadata)}. Importing under a name that already
     * exists in {@code db} returns {@link FrsStatus#INVALID_ARGUMENT}.
     *
     * @throws FrsBackendException if the blob is missing, the magic header doesn't match, the CF
     *     name is already in use, or the underlying replay puts fail
     */
    /**
     * Drops a column family (B-Prod-followup-5, spec §6g).
     *
     * <p>Removes the CF from the engine's CF maps and flips its shared {@code dropped} flag.
     * Subsequent native operations on the {@link FrsCfHandle} return {@code INVALID_ARGUMENT}.
     * Idempotent on an already-dropped CF (returns OK); rejects the default CF.
     *
     * <p>Note: this method does NOT close the Java {@link FrsCfHandle}. The caller is still
     * responsible for {@code cf.close()} to free the FFM allocation.
     *
     * @throws FrsBackendException if the native call returns a non-OK status (default CF dropped,
     *     unknown handle, …)
     */
    public void dbDropCf(FrsDb db, FrsCfHandle cf) {
        int rc;
        try {
            rc = (int) frsDbDropCf.invokeExact(db.handle(), cf.handle());
        } catch (Throwable t) {
            throw new FrsBackendException(
                    FrsStatus.PANIC, "frs_db_drop_cf threw: " + t.getMessage());
        }
        check(rc, "frs_db_drop_cf");
    }

    /**
     * Ingests pre-built SST files into the engine's L0 (B-Prod-followup-5, spec §6g).
     *
     * <p>Each path is hardlinked (or copied cross-FS) into the engine's SST directory, then
     * registered at L0 via a single atomic version edit. See {@code
     * crates/forst-rs-engine/src/db.rs::ingest_external_sst} for the caller contract (source-SST
     * compatibility, key-range overlap rules, CF visibility caveats).
     *
     * <p>An empty {@code sstPaths} array is a no-op (returns OK).
     *
     * @throws FrsBackendException if any hardlink+copy fails, the SST cannot be parsed, or the
     *     engine rejects the CF handle.
     */
    public void dbIngestExternalSst(FrsDb db, FrsCfHandle cf, java.util.List<String> sstPaths) {
        if (sstPaths == null || sstPaths.isEmpty()) {
            // Engine treats empty input as a no-op; mirror that here so
            // callers don't have to special-case.
            return;
        }
        try (Arena local = Arena.ofConfined()) {
            int n = sstPaths.size();
            // Allocate n C-string slots + an n-slot pointer array.
            MemorySegment pathsArray = local.allocate(ValueLayout.ADDRESS, n);
            for (int i = 0; i < n; i++) {
                MemorySegment cstr = allocateCString(local, sstPaths.get(i));
                pathsArray.setAtIndex(ValueLayout.ADDRESS, i, cstr);
            }
            int rc;
            try {
                rc =
                        (int)
                                frsDbIngestExternalSst.invokeExact(
                                        db.handle(), cf.handle(), pathsArray, (long) n);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_db_ingest_external_sst threw: " + t.getMessage());
            }
            check(rc, "frs_db_ingest_external_sst");
        }
    }

    public FrsCfHandle dbCreateCfFromImport(FrsDb db, Arena arena, String name, String importDir) {
        MemorySegment outCf = arena.allocate(ValueLayout.ADDRESS);
        try (Arena local = Arena.ofConfined()) {
            MemorySegment nameSeg = allocateCString(local, name);
            MemorySegment dirSeg = allocateCString(local, importDir);
            int rc;
            try {
                rc = (int) frsDbCreateCfFromImport.invokeExact(db.handle(), nameSeg, dirSeg, outCf);
            } catch (Throwable t) {
                throw new FrsBackendException(
                        FrsStatus.PANIC, "frs_db_create_cf_from_import threw: " + t.getMessage());
            }
            check(rc, "frs_db_create_cf_from_import");
        }
        MemorySegment cfHandle = outCf.get(ValueLayout.ADDRESS, 0);
        return new FrsCfHandle(this, cfHandle);
    }

    // 10. Vectorized chunked iterator (P3-A/P3-B, spec §1 §b + §2 component E)

    /** Opens a prefix-scan iterator and fills the first chunk. Returns native error code. */
    public int frsVecIterPrefixOpen(
            MemorySegment db,
            MemorySegment cf,
            MemorySegment prefix,
            int prefixLen,
            MemorySegment chunkBuf,
            int chunkBufCap,
            MemorySegment outHandle,
            MemorySegment outRowCount,
            MemorySegment outBytesUsed) {
        if (OPCOUNT) {

            oc(2);

        }
        try {
            return (int)
                    frsVecIterPrefixOpen.invokeExact(
                            db,
                            cf,
                            prefix,
                            prefixLen,
                            chunkBuf,
                            chunkBufCap,
                            outHandle,
                            outRowCount,
                            outBytesUsed);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_iter_prefix_open failed", t);
        }
    }

    /** Pulls the next chunk from an open iterator. Returns native error code. */
    public int frsVecIterPrefixNext(
            long handle,
            MemorySegment chunkBuf,
            int chunkBufCap,
            MemorySegment outRowCount,
            MemorySegment outBytesUsed) {
        if (OPCOUNT) {

            oc(3);

        }
        try {
            return (int)
                    frsVecIterPrefixNext.invokeExact(
                            handle, chunkBuf, chunkBufCap, outRowCount, outBytesUsed);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_iter_prefix_next failed", t);
        }
    }

    /** Releases the native iterator handle. Returns native error code. */
    public int frsVecIterPrefixClose(long handle) {
        try {
            return (int) frsVecIterPrefixClose.invokeExact(handle);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_iter_prefix_close failed", t);
        }
    }

    /**
     * Watchdog hook: marks handle as aborted so subsequent next() returns empty. Returns 201 if
     * unknown.
     */
    public int frsVecIterPrefixAbort(long handle) {
        try {
            return (int) frsVecIterPrefixAbort.invokeExact(handle);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_iter_prefix_abort failed", t);
        }
    }

    /**
     * PR-E3 / E-HIGH-5 / F5-4: opens N prefix iterators in a single FFI crossing.
     *
     * <p>Replaces N separate {@link #frsVecIterPrefixOpen} calls (each previously requiring its own
     * {@code Arena.ofShared()} for out-params) with one batched call. The caller pre-packs all N
     * prefixes into an SoA layout (offsets array + flat data buffer) and supplies one output array
     * for handles and one for per-iter chunk descriptors. The chunk capacity is uniform across all
     * iters in the batch (validated against each {@link #FRS_CHUNK_LAYOUT} {@code buf_cap} field).
     *
     * @param db engine handle
     * @param cf column-family handle
     * @param prefixesOff packed offsets, length n+1 (the n-th entry is the total prefix bytes)
     * @param prefixesData flat concatenated prefix bytes
     * @param n number of iters to open
     * @param outHandles array of {@code n} u64 handle slots (engine writes 0 on per-row failure)
     * @param outFirstChunks array of {@code n} {@link #FRS_CHUNK_LAYOUT} structs; caller fills {@code
     *     buf_ptr} + {@code buf_cap}, engine fills {@code row_count} + {@code bytes_used}
     * @param chunkCap uniform per-iter capacity (each chunk's {@code buf_cap} MUST match)
     * @return native error code (0 = all opens succeeded; non-zero = first per-row error)
     */
    public int frsVecIterPrefixOpenBatch(
            MemorySegment db,
            MemorySegment cf,
            MemorySegment prefixesOff,
            MemorySegment prefixesData,
            int n,
            MemorySegment outHandles,
            MemorySegment outFirstChunks,
            int chunkCap) {
        try {
            return (int)
                    frsVecIterPrefixOpenBatch.invokeExact(
                            db,
                            cf,
                            prefixesOff,
                            prefixesData,
                            n,
                            outHandles,
                            outFirstChunks,
                            chunkCap);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_iter_prefix_open_batch failed", t);
        }
    }

    /**
     * PARALLEL batched prefix-iterator open (the join read-path lever). Identical ABI + contract to
     * {@link #frsVecIterPrefixOpenBatch}, but the engine builds + drains the K probes concurrently
     * across its read pool. Drop-in replacement on the batched-open path.
     */
    public int frsVecIterPrefixOpenBatchParallel(
            MemorySegment db,
            MemorySegment cf,
            MemorySegment prefixesOff,
            MemorySegment prefixesData,
            int n,
            MemorySegment outHandles,
            MemorySegment outFirstChunks,
            int chunkCap) {
        try {
            return (int)
                    frsVecIterPrefixOpenBatchParallel.invokeExact(
                            db,
                            cf,
                            prefixesOff,
                            prefixesData,
                            n,
                            outHandles,
                            outFirstChunks,
                            chunkCap);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_iter_prefix_open_batch_parallel failed", t);
        }
    }

    /** Per-iter FrsChunk struct byte size — used by callers to allocate AoS output arrays. */
    public static long frsChunkLayoutByteSize() {
        return FRS_CHUNK_LAYOUT.byteSize();
    }

    /** Per-iter FrsChunk write: set the caller-owned chunk buffer pointer for row {@code i}. */
    public static void setFrsChunkBufPtr(MemorySegment chunks, int i, MemorySegment bufPtr) {
        FRS_CHUNK_BUF_PTR.set(chunks, (long) i * FRS_CHUNK_LAYOUT.byteSize(), bufPtr);
    }

    /** Per-iter FrsChunk write: set the per-row chunk capacity. */
    public static void setFrsChunkBufCap(MemorySegment chunks, int i, int cap) {
        FRS_CHUNK_BUF_CAP.set(chunks, (long) i * FRS_CHUNK_LAYOUT.byteSize(), cap);
    }

    /** Per-iter FrsChunk read: row_count written by the engine. */
    public static int getFrsChunkRowCount(MemorySegment chunks, int i) {
        return (int) FRS_CHUNK_ROW_COUNT.get(chunks, (long) i * FRS_CHUNK_LAYOUT.byteSize());
    }

    /** Per-iter FrsChunk read: bytes_used written by the engine. */
    public static int getFrsChunkBytesUsed(MemorySegment chunks, int i) {
        return (int) FRS_CHUNK_BYTES_USED.get(chunks, (long) i * FRS_CHUNK_LAYOUT.byteSize());
    }

    /**
     * P0: per-iter FrsChunk read of the engine flag word ({@code _reserved}); bit 0 = {@link
     * #FRS_CHUNK_EOF} (exhausted-at-open, iterator auto-closed — skip trailing next/close).
     */
    public static int getFrsChunkReserved(MemorySegment chunks, int i) {
        return (int) FRS_CHUNK_RESERVED.get(chunks, (long) i * FRS_CHUNK_LAYOUT.byteSize());
    }

    /** P0 convenience: true when row {@code i}'s first chunk carries {@link #FRS_CHUNK_EOF}. */
    public static boolean isFrsChunkEof(MemorySegment chunks, int i) {
        return (getFrsChunkReserved(chunks, i) & FRS_CHUNK_EOF) != 0;
    }

    /** Appends N merge operands for key (P6-B §1 §a). Returns native error code. */
    public int frsVecMergeAppend(
            MemorySegment db,
            MemorySegment cf,
            MemorySegment keyPtr,
            int keyLen,
            MemorySegment operandPtrs,
            MemorySegment operandLens,
            int numOperands) {
        try {
            return (int)
                    frsVecMergeAppend.invokeExact(
                            db, cf, keyPtr, keyLen, operandPtrs, operandLens, numOperands);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_merge_append failed", t);
        }
    }

    /**
     * Phase A.1 (audit-design §3 V4) — batched merge-append.
     *
     * <p>Consumes {@code n} (key, operand) rows in a single FFI call. Each row's operand is the
     * caller-encoded payload bytes (e.g. {@code [count][elem_bytes*]} for ListState semantics).
     * The engine writes one Merge row per input row, eliminating both the per-row FFM crossing and
     * the lost-update window of read-combine-write append paths.
     *
     * <p>Layout: {@code keys_off[i+1] - keys_off[i] = keys[i].length}; same for {@code ops_off}.
     *
     * @return native error code (0 = OK; see {@link
     *     org.apache.flink.state.forstrs.FrsErrorCode}).
     */
    public int frsVecMergeAppendBatch(
            MemorySegment db,
            MemorySegment cf,
            MemorySegment keysOff,
            MemorySegment keysData,
            MemorySegment opsOff,
            MemorySegment opsData,
            int n) {
        try {
            return (int)
                    frsVecMergeAppendBatch.invokeExact(
                            db,
                            cf,
                            keysOff,
                            keysData,
                            keysData.byteSize(),
                            opsOff,
                            opsData,
                            opsData.byteSize(),
                            n);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_merge_append_batch failed", t);
        }
    }

    /** [P9 §2-D] Opens a [lo, hi) range iterator and fills the first chunk. Returns error code. */
    public int frsVecIterRangeOpen(
            MemorySegment db,
            MemorySegment cf,
            MemorySegment lo,
            int loLen,
            MemorySegment hi,
            int hiLen,
            MemorySegment chunkBuf,
            int chunkBufCap,
            MemorySegment outHandle,
            MemorySegment outRowCount,
            MemorySegment outBytesUsed) {
        try {
            return (int)
                    frsVecIterRangeOpen.invokeExact(
                            db,
                            cf,
                            lo,
                            loLen,
                            hi,
                            hiLen,
                            chunkBuf,
                            chunkBufCap,
                            outHandle,
                            outRowCount,
                            outBytesUsed);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_iter_range_open failed", t);
        }
    }

    /** [P9] Pulls the next chunk from an open range iterator. Returns error code. */
    public int frsVecIterRangeNext(
            long handle,
            MemorySegment chunkBuf,
            int chunkBufCap,
            MemorySegment outRowCount,
            MemorySegment outBytesUsed) {
        try {
            return (int)
                    frsVecIterRangeNext.invokeExact(
                            handle, chunkBuf, chunkBufCap, outRowCount, outBytesUsed);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_iter_range_next failed", t);
        }
    }

    /** [P9] Releases a native range iterator handle. Returns error code. */
    public int frsVecIterRangeClose(long handle) {
        try {
            return (int) frsVecIterRangeClose.invokeExact(handle);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_iter_range_close failed", t);
        }
    }

    /** [P9] Watchdog: marks range handle as aborted. Returns 201 if handle unknown. */
    public int frsVecIterRangeAbort(long handle) {
        try {
            return (int) frsVecIterRangeAbort.invokeExact(handle);
        } catch (Throwable t) {
            throw new RuntimeException("frs_vec_iter_range_abort failed", t);
        }
    }
}
