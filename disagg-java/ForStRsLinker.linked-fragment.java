// ============================================================================
// FRS-PHASE2 LINK-MODE fragment for ffm/ForStRsLinker.java
//
// Paste section A (field declarations) next to frsCreateIncrementalCheckpointAt,
// section B (constructor binds) into the bind block immediately after
// frsDbIncrementalCheckpointResultFree, and section C (wrappers) after
// dbOpenFromIncremental. ABI source of truth:
// crates/forst-rs-ffi/src/lib.rs section "8c. LINK-mode checkpoint surface".
// ============================================================================

// ---------------------------------------------------------------------------
// A. Field declarations
// ---------------------------------------------------------------------------
private final MethodHandle frsCreateIncrementalCheckpointLinked;
private final MethodHandle frsDbLinkedCheckpointResultFree;
private final MethodHandle frsDbDiscardLinkedCheckpoint;
private final MethodHandle frsDbOpenFromLinkedCheckpointInstant;
private final MethodHandle frsDbOpenFromLinkedCheckpointInstantRemote;
private final MethodHandle frsDbAdoptedResidual;
private final MethodHandle frsDbAttachWal;
private final MethodHandle frsDbSweepAbandonedCheckpoints;

// ---------------------------------------------------------------------------
// B. Constructor binds
// ---------------------------------------------------------------------------
// FRS-PHASE2-S2/S3: LINK-mode checkpoint — zero data upload; the live SST
// set is link()ed into <db_path>/checkpoints/<chk-id>/ through the engine's
// FileMappingManager (auto-attached). Result struct is 24 bytes:
// {char* manifest_path; FrsLiveFileList* linked_new_ssts;
//  FrsLiveFileList* linked_shared_ssts;}.
this.frsCreateIncrementalCheckpointLinked =
        bind(
                "frs_create_incremental_checkpoint_linked",
                FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, // db
                        ValueLayout.ADDRESS, // snapshot
                        ValueLayout.JAVA_LONG, // checkpoint_id (u64)
                        ValueLayout.JAVA_LONG, // base_checkpoint_id (u64)
                        ValueLayout.ADDRESS)); // out (FrsLinkedCheckpointResult*)

this.frsDbLinkedCheckpointResultFree =
        bind(
                "frs_db_linked_checkpoint_result_free",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS));

// TM-side JM-discard delegate (design §9 D4): manifest-driven unlink loop;
// physical objects deleted exactly once at refs==0. Retried discard (blob
// already gone) returns FRS_STATUS_NOT_FOUND.
this.frsDbDiscardLinkedCheckpoint =
        bind(
                "frs_db_discard_linked_checkpoint",
                FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, // db
                        ValueLayout.JAVA_LONG, // checkpoint_id (u64)
                        ValueLayout.ADDRESS, // out_unlinked (u64*, nullable)
                        ValueLayout.ADDRESS)); // out_physicals_deleted (u64*, nullable)

// INSTANT-LINK restore (downloads/copies NOTHING; adopt + mapped reads;
// WAL.delta tail replayed when present). Local-FS variant.
this.frsDbOpenFromLinkedCheckpointInstant =
        bind(
                "frs_db_open_from_linked_checkpoint_instant",
                FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, // ckpt_dir (c_char*)
                        ValueLayout.ADDRESS, // target_dir (c_char*)
                        ValueLayout.ADDRESS)); // out_handle

// Remote-primary variant: same OpenDAL stack as frs_db_open_remote.
this.frsDbOpenFromLinkedCheckpointInstantRemote =
        bind(
                "frs_db_open_from_linked_checkpoint_instant_remote",
                FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, // uri (c_char*)
                        ValueLayout.ADDRESS, // opendal_config_json (c_char*, nullable)
                        ValueLayout.ADDRESS, // cache_dir (c_char*)
                        ValueLayout.JAVA_LONG, // cache_capacity_bytes (u64)
                        ValueLayout.ADDRESS, // ckpt_dir (c_char*)
                        ValueLayout.ADDRESS, // target_dir (c_char*)
                        ValueLayout.ADDRESS)); // out_handle

// CLAIM-discipline signal: live SSTs still resolving to foreign physicals.
this.frsDbAdoptedResidual =
        bind(
                "frs_db_adopted_residual",
                FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, // db
                        ValueLayout.ADDRESS)); // out (u64*)

// Per-DB, env-free WAL-DELTA opt-in. Call at open, BEFORE serving writes
// (the native side seals + flushes all pre-WAL memtable state first).
this.frsDbAttachWal =
        bind(
                "frs_db_attach_wal",
                FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, // db
                        ValueLayout.ADDRESS)); // wal_path (c_char*)

// Startup sweep (design §9 D5 crash window a): reaps chk-namespace links
// for checkpoint ids not in the JM-live set. Call at restore/open time.
this.frsDbSweepAbandonedCheckpoints =
        bind(
                "frs_db_sweep_abandoned_checkpoints",
                FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS, // db
                        ValueLayout.ADDRESS, // live_ids (u64*, nullable when count==0)
                        ValueLayout.JAVA_LONG, // live_count (size_t)
                        ValueLayout.ADDRESS, // out_unlinked (u64*, nullable)
                        ValueLayout.ADDRESS)); // out_physicals_deleted (u64*, nullable)

// ---------------------------------------------------------------------------
// C. Wrapper methods
// ---------------------------------------------------------------------------

/**
 * FRS-PHASE2: LINK-mode incremental checkpoint — ZERO data upload. The live
 * SST set is link()ed into {@code <db_path>/checkpoints/<chk-id>/}; the
 * checkpoint directory physically contains exactly {@code CHECKPOINT.blob}
 * (+ {@code WAL.delta} iff a WAL is attached, see {@link #dbAttachWal}).
 *
 * <p>{@code resultPtr} must point to a 24-byte caller-allocated buffer:
 *
 * <pre>
 * struct FrsLinkedCheckpointResult {
 *     char*            manifest_path;       // 8
 *     FrsLiveFileList* linked_new_ssts;     // 8
 *     FrsLiveFileList* linked_shared_ssts;  // 8
 * };
 * </pre>
 *
 * <p>The linked SST paths are chk-namespace LOGICAL paths — metadata-only;
 * register them as handles ({@code LinkedSstStateHandle}), NEVER open or
 * upload them byte-wise. Release via {@link #dbLinkedCheckpointResultFree}.
 */
public void createIncrementalCheckpointLinked(
        FrsDb db,
        FrsSnapshot snapshot,
        long checkpointId,
        long baseCheckpointId,
        MemorySegment resultPtr) {
    int rc;
    try {
        rc =
                (int)
                        frsCreateIncrementalCheckpointLinked.invokeExact(
                                db.handle(),
                                snapshot.handle(),
                                checkpointId,
                                baseCheckpointId,
                                resultPtr);
    } catch (Throwable t) {
        throw new FrsBackendException(
                FrsStatus.PANIC,
                "frs_create_incremental_checkpoint_linked threw: " + t.getMessage());
    }
    check(rc, "frs_create_incremental_checkpoint_linked");
}

/** Releases the inner allocations of an {@code FrsLinkedCheckpointResult}. Idempotent. */
public void dbLinkedCheckpointResultFree(MemorySegment resultPtr) {
    int rc;
    try {
        rc = (int) frsDbLinkedCheckpointResultFree.invokeExact(resultPtr);
    } catch (Throwable t) {
        throw new FrsBackendException(
                FrsStatus.PANIC,
                "frs_db_linked_checkpoint_result_free threw: " + t.getMessage());
    }
    check(rc, "frs_db_linked_checkpoint_result_free");
}

/**
 * TM-side discard of a LINK-mode checkpoint (the JM {@code discardState()} delegate). Returns
 * {@code true} when the checkpoint was discarded by this call, {@code false} when it was
 * already gone (retried discard / NOT_FOUND — idempotent for at-least-once notifications).
 */
public boolean dbDiscardLinkedCheckpoint(Arena arena, FrsDb db, long checkpointId) {
    MemorySegment unlinked = arena.allocate(ValueLayout.JAVA_LONG);
    MemorySegment deleted = arena.allocate(ValueLayout.JAVA_LONG);
    int rc;
    try {
        rc =
                (int)
                        frsDbDiscardLinkedCheckpoint.invokeExact(
                                db.handle(), checkpointId, unlinked, deleted);
    } catch (Throwable t) {
        throw new FrsBackendException(
                FrsStatus.PANIC,
                "frs_db_discard_linked_checkpoint threw: " + t.getMessage());
    }
    if (rc == FrsStatus.NOT_FOUND.code()) {
        return false;
    }
    check(rc, "frs_db_discard_linked_checkpoint");
    return true;
}

/**
 * INSTANT-LINK restore from a LINK-mode checkpoint directory (local engine FS) — downloads and
 * copies NOTHING. {@code targetDir} must be fresh. CLAIM discipline: keep the source
 * checkpoint retained until {@link #dbAdoptedResidual} reports 0.
 */
public FrsDb dbOpenFromLinkedCheckpointInstant(Arena arena, String ckptDir, String targetDir) {
    MemorySegment ckptSeg = allocateCString(arena, ckptDir);
    MemorySegment targetSeg = allocateCString(arena, targetDir);
    MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
    int rc;
    try {
        rc =
                (int)
                        frsDbOpenFromLinkedCheckpointInstant.invokeExact(
                                ckptSeg, targetSeg, outHandle);
    } catch (Throwable t) {
        throw new FrsBackendException(
                FrsStatus.PANIC,
                "frs_db_open_from_linked_checkpoint_instant threw: " + t.getMessage());
    }
    check(rc, "frs_db_open_from_linked_checkpoint_instant");
    return new FrsDb(this, outHandle.get(ValueLayout.ADDRESS, 0));
}

/**
 * Remote-primary INSTANT-LINK restore: builds the same
 * {@code CachedFileSystem(OpendalFileSystem, LocalCache)} stack as
 * {@code frs_db_open_remote} (uri/config/cache args identical to
 * {@code dbOpenRemote}), then restores {@code ckptDir} into {@code targetDir}
 * (both remote-namespace paths) without downloading any SST.
 */
public FrsDb dbOpenFromLinkedCheckpointInstantRemote(
        Arena arena,
        String uri,
        String opendalConfigJson,
        String cacheDir,
        long cacheCapacityBytes,
        String ckptDir,
        String targetDir) {
    MemorySegment uriSeg = allocateCString(arena, uri);
    MemorySegment cfgSeg =
            opendalConfigJson == null
                    ? MemorySegment.NULL
                    : allocateCString(arena, opendalConfigJson);
    MemorySegment cacheSeg = allocateCString(arena, cacheDir);
    MemorySegment ckptSeg = allocateCString(arena, ckptDir);
    MemorySegment targetSeg = allocateCString(arena, targetDir);
    MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
    int rc;
    try {
        rc =
                (int)
                        frsDbOpenFromLinkedCheckpointInstantRemote.invokeExact(
                                uriSeg,
                                cfgSeg,
                                cacheSeg,
                                cacheCapacityBytes,
                                ckptSeg,
                                targetSeg,
                                outHandle);
    } catch (Throwable t) {
        throw new FrsBackendException(
                FrsStatus.PANIC,
                "frs_db_open_from_linked_checkpoint_instant_remote threw: " + t.getMessage());
    }
    check(rc, "frs_db_open_from_linked_checkpoint_instant_remote");
    return new FrsDb(this, outHandle.get(ValueLayout.ADDRESS, 0));
}

/**
 * Number of live SSTs still resolving to physical objects OUTSIDE this engine's working
 * namespace (adopted at restore, not yet compacted away). 0 ⇒ weaned: the restore-source
 * checkpoint may be released (CLAIM-mode discipline).
 */
public long dbAdoptedResidual(Arena arena, FrsDb db) {
    MemorySegment out = arena.allocate(ValueLayout.JAVA_LONG);
    int rc;
    try {
        rc = (int) frsDbAdoptedResidual.invokeExact(db.handle(), out);
    } catch (Throwable t) {
        throw new FrsBackendException(
                FrsStatus.PANIC, "frs_db_adopted_residual threw: " + t.getMessage());
    }
    check(rc, "frs_db_adopted_residual");
    return out.get(ValueLayout.JAVA_LONG, 0);
}

/**
 * Startup sweep reaping ABANDONED link-mode checkpoint namespaces (crash window a: links
 * durable in the mapping journal for an id the JM never acked). {@code liveIds} is the
 * JM-live checkpoint id set; everything else under the chk namespace is unlinked and its
 * leftover dir removed (physicals survive on working/live refs). Idempotent. Call during
 * restore/open, BEFORE the first linked checkpoint. Returns the number of links reaped.
 */
public long dbSweepAbandonedCheckpoints(Arena arena, FrsDb db, long[] liveIds) {
    MemorySegment ids =
            liveIds.length == 0
                    ? MemorySegment.NULL
                    : arena.allocateFrom(ValueLayout.JAVA_LONG, liveIds);
    MemorySegment unlinked = arena.allocate(ValueLayout.JAVA_LONG);
    int rc;
    try {
        rc =
                (int)
                        frsDbSweepAbandonedCheckpoints.invokeExact(
                                db.handle(),
                                ids,
                                (long) liveIds.length,
                                unlinked,
                                MemorySegment.NULL);
    } catch (Throwable t) {
        throw new FrsBackendException(
                FrsStatus.PANIC,
                "frs_db_sweep_abandoned_checkpoints threw: " + t.getMessage());
    }
    check(rc, "frs_db_sweep_abandoned_checkpoints");
    return unlinked.get(ValueLayout.JAVA_LONG, 0);
}

/**
 * Attaches a write-ahead log (per-DB, env-free WAL-DELTA opt-in). With a WAL attached, LINK
 * checkpoints skip the memtable flush and capture the unflushed tail into
 * {@code <chk-dir>/WAL.delta}; restore replays it. Call at open, BEFORE serving writes —
 * the native side first seals + flushes all pre-WAL memtable state (so nothing is lost),
 * but a write racing the attach barrier is undefined. Place the WAL on fast LOCAL disk.
 * Throws INVALID_ARGUMENT if a WAL is already attached.
 */
public void dbAttachWal(Arena arena, FrsDb db, String walPath) {
    MemorySegment pathSeg = allocateCString(arena, walPath);
    int rc;
    try {
        rc = (int) frsDbAttachWal.invokeExact(db.handle(), pathSeg);
    } catch (Throwable t) {
        throw new FrsBackendException(
                FrsStatus.PANIC, "frs_db_attach_wal threw: " + t.getMessage());
    }
    check(rc, "frs_db_attach_wal");
}
