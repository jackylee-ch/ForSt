// ============================================================================
// FRS-PHASE2 LINK-MODE fragment for keyed/ForStRsSnapshotStrategy.java
//
// The ZERO-UPLOAD branch of asyncSnapshot (design §3.1 steps 1-5, §9 D3/D4;
// README D-J1..D-J5). Integration points:
//   (1) config plumbing — isLinkModeCheckpoint(), mirroring
//       isNoFlushCheckpoint() ("forst.rs.checkpoint.link-mode", default false);
//   (2) the branch below goes at the TOP of the existing
//       "Compute the incremental checkpoint" section in asyncSnapshot —
//       when it fires, the legacy upload path is SKIPPED entirely;
//   (3) notifyCheckpointSubsumed/Aborted — the discard delegate (bottom).
// ============================================================================

// ---------------------------------------------------------------------------
// (1) Config gate — class field + accessor, set from ForStRsOptions like
//     isNoFlushCheckpoint(). LINK mode additionally requires the sharing
//     strategy to be FORWARD/FORWARD_BACKWARD (D-J2): a NO_SHARING snapshot
//     must be self-contained and keeps the upload path.
// ---------------------------------------------------------------------------
private boolean isLinkModeCheckpoint() {
    return linkModeConfigured; // "forst.rs.checkpoint.link-mode" = true
}

// ---------------------------------------------------------------------------
// (2) asyncSnapshot branch — replaces manifest+SST upload for this checkpoint.
//     Placed AFTER `effectiveBaseCheckpointId` / `fullCheckpoint` are computed
//     and BEFORE the noFlush memtable-staging block (link mode and the
//     deprecated Arrow-artifact noflush are mutually exclusive: the engine
//     refuses to route noflush through link mode, design §9 D2/D12).
// ---------------------------------------------------------------------------
if (isLinkModeCheckpoint() && !fullCheckpoint) {
    if (isNoFlushCheckpoint()) {
        throw new IllegalStateException(
                "forst.rs.checkpoint.link-mode and forst.rs.checkpoint.noflush are "
                        + "mutually exclusive: link mode's memtable durability is dual-mode "
                        + "(FLUSH-on-barrier, or WAL-DELTA when forst.rs.wal.dir is set) and "
                        + "never uses the Arrow memtable artifact (design §9 D12).");
    }

    // ---- LINK-mode checkpoint: O(files) metadata, ZERO data upload. ----
    // The engine: awaits the pinned live set's uploads (freeze-fix barrier
    // unchanged), register()s + link()s the live SSTs into
    // <db_path>/checkpoints/<chk-id>/, syncs the mapping journal, writes the
    // manifest blob with the embedded mapping trailer, and — iff a WAL is
    // attached (dbAttachWal at open) — captures the unflushed memtable tail
    // as <chk-dir>/WAL.delta instead of flushing (WAL-DELTA, §9 D10).
    MemorySegment linkedResult = nativeArena.allocate(24);
    linker.createIncrementalCheckpointLinked(
            db,
            resources.getSnapshot(),
            resources.getCheckpointId(),
            effectiveBaseCheckpointId,
            linkedResult);

    Path linkedManifestPath = readCString(linkedResult, 0L);
    List<Path> linkedNewSsts = readSstList(linkedResult, PTR);
    List<Path> linkedSharedSsts = readSstList(linkedResult, 2 * PTR);
    // NOTE: to carry per-file logical sizes onto the handles, extend
    // readSstList to also surface FrsLiveFile.size (offset 8 within each
    // entry); until then 0 is acceptable — Flink uses getStateSize() for
    // accounting only.
    try {
        // ---- D-J3: the manifest (small blob) keeps its EXCLUSIVE upload —
        // Flink's metadata contract untouched; restore reads the engine-FS
        // copy, not this one.
        StreamStateHandle linkedMetaHandle =
                trackedUpload(
                                linkedManifestPath,
                                streamFactory,
                                CheckpointedStateScope.EXCLUSIVE,
                                uploadTracker)
                        .get();

        // ---- Linked SSTs become metadata-only handles. NOTHING is uploaded.
        // new/shared split is preserved purely as the registry-registration
        // hint (§3.1.5): both lists wrap LinkedSstStateHandle; the local
        // sstRegistry is NOT consulted (cross-checkpoint sharing lives in the
        // engine mapping refcounts, not in upload-identity dedup — §9 D4).
        List<HandleAndLocalPath> linkedShared =
                new ArrayList<>(linkedNewSsts.size() + linkedSharedSsts.size());
        for (Path p : linkedNewSsts) {
            linkedShared.add(
                    HandleAndLocalPath.of(
                            new LinkedSstStateHandle(p.toString(), /* size= */ 0L),
                            p.getFileName().toString()));
        }
        for (Path p : linkedSharedSsts) {
            linkedShared.add(
                    HandleAndLocalPath.of(
                            new LinkedSstStateHandle(p.toString(), /* size= */ 0L),
                            p.getFileName().toString()));
        }

        // ---- Registry blob: unchanged EXCLUSIVE private-state entry. ----
        List<HandleAndLocalPath> linkedPrivate = new ArrayList<>(1);
        byte[] linkedRegistryBlob = resources.getRegistryBlob();
        if (linkedRegistryBlob != null && linkedRegistryBlob.length > 0) {
            StreamStateHandle registryHandle =
                    uploadRegistryBlob(linkedRegistryBlob, streamFactory);
            uploadTracker.trackHandle(registryHandle);
            linkedPrivate.add(
                    HandleAndLocalPath.of(registryHandle, SERIALIZER_REGISTRY_LOCAL_PATH));
        }

        IncrementalRemoteKeyedStateHandle linkedHandle =
                new IncrementalRemoteKeyedStateHandle(
                        backendIdentifier,
                        keyGroupRange,
                        resources.getCheckpointId(),
                        /* sharedState= */ linkedShared,
                        /* privateState= */ linkedPrivate,
                        linkedMetaHandle);
        return SnapshotResult.of(linkedHandle);
    } finally {
        try {
            linker.dbLinkedCheckpointResultFree(linkedResult);
        } catch (RuntimeException ignored) {
            // Idempotent on the native side.
        }
    }
}
// ... legacy upload path continues unchanged below (flag off ⇒ byte-identical).

// ---------------------------------------------------------------------------
// (3) Discard delegate (D-J1) — in the keyed backend / strategy owner that
//     receives checkpoint lifecycle notifications. LinkedSstStateHandle's
//     JM-side discardState() is a no-op; THIS is the authoritative cleanup.
//     Idempotent: a retried notification finds the blob gone (returns false).
// ---------------------------------------------------------------------------
@Override
public void notifyCheckpointSubsumed(long checkpointId) throws Exception {
    if (isLinkModeCheckpoint()) {
        boolean discarded = linker.dbDiscardLinkedCheckpoint(nativeArena, db, checkpointId);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "LINK-mode checkpoint {} subsumed; TM-side discard {}",
                    checkpointId,
                    discarded ? "completed" : "was already done");
        }
    }
    // ... existing subsume handling ...
}

@Override
public void notifyCheckpointAborted(long checkpointId) throws Exception {
    // ... existing abort/rollback handling first ...
    if (isLinkModeCheckpoint()) {
        // An aborted link checkpoint may have already linked its live set
        // (crash window D5-b). Best-effort unlink; NOT_FOUND (never linked /
        // already discarded) is fine.
        linker.dbDiscardLinkedCheckpoint(nativeArena, db, checkpointId);
    }
}
