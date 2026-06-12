// ============================================================================
// FRS-PHASE2 LINK-MODE fragment for keyed/ForStRsRestoreOperation.java
//
// The DOWNLOAD-SKIP (instant-link restore) branch of restoreNoRescaling
// (design §5 Stage-3; README D-J4). Detection: a LINK-mode checkpoint's
// sharedState entries are LinkedSstStateHandle — no byte-readable SSTs exist
// to download. The engine resolves everything from the chk dir on the ENGINE
// filesystem (the uploaded manifest copy is not used).
// ============================================================================

// ---------------------------------------------------------------------------
// Branch — placed at the TOP of restoreNoRescaling, before the download-dir
// creation. When it fires, the entire download loop is skipped.
// ---------------------------------------------------------------------------
private RestoreResult restoreNoRescaling(IncrementalRemoteKeyedStateHandle handle)
        throws IOException {

    boolean linkMode =
            !handle.getSharedState().isEmpty()
                    && handle.getSharedState().stream()
                            .allMatch(h -> h.getHandle() instanceof LinkedSstStateHandle);
    if (linkMode) {
        return restoreInstantLinked(handle);
    }
    // ... existing download-based restore continues unchanged ...
}

/**
 * FRS-PHASE2: INSTANT-LINK restore — downloads NOTHING. The chk dir
 * ({@code <db_path>/checkpoints/<%020d ckpt>}) is derived from any linked
 * handle's path; the engine adopt()s each physical under the new working
 * namespace (NotOwned — it will never delete a checkpoint-owned object),
 * opens through the mapped read-path indirection, replays {@code WAL.delta}
 * when present, and warms the cache lazily. Restore wall-time is O(files)
 * metadata — flat in state size (recorded: ~12 ms across a 16x sweep).
 *
 * <p>CLAIM discipline (D-J4): the restore-source checkpoint must stay
 * retained until {@code linker.dbAdoptedResidual(arena, db) == 0}. Flink
 * CLAIM mode + the engine's compaction weaning satisfy this; surfacing
 * residual==0 to the JM as "safe to release" is follow-up wiring.
 */
private RestoreResult restoreInstantLinked(IncrementalRemoteKeyedStateHandle handle)
        throws IOException {
    // Every linked path lives in the same chk dir (design §9 D1) — derive it.
    LinkedSstStateHandle first =
            (LinkedSstStateHandle) handle.getSharedState().get(0).getHandle();
    java.nio.file.Path linked = java.nio.file.Paths.get(first.getLinkedPath());
    String ckptDir = linked.getParent().toString();

    // E5-HIGH-2 registry blob: still a normal uploaded private-state entry —
    // download + parse exactly as the legacy path does (small blob).
    HandleAndLocalPath registryEntry = null;
    for (HandleAndLocalPath hlp : handle.getPrivateState()) {
        if (ForStRsSnapshotStrategy.SERIALIZER_REGISTRY_LOCAL_PATH.equals(hlp.getLocalPath())) {
            registryEntry = hlp;
        }
    }
    Map<String, StateSerializerMetadata> restoredSerializerMetadata =
            registryEntry == null
                    ? Collections.emptyMap()
                    : downloadAndParseRegistryBlob(registryEntry, handle);

    // Instant restore: local-primary uses the 3-arg variant; remote-primary
    // (storageUri configured) uses the remote variant with the SAME
    // uri/config/cache parameters the backend used for frs_db_open_remote.
    FrsDb db;
    try {
        if (storageUri == null) {
            db =
                    linker.dbOpenFromLinkedCheckpointInstant(
                            arena, ckptDir, targetDir.toString());
        } else {
            db =
                    linker.dbOpenFromLinkedCheckpointInstantRemote(
                            arena,
                            storageUri,
                            opendalConfigJson,
                            cacheDir.toString(),
                            cacheCapacityBytes,
                            ckptDir,
                            targetDir.toString());
        }
    } catch (RuntimeException re) {
        // Loud failure (missing physical / no mapping trailer / torn
        // WAL.delta) — never a silent empty state.
        throw new ForStRsCheckpointRestoreException(
                ckptDir,
                handle.getCheckpointId(),
                "ForSt-RS instant-link restore refused: " + re.getMessage(),
                re);
    }
    FrsCfHandle defaultCf;
    try {
        defaultCf = linker.dbDefaultCf(db, arena);
    } catch (RuntimeException re) {
        db.close();
        throw new ForStRsCheckpointRestoreException(
                targetDir.toString(),
                handle.getCheckpointId(),
                "ForSt-RS engine restored, but default CF unreachable: " + re.getMessage(),
                re);
    }

    // NO sstRegistry re-population (legacy step 4): under link mode the next
    // checkpoint's sharing is resolved by the engine mapping layer (chained
    // link checkpoints resolve TRANSITIVELY to the original physicals —
    // engine register-rebind guard), not by upload-identity registry dedup.

    if (LOG.isInfoEnabled()) {
        LOG.info(
                "Instant-link restore of checkpoint {} from {} complete; adopted residual = {}",
                handle.getCheckpointId(),
                ckptDir,
                linker.dbAdoptedResidual(arena, db));
    }
    return new RestoreResult(
            db, defaultCf, handle.getCheckpointId(), restoredSerializerMetadata);
}
