# ForSt-RS CheckpointableKeyedStateBackend (B-Prod) — Design

**Status**: Draft (awaiting user review)
**Date**: 2026-05-10
**Track**: B-Prod (production-grade Flink keyed state backend)
**Related goals**: G-B (replace ForSt backend with forst-rs backend in Flink production jobs)
**Sibling specs**: TBD — Distributed-Forst track (items #2-5: serverless distributed LSM for Delta Join + multi-app usage)

## 1. Problem

`ForStRsKeyedStateBackend` today only `implements Closeable`. It exposes a custom synchronous
`snapshot(Path)` and uses a `"k/" || serialize(K) || "/" || stateName || "/"` composite key
encoding. That layout cannot plug into Flink's checkpoint protocol:

- No `RunnableFuture<SnapshotResult<KeyedStateHandle>>` async snapshot
- No `KeyGroupRange` awareness — can't be rescaled
- No `IncrementalKeyedStateHandle` — every snapshot is a full copy
- No `notifyCheckpointComplete` / `notifyCheckpointAborted` lifecycle
- Doesn't extend `AbstractKeyedStateBackend<K>`, so the keyed state SPI registries never see
  the registered state handles in the form Flink's runtime expects

Result: forst-rs is unusable as a drop-in `state.backend` for production Flink jobs. Local /
embedded / test usage works; checkpoint-driven recovery does not.

## 2. Goal

Make `ForStRsKeyedStateBackend extends AbstractKeyedStateBackend<K>` with full
`CheckpointableKeyedStateBackend<K>` SPI compliance, supporting:

- **Async incremental snapshots** with shared SST registry
- **Rescaling** via `SavepointKeyedStateHandle` (parallelism change between savepoint/restore)
- **Strict restore** semantics (missing baseline SST → `CheckpointRestoreException`)
- **Two CF modes** behind `state.backend.forst-rs.cf.mode = single | per-state` (default
  `single` for compatibility, `per-state` for jobs needing per-state CF isolation)
- **Virtual-thread async upload** to Flink's `CheckpointStorage` (which routes to OpenDAL via
  Flink's existing FsCheckpointStorage / S3CheckpointStorage)

## 3. Non-goals (deferred to v2 / sibling specs)

- PB-scale state per backend instance — needs distributed sharding (G-C track)
- Cross-app shared SST deduplication — out of scope; SST registry is per-backend
- Multi-version / MVCC reads during snapshot — accept point-in-time snapshot semantics
- TTL-aware snapshot pruning — TTL filter runs at compaction time, not snapshot time
- Per-state TTL when `cf.mode=single` — needs the per-state CF mode (covered here)

## 4. Scaling envelope

| State **per backend** (per TM) | `cf.mode=single` | `cf.mode=per-state` |
|---|---|---|
| < 100 GB | ✅ Best (single CF has lowest overhead) | ✅ Fine |
| 100 GB – 1 TB | ✅ Good | ✅ Good |
| 1 – 10 TB | 🟡 Borderline (slow L_max compaction; manifest grows) | ✅ Better (per-state isolation) |
| 10 – 100 TB | ❌ Don't use | 🟡 Borderline |
| 100+ TB | ❌ | ❌ |

For PB-scale total job state, parallelism is the answer (PB / N TMs). For PB **per TM**, the
G-C distributed-forst track replaces this design entirely.

## 5. Component layout

```
flink-statebackend-forst-rs/src/main/java/.../keyed/
├── ForStRsKeyedStateBackend.java          (rewrite — extends AbstractKeyedStateBackend<K>)
├── ForStRsKeyedStateBackendBuilder.java   (NEW — Flink builder convention)
├── ForStRsSnapshotStrategy.java           (NEW — implements SnapshotStrategy)
├── ForStRsRestoreOperation.java           (NEW — handles full + incremental + rescaling)
├── ForStRsIncrementalKeyedStateHandle.java (NEW — implements IncrementalKeyedStateHandle)
├── ForStRsKeyGroupedSerializer.java       (NEW — composite key encoding helper)
├── cf/
│   ├── CfRouter.java                      (NEW — interface; routes stateName → FrsCfHandle)
│   ├── SingleCfRouter.java                (NEW — one CF for all states; default)
│   └── PerStateCfRouter.java              (NEW — one CF per registered state)
└── sst/
    ├── ForStRsSstRegistry.java            (NEW — per-backend shared SST tracking)
    └── ForStRsSstUploader.java            (NEW — virtual-thread uploader to CheckpointStorage)
```

Existing state classes (`ForStRsValueState` etc.) get a thin update: composite key prefix
changes from `"k/"` to `kg(2 bytes BE)` and CF lookup goes through `CfRouter`. Their public API
stays the same.

## 6. Composite key encoding

```
For Value/List/Reducing/Aggregating:
  composite = kg(2 bytes BE) || serialize(K) || '/' || stateName.bytes || '/'

For Map (with user-key UK):
  composite = kg(2 bytes BE) || serialize(K) || '/' || stateName.bytes || '/' || serialize(UK)
```

- `kg = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism)` — Flink's standard
  murmur-based key-group router (NOT a custom hash; matches ForSt-Java behavior so savepoints
  are interoperable in shape, even if not bit-equivalent)
- The 2-byte big-endian prefix lets us prefix-scan **all entries in one key-group** with a
  single iterator
- Per-state-per-key-group prefix (`kg || '...' || stateName || '/'`) is what snapshot iteration
  uses

**Collision constraint**: if `serialize(K)` can contain the byte sequence `'/' || stateName ||
'/'`, decoding is ambiguous. Mitigation: bias toward the **last** occurrence of the marker (so
map UK suffixes never confuse the boundary). Documented as a constraint; for Flink's standard
length-prefixed serializers this is unambiguous in practice.

## 7. CfRouter abstraction

```java
public interface CfRouter extends Closeable {
    /** Returns the CF this state name's writes go to. Idempotent on repeat calls. */
    FrsCfHandle getCfForState(String stateName);

    /** All CFs the backend currently owns, in deterministic iteration order. */
    Collection<FrsCfHandle> allCfs();

    /** Inverse of getCfForState — used by restore to map manifest entries back. */
    String stateNameForCf(FrsCfHandle cf);

    /** True if this router uses one CF for everything. */
    boolean isSingleCf();
}
```

- `SingleCfRouter`: holds one default CF; every `getCfForState` returns it; `allCfs()` is a
  one-element list.
- `PerStateCfRouter`: lazily creates a CF on first `getCfForState(name)` via
  `linker.dbCreateCf(db, name)`. Persists the name→CF map on snapshot via the manifest so
  restore can recreate the same CFs.

The router is constructed by `ForStRsKeyedStateBackendBuilder` based on
`state.backend.forst-rs.cf.mode`.

## 8. Snapshot flow (incremental)

```
snapshot(checkpointId, ts, factory, options) -> RunnableFuture<SnapshotResult<KeyedStateHandle>>
  │
  ├─ SYNC PHASE (task thread, < ~1 ms):
  │  1. linker.flush(db) on every CF in router.allCfs()
  │     — forces L0 from memtable so all in-flight writes land in SSTs
  │  2. result = linker.createIncrementalCheckpoint(db, checkpointId, lastCompletedId)
  │     → returns: { newSstFiles: [path...], sharedSstFiles: [path...], manifestPath,
  │                  cfMap: {name → cf_id} }
  │  3. Snapshot resources captured: { result, keyGroupRange, factory, sstRegistry }
  │
  └─ ASYNC PHASE (virtual thread, runs after sync returns):
     1. For each newSstFile not yet in sstRegistry:
        — factory.createCheckpointStreamFactory().upload(file) → StreamStateHandle
        — register in privateState
     2. For each sharedSstFile:
        — if registry knows it for this checkpoint chain: reuse existing handle
        — otherwise: upload + register in sharedState
     3. Upload manifest (compact JSON with cfMap, kgRange, base_id) → metaStateHandle
     4. Return ForStRsIncrementalKeyedStateHandle(
            backendId, keyGroupRange, checkpointId, baseId,
            sharedState, privateState, metaStateHandle, cfMap)
```

`notifyCheckpointComplete(id)`: mark earlier checkpoints' SSTs no longer in shared use →
registry decrements ref counts → CheckpointStorage cleanup runs on next checkpoint.

`notifyCheckpointAborted(id)`: discard the upload, keep prior baseline. Uploaded SSTs remain
orphaned in CheckpointStorage; cleaned up by Flink's checkpoint coordinator GC.

## 9. Restore flow

```
restore(Collection<KeyedStateHandle> handles):
  │
  ├─ Determine target keyGroupRange from environment.taskInfo
  │
  ├─ Phase 1 (NO rescaling: source range == target range, single handle):
  │  1. Pick latest IncrementalHandle (highest checkpointId in chain)
  │  2. Download manifest from metaStateHandle, parse cfMap
  │  3. Download all SSTs referenced by manifest (sharedState ∪ privateState)
  │     → local target_dir
  │  4. STRICT CHECK: every SST in manifest must be downloaded
  │     → missing → throw CheckpointRestoreException(path, checkpointId)
  │  5. linker.openFromIncremental(target_dir, manifestPath, sstPathsArray) → FrsDb
  │  6. Reconstruct CfRouter from cfMap (single or per-state mode encoded in manifest)
  │
  └─ Phase 2 (RESCALING: source range != target range, possibly multiple handles):
     1. For each input handle:
        a. Download to a temp dir, open as a temporary read-only FrsDb
        b. Determine which key-groups overlap with target range
     2. Open empty target FrsDb at target_dir with target CfRouter
     3. For each key-group kg in target range:
        a. Find input handle whose source range contains kg
        b. For each cf in input handle's allCfs():
           - prefixIterate(kg(2 bytes)) on cf
           - For every (composite_k, v): write into target router.getCfForState(stateName)
             where stateName is parsed from composite_k via the marker-scan above
     4. Close all temporary input DBs
     5. Target FrsDb is now the restored state for the target key-group range
```

Rescaling is O(keys-in-target-range). Acceptable because rescaling is rare. For per-state CF
mode, the rescaling restore writes into the same per-state CFs the source had — manifest
preserves CF identity across restore.

## 10. New engine FFI

Two new C ABI exports in `crates/forst-rs-ffi/src/lib.rs`:

```rust
/// Persists a manifest snapshot tagged with checkpoint_id; returns the SST list
/// separated into "new since base" and "shared". base_checkpoint_id == 0 means
/// full snapshot (all SSTs are "new"). Reuses internal logic from
/// frs_db_get_live_files (prior turn) plus manifest persistence.
#[no_mangle]
pub unsafe extern "C" fn frs_create_incremental_checkpoint(
    db: FrsDb,
    checkpoint_id: u64,
    base_checkpoint_id: u64,
    out: *mut FrsIncrementalCheckpointResult,
) -> i32;

#[repr(C)]
pub struct FrsIncrementalCheckpointResult {
    pub manifest_path: *mut c_char,         // points into engine-owned dir; freed by caller
    pub new_ssts: *mut FrsLiveFileList,     // SSTs new since base_checkpoint_id
    pub shared_ssts: *mut FrsLiveFileList,  // SSTs shared with base_checkpoint_id
}

/// Reconstructs a DB at target_dir from base manifest + extra SST files.
/// Engine creates symlinks/hardlinks where same-FS, copies otherwise.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_from_incremental(
    target_dir: *const c_char,
    base_manifest: *const c_char,
    sst_files: *const *const c_char,   // array of c_char* paths
    sst_file_count: usize,
    out_handle: *mut FrsDb,
) -> i32;
```

Both reuse code from the `frs_db_get_live_files` work landed in the previous turn. New FFI
implementation: ~150 LOC + ~10 unit tests.

## 11. Error handling

| Failure | Behavior |
|---|---|
| `frs_flush` fails on snapshot | RunnableFuture completes exceptionally; checkpoint aborted |
| Upload to CheckpointStorage fails | Async future fails; Flink retries on next checkpoint |
| Restore: missing SST | `CheckpointRestoreException(path, base_id)` — operator picks different checkpoint |
| Restore: corrupt manifest | `CheckpointRestoreException` with `Cause: ManifestParseError` |
| Snapshot interrupted (job cancel) | Virtual thread interrupted; uploaded SSTs left in CheckpointStorage; next checkpoint or coordinator GC cleans up |
| Rescaling: source key-group missing from any input handle | `IllegalStateException` — programmer error (state lost) |
| `cf.mode=per-state` exceeds CF count limit | Engine errors `FRS_STATUS_RESOURCE_EXHAUSTED`; surface as backend init failure with config hint |

## 12. Testing strategy

```
Unit tests (~30):
  - KeyGroup encoding round-trip (encode → decode → equals)
  - KeyGroup prefix-scan boundary (kg=0, kg=maxParallelism-1, kg=middle)
  - SingleCfRouter / PerStateCfRouter conformance (same CfRouter contract)
  - ForStRsSstRegistry: ref counting, retention across checkpoints
  - ForStRsKeyGroupedSerializer: stateName collision avoidance (last-marker scan)
  - Manifest serialize/deserialize round-trip including cfMap

Integration tests (~15):
  - Full snapshot + restore round-trip (no parallelism change), both cf modes
  - 3-checkpoint incremental chain + restore from checkpoint 3
  - Rescale 4 → 8 → 4 (verify state preserved)
  - Strict-restore: delete an SST after upload, expect CheckpointRestoreException
  - TTL during snapshot (expired entries excluded from snapshot SSTs)
  - Empty-state snapshot edge case
  - Concurrent snapshot + write (snapshot must see point-in-time consistent)
  - cf.mode=per-state with 8 distinct states: verify 8 CFs created and snapshotted

E2E (~3):
  - MiniCluster job: keyBy + ValueState + checkpoint + restart from checkpoint
  - MiniCluster: same as above + rescale on restore (4 → 6)
  - Long-running soak: 10k checkpoints, verify SST registry doesn't leak
```

## 13. Implementation order (5 PRs)

| PR | Scope | Effort | Depends on |
|---|---|---|---|
| **B-Prod-P1** | KeyGroup encoding + state class updates + AbstractKeyedStateBackend skeleton + CfRouter interface + SingleCfRouter + PerStateCfRouter | 4-5 days | none |
| **B-Prod-P2** | Engine FFI: `frs_create_incremental_checkpoint` + `frs_db_open_from_incremental` + Rust tests | 2 days | none (parallel with P1) |
| **B-Prod-P3** | ForStRsSnapshotStrategy + ForStRsIncrementalKeyedStateHandle + ForStRsSstRegistry + virtual-thread uploader | 5 days | P1, P2 |
| **B-Prod-P4** | ForStRsRestoreOperation (full + incremental + rescaling, both cf modes) + integration tests + MiniCluster E2E | 5 days | P3 |
| **B-Prod-P5** | Benchmarks comparing single-CF vs per-state-CF at increasing state sizes (100MB → 1GB → 10GB) + tuning guide | 2-3 days | P4 |

**Total**: ~18-20 working days = ~3.5 weeks single-track. P1 + P2 parallel ⇒ ~3 weeks.

## 14. Risks & open questions

| Risk | Mitigation |
|---|---|
| Flink internal API churn between 2.x minor versions | Pin to Flink 2.2.0 (current branch); document API touch points; CI matrix lane catches regressions early |
| Virtual-thread + native FFM call interaction (potential pinning) | Test under load; fall back to platform thread pool if pinning observed (1-day fix, swap `Thread.ofVirtual()` → `Executors.newCachedThreadPool()`) |
| SST file lifecycle ownership ambiguity | Engine never deletes SSTs from a checkpoint dir; Java side owns CheckpointStorage cleanup via `notifyCheckpointComplete`; SST registry tracks ref counts |
| Composite key collision if `serialize(K)` contains `'/' || stateName || '/'` | Last-marker bias documented; for Flink's length-prefixed serializers this is unambiguous; if proven wrong in benches, add length prefix in P1 |
| Rescaling perf (O(N) iterate per restore) | Acceptable for v1; optimize via per-key-group SST split in v2 if measured slow |
| Per-state CF count explosion (jobs with 100+ states) | Document soft limit at 256 CFs; fail fast at backend init with explicit guidance to switch to `cf.mode=single` |
| Manifest format versioning | Include `manifest_version: 1` field; restore checks compatibility; future bumps add migration path |

## 15. Out of scope (will re-enter design later)

- **Items #2-5 (Distributed-Forst track)** — replaces this design's scaling envelope with
  consistent-hash sharding across many backend instances. Separate spec.
- **Item #2 (Flink planner rule for Fluss DJ replacement)** — needs the Distributed-Forst
  primitives first, then a planner-side spec.
- **Multi-engine usage of forst-rs (Spark, Doris, etc.)** — orthogonal; needs a stable cdylib
  ABI guarantee + multi-engine compat layer. Separate spec.

## 16. Acceptance criteria

- A Flink MiniCluster job using `ForStRsStateBackend` with `cf.mode=single`:
  1. Runs keyBy + ValueState/MapState
  2. Checkpoints successfully (incremental, async)
  3. Restarts from latest checkpoint with state intact
  4. Restarts from a savepoint with parallelism change (e.g. 4 → 8)
- Same job under `cf.mode=per-state` passes the same 4 criteria
- Strict-restore test: deleting an SST from CheckpointStorage causes
  `CheckpointRestoreException`, not silent data loss
- 10k-checkpoint soak: SST registry size remains bounded by the configured retention
- Snapshot path doesn't block task thread for more than 5 ms (95p) at 1 GB state
