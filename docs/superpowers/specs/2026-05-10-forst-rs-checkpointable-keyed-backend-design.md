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

Furthermore, the engine has no MVCC: snapshots conceptually need to flush the memtable to be
self-contained, which blocks the task thread for 100s of ms under write pressure.

Result: forst-rs is unusable as a drop-in `state.backend` for production Flink jobs. Local /
embedded / test usage works; checkpoint-driven recovery does not.

## 2. Goal

Make `ForStRsKeyedStateBackend extends AbstractKeyedStateBackend<K>` with full
`CheckpointableKeyedStateBackend<K>` SPI compliance, supporting:

- **MVCC snapshot isolation** at the engine layer (RocksDB-style sequence-tagged keys +
  snapshot-bounded version retention). Lets the snapshot sync phase capture a seq number
  without blocking on memtable flush.
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
- TTL-aware snapshot pruning — TTL filter runs at compaction time, not snapshot time
- Per-state TTL when `cf.mode=single` — needs the per-state CF mode (covered here)
- Time-travel reads beyond active snapshots — MVCC retention is snapshot-bounded only;
  versions are dropped once `min_active_snapshot.seq` advances past them

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

### 5a. Engine + FFI (Rust)

```
crates/forst-rs-engine/src/
├── mvcc/                                  (NEW — MVCC subsystem)
│   ├── mod.rs
│   ├── snapshot.rs                        (Snapshot type + SnapshotRegistry; atomic min-seq tracker)
│   ├── reader.rs                          (versioned read paths; latest-version-≤-seq lookup)
│   └── compaction_policy.rs               (min_active_snapshot_seq invariant; drop-old-version rule)
├── memtable.rs                            (UPDATE — store InternalKey with seq+type tags)
└── compaction.rs                          (UPDATE — call compaction_policy::should_drop)

crates/forst-rs-common/src/types.rs        (UPDATE — InternalKey already has seq+type fields;
                                            extend builders + add op_type constants)

crates/forst-rs-ffi/src/lib.rs             (NEW exports: frs_db_snapshot, frs_db_release_snapshot,
                                            frs_get_at, frs_iterator_open_at,
                                            frs_create_incremental_checkpoint_at,
                                            frs_db_open_from_incremental)
```

### 5b. Flink module (Java)

```
flink-statebackend-forst-rs/src/main/java/.../
├── ffm/
│   ├── ForStRsLinker.java                 (UPDATE — bind 6 new FFI methods; add FrsSnapshot)
│   └── FrsSnapshot.java                   (NEW — snapshot handle, AutoCloseable)
└── keyed/
    ├── ForStRsKeyedStateBackend.java      (rewrite — extends AbstractKeyedStateBackend<K>)
    ├── ForStRsKeyedStateBackendBuilder.java (NEW — Flink builder convention)
    ├── ForStRsSnapshotStrategy.java       (NEW — implements SnapshotStrategy; uses MVCC seq capture)
    ├── ForStRsRestoreOperation.java       (NEW — full + incremental + rescaling)
    ├── ForStRsIncrementalKeyedStateHandle.java (NEW — implements IncrementalKeyedStateHandle)
    ├── ForStRsKeyGroupedSerializer.java   (NEW — composite key encoding helper)
    ├── cf/
    │   ├── CfRouter.java                  (NEW — interface)
    │   ├── SingleCfRouter.java            (NEW — default)
    │   └── PerStateCfRouter.java          (NEW)
    └── sst/
        ├── ForStRsSstRegistry.java        (NEW — per-backend shared SST tracking)
        └── ForStRsSstUploader.java        (NEW — virtual-thread uploader)
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

## 6a. MVCC engine subsystem

### 6a.1 InternalKey: byte-level RocksDB compatibility (NORMATIVE)

forst-rs commits to **byte-level on-disk compatibility** with RocksDB's InternalKey
encoding so that production diagnostic tooling (`sst_dump`, `ldb`, third-party SST
inspectors) remains usable against forst-rs SSTs. This is a **major operational asset**
worth preserving and not negotiable.

**On-disk layout** (matches `rocksdb/db/dbformat.h`):

```
InternalKey bytes = user_key || tag(8 bytes, little-endian)

tag = (sequence << 8) | type
    where sequence ∈ [0, 2^56) (7 bytes worth)
          type     ∈ [0, 256)  (1 byte)

On-disk byte order of the tag (little-endian):
  byte 0:    type
  bytes 1-7: sequence in little-endian
```

**Type ordinals** (must match RocksDB's `ValueType` enum exactly):

| Ordinal | Name | Status in forst-rs v1 |
|---|---|---|
| 0x0 | kTypeDeletion | implemented |
| 0x1 | kTypeValue | implemented |
| 0x2 | kTypeMerge | reserved (errors at write time; no merge operator yet) |
| 0x4 | kTypeColumnFamilyDeletion | reserved |
| 0x5 | kTypeColumnFamilyValue | reserved |
| 0x7 | kTypeSingleDeletion | reserved |
| 0xB | kTypeRangeDeletion | reserved |
| 0xF | kTypeBlobIndex | reserved |
| (others) | reserved by RocksDB | reject at read time with `FRS_STATUS_UNSUPPORTED_VERSION` |

**Implementation constraints**:

- Sort order: `(user_key ASC, sequence DESC)` — same as RocksDB. Latest version of a key
  comes first in a forward iterator.
- Comparator: byte-wise on user_key, then numeric DESC on sequence. Implemented as a single
  byte-wise comparison if user_key bytes precede the tag (which they do in this layout).
- The existing `forst-rs-common::InternalKey` already has `seq + op_type` fields; P0 work
  changes the encoder/decoder to match the RocksDB byte layout described above (was a
  forst-rs-internal layout previously).

**Verification gate**: P0 ships with a test that writes 3 keys to a forst-rs SST, opens it
with the upstream `sst_dump --command=scan` binary, and asserts the dumped keys + sequences
+ types match. This is a hard CI gate.

### 6a.2 Snapshot type & registry

```rust
pub struct Snapshot {
    seq: SequenceNumber,
    db_id: DbId,                   // (4) — bind to DB instance for lifetime check
    captured_at: Instant,          // (3) — for snapshot_max_age_ms enforcement
    registry: Arc<SnapshotRegistry>,
}

pub struct SnapshotRegistry {
    // BTreeMap<seq, ref_count> — sorted access for finding the minimum live seq
    active: Mutex<BTreeMap<SequenceNumber, AtomicUsize>>,
    // Cached min_active for hot-path compaction reads (avoids lock).
    // Updated on every capture/release; stale reads return a value SAFER than truth
    // (i.e., older seq, retains more versions) so compaction never drops too aggressively.
    cached_min: AtomicU64,
}

impl SnapshotRegistry {
    pub fn capture(&self, db_id: DbId, current_seq: SequenceNumber) -> Snapshot { ... }
    pub fn release(&self, seq: SequenceNumber) { ... }
    pub fn min_active(&self) -> SequenceNumber { ... } // u64::MAX if empty
    pub fn oldest_age_ms(&self) -> u64 { ... }
    pub fn active_count(&self) -> usize { ... }
}

impl Drop for Snapshot {
    fn drop(&mut self) { self.registry.release(self.seq); }
}
```

### 6a.3 Long-lived snapshot policy (NORMATIVE)

Long-lived snapshots pin storage and degrade compaction efficiency. The engine enforces:

| Knob | Default | Behavior |
|---|---|---|
| Config: `forst-rs.mvcc.snapshot.max_age_ms` | `300000` (5 min) | Soft limit |
| **On overage** | — | **WARN only**, **NEVER auto-release** (auto-release would silently break correctness contracts) |
| Metric: `forst.snapshot.oldest_age_ms` | — | Gauge; emitted once per metric scrape interval |
| Metric: `forst.snapshot.active_count` | — | Gauge; emitted once per metric scrape interval |
| Metric: `forst.snapshot.pinned_bytes` | — | Estimated bytes retained because of active snapshots |

The warn line includes: snapshot age, captured_at, db_id, and a hint pointing at the
operator runbook. Production operators are expected to monitor `oldest_age_ms` and alert
when it crosses the configured threshold; runaway snapshot leaks are a programmer bug,
not something the engine can safely paper over.

### 6a.4 Sequence number overflow policy (NORMATIVE)

Sequence numbers are 56-bit (7 bytes). At 1 M writes/sec sustained, the space lasts
~2,285 years. At 1 G writes/sec it lasts ~2.3 years. The engine treats `seq ≥ 2^60`
(64× safety margin from the 56-bit on-disk space, allowing room for future 8-byte
expansion) as a fatal condition:

| Threshold | Behavior |
|---|---|
| `seq >= 2^59` | WARN: "sequence number high; consider checkpoint + restart" |
| `seq >= 2^60` | FATAL: log + return `FRS_STATUS_INTERNAL` from every write; backend stops accepting writes; reads continue |

**Recovery**: operator restarts the backend from the latest checkpoint. The engine
restores its global sequence counter from `manifest.checkpoint_seq`, NOT from zero —
this preserves MVCC ordering across restart. The checkpoint manifest written in §10b
already carries `snapshot_seq`; that's the recovery source.

### 6a.5 Read & iterator paths

**Read path** (`get_at(snapshot, user_key)` — assumes `snapshot.db_id == this_db.id`):

1. Probe memtable for entries `(user_key, *, *)`; pick the one with largest `seq <= snapshot.seq`
2. If the picked entry's `op_type == DELETION`: return `Ok(None)`
3. If found: return `Ok(Some(value))`
4. Otherwise (no memtable hit): probe L0/L1/Lmax SSTs in order; same latest-seq-≤-snapshot logic
5. If no version exists with `seq <= snapshot.seq`: return `Ok(None)` (key didn't exist at snapshot time)

**Iterator path** (`iter_at(snapshot)`):

- Standard merging iterator over memtable + L0 + L1 + Lmax
- Filters by `seq <= snapshot.seq` per user_key (skip same user_key after the first hit)
- Skip entries with `op_type == DELETION`

**Compaction policy** (`compaction_policy::should_drop`):

```rust
fn should_drop(entry: &InternalKey, newer_version_exists: bool, min_active_snapshot: Seq) -> bool {
    // Keep if any active snapshot might need this version.
    if entry.seq >= min_active_snapshot { return false; }
    // Drop only if a newer version exists for the same user_key (that newer version
    // serves all snapshots seq >= entry.seq + 1).
    newer_version_exists
}
```

**Concurrency**:
- `SnapshotRegistry` is `Arc<Mutex<BTreeMap>>` — fine because snapshot create/release is rare
  vs read/write traffic
- `min_active` is read by every compaction; cache the value with epoch counter to avoid
  taking the lock on the hot path
- Engine writes increment a global `AtomicU64` sequence counter; snapshots capture its current
  value

**Memory & storage cost**:
- Per snapshot: ~32 bytes (Snapshot struct + registry entry)
- Per stale version retained: `sizeof(InternalKey + value)` until compaction can drop it
- Worst case: long-lived analytics snapshot pins versions for hours; documented in operator
  guide

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

## 8. Snapshot flow (incremental, MVCC-based)

```
snapshot(checkpointId, ts, factory, options) -> RunnableFuture<SnapshotResult<KeyedStateHandle>>
  │
  ├─ SYNC PHASE (task thread, target < 1 ms; MVCC makes this near-zero):
  │  1. snapshot = linker.dbSnapshot(db)
  │     — captures current global seq into a Snapshot handle; ZERO blocking
  │     — pins all versions with seq ≤ snapshot.seq from compaction-time deletion
  │  2. result = linker.createIncrementalCheckpointAt(db, snapshot, checkpointId, lastCompletedId)
  │     — engine ASYNCHRONOUSLY persists memtable contents at snapshot.seq into a new SST
  │     — combines that SST + existing SSTs into a manifest tagged with checkpointId
  │     — returns IMMEDIATELY with manifest reference:
  │     → { newSstFiles: [path...], sharedSstFiles: [path...], manifestPath,
  │         cfMap: {name → cf_id}, snapshot_handle (still alive) }
  │  3. Sync phase ends — task thread released back to checkpoint barrier propagation
  │
  └─ ASYNC PHASE (virtual thread, runs after sync returns):
     1. WAIT for engine background flush of memtable-at-snapshot.seq to finish (poll/cv)
     2. For each newSstFile not yet in sstRegistry:
        — factory.createCheckpointStreamFactory().upload(file) → StreamStateHandle
        — register in privateState
     3. For each sharedSstFile:
        — if registry knows it for this checkpoint chain: reuse existing handle
        — otherwise: upload + register in sharedState
     4. Upload manifest (compact JSON with cfMap, kgRange, base_id, snapshot_seq)
        → metaStateHandle
     5. linker.dbReleaseSnapshot(snapshot) — UNPIN versions; compaction can resume normal
        retention
     6. Return ForStRsIncrementalKeyedStateHandle(
            backendId, keyGroupRange, checkpointId, baseId,
            sharedState, privateState, metaStateHandle, cfMap)
```

**Key MVCC win**: the sync phase no longer calls `flush()` (which can take 100-500ms under
write pressure). It captures a seq number, queues memtable persistence asynchronously, and
returns. Checkpoint barrier latency drops from ~hundreds of ms to ~µs.

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

Six new C ABI exports in `crates/forst-rs-ffi/src/lib.rs`.

### 10.0 ABI lifetime contract for snapshot handles (NORMATIVE)

These constraints are part of the FFI contract; **adding/changing them post-v1 is a
breaking ABI change**. They are enforced at the boundary by explicit checks (not just
documentation) wherever feasible.

| Constraint | Enforcement |
|---|---|
| **Same-DB**: a snapshot must be released against the SAME `FrsDb` instance that issued it. Cross-DB release is undefined behavior. | Snapshot carries `db_id` field (§6a.2). `frs_db_release_snapshot` checks `snapshot.db_id == db.id` and returns `FRS_STATUS_INVALID_ARGUMENT` on mismatch. |
| **No use-after-release**: snapshot handles passed to `frs_get_at` / `frs_iterator_open_at` must not race with `frs_db_release_snapshot` on another thread. | Snapshot ref-counts via `Arc`; release decrements. Concurrent `get_at` holds an `Arc` clone for the call duration; final release runs Drop only when ref-count reaches zero. Engine therefore tolerates the race **technically** but documents it as caller-beware: don't release a snapshot while a thread is still using it. |
| **Bounded lifetime**: snapshot handles must NOT outlive the checkpoint barrier they were created for. Checkpoint snapshots are managed entirely by the backend; operators MUST NOT hold them across barriers. | Backend owns all snapshots; user-facing FFI surface (compat_jni + ForStRsLinker) does NOT expose `dbSnapshot()` / `releaseSnapshot()` to Flink user code. Long-lived analytics snapshots (if added in v2) get a separate "named snapshot" API with explicit operator opt-in. |
| **DB close while iterators alive**: closing `FrsDb` while iterators (or get_at calls) still hold a snapshot must FAIL LOUDLY, never leak silently. | `frs_db_close` checks `db.outstanding_handles_count > 0` and returns `FRS_STATUS_RESOURCE_BUSY`. If forced (`frs_db_force_close`), iterators outstanding return `FRS_STATUS_DB_CLOSED` on next `iterator_next`. The default `frs_db_close` never panics; force-close is an explicit, separate symbol. |
| **Iterator lifetime ≤ snapshot lifetime**: an iterator opened with `iterator_open_at(snapshot)` must be closed before the snapshot is released. | Iterator carries an `Arc<Snapshot>` clone, so the snapshot stays alive as long as the iterator exists. Release on a snapshot still in use just decrements the user's ref count; engine continues to hold it via the iterator. |

These rules are tested via:
- Cross-DB release attempt (returns INVALID_ARGUMENT)
- Close-with-outstanding-iter (returns RESOURCE_BUSY)
- Force-close-with-outstanding-iter (next iterator_next returns DB_CLOSED, no panic)
- Concurrent release-while-reading stress test (1000 iterations, no UAF/no panic)

### 10a. MVCC primitives

### 10a. MVCC primitives

```rust
/// Captures a snapshot at the current global sequence number. The snapshot pins
/// all versions with seq ≤ captured_seq from compaction-time deletion.
/// Caller MUST eventually call frs_db_release_snapshot or leak versions forever.
#[no_mangle]
pub unsafe extern "C" fn frs_db_snapshot(
    db: FrsDb,
    out_snapshot: *mut FrsSnapshot,
) -> i32;

/// Releases a snapshot. Idempotent on null. After release, compaction can drop
/// versions with seq ≤ the released snapshot's seq if newer versions exist.
#[no_mangle]
pub unsafe extern "C" fn frs_db_release_snapshot(
    db: FrsDb,
    snapshot: FrsSnapshot,
) -> i32;

/// Reads the latest version of `key` with seq ≤ snapshot.seq. Returns
/// FRS_STATUS_NOT_FOUND if no version exists at snapshot time, or if the
/// latest version is a deletion tombstone.
#[no_mangle]
pub unsafe extern "C" fn frs_get_at(
    db: FrsDb,
    cf: FrsCfHandle,
    snapshot: FrsSnapshot,
    key: *const u8,
    key_len: usize,
    out_value: *mut FrsBytes,
) -> i32;

/// Opens an iterator that filters by snapshot.seq — yields the latest version of
/// each user_key with seq ≤ snapshot.seq, skipping deletion tombstones.
#[no_mangle]
pub unsafe extern "C" fn frs_iterator_open_at(
    db: FrsDb,
    cf: FrsCfHandle,
    snapshot: FrsSnapshot,
    out_iter: *mut FrsIterator,
) -> i32;
```

### 10b. Snapshot-aware checkpoint primitives

```rust
/// Persists a manifest snapshot tagged with checkpoint_id, capturing state at
/// snapshot.seq. Engine ASYNCHRONOUSLY flushes the memtable subset visible at
/// snapshot.seq into a new SST and returns IMMEDIATELY with the manifest path.
/// Caller polls/waits via metadata before uploading.
/// Returns SST list separated into "new since base" and "shared".
/// base_checkpoint_id == 0 means full snapshot.
#[no_mangle]
pub unsafe extern "C" fn frs_create_incremental_checkpoint_at(
    db: FrsDb,
    snapshot: FrsSnapshot,
    checkpoint_id: u64,
    base_checkpoint_id: u64,
    out: *mut FrsIncrementalCheckpointResult,
) -> i32;

#[repr(C)]
pub struct FrsIncrementalCheckpointResult {
    pub manifest_path: *mut c_char,         // points into engine-owned dir; freed by caller
    pub new_ssts: *mut FrsLiveFileList,     // SSTs new since base_checkpoint_id
    pub shared_ssts: *mut FrsLiveFileList,  // SSTs shared with base_checkpoint_id
    pub flush_done_eventfd: c_int,          // poll/wait this fd for memtable flush completion
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

MVCC primitives: ~400 LOC engine + ~150 LOC FFI + 30+ tests.
Checkpoint primitives reuse the prior turn's `frs_db_get_live_files` work: ~200 LOC + 15 tests.

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
Engine MVCC unit tests (~30 — Rust):
  - InternalKey encode/decode round-trip with seq + op_type
  - Memtable insert/get with MVCC: latest seq ≤ snapshot.seq returned
  - SnapshotRegistry: capture/release ref counting; min_active correctness
  - Reader: get_at returns None when key was deleted at-or-before snapshot.seq
  - Reader: get_at returns prior version when delete happens AFTER snapshot.seq
  - Iterator: skips DELETION tombstones; respects snapshot.seq filter
  - Iterator: dedupes user_key (returns latest version with seq ≤ snapshot.seq only)
  - Compaction policy: drops version when (seq < min_active && newer exists)
  - Compaction policy: keeps version when seq ≥ min_active
  - Compaction policy: keeps tombstone if it's the latest version visible to any snapshot
  - Concurrent snapshot + writes: 1k writes during snapshot read must not affect snapshot
  - Snapshot drop releases registry entry (Drop impl correctness)

Java unit tests (~30):
  - KeyGroup encoding round-trip (encode → decode → equals)
  - KeyGroup prefix-scan boundary (kg=0, kg=maxParallelism-1, kg=middle)
  - SingleCfRouter / PerStateCfRouter conformance (same CfRouter contract)
  - ForStRsSstRegistry: ref counting, retention across checkpoints
  - ForStRsKeyGroupedSerializer: stateName collision avoidance (last-marker scan)
  - Manifest serialize/deserialize round-trip including cfMap + snapshot_seq
  - FrsSnapshot AutoCloseable correctly releases on close + on try-with-resources exception

Integration tests (~18):
  - Full snapshot + restore round-trip (no parallelism change), both cf modes
  - 3-checkpoint incremental chain + restore from checkpoint 3
  - Rescale 4 → 8 → 4 (verify state preserved)
  - Strict-restore: delete an SST after upload, expect CheckpointRestoreException
  - TTL during snapshot (expired entries excluded from snapshot SSTs)
  - Empty-state snapshot edge case
  - **MVCC: 100k concurrent writes during snapshot — snapshot reads see only pre-snapshot state**
  - **MVCC: snapshot held while compaction runs — pinned versions survive**
  - **MVCC: after releaseSnapshot, compaction reclaims pinned space**
  - cf.mode=per-state with 8 distinct states: verify 8 CFs created and snapshotted

E2E (~4):
  - MiniCluster job: keyBy + ValueState + checkpoint + restart from checkpoint
  - MiniCluster: same as above + rescale on restore (4 → 6)
  - **MiniCluster: snapshot sync phase < 1 ms latency under 100k-writes/sec load (proves MVCC win)**
  - Long-running soak: 10k checkpoints, verify SST registry + snapshot registry don't leak
```

## 13. Implementation order (6 PRs)

| PR | Scope | Effort | Depends on |
|---|---|---|---|
| **B-Prod-P0** | Engine MVCC: InternalKey (extend), SnapshotRegistry, versioned reader, compaction policy + 30+ Rust tests | 8-10 days | none |
| **B-Prod-P1** | KeyGroup encoding + state class updates + AbstractKeyedStateBackend skeleton + CfRouter interface + SingleCfRouter + PerStateCfRouter | 4-5 days | none (parallel with P0) |
| **B-Prod-P2** | Engine FFI: `frs_db_snapshot/release_snapshot/get_at/iterator_open_at` (MVCC) + `frs_create_incremental_checkpoint_at/db_open_from_incremental` + Rust tests + ForStRsLinker bindings + FrsSnapshot Java type | 3 days | P0 |
| **B-Prod-P3** | ForStRsSnapshotStrategy (using MVCC seq capture) + ForStRsIncrementalKeyedStateHandle + ForStRsSstRegistry + virtual-thread uploader | 5 days | P1, P2 |
| **B-Prod-P4** | ForStRsRestoreOperation (full + incremental + rescaling, both cf modes) + integration tests + MiniCluster E2E | 5 days | P3 |
| **B-Prod-P5** | Benchmarks: single-CF vs per-state-CF at increasing state sizes (100MB → 1GB → 10GB → 100GB if feasible) + sync-phase latency w/wo MVCC + tuning guide | 3 days | P4 |

**Total**: ~28-31 working days = ~5-6 weeks single-track. P0 + P1 parallel ⇒ ~5 weeks.

Critical path: P0 → P2 → P3 → P4 → P5. P1 runs in parallel with P0 and lands by the time P3
needs it.

## 14. Risks & open questions

| Risk | Mitigation |
|---|---|
| Flink internal API churn between 2.x minor versions | Pin to Flink 2.2.0 (current branch); document API touch points; CI matrix lane catches regressions early |
| Virtual-thread + native FFM call interaction (potential pinning) | Test under load; fall back to platform thread pool if pinning observed (1-day fix, swap `Thread.ofVirtual()` → `Executors.newCachedThreadPool()`) |
| SST file lifecycle ownership ambiguity | Engine never deletes SSTs from a checkpoint dir; Java side owns CheckpointStorage cleanup via `notifyCheckpointComplete`; SST registry tracks ref counts |
| Composite key collision if `serialize(K)` contains `'/' || stateName || '/'` | Last-marker bias documented; for Flink's length-prefixed serializers this is unambiguous; if proven wrong in benches, add length prefix in P1 |
| Rescaling perf (O(N) iterate per restore) | Acceptable for v1; optimize via per-key-group SST split in v2 if measured slow |
| Per-state CF count explosion (jobs with 100+ states) | Document soft limit at 256 CFs; fail fast at backend init with explicit guidance to switch to `cf.mode=single` |
| Manifest format versioning | Include `manifest_version: 2` field (bumped from v1 spec since cfMap + snapshot_seq added); restore checks compatibility; future bumps add migration path |
| MVCC: long-lived snapshots pin storage | **Normative constraint in §6a.3** — config `snapshot.max_age_ms`, warn-only behavior, `forst.snapshot.oldest_age_ms` + `active_count` + `pinned_bytes` gauges, runbook hint in warn line. Operator monitors and alerts. |
| MVCC: compaction policy bug → silent data loss | Property-test the compaction policy: for any (set of writes, set of snapshots) sequence, verify reads at each snapshot return the correct value; fuzz with proptest. CI gate. |
| MVCC: sequence number overflow (56-bit on-disk, 60-bit safety threshold) | **Normative constraint in §6a.4** — fatal at seq ≥ 2^60, log + writes return INTERNAL, reads continue, recovery via checkpoint manifest's `snapshot_seq`. At 1 M writes/sec the threshold is ~36,500 years away; operationally unreachable but defined. |
| RocksDB byte-compat: divergence in InternalKey layout would silently break sst_dump | **Normative constraint in §6a.1** — CI gate runs `sst_dump --command=scan` against a forst-rs SST and asserts byte-equivalent output. P0 cannot land without this test green. |

## 15. Out of scope (will re-enter design later)

- **Items #2-5 (Distributed-Forst track)** — replaces this design's scaling envelope with
  consistent-hash sharding across many backend instances. Separate spec.
- **Item #2 (Flink planner rule for Fluss DJ replacement)** — needs the Distributed-Forst
  primitives first, then a planner-side spec.

## 16. Acceptance criteria

**Functional**:
- A Flink MiniCluster job using `ForStRsStateBackend` with `cf.mode=single`:
  1. Runs keyBy + ValueState/MapState
  2. Checkpoints successfully (incremental, async)
  3. Restarts from latest checkpoint with state intact
  4. Restarts from a savepoint with parallelism change (e.g. 4 → 8)
- Same job under `cf.mode=per-state` passes the same 4 criteria
- Strict-restore test: deleting an SST from CheckpointStorage causes
  `CheckpointRestoreException`, not silent data loss
- **MVCC isolation test**: a long-running snapshot that runs concurrently with 100k writes
  must see exactly the state-at-snapshot-time (writes after `dbSnapshot()` invisible to the
  snapshot's reads/iteration)
- **MVCC retention test**: after `releaseSnapshot()`, compaction drops the pinned versions
  on next compaction cycle (verify SST file count + manifest entries shrink)

**Non-functional**:

- 10k-checkpoint soak: SST registry size remains bounded by configured retention; engine
  Snapshot count returns to zero after each checkpoint completes
- Async snapshot completes within `state.backend.forst-rs.snapshot.timeout` (default 60s)
  for 1 GB state on local-FS CheckpointStorage

**Sync-phase latency (split into two sub-metrics, measured under load)**:

| Sub-metric | Threshold | What it verifies |
|---|---|---|
| `dbSnapshot()` call itself | **< 100 µs P99** | `captureSeq` is genuinely O(1); `SnapshotRegistry` has no lock contention |
| Sync phase end-to-end (barrier → ack) | **< 1 ms P95** | No surprises in JNI boundary, `createIncrementalCheckpointAt` setup, metadata serialization |

**Measurement condition**: 100 concurrent in-flight snapshots held during the test (NOT
serial single-snapshot). This matches the async state API workload, where multiple
operators may be checkpointing simultaneously, and prevents low-concurrency tests from
masking lock-contention regressions that only show up in production.

The sub-metric split exists because end-to-end can hide design regressions: a future
refactor adding a lock that's invisible at low concurrency degrades to 100 ms in
production. The `dbSnapshot()` sub-metric catches such regressions in isolation.

**Long-lived snapshot acceptance test** (per §6a.3 normative constraint):

- Hold a snapshot for 10 minutes (2× the default `snapshot_max_age_ms = 300000`)
- Verify: warning fires after 5 min and again at scrape interval
- Verify: backend continues accepting reads + writes throughout
- Verify: `forst.snapshot.oldest_age_ms` gauge advances correctly
- Verify: snapshot is NEVER auto-released (the read view stays valid for the full 10 min)
- After release: verify pinned versions are reclaimed by next compaction cycle
