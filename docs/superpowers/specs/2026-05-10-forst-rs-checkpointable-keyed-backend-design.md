# ForSt-RS CheckpointableKeyedStateBackend (B-Prod) — Design

**Status**: Draft (awaiting user review)
**Date**: 2026-05-10
**Track**: B-Prod (production-grade Flink keyed state backend)
**Related goals**: G-B (replace ForSt backend with forst-rs backend in Flink production jobs)
**Sibling specs**: TBD — Distributed-Forst track (items #2-5: serverless distributed LSM for Delta Join + multi-app usage)

## 1. Problem

`ForStRsKeyedStateBackend` today only `implements Closeable`. It cannot replace community
ForSt as a Flink state backend because it lacks both the checkpoint protocol AND the
production capability set:

**Missing checkpoint protocol** (this gates all production use):
- No `RunnableFuture<SnapshotResult<KeyedStateHandle>>` async snapshot
- No `KeyGroupRange` awareness — can't be rescaled
- No `IncrementalKeyedStateHandle` — every snapshot is a full copy
- No `notifyCheckpointComplete` / `notifyCheckpointAborted` lifecycle
- Doesn't extend `AbstractKeyedStateBackend<K>`
- Engine has no MVCC: snapshots conceptually need to flush the memtable to be
  self-contained, which blocks the task thread for 100s of ms under write pressure

**Missing production capabilities** (community ForSt has these; forst-rs doesn't):
- Disaggregated remote storage as primary (S3/GCS state, local cache) — community ForSt's
  headline differentiator
- Runtime tuning of block cache + WriteBufferManager
- Async state API (Flink 2.x)
- Timer service / priority queues for windowing
- State import/export migration

Result: forst-rs is unusable as a drop-in `state.backend` for production Flink jobs. Local /
embedded / test usage works; checkpoint-driven recovery + cloud-deployed jobs + windowing +
state migration do not.

## 2. Goal

Total feature parity with community Flink ForSt for keyed state, plus performance wins from
Rust + Arrow + JDK 25 FFM. Specifically, make `ForStRsKeyedStateBackend extends
AbstractKeyedStateBackend<K>` with full `CheckpointableKeyedStateBackend<K>` SPI
compliance AND all production-grade ForSt capabilities:

**Checkpoint protocol (§§6–10):**
- **MVCC snapshot isolation** at the engine layer (RocksDB byte-compat sequence-tagged keys
  + snapshot-bounded version retention). Lets the snapshot sync phase capture a seq number
  without blocking on memtable flush.
- **Async incremental snapshots** with shared SST registry
- **Rescaling** via `SavepointKeyedStateHandle` (parallelism change between savepoint/restore)
- **Strict restore** semantics (missing baseline SST → `CheckpointRestoreException`)
- **Two CF modes** behind `state.backend.forst-rs.cf.mode = single | per-state`
- **Virtual-thread async upload** to Flink's `CheckpointStorage`

**Production parity capabilities (§§6c–6g):**
- **Disaggregated remote storage as primary** (state on S3/GCS via OpenDAL, local disk as
  cache only) — community ForSt's headline feature
- **Block cache + WriteBufferManager runtime tuning** — production knobs operators expect
- **Async state API** (Flink 2.x async stateful ops with virtual-thread per-key serialization)
- **Timer service / priority queues** (KeyGroupedInternalPriorityQueue for windowing)
- **State import/export migration** (cross-job state transfer)

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

## 5c. Production parity capability map

This spec covers TOTAL feature parity with community Flink ForSt for keyed state. The
capabilities below are organized into 5 additional areas (§§6c–6g) layered on top of the
core checkpoint protocol (§§6–10). Each is a normative requirement of v1.

| Capability | Section | PR(s) |
|---|---|---|
| Disaggregated remote storage as primary (state on S3/GCS, local disk as cache) | §6c | P6 |
| Block cache + WriteBufferManager runtime tuning | §6d | P7 |
| Async state API (Flink 2.x async stateful ops) | §6e | P8 |
| Timer service / priority queues (windowing) | §6f | P9 |
| Import/export state migration (`ExportImportFilesMetaData` equivalent) | §6g | P10 |

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

### 6a.1 InternalKey: type ordinal compatibility with RocksDB (NORMATIVE)

forst-rs's SST file format is **Apache Arrow / Parquet-style columnar** (4 columns: key,
value, sequence, op_type stored as separate Arrow arrays per SST). It is **NOT byte-compatible
with RocksDB's block-based SST format** — `sst_dump` cannot decode forst-rs SSTs and is not
attempted as a CI gate. The Arrow layout is foundational to the vectorization wins
(Goal G-B; Arrow-driven memtable + SST is core to the perf delta).

What IS RocksDB-compatible:

- **OpType discriminants match RocksDB's `ValueType` enum** so any future surface that needs
  to round-trip op_type bytes through a RocksDB-shaped pipeline (FFI bridges, future WAL,
  one-off migration tools) can do so without a translation layer.
- **`InternalKey::encode_to_disk` / `decode_from_disk` utility methods** produce/consume the
  classic `user_key || tag(8B LE), tag = (seq<<8)|type` layout. Today they have no SST
  callers (the columnar layout doesn't need them), but they exist as a stable utility for
  any future RocksDB-shaped path.

**Type ordinals** (must match RocksDB's `ValueType` enum exactly for the OpType
discriminants forst-rs uses):

| Ordinal | Name | Status in forst-rs v1 |
|---|---|---|
| 0x0 | kTypeDeletion (`OpType::Delete`) | implemented |
| 0x1 | kTypeValue (`OpType::Put`) | implemented |
| 0x2 | kTypeMerge (`OpType::Merge`) | reserved (errors at write time; no merge operator yet) |
| 0x7 | kTypeSingleDeletion (`OpType::SingleDelete`) | reserved |
| (other RocksDB ordinals) | not used in forst-rs | unknown ordinals returned as `Corruption` from decode paths |

**Implementation constraints**:

- Sort order in memtable + SST iteration: `(user_key ASC, sequence DESC)` — same as RocksDB.
  Latest version of a key comes first in a forward iterator.
- Existing `forst-rs-common::OpType` ordinals were swapped to RocksDB-matching values in
  Task 0.1; existing `InternalKey::encode_to_disk/decode_from_disk` utility methods landed
  in Task 0.2. SST writer/reader paths remain Arrow-columnar (Task 0.3 dropped — no
  byte-concat call sites to refactor).

**Verification**: Task 0.1 + 0.2 unit tests (5 disk-format tests + ordinal table) gate the
ordinal+utility surface. SST file format is verified by the existing engine integration
tests (memtable + SST round-trip + compaction). The earlier-spec'd `sst_dump` CI gate is
dropped because the Arrow SST format is incompatible with `sst_dump` by design.

**Diagnostic tooling**: Arrow SSTs can be inspected with `parquet-tools` and any Arrow-aware
viewer. A forst-rs-specific dumper (CLI shipping the Arrow schema + decoder) is queued as a
future operational improvement; not gated on B-Prod.

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

## 6c. Disaggregated remote storage as primary (NORMATIVE)

**Headline community-ForSt feature**: state lives on S3/GCS/Azure Blob, local disk is just
a cache. Without this, forst-rs is local-only and not viable for large cloud-deployed jobs.

**Architecture**:
```
ForStRsKeyedStateBackend
        ↓ opens db with URI
ForStRsLinker.dbOpenRemote(uri, opendalConfig, cacheConfig)
        ↓ FFI
frs_db_open_remote(
    uri: *const c_char,                    // e.g. "s3://bucket/path" or "file:///local"
    opendal_config_json: *const c_char,    // {"endpoint":"...", "region":"...", "access_key_id":"..."}
    cache_dir: *const c_char,              // local cache directory (mandatory)
    cache_capacity_bytes: u64,             // LRU cache size (e.g. 1 GiB)
    out_handle: *mut FrsDb)
```

**Engine integration**:
- Engine already has OpenDAL backend (`crates/forst-rs-io/src/opendal_fs.rs`); P6 wires it
  into `EngineOptions.fs` selection based on URI scheme
- Local cache layer: SST files cached in `cache_dir` with LRU eviction; reads check cache
  first, fall back to remote
- Writes: SSTs written locally first then uploaded asynchronously; manifest persisted to
  remote on every flush
- Failure mode: if remote unavailable on read AND not in cache, return
  `FRS_STATUS_REMOTE_UNAVAILABLE`; backend surfaces as restartable error

**Operator config** (Flink `state.backend.forst-rs.*`):
```properties
state.backend.forst-rs.storage.uri = s3://my-bucket/flink-state/
state.backend.forst-rs.storage.opendal.s3.endpoint = ...
state.backend.forst-rs.storage.opendal.s3.region = ...
state.backend.forst-rs.cache.dir = /tmp/forst-rs-cache
state.backend.forst-rs.cache.size_mb = 1024
```

**Acceptance**: a Flink MiniCluster job using `state.backend.forst-rs.storage.uri =
s3://mock/...` (via S3-mock or MinIO) checkpoints, restarts, and reads state successfully.
Local cache stays bounded.

## 6d. Block cache + WriteBufferManager runtime tuning (NORMATIVE)

**Production knobs**: operators need to tune block cache size and the cross-CF memtable
budget without recompiling.

**FFM additions**:
```java
public final class ForStRsOptions {
    /** Shared LRU block cache size across all CFs. Default 256 MiB. */
    long blockCacheCapacityBytes = 256L * 1024 * 1024;

    /** Total memtable budget across all CFs (WriteBufferManager). Default 512 MiB. */
    long writeBufferManagerCapacityBytes = 512L * 1024 * 1024;

    /** Per-CF memtable size. Default 64 MiB. */
    long writeBufferSize = 64L * 1024 * 1024;

    /** Max memtable count per CF (sealed + active). Default 4. */
    int maxWriteBufferNumber = 4;

    /** Background compaction threads. Default 4. */
    int maxBackgroundCompactions = 4;

    /** Background flush threads. Default 4. */
    int maxBackgroundFlushes = 4;
}
```

**FFI**: extend the existing `frs_db_open_memory_tuned` (already takes 4 knobs) with 2 new
parameters for block cache capacity + write-buffer-manager capacity. Or add a new
`frs_db_open_with_options(opts: *const FrsEngineOptions, out: *mut FrsDb)` that takes a
struct so future expansion doesn't break the ABI.

**Acceptance**: same job under two configs (256 MiB vs 1 GiB block cache) shows measurably
different read latency (P95 ratio > 1.5x at saturated read pattern). WriteBufferManager
config caps total memtable bytes across CFs.

## 6e. Async state API (NORMATIVE)

**Flink 2.x feature**: state operations return `CompletableFuture<T>` instead of blocking.
Lets operators avoid blocking the task thread on remote-storage reads — critical with the
disaggregated storage from §6c.

**Architecture**:
```
ForStRsKeyedStateBackend.getValueState(...) returns ForStRsValueState<T>     // sync (existing)
ForStRsKeyedStateBackend.getAsyncValueState(...) returns ForStRsAsyncValueState<T>  // NEW

ForStRsAsyncValueState<T>:
    CompletableFuture<T> value()           // non-blocking
    CompletableFuture<Void> update(T)
    CompletableFuture<Void> clear()
```

**Implementation**: each async op submits to a virtual-thread executor that calls into the
sync state class, returning the result via the CompletableFuture. Per-key ordering preserved
via a per-key futures chain (so 2 concurrent updates on the same key serialize through
`thenCompose`).

**Per-key state queue**: ForStRsAsync*State classes need access to a per-key serialization
queue so writes don't reorder. Implemented as `ConcurrentHashMap<K, CompletableFuture<?>>`
where the value is the tail of the in-flight chain for that key.

**Acceptance**: 100k concurrent get-then-put cycles on 1k distinct keys complete with no
reordering anomalies (verified by sequence-numbered values and per-key invariant checks).

## 6f. Timer service / priority queues (NORMATIVE)

**Flink need**: windowing operators use `KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer>`
for processing-time and event-time timers. Without this, no windows.

**Architecture**:
- Add `ForStRsKeyedStateBackend.create<T>InternalPriorityQueue(name, elementSerializer)`
  → returns `KeyGroupedInternalPriorityQueue<T>` impl
- Composite key layout: `kg(2 bytes BE) || ts(8 bytes BE) || serializeT(payload)` —
  prefix-scan in (kg, ts) order yields earliest-timestamp-first iteration
- Operations:
  - `add(T)` → put with composite key
  - `poll()` → seek to first key in kg range, delete + return
  - `peek()` → seek to first key, return without deleting
  - `removeAll(set)` → batch delete

**Performance**: amortized O(log N) via LSM ordering; no in-memory heap needed because the
SST sort order IS the heap order. Memtable for "hot tip" (next-to-fire timers) gets cached
naturally.

**Acceptance**: tumbling window job over 1M events with 100k distinct keys completes
correctly; timer service emits exactly N window-fire events (verified against expected
window count).

## 6g. State import/export migration (NORMATIVE)

**Flink use case**: copy state from one job to another, rebuild from another job's snapshot,
state inspection tools.

**Architecture**:
```java
public final class ForStRsStateMigration {
    /** Exports a CF's data to a portable directory (SSTs + metadata). */
    public void exportColumnFamily(FrsCfHandle cf, Path exportDir);

    /** Creates a new CF in this DB by importing from a previously exported directory. */
    public FrsCfHandle createColumnFamilyFromImport(String name, Path importDir);
}
```

**FFI**:
```rust
pub unsafe extern "C" fn frs_cf_export(
    db: FrsDb, cf: FrsCfHandle, export_dir: *const c_char) -> i32;

pub unsafe extern "C" fn frs_db_create_cf_from_import(
    db: FrsDb, name: *const c_char, import_dir: *const c_char,
    out_cf: *mut FrsCfHandle) -> i32;
```

**Engine implementation**: export = hardlink/copy SSTs to `export_dir/sst/`, write
manifest with CF metadata; import = read manifest, register SSTs into a new CF in target
DB, rebuild block index.

**Acceptance**: round-trip test: write 10k keys, export, drop CF, import as new CF in same
DB, verify all keys readable with original values.

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

## 13. Implementation order (11 PRs)

| PR | Scope | Effort | Depends on |
|---|---|---|---|
| **B-Prod-P0** | Engine MVCC: InternalKey (RocksDB byte-compat ordinal swap), SnapshotRegistry, versioned reader, compaction policy + 30+ Rust tests + sst_dump CI gate | 8-10 days | none |
| **B-Prod-P1** | KeyGroup encoding + state class updates + AbstractKeyedStateBackend skeleton + CfRouter interface + SingleCfRouter + PerStateCfRouter | 4-5 days | none (parallel with P0) |
| **B-Prod-P2** | Engine FFI: `frs_db_snapshot/release_snapshot/get_at/iterator_open_at` (MVCC) + `frs_create_incremental_checkpoint_at/db_open_from_incremental` + Rust tests + ForStRsLinker bindings + FrsSnapshot Java type | 3 days | P0 |
| **B-Prod-P3** | ForStRsSnapshotStrategy (MVCC seq capture) + ForStRsIncrementalKeyedStateHandle + ForStRsSstRegistry + virtual-thread uploader | 5 days | P1, P2 |
| **B-Prod-P4** | ForStRsRestoreOperation (full + incremental + rescaling, both cf modes) + integration tests + MiniCluster E2E | 5 days | P3 |
| **B-Prod-P5** | Benchmarks: single-CF vs per-state-CF + sync-phase latency w/wo MVCC + tuning guide | 3 days | P4 |
| **B-Prod-P6** | Disaggregated remote storage as primary (§6c): `frs_db_open_remote` engine FFI + OpenDAL routing + local cache LRU + ForStRsLinker bindings + ForStRsOptions config + S3-mock MiniCluster E2E | 6 days | P4 |
| **B-Prod-P7** | Block cache + WriteBufferManager runtime tuning (§6d): `frs_db_open_with_options` FFI taking `FrsEngineOptions` struct + ForStRsOptions Java config + JMH bench showing latency change between configs | 3 days | P4 |
| **B-Prod-P8** | Async state API (§6e): ForStRsAsync{Value,List,Map,Reducing,Aggregating}State + per-key futures-chain serialization + ForStRsKeyedStateBackend.getAsync*State methods + 100k concurrent op stress test | 6 days | P4 |
| **B-Prod-P9** | Timer service / priority queues (§6f): KeyGroupedInternalPriorityQueue impl with `kg||ts||payload` composite encoding + tumbling window MiniCluster E2E | 4 days | P1 (key-group infra) |
| **B-Prod-P10** | State import/export (§6g): `frs_cf_export` + `frs_db_create_cf_from_import` FFI + ForStRsStateMigration Java API + round-trip test | 3 days | P4 |

**Total**: ~50-53 working days = ~10-11 weeks single-track.

**Critical path** (longest dependency chain): P0 → P2 → P3 → P4 → {P6, P7, P8, P9, P10} → done.

**Parallelism opportunities**:
- P0 + P1 in parallel (different parts of stack) ⇒ saves ~1 week
- P6, P7, P8, P9, P10 all depend only on P4 — fully parallelizable across 5 streams
  (saves ~3 weeks if 2-3 implementers; ~4-5 weeks if 5 implementers)
- Realistic team-of-2: ~7-8 weeks
- Realistic team-of-3+: ~6-7 weeks
- Single implementer: ~10-11 weeks

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
| RocksDB-compat misunderstanding: spec previously claimed sst_dump compat | **Resolved 2026-05-11**: SSTs are Arrow-columnar by design (key vectorization win); sst_dump cannot decode them. Spec §6a.1 revised to scope the compat claim to OpType ordinals + InternalKey encode/decode utility methods only. Diagnostic tooling pivot: parquet-tools + future forst-rs dumper. |

## 15. Out of scope (will re-enter design later)

- **Items #2-5 (Distributed-Forst track)** — replaces this design's scaling envelope with
  consistent-hash sharding across many backend instances. Separate spec.
- **Item #2 (Flink planner rule for Fluss DJ replacement)** — needs the Distributed-Forst
  primitives first, then a planner-side spec.

## 16. Acceptance criteria

**Functional — checkpoint protocol**:
- A Flink MiniCluster job using `ForStRsStateBackend` with `cf.mode=single`:
  1. Runs keyBy + ValueState/MapState
  2. Checkpoints successfully (incremental, async)
  3. Restarts from latest checkpoint with state intact
  4. Restarts from a savepoint with parallelism change (e.g. 4 → 8)
- Same job under `cf.mode=per-state` passes the same 4 criteria
- Strict-restore test: deleting an SST from CheckpointStorage causes
  `CheckpointRestoreException`, not silent data loss
- **MVCC isolation test**: a long-running snapshot that runs concurrently with 100k writes
  must see exactly the state-at-snapshot-time
- **MVCC retention test**: after `releaseSnapshot()`, compaction drops the pinned versions
  on next compaction cycle
- **RocksDB byte-compat test**: `sst_dump --command=scan` against a forst-rs SST returns
  the expected user keys, sequences, and types (CI gate)

**Functional — production parity**:
- **Remote-primary storage** (§6c): MiniCluster job with `storage.uri = s3://mock/...`
  (S3-mock or MinIO) checkpoints, restarts, reads state successfully; local cache stays
  bounded by `cache.size_mb`
- **Block cache tuning** (§6d): same job under 256 MiB vs 1 GiB block cache configs shows
  P95 read-latency ratio > 1.5x at saturated read pattern
- **WriteBufferManager** (§6d): total memtable bytes across CFs respects configured cap
- **Async state API** (§6e): 100k concurrent get-then-put cycles on 1k distinct keys
  complete with no per-key reordering anomalies
- **Timer service** (§6f): tumbling window job over 1M events with 100k distinct keys
  emits exactly N window-fire events
- **Import/export** (§6g): write 10k keys → export → drop CF → import as new CF in same
  DB → all keys readable with original values

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
