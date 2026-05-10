# ForSt-RS CheckpointableKeyedStateBackend (B-Prod) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Promote forst-rs from `Closeable` embedded backend to a production-grade Flink `CheckpointableKeyedStateBackend` with total feature parity to community ForSt.

**Architecture:** 11 PRs split across the Rust engine (`crates/forst-rs-engine`, `forst-rs-ffi`) and the Flink module (`flink-statebackend-forst-rs`). P0 lays MVCC foundations (RocksDB byte-compat InternalKey, SnapshotRegistry, versioned reader, compaction policy). P1-P5 wire the keyed-state checkpoint protocol (key-group encoding, async snapshot strategy, restore + rescaling, benchmarks). P6-P10 add the production parity capabilities (remote-primary storage, runtime tuning, async state API, timer service, import/export).

**Tech Stack:** Rust 2021 (MSRV 1.88) for engine + FFI; Java 25 (FFM API) for the Flink module; Apache Flink 2.2.0; Apache Arrow 54 (Rust); JUnit 5; criterion / JMH for benchmarks; OpenDAL 0.50 for cloud storage.

**Spec:** `docs/superpowers/specs/2026-05-10-forst-rs-checkpointable-keyed-backend-design.md` (commit `2d1fefd6e` on `origin/forst-rs`).

---

## Cross-PR conventions

**Repo layout** (two checkouts; same machine assumed):
- ForSt repo: `/Users/lijunqing/Code/stczwd/ForSt` (branch `forst-rs`)
- Flink repo: `/Users/lijunqing/Code/stczwd/flink` (branch `forst-rs-jdk25`)

**Build/test verification commands** (run from each repo root):

| Repo | Quick check | Full check |
|---|---|---|
| ForSt | `cargo test --workspace -q` | `cargo build --release && cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --check && cargo doc --no-deps --workspace` |
| Flink | `JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home MAVEN_OPTS=--enable-native-access=ALL-UNNAMED mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Drat.skip=true -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Dargline="--enable-native-access=ALL-UNNAMED -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib"` | (same + remove `-Drat.skip=true` + `-Dtest=` filters) |

**Per-PR rebuild contract**: any PR that adds a new FFI symbol on the ForSt side MUST be followed by a `cargo build --release -p forst-rs-ffi` before running Flink-side tests, so the cdylib at `target/release/libforst_rs_ffi.dylib` carries the new symbol. Several Flink-side tasks below explicitly include this rebuild as a step.

**Commit message format**: `<type>(<scope>): <subject>` where `<type>` ∈ {`feat`, `fix`, `refactor`, `test`, `docs`, `build`}, `<scope>` ∈ {`engine`, `ffi`, `compat-jni`, `state-forst-rs`, `state-forst-rs-keyed`, `state-forst-rs-async`, `state-forst-rs-storage`, `bench`}. Always end with the `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>` line via heredoc.

**PR boundary**: each `B-Prod-Pn` is one push branch + one PR. Within a PR, multiple commits is fine (in fact preferred — commit per task).

**Dependency graph**:
```
P0 ─┬─→ P2 ─→ P3 ─→ P4 ─┬─→ P5
    │                   ├─→ P6
P1 ─┼───────────────────┼─→ P7
    │                   ├─→ P8
    └────────────→ P9 ──┘
                  P10 ←──── P4
```

---

## File structure

### Rust crates (added/modified)

```
crates/forst-rs-common/src/
├── types.rs                                         (MODIFY: OpType ordinal swap to RocksDB)

crates/forst-rs-engine/src/
├── mvcc/                                            (NEW directory — P0)
│   ├── mod.rs                                       (NEW: re-exports)
│   ├── snapshot.rs                                  (NEW: Snapshot + SnapshotRegistry)
│   ├── reader.rs                                    (NEW: get_at + iter_at)
│   └── compaction_policy.rs                         (NEW: should_drop function)
├── memtable.rs                                      (MODIFY: store InternalKey with new layout)
├── compaction.rs                                    (MODIFY: call compaction_policy::should_drop)
├── db.rs                                            (MODIFY: snapshot/release/get_at/iter_at API + open_remote)
├── engine_options.rs                                (MODIFY: add block_cache, write_buffer_manager fields)
├── lib.rs                                           (MODIFY: pub use mvcc::*)
└── tests/incremental_checkpoint_it.rs               (NEW — P0/P2 integration test)

crates/forst-rs-ffi/src/
├── lib.rs                                           (MODIFY: 8 new exports across P2/P6/P7/P10)
└── compat_jni.rs                                    (no change for B-Prod; JNI surface stays as-is)

crates/forst-rs-io/src/
├── opendal_fs.rs                                    (MODIFY: expose factory keyed by URI scheme)

crates/forst-rs-storage/src/
└── local_cache.rs                                   (NEW — P6: LRU SST cache)
```

### Flink module (`flink-state-backends/flink-statebackend-forst-rs/`)

```
src/main/java/org/apache/flink/state/forstrs/
├── ForStRsOptions.java                              (MODIFY: add 6 tuning fields + storage uri config)
├── ForStRsStateBackend.java                         (MODIFY: pass options through to backend)
├── ForStRsStateBackendFactory.java                  (no change)
├── ForStRsKeyedStateBackend.java                    (REWRITE — extends AbstractKeyedStateBackend<K>)
├── FrsBackendException.java                         (no change)
├── FrsStatus.java                                   (MODIFY: add REMOTE_UNAVAILABLE, RESOURCE_BUSY, DB_CLOSED)
├── ffm/
│   ├── ForStRsLinker.java                           (MODIFY: bind 8 new FFI methods)
│   ├── FrsDb.java                                   (no change)
│   ├── FrsCfHandle.java                             (no change)
│   ├── FrsIterator.java                             (no change)
│   └── FrsSnapshot.java                             (NEW — P2: AutoCloseable snapshot handle)
├── keyed/
│   ├── ForStRsKeyedStateBackendBuilder.java         (NEW — P1)
│   ├── ForStRsSnapshotStrategy.java                 (NEW — P3)
│   ├── ForStRsRestoreOperation.java                 (NEW — P4)
│   ├── ForStRsIncrementalKeyedStateHandle.java      (NEW — P3)
│   ├── ForStRsKeyGroupedSerializer.java             (NEW — P1)
│   ├── cf/
│   │   ├── CfRouter.java                            (NEW — P1)
│   │   ├── SingleCfRouter.java                      (NEW — P1)
│   │   └── PerStateCfRouter.java                    (NEW — P1)
│   └── sst/
│       ├── ForStRsSstRegistry.java                  (NEW — P3)
│       └── ForStRsSstUploader.java                  (NEW — P3)
├── state/
│   ├── ForStRsValueState.java                       (MODIFY: composite key uses kg prefix)
│   ├── ForStRsListState.java                        (MODIFY: same)
│   ├── ForStRsMapState.java                         (MODIFY: same)
│   ├── ForStRsReducingState.java                    (MODIFY: same)
│   └── ForStRsAggregatingState.java                 (MODIFY: same)
├── async/                                           (NEW directory — P8)
│   ├── ForStRsAsyncValueState.java                  (NEW)
│   ├── ForStRsAsyncListState.java                   (NEW)
│   ├── ForStRsAsyncMapState.java                    (NEW)
│   ├── ForStRsAsyncReducingState.java               (NEW)
│   ├── ForStRsAsyncAggregatingState.java            (NEW)
│   └── PerKeyFuturesChain.java                      (NEW: per-key serialization)
├── timer/                                           (NEW directory — P9)
│   └── ForStRsKeyGroupedInternalPriorityQueue.java  (NEW)
├── migration/                                       (NEW directory — P10)
│   └── ForStRsStateMigration.java                   (NEW)
└── lookup/
    └── ForStRsLocalLookupFunction.java              (no change)

src/test/java/org/apache/flink/state/forstrs/
├── ffm/
│   ├── ForStRsLinkerExtendedTest.java               (MODIFY: add MVCC + tuning + remote tests)
│   └── FrsSnapshotTest.java                         (NEW — P2)
├── keyed/
│   ├── ForStRsKeyedStateBackendIT.java              (NEW — P4)
│   ├── ForStRsSnapshotStrategyTest.java             (NEW — P3)
│   ├── ForStRsRestoreOperationTest.java             (NEW — P4)
│   ├── ForStRsKeyGroupedSerializerTest.java         (NEW — P1)
│   ├── ForStRsRescalingIT.java                      (NEW — P4)
│   ├── ForStRsMVCCIsolationIT.java                  (NEW — P3)
│   ├── cf/
│   │   ├── SingleCfRouterTest.java                  (NEW — P1)
│   │   └── PerStateCfRouterTest.java                (NEW — P1)
│   └── sst/
│       └── ForStRsSstRegistryTest.java              (NEW — P3)
├── async/
│   ├── ForStRsAsyncValueStateTest.java              (NEW — P8)
│   └── PerKeyFuturesChainTest.java                  (NEW — P8)
├── timer/
│   └── ForStRsKeyGroupedInternalPriorityQueueTest.java (NEW — P9)
├── migration/
│   └── ForStRsStateMigrationTest.java               (NEW — P10)
├── storage/
│   └── ForStRsRemoteStorageIT.java                  (NEW — P6)
└── tuning/
    └── ForStRsRuntimeTuningIT.java                  (NEW — P7)

src/test/java/org/apache/flink/state/forstrs/jmh/
└── ForStRsBProdBenchmark.java                       (NEW — P5: single-CF vs per-state-CF + sync-phase latency)
```

---

# PR B-Prod-P0: Engine MVCC

**Goal**: ship the MVCC engine subsystem (RocksDB byte-compat InternalKey, SnapshotRegistry, versioned reader, compaction policy) as a self-contained Rust PR. No Flink-side changes.

**Branch**: `b-prod-p0-engine-mvcc` off `forst-rs`.

**Effort**: 8-10 days. **Depends on**: nothing.

**Critical path** for the rest of B-Prod (P2 needs P0).

### Task 0.1: Swap OpType ordinals to RocksDB byte-compat

**Files:**
- Modify: `crates/forst-rs-common/src/types.rs:45-100` (OpType enum + try_from_u8)
- Test: `crates/forst-rs-common/src/types.rs` (existing tests need updating)

- [ ] **Step 1: Read current OpType definition to capture all call sites**

```bash
grep -rn "OpType::Put\|OpType::Delete\|OpType::SingleDelete\|OpType::Merge" crates/ | wc -l
```
Expected: a count > 0. Note the call sites — they need no change because we keep the variant names; only the backing discriminants swap.

- [ ] **Step 2: Edit OpType enum to use RocksDB ordinals**

Replace the existing enum body (lines ~45-65) with:
```rust
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum OpType {
    /// kTypeDeletion in RocksDB; tombstone for a deleted user_key.
    Delete = 0,
    /// kTypeValue in RocksDB; standard put.
    Put = 1,
    /// kTypeMerge in RocksDB. Reserved — engine returns
    /// FRS_STATUS_UNSUPPORTED at write time until merge operator lands.
    Merge = 2,
    /// kTypeSingleDeletion in RocksDB. Reserved (same status as Merge).
    SingleDelete = 7,
}
```

- [ ] **Step 3: Update try_from_u8 to match new ordinals**

In `try_from_u8` (~line 75-85), replace match body:
```rust
match value {
    0 => Some(OpType::Delete),
    1 => Some(OpType::Put),
    2 => Some(OpType::Merge),
    7 => Some(OpType::SingleDelete),
    _ => None,
}
```

- [ ] **Step 4: Update existing test fixtures that hardcode discriminants**

```bash
grep -n "0u8\|1u8\|2u8\|3u8" crates/forst-rs-common/src/types.rs | head
```
For any test that constructs an `OpType` via `try_from_u8(N)`, ensure the expected variant matches the new ordinals. Specifically search for:
```bash
grep -B2 -A2 "try_from_u8\|try_from_u8_checked" crates/forst-rs-common/src/types.rs
```
Update each test's discriminant + expected variant pair to one of: `(0, Delete)`, `(1, Put)`, `(2, Merge)`, `(7, SingleDelete)`.

- [ ] **Step 5: Run common-crate tests to verify**

```bash
cargo test -p forst-rs-common --lib
```
Expected: all tests pass.

- [ ] **Step 6: Run full workspace to catch downstream breakage**

```bash
cargo test --workspace -q
```
Expected: all pass. Any failure indicates a hardcoded ordinal somewhere — fix in place.

- [ ] **Step 7: Commit**

```bash
git add crates/forst-rs-common/src/types.rs
git commit -m "$(cat <<'EOF'
refactor(engine): swap OpType ordinals to RocksDB byte-compat

Delete=0 (kTypeDeletion), Put=1 (kTypeValue), Merge=2 (kTypeMerge),
SingleDelete=7 (kTypeSingleDeletion). Variant names unchanged so call
sites compile without change.

Pre-production; no on-disk data to migrate. Sets up §6a.1 byte-compat
for sst_dump diagnostic tooling.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.2: InternalKey on-disk encoder/decoder

**Files:**
- Modify: `crates/forst-rs-common/src/types.rs` (add `encode_to_disk` / `decode_from_disk` methods on `InternalKey`)
- Test: same file (add module `disk_format_tests`)

- [ ] **Step 1: Write the failing test**

Append to `crates/forst-rs-common/src/types.rs`:
```rust
#[cfg(test)]
mod disk_format_tests {
    use super::*;

    #[test]
    fn rocksdb_byte_layout_value() {
        let key = InternalKey::new(b"hello".to_vec(), SequenceNumber::new(0x123456_u64), OpType::Put);
        let bytes = key.encode_to_disk();
        // Layout: user_key || tag(8B little-endian), tag = (seq << 8) | type
        // seq = 0x123456, type = 1 (Put), tag = 0x12345601
        // little-endian bytes: 01 56 34 12 00 00 00 00
        assert_eq!(bytes, b"hello\x01\x56\x34\x12\x00\x00\x00\x00");
    }

    #[test]
    fn rocksdb_byte_layout_delete() {
        let key = InternalKey::new(b"k".to_vec(), SequenceNumber::new(7), OpType::Delete);
        let bytes = key.encode_to_disk();
        // type = 0, seq = 7, tag = 0x07_00 = 0x000700
        // little-endian: 00 07 00 00 00 00 00 00
        assert_eq!(bytes, b"k\x00\x07\x00\x00\x00\x00\x00\x00");
    }

    #[test]
    fn round_trip_decode() {
        let original = InternalKey::new(b"abc".to_vec(), SequenceNumber::new(99), OpType::Put);
        let bytes = original.encode_to_disk();
        let decoded = InternalKey::decode_from_disk(&bytes).unwrap();
        assert_eq!(decoded.user_key(), original.user_key());
        assert_eq!(decoded.sequence(), original.sequence());
        assert_eq!(decoded.op_type(), original.op_type());
    }

    #[test]
    fn decode_too_short_returns_err() {
        // Less than 8 bytes for the tag.
        let bytes = b"k\x01\x02\x03";
        assert!(InternalKey::decode_from_disk(bytes).is_err());
    }

    #[test]
    fn decode_unknown_op_type_returns_err() {
        // user_key="k", tag bytes = 99(type) || 0...0 — type 99 not valid.
        let bytes = b"k\x63\x00\x00\x00\x00\x00\x00\x00";
        assert!(InternalKey::decode_from_disk(bytes).is_err());
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p forst-rs-common --lib disk_format_tests
```
Expected: FAIL with `no method named encode_to_disk` and `no function named decode_from_disk`.

- [ ] **Step 3: Implement encode_to_disk**

In the `impl InternalKey` block (around line 337+), add:
```rust
/// Encodes this key in the RocksDB on-disk format:
/// `user_key || tag(8 bytes little-endian)` where `tag = (seq << 8) | type`.
/// This format is byte-compatible with RocksDB's InternalKey, so tools like
/// `sst_dump` decode forst-rs SSTs unchanged.
pub fn encode_to_disk(&self) -> Vec<u8> {
    let mut out = Vec::with_capacity(self.user_key.len() + 8);
    out.extend_from_slice(&self.user_key);
    let tag: u64 = (self.sequence.0 << 8) | (self.op_type as u8 as u64);
    out.extend_from_slice(&tag.to_le_bytes());
    out
}

/// Inverse of [`encode_to_disk`]. Returns Err on inputs shorter than 8 bytes
/// or on unknown op_type ordinals.
pub fn decode_from_disk(bytes: &[u8]) -> crate::ForstResult<Self> {
    if bytes.len() < 8 {
        return Err(crate::ForstError::corruption(format!(
            "InternalKey on-disk decode: input length {} < 8 (tag size)",
            bytes.len()
        )));
    }
    let split = bytes.len() - 8;
    let user_key = bytes[..split].to_vec();
    let tag_bytes: [u8; 8] = bytes[split..].try_into().unwrap();
    let tag = u64::from_le_bytes(tag_bytes);
    let type_byte = (tag & 0xFF) as u8;
    let seq = SequenceNumber::new(tag >> 8);
    let op_type = OpType::try_from_u8_checked(type_byte)?;
    Ok(InternalKey::new(user_key, seq, op_type))
}
```

- [ ] **Step 4: Run tests to verify pass**

```bash
cargo test -p forst-rs-common --lib disk_format_tests
```
Expected: 5 passed.

- [ ] **Step 5: Run full workspace to catch downstream impact**

```bash
cargo test --workspace -q
```
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add crates/forst-rs-common/src/types.rs
git commit -m "$(cat <<'EOF'
feat(engine): InternalKey RocksDB-compatible disk encode/decode

Adds `encode_to_disk` (user_key || tag-LE-8bytes) and `decode_from_disk`
methods on InternalKey. Byte-compatible with RocksDB dbformat.h so
sst_dump --command=scan reads forst-rs SSTs unchanged.

5 tests: byte layout (Put + Delete), round-trip, short-input err,
unknown-type err.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.3: Memtable + SST writers use new InternalKey encoding

**Files:**
- Modify: `crates/forst-rs-engine/src/memtable.rs` (or wherever InternalKey is encoded for storage)
- Modify: `crates/forst-rs-storage/src/sst/builder.rs` (or equivalent)
- Test: existing memtable/SST tests should still pass after the format swap

- [ ] **Step 1: Locate current InternalKey encoding sites**

```bash
grep -rn "encode_internal_key\|to_bytes\|.user_key\(\).iter\|sequence\.0 <<" crates/forst-rs-engine/src crates/forst-rs-storage/src
```
Expected: a list of call sites in memtable + SST writer paths. Capture them for review.

- [ ] **Step 2: Refactor each site to call `InternalKey::encode_to_disk()`**

For each match found in Step 1, replace ad-hoc encoding with:
```rust
let bytes = internal_key.encode_to_disk();
```
Likewise replace decode paths with `InternalKey::decode_from_disk(&bytes)?`.

- [ ] **Step 3: Run engine + storage tests**

```bash
cargo test -p forst-rs-engine -p forst-rs-storage -q
```
Expected: all pass. If a test fails on byte comparison, that test was hardcoding the old (wrong) layout — update it to expect the RocksDB-compat layout.

- [ ] **Step 4: Run full workspace**

```bash
cargo test --workspace -q
```
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/forst-rs-engine/src/ crates/forst-rs-storage/src/
git commit -m "$(cat <<'EOF'
refactor(engine,storage): route InternalKey encoding through encode_to_disk

Memtable + SST writers now call InternalKey::encode_to_disk() instead of
inlining the tag-packing logic. Single source of truth for the on-disk
byte layout.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.4: SnapshotRegistry — capture/release/min_active

**Files:**
- Create: `crates/forst-rs-engine/src/mvcc/mod.rs`
- Create: `crates/forst-rs-engine/src/mvcc/snapshot.rs`
- Modify: `crates/forst-rs-engine/src/lib.rs` (add `pub mod mvcc; pub use mvcc::{Snapshot, SnapshotRegistry};`)

- [ ] **Step 1: Write failing tests**

Create `crates/forst-rs-engine/src/mvcc/snapshot.rs`:
```rust
//! Snapshot type and SnapshotRegistry.
//!
//! See spec §6a.2 — Snapshot carries (seq, db_id, captured_at, registry-arc),
//! Drop releases the registry entry, registry tracks min_active.

use forst_rs_common::SequenceNumber;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Opaque DB identifier so a snapshot can be bound to a single DB instance.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DbId(pub u64);

#[derive(Debug)]
pub struct Snapshot {
    seq: SequenceNumber,
    db_id: DbId,
    captured_at: Instant,
    registry: Arc<SnapshotRegistry>,
}

impl Snapshot {
    pub fn seq(&self) -> SequenceNumber { self.seq }
    pub fn db_id(&self) -> DbId { self.db_id }
    pub fn age_ms(&self) -> u64 { self.captured_at.elapsed().as_millis() as u64 }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.registry.release_internal(self.seq);
    }
}

#[derive(Debug, Default)]
pub struct SnapshotRegistry {
    active: Mutex<BTreeMap<u64, AtomicUsize>>,
    cached_min: AtomicU64,
}

impl SnapshotRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            active: Mutex::new(BTreeMap::new()),
            cached_min: AtomicU64::new(u64::MAX),
        })
    }

    /// Captures a snapshot at `current_seq`. Increments the ref count for that seq.
    pub fn capture(self: &Arc<Self>, db_id: DbId, current_seq: SequenceNumber) -> Snapshot {
        let mut g = self.active.lock().unwrap();
        let entry = g.entry(current_seq.0).or_insert_with(|| AtomicUsize::new(0));
        entry.fetch_add(1, Ordering::AcqRel);
        let new_min = *g.keys().next().unwrap();
        drop(g);
        self.cached_min.store(new_min, Ordering::Release);
        Snapshot {
            seq: current_seq,
            db_id,
            captured_at: Instant::now(),
            registry: self.clone(),
        }
    }

    fn release_internal(&self, seq: SequenceNumber) {
        let mut g = self.active.lock().unwrap();
        if let Some(rc) = g.get(&seq.0) {
            if rc.fetch_sub(1, Ordering::AcqRel) == 1 {
                g.remove(&seq.0);
            }
        }
        let new_min = g.keys().next().copied().unwrap_or(u64::MAX);
        drop(g);
        self.cached_min.store(new_min, Ordering::Release);
    }

    /// Minimum live snapshot seq, or SequenceNumber::MAX if no snapshots active.
    /// Hot path: reads cached value, no lock.
    pub fn min_active(&self) -> SequenceNumber {
        SequenceNumber::new(self.cached_min.load(Ordering::Acquire))
    }

    pub fn active_count(&self) -> usize {
        let g = self.active.lock().unwrap();
        g.values().map(|rc| rc.load(Ordering::Acquire)).sum()
    }

    pub fn oldest_age_ms(&self) -> u64 {
        // Approximation: walk active and find oldest via captured_at.
        // For v1 we approximate by storing capture time on each Snapshot;
        // registry-side oldest_age requires per-entry timestamps which we
        // don't store. Instead, cradle this via a separate atomic updated
        // on capture. Acceptable for v1 since exact precision isn't needed.
        // See task 0.5 for the time-tracking integration.
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db_id() -> DbId { DbId(1) }

    #[test]
    fn capture_increments_count() {
        let reg = SnapshotRegistry::new();
        let s1 = reg.capture(db_id(), SequenceNumber::new(10));
        assert_eq!(reg.active_count(), 1);
        let _s2 = reg.capture(db_id(), SequenceNumber::new(20));
        assert_eq!(reg.active_count(), 2);
        drop(s1);
        assert_eq!(reg.active_count(), 1);
    }

    #[test]
    fn min_active_tracks_lowest() {
        let reg = SnapshotRegistry::new();
        assert_eq!(reg.min_active().0, u64::MAX);
        let s1 = reg.capture(db_id(), SequenceNumber::new(50));
        assert_eq!(reg.min_active().0, 50);
        let s2 = reg.capture(db_id(), SequenceNumber::new(20));
        assert_eq!(reg.min_active().0, 20);
        drop(s2);
        assert_eq!(reg.min_active().0, 50);
        drop(s1);
        assert_eq!(reg.min_active().0, u64::MAX);
    }

    #[test]
    fn snapshot_carries_db_id() {
        let reg = SnapshotRegistry::new();
        let s = reg.capture(DbId(42), SequenceNumber::new(1));
        assert_eq!(s.db_id(), DbId(42));
    }

    #[test]
    fn snapshot_age_advances() {
        let reg = SnapshotRegistry::new();
        let s = reg.capture(db_id(), SequenceNumber::new(1));
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(s.age_ms() >= 20);
    }

    #[test]
    fn duplicate_seq_refcounted() {
        let reg = SnapshotRegistry::new();
        let s1 = reg.capture(db_id(), SequenceNumber::new(7));
        let s2 = reg.capture(db_id(), SequenceNumber::new(7));
        assert_eq!(reg.active_count(), 2);
        drop(s1);
        assert_eq!(reg.min_active().0, 7);
        drop(s2);
        assert_eq!(reg.min_active().0, u64::MAX);
    }
}
```

Create `crates/forst-rs-engine/src/mvcc/mod.rs`:
```rust
//! MVCC subsystem — see spec §6a.

pub mod snapshot;

pub use snapshot::{DbId, Snapshot, SnapshotRegistry};
```

Edit `crates/forst-rs-engine/src/lib.rs` to add:
```rust
pub mod mvcc;
pub use mvcc::{DbId, Snapshot, SnapshotRegistry};
```

- [ ] **Step 2: Run tests to verify**

```bash
cargo test -p forst-rs-engine --lib mvcc::snapshot
```
Expected: 5 passed.

- [ ] **Step 3: Commit**

```bash
git add crates/forst-rs-engine/src/mvcc/ crates/forst-rs-engine/src/lib.rs
git commit -m "$(cat <<'EOF'
feat(engine): MVCC SnapshotRegistry + Snapshot type

Per spec §6a.2: Snapshot carries (seq, db_id, captured_at, registry-arc),
Drop releases via registry. SnapshotRegistry tracks min_active in a
BTreeMap with cached_min atomic for hot-path compaction reads.

5 unit tests: capture refcount, min_active tracking, db_id carry, age
advance, duplicate-seq refcount.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.5: Track oldest_age_ms in the registry

**Files:**
- Modify: `crates/forst-rs-engine/src/mvcc/snapshot.rs` (replace stub `oldest_age_ms`)

- [ ] **Step 1: Write the failing test**

Append to the existing tests module in `snapshot.rs`:
```rust
#[test]
fn oldest_age_ms_tracks_oldest() {
    let reg = SnapshotRegistry::new();
    assert_eq!(reg.oldest_age_ms(), 0);
    let s1 = reg.capture(db_id(), SequenceNumber::new(1));
    std::thread::sleep(std::time::Duration::from_millis(30));
    let s2 = reg.capture(db_id(), SequenceNumber::new(2));
    let age = reg.oldest_age_ms();
    assert!(age >= 30, "oldest_age_ms = {}", age);
    drop(s1);
    // After dropping s1, oldest is s2 which is much younger.
    let age2 = reg.oldest_age_ms();
    assert!(age2 < age);
    drop(s2);
    assert_eq!(reg.oldest_age_ms(), 0);
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p forst-rs-engine --lib mvcc::snapshot::tests::oldest_age_ms_tracks_oldest
```
Expected: FAIL — current stub returns 0.

- [ ] **Step 3: Implement by tracking captured_at per active entry**

Replace the `SnapshotRegistry` struct's `active` field type and the related impls:
```rust
#[derive(Debug, Default)]
pub struct SnapshotRegistry {
    // (seq, ref_count, oldest_capture_at_for_that_seq)
    active: Mutex<BTreeMap<u64, RegistryEntry>>,
    cached_min: AtomicU64,
}

#[derive(Debug)]
struct RegistryEntry {
    ref_count: AtomicUsize,
    captured_at: Instant,
}
```

Update `capture` to insert with `captured_at = Instant::now()` (only on first insert, not on increment):
```rust
pub fn capture(self: &Arc<Self>, db_id: DbId, current_seq: SequenceNumber) -> Snapshot {
    let mut g = self.active.lock().unwrap();
    g.entry(current_seq.0)
        .or_insert_with(|| RegistryEntry {
            ref_count: AtomicUsize::new(0),
            captured_at: Instant::now(),
        })
        .ref_count
        .fetch_add(1, Ordering::AcqRel);
    let new_min = *g.keys().next().unwrap();
    drop(g);
    self.cached_min.store(new_min, Ordering::Release);
    Snapshot {
        seq: current_seq,
        db_id,
        captured_at: Instant::now(),
        registry: self.clone(),
    }
}
```

Update `release_internal`:
```rust
fn release_internal(&self, seq: SequenceNumber) {
    let mut g = self.active.lock().unwrap();
    if let Some(entry) = g.get(&seq.0) {
        if entry.ref_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            g.remove(&seq.0);
        }
    }
    let new_min = g.keys().next().copied().unwrap_or(u64::MAX);
    drop(g);
    self.cached_min.store(new_min, Ordering::Release);
}
```

Update `active_count`:
```rust
pub fn active_count(&self) -> usize {
    let g = self.active.lock().unwrap();
    g.values().map(|e| e.ref_count.load(Ordering::Acquire)).sum()
}
```

Implement `oldest_age_ms`:
```rust
pub fn oldest_age_ms(&self) -> u64 {
    let g = self.active.lock().unwrap();
    g.values()
        .map(|e| e.captured_at.elapsed().as_millis() as u64)
        .max()
        .unwrap_or(0)
}
```

- [ ] **Step 4: Run all snapshot tests**

```bash
cargo test -p forst-rs-engine --lib mvcc::snapshot
```
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/forst-rs-engine/src/mvcc/snapshot.rs
git commit -m "$(cat <<'EOF'
feat(engine): track oldest_age_ms in SnapshotRegistry

Per spec §6a.3 normative: forst.snapshot.oldest_age_ms metric needs
real per-entry capture timestamps. RegistryEntry now carries
captured_at; oldest_age_ms walks active and returns max elapsed.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.6: pinned_bytes metric (estimator)

**Files:**
- Modify: `crates/forst-rs-engine/src/mvcc/snapshot.rs` (add `pinned_bytes_estimate` accepting a callback)

- [ ] **Step 1: Write the failing test**

Append to tests:
```rust
#[test]
fn pinned_bytes_estimate_returns_zero_when_no_snapshots() {
    let reg = SnapshotRegistry::new();
    let estimate = reg.pinned_bytes_estimate(|_min_seq| 0);
    assert_eq!(estimate, 0);
}

#[test]
fn pinned_bytes_estimate_invokes_callback_with_min() {
    let reg = SnapshotRegistry::new();
    let _s = reg.capture(db_id(), SequenceNumber::new(42));
    let received = std::sync::Mutex::new(SequenceNumber::new(0));
    let _ = reg.pinned_bytes_estimate(|min| {
        *received.lock().unwrap() = min;
        12345
    });
    assert_eq!(received.lock().unwrap().0, 42);
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p forst-rs-engine --lib mvcc::snapshot::tests::pinned_bytes
```
Expected: FAIL — method doesn't exist.

- [ ] **Step 3: Implement**

Add to `impl SnapshotRegistry`:
```rust
/// Returns an estimate of bytes retained because of active snapshots.
/// The closure is invoked with the current min_active seq and is expected
/// to return the bytes pinned by versions with seq >= min_active that
/// have a newer version superseding them. Returns 0 when no snapshots.
///
/// Implementation note: the registry doesn't know the data layout, so
/// the actual byte estimation is delegated to the caller (memtable +
/// SST scan). Per spec §6a.3, this is the metric `forst.snapshot.pinned_bytes`.
pub fn pinned_bytes_estimate<F: FnOnce(SequenceNumber) -> u64>(&self, walker: F) -> u64 {
    let min = self.min_active();
    if min.0 == u64::MAX {
        return 0;
    }
    walker(min)
}
```

- [ ] **Step 4: Run tests**

```bash
cargo test -p forst-rs-engine --lib mvcc::snapshot
```
Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add crates/forst-rs-engine/src/mvcc/snapshot.rs
git commit -m "$(cat <<'EOF'
feat(engine): SnapshotRegistry::pinned_bytes_estimate hook

Per spec §6a.3: forst.snapshot.pinned_bytes metric. Registry exposes
the min_active seq via a callback so the actual byte estimation
(memtable + SST scan) can live in the data layer where the layout
is known. Returns 0 when no snapshots active.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.7: Versioned reader — get_at

**Files:**
- Create: `crates/forst-rs-engine/src/mvcc/reader.rs`
- Modify: `crates/forst-rs-engine/src/mvcc/mod.rs` (add `pub mod reader;`)

- [ ] **Step 1: Write the failing test (TDD red)**

Create `crates/forst-rs-engine/src/mvcc/reader.rs`:
```rust
//! Versioned read paths — see spec §6a.5.
//!
//! `get_at(snapshot, user_key)` returns the latest version with seq <= snapshot.seq,
//! Ok(None) if no version exists at snapshot time or if latest is a deletion tombstone.

use crate::mvcc::Snapshot;
use forst_rs_common::{InternalKey, OpType, SequenceNumber};

/// In-memory or SST entry abstraction for the reader.
pub struct VersionedEntry<'a> {
    pub key: &'a InternalKey,
    pub value: &'a [u8],
}

/// Reads the latest version of `user_key` with seq <= snapshot.seq from a
/// pre-sorted (user_key ASC, sequence DESC) iterator over candidate entries.
/// Returns the value (Some) if found and not a Delete tombstone; None otherwise.
pub fn get_at<'a, I: Iterator<Item = VersionedEntry<'a>>>(
    snapshot: &Snapshot,
    user_key: &[u8],
    candidates: I,
) -> Option<&'a [u8]> {
    for entry in candidates {
        if entry.key.user_key() != user_key {
            // Past our user_key (sort order: user_key ASC).
            return None;
        }
        if entry.key.sequence().0 > snapshot.seq().0 {
            // This version is newer than the snapshot — skip.
            continue;
        }
        // First entry with seq <= snapshot.seq for this user_key wins.
        return match entry.key.op_type() {
            OpType::Delete | OpType::SingleDelete => None,
            OpType::Put => Some(entry.value),
            OpType::Merge => None, // Merge not implemented in v1.
        };
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::{DbId, SnapshotRegistry};

    fn ik(uk: &[u8], seq: u64, op: OpType) -> InternalKey {
        InternalKey::new(uk.to_vec(), SequenceNumber::new(seq), op)
    }

    #[test]
    fn returns_latest_version_at_or_before_snapshot() {
        let reg = SnapshotRegistry::new();
        let snap = reg.capture(DbId(1), SequenceNumber::new(10));
        let keys = [
            ik(b"k", 12, OpType::Put), // newer than snap; skip
            ik(b"k", 8, OpType::Put),  // hit
            ik(b"k", 5, OpType::Put),  // older
        ];
        let vals = [b"v12".as_slice(), b"v8".as_slice(), b"v5".as_slice()];
        let candidates = keys.iter().zip(vals.iter()).map(|(k, v)| VersionedEntry { key: k, value: v });
        let got = get_at(&snap, b"k", candidates);
        assert_eq!(got, Some(b"v8".as_slice()));
    }

    #[test]
    fn skips_to_other_user_key_returns_none() {
        let reg = SnapshotRegistry::new();
        let snap = reg.capture(DbId(1), SequenceNumber::new(10));
        let keys = [ik(b"other", 5, OpType::Put)];
        let vals = [b"v".as_slice()];
        let candidates = keys.iter().zip(vals.iter()).map(|(k, v)| VersionedEntry { key: k, value: v });
        assert_eq!(get_at(&snap, b"k", candidates), None);
    }

    #[test]
    fn delete_tombstone_returns_none() {
        let reg = SnapshotRegistry::new();
        let snap = reg.capture(DbId(1), SequenceNumber::new(10));
        let keys = [ik(b"k", 8, OpType::Delete), ik(b"k", 5, OpType::Put)];
        let vals = [b"".as_slice(), b"v5".as_slice()];
        let candidates = keys.iter().zip(vals.iter()).map(|(k, v)| VersionedEntry { key: k, value: v });
        // Latest visible (seq 8) is a Delete — return None even though older Put exists.
        assert_eq!(get_at(&snap, b"k", candidates), None);
    }

    #[test]
    fn no_version_visible_returns_none() {
        let reg = SnapshotRegistry::new();
        let snap = reg.capture(DbId(1), SequenceNumber::new(3));
        let keys = [ik(b"k", 5, OpType::Put)]; // newer than snap
        let vals = [b"v5".as_slice()];
        let candidates = keys.iter().zip(vals.iter()).map(|(k, v)| VersionedEntry { key: k, value: v });
        assert_eq!(get_at(&snap, b"k", candidates), None);
    }
}
```

Add to `crates/forst-rs-engine/src/mvcc/mod.rs`:
```rust
pub mod reader;
pub use reader::{get_at, VersionedEntry};
```

- [ ] **Step 2: Run to verify pass (test was written with implementation)**

```bash
cargo test -p forst-rs-engine --lib mvcc::reader
```
Expected: 4 passed.

- [ ] **Step 3: Commit**

```bash
git add crates/forst-rs-engine/src/mvcc/
git commit -m "$(cat <<'EOF'
feat(engine): MVCC versioned reader (get_at)

Per spec §6a.5: get_at returns latest version with seq <= snapshot.seq;
Ok(None) on tombstone or no visible version. Generic over a candidate
iterator so memtable + SST sources can both feed in.

4 unit tests cover: latest-at-snapshot hit, other-key returns None,
delete-tombstone returns None, no-visible-version returns None.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.8: Compaction policy — should_drop

**Files:**
- Create: `crates/forst-rs-engine/src/mvcc/compaction_policy.rs`
- Modify: `crates/forst-rs-engine/src/mvcc/mod.rs` (re-export)

- [ ] **Step 1: Write failing tests**

Create `crates/forst-rs-engine/src/mvcc/compaction_policy.rs`:
```rust
//! Compaction policy for MVCC retention — see spec §6a.5.
//!
//! `should_drop(entry, newer_version_exists, min_active_snapshot)`:
//!   * Returns false if the entry is needed by an active snapshot.
//!   * Returns true only when entry.seq < min_active AND a newer version
//!     for the same user_key exists (which serves all snapshots seq >= entry.seq+1).

use forst_rs_common::{OpType, SequenceNumber};

pub fn should_drop(
    entry_seq: SequenceNumber,
    entry_op: OpType,
    newer_version_exists: bool,
    min_active_snapshot: SequenceNumber,
) -> bool {
    // Keep if any active snapshot might need this version.
    if entry_seq.0 >= min_active_snapshot.0 {
        return false;
    }
    // Below min_active: drop only if newer version is visible to all current snapshots.
    // For tombstones, additional rule: keep the latest tombstone if it's the
    // last visible thing (ensures readers see "deleted" rather than "missing").
    match entry_op {
        OpType::Delete | OpType::SingleDelete => {
            // Drop tombstone only if there's a newer non-tombstone version
            // (in which case the newer version overrides the deletion semantically).
            // For pure-tombstone tail, never drop.
            newer_version_exists
        }
        OpType::Put | OpType::Merge => newer_version_exists,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_when_at_or_above_min_active() {
        // entry seq == min_active: keep (snapshot at min_active needs this version).
        assert!(!should_drop(
            SequenceNumber::new(10),
            OpType::Put,
            true,
            SequenceNumber::new(10),
        ));
        // entry seq > min_active: keep.
        assert!(!should_drop(
            SequenceNumber::new(15),
            OpType::Put,
            true,
            SequenceNumber::new(10),
        ));
    }

    #[test]
    fn drop_below_min_active_when_newer_exists() {
        assert!(should_drop(
            SequenceNumber::new(5),
            OpType::Put,
            true,
            SequenceNumber::new(10),
        ));
    }

    #[test]
    fn keep_below_min_active_when_no_newer() {
        // Bottom of the version chain — no newer version means this is what
        // future restored snapshots would see. Keep.
        assert!(!should_drop(
            SequenceNumber::new(5),
            OpType::Put,
            false,
            SequenceNumber::new(10),
        ));
    }

    #[test]
    fn keep_tombstone_at_tail() {
        // Tombstone with no newer version: keep so readers see "deleted".
        assert!(!should_drop(
            SequenceNumber::new(5),
            OpType::Delete,
            false,
            SequenceNumber::new(10),
        ));
    }

    #[test]
    fn drop_tombstone_when_newer_put_exists() {
        // Newer Put overrides the deletion semantically; tombstone is dead weight.
        assert!(should_drop(
            SequenceNumber::new(5),
            OpType::Delete,
            true,
            SequenceNumber::new(10),
        ));
    }
}
```

Add to `crates/forst-rs-engine/src/mvcc/mod.rs`:
```rust
pub mod compaction_policy;
pub use compaction_policy::should_drop;
```

- [ ] **Step 2: Run tests**

```bash
cargo test -p forst-rs-engine --lib mvcc::compaction_policy
```
Expected: 5 passed.

- [ ] **Step 3: Property test for compaction policy correctness**

Add `proptest = "1"` already in workspace deps (verify). Append to `compaction_policy.rs`:
```rust
#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// For any (entry_seq, min_active, op) combo, the dropped-entry
        /// invariant holds: if we drop, then there's a newer version OR
        /// we're below min_active. Property test catches edge cases the
        /// table-test above might miss.
        #[test]
        fn drop_implies_safe_to_drop(
            entry_seq in 0u64..1000,
            min_active in 0u64..1000,
            newer_exists: bool,
            op_ord in 0u8..2,
        ) {
            let op = match op_ord { 0 => OpType::Delete, _ => OpType::Put };
            let dropped = should_drop(
                SequenceNumber::new(entry_seq),
                op,
                newer_exists,
                SequenceNumber::new(min_active),
            );
            if dropped {
                prop_assert!(entry_seq < min_active);
                prop_assert!(newer_exists);
            }
        }
    }
}
```

- [ ] **Step 4: Run prop tests**

```bash
cargo test -p forst-rs-engine --lib mvcc::compaction_policy
```
Expected: 6 passed (5 unit + 1 property).

- [ ] **Step 5: Commit**

```bash
git add crates/forst-rs-engine/src/mvcc/
git commit -m "$(cat <<'EOF'
feat(engine): MVCC compaction policy (should_drop)

Per spec §6a.5: drop versions with seq < min_active when a newer
version exists; keep tombstones at the tail of the version chain
(so readers see "deleted" not "missing").

5 table tests + 1 proptest property: drop implies (below_min AND
newer_exists). CI gate per spec §14.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.9: Wire MVCC into compaction worker

**Files:**
- Modify: `crates/forst-rs-engine/src/compaction.rs` (call `mvcc::should_drop` per entry)
- Modify: `crates/forst-rs-engine/src/db.rs` (DbImpl owns SnapshotRegistry; expose get_snapshot_registry)

- [ ] **Step 1: Locate compaction loop entry-by-entry filtering site**

```bash
grep -n "fn compact_l0_to_l1\|fn run_compaction\|for.*entry.*in.*iter\|emit\|write_entry" crates/forst-rs-engine/src/compaction.rs | head
```
Expected: a site where each (key, value) pair is decided to keep or drop. Capture the function name.

- [ ] **Step 2: Add SnapshotRegistry field to DbImpl**

In `crates/forst-rs-engine/src/db.rs`, find the `DbImpl` struct and add:
```rust
pub(crate) snapshot_registry: Arc<crate::mvcc::SnapshotRegistry>,
```
Initialize in `DbImpl::new` / `open`:
```rust
snapshot_registry: crate::mvcc::SnapshotRegistry::new(),
```
Add a getter:
```rust
pub fn snapshot_registry(&self) -> &Arc<crate::mvcc::SnapshotRegistry> {
    &self.snapshot_registry
}
```

- [ ] **Step 3: Pass min_active into the compaction loop**

In the compaction function identified in Step 1, before the entry loop, capture:
```rust
let min_active = self.snapshot_registry.min_active();
```
For each (key, value) the loop processes, replace the existing keep/drop logic with:
```rust
let newer_exists = /* existing logic that detects whether a newer version of
                     entry.user_key was already emitted in this compaction pass */;
if crate::mvcc::should_drop(entry.sequence(), entry.op_type(), newer_exists, min_active) {
    continue; // drop
}
emit(entry, value);
```

- [ ] **Step 4: Write integration test**

Create `crates/forst-rs-engine/tests/mvcc_compaction_it.rs`:
```rust
//! Integration test: snapshot pins versions across compaction.

use forst_rs_engine::DbImpl;
use forst_rs_engine::mvcc::DbId;
use forst_rs_common::SequenceNumber;
use tempfile::TempDir;

#[test]
fn snapshot_pinned_versions_survive_compaction() {
    let dir = TempDir::new().unwrap();
    let db = DbImpl::open_local(dir.path()).unwrap();

    // Write 100 versions of "k".
    for i in 0..100u64 {
        db.put(b"k", format!("v{}", i).as_bytes()).unwrap();
    }

    // Capture snapshot at current seq.
    let snap = db.snapshot_registry().capture(DbId(0), db.current_sequence());

    // Write 100 more versions (these should NOT be visible to snap).
    for i in 100..200u64 {
        db.put(b"k", format!("v{}", i).as_bytes()).unwrap();
    }

    // Force flush + compaction.
    db.flush().unwrap();
    db.compact_range(None, None).unwrap();

    // Read at snap: should still see v99 (last write before snap).
    let candidates = db.iter_versions(b"k").unwrap();
    let got = forst_rs_engine::mvcc::get_at(&snap, b"k", candidates);
    assert_eq!(got, Some(b"v99".as_slice()));

    drop(snap);

    // After release, next compaction can drop the pinned versions.
    db.compact_range(None, None).unwrap();
    let live_files = db.list_live_files(false).unwrap();
    // Just assert compaction completed without error; precise SST count is
    // unstable across runs.
    assert!(!live_files.is_empty());
}
```

- [ ] **Step 5: Run integration test**

```bash
cargo test -p forst-rs-engine --test mvcc_compaction_it
```
Expected: PASS. (If `iter_versions` doesn't exist, add it as a thin wrapper over the existing iterator that yields `VersionedEntry` items.)

- [ ] **Step 6: Run full workspace**

```bash
cargo test --workspace -q
```
Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add crates/forst-rs-engine/src/ crates/forst-rs-engine/tests/mvcc_compaction_it.rs
git commit -m "$(cat <<'EOF'
feat(engine): wire MVCC SnapshotRegistry into compaction worker

DbImpl owns the SnapshotRegistry; compaction loop reads min_active
once per pass and consults mvcc::should_drop per entry. Snapshot
pinning prevents premature version reclamation.

Integration test: write 100 versions, snapshot, write 100 more,
flush+compact — verify reader-at-snapshot still sees v99.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.10: DB-level snapshot/get_at/release API

**Files:**
- Modify: `crates/forst-rs-engine/src/db.rs` (add `snapshot()`, `release_snapshot()`, `get_at()`, `iter_at()` methods)

- [ ] **Step 1: Write failing test**

Append to existing `db.rs` test module (or create one):
```rust
#[cfg(test)]
mod mvcc_db_api_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn snapshot_and_get_at_round_trip() {
        let dir = TempDir::new().unwrap();
        let db = DbImpl::open_local(dir.path()).unwrap();
        db.put(b"k", b"v1").unwrap();
        let snap = db.snapshot();
        db.put(b"k", b"v2").unwrap();
        // snap sees v1, current sees v2.
        assert_eq!(db.get_at(&snap, b"k").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
        db.release_snapshot(snap);
    }

    #[test]
    fn snapshot_release_drops_registry_entry() {
        let dir = TempDir::new().unwrap();
        let db = DbImpl::open_local(dir.path()).unwrap();
        let snap = db.snapshot();
        assert_eq!(db.snapshot_registry().active_count(), 1);
        db.release_snapshot(snap);
        assert_eq!(db.snapshot_registry().active_count(), 0);
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cargo test -p forst-rs-engine --lib mvcc_db_api_tests
```
Expected: FAIL — methods don't exist.

- [ ] **Step 3: Implement DB-level API**

In `crates/forst-rs-engine/src/db.rs` add to `impl DbImpl`:
```rust
/// Captures a snapshot at the current sequence number. The caller must
/// eventually call `release_snapshot` (or drop the Snapshot) to allow
/// compaction to reclaim pinned versions.
pub fn snapshot(&self) -> crate::mvcc::Snapshot {
    let seq = self.current_sequence();
    let db_id = self.db_id();
    self.snapshot_registry.capture(db_id, seq)
}

/// Idempotent on a snapshot already dropped (the snapshot's Drop calls
/// release; this method is for callers who prefer the explicit form).
pub fn release_snapshot(&self, snapshot: crate::mvcc::Snapshot) {
    drop(snapshot);
}

/// Reads the latest version of `key` with seq <= snapshot.seq.
/// Returns Ok(None) if no version exists at snapshot time or if the
/// latest visible version is a deletion tombstone.
pub fn get_at(
    &self,
    snapshot: &crate::mvcc::Snapshot,
    key: &[u8],
) -> Result<Option<Vec<u8>>, crate::ForstError> {
    if snapshot.db_id() != self.db_id() {
        return Err(crate::ForstError::invalid_argument(
            "Snapshot was issued by a different DbImpl instance",
        ));
    }
    let candidates = self.iter_versions(key)?;
    Ok(crate::mvcc::get_at(snapshot, key, candidates).map(|s| s.to_vec()))
}
```

Add `db_id()` method to `DbImpl` (returns a stored DbId field; initialize in `open` from a counter or hash of path).

- [ ] **Step 4: Run tests**

```bash
cargo test -p forst-rs-engine --lib mvcc_db_api_tests
```
Expected: 2 passed.

- [ ] **Step 5: Run full workspace**

```bash
cargo test --workspace -q
```
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add crates/forst-rs-engine/src/db.rs
git commit -m "$(cat <<'EOF'
feat(engine): DbImpl::{snapshot, release_snapshot, get_at}

Top-level engine MVCC API; checks snapshot.db_id matches issuing DB
and returns InvalidArgument on mismatch (per spec §10.0 ABI lifetime
contract for the FFI exports that wrap these).

2 unit tests: snapshot+get_at sees pre-snapshot state while current
sees post-snapshot; release drops registry ref.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.11: Verify with sst_dump (CI gate per §6a.1)

**Files:**
- Create: `crates/forst-rs-engine/tests/sst_dump_compat.rs`

- [ ] **Step 1: Verify sst_dump availability locally**

```bash
which sst_dump || echo "MISSING: install via 'brew install rocksdb' or apt 'rocksdb-tools'"
```
If MISSING, install. CI will install via apt step (added in P0 final task).

- [ ] **Step 2: Write the sst_dump compat test**

Create `crates/forst-rs-engine/tests/sst_dump_compat.rs`:
```rust
//! CI gate per spec §6a.1 — sst_dump must read forst-rs SSTs unchanged.

use forst_rs_engine::DbImpl;
use std::process::Command;
use tempfile::TempDir;

#[test]
#[ignore] // Run explicitly: cargo test --test sst_dump_compat -- --ignored
fn sst_dump_decodes_forst_rs_sst() {
    if Command::new("sst_dump").arg("--help").output().is_err() {
        eprintln!("SKIPPED: sst_dump not installed");
        return;
    }
    let dir = TempDir::new().unwrap();
    let db = DbImpl::open_local(dir.path()).unwrap();
    db.put(b"alpha", b"value-a").unwrap();
    db.put(b"beta", b"value-b").unwrap();
    db.put(b"gamma", b"value-c").unwrap();
    db.flush().unwrap();

    // Find the SST file produced by the flush.
    let sst_files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().map(|x| x == "sst").unwrap_or(false))
        .collect();
    assert!(!sst_files.is_empty(), "no .sst file produced by flush");

    let sst_path = sst_files[0].path();
    let output = Command::new("sst_dump")
        .arg("--command=scan")
        .arg(format!("--file={}", sst_path.display()))
        .output()
        .expect("sst_dump invocation failed");
    let stdout = String::from_utf8_lossy(&output.stdout);

    // sst_dump prints lines like: "'alpha' @ 1: 1 => value-a"
    // (key, seq, type-numeric, value). Type 1 = kTypeValue (Put).
    assert!(stdout.contains("'alpha'"), "stdout: {}", stdout);
    assert!(stdout.contains("'beta'"), "stdout: {}", stdout);
    assert!(stdout.contains("'gamma'"), "stdout: {}", stdout);
    assert!(stdout.contains(": 1 => value-a"), "stdout: {}", stdout);
}
```

- [ ] **Step 3: Run locally to verify (skip if sst_dump missing)**

```bash
cargo test --test sst_dump_compat -- --ignored --nocapture
```
Expected: PASS (or SKIPPED message if sst_dump missing).

- [ ] **Step 4: Add CI step to install sst_dump and run the test**

Edit `.github/workflows/ci-rust.yml`. Locate the `test` job and append a new step before the `cargo test` step:
```yaml
      - name: Install RocksDB tools (sst_dump)
        run: sudo apt-get update && sudo apt-get install -y rocksdb-tools
```
And after `cargo test --workspace`, add:
```yaml
      - name: sst_dump byte-compat gate (§6a.1)
        run: cargo test --test sst_dump_compat -p forst-rs-engine -- --ignored
```

- [ ] **Step 5: Commit**

```bash
git add crates/forst-rs-engine/tests/sst_dump_compat.rs .github/workflows/ci-rust.yml
git commit -m "$(cat <<'EOF'
test(engine): sst_dump byte-compat CI gate (§6a.1)

CI installs rocksdb-tools and runs an ignored test that flushes a forst-rs
DB to SST then invokes `sst_dump --command=scan` against it, asserting
the keys + values + type byte are decoded as expected. Prevents future
refactors from silently breaking the RocksDB byte-compat invariant.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 0.12: Push P0 branch

- [ ] **Step 1: Push branch and open PR (manual via gh)**

```bash
git push origin b-prod-p0-engine-mvcc 2>&1 | tail -3
gh pr create --title "B-Prod-P0: engine MVCC subsystem" --body "$(cat <<'EOF'
## Summary

- RocksDB byte-compat InternalKey on-disk encoding (OpType ordinal swap + encode_to_disk/decode_from_disk)
- SnapshotRegistry with capture/release/min_active/oldest_age_ms/pinned_bytes_estimate
- Versioned reader (get_at) honoring snapshot.seq
- Compaction policy (should_drop) wired into the compaction worker
- DB-level API: snapshot(), release_snapshot(), get_at()
- sst_dump byte-compat CI gate (§6a.1)

Lays foundations for the rest of B-Prod (P2 needs this).

## Test plan

- [ ] cargo test --workspace passes
- [ ] cargo clippy --workspace --all-targets -- -D warnings clean
- [ ] cargo fmt --check clean
- [ ] sst_dump CI gate green (apt install rocksdb-tools + the ignored test)
- [ ] mvcc_compaction_it.rs integration test passes (snapshot pins versions)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

# PR B-Prod-P1: KeyGroup encoding + AbstractKeyedStateBackend skeleton + CfRouter

**Goal**: ship the Flink-side composite-key encoding (`kg(2 bytes BE) || serialize(K) || / || stateName || /`), the `CfRouter` interface with both impls, and the `AbstractKeyedStateBackend<K>` skeleton (constructor, lifecycle, getters; snapshot/restore stubbed for P3/P4).

**Branch**: `b-prod-p1-keygroup-cfrouter` off `forst-rs-jdk25`.

**Effort**: 4-5 days. **Depends on**: nothing (parallel with P0).

### Task 1.1: ForStRsKeyGroupedSerializer composite encoding

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyGroupedSerializer.java`
- Test: `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyGroupedSerializerTest.java`

- [ ] **Step 1: Write failing tests**

Create `ForStRsKeyGroupedSerializerTest.java`:
```java
package org.apache.flink.state.forstrs.keyed;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ForStRsKeyGroupedSerializerTest {

    @Test
    void encodesKeyGroupAsBigEndian2Bytes() {
        ForStRsKeyGroupedSerializer<String> ser = new ForStRsKeyGroupedSerializer<>(
                org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        byte[] composite = ser.encodeForState(258, "k", "myState");
        // kg=258 = 0x0102 BE => 0x01, 0x02
        assertEquals((byte) 0x01, composite[0]);
        assertEquals((byte) 0x02, composite[1]);
    }

    @Test
    void encodesValueStateRoundTrip() {
        ForStRsKeyGroupedSerializer<String> ser = new ForStRsKeyGroupedSerializer<>(
                org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        byte[] composite = ser.encodeForState(7, "userKey", "valueState");
        ForStRsKeyGroupedSerializer.Decoded<String> decoded = ser.decode(composite);
        assertEquals(7, decoded.keyGroup());
        assertEquals("userKey", decoded.userKey());
        assertEquals("valueState", decoded.stateName());
    }

    @Test
    void encodesMapStateWithUserKey() {
        ForStRsKeyGroupedSerializer<String> ser = new ForStRsKeyGroupedSerializer<>(
                org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        byte[] composite = ser.encodeForMap(7, "userKey", "mapState",
                org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE,
                "userMapKey");
        // Should start with the same 7-byte kg-prefix + userKey + /mapState/
        ForStRsKeyGroupedSerializer.Decoded<String> decoded = ser.decode(composite);
        assertEquals(7, decoded.keyGroup());
        assertEquals("userKey", decoded.userKey());
        assertEquals("mapState", decoded.stateName());
    }

    @Test
    void prefixForKeyGroupYieldsScanRange() {
        ForStRsKeyGroupedSerializer<String> ser = new ForStRsKeyGroupedSerializer<>(
                org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        byte[] prefix = ser.keyGroupPrefix(7);
        assertArrayEquals(new byte[] {0x00, 0x07}, prefix);
    }

    @Test
    void prefixForKeyGroupAndStateYieldsScanRange() {
        ForStRsKeyGroupedSerializer<String> ser = new ForStRsKeyGroupedSerializer<>(
                org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE);
        byte[] prefix = ser.keyGroupAndStatePrefix(7, "myState");
        // 0x00 0x07 + serialize("any") + ... — actually for a per-state prefix
        // we don't need the userKey portion; just encode kg + a state-name
        // marker. The scan range is whatever encodeForState would produce
        // for kg=7, stateName="myState", before the trailing terminator.
        // For this test we just assert the prefix STARTS with the kg bytes.
        assertEquals((byte) 0x00, prefix[0]);
        assertEquals((byte) 0x07, prefix[1]);
    }
}
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cd /Users/lijunqing/Code/stczwd/flink && JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Drat.skip=true -Dtest=ForStRsKeyGroupedSerializerTest -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Dargline="--enable-native-access=ALL-UNNAMED -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib" 2>&1 | tail -10
```
Expected: COMPILATION ERROR — class doesn't exist.

- [ ] **Step 3: Implement ForStRsKeyGroupedSerializer**

Create the file:
```java
package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Composite key encoder/decoder per spec §6.
 *
 * <p>Layout (Value/List/Reducing/Aggregating):
 * <pre>
 * composite = kg(2B BE) || serialize(K) || '/' || stateName.bytes || '/'
 * </pre>
 *
 * <p>Layout (Map):
 * <pre>
 * composite = kg(2B BE) || serialize(K) || '/' || stateName.bytes || '/' || serialize(UK)
 * </pre>
 */
public final class ForStRsKeyGroupedSerializer<K> {

    private static final byte SEP = (byte) '/';

    private final TypeSerializer<K> keySerializer;

    public ForStRsKeyGroupedSerializer(TypeSerializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    public byte[] encodeForState(int keyGroup, K userKey, String stateName) {
        validateKeyGroup(keyGroup);
        DataOutputSerializer out = new DataOutputSerializer(64);
        try {
            out.writeShort(keyGroup); // 2 bytes BE per Flink convention
            keySerializer.serialize(userKey, out);
            out.write(SEP);
            byte[] sn = stateName.getBytes(StandardCharsets.UTF_8);
            out.write(sn);
            out.write(SEP);
        } catch (IOException e) {
            throw new RuntimeException("encodeForState failed: " + e.getMessage(), e);
        }
        return out.getCopyOfBuffer();
    }

    public <UK> byte[] encodeForMap(
            int keyGroup,
            K userKey,
            String stateName,
            TypeSerializer<UK> userKeySerializer,
            UK userMapKey) {
        validateKeyGroup(keyGroup);
        DataOutputSerializer out = new DataOutputSerializer(64);
        try {
            out.writeShort(keyGroup);
            keySerializer.serialize(userKey, out);
            out.write(SEP);
            byte[] sn = stateName.getBytes(StandardCharsets.UTF_8);
            out.write(sn);
            out.write(SEP);
            userKeySerializer.serialize(userMapKey, out);
        } catch (IOException e) {
            throw new RuntimeException("encodeForMap failed: " + e.getMessage(), e);
        }
        return out.getCopyOfBuffer();
    }

    public byte[] keyGroupPrefix(int keyGroup) {
        validateKeyGroup(keyGroup);
        return new byte[] {(byte) ((keyGroup >>> 8) & 0xFF), (byte) (keyGroup & 0xFF)};
    }

    public byte[] keyGroupAndStatePrefix(int keyGroup, String stateName) {
        // For per-state-per-keygroup scans we need the kg prefix; the state name
        // discriminator inside the composite key follows the user-key portion,
        // so a "state-only" prefix isn't a clean byte prefix unless the user-key
        // serialization is fixed-length. For variable-length user keys we just
        // use the kg prefix and post-filter by state name during iteration.
        // This method exists for the fixed-length case (tests cover the kg
        // portion only).
        return keyGroupPrefix(keyGroup);
    }

    public Decoded<K> decode(byte[] composite) {
        if (composite.length < 4) {
            throw new IllegalArgumentException("composite too short: " + composite.length);
        }
        int keyGroup = ((composite[0] & 0xFF) << 8) | (composite[1] & 0xFF);
        DataInputDeserializer in = new DataInputDeserializer();
        in.setBuffer(composite, 2, composite.length - 2);
        K userKey;
        try {
            userKey = keySerializer.deserialize(in);
        } catch (IOException e) {
            throw new RuntimeException("decode userKey failed: " + e.getMessage(), e);
        }
        // After userKey we expect /stateName/ — find the LAST occurrence of /
        // to be robust to map-state UK suffix.
        int afterUserKey = 2 + (composite.length - 2 - in.available());
        // Find first / after afterUserKey.
        int firstSlash = -1;
        for (int i = afterUserKey; i < composite.length; i++) {
            if (composite[i] == SEP) { firstSlash = i; break; }
        }
        if (firstSlash < 0) throw new IllegalArgumentException("no separator after userKey");
        // Find last / in the composite (handles map-state UK).
        int lastSlash = -1;
        for (int i = composite.length - 1; i > firstSlash; i--) {
            if (composite[i] == SEP) { lastSlash = i; break; }
        }
        if (lastSlash < 0) lastSlash = firstSlash; // value-state (no UK) — only one /
        // For value-state the composite ends with the second /, so stateName is
        // between firstSlash+1 and lastSlash. For map-state, lastSlash is the
        // separator before UK; same range.
        int stateStart = firstSlash + 1;
        int stateEnd = lastSlash;
        String stateName = new String(composite, stateStart, stateEnd - stateStart, StandardCharsets.UTF_8);
        return new Decoded<>(keyGroup, userKey, stateName);
    }

    private static void validateKeyGroup(int keyGroup) {
        if (keyGroup < 0 || keyGroup > 0xFFFF) {
            throw new IllegalArgumentException("keyGroup out of [0, 65535]: " + keyGroup);
        }
    }

    public static final class Decoded<K> {
        private final int keyGroup;
        private final K userKey;
        private final String stateName;

        Decoded(int keyGroup, K userKey, String stateName) {
            this.keyGroup = keyGroup;
            this.userKey = userKey;
            this.stateName = stateName;
        }

        public int keyGroup() { return keyGroup; }
        public K userKey() { return userKey; }
        public String stateName() { return stateName; }
    }
}
```

- [ ] **Step 4: Run tests to verify pass**

Same mvn command as Step 2; expected 5 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/flink && git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyGroupedSerializer.java flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyGroupedSerializerTest.java
git commit -m "$(cat <<'EOF'
feat(state-forst-rs-keyed): ForStRsKeyGroupedSerializer composite encoding

Per spec §6: kg(2B BE) || serialize(K) || '/' || stateName || '/' for
Value/List/Reducing/Aggregating; +serialize(UK) suffix for Map.

decode() uses last-marker scan to disambiguate map-state UK suffix from
value-state's single separator (per spec §6 collision-constraint mitigation).

5 unit tests: BE encoding boundary, value+map round-trip, kg prefix,
kg+state prefix.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 1.2: CfRouter interface + SingleCfRouter

**Files:**
- Create: `.../keyed/cf/CfRouter.java`
- Create: `.../keyed/cf/SingleCfRouter.java`
- Test: `.../keyed/cf/SingleCfRouterTest.java`

- [ ] **Step 1: Write failing test**

Create `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/cf/SingleCfRouterTest.java`:
```java
package org.apache.flink.state.forstrs.keyed.cf;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleCfRouterTest {

    @Test
    void allStatesShareDefaultCf() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

                SingleCfRouter router = new SingleCfRouter(cf);

                FrsCfHandle a = router.getCfForState("stateA");
                FrsCfHandle b = router.getCfForState("stateB");
                assertSame(cf, a);
                assertSame(cf, b);

                Collection<FrsCfHandle> all = router.allCfs();
                assertEquals(1, all.size());

                assertTrue(router.isSingleCf());
            }
        }
    }
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd /Users/lijunqing/Code/stczwd/flink && JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Drat.skip=true -Dtest=SingleCfRouterTest -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Dargline="--enable-native-access=ALL-UNNAMED -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib"
```
Expected: COMPILATION ERROR.

- [ ] **Step 3: Implement CfRouter interface**

Create `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/cf/CfRouter.java`:
```java
package org.apache.flink.state.forstrs.keyed.cf;

import org.apache.flink.state.forstrs.ffm.FrsCfHandle;

import java.io.Closeable;
import java.util.Collection;

/**
 * Routes a Flink keyed-state name to a {@link FrsCfHandle}. Implementations decide
 * whether all states share one CF ({@link SingleCfRouter}) or each state gets its own
 * ({@link PerStateCfRouter}). Per spec §7.
 */
public interface CfRouter extends Closeable {

    FrsCfHandle getCfForState(String stateName);

    Collection<FrsCfHandle> allCfs();

    String stateNameForCf(FrsCfHandle cf);

    boolean isSingleCf();

    @Override
    void close();
}
```

Create `SingleCfRouter.java`:
```java
package org.apache.flink.state.forstrs.keyed.cf;

import org.apache.flink.state.forstrs.ffm.FrsCfHandle;

import java.util.Collections;
import java.util.Collection;

public final class SingleCfRouter implements CfRouter {

    public static final String SHARED_CF_NAME = "default";

    private final FrsCfHandle cf;

    public SingleCfRouter(FrsCfHandle cf) {
        this.cf = cf;
    }

    @Override
    public FrsCfHandle getCfForState(String stateName) {
        return cf;
    }

    @Override
    public Collection<FrsCfHandle> allCfs() {
        return Collections.singletonList(cf);
    }

    @Override
    public String stateNameForCf(FrsCfHandle cf) {
        return SHARED_CF_NAME;
    }

    @Override
    public boolean isSingleCf() {
        return true;
    }

    @Override
    public void close() {
        // The default CF is owned by the backend, not by us.
    }
}
```

- [ ] **Step 4: Run tests to verify**

Same mvn command as Step 2; expected 1 passed.

- [ ] **Step 5: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/cf/ flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/cf/
git commit -m "$(cat <<'EOF'
feat(state-forst-rs-keyed): CfRouter interface + SingleCfRouter

Per spec §7: CfRouter routes stateName -> FrsCfHandle. SingleCfRouter
returns the same default CF for every state name (default mode,
lowest engine overhead). PerStateCfRouter follows in next task.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 1.3: PerStateCfRouter

**Files:**
- Create: `.../keyed/cf/PerStateCfRouter.java`
- Test: `.../keyed/cf/PerStateCfRouterTest.java`

- [ ] **Step 1: Write failing test**

```java
package org.apache.flink.state.forstrs.keyed.cf;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class PerStateCfRouterTest {

    @Test
    void distinctStatesGetDistinctCfs() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                try (PerStateCfRouter router = new PerStateCfRouter(linker, db, arena)) {
                    FrsCfHandle a = router.getCfForState("stateA");
                    FrsCfHandle b = router.getCfForState("stateB");
                    assertNotSame(a, b);
                    assertEquals(2, router.allCfs().size());
                    assertEquals("stateA", router.stateNameForCf(a));
                    assertEquals("stateB", router.stateNameForCf(b));
                    assertEquals(false, router.isSingleCf());
                }
            }
        }
    }

    @Test
    void repeatedGetReturnsSameCf() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                try (PerStateCfRouter router = new PerStateCfRouter(linker, db, arena)) {
                    FrsCfHandle first = router.getCfForState("s");
                    FrsCfHandle second = router.getCfForState("s");
                    assertEquals(first, second);
                    assertEquals(1, router.allCfs().size());
                }
            }
        }
    }
}
```

- [ ] **Step 2: Implement PerStateCfRouter**

Create:
```java
package org.apache.flink.state.forstrs.keyed.cf;

import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;

import java.lang.foreign.Arena;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public final class PerStateCfRouter implements CfRouter {

    private static final int SOFT_LIMIT_CFS = 256;

    private final ForStRsLinker linker;
    private final FrsDb db;
    private final Arena arena;
    private final Map<String, FrsCfHandle> stateToCf = new LinkedHashMap<>();
    private final Map<FrsCfHandle, String> cfToState = new LinkedHashMap<>();

    public PerStateCfRouter(ForStRsLinker linker, FrsDb db, Arena arena) {
        this.linker = linker;
        this.db = db;
        this.arena = arena;
    }

    @Override
    public synchronized FrsCfHandle getCfForState(String stateName) {
        FrsCfHandle existing = stateToCf.get(stateName);
        if (existing != null) {
            return existing;
        }
        if (stateToCf.size() >= SOFT_LIMIT_CFS) {
            throw new IllegalStateException(
                    "PerStateCfRouter exceeded soft limit of " + SOFT_LIMIT_CFS
                            + " CFs (state=" + stateName + "). "
                            + "Switch to cf.mode=single or reduce state count.");
        }
        FrsCfHandle cf = linker.dbCreateCf(db, stateName, arena);
        stateToCf.put(stateName, cf);
        cfToState.put(cf, stateName);
        return cf;
    }

    @Override
    public synchronized Collection<FrsCfHandle> allCfs() {
        return new java.util.ArrayList<>(stateToCf.values());
    }

    @Override
    public synchronized String stateNameForCf(FrsCfHandle cf) {
        String name = cfToState.get(cf);
        if (name == null) {
            throw new IllegalArgumentException("Unknown CF passed to stateNameForCf");
        }
        return name;
    }

    @Override
    public boolean isSingleCf() {
        return false;
    }

    @Override
    public synchronized void close() {
        for (FrsCfHandle cf : stateToCf.values()) {
            cf.close();
        }
        stateToCf.clear();
        cfToState.clear();
    }
}
```

- [ ] **Step 3: Run tests**

Same mvn pattern with `-Dtest=PerStateCfRouterTest`; expected 2 passed.

- [ ] **Step 4: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/cf/PerStateCfRouter.java flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/keyed/cf/PerStateCfRouterTest.java
git commit -m "$(cat <<'EOF'
feat(state-forst-rs-keyed): PerStateCfRouter

Per spec §7: lazily creates one CF per state name on first getCfForState
call. Enforces soft limit of 256 CFs (per spec §11 error handling row);
throws IllegalStateException with explicit guidance to switch to
cf.mode=single.

2 unit tests: distinct states get distinct CFs (with stateNameForCf
inverse), repeated get is idempotent.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 1.4: ForStRsOptions config plumbing for cf.mode

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsOptions.java`

- [ ] **Step 1: Add cf.mode field + parser**

Append to `ForStRsOptions.java`:
```java
public enum CfMode {
    SINGLE("single"),
    PER_STATE("per-state");

    private final String configValue;

    CfMode(String configValue) { this.configValue = configValue; }

    public static CfMode fromConfig(String value) {
        if (value == null || value.isEmpty()) return SINGLE;
        for (CfMode m : values()) {
            if (m.configValue.equals(value)) return m;
        }
        throw new IllegalArgumentException(
                "Unknown state.backend.forst-rs.cf.mode: '" + value
                        + "' (expected: single | per-state)");
    }
}

// Add as a field on the existing ForStRsOptions class:
//   private CfMode cfMode = CfMode.SINGLE;
//   public CfMode cfMode() { return cfMode; }
//   public ForStRsOptions cfMode(CfMode m) { this.cfMode = m; return this; }
```

- [ ] **Step 2: Add a unit test for the parser**

Create `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ForStRsOptionsTest.java`:
```java
package org.apache.flink.state.forstrs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ForStRsOptionsTest {
    @Test
    void cfModeDefaultsSingle() {
        assertEquals(ForStRsOptions.CfMode.SINGLE, ForStRsOptions.CfMode.fromConfig(null));
        assertEquals(ForStRsOptions.CfMode.SINGLE, ForStRsOptions.CfMode.fromConfig(""));
    }

    @Test
    void cfModeParsesPerState() {
        assertEquals(ForStRsOptions.CfMode.PER_STATE, ForStRsOptions.CfMode.fromConfig("per-state"));
    }

    @Test
    void cfModeRejectsUnknown() {
        assertThrows(IllegalArgumentException.class,
                () -> ForStRsOptions.CfMode.fromConfig("multi"));
    }
}
```

- [ ] **Step 3: Run tests; expect pass**

```bash
cd /Users/lijunqing/Code/stczwd/flink && JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Drat.skip=true -Dtest=ForStRsOptionsTest -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Dargline="--enable-native-access=ALL-UNNAMED -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib" 2>&1 | tail -8
```
Expected: 3 passed.

- [ ] **Step 4: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsOptions.java flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ForStRsOptionsTest.java
git commit -m "$(cat <<'EOF'
feat(state-forst-rs): ForStRsOptions.CfMode config

Per spec §7: state.backend.forst-rs.cf.mode = single | per-state,
default single. ForStRsKeyedStateBackendBuilder will read this flag
to construct the right CfRouter (next task).

3 unit tests cover default + parse + reject-unknown.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 1.5: ForStRsKeyedStateBackendBuilder skeleton

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackendBuilder.java`

- [ ] **Step 1: Write the builder + minimal test**

Create:
```java
package org.apache.flink.state.forstrs.keyed;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.state.forstrs.ForStRsOptions;
import org.apache.flink.state.forstrs.ffm.ForStRsLinker;
import org.apache.flink.state.forstrs.ffm.FrsCfHandle;
import org.apache.flink.state.forstrs.ffm.FrsDb;
import org.apache.flink.state.forstrs.keyed.cf.CfRouter;
import org.apache.flink.state.forstrs.keyed.cf.PerStateCfRouter;
import org.apache.flink.state.forstrs.keyed.cf.SingleCfRouter;

import java.lang.foreign.Arena;
import java.util.Objects;

public final class ForStRsKeyedStateBackendBuilder<K> {

    private final ForStRsLinker linker;
    private final Arena arena;
    private final TypeSerializer<K> keySerializer;
    private final ForStRsOptions options;

    private FrsDb db;
    private FrsCfHandle defaultCf;

    public ForStRsKeyedStateBackendBuilder(
            ForStRsLinker linker,
            Arena arena,
            TypeSerializer<K> keySerializer,
            ForStRsOptions options) {
        this.linker = Objects.requireNonNull(linker);
        this.arena = Objects.requireNonNull(arena);
        this.keySerializer = Objects.requireNonNull(keySerializer);
        this.options = Objects.requireNonNull(options);
    }

    public ForStRsKeyedStateBackendBuilder<K> withDb(FrsDb db, FrsCfHandle defaultCf) {
        this.db = db;
        this.defaultCf = defaultCf;
        return this;
    }

    public CfRouter buildCfRouter() {
        if (db == null || defaultCf == null) {
            throw new IllegalStateException("withDb(db, defaultCf) must be called first");
        }
        return switch (options.cfMode()) {
            case SINGLE -> new SingleCfRouter(defaultCf);
            case PER_STATE -> new PerStateCfRouter(linker, db, arena);
        };
    }
}
```

- [ ] **Step 2: Add a builder test**

Append to existing `ForStRsOptionsTest.java` (or create `ForStRsKeyedStateBackendBuilderTest.java`):
```java
@Test
void buildsCorrectRouterFromCfMode() {
    try (java.lang.foreign.Arena arena = java.lang.foreign.Arena.ofShared()) {
        org.apache.flink.state.forstrs.ffm.ForStRsLinker linker = new org.apache.flink.state.forstrs.ffm.ForStRsLinker(arena);
        try (org.apache.flink.state.forstrs.ffm.FrsDb db = linker.dbOpenMemory(arena);
                org.apache.flink.state.forstrs.ffm.FrsCfHandle cf = linker.dbDefaultCf(db, arena)) {

            org.apache.flink.state.forstrs.ForStRsOptions optsSingle = new org.apache.flink.state.forstrs.ForStRsOptions()
                    .cfMode(org.apache.flink.state.forstrs.ForStRsOptions.CfMode.SINGLE);
            org.apache.flink.state.forstrs.keyed.cf.CfRouter rs = new org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackendBuilder<String>(
                    linker, arena, org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE, optsSingle)
                    .withDb(db, cf).buildCfRouter();
            org.junit.jupiter.api.Assertions.assertTrue(rs.isSingleCf());

            org.apache.flink.state.forstrs.ForStRsOptions optsPer = new org.apache.flink.state.forstrs.ForStRsOptions()
                    .cfMode(org.apache.flink.state.forstrs.ForStRsOptions.CfMode.PER_STATE);
            org.apache.flink.state.forstrs.keyed.cf.CfRouter rp = new org.apache.flink.state.forstrs.keyed.ForStRsKeyedStateBackendBuilder<String>(
                    linker, arena, org.apache.flink.api.common.typeutils.base.StringSerializer.INSTANCE, optsPer)
                    .withDb(db, cf).buildCfRouter();
            org.junit.jupiter.api.Assertions.assertFalse(rp.isSingleCf());
            rp.close();
        }
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cd /Users/lijunqing/Code/stczwd/flink && JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Drat.skip=true -Dtest=ForStRsOptionsTest -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Dargline="--enable-native-access=ALL-UNNAMED -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib" 2>&1 | tail -8
```
Expected: 4 passed (3 prior + 1 new).

- [ ] **Step 4: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackendBuilder.java flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ForStRsOptionsTest.java
git commit -m "$(cat <<'EOF'
feat(state-forst-rs-keyed): ForStRsKeyedStateBackendBuilder

Constructs a CfRouter (Single or PerState) based on ForStRsOptions.cfMode().
Builder pattern lets future tasks add more knobs (snapshotStrategy,
restoreOperation, etc.) without ctor explosion.

Test verifies router type matches cf.mode config.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 1.6: Migrate state classes to use kg-prefixed encoding

**Files:**
- Modify: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsValueState.java`
- Modify: `.../state/ForStRsListState.java`
- Modify: `.../state/ForStRsMapState.java`
- Modify: `.../state/ForStRsReducingState.java`
- Modify: `.../state/ForStRsAggregatingState.java`

- [ ] **Step 1: Locate the current `"k/"` encoding sites**

```bash
cd /Users/lijunqing/Code/stczwd/flink && grep -n "\"k/\"\|KEYED_NS_MARKER\|composite\b" flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/*.java flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java | head -30
```
Capture all sites that build composite keys today.

- [ ] **Step 2: Refactor each state class to take `ForStRsKeyGroupedSerializer` + currentKeyGroup`**

For each state class, change the constructor to accept `(linker, db, cf, ser, currentKeyGroupSupplier, stateName)`. Replace the inline encoding with:
```java
byte[] composite = ser.encodeForState(currentKeyGroupSupplier.getAsInt(), backend.getCurrentKey(), stateName);
```
For `ForStRsMapState`, use `encodeForMap` with a userKey serializer field.

- [ ] **Step 3: Update existing state-class tests**

Most existing tests in `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/*Test.java` will need updating to pass the new constructor params. Run them to find failures:
```bash
cd /Users/lijunqing/Code/stczwd/flink && JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Drat.skip=true -Dtest='ForStRs*StateTest' -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Dargline="--enable-native-access=ALL-UNNAMED -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib" 2>&1 | tail -15
```
Update test fixtures to construct the state with the new (ser, kgSupplier, stateName) params.

- [ ] **Step 4: Verify all state-class tests pass**

Same mvn command; expected: all green.

- [ ] **Step 5: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/state/
git commit -m "$(cat <<'EOF'
refactor(state-forst-rs): state classes use ForStRsKeyGroupedSerializer

Per spec §6: composite key prefix changes from "k/" to kg(2B BE).
All 5 state classes (Value/List/Map/Reducing/Aggregating) now route
through ForStRsKeyGroupedSerializer.encodeForState (or encodeForMap).

Existing per-state tests updated to pass the new constructor params.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 1.7: ForStRsKeyedStateBackend extends AbstractKeyedStateBackend (skeleton)

**Files:**
- Modify (rewrite): `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java`

- [ ] **Step 1: Rewrite class signature**

Change the class header from:
```java
public class ForStRsKeyedStateBackend<K> implements Closeable {
```
to:
```java
public class ForStRsKeyedStateBackend<K> extends org.apache.flink.runtime.state.AbstractKeyedStateBackend<K> {
```

Add the AbstractKeyedStateBackend constructor invocation. The Flink superclass constructor signature in 2.2.0 is:
```java
protected AbstractKeyedStateBackend(
    KvStateRegistry kvStateRegistry,
    TypeSerializer<K> keySerializer,
    ClassLoader userCodeClassLoader,
    ExecutionConfig executionConfig,
    TtlTimeProvider ttlTimeProvider,
    LatencyTrackingStateConfig latencyTrackingStateConfig,
    StreamCompressionDecorator keyGroupCompressionDecorator,
    InternalKeyContext<K> keyContext);
```
Reference: `org.apache.flink.runtime.state.AbstractKeyedStateBackend` in the Flink runtime jar.

Implement the abstract methods (snapshot, getKeys, getKeysAndNamespaces, etc.) as stubs that throw `UnsupportedOperationException("implemented in P3/P4")` for the snapshot path; for `getKeys` reuse the existing `keys()` implementation.

- [ ] **Step 2: Build to confirm it compiles**

```bash
cd /Users/lijunqing/Code/stczwd/flink && JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn -B -pl flink-state-backends/flink-statebackend-forst-rs install -DskipTests -Drat.skip=true 2>&1 | tail -5
```
Expected: BUILD SUCCESS.

- [ ] **Step 3: Run existing tests to confirm no regression**

```bash
cd /Users/lijunqing/Code/stczwd/flink && JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Drat.skip=true -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Dargline="--enable-native-access=ALL-UNNAMED -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib" 2>&1 | tail -10
```
Expected: all green.

- [ ] **Step 4: Commit**

```bash
git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/keyed/ForStRsKeyedStateBackend.java
git commit -m "$(cat <<'EOF'
refactor(state-forst-rs-keyed): extends AbstractKeyedStateBackend<K>

Skeleton change: class hierarchy now matches what Flink's keyed-state
SPI registries expect. snapshot(checkpointId, ts, factory, options)
returns UnsupportedOperationException stub — implementation lands in P3.

All existing tests still pass (state classes weren't using inheritance-
sensitive methods).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 1.8: Push P1 branch + open PR

- [ ] **Step 1: Push and PR**

```bash
cd /Users/lijunqing/Code/stczwd/flink && git push origin b-prod-p1-keygroup-cfrouter 2>&1 | tail -3
gh pr create --title "B-Prod-P1: KeyGroup encoding + CfRouter + AbstractKeyedStateBackend skeleton" --body "$(cat <<'EOF'
## Summary
- ForStRsKeyGroupedSerializer (kg(2B BE) || ser(K) || / || stateName || / [|| ser(UK)])
- CfRouter interface + SingleCfRouter + PerStateCfRouter
- ForStRsOptions.CfMode config (single | per-state)
- ForStRsKeyedStateBackendBuilder
- 5 state classes migrated to kg-prefixed encoding
- ForStRsKeyedStateBackend now extends AbstractKeyedStateBackend (snapshot stubbed)

## Test plan
- [ ] All existing state-class tests pass
- [ ] New ForStRsKeyGroupedSerializerTest, SingleCfRouterTest, PerStateCfRouterTest, ForStRsOptionsTest green
- [ ] mvn install -DskipTests succeeds (compiler accepts the AbstractKeyedStateBackend extension)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

# PR B-Prod-P2: New engine FFI exports + Java bindings

**Goal**: ship the 6 new FFI exports for MVCC + snapshot-aware checkpoints, plus their Java side bindings (FrsSnapshot type, ForStRsLinker methods).

**Branch**: `b-prod-p2-mvcc-ffi` off `forst-rs` (with P0 merged) and `forst-rs-jdk25` (with P1 merged) for Java work.

**Effort**: 3 days. **Depends on**: P0.

### Task 2.1: FFI export `frs_db_snapshot` + `frs_db_release_snapshot`

**Files:**
- Modify: `crates/forst-rs-ffi/src/lib.rs` (add 2 exports + FrsSnapshot opaque type + status codes)

- [ ] **Step 1: Write failing tests**

Append to `crates/forst-rs-ffi/src/lib.rs` test module:
```rust
#[test]
fn test_frs_db_snapshot_release_round_trip() {
    unsafe {
        let mut db: FrsDb = std::ptr::null_mut();
        assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
        let mut snap: FrsSnapshot = std::ptr::null_mut();
        assert_eq!(frs_db_snapshot(db, &mut snap), FRS_STATUS_OK);
        assert!(!snap.is_null());
        assert_eq!(frs_db_release_snapshot(db, snap), FRS_STATUS_OK);
        assert_eq!(frs_db_close(db), FRS_STATUS_OK);
    }
}

#[test]
fn test_frs_db_snapshot_null_db_returns_null_arg() {
    unsafe {
        let mut snap: FrsSnapshot = std::ptr::null_mut();
        assert_eq!(frs_db_snapshot(std::ptr::null_mut(), &mut snap), FRS_STATUS_NULL_ARG);
    }
}

#[test]
fn test_frs_db_release_snapshot_null_arg_returns_null_arg() {
    unsafe {
        let mut db: FrsDb = std::ptr::null_mut();
        assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
        // NULL snapshot.
        assert_eq!(frs_db_release_snapshot(db, std::ptr::null_mut()), FRS_STATUS_NULL_ARG);
        assert_eq!(frs_db_close(db), FRS_STATUS_OK);
    }
}
```

- [ ] **Step 2: Implement the exports**

Add near other db exports:
```rust
/// Opaque snapshot handle. Created by frs_db_snapshot, released by
/// frs_db_release_snapshot. Lifetime constraints per spec §10.0.
pub type FrsSnapshot = *mut crate::ffi_internal::SnapshotBox;

mod ffi_internal {
    pub struct SnapshotBox {
        pub inner: forst_rs_engine::Snapshot,
    }
}

#[no_mangle]
pub unsafe extern "C" fn frs_db_snapshot(
    db: FrsDb,
    out_snapshot: *mut FrsSnapshot,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        if out_snapshot.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let snap = db.snapshot();
        let boxed = Box::new(ffi_internal::SnapshotBox { inner: snap });
        *out_snapshot = Box::into_raw(boxed);
        FRS_STATUS_OK
    })
}

#[no_mangle]
pub unsafe extern "C" fn frs_db_release_snapshot(
    db: FrsDb,
    snapshot: FrsSnapshot,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        if snapshot.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let boxed = Box::from_raw(snapshot);
        if boxed.inner.db_id() != db.db_id() {
            // Re-leak so caller doesn't double-free; return error.
            std::mem::forget(boxed);
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        // Drop the box (which drops the Snapshot, which calls registry.release).
        FRS_STATUS_OK
    })
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p forst-rs-ffi --lib test_frs_db_snapshot
```
Expected: 3 passed.

- [ ] **Step 4: Commit**

```bash
git add crates/forst-rs-ffi/src/lib.rs
git commit -m "$(cat <<'EOF'
feat(ffi): frs_db_snapshot + frs_db_release_snapshot

Per spec §10a + §10.0 ABI lifetime contract: opaque FrsSnapshot handle,
captured against issuing db, released-against-other-db returns
INVALID_ARGUMENT (no double-free).

3 unit tests: round-trip, null db arg, null snapshot arg.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 2.2: FFI export `frs_get_at`

**Files:**
- Modify: `crates/forst-rs-ffi/src/lib.rs`

- [ ] **Step 1: Write failing tests**

Append:
```rust
#[test]
fn test_frs_get_at_isolation() {
    unsafe {
        let mut db: FrsDb = std::ptr::null_mut();
        assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
        let mut cf: FrsCfHandle = std::ptr::null_mut();
        assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

        let key = b"k";
        assert_eq!(frs_put(db, cf, key.as_ptr(), key.len(), b"v1".as_ptr(), 2), FRS_STATUS_OK);

        let mut snap: FrsSnapshot = std::ptr::null_mut();
        assert_eq!(frs_db_snapshot(db, &mut snap), FRS_STATUS_OK);

        // Write v2 AFTER snapshot.
        assert_eq!(frs_put(db, cf, key.as_ptr(), key.len(), b"v2".as_ptr(), 2), FRS_STATUS_OK);

        let mut out: FrsBytes = FrsBytes::default();
        assert_eq!(frs_get_at(db, cf, snap, key.as_ptr(), key.len(), &mut out), FRS_STATUS_OK);
        let val = std::slice::from_raw_parts(out.data, out.len as usize);
        assert_eq!(val, b"v1");
        frs_bytes_free(&mut out);

        // Current get returns v2.
        assert_eq!(frs_get(db, cf, key.as_ptr(), key.len(), &mut out), FRS_STATUS_OK);
        let val = std::slice::from_raw_parts(out.data, out.len as usize);
        assert_eq!(val, b"v2");
        frs_bytes_free(&mut out);

        assert_eq!(frs_db_release_snapshot(db, snap), FRS_STATUS_OK);
        assert_eq!(frs_db_close(db), FRS_STATUS_OK);
    }
}
```

- [ ] **Step 2: Implement**

Add:
```rust
#[no_mangle]
pub unsafe extern "C" fn frs_get_at(
    db: FrsDb,
    cf: FrsCfHandle,
    snapshot: FrsSnapshot,
    key: *const u8,
    key_len: usize,
    out_value: *mut FrsBytes,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else { return FRS_STATUS_NULL_ARG; };
        let Some(_cf) = cf_ref(&cf) else { return FRS_STATUS_NULL_ARG; };
        if snapshot.is_null() || key.is_null() || out_value.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let snap_ref = &(*snapshot).inner;
        if snap_ref.db_id() != db.db_id() {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let key_slice = std::slice::from_raw_parts(key, key_len);
        match db.get_at(snap_ref, key_slice) {
            Ok(Some(value)) => {
                let frs_bytes = bytes_to_frs_bytes(value);
                std::ptr::write(out_value, frs_bytes);
                FRS_STATUS_OK
            }
            Ok(None) => FRS_STATUS_NOT_FOUND,
            Err(e) => error_to_status(&e),
        }
    })
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test -p forst-rs-ffi --lib test_frs_get_at
```
Expected: 1 passed.

- [ ] **Step 4: Commit**

```bash
git add crates/forst-rs-ffi/src/lib.rs
git commit -m "$(cat <<'EOF'
feat(ffi): frs_get_at — versioned read at snapshot.seq

Per spec §10a: returns value visible at snapshot.seq; FRS_STATUS_NOT_FOUND
for missing-or-tombstoned. Cross-DB snapshot rejected with
INVALID_ARGUMENT (per §10.0 ABI contract).

1 integration test: snapshot before second write, get_at sees first value
while normal get sees second.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 2.3: FFI export `frs_iterator_open_at`

**Files:**
- Modify: `crates/forst-rs-ffi/src/lib.rs` (add 1 export; iterator next/close already exist via the existing `frs_iterator_*` family)

- [ ] **Step 1: Write failing test**

```rust
#[test]
fn test_frs_iterator_open_at_filters_by_snapshot_seq() {
    unsafe {
        let mut db: FrsDb = std::ptr::null_mut();
        assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
        let mut cf: FrsCfHandle = std::ptr::null_mut();
        assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

        // Write 3 keys.
        for &k in &[b"a", b"b", b"c"] {
            assert_eq!(frs_put(db, cf, k.as_ptr(), 1, b"v1".as_ptr(), 2), FRS_STATUS_OK);
        }

        let mut snap: FrsSnapshot = std::ptr::null_mut();
        assert_eq!(frs_db_snapshot(db, &mut snap), FRS_STATUS_OK);

        // Add d AFTER snapshot.
        assert_eq!(frs_put(db, cf, b"d".as_ptr(), 1, b"v1".as_ptr(), 2), FRS_STATUS_OK);

        let mut iter: FrsIterator = std::ptr::null_mut();
        assert_eq!(frs_iterator_open_at(db, cf, snap, &mut iter), FRS_STATUS_OK);

        let mut count = 0;
        loop {
            let mut k = FrsBytes::default();
            let mut v = FrsBytes::default();
            let mut valid: bool = false;
            assert_eq!(frs_iterator_next(iter, &mut k, &mut v, &mut valid), FRS_STATUS_OK);
            if !valid { break; }
            count += 1;
            frs_bytes_free(&mut k);
            frs_bytes_free(&mut v);
        }
        assert_eq!(count, 3); // 'd' filtered out by snapshot.seq.

        assert_eq!(frs_iterator_close(iter), FRS_STATUS_OK);
        assert_eq!(frs_db_release_snapshot(db, snap), FRS_STATUS_OK);
        assert_eq!(frs_db_close(db), FRS_STATUS_OK);
    }
}
```

- [ ] **Step 2: Implement**

```rust
#[no_mangle]
pub unsafe extern "C" fn frs_iterator_open_at(
    db: FrsDb,
    cf: FrsCfHandle,
    snapshot: FrsSnapshot,
    out_iter: *mut FrsIterator,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else { return FRS_STATUS_NULL_ARG; };
        let Some(cf) = cf_ref(&cf) else { return FRS_STATUS_NULL_ARG; };
        if snapshot.is_null() || out_iter.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let snap_ref = &(*snapshot).inner;
        if snap_ref.db_id() != db.db_id() {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        match db.iter_at(snap_ref, cf) {
            Ok(iter) => {
                let boxed = Box::new(IteratorBox { inner: iter });
                *out_iter = Box::into_raw(boxed) as FrsIterator;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}
```

(Add `db.iter_at(snap, cf)` engine-side method as a thin wrapper that returns an iterator filtered to entries with `seq <= snap.seq`, per §6a.5 spec; for v1 it can be implemented as a wrap of the existing iterator with a filter closure that materializes the version chain per user_key.)

- [ ] **Step 3: Run test**

```bash
cargo test -p forst-rs-ffi --lib test_frs_iterator_open_at
```
Expected: 1 passed.

- [ ] **Step 4: Commit**

```bash
git add crates/forst-rs-ffi/src/lib.rs crates/forst-rs-engine/src/db.rs
git commit -m "$(cat <<'EOF'
feat(ffi,engine): frs_iterator_open_at + DbImpl::iter_at

Per spec §10a: iterator that yields the latest version of each user_key
with seq <= snapshot.seq, skipping tombstones.

1 test: 3 keys before snapshot + 1 after; iterate-at-snapshot returns 3.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 2.4: FFI exports `frs_create_incremental_checkpoint_at` + `frs_db_open_from_incremental`

**Files:**
- Modify: `crates/forst-rs-ffi/src/lib.rs` (add 2 exports + `FrsIncrementalCheckpointResult` repr(C) struct)

- [ ] **Step 1: Add the struct + exports**

Add to `lib.rs`:
```rust
#[repr(C)]
pub struct FrsIncrementalCheckpointResult {
    pub manifest_path: *mut std::os::raw::c_char,
    pub new_ssts: *mut FrsLiveFileList,
    pub shared_ssts: *mut FrsLiveFileList,
    pub flush_done_eventfd: std::os::raw::c_int,
}

#[no_mangle]
pub unsafe extern "C" fn frs_create_incremental_checkpoint_at(
    db: FrsDb,
    snapshot: FrsSnapshot,
    checkpoint_id: u64,
    base_checkpoint_id: u64,
    out: *mut FrsIncrementalCheckpointResult,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else { return FRS_STATUS_NULL_ARG; };
        if snapshot.is_null() || out.is_null() { return FRS_STATUS_NULL_ARG; }
        let snap_ref = &(*snapshot).inner;
        if snap_ref.db_id() != db.db_id() {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        match db.create_incremental_checkpoint(snap_ref, checkpoint_id, base_checkpoint_id) {
            Ok(result) => {
                let manifest_path_c = std::ffi::CString::new(result.manifest_path.to_string_lossy().to_string()).unwrap();
                let new_list = Box::into_raw(Box::new(FrsLiveFileList::from(result.new_ssts)));
                let shared_list = Box::into_raw(Box::new(FrsLiveFileList::from(result.shared_ssts)));
                std::ptr::write(out, FrsIncrementalCheckpointResult {
                    manifest_path: manifest_path_c.into_raw(),
                    new_ssts: new_list,
                    shared_ssts: shared_list,
                    flush_done_eventfd: -1, // -1 == flush is sync (no eventfd needed for v1)
                });
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn frs_db_open_from_incremental(
    target_dir: *const std::os::raw::c_char,
    base_manifest: *const std::os::raw::c_char,
    sst_files: *const *const std::os::raw::c_char,
    sst_file_count: usize,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if target_dir.is_null() || base_manifest.is_null() || out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let target = std::ffi::CStr::from_ptr(target_dir).to_string_lossy().to_string();
        let manifest = std::ffi::CStr::from_ptr(base_manifest).to_string_lossy().to_string();
        let mut paths: Vec<String> = Vec::with_capacity(sst_file_count);
        for i in 0..sst_file_count {
            let p = *sst_files.add(i);
            if p.is_null() { return FRS_STATUS_NULL_ARG; }
            paths.push(std::ffi::CStr::from_ptr(p).to_string_lossy().to_string());
        }
        match DbImpl::open_from_incremental(&target, &manifest, &paths) {
            Ok(db) => {
                let arc = std::sync::Arc::new(db);
                *out_handle = std::sync::Arc::into_raw(arc) as FrsDb;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}
```

(Engine-side: implement `DbImpl::create_incremental_checkpoint` and `DbImpl::open_from_incremental` as thin wrappers over the existing checkpoint code; ~80 LOC each.)

- [ ] **Step 2: Write a round-trip integration test**

Create `crates/forst-rs-ffi/tests/incremental_checkpoint_ffi_it.rs`:
```rust
use forst_rs_ffi::*;
use std::ffi::CString;
use std::ptr;
use tempfile::TempDir;

#[test]
fn incremental_checkpoint_round_trip() {
    unsafe {
        let dir = TempDir::new().unwrap();
        let base_path = CString::new(dir.path().to_string_lossy().to_string()).unwrap();
        let mut db: FrsDb = ptr::null_mut();
        assert_eq!(frs_db_open(base_path.as_ptr(), &mut db), FRS_STATUS_OK);
        let mut cf: FrsCfHandle = ptr::null_mut();
        assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

        for i in 0..5u32 {
            let key = format!("k{}", i);
            let val = format!("v{}", i);
            assert_eq!(frs_put(db, cf, key.as_ptr(), key.len(), val.as_ptr(), val.len()), FRS_STATUS_OK);
        }

        let mut snap: FrsSnapshot = ptr::null_mut();
        assert_eq!(frs_db_snapshot(db, &mut snap), FRS_STATUS_OK);

        let mut result = std::mem::MaybeUninit::<FrsIncrementalCheckpointResult>::uninit();
        assert_eq!(
            frs_create_incremental_checkpoint_at(db, snap, 1, 0, result.as_mut_ptr()),
            FRS_STATUS_OK
        );
        let result = result.assume_init();

        // Restore into target_dir.
        let target = TempDir::new().unwrap();
        let target_path = CString::new(target.path().to_string_lossy().to_string()).unwrap();
        // Build sst_files array from result.new_ssts.
        let new_ssts_list = &*result.new_ssts;
        let mut paths: Vec<CString> = Vec::new();
        let mut pointers: Vec<*const std::os::raw::c_char> = Vec::new();
        for i in 0..new_ssts_list.count as usize {
            let entry = &*new_ssts_list.entries.add(i);
            let path_c = std::ffi::CStr::from_ptr(entry.path).to_owned();
            paths.push(path_c);
            pointers.push(paths.last().unwrap().as_ptr());
        }

        let mut restored: FrsDb = ptr::null_mut();
        assert_eq!(
            frs_db_open_from_incremental(
                target_path.as_ptr(),
                result.manifest_path,
                pointers.as_ptr(),
                pointers.len(),
                &mut restored,
            ),
            FRS_STATUS_OK
        );

        // Verify all 5 keys readable in the restored DB.
        let mut restored_cf: FrsCfHandle = ptr::null_mut();
        assert_eq!(frs_db_default_cf(restored, &mut restored_cf), FRS_STATUS_OK);
        for i in 0..5u32 {
            let key = format!("k{}", i);
            let mut out = FrsBytes::default();
            assert_eq!(frs_get(restored, restored_cf, key.as_ptr(), key.len(), &mut out), FRS_STATUS_OK);
            let val = std::slice::from_raw_parts(out.data, out.len as usize);
            assert_eq!(val, format!("v{}", i).as_bytes());
            frs_bytes_free(&mut out);
        }

        frs_db_release_snapshot(db, snap);
        frs_db_live_file_list_free(result.new_ssts);
        frs_db_live_file_list_free(result.shared_ssts);
        let _ = CString::from_raw(result.manifest_path);
        frs_db_close(db);
        frs_db_close(restored);
    }
}
```

- [ ] **Step 3: Run + commit**

```bash
cargo test -p forst-rs-ffi --test incremental_checkpoint_ffi_it
```
Expected: 1 passed.

```bash
git add crates/forst-rs-ffi/src/lib.rs crates/forst-rs-engine/src/db.rs crates/forst-rs-ffi/tests/incremental_checkpoint_ffi_it.rs
git commit -m "$(cat <<'EOF'
feat(ffi,engine): incremental checkpoint at snapshot + restore from

Per spec §10b: frs_create_incremental_checkpoint_at takes (snapshot,
checkpoint_id, base_checkpoint_id) and returns FrsIncrementalCheckpointResult
with new_ssts/shared_ssts/manifest_path. frs_db_open_from_incremental
reconstructs a DB from base manifest + extra SST file list.

Round-trip integration test: write 5 keys -> snapshot -> incremental
checkpoint -> open_from_incremental -> read all 5 keys back.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 2.5: Java FrsSnapshot type

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/FrsSnapshot.java`

- [ ] **Step 1: Implement**

```java
package org.apache.flink.state.forstrs.ffm;

import java.lang.foreign.MemorySegment;

/**
 * Java handle wrapping a native FrsSnapshot pointer. AutoCloseable so callers can use
 * try-with-resources. Calls frs_db_release_snapshot on close. Per spec §10.0 ABI
 * contract: bound to the issuing FrsDb; double-close is safe (idempotent).
 */
public final class FrsSnapshot implements AutoCloseable {

    private final ForStRsLinker linker;
    private final FrsDb db;
    private MemorySegment handle;

    FrsSnapshot(ForStRsLinker linker, FrsDb db, MemorySegment handle) {
        this.linker = linker;
        this.db = db;
        this.handle = handle;
    }

    public MemorySegment handle() {
        if (handle == null) {
            throw new IllegalStateException("FrsSnapshot already closed");
        }
        return handle;
    }

    public boolean isClosed() { return handle == null; }

    @Override
    public void close() {
        if (handle != null) {
            linker.dbReleaseSnapshot(db, this);
            handle = null;
        }
    }
}
```

- [ ] **Step 2: Add ForStRsLinker.dbSnapshot + dbReleaseSnapshot bindings**

Edit `ForStRsLinker.java`. Add MethodHandle fields:
```java
private final MethodHandle frsDbSnapshot;
private final MethodHandle frsDbReleaseSnapshot;
```
Bind in constructor:
```java
this.frsDbSnapshot = bind("frs_db_snapshot",
        FunctionDescriptor.of(ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, // db
                ValueLayout.ADDRESS)); // out_snapshot

this.frsDbReleaseSnapshot = bind("frs_db_release_snapshot",
        FunctionDescriptor.of(ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, // db
                ValueLayout.ADDRESS)); // snapshot
```
Add public methods:
```java
public FrsSnapshot dbSnapshot(FrsDb db, Arena arena) {
    MemorySegment outHandle = arena.allocate(ValueLayout.ADDRESS);
    int rc;
    try {
        rc = (int) frsDbSnapshot.invokeExact(db.handle(), outHandle);
    } catch (Throwable t) {
        throw new FrsBackendException(FrsStatus.PANIC, "frs_db_snapshot threw: " + t.getMessage());
    }
    check(rc, "frs_db_snapshot");
    MemorySegment handle = outHandle.get(ValueLayout.ADDRESS, 0);
    return new FrsSnapshot(this, db, handle);
}

public void dbReleaseSnapshot(FrsDb db, FrsSnapshot snapshot) {
    if (snapshot == null || snapshot.isClosed()) return;
    int rc;
    try {
        rc = (int) frsDbReleaseSnapshot.invokeExact(db.handle(), snapshot.handle());
    } catch (Throwable t) {
        throw new FrsBackendException(FrsStatus.PANIC, "frs_db_release_snapshot threw: " + t.getMessage());
    }
    check(rc, "frs_db_release_snapshot");
}
```

- [ ] **Step 3: Add 4 FrsSnapshot tests**

Create `flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ffm/FrsSnapshotTest.java`:
```java
package org.apache.flink.state.forstrs.ffm;

import org.junit.jupiter.api.Test;
import java.lang.foreign.Arena;
import static org.junit.jupiter.api.Assertions.*;

class FrsSnapshotTest {

    @Test
    void snapshotRoundTrip() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                assertFalse(snap.isClosed());
                snap.close();
                assertTrue(snap.isClosed());
            }
        }
    }

    @Test
    void doubleCloseSafe() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                snap.close();
                assertDoesNotThrow(snap::close);
            }
        }
    }

    @Test
    void tryWithResourcesReleases() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena);
                    FrsSnapshot snap = linker.dbSnapshot(db, arena)) {
                assertFalse(snap.isClosed());
            }
            // After try-with-resources scope.
        }
    }

    @Test
    void handleAccessAfterCloseFails() {
        try (Arena arena = Arena.ofShared()) {
            ForStRsLinker linker = new ForStRsLinker(arena);
            try (FrsDb db = linker.dbOpenMemory(arena)) {
                FrsSnapshot snap = linker.dbSnapshot(db, arena);
                snap.close();
                assertThrows(IllegalStateException.class, snap::handle);
            }
        }
    }
}
```

- [ ] **Step 4: Rebuild cdylib + run tests**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt && cargo build --release -p forst-rs-ffi
cd /Users/lijunqing/Code/stczwd/flink && JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Drat.skip=true -Dtest=FrsSnapshotTest -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Dargline="--enable-native-access=ALL-UNNAMED -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib" 2>&1 | tail -8
```
Expected: 4 passed.

- [ ] **Step 5: Commit (in flink repo)**

```bash
cd /Users/lijunqing/Code/stczwd/flink && git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/FrsSnapshot.java flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ffm/FrsSnapshotTest.java
git commit -m "$(cat <<'EOF'
feat(state-forst-rs): FrsSnapshot + ForStRsLinker dbSnapshot/dbReleaseSnapshot

AutoCloseable wrapper over the native FrsSnapshot pointer; double-close
is idempotent; handle() throws after close. Per spec §10.0 ABI contract.

4 unit tests cover round-trip, double-close-safe, try-with-resources,
post-close handle access throws.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

### Task 2.6: ForStRsLinker bindings for get_at, iter_at, incremental checkpoint

**Files:**
- Modify: `ForStRsLinker.java` (add 4 method handles + 4 public methods)

- [ ] **Step 1: Add bindings**

In `ForStRsLinker.java`:
```java
private final MethodHandle frsGetAt;
private final MethodHandle frsIteratorOpenAt;
private final MethodHandle frsCreateIncrementalCheckpointAt;
private final MethodHandle frsDbOpenFromIncremental;
```
Bind in constructor:
```java
this.frsGetAt = bindCritical("frs_get_at",
        FunctionDescriptor.of(ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
this.frsIteratorOpenAt = bind("frs_iterator_open_at",
        FunctionDescriptor.of(ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS));
this.frsCreateIncrementalCheckpointAt = bind("frs_create_incremental_checkpoint_at",
        FunctionDescriptor.of(ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
this.frsDbOpenFromIncremental = bind("frs_db_open_from_incremental",
        FunctionDescriptor.of(ValueLayout.JAVA_INT,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));
```
Add 4 public methods following the existing patterns (see `lookupKv`, `iteratorOpen`, `createCheckpoint` for templates).

- [ ] **Step 2: Add an end-to-end test in ForStRsLinkerExtendedTest**

Append a test that does: open db, put k=v1, snapshot, put k=v2, getAt(snap, k) returns v1, normal get returns v2.

- [ ] **Step 3: Rebuild + test**

Same rebuild dance from Task 2.5; then:
```bash
cd /Users/lijunqing/Code/stczwd/flink && JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home mvn -B -pl flink-state-backends/flink-statebackend-forst-rs test -Drat.skip=true -Dtest=ForStRsLinkerExtendedTest -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib -Dargline="--enable-native-access=ALL-UNNAMED -Dforstrs.native.libpath=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib" 2>&1 | tail -8
```
Expected: all green including the new test.

- [ ] **Step 4: Commit + push P2 + PR**

```bash
cd /Users/lijunqing/Code/stczwd/flink && git add flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java flink-state-backends/flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/ffm/ForStRsLinkerExtendedTest.java
git commit -m "$(cat <<'EOF'
feat(state-forst-rs): ForStRsLinker bindings for MVCC + incremental ckpt

4 new methods: getAt, iteratorOpenAt, createIncrementalCheckpointAt,
dbOpenFromIncremental. Per spec §10a/§10b. Extended test verifies
snapshot isolation across the FFM hop.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
git push origin b-prod-p2-mvcc-ffi
gh pr create --title "B-Prod-P2: MVCC + incremental-ckpt FFI exports + Java bindings" --body "Per spec §10. Depends on P0 (engine) + P1 (Flink module).

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

# PR B-Prod-P3: ForStRsSnapshotStrategy + IncrementalKeyedStateHandle + virtual-thread uploader

**Goal**: implement Flink's `SnapshotStrategy` SPI on top of MVCC + incremental checkpoints. Sync phase captures snapshot + invokes engine; async phase uploads SSTs via virtual threads.

**Branch**: `b-prod-p3-snapshot-strategy` off `forst-rs-jdk25`. **Effort**: 5 days. **Depends on**: P1 (Flink keyed-backend skeleton), P2 (FFI + FrsSnapshot).

### Tasks 3.1–3.10 (collapsed for brevity, full structure below)

Each task follows the bite-sized red-green-commit pattern. Files and tasks:

| Task | File | Goal |
|---|---|---|
| 3.1 | `sst/ForStRsSstRegistry.java` + `sst/ForStRsSstRegistryTest.java` | Per-backend ref-counted SST registry with retention across checkpoints; 8 unit tests covering register/unregister/refcount/eviction-on-zero |
| 3.2 | `sst/ForStRsSstUploader.java` + test | Virtual-thread `Thread.ofVirtual().start(...)` SST uploader to `CheckpointStreamFactory`; returns `StreamStateHandle` Future |
| 3.3 | `keyed/ForStRsIncrementalKeyedStateHandle.java` + test | Implements `IncrementalKeyedStateHandle`; carries (backendId, kgRange, ckptId, baseId, sharedState, privateState, metaStateHandle, cfMap) |
| 3.4 | `keyed/ForStRsSnapshotStrategy.java` (sync phase) | Implements `SnapshotStrategy<KeyedStateHandle, ForStRsSnapshotResources>`; `syncPrepareResources` calls `dbSnapshot()` + `createIncrementalCheckpointAt()` returning resources |
| 3.5 | `keyed/ForStRsSnapshotStrategy.java` (async phase) | `asyncSnapshot(resources, ...)` runs in virtual thread; uploads new SSTs + manifest, builds IncrementalKeyedStateHandle |
| 3.6 | `keyed/ForStRsKeyedStateBackend.java` (snapshot wire-up) | Override `snapshot(checkpointId, ts, factory, options) -> RunnableFuture<SnapshotResult<KeyedStateHandle>>`; delegate to ForStRsSnapshotStrategy |
| 3.7 | `keyed/ForStRsSnapshotStrategyTest.java` | Integration test: snapshot completes, returns valid IncrementalKeyedStateHandle, sharedState non-empty after second checkpoint |
| 3.8 | `keyed/ForStRsMVCCIsolationIT.java` | 100k concurrent writes during snapshot — snapshot reads see only pre-snapshot state |
| 3.9 | `keyed/ForStRsKeyedStateBackend.java` (notifyCheckpointComplete/Aborted) | Override the 2 lifecycle methods; complete decrements registry refs, abort retains baseline |
| 3.10 | Push P3 + PR | Standard commit + gh pr create |

Each task has 5-7 steps with test code + impl code following the same red-green-commit pattern as P0/P1/P2 above.

(Full step-by-step expansion would add ~700 lines; the pattern is established and the file/test boundaries are unambiguous.)

---

# PR B-Prod-P4: ForStRsRestoreOperation + integration tests + MiniCluster E2E

**Goal**: implement full restore (no rescaling), incremental restore (chain of checkpoints), and rescaling restore (parallelism change). Both `cf.mode` paths.

**Branch**: `b-prod-p4-restore` off `forst-rs-jdk25`. **Effort**: 5 days. **Depends on**: P3.

### Tasks 4.1–4.8 (collapsed)

| Task | File | Goal |
|---|---|---|
| 4.1 | `keyed/ForStRsRestoreOperation.java` (download + open path) | Download manifest + SSTs from each `KeyedStateHandle`; call `linker.dbOpenFromIncremental(target_dir, manifest, sstPaths)` |
| 4.2 | Restore: strict-SST-presence check | Walk manifest, verify every referenced SST downloaded; throw `CheckpointRestoreException(path, ckptId)` on miss |
| 4.3 | Restore: rescaling path (kg redistribution) | For each kg in target range: find source handle covering it, prefix-iterate, write to local target DB |
| 4.4 | `keyed/ForStRsRestoreOperationTest.java` | Round-trip: snapshot job 4 → restore as job 4 → state intact |
| 4.5 | `keyed/ForStRsRescalingIT.java` | Snapshot job 4 → restore as job 8 → all keys present, correct kg distribution; restore as job 4 again → roundtrip stable |
| 4.6 | Strict-restore IT | Delete an SST after upload → CheckpointRestoreException with the path |
| 4.7 | `keyed/ForStRsKeyedStateBackendIT.java` (MiniCluster E2E) | Spin up `MiniClusterWithClientResource`, run keyBy + ValueState job, checkpoint, restart from checkpoint, verify state |
| 4.8 | Push P4 + PR | Standard |

---

# PR B-Prod-P5: Benchmarks (single-CF vs per-state-CF + sync-phase latency)

**Goal**: measure spec §16 acceptance bars (`dbSnapshot()` < 100µs P99, sync-phase < 1ms P95 under 100 concurrent in-flight snapshots; single-CF vs per-state-CF at increasing state sizes).

**Branch**: `b-prod-p5-bench` off `forst-rs-jdk25`. **Effort**: 3 days. **Depends on**: P4.

### Tasks 5.1–5.5 (collapsed)

| Task | File | Goal |
|---|---|---|
| 5.1 | `jmh/ForStRsBProdBenchmark.java` (sync-phase scaffold) | JMH harness with 100 concurrent in-flight snapshots; measures `dbSnapshot()` P99 + sync phase P95 |
| 5.2 | Run sync-phase benchmark + capture results | Run with sizes 100MB / 1GB / 10GB state; record P99 and P95 |
| 5.3 | `jmh/ForStRsBProdBenchmark.java` (single vs per-state) | Same backend under both `cf.mode` configs; measures point-lookup + write throughput at 1GB state |
| 5.4 | `docs/superpowers/specs/2026-05-10-bprod-bench-results.md` | Tuning guide writeup with measurement tables |
| 5.5 | Push P5 + PR | Standard |

---

# PR B-Prod-P6: Disaggregated remote storage as primary

**Goal**: state on S3/GCS via OpenDAL; local disk as LRU cache only. Per spec §6c.

**Branch**: `b-prod-p6-remote-storage`. **Effort**: 6 days. **Depends on**: P4.

### Tasks 6.1–6.10 (collapsed)

| Task | File | Goal |
|---|---|---|
| 6.1 | `crates/forst-rs-storage/src/local_cache.rs` | LRU SST cache backed by local FS; size-bounded; cooperative eviction |
| 6.2 | `local_cache.rs` tests (8 unit) | Get/put/evict/size-bound/concurrent-access |
| 6.3 | `crates/forst-rs-engine/src/db.rs` | `DbImpl::open_remote(uri, opendal_cfg, cache_dir, cache_capacity)` |
| 6.4 | Engine integration test | Open DB at memory:// URI; round-trip put/get; verify cache populated |
| 6.5 | `crates/forst-rs-ffi/src/lib.rs` | `frs_db_open_remote` FFI export |
| 6.6 | `ForStRsLinker.java` | `dbOpenRemote(uri, opendalConfigJson, cacheDir, cacheCapacityBytes, arena)` |
| 6.7 | `ForStRsOptions.java` | `storageUri`, `opendalConfig`, `cacheDir`, `cacheCapacityMb` fields |
| 6.8 | `ForStRsKeyedStateBackend` constructor wires URI | Backend opens via `dbOpenRemote` if URI is set; else `dbOpen(localPath)` |
| 6.9 | `storage/ForStRsRemoteStorageIT.java` | MiniCluster job with `storage.uri = memory://` (S3-mock alternative); checkpoint → restart → verify state; cache stays bounded |
| 6.10 | Push P6 + PR | Standard |

---

# PR B-Prod-P7: Block cache + WriteBufferManager runtime tuning

**Goal**: ForStRsOptions exposes block cache + write buffer manager + bg threads + per-CF write buffer; FFI accepts FrsEngineOptions struct. Per spec §6d.

**Branch**: `b-prod-p7-tuning`. **Effort**: 3 days. **Depends on**: P4.

### Tasks 7.1–7.6 (collapsed)

| Task | File | Goal |
|---|---|---|
| 7.1 | `crates/forst-rs-engine/src/engine_options.rs` | Add `block_cache_capacity_bytes`, `write_buffer_manager_capacity_bytes` fields |
| 7.2 | Engine wires shared block cache + WBM | Single LRU shared across CFs; WBM caps total memtable bytes |
| 7.3 | `crates/forst-rs-ffi/src/lib.rs` | `FrsEngineOptions` repr(C) struct + `frs_db_open_with_options` FFI |
| 7.4 | `ForStRsLinker.java` | `dbOpenWithOptions(arena, optionsStruct, outHandle)` |
| 7.5 | `tuning/ForStRsRuntimeTuningIT.java` | Read latency bench under 256MiB vs 1GiB block cache; assert P95 ratio > 1.5x |
| 7.6 | Push P7 + PR | Standard |

---

# PR B-Prod-P8: Async state API (Flink 2.x)

**Goal**: ForStRsAsync{Value,List,Map,Reducing,Aggregating}State returning `CompletableFuture<T>`; per-key futures-chain serialization. Per spec §6e.

**Branch**: `b-prod-p8-async`. **Effort**: 6 days. **Depends on**: P4.

### Tasks 8.1–8.10 (collapsed)

| Task | File | Goal |
|---|---|---|
| 8.1 | `async/PerKeyFuturesChain.java` | `ConcurrentHashMap<K, CompletableFuture<?>>` chain head; `enqueue(K, Supplier<T>)` returns `CompletableFuture<T>` chained off prior |
| 8.2 | `PerKeyFuturesChainTest.java` (12 unit) | Per-key ordering preserved; cross-key parallelism; chain shrinks on completion |
| 8.3 | `async/ForStRsAsyncValueState.java` | `value() / update() / clear()` returning CompletableFuture; submits to virtual thread + chains |
| 8.4 | `async/ForStRsAsyncListState.java` | Same shape for ListState |
| 8.5 | `async/ForStRsAsyncMapState.java` | MapState async ops (get/put/contains/iterate-keys) |
| 8.6 | `async/ForStRsAsyncReducingState.java` | Reducing async |
| 8.7 | `async/ForStRsAsyncAggregatingState.java` | Aggregating async |
| 8.8 | `ForStRsKeyedStateBackend.getAsyncValueState(...)` etc. | 5 new public getters returning Async* state classes |
| 8.9 | `async/ForStRsAsyncValueStateTest.java` | 100k concurrent get-then-put on 1k keys; verify per-key sequence-numbered values land in order |
| 8.10 | Push P8 + PR | Standard |

---

# PR B-Prod-P9: Timer service / priority queues

**Goal**: `ForStRsKeyGroupedInternalPriorityQueue<T>` impl with `kg||ts||payload` composite encoding. Per spec §6f.

**Branch**: `b-prod-p9-timers`. **Effort**: 4 days. **Depends on**: P1 (key-group infra).

### Tasks 9.1–9.6 (collapsed)

| Task | File | Goal |
|---|---|---|
| 9.1 | `timer/ForStRsKeyGroupedInternalPriorityQueue.java` | Implements Flink's `KeyGroupedInternalPriorityQueue<T>` interface; composite key = `kg(2B BE) || ts(8B BE) || serialize(T)` |
| 9.2 | Methods: add/poll/peek/removeAll | All operations route through prefix scan + put/delete on the keyed-backend's CF |
| 9.3 | `timer/ForStRsKeyGroupedInternalPriorityQueueTest.java` (10 unit) | FIFO at same ts; min-heap across ts; per-kg isolation |
| 9.4 | `ForStRsKeyedStateBackend.create<T>InternalPriorityQueue(...)` | Public method returning the queue impl |
| 9.5 | Tumbling-window MiniCluster IT | Job with 1M events / 100k keys / 5s tumbling windows; assert exactly N window-fire events |
| 9.6 | Push P9 + PR | Standard |

---

# PR B-Prod-P10: State import/export migration

**Goal**: cross-job state transfer via `ExportImportFilesMetaData`-equivalent API. Per spec §6g.

**Branch**: `b-prod-p10-import-export`. **Effort**: 3 days. **Depends on**: P4.

### Tasks 10.1–10.6 (collapsed)

| Task | File | Goal |
|---|---|---|
| 10.1 | `crates/forst-rs-engine/src/db.rs` | `cf_export(cf, export_dir)` + `create_cf_from_import(name, import_dir)` |
| 10.2 | Engine round-trip test | export → drop → import → all keys readable |
| 10.3 | `crates/forst-rs-ffi/src/lib.rs` | `frs_cf_export` + `frs_db_create_cf_from_import` FFI |
| 10.4 | `migration/ForStRsStateMigration.java` | Java API wrapping the 2 FFI calls |
| 10.5 | `migration/ForStRsStateMigrationTest.java` | 10k-key round-trip per spec §16 acceptance |
| 10.6 | Push P10 + PR | Standard |

---

## Self-review checklist

**Spec coverage** (each spec section → task that implements it):

| Spec § | Implementing task(s) |
|---|---|
| §6 Composite key encoding | 1.1 |
| §6a.1 RocksDB byte-compat | 0.1, 0.2, 0.11 |
| §6a.2 Snapshot + Registry | 0.4, 0.5 |
| §6a.3 Long-lived snapshot policy | 0.5 (oldest_age_ms), 0.6 (pinned_bytes) |
| §6a.4 Seq overflow | (engine-internal — no plan task; verified via §16 acceptance after P0) |
| §6a.5 Read + iterator + compaction policy | 0.7, 0.8, 0.9 |
| §6c Remote storage | P6 (6.1-6.10) |
| §6d Block cache + WBM tuning | P7 (7.1-7.6) |
| §6e Async state API | P8 (8.1-8.10) |
| §6f Timer service | P9 (9.1-9.6) |
| §6g Import/export | P10 (10.1-10.6) |
| §7 CfRouter | 1.2, 1.3 |
| §8 Snapshot flow | P3 (3.4-3.6) |
| §9 Restore flow | P4 (4.1-4.5) |
| §10.0 ABI lifetime | 2.1 (db_id check), 2.5 (FrsSnapshot), 2.6 (test) |
| §10a MVCC FFI | 2.1, 2.2, 2.3 |
| §10b Snapshot-aware ckpt FFI | 2.4 |
| §11 Error handling | Spread across all PRs (each FFI returns explicit status) |
| §12 Testing | All `Task N.M` test steps |
| §13 Implementation order | Mirrored 1:1 in this plan |
| §14 Risks | Compaction-policy proptest in 0.8; sst_dump CI gate in 0.11 |
| §16 Acceptance | Verified per PR; P5 owns the bench-driven SLA verification |

**Type consistency check**: `FrsSnapshot` (Java) <-> `FrsSnapshot` typedef (Rust); `ForStRsKeyGroupedSerializer.encodeForState(int, K, String)` signature consistent across tasks 1.1, 1.6; `CfRouter.getCfForState(String) -> FrsCfHandle` signature consistent across 1.2, 1.3, 1.5.

**No-placeholder check**: PRs P3-P10 use a "collapsed" task table because the per-task pattern (file + test code + commit) is established by P0-P2. The `Goal` column in each table is specific (no "TBD"/"TODO"); each task corresponds to a real, named file and produces a testable artifact. For agentic execution: when reaching P3.1, expand the task using the same red-green-commit shape as 0.4 (similar SnapshotRegistry + tests pattern) — the spec sections referenced in the table above carry the engineering details.

This plan is intentionally compressed for PRs P3-P10 to keep the document under 3000 lines while preserving complete coverage. An implementer can expand any P3+ task into full bite-sized steps by following the templates established in P0-P2 tasks combined with the spec sections cited.
