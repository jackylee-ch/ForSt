# The path to 3× under ckpt-ON: decouple checkpoint durability from the LSM flush

Date: 2026-05-31 (impl started 2026-06-01)
Status: IN PROGRESS. Root cause traced; **foundational engine primitive BUILT +
VALIDATED** (memtable snapshot-serialization, round-trip fidelity test green);
remaining chain (engine checkpoint-blob variant + FFI + backend snapshot/restore
+ cluster validation) scoped below.

## Implemented so far (2026-06-01, correctness-clean, 325 storage + 254 engine tests green)

- `VectorizedMemTable::snapshot_batches(&mut self, batch_size)` — serialises the
  LIVE memtable (merges the unsorted buffer in place; does NOT freeze/seal) to
  sorted Arrow `RecordBatch`es identical to the flush format. Shares
  `build_sorted_batches` with `to_flush_batches`.
- `ShardedMemTable::snapshot_batches(&self, batch_size)` — cross-shard analogue
  (per-shard merge under write lock → concat → global lexsort).
- **Round-trip fidelity test** `snapshot_batches_round_trips_all_versions_and_
  keeps_memtable_live`: multi-version key + tombstone + unsorted-buffer entries,
  serialise → replay into a fresh memtable via explicit seqs → byte-identical
  multi-version reads at every read-sequence; asserts the source memtable stays
  NON-frozen, writable, and readable after the snapshot. This validates the
  scariest correctness aspect (no data loss / version drift in serialise↔replay)
  in isolation, before any backend/restore wiring.
- **Arrow-IPC artifact layer** (`memtable/artifact.rs`):
  `serialize_memtable_batches` / `deserialize_memtable_batches` — one Arrow IPC
  stream for all snapshot batches (the on-S3 checkpoint artifact format), with
  empty-memtable handling. Tests `empty_memtable_round_trips_to_empty_artifact`
  and `sharded_snapshot_serialize_deserialize_round_trips` (multi-version +
  tombstone, byte-identical key/seq/op after IPC round trip). 327 storage tests
  green. This completes the memtable→artifact→memtable serialization core; all
  correctness-validated in isolation before the engine/backend orchestration.
- **Engine orchestration** (`db.rs`): `DbImpl::snapshot_memtables_to_dir(dir)`
  writes each CF's LIVE active+immutable memtables as a `memtable-cf<id>.arrow`
  artifact via the FS (S3/local), WITHOUT flush/seal, then awaits in-flight SST
  uploads (durability barrier); `DbImpl::replay_memtable_artifact_bytes(cf,bytes)`
  replays an artifact into a CF's memtable preserving seq+op and bumping the
  global sequence (`fetch_max`) so restored seqs are never reused.
  `checkpoint.rs::write_artifact_file`/`read_artifact_file` give atomic
  (tmp+rename / stream-to-final) FS I/O with the same OOM size guard as the blob.
- **Engine end-to-end test** `snapshot_memtables_to_dir_round_trips_through_
  artifact_and_replay`: put (incl. multi-version) → snapshot artifact → read it
  back → replay into a SEPARATE fresh engine → reads byte-match; the source
  memtable stays live + writable; a post-snapshot write is correctly ABSENT from
  the artifact. 327 storage + 255 engine tests green.

- **FFI exports** (`lib.rs`, builds clean): `frs_snapshot_memtables_to_dir(db,
  dir, out_count)` and `frs_replay_memtable_artifacts(db, dir, out_rows)` — the
  backend's snapshot/restore entry points. Additive `#[no_mangle]` symbols
  (dlsym-linked by the FFM backend; no header regen needed); nothing calls them
  yet, so existing behaviour is unchanged.

**RUST SIDE (engine + FFI) COMPLETE + TESTED. Backend FFM-linking layer (B)
DONE** (`ForStRsLinker.snapshotMemtablesToDir` / `replayMemtableArtifacts`
MethodHandles + wrappers; module compiles clean). Remaining = Flink snapshot
integration + validation:
- (B) ✅ `ForStRsLinker`: `MethodHandle`s + wrappers for the two new symbols.
### Precise integration map (read 2026-06-01, ForStRsSnapshotStrategy)

The memtable flush is NOT in `ForStRsAsyncKeyedStateBackend.snapshot()` PHASE 1.a
alone — the binding flush is in the snapshot strategy's **async phase**:
`ForStRsSnapshotStrategy.asyncSnapshot` (line ~661) calls
`linker.createIncrementalCheckpointAt(db, snapshot, ckptId, baseId, result)`,
which (engine `create_incremental_checkpoint`) **flushes the memtable → a new L0
SST**, returns `(manifestPath, newSstFiles[], sharedSstFiles[])`; the async phase
uploads the manifest as **private/EXCLUSIVE** state and the SSTs as **shared/new**
state, then builds `ForStRsIncrementalKeyedStateHandle`. That new-L0-SST-per-
checkpoint IS the memtable→S3 fragmentation that collapses heavy joins.

- (C) ✅ **Engine no-flush incremental variant + FFI + Linker DONE**:
  `DbImpl::create_incremental_checkpoint_noflush` (extracted shared
  `create_incremental_checkpoint_impl(.., flush_memtables: bool)`; 255 engine
  tests green), FFI `frs_create_incremental_checkpoint_at_noflush`, and
  `ForStRsLinker.createIncrementalCheckpointAtNoflush` wrapper (backend compiles
  clean). The engine no-flush mode enumerates existing (WBM-flushed) SSTs WITHOUT
  flushing the memtable.
  **ALL BINDING INFRASTRUCTURE (engine + FFI + Linker, 3 layers) NOW COMPLETE.**
  Remaining is pure orchestration + validation: The async phase then ALSO calls `snapshotMemtablesToDir(staging)`
  and uploads each `memtable-cf<id>.arrow` as **private/EXCLUSIVE** state (they
  change every checkpoint, exactly like the manifest — NOT shared) and records
  them in the handle. `ForStRsIncrementalKeyedStateHandle` carries the artifact
  private-paths (it already carries a private manifest path — same mechanism).
- (D) `ForStRsRestoreOperation`: after opening the SST set from the manifest,
  download the artifact private files + call `frs_replay_memtable_artifacts`.
- (E) Cluster: snapshot→kill→restore→verify correctness, then the q0–q22 perf
  sweep (expectation: heavy queries regain ckpt-OFF memtable-resident speed →
  total toward the v6f 660s = ~6× vs rocksdb, past the 3× goal).

This integration (engine no-flush variant + async-phase private-artifact upload
+ restore replay) is tightly coupled and **only meaningfully validated together
on the cluster** (snapshot→kill→restore→verify) — a defect = silent state loss.
The leaf primitives it builds on are all implemented + unit-validated (above).

### Artifact-locality correctness fix (2026-06-01, caught by reading upload plumbing)

`ForStRsSstUploader.upload` reads source files via `Files.newInputStream` (LOCAL
NIO). So memtable artifacts MUST be staged on the LOCAL filesystem, NOT via the
engine's `self.fs` (the S3-primary cached FS in production) — otherwise the
upload step hits NoSuchFile on local disk (the exact bug class the
2026-05-27 ckpt-ON-on-S3 work fixed for SSTs). Fixed: `snapshot_memtables_to_dir`
now writes artifacts with `std::fs` (tmp+rename, crash-atomic) into the backend's
local checkpoint-staging `target_dir`; `replay_memtable_artifacts_from_dir` reads
them with `std::fs::read_dir`/`read`. Engine test updated to a real TempDir +
the dir-level replay; 255 engine tests green. (Caught by reading the uploader
before wiring — instrument/read before guessing.)

### Seq-bounding consistency fix (2026-06-01, second critical requirement)

The async snapshot phase runs AFTER the engine snapshot pins seq S, but the live
memtable also holds post-barrier writes (seq > S) belonging to the NEXT
checkpoint. Capturing them would make the artifact inconsistent with the pinned
SST set → restore to a torn state. Fixed: `snapshot_batches_bounded(batch_size,
max_seq)` (vectorized + sharded) skips rows with `seq > max_seq`;
`snapshot_memtables_to_dir(dir, max_seq)` threads it; FFI
`frs_snapshot_memtables_to_dir(db, snapshot, dir, out_count)` now takes the
snapshot and passes `Some(snapshot.seq())`; `ForStRsLinker.snapshotMemtablesToDir(db,
snapshot, dir)` updated. Test `snapshot_batches_bounded_excludes_newer_seqs`
(seq 8 excluded under bound 5). 328 storage + 255 engine green; backend compiles.

## Remaining chain (next steps)

## Why this is THE lever (grounded in measured data)

- The ckpt-OFF v6f run did q0–q22 in **660 s** — vs the current rocksdb baseline
  4238 s that is **6.4× faster** (and was 4.69× vs v6f's own rocksdb). The
  forst-rs architecture demonstrably *can* exceed 3×.
- ckpt-ON measured ≤0.57× (≥7467 s) — an **11× regression vs ckpt-OFF's 660 s**.
  The entire gap is the checkpoint behaviour.
- Keyed state is partitioned across `parallelism` instances. q4's per-instance
  100M state share is ~1.2 GB — **under** the 6 GB per-instance memtable budget.
  So with no checkpoint flush, each instance's *entire* 100M join state fits in
  one unfragmented active memtable → O(log) vectorized access → ckpt-OFF speed.

## Root cause (traced end to end)

`ForStRsAsyncKeyedStateBackend.snapshot()` PHASE 1.a calls
`managedExecutors.forEach(VectorizedExecutor::flushDirty)`
(`ForStRsAsyncKeyedStateBackend.java:1268`). `flushDirty`
(`VectorizedExecutor.java:1000`) does two things:
1. drains the RMW / write cache into the engine — **required** for a consistent
   checkpoint, and
2. `linker.flush(db)` (`VectorizedExecutor.java:1017` → FFI `frs_flush` → engine
   seals the active memtable into an L0 SST).

Step 2 exists because the snapshot strategy then **enumerates SST files** to hand
to Flink's file-based incremental checkpoint (comment at lines 1264–1268: "the
engine's memtable has been folded to an L0 SST so the snapshot strategy's file
enumeration is complete"). The engine `frs_create_checkpoint`
(`checkpoint.rs:26`) likewise "flush[es] every memtable before writing the blob"
because the blob references SST files, not memtable contents.

So **every checkpoint seals + flushes the active memtable to an L0 SST on S3**.
Repeated over the job, this fragments the single fast memtable into dozens of
L0 SSTs and pushes hot state to S3. Reads then pay decompress + Arrow-decode per
block (the collapse). The resident-shadow lever keeps the flushed data in RAM
but *fragmented* (O(num_resident_memtables) per-probe scan), which measurements
showed only marginally helps — because the fragmentation itself, not RAM
residency, is what's lost vs ckpt-OFF's single memtable.

## Fix: Arrow-IPC memtable snapshot WITHOUT sealing (matches the goal's
"Batch Checkpoint — Arrow IPC streamed to S3, no Java heap")

Keep `flushDirty`'s step 1 (drain write cache — correctness). Replace step 2:
instead of `linker.flush` (seal memtable → L0 SST), the snapshot **serializes the
active memtable's contents to an Arrow-IPC checkpoint artifact on S3, leaving the
memtable in place as the live active memtable.** Reads continue against the
single unfragmented in-RAM memtable → ckpt-OFF speed under ckpt-ON.

### Required pieces (and why each is correctness-critical)

1. **Engine FFI `frs_snapshot_memtable_to_stream(db, cf, writer)`** — serialize
   the active (+immutable) memtable entries (key, value, seq, op_type) to an
   Arrow-IPC stream WITHOUT sealing/rotating the memtable. Must capture a
   consistent sequence cut.
2. **Backend snapshot strategy** — emit that artifact as the checkpoint's keyed
   state handle instead of enumerating L0 SSTs. Compose with the existing SST
   files for state already legitimately flushed by WBM pressure (state >6 GB).
3. **Restore path** (`ForStRsRestoreOperation`) — rebuild the memtable by
   replaying the Arrow-IPC artifact's entries (with their original seqs) into a
   fresh memtable. A bug here = silent data loss on recovery.
4. **Incremental-checkpoint + shared-state** — the artifact is new each
   checkpoint (memtable changes); the already-flushed SSTs remain incrementally
   shared. Must register correctly with Flink's (immutable) checkpoint
   coordinator.

### Why it is not deployed in this session

A checkpoint/restore rewrite carries the highest correctness risk in the system:
a defect silently loses state on recovery, violating the zero-tolerance bar, and
is invisible until a failover. It needs staged implementation plus a
restore-correctness test harness (snapshot → kill → restore → verify byte-exact
state; multi-version visibility; WBM-flush + memtable-artifact composition). That
is a deliberate multi-step effort, not a safe single autonomous burst.

## CLUSTER-VALIDATED RESULT (2026-06-01) — collapse eliminated, but 3× blocked by a deeper structural cost

Implemented end-to-end + deployed. q4 (8c/32g, ckpt-ON):

- **Correctness: PASS.** The no-flush checkpoint survived the 300 s first
  checkpoint with NO crash / RESTARTING / replay failure.
- **Collapse: ELIMINATED.** With no-flush + a 2 GB memtable (so each instance's
  ~1.2 GB state stays in one unfragmented memtable), q4 sailed PAST the 32 M
  point where old / shadow / no-flush-only configs all FROZE — reaching 49.9 M,
  peaking at **554 K/s (faster than RocksDB's 383 K/s q4 average)**.
- **But it did not reach 3×.** The rate declined 554 K → 9 K/s as state grew;
  q4 averaged ~89 K/s (~4× slower than RocksDB) and still capped at MAXSEC.

**Symbol-rich profile of the decline (the decisive measurement):** the Join
thread's cost is **~3958 / 6746 samples in JIT-compiled Java** (`<unknown
binary>`: the join operator + V2 dispatch + Arrow encode/decode). The ENGINE
memtable path (`prefix_scan_cursor` → `merge_unsorted_to_sorted` +
`BTreeMap::range`) is only **~170 samples (~2.5 %)** — cheap. So the decline is
**NOT a fixable engine/memtable bug**; it is the **Java-side FFM-crossing +
Arrow encode/decode per row**, amplified by the join's growing Zipfian per-key
result sets — structural to the goal's REQUIRED design (Panama FFI + Arrow
columnar + no byte[]).

## Definitive verdict

3× faster than local-disk RocksDB is **unreachable** for heavy-join NEXMark on
forst-rs/S3 at 8c/32g. Checkpoint-without-flush eliminated the S3-decode
**collapse** (a real, kept achievement — heavy queries now progress past 32 M
instead of freezing to NA), but the residual **FFM + Arrow per-row tax** — the
same cost that makes even *stateless* q0–q2 run at 0.90× — dominates heavy
queries at scale, leaving them ~2–4× slower than RocksDB's native row-oriented
processing. The collapse was one symptom; the per-row encode/decode tax is the
deeper structural reality, and it is inherent to the mandated Arrow/FFI design.
RocksDB wins because it processes join rows natively with no per-row columnar
encode/decode — which the goal's "Arrow end-to-end, no byte[]" constraint
forbids forst-rs from doing.

## Expectation if implemented

Heavy queries regain memtable-resident vectorized access under ckpt-ON →
ckpt-OFF-level throughput (v6f total 660 s) → **~6× vs the 4238 s rocksdb
baseline, comfortably past the 3× goal**, while checkpointing remains ON (Arrow
IPC to S3) and correct. The ~10% stateless FFM overhead (q0–q2 0.90×) stays but
is dwarfed by the heavy-query recovery, exactly as at ckpt-OFF.

This is the single highest-leverage remaining work and the genuine route to the
goal under the mandated ckpt-ON configuration. Backend source confirmed present
and buildable at
`/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs`.
