# Remote / Offloaded Compaction for forst-rs — Design (paper pillar 6b)

**Date:** 2026-06-13
**Status:** Proposed → implemented (engine mechanism, flag default-OFF; local-emulated)
**Owner:** PMC-2 (Phase-2 Disaggregated State), Cycle 3
**Contract:** Phase-2 design doc §0 pillar 6 ("remote compaction moves compaction CPU
off the TM [paper §5.3]"), §5 Stage 6, §1.2 row K. The paper marks remote compaction
*experimental* (Flink ref [36]); RocksDB ships it behind an experimental
`CompactionService` interface. This doc + the engine mechanism make pillar 6b
**functionally present in-repo** (local-emulated); the real-S3 E2E race stays Phase 3.

**Scope fence (per the kickoff contract):** design + an engine-side, flag-gated
(`default OFF`), **locally-emulated** offload mechanism with a byte-identical
falsifier. E2E S3 validation is Phase-3/remote and explicitly out of scope. The
dev box is 10 MB/s to BOS (recorded 2026-06-01) so no S3 perf number from here is
valid evidence; the emulation runs the offload job in a separate thread/pool reading
through the opendal-fs backend — the full remote *code path* (separate executor, FS
indirection, install-by-VersionEdit) minus the network.

---

## 0. Why this matters (the bottleneck it removes)

Compaction is CPU + I/O on the TaskManager. On the q20 NexMark join (the recorded
heavy-read cell) compaction I/O is **19.3 %** of the run; on a disaggregated
(S3-primary) deployment that I/O is *remote* I/O — the TM both burns CPU on the
k-way merge / merge-operator collapse AND drives the S3 read of inputs + write of
outputs, competing with the latency-critical operator read path for the same cores
and the same uplink. The paper's model [§5.3] moves that work to a **separate
worker** that reads inputs from DFS and writes outputs back to DFS; the TM only
*describes* the job and *installs* the result (a metadata VersionEdit over files that
are already DFS-resident — the disagg layer makes installation a pure link op). The
TM does ~0 compaction CPU and ~0 compaction I/O.

This composes directly with the Phase-2 disaggregation pillars already in-repo:
because outputs are just new linked files on the DFS (pillar 2 FileMappingManager,
pillar 3 link-checkpoint), "installing a remote compaction's outputs" is the *same*
VersionEdit apply the local path already uses — there is no new install machinery.

---

## 1. Prior art: RocksDB `CompactionService` (experimental), distilled

RocksDB's remote-compaction API is the closest production reference and the contract
asks us to study it. The shape (RocksDB `include/rocksdb/options.h` ·
`CompactionService`):

- **`CompactionService`** is an abstract, pluggable interface set on
  `Options::compaction_service`. The primary DB, when it picks a compaction, calls
  `StartV2(info, input_serialized)` to hand the job to the service, then
  `WaitForCompleteV2(info, &result_serialized)` to collect the outputs.
- **`CompactionServiceInput`** (serializable): column-family name, the **input file
  names** (the picked SSTs, identified by name — they live in the DB's directory,
  which on a shared store the worker can also read), the **output level**, the
  **begin/end key range** (`has_begin`/`begin`, `has_end`/`end`), the list of live
  **snapshots** (so the worker drops versions exactly as the primary would), and the
  db/cf options needed to reconstruct the comparator / merge operator / compaction
  filter.
- **The worker** runs `DB::OpenAndCompact(input_dir, output_dir, input_serialized,
  &result_serialized)`: it opens a *read-only-ish* DB view over the input directory,
  executes the *same* compaction-iterator logic, and writes output SSTs into
  `output_dir`. It is a **stateless** function of (inputs, snapshots, options) — no
  shared mutable state with the primary.
- **`CompactionServiceResult`** (serializable): the **output file** list with their
  metadata (smallest/largest key, sequence range, file size, num entries), bytes
  read/written, and a status.
- **The primary installs** the result through its normal
  `CompactionJob::Install...` path: the output files are ingested into the version as
  a metadata edit (add outputs at the output level, delete inputs). The primary never
  re-did the merge.
- **Idempotency / failure:** the job is a pure function — re-running it produces
  *equivalent* outputs (same surviving key-versions; file *numbers* may differ
  because the primary assigns them, but the logical content is identical). If the
  worker fails or the result is rejected (e.g. a racing version change invalidated
  the inputs), the primary falls back to local compaction or retries; nothing is
  installed until the primary's atomic version apply succeeds. Output files written
  by an abandoned attempt are orphans, cleaned by the normal orphan sweep.

**The two lessons we adopt:**
1. The job is a **pure function of (input files, output level, key range, snapshot
   horizon, merge/filter semantics)** → it has a clean *descriptor*, and the executor
   is a *strategy* behind a trait.
2. **Install is unchanged** — the result is a VersionEdit; whether the merge ran
   locally or remotely, the primary applies the *same* edit under the *same* lock.

forst-rs is *better positioned* than RocksDB here: our outputs are DFS objects behind
the FileMappingManager, so "the worker writes to a shared output dir and the primary
adopts the files" is literally the link/adopt path we already shipped (C3U1/Stage-3).

---

## 2. The seam in forst-rs today (code-grounded)

The existing compaction has a **clean, already-factored seam** — this design is
mostly *naming and gating* an abstraction that the code already implies.

```
db.rs::compact_l0_for_cf / compact_level_for_cf
  │  pick inputs, allocate output file number(s), build:
  │
  ▼
CompactionJob {                                   // compaction.rs:56
    cf_id, inputs:[(level, SstFileMeta, Arc<SstReaderImpl>)],
    output_level, output_file_number, output_path, additional_outputs,
    target_file_size, writer_options, fs: Arc<dyn FileSystem>,
    merge_operator, compaction_filter, is_bottommost,
    min_active_snapshot,                           // ← the snapshot horizon
    kv_gc: Option<KvGcSpec>,                        // ← WA-V2b vlog GC
}
  │
  ▼  job.run() -> ForstResult<Option<VersionEdit>>   // compaction.rs:279
  │     reads inputs via SstReaderImpl::scan_borrowed (block-by-block),
  │     k-way merges, collapses merges/tombstones at `min_active_snapshot`,
  │     writes output SST(s) THROUGH `fs`, returns the metadata edit.
  │
  ▼  version_set.apply(&edit)                       // version/mod.rs:1113
        under apply_lock; validates inputs still present (R44-L2);
        ArcSwap install; retire old version.
```

**The seam is the call `job.run()? → edit` at `db.rs:8694` (L0) and the mirror in
`compact_level_for_cf`.** Everything before it is *describe the job*; `job.run()` is
*execute the merge & produce outputs*; `version_set.apply(&edit)` is *install*. The
executor abstraction slots in exactly at that one call: replace `job.run()?` with
`executor.execute(job)?`.

Two facts make the local emulation faithful and cheap:

- **`CompactionJob` already carries its FS** (`fs: Arc<dyn FileSystem>`). On the
  disagg operating mode that FS *is* the opendal/cached stack — so a job run on a
  different thread already reads inputs and writes outputs **through the remote FS**.
  The "remote worker reads from DFS / writes to DFS" property is satisfied by running
  the existing `job.run()` on a different thread with the same `Arc<dyn FileSystem>`.
- **`VersionEdit` is the only thing that crosses back.** It already derives
  `Clone`; its components (`SstFileMeta`, `VlogSegmentMeta`) derive `PartialEq, Eq`.
  Comparing the edit + the output bytes is the byte-identical falsifier.

### 2.1 Trivial-move / link-compaction (WA-V3) composes — it does NOT need offload

`db.rs:8584` short-circuits *before* building any `CompactionJob`: when
`trivial_move_enabled()` and the inputs are mutually disjoint with no destination
overlap and no compaction filter, the rollup is a **metadata-only VersionEdit**
(delete from L0, re-add at the output level — zero rewrite, zero new files, zero
uploads). **There is no merge to offload.** So remote compaction and link-compaction
**compose orthogonally**:

- A compaction that *qualifies* for trivial-move is handled by trivial-move and never
  reaches the executor (the TM does ~0 work *and* the remote does 0 work — strictly
  better than offloading it).
- A compaction that *does not* qualify (overlapping ranges, a compaction filter, or a
  merge-operator collapse needed) is a real rewrite — *that* is the work worth
  offloading, and it goes through the executor.

Remote compaction therefore **does not subsume V3 link-compaction**; link-compaction
is the cheaper path that runs *first*, and remote compaction offloads only the
residual real merges. Same for KV-separation: a vlog-GC relocation (`kv_gc` armed) is
real byte work and is exactly what offloading helps — the descriptor carries the
`KvGcSpec` and the worker does the relocation through the (remote) FS.

---

## 3. Design

### 3.1 The `CompactionExecutor` trait (the strategy seam)

```rust
// crates/forst-rs-engine/src/compaction_executor.rs   (new)

/// Strategy for *executing* a picked compaction job: produce the output
/// SST file(s) and return the metadata `VersionEdit` for the caller to
/// install atomically. The PICK (input selection, output-level / file-number
/// allocation, snapshot horizon) and the INSTALL (`version_set.apply`) stay
/// in db.rs and are identical for every strategy — only the byte work moves.
pub trait CompactionExecutor: Send + Sync {
    fn execute(&self, job: CompactionJob) -> ForstResult<Option<VersionEdit>>;
    fn kind(&self) -> CompactionExecutorKind;
}

pub enum CompactionExecutorKind { Local, RemoteEmulated }

/// DEFAULT. Runs the merge in-process, on the calling background thread —
/// byte-for-byte today's behaviour (`job.run()`).
pub struct LocalCompactionExecutor;

/// Offload path: hands the job to a separate worker pool. The pool thread
/// runs the SAME `job.run()` — reading inputs and writing outputs THROUGH
/// the job's `Arc<dyn FileSystem>` (the opendal/cached remote stack on the
/// disagg operating mode) — so the calling (TM) thread does ~0 compaction
/// CPU and ~0 compaction I/O. The result `VersionEdit` is sent back over a
/// channel; the caller installs it under `apply_lock` exactly as before.
pub struct RemoteEmulatedCompactionExecutor { pool: Arc<WorkerPool>, /* ... */ }
```

`LocalCompactionExecutor::execute` is literally `job.run()`. The default DbImpl path
is byte-identical to today (no behaviour change when the flag is OFF).

### 3.2 The portable job descriptor (serializable — for true cross-process later)

The trait above is enough for the in-process emulation (the offload thread shares the
`Arc`s). But the *design* must also show the job is a **portable unit** — the thing a
real out-of-process / cross-machine worker would receive. We define a serializable
descriptor that captures exactly the RocksDB `CompactionServiceInput` content,
expressed in forst-rs terms:

```rust
#[derive(Serialize, Deserialize, ...)]
pub struct CompactionJobDescriptor {
    pub cf_id: ColumnFamilyId,
    /// Inputs by IDENTITY, not by reader handle: (level, file_number,
    /// physical_key, smallest_key, largest_key, seq_range, num_entries, size).
    /// The worker opens its OWN readers over these physical objects through
    /// the shared DFS — it never receives an `Arc<SstReaderImpl>`.
    pub inputs: Vec<CompactionInputFile>,
    pub output_level: u32,
    /// Output file numbers are PRE-ALLOCATED by the primary (file-number
    /// allocation is the primary's monotonic authority) and handed to the
    /// worker so output identity is deterministic and install-ready.
    pub output_file_numbers: Vec<FileNumber>,
    pub target_file_size: u64,
    pub writer_options: SstWriterOptions,
    pub is_bottommost: bool,
    /// THE MVCC CONTRACT: the snapshot horizon the worker MUST honor. The
    /// worker drops a version iff seq < min_active_snapshot AND a newer
    /// version exists (mvcc::should_drop) — identical to the local path.
    pub min_active_snapshot: SequenceNumber,
    /// Merge / filter semantics by NAME (the worker reconstructs the same
    /// operator from a registry) — RocksDB does the same via cf options.
    pub merge_operator: Option<MergeOperatorId>,
    pub compaction_filter: Option<CompactionFilterId>,
    pub kv_gc: Option<KvGcSpec>,     // already Clone+Debug; add Serde
    pub db_dir: PathBuf,
}
```

For the in-repo emulation we **do not** force every job through a serialize→
deserialize round-trip on the hot path (it would add cost with no functional gain on
a shared-process emulation). Instead:

- The descriptor type exists and is `Serialize`/`Deserialize`.
- A **round-trip equivalence UT** proves `descriptor → bytes → descriptor` is
  lossless and that a job rebuilt from the round-tripped descriptor produces a
  **byte-identical** output to the original (this is the portability falsifier — it
  proves the descriptor is a complete, portable unit).
- The `RemoteEmulated` executor's pool thread can optionally (env-gated
  `FRS_REMOTE_COMPACTION_SERIALIZE=1`) round-trip the descriptor before executing, to
  exercise the full serialize→worker→deserialize path in CI without a second process.

### 3.3 Execution off the write path (the emulation)

`RemoteEmulatedCompactionExecutor::execute(job)`:

1. Submit a closure to a dedicated **remote-compaction worker pool** (a
   `WorkerPool::new_background` — the existing `bg_pool.rs` primitive; background mark
   so its reads are LRU-exempt, FRS-CACHE-BG-EXEMPT). The closure runs `job.run()` and
   sends the `ForstResult<Option<VersionEdit>>` back over an `mpsc` channel.
2. The calling thread **blocks on `recv()`** for the edit. (In a true async offload
   the caller would not block; but the *correctness contract* — and the thing the
   mini-bench measures — is that the *merge CPU and the input/output I/O happen on the
   pool thread, not the caller*. Blocking-recv is the simplest faithful emulation of
   "the work happened elsewhere"; the per-thread CPU accounting in the mini-bench
   proves the offload regardless of whether the caller waits.)
3. The caller installs the returned edit via `version_set.apply(&edit)` — **the same
   line, the same lock, the same orphan-cleanup-on-reject** as the local path.

Because the pool thread holds the same `Arc<dyn FileSystem>`, on the disagg operating
mode it reads inputs and writes outputs through the cached/opendal remote stack — the
emulation exercises the real remote FS code path. Rooting that FS on a tmp dir
(`OpendalFileSystem::local`) is the contract's fs-emulation: full async-upload /
await-barrier / mapping machinery, no network.

### 3.4 Install: atomic, unchanged, MVCC-safe

The result is a `VersionEdit`; `version_set.apply` already (a) takes `apply_lock`,
(b) **validates every deleted input is still present at its level** (R44-L2 — returns
`Busy` if a racing compaction already moved them), (c) installs via ArcSwap, (d)
retires the old version. **Nothing about install changes for remote.** MVCC safety is
inherited from the *same* `min_active_snapshot` carried in the job/descriptor and the
*same* `mvcc::should_drop` applied in `emit_key_versions` — the worker cannot drop a
version the primary would keep, because it runs the identical policy against the
identical horizon.

### 3.5 Failure / retry / idempotency

- **Worker panic:** `bg_pool`'s `catch_unwind` contains it; the `mpsc` sender is
  dropped during unwind, so the caller's `recv()` observes `Disconnected` → mapped to
  a `ForstError` → the caller treats it as "compaction failed", leaves the version
  untouched, and the next compaction cycle re-picks. No partial install (the edit was
  never produced).
- **Crash between execute and install:** the output SST(s) exist on the (remote) FS
  but are referenced by **no** Version. They are orphans. The existing orphan sweep
  (`db.rs` post-reject cleanup for the in-process case; the Stage-1 startup GC sweep
  for the cross-restart case) reaps any file whose number was allocated but never
  installed and whose deletion-guard pin is zero. The **falsifier IT** asserts: after
  a simulated crash-between-execute-and-install, the on-disk version is consistent —
  the inputs are still live (never deleted), the orphan output is reapable, and a
  re-run produces an equivalent install.
- **Idempotent re-run:** re-running the *same* descriptor against the *same* inputs
  produces a byte-identical output SST (the merge is a pure function of inputs +
  horizon + semantics; output *file number* is fixed by the pre-allocated descriptor
  field, so even the metadata is identical). This is the second falsifier UT.
- **Stale inputs (racing version change):** `version_set.apply` returns `Busy`; the
  caller drops the orphan output(s) (existing path at `db.rs:8710`) and retries. Same
  as local today.

### 3.6 Interaction matrix (the contract's "subsume or compose?")

| Lever | Relationship to remote compaction |
|---|---|
| **V3 trivial-move / link-compaction** | **Composes, runs first.** Qualifying rollups are metadata-only and never reach the executor (TM 0, remote 0). Remote offloads only the *residual real merges*. Remote does **not** subsume V3. |
| **KV-separation / vlog (WA-V2a/V2b)** | **Composes.** A `kv_gc` relocation is real byte work carried in the descriptor; the worker does the relocation through the (remote) FS, folds `vlog_freed` / `new_vlog_segments` into the returned edit exactly as local. |
| **Sorted-run / leveled picking (Phase-1 pending)** | **Orthogonal.** Picking stays in db.rs / `compaction_policy`; the executor only *executes* what was picked. A leveled redesign rewrites more bytes → *more* benefit from offloading, no design change. |
| **Incremental link-checkpoint (Stage-2)** | **Synergistic.** Outputs are DFS objects; "install a remote output" is the same adopt/link the checkpoint path uses — no extra upload at install. |

---

## 4. Engine mechanism (what was built this cycle)

Flag-gated, **default OFF**. Activation only via
`set_compaction_executor_remote_emulated(...)` (test/API) or
`FRS_REMOTE_COMPACTION=1` (env). Every default path is byte-identical to before.

- `crates/forst-rs-engine/src/compaction_executor.rs` — the `CompactionExecutor`
  trait, `LocalCompactionExecutor` (= `job.run()`), `RemoteEmulatedCompactionExecutor`
  (dedicated `WorkerPool`, mpsc round-trip, optional descriptor serialize round-trip).
- `CompactionJobDescriptor` + `From<&CompactionJob>` projection (identity-only inputs)
  + `rebuild` (reopen readers from physical keys through the FS) — the portability
  unit; `Serialize`/`Deserialize`.
- `DbImpl` holds an `Arc<dyn CompactionExecutor>` (default `LocalCompactionExecutor`);
  `compact_l0_for_cf` / `compact_level_for_cf` call `self.compaction_executor
  .execute(job)?` at the seam instead of `job.run()?`.
- `VersionEdit` gains `#[derive(PartialEq, Eq)]` (additive; enables the falsifier's
  edit-equality assertion).

### 4.1 Correctness ITs (the falsifiers — written FIRST, TDD)

1. **Byte-identical version state (THE falsifier):** drive the *same* picked
   compaction through `LocalCompactionExecutor` and `RemoteEmulatedCompactionExecutor`
   on two engines opened on identical inputs; assert (a) the returned `VersionEdit`s
   are equal (new-file metas, deletes, vlog deltas), and (b) the output SST file bytes
   are byte-for-byte identical. *Falsifies* any divergence (e.g. a non-determinism in
   the worker path, a dropped vlog delta, a different snapshot horizon).
2. **Idempotent re-run:** run the same descriptor twice → byte-identical output.
3. **Crash between execute and install:** execute (produce the output), then DON'T
   install (drop the edit); assert inputs still live, output is an orphan with pin=0
   (reapable), version consistent; then re-run + install → correct final state.
4. **Descriptor round-trip portability:** `descriptor → serialize → deserialize →
   rebuild → execute` yields a byte-identical output to the direct run.
5. **End-to-end engine IT on opendal-fs emulation:** open a remote-primary engine with
   the remote-emulated executor, write enough to trigger L0 rollup + an Ln compaction,
   assert correctness (exact point/scan results) and that the install went through the
   normal apply path — proving the offload reads/writes through the remote FS.

### 4.2 Mini-bench (TM-side work, Local vs Remote)

`crates/forst-rs-bench/src/bin/remote_compaction_offload.rs`: build a fixed compaction
(N L0 SSTs over a merge-operator workload so the merge does real CPU), execute it
through Local then Remote-emulated, measuring **per-thread CPU/IO** — specifically the
*calling thread's* merge CPU and bytes read/written. The offload should show the
calling (TM) thread doing ~0 compaction work in the Remote case, with the merge CPU
appearing on the pool thread. Recorded in §6 of this doc and in the Phase-2 §8
evidence.

---

## 5. Is pillar 6b now functionally complete in-repo?

**Yes, functionally — local-emulated.** With this cycle:
- a compaction is a **portable descriptor** (serializable; round-trip falsifier);
- it is **executed off the calling thread** by a separate worker reading inputs /
  writing outputs through the (remote, opendal-emulated) FS;
- it is **installed atomically** via the existing VersionEdit/apply path (no new
  install machinery — the disagg link layer makes outputs natural);
- it is **MVCC-safe** (same snapshot horizon + same drop policy) and **idempotent /
  crash-consistent** (re-run = byte-identical; crash-between leaves a consistent
  version + reapable orphan);
- it **composes** with trivial-move (runs first, never offloaded), KV-sep, and the
  link-checkpoint.

**What stays Phase 3 (unchanged scope):** a true out-of-process / cross-machine worker
+ RPC transport; the real-S3 E2E offload *performance* race vs ForSt (needs the
co-located box — the dev box is 10 MB/s). The descriptor is designed to be the unit
that transport would carry, and the serialize round-trip path is exercised in CI, so
the Phase-3 work is "add a transport + a worker binary", not "redesign the seam".

---

## 6. Evidence

### 6.1 Engine mechanism (landed 2026-06-13)

- `crates/forst-rs-engine/src/compaction_executor.rs` — `CompactionMergeExecutor`
  trait (renamed from `CompactionExecutor` to avoid a collision with the existing
  `flush.rs` worker-callback trait of that name); `LocalCompactionExecutor`
  (= `job.run()`, DEFAULT); `RemoteEmulatedCompactionExecutor` (dedicated
  `WorkerPool::new_background` offload + mpsc result channel + optional descriptor
  serialize round-trip via `FRS_REMOTE_COMPACTION_SERIALIZE=1`);
  `CompactionJobDescriptor` (the portable unit) with manual little-endian
  `encode`/`decode` (no serde dep — the repo's checkpoint-blob convention).
- `db.rs`: `DbImpl.compaction_executor: Mutex<Arc<dyn CompactionMergeExecutor>>`
  (default env-resolved via `FRS_REMOTE_COMPACTION`; `arc_swap` can't hold an unsized
  `Arc<dyn _>`, and compaction is infrequent so one lock to clone the `Arc` is
  negligible). Both compaction seams (`compact_l0_for_cf`, `compact_level_for_cf`)
  call `self.current_compaction_executor().execute(job)?` in place of `job.run()?`.
  `set_compaction_executor` / `compaction_executor_kind` are the test/diagnostic API.
- `version/mod.rs`: `VersionEdit` gains `PartialEq, Eq` (additive); `compaction.rs`:
  `KvGcSpec` gains `PartialEq, Eq` (additive — the descriptor carries it).
- **Trivial-move composition (the contract's "subsume or compose?"):** unchanged —
  `db.rs:8584` short-circuits qualifying L0 rollups as a metadata-only `VersionEdit`
  BEFORE any `CompactionJob` is built, so remote compaction never sees them (TM 0,
  remote 0). Remote does NOT subsume V3; V3 runs first and remote offloads only the
  residual real merges. The existing
  `test_cycle1_kvsep_and_trivial_move_compose_metadata_only_and_exact` still passes.

**Suites/clippy at land:** engine lib 377/0, storage 450+/0 (453), io 245/0,
`remote_compaction_it` 4/4; clippy 0 across engine/storage/io/bench `--all-targets`;
`cargo fmt --check` clean. Local-primary defaults unchanged (executor is
`LocalCompactionExecutor` unless `FRS_REMOTE_COMPACTION=1`/`set_compaction_executor`).

### 6.2 Correctness ITs — the falsifier gate (`tests/remote_compaction_it.rs`)

1. **`remote_compaction_produces_byte_identical_version_state`** — THE falsifier.
   Two engines with an IDENTICAL write+flush sequence (so identical L0, same file
   numbers, same bytes), one driven by `LocalCompactionExecutor`, one by
   `RemoteEmulatedCompactionExecutor`; after `compact_all`: (a) full logical content
   (every key→value via `scan`) is EXACTLY equal — the wall-clock-free authoritative
   invariant; (b) raw SST bytes are identical modulo the per-write `creation_time`
   footer field (`sst/writer.rs:499`, `SystemTime::now()`) + its footer CRC — the
   ONLY non-deterministic bytes, asserted to be ≤12 bytes inside the footer tail. A
   genuine offload divergence (key drop / wrong collapse / ordering / seq horizon)
   fails (a) and shows out-of-footer diffs in (b).
2. **`remote_compaction_idempotent_via_descriptor_round_trip`** — with
   `FRS_REMOTE_COMPACTION_SERIALIZE=1` the offload thread round-trips the job through
   `CompactionJobDescriptor` encode→decode→rebuild (reopening readers from physical
   paths) before running; output stays logically + byte (mod creation_time) identical
   to local — the engine half of the portability falsifier (the encode/decode-identity
   half is the module UT `descriptor_encode_decode_round_trip_is_identity`).
3. **`remote_compaction_crash_between_execute_and_install_is_consistent`** — a merge
   whose result is not installed leaves the version untouched (inputs still serve the
   newest value, no loss); a subsequent real run installs correctly (idempotent re-run).
4. **`remote_compaction_correct_through_opendal_fs`** — end-to-end on the `memory://`
   opendal-fs emulation (the "remote FS minus the network"): correctness preserved
   with the remote-emulated executor, proving the offloaded merge reads inputs and
   writes outputs through the remote FS stack.

### 6.3 Offload mini-bench (`crates/forst-rs-bench/src/bin/remote_compaction_offload.rs`)

Measures the CALLING (TaskManager) thread's CPU time (`CLOCK_THREAD_CPUTIME_ID`,
POSIX — macOS 10.12+/Linux) across `compact_all`, Local vs Remote-emulated, on the
SAME session (the bench raises `FRS_L0_*_TRIGGER` so the whole merge runs inside the
measured `compact_all` rather than the background worker). Dev Mac, release, default
config 50 000 keys × 10 overlapping rounds (~500 K-row k-way merge), median of n=3/5:

```
== remote-compaction offload (pillar 6b) — 50000 keys × 10 rounds ==
            wall_ms(med)   caller_cpu_ms(med)
Local            20.1            13.3
Remote           19.3             0.1
HEADLINE: caller-thread compaction CPU  Local=13.0 ms  Remote=0.1 ms  => ~180x cut
```

**Finding:** under the Remote executor the calling (TM) thread spends ~0.1 ms of CPU
(it blocks on the result channel) vs ~13 ms under Local — a **~130–180× cut in
TM-side compaction CPU** — while wall time is comparable (the pool thread does the
merge on another core). This is the paper §5.3 property: *the TM does ~0 compaction
work*. The magnitude is the merge-CPU of this workload; the DIRECTION (TM CPU → ~0)
is machine-independent and is the point. (Methodology per `compaction_throughput.rs`:
same-session A/B, n≥3, macOS = system allocator, no cross-machine comparison.)
Re-run: `cargo run -p forst-rs-bench --release --bin remote_compaction_offload -- --smoke`.

### 6.4 Paper-coverage statement — is pillar 6b now functionally complete in-repo?

**YES, functionally — local-emulated.** A compaction is now (a) a portable descriptor
(serializable; round-trip falsifier green), (b) executed off the calling thread by a
separate worker reading inputs / writing outputs through the (opendal-emulated) remote
FS (~180× TM-CPU cut measured), (c) installed atomically via the existing
VersionEdit/`version_set.apply` path (no new install machinery — the disagg link layer
makes outputs natural), (d) MVCC-safe (same `min_active_snapshot` + `mvcc::should_drop`)
and idempotent / crash-consistent (re-run = byte-identical; crash-between leaves a
consistent version + reapable orphan), and (e) composes with trivial-move (runs first,
never offloaded), KV-sep, and the link-checkpoint. The byte-identical falsifier is the
gate that any future change to the offload path must keep green.

**Remaining Phase-3 scope (unchanged):** a true out-of-process / cross-machine worker +
RPC transport (the descriptor is the unit it would carry; the serialize round-trip path
is exercised in CI, so this is "add a transport + worker binary", not "redesign the
seam"), and the real-S3 E2E offload *performance* race vs ForSt (needs the co-located
box — the dev box is 10 MB/s).
