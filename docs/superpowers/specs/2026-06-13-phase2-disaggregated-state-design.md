# Phase-2: Disaggregated State for forst-rs — Design

**Date:** 2026-06-13
**Status:** Proposed (PMC kickoff)
**Program goal:** fully reproduce the Disaggregated State model of ForSt / Flink 2.0
(paper: *Disaggregated State Management in Apache Flink 2.0*, Mei et al., PVLDB 18(12):
4846–4859, 2025 — cited below as **[paper §x]**), then optimize to beat ForSt on S3.
**Phase-2 scope:** FUNCTIONAL reproduction + partial benchmarks. The E2E S3 performance
race is **Phase 3** and explicitly out of scope (current dev-box S3 access is ~10 MB/s
to BOS-Beijing — recorded 2026-06-01 via `crates/forst-rs-io/examples/s3bw.rs` — so
write/checkpoint-heavy S3 numbers from this box are not valid evidence).

---

## 0. The paper's model, distilled

The ForSt disaggregated model [paper §3.3, §5] has six pillars:

1. **DFS is the PRIMARY state home.** Active working state lives in a *Working
   Directory* on DFS (HDFS/S3/OSS); state updates are *streamed continuously* to DFS;
   local disk is an **optional, secondary cache**, never the source of truth
   [paper §3.3, Fig. 4].
2. **Unified File System (UFS).** A logical-file layer over diverse DFS backends that
   maintains **logical→physical mappings and reference counts**, providing hard-link
   semantics even on object stores that have none (S3) [paper §5.1].
3. **Checkpoint = metadata + file linking.** Because flushed SSTs are already durable
   on DFS, a checkpoint hard-links the live file set into the checkpoint directory —
   no data re-upload. JM deletion is *delegated* to the UFS; physical deletion happens
   only when the refcount reaches zero [paper §5.2, Fig. 8]. Result: checkpoints
   complete in seconds regardless of state size [paper §6.1, Fig. 9].
4. **Restore/rescale = link, not copy.** New instances link checkpoint files into their
   working directory and start instantly; cache warms lazily. 16–49× faster
   reconfiguration [paper §5.2, §6.1, Fig. 10].
5. **Local cache = block LRU in memory + file-based secondary cache on disk, governed
   by a History-Based Policy** (LRU eviction + frequency-based reload: files whose
   access frequency over the preceding minute exceeds a threshold are re-admitted);
   policy is pluggable [paper §5.4]. Even a 1 GB disk cache recovers most of the
   local-disk performance [paper §6.2, Fig. 13].
6. **Async execution (AEC)** hides remote latency (NVMe 68 µs vs OSS 23 ms,
   [paper §4, Table 1]); **remote compaction** moves compaction CPU off the TM
   [paper §5.3] (experimental in Flink [36]).

---

## 1. Architecture: the ForSt model mapped onto forst-rs

### 1.1 Target operating model

```
                TaskManager (per slot)
  ┌────────────────────────────────────────────────┐
  │  Flink async exec (AEC) — EXISTS (Flink 2.x    │
  │  fork + ForStRsStateExecutor V2 batched)       │
  │  ┌──────────────────────────────────────────┐  │
  │  │ forst-rs engine                          │  │
  │  │  memtable + WAL ──────── LOCAL (NVMe)    │  │
  │  │  block cache (ShardedClock) ── RAM       │  │
  │  │  LocalCache (bounded LRU file cache) ──┐ │  │
  │  └────────────────────────────────────────┼─┘  │
  └───────────────────────────────────────────┼────┘
                                              │ CachedFileSystem (read-through,
                                              │ write-through-to-remote)
              ┌───────────────────────────────▼───────────────┐
              │  DFS (S3/BOS/fs-emulation) — PRIMARY SST home │
              │  working dir  ◄── FileMappingManager links ──► checkpoint dirs │
              └───────────────────────────────────────────────┘
```

Remote is the SST home; checkpoint and working directories share physical objects
through a metadata linking layer; local disk only caches.

### 1.2 Component inventory — what exists / what's new (evidence-cited)

| # | Component (paper pillar) | Exists in forst-rs today | Evidence (file:line) | Gap / new work |
|---|---|---|---|---|
| A | Remote (S3) storage backend | **YES** — OpenDAL FS with s3/fs/memory schemes, retry wrapper, async upload tracking | `crates/forst-rs-io/src/opendal_backend.rs:198` (`OpendalFileSystem`), `:377` (`s3()`), `:362` (`local()` — fs emulation); scheme router `crates/forst-rs-io/src/router.rs:86,116,146`, `FileLocality`/`file_locality` `:568,579` | none structural; Stage-0 probe decides BOS vs fs-emulation |
| B | DB-on-remote with local cache (working dir on DFS) | **PARTIAL** — `DbImpl::open_remote` wires remote FS + LRU cache as the engine FS; but the *operating model* used in all benchmarks is local-primary (S3 only as checkpoint target), and checkpoints still re-upload | engine: `crates/forst-rs-engine/src/db.rs:987` (`open_remote`), `:1013-1022` (`build_opendal_fs_from_uri` → `CachedFileSystem::new`); FFI `crates/forst-rs-ffi/src/lib.rs:707` (`frs_db_open_remote`); Java `ForStRsOptions.java:98` (`storageUri`), `:144` (cache dir); bench config `scripts/bench-4way-s3.sh:9,98-100` (`forst-rs-ffm-s3` ← `config-forst-rs.yaml.tpl`) | make remote-primary the disagg operating mode; co-locate working dir + checkpoint namespace so files are linkable (§3) |
| C | Local write-through SST cache | **YES** — flush/compaction outputs populate the cache synchronously; reads never block on the S3 upload; bounded LRU with fd cache | `crates/forst-rs-storage/src/cached_fs.rs:66` (`CachedFileSystem`), `:115-130` (`FRS-LOCAL-FIRST-SST` / `LocalFirstSstFile:681`), `:95-109` (write-back read-race handling), `:1345` (write-through test); `crates/forst-rs-storage/src/local_cache.rs:69` (LRU, `capacity_bytes`), `:86,294` (FRS-FDCACHE) | eviction is pure LRU; **History-Based Policy missing** (paper §5.4): no frequency-based re-admission, policy not pluggable (§4.2) |
| D | Remote-aware read path | **YES** — BlockPrefetcher has a distinct remote regime (deep 4 MiB ramp vs 256 KiB local), keyed off `is_local()` | `crates/forst-rs-storage/src/sst/prefetch.rs:341` (`BlockPrefetcher`), `:356` (regime), `:373,403` (`is_local_file`), docs `:23-35` (cold/ramp, remote 64-block cap); RAM block cache `crates/forst-rs-storage/src/cache/clock.rs` (ShardedClock) | works when remote is rare (evicted files); needs cache-admission + miss-path hardening when S3 is primary and state ≫ cache budget (§4.1) |
| E | Incremental checkpoints | **YES** — manifest + `new_ssts`/`shared_ssts` split vs a base checkpoint; Java side maps to std `IncrementalRemoteKeyedStateHandle` + `SharedStateRegistry` | engine `crates/forst-rs-engine/src/db.rs:535-547` (`IncrementalCheckpointResult`), `:5287` (`create_incremental_checkpoint`); Java `ForStRsSnapshotStrategy.java:737` (`createIncrementalCheckpointAt`), `:803-806` (SHARED scope for new SSTs), `:901-910` (shared resolved from registry, NOT re-uploaded), `:1032` (`IncrementalRemoteKeyedStateHandle`) | incremental **by upload-dedup, not by linking**: `new_ssts` are still byte-copied from the local working dir into checkpoint storage via `ForStRsSstUploader` (`ForStRsSnapshotStrategy.java:70,102,790-812`); paper model uploads **nothing** at checkpoint time (§3) |
| F | Checkpoint-freeze decoupling | **YES (fixed)** — checkpoint awaits only its pinned live set's uploads, not all in-flight (compaction) uploads | `crates/forst-rs-engine/src/db.rs:5378-5390` (freeze-fix comment + per-file `await_upload` design; measured ckpt3 332 s → ~1.6 s), `:8237`; race-free pinning under `apply_lock` `:5392-5404` (R31-H1) | preserve invariant under the link-based redesign (§3.4, risk R4) |
| G | noflush / memtable-artifact checkpoints | **YES, but NOT incremental** — live memtable captured as a FULL Arrow-IPC artifact per checkpoint, uploaded EXCLUSIVE | engine `db.rs:5304` (`create_incremental_checkpoint_noflush`), `:4628` (`snapshot_memtables_to_dir`), `:4772` (`replay_memtable_artifacts_from_dir`); Java `ForStRsSnapshotStrategy.java:719,730` (artifact capture + noflush call), `:173` (`forst.rs.checkpoint.noflush` property), `:817-842` (EXCLUSIVE upload) | replace with WAL-tail durability (memtable **delta**, §3.3); WAL exists with Phase-3 ckpt barrier (`db.rs:5337-5349`, `wal_sync` `:5376`) but **restore replay (WAL Phase 4) is incomplete** (`db.rs:5344-5347` comment) |
| H | File-mapping / ownership layer (UFS) | **NO (scaffold only)** — a 3-level ownership model exists but is **unwired** (zero call sites beyond the re-export); engine refcounts are process-local pins for deletion safety, not cross-checkpoint links | `crates/forst-rs-io/src/ownership.rs:68` (`PrivateOwnedByDb`/`ShareableOwnedByDb`/`NotOwned`), `:141` (`FileOwnershipTracker`); only consumer: `crates/forst-rs-io/src/lib.rs:50` (re-export); process-local pinning: `crates/forst-rs-engine/src/file_deletion_guard.rs:41-109` (`pin`/`can_delete`/`pin_batch`) | **the core Phase-2 build**: durable logical→physical mapping + refcounts + link/unlink + GC, à la ForSt `FileMappingManager` (§2) |
| I | Restore | **PARTIAL** — manifest-driven restore exists but **downloads every SST** | engine `db.rs:5755` (`open_from_incremental`); Java `ForStRsRestoreOperation.java:74-78` ("download manifest + each SST") | instant-link restore + lazy cache warm (§5 Stage 3); rescale-by-clip |
| J | Async execution model (AEC) | **YES** — the Flink 2.x fork's async exec + forst-rs V2 batched state executor are the production path (NexMark runs on it) | `ForStRsStateExecutor.java`, `ForStRsStateRequestClassifier.java`, `async/`, `exec/` under `flink-statebackend-forst-rs/.../forstrs/` | no Phase-2 work |
| K | Remote compaction | **NO** | — | explicitly deferred (paper marks it experimental; §5 Stage 6 stub, Phase-3+) |

**Net Phase-2 build list:** H (FileMappingManager) → E′ (link-based checkpoint) →
I′ (link-based restore/rescale) → G′ (WAL-delta memtable durability) → C′
(history-based cache policy), each over the remote-primary operating mode (B′).

---

## 2. The file-mapping / ownership layer (new — pillar 2)

### 2.1 Why a metadata layer, not hard links

S3/BOS have no hard links; the paper's UFS solves exactly this: *"The UFS maintains
the mapping and reference counts between logical files and their physical locations…
efficient move or link operations without necessitating physical file relocation or
data duplication"* [paper §5.1]. We implement the same as a forst-rs metadata layer.

### 2.2 Design

New module `crates/forst-rs-io/src/file_mapping.rs` (`FileMappingManager`), wired into
the FS stack between the engine and `CachedFileSystem`/`OpendalFileSystem`:

```rust
/// One physical object on the DFS.
struct PhysicalFile { key: String /* object key */, size: u64, refs: u32 }

/// Logical path (working-dir or checkpoint-dir path) -> physical object.
pub struct FileMappingManager {
    map: Mutex<HashMap<LogicalPath, Arc<PhysicalEntry>>>,
    journal: MappingJournal,   // durable, see §2.4
}

impl FileMappingManager {
    /// Working-dir create: identity mapping, refs = 1.
    pub fn register(&self, logical: &Path, physical_key: &str, size: u64);
    /// Checkpoint link: new logical name -> SAME physical, refs += 1. O(metadata).
    pub fn link(&self, src_logical: &Path, dst_logical: &Path) -> ForstResult<()>;
    /// Unlink: refs -= 1; at 0 → physical delete delegated to backing FS.
    pub fn unlink(&self, logical: &Path) -> ForstResult<UnlinkOutcome>;
    /// Restore link: adopt an existing physical object under a new working path.
    pub fn adopt(&self, logical: &Path, physical_key: &str) -> ForstResult<()>;
}
```

- **Ownership semantics** reuse the existing (unwired) model
  (`ownership.rs:68`): working-dir SSTs are `ShareableOwnedByDb`; once a checkpoint
  links them their lifecycle is governed by refcount; objects adopted from a restored
  checkpoint enter as `NotOwned` until re-linked into the new working namespace.
- **Relation to `FileDeletionGuard`** (`file_deletion_guard.rs:41`): the guard stays —
  it protects *process-local readers/snapshots* from unlink-under-read. The mapping
  manager governs *durable* lifetime. Deletion order: engine decides a file is dead
  (compaction obsoletes it) → `delete_file_guarded` (process-local safety, unchanged)
  → instead of FS `delete_file`, call `unlink(working_path)` → physical delete only
  at refs==0.

### 2.3 Flink SharedStateRegistry interplay (paper Fig. 8 mapped to Flink reality)

Today shared SSTs are deduplicated by *upload identity* in the registry
(`ForStRsSnapshotStrategy.java:901-910`). Under linking:

1. At checkpoint *k*, the backend `link()`s each live SST into
   `<ckpt-root>/chk-k/` and emits a `StreamStateHandle` whose location is the
   **checkpoint-namespace logical path** (not the working path) — registered SHARED
   exactly as now (`:803-806`), so JM/registry code is untouched.
2. JM discards checkpoint *k* → registry calls `discardState()` on unreferenced
   handles → our handle's discard is **delegated** to the mapping layer:
   `unlink(<chk-k>/file)` (paper: "the JM delegates the deletion to the UFS",
   §5.2). The TM-side mapping service performs physical deletion at refs==0.
   Because JM-side discard may outlive the TM, the discard call routes through a
   small ownership protocol: handles carry the physical key; discard writes a
   tombstone the mapping journal GC consumes (§2.4) — never a direct S3 delete.
3. Working-dir compaction obsoletes a linked SST → `unlink(working_path)` only;
   physical bytes survive while any checkpoint refs them (refs>0). This is exactly
   the Fig. 8 ①–⑥ flow.

### 2.4 Durability of the mapping itself

The mapping must survive TM crash without becoming the new consistency hazard:

- **Journal**: append-only mapping journal (link/unlink records) stored next to the
  working dir on DFS, checkpointed into the manifest blob the engine already writes
  (`CHECKPOINT.blob`, `db.rs:5282-5286`) so every checkpoint embeds a consistent
  mapping snapshot; journal tail replayed on restore.
- **Crash GC**: orphaned physicals (registered, refs never linked by any surviving
  checkpoint, working dir gone) are reaped by a startup sweep comparing the DFS
  listing against the union of live checkpoint manifests — same job the existing
  `FileOwnershipTracker` enum was designed to label.

---

## 3. Checkpoint path redesign (pillar 3)

### 3.1 Today's flow (data-copy) vs target (link)

Today: engine computes `new_ssts`/`shared_ssts` (`db.rs:5287`); Java uploads manifest +
every `new_sst` byte-for-byte via `ForStRsSstUploader` (`ForStRsSnapshotStrategy.java:790-812`)
and the full memtable artifact (`:817-842`). Upload time ∝ new-SST bytes — this is the
Flink-1.x shape the paper measures at 30–50 s tails [paper §6.1, Fig. 9].

Target (remote-primary): SSTs were **streamed to DFS at flush/compaction time**
(write-through already does this: `cached_fs.rs:33-40` pass-through writes +
async upload with `await_upload` barriers). Checkpoint *k* becomes:

1. `wal_sync()` or flush-on-barrier (mode choice, §3.3) — durability of the tail.
2. Await uploads of **only the pinned live set** — the existing freeze-fix barrier
   (`db.rs:5378-5390`) is kept verbatim; in steady state these uploads completed long
   ago, so this is a no-op check, not a stall.
3. Write the manifest (already exists).
4. `link()` every live SST + manifest into `<ckpt-root>/chk-k/` — **O(files) metadata
   ops, zero data movement**.
5. Return handles pointing at the linked logical paths; `shared_ssts` vs `new_ssts`
   distinction survives only as a registry-registration hint — *neither* is uploaded.

`stage_checkpoint_artifacts_local` (`db.rs:5582+`) and the uploader path remain for
local-primary mode (unchanged default until Phase 3).

### 3.2 Why this preserves the freeze fix

The 2026-06-02 freeze (`db.rs:5382-5390`: blanket `await_all_uploads` drained
background compaction uploads; ckpt3 = 332 s) taught: checkpoint latency must be
independent of unrelated I/O. Linking strengthens this — the only synchronous waits
left are (a) the pinned-set `await_upload` no-op check and (b) metadata-op round
trips. **Invariant (kept as a test): no checkpoint code path may call
`await_all_uploads`** (regression test exists at `db.rs:15668-15753` — the
upload-counting mock FS; extend it to the link path).

### 3.3 Memtable durability: flush-on-barrier vs WAL-delta (recorded trade-offs)

Two modes, config-selected, both must exist because the evidence cuts both ways:

| Mode | Mechanics | Recorded evidence |
|---|---|---|
| **FLUSH (flush-on-barrier)** | barrier forces memtable→L0 SST (already durable via write-through) → linked like any SST | q4 A/B (2026-06-06): checkpoint-flush is **load-bearing** — ckpt OFF 1486 s/44 GB vs ckpt-30 s 557 s/33 GB; flushing bounds memtables. Default for memory-tight boxes (8c/32g: `noflush=false`). |
| **WAL-DELTA** | checkpoint = `wal_sync` (`db.rs:5376`) + link WAL segments; memtable NOT flushed, NOT uploaded; restore replays the tail | q4 noflush won on host (362 s vs 557 s) but peaks 44 GB (memtable resident) — exceeds 8c/32g; and the current noflush artifact is a **full-memtable copy per checkpoint** (`snapshot_memtables_to_dir db.rs:4628`) — the recorded non-incremental gap. WAL-delta replaces it: per-checkpoint cost = fsync + link, **independent of memtable size**. Blocker: WAL Phase 4 (restore replay) is incomplete (`db.rs:5344-5347`). |

Phase-2 ships FLUSH as the correct-by-construction default and completes WAL Phase 4
to enable WAL-DELTA behind the existing `FRS_WAL_DIR` gate. The Arrow-IPC artifact
path stays only as a deprecated fallback for local-primary noflush users.

### 3.4 Upload pipelining

With remote-primary, the upload pipeline moves from "checkpoint asset" to "flush
asset": a flush is not *complete* (its SST not linkable) until its upload lands.
Backpressure therefore shifts to the flush/WBM path — which is where the engine's
write controller already lives (`write_controller.rs`). No new mechanism; the
existing async upload + `await_upload`-per-file design is exactly the pipelining the
paper implies ("streams state updates continuously to the DFS", §3.3, §5.2). Risk R4
covers the bandwidth-starved regime.

---

## 4. Read path on cache miss when S3 is primary (pillar 5)

### 4.1 What changes

Foundation that stays: `BlockPrefetcher` remote regime (deep 4 MiB ramp,
`prefetch.rs:23-35,341-427`), `LocalFirstSstFile` local-first reads
(`cached_fs.rs:115-130`), ShardedClock RAM block cache, and the write-through
population that guarantees **newly flushed/compacted SSTs are always cache-hits**
(`cached_fs.rs:1345` test). What must change when state ≫ cache budget:

1. **Miss granularity.** Today a `LocalCache` miss fetches the **entire file**
   (`cached_fs.rs:28-32`), and over-budget files "fall through to the remote backend
   on every read" (`:23-24`). With S3-primary, whole-file fetch on a cold 64 MB SST
   ahead of a 1-block probe is the wrong default. Add **admission-gated fetch**: a
   miss serves the needed range via the remote `RandomAccessFile` (pass-through,
   prefetcher's remote regime applies), and *separately* the admission policy decides
   whether to schedule a background whole-file fill. This is precisely the paper's
   split between serving reads and *loading* files into cache [paper §5.4].
2. **History-Based Policy** [paper §5.4]: eviction stays LRU
   (`local_cache.rs:162-193` deque machinery reused); **loading** becomes
   frequency-driven — track per-file access counts over a sliding ~1-minute window;
   files on the remote exceeding a threshold are admitted (background fill). This
   kills the cache-thrash failure mode the paper calls out, and matches the recorded
   q9 lesson (2026-05-31: state past the cache budget living S3-only collapsed
   throughput until write-through landed).
3. **Pluggable policy trait** so LRU-only (today) vs history-based can be A/B'd:
   `trait CacheAdmission { fn on_access(&self, key, locality); fn should_admit(&self, key) -> bool; }`.

### 4.2 What explicitly does NOT change in Phase-2

No parallel read threads / coalesced-multiget redesign here — that is the separate
read-path architecture track (2026-06-08 parallel+coalesced design), and recorded
evidence says profile-first. Phase-2's read-path bar is *functional correctness +
no-collapse* under cache pressure, not S3 latency wins (Phase 3).

---

## 5. Staged plan (functional-first; each stage has UT/IT gates + a partial benchmark)

Benchmark substrate for ALL stages: **fs-emulation by default** —
`OpendalFileSystem::local` (`opendal_backend.rs:362`) rooted on a tmp dir gives the
full remote code path (async uploads, await barriers, mapping layer) minus network;
BOS only if Stage 0 proves it usable. NexMark runs use the existing
`forst-rs-ffm-s3` harness (`scripts/bench-4way-s3.sh:9,98-100`).

### Stage 0 — S3-reachability probe (gate for everything else)
- **Build:** `scripts/probe-s3.sh` wrapping `cargo run -p forst-rs-io --example s3bw`
  (`crates/forst-rs-io/examples/s3bw.rs`): measure RTT, single-stream up/down MB/s,
  PUT/DELETE metadata-op latency against `$S3_ENDPOINT`. Decision matrix written to
  the report: ≥100 MB/s & RTT <5 ms → BOS usable for partial benches; else
  fs-emulation only (recorded baseline: dev-Mac→BOS = 10.2 MB/s, 23 ms — emulation).
  Also probe **read-after-write visibility** (PUT→GET loop) — feeds risk R2.
- **Gate:** script committed + one recorded probe report. No code changes.

### Stage 1 — FileMappingManager (§2)
- **Build:** `forst-rs-io/src/file_mapping.rs` + journal; wire `FileOwnershipTracker`
  semantics; route engine `delete_file_guarded` physical deletes through `unlink`.
- **UT gates:** link/unlink/refcount; unlink-at-refs>0 keeps bytes; adopt; journal
  replay idempotence; concurrent link-vs-compaction-delete (pin interplay with
  `FileDeletionGuard`); crash-GC sweep reaps orphans, never reaps refs>0.
- **IT gate:** engine-level — checkpoint→link→compact-away-working-copy→read-via-
  checkpoint-link still byte-exact; discard last ref → physical gone exactly once.
- **Partial bench:** metadata-op cost: 10k-file link vs 10k-file copy on fs-emulation
  (expect ≥100× — this is the headline mechanism).

### Stage 2 — Link-based checkpoint on remote-primary (§3.1–3.2)
- **Build:** engine `create_incremental_checkpoint` gains a `link_mode` that emits
  linked checkpoint paths instead of staging/upload lists; Java
  `ForStRsSnapshotStrategy` gains a zero-upload branch (handles wrap linked DFS
  paths; SHARED registration unchanged); `storageUri` mode flips the NexMark harness
  config (new template variant beside `config-forst-rs.yaml.tpl`).
- **UT gates:** no-`await_all_uploads` invariant on the link path (extend the
  `db.rs:15668` mock-FS counter test); manifest never references an un-uploaded
  physical (pinned-set barrier still enforced).
- **IT gates:** checkpoint→restore round-trip byte-exact on fs-emulation; JM
  discard→refcount→delete via a SharedStateRegistry harness test; chained
  checkpoints share physicals (assert object count, not just handle dedup).
- **Partial bench:** checkpoint duration vs state size (1/4/16 GB synthetic): must be
  ~flat (metadata-bound) vs today's linear upload — the paper's Fig. 9 claim,
  reproduced on emulation. Plus q3+q5-class NexMark 5M correctness sweep (the
  recorded correctness-before-perf rule).

### Stage 3 — Instant-link restore + rescale (§1.2-I)
- **Build:** `open_from_incremental` link-mode: `adopt()` physicals into the new
  working namespace, open readers lazily through `CachedFileSystem` (no downloads);
  Java `ForStRsRestoreOperation` skips the download loop; rescale = link the
  key-group-clipped subset (clip filter at iterator level; compaction prunes).
- **IT gates:** restore correctness (exact counts) with a COLD cache; rescale in/out
  parallelism 2↔4 with key-group ownership asserts; restore-under-missing-object
  fails loudly (no silent empty state).
- **Partial bench:** restore wall-time vs state size — must be ~flat (paper Fig. 10
  shape) on emulation; record cache-warm tail separately.

### Stage 4 — WAL-delta memtable durability (§3.3)
- **Build:** WAL Phase 4 (restore replay of the tail, completing `db.rs:5344-5347`);
  checkpoint-mode flag (FLUSH default, WAL-DELTA gated); deprecate the full-memtable
  Arrow artifact for remote-primary.
- **UT/IT gates:** kill-after-ckpt→replay→byte-exact; WAL GC after covering ckpt;
  mode A/B correctness on q4-class workload at 5M.
- **Partial bench:** per-checkpoint cost vs memtable size in both modes (expect
  WAL-DELTA flat); q4 10M A/B citing the recorded flush-is-load-bearing trade-off
  (memory ceiling watched: 8c/32g profile).

### Stage 5 — History-based cache policy + cache-pressure soak (§4)
- **Build:** admission-gated miss path + frequency-reload policy + pluggable trait.
- **UT gates:** policy unit tests (window accounting, threshold admit, thrash
  scenario from the paper: scan working set 2× cache budget — LRU thrashes,
  history-based stabilizes).
- **IT/partial bench:** q9/q20-class join at 10M on fs-emulation with cache budget
  deliberately ≪ state: no freeze, no collapse (the 2026-05-31 q9 failure mode as a
  regression test), throughput within a recorded band of local-primary.

### Stage 6 — (boundary) remote-compaction stub + E2E S3 race — **Phase 3, out of scope**
Design stub only (§1.2-K): compaction request = input-manifest of DFS-resident files;
stateless compactor reads/writes DFS; backend applies the VersionEdit. Nothing built
in Phase-2.

---

## 6. Risks & Phase-1 interactions

- **R1 — Mapping-journal consistency is the new single point of truth.** A bug here
  is data loss (premature delete) or unbounded leak. Mitigations: journal embedded in
  the checkpoint blob (atomic with the manifest), tombstone-based JM discard (§2.3),
  startup GC sweep with refs>0 hard-stop, and the Stage-1 exactly-once-delete IT.
- **R2 — Object-store visibility semantics.** The paper flags S3 eventual visibility
  [§5.1]. Modern AWS S3 is strong read-after-write; **BOS must be probed** (Stage 0
  PUT→GET loop). The existing write-back read-race retry (`cached_fs.rs:95-109`)
  already handles the in-process window; cross-process (restore reading a just-linked
  object) relies on store semantics — if BOS is eventual, restore adds a
  bounded-retry stat loop.
- **R3 — SharedStateRegistry double-delete/leak across restarts.** Handles now carry
  physical keys whose lifetime spans jobs. The registry harness IT (Stage 2) +
  tombstone protocol cover it; leak detection = Stage-1 GC sweep report.
- **R4 — Freeze-class regressions return via the flush path.** Remote-primary makes
  flush completion depend on upload bandwidth; a slow uplink turns WBM backpressure
  into a stall (this is *correct* behavior — write-stall — but must be the engine's
  governed backpressure, not a checkpoint freeze). Mitigation: no-`await_all_uploads`
  invariant test (Stage 2), write-controller owns the stall, and the Phase-1
  memory-model work (allow_stall-style backpressure, 2026-06-08 design) is the
  governing mechanism. Benchmarks on slow links are explicitly invalid (Stage 0).
- **R5 — Phase-1 coupling: pending sorted-run/leveled redesign.** The q7 analysis
  (docs/superpowers/specs/2026-06-12-forst-architecture-q7-analysis.md:23-26,118)
  recommends moving from size-tiered L1..Ln (L0 slowdown 40/stop 64,
  `write_controller.rs:95-97`) toward leveled discipline; the design is held at an
  acceptance gate (commit 0e3a11b80). **Coupling analysis:** the mapping layer is
  file-id-granular and LSM-shape-agnostic — link/refcount semantics are identical
  under tiered or leveled, so Stages 1–4 do not block on it. What DOES couple:
  (a) leveled compaction rewrites more bytes → higher steady upload bandwidth demand
  (R4 interacts); (b) smaller target files (Phase-1 close design module B: 64 MB SSTs)
  → more objects → more metadata ops per checkpoint/restore and more S3 PUT/DELETE
  cost — Stage-2/3 benches must be re-run after leveled lands, and the Stage-2
  metadata-op bench gives the per-file cost constant to extrapolate with.
  Decision: **decouple** — build Phase-2 on the current LSM shape; re-validate
  benches post-leveled.
- **R6 — Performance regression risk for local-primary users.** Remote-primary is a
  mode behind `storageUri` (already optional, `ForStRsOptions.java:98`); the
  local-primary path and its NexMark numbers stay the default until Phase 3 proves
  the race. All new code paths flag-gated; existing tests must stay green.
- **R7 — Dev-box S3 invalidity.** All Phase-2 partial benchmarks are emulation-first
  by design; any BOS numbers are annotated with the Stage-0 probe report. The S3 perf
  race needs the co-located box (Phase 3).

---

## 7. Acceptance summary for Phase-2

Phase-2 is DONE when: (1) Stages 0–5 gates green; (2) on fs-emulation, checkpoint and
restore wall-times are ~flat in state size (link-mode), with zero data re-upload for
unchanged SSTs (object-count assert); (3) q0–q22 5M correctness sweep passes in
remote-primary link-mode; (4) local-primary defaults and benchmarks unchanged. The S3
performance race vs ForSt — including remote compaction — is Phase 3.

---

## 8. Evidence (stage gates, recorded as they land)

### Stage 0 — probe script committed + recorded probe report

`scripts/probe-s3.sh` wraps `crates/forst-rs-io/examples/s3bw.rs --probe`
(RTT / up+down MB/s / PUT+DELETE latency / read-after-write visibility) and
applies the §Stage-0 decision matrix. Recorded fs-emulation SELF-TEST report
(dev Mac, 2026-06-12 — validates the probe machinery end-to-end; NOT S3
evidence; the BOS verdict must be re-recorded on the co-located remote box):

```
PROBE rtt_ms_median=0.019 rtt_ms_p90=0.033 rtt_samples=20
PROBE put_ms_median=4.016 put_ms_p90=5.289 put_bytes=4096 put_samples=20
PROBE raw_visibility=strong raw_max_attempts=1 raw_trials=10
PROBE upload_mbps=1300.4 upload_mb=50 upload_secs=0.04
PROBE download_mbps=3832.9 download_mb=50 download_secs=0.01
PROBE delete_ms_median=0.062 delete_ms_p90=0.103 delete_samples=20
probe-s3 REPORT: mode=selftest rtt_ms_median=0.019 upload_mbps=1300.4
  download_mbps=3832.9 raw_visibility=strong decision=bos-usable
```

Standing baseline for the dev box stays as recorded 2026-06-01: dev-Mac→BOS =
10.2 MB/s, ~23 ms RTT ⇒ **fs-emulation only** on this box (decision matrix
would emit `fs-emulation-only`); all Stage-1+ partial benches below are
emulation-based per §5.

### Stage 1 — FileMappingManager (landed)

Built (`crates/forst-rs-io/src/file_mapping.rs` + engine wiring in
`crates/forst-rs-engine/src/db.rs` / `checkpoint.rs`):

- `FileMappingManager` — durable logical→physical mapping, derived refcounts
  (refs == |logical links|, which makes journal replay idempotent),
  `register`/`link`/`unlink`/`adopt`, JM-discard `tombstone` protocol (§2.3 —
  never a direct S3 delete), `gc_sweep` (refs>0 hard-stop), CRC-framed
  append-only journal (truncated-tail tolerant) + `snapshot_bytes`/
  `restore_snapshot`.
- `FileOwnershipTracker` (previously dormant, `ownership.rs:141`) is now the
  layer's ownership authority: register → `ShareableOwnedByDb`, adopt →
  `NotOwned` (drain keeps bytes unless tombstoned).
- Engine: `DbImpl::attach_file_mapping` (OnceLock, **None by default — Stage-1
  inert**); `delete_file_guarded`/`reap_pending_deletions` route through the
  single `delete_sst_physical` choke point → `unlink()` when the working path
  is registered; `mapping_register_live_ssts()` = the Stage-2 working-dir
  registration hook. Mapping snapshot embedded in `CHECKPOINT.blob` via a
  tail envelope (`append_mapping_trailer`/`split_mapping_trailer`; legacy
  blobs pass through unchanged; no-mapping blobs byte-identical).

Gates green (2026-06-12):

- UT (forst-rs-io): link/unlink/refcount, unlink-at-refs>0 keeps bytes,
  adopt/NotOwned, tombstone defer/immediate/overrides-NotOwned, journal replay
  + double-apply idempotence + torn-tail tolerance, snapshot round-trip +
  corruption reject, **concurrent link-vs-unlink race** (8 rounds × 4 linker
  threads: bytes exist iff refs>0; delete only when no link landed), GC sweep
  reaps orphans/never refs>0.
- IT (forst-rs-engine, `db.rs`):
  `test_phase2_s1_mapping_checkpoint_link_lifecycle` — checkpoint→link→
  compact-away-working-copy→read-via-checkpoint-link byte-exact; pinned input
  defers unlink (FileDeletionGuard interplay); **last-ref discard deletes the
  physical exactly once** (second unlink = NotFound).
  `test_phase2_s1_checkpoint_blob_mapping_trailer_roundtrip` — trailer embeds
  + restore strips transparently.
- **10k-file link-vs-copy bench pair** (fs-emulation, dev Mac,
  `examples/link_vs_copy.rs`, 64 KiB files):

  ```
  LINKBENCH n=10000 size_kb=64 copy_total_ms=45203.0 link_total_ms=14.3
            copy_per_file_us=4520.3 link_per_file_us=1.4 speedup=3172x
  ```

  ≥100× expectation exceeded (3172×) — the headline link-mechanism cost
  constant for R5's per-file extrapolation is ~1.4 µs/link (+ journal append).

### ForSt-mechanism adoption #1 (competitive analysis §2.1 items a+b) —
### requester-class exemption + write-only admission (landed, default-OFF)

Built (`crates/forst-rs-storage/src/requester.rs`, `local_cache.rs`,
`cached_fs.rs`; engine wiring `bg_pool.rs` + the two pool constructors in
`db.rs`):

- **Requester class** (`requester.rs`): thread-local background mark
  (`mark_thread_background` sticky / `BackgroundScope` RAII). Engine flush +
  compaction pool workers are marked (`WorkerPool::new_background`); the
  read pool (foreground parallel operator reads) is NOT. Advisory — inert
  unless a cache policy opts in.
- **`CachePolicy`** on `LocalCache` (env: `FRS_CACHE_BG_EXEMPT=1`,
  `FRS_CACHE_ADMISSION=1` + `_PROMOTE`/`_EVICT_LIMIT`/`_TRACKER_CAP`; both
  default OFF ⇒ byte-identical legacy behavior; explicit
  `open_with_policy` for tests):
  - *bg-exempt* (ForSt §2.1.4): background accesses don't promote LRU order,
    don't accumulate admission credit, and are excluded from the headline
    hit-rate stats (separate `bg_hits/bg_misses`).
  - *read-fill admission* (ForSt §2.1.1-2.1.3 write-only admission +
    count-to-promote + promoteLimit): demand-read miss fills (whole-file
    `fetch_through_cache_gated` + per-chunk fills) are admitted only after
    `access_before_promote` (default 2) foreground touches; a key evicted
    ≥ `promote_limit` (default 3) times is blocked (anti-thrash cap; FIFO
    tracker aging = the cheap stand-in for ForSt's epoch decay).
    Write-through (`CachePopulatingWritableFile`) and explicit prefetch
    (`ensure_cached`/`prefetch_files*`) always admit.

Gates green (2026-06-12): 14 new UTs (bg-no-promote on get/get_range/
file_handle paths, count-to-promote, promote-limit permanent block,
bg-no-credit, write-path-never-gated, paper-§5.4 thrash scenario LRU-vs-
admission, tracker bound, demand-vs-prefetch-vs-write-through FS
integration, chunk-path pass-through-while-rejected, bg-pool marking);
full forst-rs-storage suite 432/0; engine suite green; clippy clean.

**Hit-rate minibench** (`examples/cache_admission_bench.rs`, dev Mac,
release; hot=32 files ≤ budget=64×64 KiB, bg compaction-scan 128 files=2×
budget per round, fg cold scan 2× budget every 3rd round, 12 rounds):

```
ADMBENCH cell=legacy-lru          fg_hot_hit_rate=88.5% fills=2400 fill_mb=150.0 elapsed_ms=19413
ADMBENCH cell=bg-exempt           fg_hot_hit_rate=88.5% fills=2400 fill_mb=150.0 elapsed_ms=19559
ADMBENCH cell=admission           fg_hot_hit_rate=37.5% fills=704  fill_mb=44.0  elapsed_ms=5782
ADMBENCH cell=bg-exempt+admission fg_hot_hit_rate=97.9% fills=256  fill_mb=16.0  elapsed_ms=2170
```

Findings (recorded): (1) the PAIR is the mechanism — +9.4 pp fg hot-set hit
rate and **9.4× less cache-fill churn** (150→16 MB; wall 19.4→2.2 s, churn
fsyncs dominate) vs legacy; (2) bg-exempt ALONE is a no-op here because
miss-FILLS (not hit-touches) do the evicting — confirms ForSt's write-only
admission is the load-bearing half; (3) **admission WITHOUT bg-exempt is an
anti-config** (−51 pp): unexempted scan touches earn admission credit, their
fills evict the hot set, and the thrash cap then permanently blocks the
re-faulted HOT keys. Ops rule: `FRS_CACHE_ADMISSION` must ship with
`FRS_CACHE_BG_EXEMPT`.

**Follow-up unit (same session): batch-prefetch gating + engine-level
cache-pressure IT.**

- `prefetch_files_concurrent` (the engine's PER-BATCH warm,
  `prefetch_sst_files_for_batch → prefetch_concurrent`) is a demand read in
  disguise — under budget ≪ state, force-filling whole SSTs per batch IS the
  thrash driver. It now consults `admit_read_fill` per miss when admission
  is enabled (rejected paths simply aren't fetched; reads serve pass-through
  at chunk granularity). Serial explicit warms (`ensure_cached`,
  `prefetch_files`) keep force-admit. UT: 1st wave no-fill / 2nd wave fills.
- **Stage-5 partial IT** (`crates/forst-rs-engine/tests/`
  `cache_admission_pressure_it.rs`, fs-emulation memory://, 32 incompressible
  ~128 KiB SSTs vs a 256 KiB cache budget, resident shadow clamped to 1 MiB):
  (1) legacy default cell byte-exact across 3 get passes + batch_get with the
  admission machinery provably inert — the 2026-05-31 q9 "state ≫ cache"
  collapse shape as a correctness regression test; (2) admission cell
  byte-exact + budget bound + gate ENGAGED by a checkpoint-staging-class
  whole-file sweep (first-touch rejections, admitted ≤ rejected); (3)
  cold-cache eviction regime (all write-through entries invalidated)
  byte-exact via the reader remote-fallback paths.
- **Coverage finding (recorded):** the engine opens SST readers EAGERLY at
  flush time against the write-through copy, and post-eviction reads go
  through `LocalFirstSstFile`'s direct remote fallback — neither is a cache
  demand-FILL, so steady-state point reads never consult admission in the
  current composition. Admission governs: staging/sequential whole-file
  reads, chunk-path readers opened post-eviction (restore-class), and the
  per-batch concurrent warm. The q9-class 10M soak on a real box remains
  open Stage-5 scope, as does the §4.1.1 background-fill scheduler and the
  pluggable-policy trait formalization.
- **Stage-3 prep — restore pre-seed API** (ForSt §2.1.6
  `registerInCache`): `LocalCache::pre_seed_admission(key)` seeds the
  tracker to threshold−1 so a restored file's FIRST foreground touch
  admits its load-back (never resurrects a promote-limit-blocked key;
  background touches still never admit; pure no-op when admission is off).
  Ready for the Stage-3 restore path to call per linked SST. UT covers all
  four properties.

### Stage 2 — Link-based checkpoint on remote-primary (landed, per §9 decisions)

Built (engine `crates/forst-rs-engine/src/db.rs`, io
`crates/forst-rs-io/src/file_mapping.rs`):

- `create_incremental_checkpoint_linked` + double-keyed gate (D2:
  `FRS_CKPT_LINK_MODE=1` AND owner-attached mapping; noflush variant never
  routes; WAL+link rejected until WAL Phase 4). Default path byte-identical
  when off (gate test).
- Link-mode barrier sequence (D5): pinned-set `await_upload` → `register()`
  live set → `link()` into `<db_path>/checkpoints/<chk-id>/` → `sync_journal()`
  → manifest blob with embedded mapping trailer. ZERO data movement: the chk
  dir physically contains exactly one file (CHECKPOINT.blob); handles are
  `linked_new_ssts`/`linked_shared_ssts` at chk-namespace logical paths;
  upload lists empty (D3).
- Discard flow (D4): `discard_linked_checkpoint` = manifest-driven unlink
  loop; physical delete exactly once at refs==0; JM-unreachable fallback =
  journal tombstone (Stage-1 machinery).
- Minimal restore (D7): `open_from_linked_checkpoint` — resolve linked paths
  via the blob's embedded `MappingSnapshotView`, materialize-by-copy,
  fail-loud on missing physicals. Adopt+lazy-read instant restore = Stage 3.

Gates green (2026-06-12):

- **no-`await_all_uploads` invariant extended to the link path** (the Stage-2
  gate on the `UploadRecordingFs` mock): link-mode checkpoint makes ZERO
  `await_all_uploads` calls AND still awaits every pinned live SST per-file;
  zero-upload object-count assert: chk dir = exactly `CHECKPOINT.blob`,
  linked paths are metadata-only (no bytes) and resolve through the mapping.
- **Registry discard→refcount→delete IT**: chained chk-1/chk-2 share
  PHYSICALS (handle count exceeds object count; shared SST refs ≥ 3); discard
  chk-1 deletes nothing (chk-2 + working refs); working copies compacted away
  → bytes survive on chk-2's ref alone; JM-style tombstone with refs held
  defers; discard chk-2 → every physical deleted exactly once; retried
  discard = NotFound.
- **Checkpoint→restore round-trip byte-exact** on the linked layout
  (overwrites + deletes at snapshot time; post-checkpoint churn/compaction
  does not leak into the restore); restored engine writable;
  restore-under-missing-object fails loudly (no silent empty state).
- Engine-level correctness ITs stand in for the 5M NexMark sweep (needs the
  Java zero-upload branch — Stage 3; no flink writes in Stage 2).

**Checkpoint-duration-vs-state-size minibench** (the §5 Stage-2 "~flat" gate;
fs-emulation on local FS, incompressible 4 KiB values, median of 3,
`crates/forst-rs-engine/examples/ckpt_link_flat_bench.rs`, dev Mac
2026-06-12):

```
scale    ssts   state_mb   copy_median_ms   link_median_ms     ratio
1x          8       32.3             63.8             16.0        4x
4x         32      129.2            205.1             16.1       13x
16x        11      516.9            344.1             16.0       21x
```

Re-validated at adoption (2026-06-13, fresh build of the salvaged worktree):
link stays flat (14.9 / 16.0 / 16.0 ms) while copy grows 58.8 → 180.0 →
476.7 ms (ratio 4× / 11× / 30×; the 16× live set compacted to 9 files).

LINK mode is **flat in state size** (16.0 / 16.1 / 16.0 ms across a 16×
state-size sweep — the paper's Fig. 9 shape reproduced) while COPY mode grows
with bytes (64 → 205 → 344 ms; sub-linear only because the 16× run's live set
compacted to 11 larger files and local page-cache copies are cheap — on a
real uplink copy cost is bandwidth-bound, recorded 2026-06-01 at 10 MB/s).
Zero data re-upload for unchanged files is asserted structurally (object
count), not inferred from timing.

**Stage-3 needs (recorded):** Java `ForStRsSnapshotStrategy` zero-upload
branch consuming `linked_*` handles + `ForStRsRestoreOperation` download-loop
skip; adopt()+lazy-read instant restore (replace D7's copy); mapping-journal
tail replay on restore; startup sweep reaping abandoned chk-k link leaks
(D5 crash window a); FFI surface for linked handles + attach-at-open wiring
so the env key becomes effective end-to-end.

### Stage 3 — Instant-link restore (engine side, landed 2026-06-13)

Built (engine `crates/forst-rs-engine/src/db.rs`, io
`crates/forst-rs-io/src/file_mapping.rs` + `filesystem.rs`, storage
`crates/forst-rs-storage/src/cached_fs.rs`):

- **`MappedFileSystem`** (io) — the UFS read-path indirection (paper §5.1):
  resolves logical paths through the `FileMappingManager` on READ-side ops
  only (`open_*_file` reads, `file_exists`, metadata, `ensure_cached`,
  `prefetch_concurrent`, `await_upload`, `pre_seed_admission`);
  writes/deletes/renames pass through UNresolved — a passthrough delete of a
  mapped-but-byteless logical path can never reach the shared physical.
- **`open_from_linked_checkpoint_instant`** — restore downloads/copies
  NOTHING: per live SST resolve `<chk-k>/NNNNNN.sst` → physical via the
  blob-embedded snapshot, `adopt()` under `<target>/NNNNNN.sst`
  (NotOwned — the restored engine never physically deletes a
  checkpoint-owned object), sync the target journal, write the stripped
  blob, open through `MappedFileSystem`, attach the mapping. The target dir
  physically holds exactly CHECKPOINT.blob + MAPPING.journal.
- **Lazy warm** — `FileSystem::pre_seed_admission` (default no-op trait
  hook); `CachedFileSystem` forwards to `LocalCache::pre_seed_admission`
  (ForSt §2.1.6 registerInCache); the instant restore hints every adopted
  SST so its first foreground touch admits the load-back.
- **Register-rebind guard** (link-mode checkpoint): `register()` only
  unmapped working paths — on a restored engine an adopted working path
  maps to the SOURCE physical (no bytes at the working key); rebinding
  would emit byte-less links. Chained link-checkpoints after restore now
  resolve TRANSITIVELY to the original physicals.
- **`adopted_residual()`** — count of live SSTs still resolving to foreign
  physicals; 0 = weaned, the caller's signal that the restore-source
  checkpoint can be discarded safely (CLAIM-mode discipline; the cross-job
  ownership-transfer protocol = Java-integration stage).

Gates green (2026-06-13):

- **Zero-copy byte-exact IT**: cold instant restore reads exactly the
  snapshot-time state (overwrites + deletes; post-checkpoint source churn
  invisible); target dir object-count assert (blob + journal only);
  restored engine writable; `adopted_residual > 0`.
- **Crash-point IT** (D5-class window): partial restore (subset adopted +
  journal synced, blob never written) retried over the SAME target
  completes via journal replay + idempotent adopt, byte-exact.
- **Missing-physical IT**: loud NotFound (no silent empty state) — `adopt`
  verifies the physical before any state lands.
- **Ownership-boundary IT**: churn on the restored engine weans
  `adopted_residual` → 0 WITHOUT deleting any foreign physical (NotOwned
  discipline); discarding the source checkpoint afterwards still deletes
  each physical exactly once.
- **Chained-restore IT**: linked checkpoint ON an instant-restored engine →
  second-generation instant restore byte-exact for adopted AND new data
  (the register-rebind guard proven end-to-end).
- io `MappedFileSystem` UTs (resolve-on-read / passthrough-on-write+delete /
  unmapped transparency); storage trait-level pre-seed UT (seeded file
  admits on FIRST touch). engine 343/0, io 231/0, storage 444/0; clippy 0.

**Restore-duration-vs-state-size minibench** (the §5 Stage-3 "~flat" gate;
fs-emulation on local FS, same fixture as the Stage-2 bench, median of 3,
`crates/forst-rs-engine/examples/restore_link_flat_bench.rs`, dev Mac
2026-06-13):

```
scale    ssts   state_mb   copy_median_ms   instant_median_ms     ratio
1x          8       32.3             73.9                11.0        7x
4x         32      129.2            312.9                12.3       26x
16x        11      516.9            363.0                12.0       30x
```

INSTANT restore is **flat in state size** (11.0 / 12.3 / 12.0 ms across a
16× sweep — paper Fig. 10 shape) while COPY restore grows with bytes
(74 → 313 → 363 ms on local page-cache; bandwidth-bound on a real uplink).
Each instant rep also proves a cold spot-read through the mapped
indirection.

**Stage-3 residue (recorded, next):** Java zero-upload + download-skip
branches and FFI surface for `linked_*` handles (cross-repo); mapping-journal
TAIL replay on restore (blob-embedded snapshot only today); startup sweep for
abandoned chk-k link leaks (D5 crash window a); rescale-by-clip (key-group
clipped adoption). Stage 4 (WAL-delta memtable durability) is next in-repo.

### Stage 4 — WAL-delta memtable durability on link mode (WAL Phase 4, landed 2026-06-13)

Decisions recorded at implementation (binding, D8–D12):

- **D8 capture barrier.** Link-mode checkpoint with WAL enabled holds the
  WAL lock across `sync` + whole-segment read, writing the image to
  `<chk-k>/WAL.delta` on the ENGINE filesystem (`wal_capture_to`). The lock
  makes the barrier EXACT: a mutation is in checkpoint k iff its append
  completed before the capture (write paths append under the same lock);
  mid-flight writes are excluded — Flink barrier alignment means none exist
  at a real barrier. v1 captures the WHOLE segment (not a delta): correct
  because restore floor-filters; cost recorded below. The D1 object-count
  invariant becomes: chk dir = `CHECKPOINT.blob` + (WAL mode, non-empty WAL
  only) `WAL.delta`.
- **D9 replay.** Restore replays records with
  `seq > per-CF flushed floor` (max `max_sequence` over the restored
  manifest's live SSTs of that CF) via `put_with_seq`, preserving original
  seq + op_type (the `replay_memtable_artifact_bytes` mechanism); the floor
  filter also dedups whole-segment supersets and WBM flushes racing the
  capture. A torn `WAL.delta` tail fails the restore LOUDLY (the image was
  synced-then-copied; torn = damaged artifact). Wired into BOTH linked
  restore paths (instant + copy).
- **D10 gate.** The Stage-2 WAL+link rejection is LIFTED: WAL attached ⇒
  the linked checkpoint runs WAL-DELTA (flush skipped); no WAL ⇒
  FLUSH-on-barrier. The mode boundary is the WAL's presence, nothing else.
- **D11 GC.** Deferred to Phase-5 segment rotation: the live WAL grows for
  the DB lifetime and every capture copies it whole (recorded residue).
- **D12** noflush Arrow-artifact path unchanged (still never link-routed).

Gates green (2026-06-13):

- **Tail-replay IT** (kill-after-ckpt shape): flushed floor (100 rows in
  SSTs) + 70-record unflushed tail (inserts + tombstones + overwrites) →
  WAL-DELTA linked checkpoint (chk dir = blob + WAL.delta exactly; no
  forced flush) → BOTH restores byte-exact; restored memtable holds
  EXACTLY the 70 tail records (floor filter, no double-apply).
- **Barrier-exactness IT**: a write after the capture is absent from the
  restore; the restored engine writes above every replayed seq.
- **Mode-boundary IT**: FLUSH-mode (no WAL) linked checkpoint emits no
  WAL.delta and restores with an empty memtable.
- engine 346/0, io 231/0, storage 444/0 cumulative 1105/0; clippy 0.

**Checkpoint-cost-vs-memtable-size minibench**
(`crates/forst-rs-engine/examples/ckpt_wal_delta_bench.rs`, fs-emulation,
fresh DB per rep, median of 3, ALL state unflushed, dev Mac 2026-06-13):

```
scale   memtable_mb    flush_median_ms    wal_delta_median_ms   ratio
1x              4.0               31.1                   17.5    1.8x
4x             16.0               33.6                   26.2    1.3x
16x            64.0               72.8                   70.4    1.0x
```

HONEST finding: WAL-DELTA **v1 is NOT flat** in memtable size — whole-
segment capture copies O(unflushed bytes), converging with FLUSH cost at
64 MB. The §3.3 "independent of memtable size" promise requires the
Phase-5 delta capture (sealed-segment rotation: checkpoint copies only the
since-last-ckpt segment and LINKS prior sealed segments through the
mapping layer). v1's win is real but modest (1.8× at small tails) and it
keeps memtables unflushed (the q4 noflush memory trade-off applies: 8c/32g
boxes should stay FLUSH). Recorded as the Stage-4→5 residue alongside WAL
GC.

**Stage-4 residue (recorded):** Phase-5 sealed-segment rotation (flat
capture + WAL GC at covering checkpoint + linking shared sealed segments);
q4-class 5M mode A/B (needs the Java branch — cross-repo with the Stage-3
residue); local-WAL-dir placement for remote-primary TMs (today the segment
lives wherever `FRS_WAL_DIR` points; capture re-homes bytes to the engine
FS at the barrier).

### FFI `linked_*` surface + Java adoption package (landed 2026-06-13, cycle 2 unit 1)

Built — the end-to-end enabler for the Stage-3 cross-repo residue:

- **FFI** (`crates/forst-rs-ffi/src/lib.rs` section 8c):
  `frs_create_incremental_checkpoint_linked` (+ 24-byte
  `FrsLinkedCheckpointResult` {manifest, linked_new, linked_shared} +
  idempotent free), `frs_db_discard_linked_checkpoint` (retried →
  NOT_FOUND), `frs_db_open_from_linked_checkpoint_instant` (local FS) and
  `_remote` (same OpenDAL+LocalCache stack as `frs_db_open_remote`),
  `frs_db_adopted_residual`, `frs_db_attach_wal` (per-DB env-free
  WAL-DELTA opt-in — the "wal_capture variant": with a WAL attached the
  linked entry point runs WAL-DELTA automatically, §9 D10).
- **Engine additive**: `open_from_linked_checkpoint[_instant]_with_default_cf`
  (the restored default CF carries the raw-concat merge operator on the
  FFI/Java route), `open_from_linked_checkpoint_instant_remote`,
  `attach_wal_at`. **Footgun found & fixed:** `attach_wal_at` runs a
  PRE-WAL durability barrier (seal + flush every CF) — without it, rows
  living only in the active memtable at attach time are in NEITHER the
  SSTs nor the WAL and the first WAL-DELTA linked checkpoint silently
  loses them (caught by the FFI IT; regression UT
  `test_phase2_ffi_attach_wal_at_flushes_pre_wal_state`).
- **Java adoption package** (`disagg-java/`, COPIES — flink repo
  read-only): `ForStRsLinker.linked-fragment.java` (FFM binds + wrappers,
  ABI-exact), `LinkedSstStateHandle.java` (JM-side no-op discard —
  delegation per §9 D4/paper §5.2), `ForStRsSnapshotStrategy.link-mode-`
  and `ForStRsRestoreOperation.download-skip-` fragments, README with
  binding decisions D-J1..D-J5 (discard delegation via
  notifyCheckpointSubsumed; link mode gated to FORWARD sharing; manifest
  keeps its small EXCLUSIVE upload; CLAIM discipline on
  `adopted_residual`; config keys `forst.rs.checkpoint.link-mode` /
  `forst.rs.wal.dir`) + the 5M correctness gate list.

Gates green (2026-06-13): FFI IT `linked_checkpoint_ffi_it.rs` — FLUSH
round-trip (zero-upload object-count assert through the C ABI, instant
restore byte-exact, residual>0 restored / 0 source, restored writable,
double-free idempotent); WAL-DELTA (attach → blob+WAL.delta exactly, tail
+ overwrite replayed, post-barrier write absent, double-attach rejected);
discard (unlinked==linked count, physicals_deleted==0 under working refs,
retry NOT_FOUND, null-arg paths). Suites: engine 349 (348+1 ignored)/0,
io 231/0, storage 442/0, ffi green; clippy 0.

### Mapping-journal tail replay on restore + abandoned-chk startup sweep (landed 2026-06-13, cycle 2 unit 2)

Built — closes the two recorded Stage-3 residue items (§2.4 tail replay;
§9 D5 crash window a):

- **`MappingJournalView`** (io) — READ-ONLY full replay of a mapping
  journal (never appends; torn tail tolerated like `new`). Carries the
  truths a blob-frozen trailer cannot: post-checkpoint **tombstones** and
  link/unlink churn. + `FileMappingManager::logical_paths_under` /
  `is_tombstoned` (additive only — write-amp agent coordination intact).
- **Tail consult on instant restore**: `open_from_linked_checkpoint_instant`
  loads the SOURCE journal (`<src_db>/MAPPING.journal`, derived from the
  D1 layout) when reachable and (a) REFUSES to adopt a physical carrying a
  JM-discard tombstone (it evaporates when surviving refs drain — silent
  state loss otherwise), (b) corruption-gates a journal-vs-blob resolution
  mismatch on the immutable chk namespace. Journal absent ⇒ blob-only
  fallback, byte-identical to Stage-3 behavior.
- **`DbImpl::sweep_abandoned_checkpoint_links(live_ids)`** — the startup
  sweep: enumerates chk-namespace links straight from the journal-replayed
  state (window a has NO blob), unlinks every id not in the JM-live set,
  removes leftover chk dirs; physicals survive on working/live-checkpoint
  refs; idempotent. FFI: `frs_db_sweep_abandoned_checkpoints` (+ linker
  fragment binding + README D-J1 wiring already pointed at it).

Gates green (2026-06-13): io UTs ×4 (view None/current-state-with-
tombstones/torn-tail/paths-under sorted+filtered; read-only proof —
journal bytes identical before/after load); engine crash-point ITs ×2 —
sweep IT covers BOTH abandonment shapes (window a journal-only links via
direct mapping ops + window b blob-written-never-acked), live-id
protection, physicals_deleted==0 under working refs, idempotence,
live-checkpoint restore byte-exact post-sweep, discard-after-sweep sane;
tombstone IT proves refusal (loud, names the tombstone) + blob-only
fallback when the journal is unreachable. FFI IT (sweep through the C
ABI: reap count, idempotence, live-set protection, null-arg). Suites:
engine 350/0, io 235/0, storage 442/0, ffi ITs 4/0; clippy 0.

### Phase-5 WAL sealed-segment rotation + GC (landed 2026-06-13, cycle 2 unit 3)

Built — closes the D11 residue (v1 whole-segment capture, NOT flat):

- **`WalWriter::seal_and_rotate`** (+ `SealedSegment`, per-CF max-seq
  tracking incl. reopen-seeding): syncs, renames the live segment to a
  unique `.seal-NNNN` sibling, reopens fresh — barrier-exact under the
  engine WAL lock.
- **`wal_capture_to` v2**: seal → **re-home ONCE** (sealed bytes copied to
  `<db_path>/wal/WAL-NNNNNN.seg` on the engine FS, `register()`ed) →
  **GC** (segments whose per-CF max seq is covered by every CF's flushed
  floor lose their working ref; bytes deleted exactly once when the last
  checkpoint ref drains) → **LINK** every still-live segment into
  `<chk-k>/` (metadata-only, resolved via the blob trailer). The D8
  object-count invariant TIGHTENS: the chk dir is physically blob-only
  even in WAL mode (no more `WAL.delta` for new checkpoints).
- **Restore replay v2**: handles linked segments (trailer-enumerated via
  new `MappingSnapshotView::paths_under`) AND legacy `WAL.delta` images;
  torn tail anywhere = loud corruption. **Hazard found & closed
  (chain-of-restores)**: the replayed tail lived only in the restored
  memtable — a next WAL-DELTA checkpoint (no flush) would silently drop
  it; replay now RE-LOGS the tail into the restoring engine's live WAL
  when one is attached (env route), and `attach_wal_at`'s pre-WAL flush
  barrier covers the explicit route.
- **Discard is now NAMESPACE-driven** (`logical_paths_under(<chk-k>/)`,
  not manifest-driven): covers WAL-segment links uniformly, and reaps
  window-a leftovers (links journaled, blob lost) on a direct JM discard
  instead of stranding them for the sweep. Retry contract unchanged
  (no blob AND no links ⇒ NotFound).

Gates green (2026-06-13): wal UT (rotation isolates tail / per-CF max /
unique seal names / reopen-seeded tracker); engine ITs ×3 — chain IT
(ckpt-2's re-homed copy contains ONLY the since-ckpt-1 records (flat
capture proof at the byte level), refs walk working+chk1+chk2, restores
of chk-1/2/3 byte-exact with memtable counts 20/40/0, GC at flush drops
working refs only, discard chain deletes both segment physicals exactly
once); restored-engine WAL chain via `attach_wal_at` (replayed tail
survives the next WAL-DELTA checkpoint); re-log branch UT (tail durable
in the target's live WAL, 30/30). Stage-4 tests updated to the tightened
blob-only invariant. Suites: engine 354/0, io 235/0, ffi green; clippy 0.

**Re-run Stage-4 bench** (`ckpt_wal_delta_bench` extended with the
steady-state cell: ckpt-1 over scale×4 MiB unflushed, then a FIXED
256 KiB tail → ckpt-2; median of 3, local FS, dev Mac 2026-06-13):

```
scale    state_mb  flush_ck1_ms  flush_ck2_ms  wal_ck1_ms  wal_ck2_ms
1x            4.0          29.0          21.8        27.8        24.2
4x           16.0          41.0          21.9        33.2        22.3
16x          64.0          72.1          20.0        82.2        25.1
```

**The §3.3 "independent of memtable size" promise now holds**: steady-
state WAL-DELTA capture (`wal_ck2`) is FLAT — 24.2 / 22.3 / 25.1 ms
across the 16× sweep — vs v1 which re-copied the whole accumulated tail
every checkpoint (the v1 shape is `wal_ck1`: 28 → 33 → 82 ms, growing
with bytes). First-checkpoint cost stays O(cold tail) by nature (the
one-time re-home). Residue: cross-CF coverage uses per-CF floors —
records of a CF that never flushes pin their segment's working ref
(bounded by WBM flush cadence); dropped-CF records pin forever
(conservative leak, reaped when the linking checkpoints are discarded).
**[CLOSED by cycle-3 unit 4 below: dropped-CF + floor-regression pins
released precisely; the genuinely-unflushed-tail pin is retained as
specified (it IS the tail's only durable copy).]**

---

### Cycle 3 unit 1 — UUID physical keys (landed 2026-06-13, C3U1)

ForSt `toUUIDPath` mechanism (competitive analysis §2.2d), mapping-layer
only, default OFF (`MappedFileSystem::with_uuid_physical_keys` is the only
activation; `::new` and every engine default path byte-identical):

- SST-class writes through a uuid-keyed `MappedFileSystem` mint
  `uuid-<32hex>.sst` physicals (same dir; `.sst` kept for staging names so
  `gc_sweep` covers every minted object); logical names unchanged.
- Staging→final publication = `FileMappingManager::rename_logical` — an
  atomic metadata re-point (link-before-unlink so refs never dip to 0);
  deletes of mapped paths route through refcounted `unlink`;
  `sweep_temp_logicals` reaps crashed staging mints at mount.
- `restore_snapshot` rewrites the FIRST logical of each owned physical as
  Register (the old `path==key` identity test downgraded uuid registers to
  Link, losing sizes); journal handles drop after sync (object-store
  writers CLOSE on sync — post-checkpoint appends reopen, fixing
  "append after close" on any opendal-backed mapping).

Gates green: rename-free invariant on opendal-fs emulation — engine
put→flush→link-ckpt→compact→instant-restore with **0 SST-class renames**
reaching the backend, uuid-shaped physicals, byte-exact restore; crashed-
staging sweep IT; journal+snapshot round-trips; truncate-never-clobbers-
shared-physical. io 240/0, engine 355/0 at land.

### Cycle 3 unit 2 — non-SST-always-local routing (landed 2026-06-13, C3U2)

ForSt `FileOwnershipDecider` rule (§2.2c), router-level, default OFF
(`FRS_REMOTE_NONSST_LOCAL=1` wraps the remote-primary stacks in
`FileSystemRouter::with_remote(LocalFileSystem, CachedFileSystem(OpenDAL))`):

- MANIFEST/CURRENT/OPTIONS, WAL `.log` + `WAL-*.seg`, `MAPPING.journal`,
  `CHECKPOINT.blob` pinned LOCAL — zero S3 metadata chatter; only
  SST-class objects go remote through the cache stack.
- Router correctness holes closed while wiring: `await_upload` now routes
  to the owning leg (pre-fix the checkpoint per-file durability barrier
  NO-OPed through the trait default in tiered mode — a restore could
  observe a manifest referencing an un-uploaded SST); `await_all_uploads`
  fans out across distinct legs; `prefetch_concurrent` splits by leg;
  `supports_atomic_rename` = AND of legs.

Gates green: file-class locality catalog UTs (10 local classes, uuid SSTs
remote); **zero remote ops on non-SST paths across a full link-checkpoint
cycle** (count assertion over a recording opendal-emulation remote leg);
blob+journal on the local leg only; instant restore byte-exact with only
the once-per-open R52-M2 orphan-scan dir listing remote.

### Cycle 3 unit 3 — §4.1.1 background-fill scheduler (landed 2026-06-13, C3U3)

Post-restore lazy warm → PACED background fill, default OFF
(`FRS_RESTORE_BG_FILL=1` + `_WORKERS`/`_PACE_MB` on the remote instant-
restore path; explicit `start/finish_restore_background_fill` engine API):

- Read pool of background-class workers (FRS-CACHE-BG-EXEMPT: no LRU/stat
  pollution) drains the adopted set through
  `CachedFileSystem::fill_file_cold` — **Bottom** (`LocalCache::put_cold`
  inserts at the eviction end; an untouched prefill is the first victim)
  / **Skip** (`promote_limit`-blocked keys never re-loaded; fills never
  evict live entries — the no-evict decision is atomic under the cache
  mutex per review R1-H1) per the merged admission machinery.
- Global bytes/sec pacing (token-bucket-by-schedule, prompt cancel);
  engine drop cancels+joins the pool.

Gates green: scheduler UTs (budget-cap exact, Bottom victim order, Skip
on blocked, pacing lower-bound, prompt cancel) + engine IT (cold-cache
instant restore: every adopted physical warmed, 0 errors, idempotent
restart, byte-exact). **Warm-time vs foreground-impact bench pair**
(`restore_bgfill_bench`, 64×256 KiB set, modeled 10 ms/GET ×4-slot remote,
4 s fg window, n=3 dev Mac):

```
cell           warm_ms      fg_slow(>1ms)   fg_p999_us
lazy           1429-2562    64 (every 1st touch stalls inline)   72-114
bgfill-fast     477-528     17-18                                 37-38
bgfill-paced    896-918     37-38                                 61-64
```

### Cycle 3 unit 4 — WAL GC precise pin release (landed 2026-06-13, C3U4)

Closes the Phase-5 residue above:

- **Dropped-CF forever-pin fixed**: coverage treats records of CFs absent
  from the live registry as covered (their state is gone by definition;
  CF ids are never reused). Checkpoints taken before the drop keep their
  own links and restore the CF byte-exact.
- **Floor-regression re-pin fixed**: a MONOTONIC per-CF flushed floor
  (`wal_flushed_floors`, advanced at flush-install) merges with the
  live-SST-derived floor, which regresses to 0 when a CF's SSTs are later
  compacted away entirely.
- **Mixed-segment restore fixed**: `replay_linked_wal_delta` SKIPS
  records of CFs absent from the restored manifest instead of failing
  the whole restore on `lookup_cf_by_id`; skipped records are not
  re-logged into the chain-of-restores WAL.

Gates green (pin-release ITs): dropped-CF working ref released at the
next checkpoint GC (refs 2→1, not linked into new chks), tracked floor
advances at flush, pre-drop checkpoint restores the CF byte-exact, its
discard deletes the segment physical exactly once; mixed segment stays
linked for the live tail and restores with dropped-CF records skipped
(pre-fix: whole restore errored).

### Cycle 3 PMC review (round 1, 2026-06-13)

`docs/superpowers/specs/review-rounds/phase2-cycle3-pmc-review.md` —
8 findings: R1-H1 (cold-fill eviction race), R1-M1 (cold-update demotion),
R1-M2 (`rename_logical` self-rename data loss) ALL FIXED with regression
UTs; 4 LOW accepted+documented; 2 notes. Post-fix suites: io 245/0,
storage 450/0, engine 359/0; clippy 0.

---

## 9. §Stage-2-detail — PMC refinement (2026-06-12, recorded before implementation)

Decisions for the points §3/§5-Stage-2 left open, with rationale. These bind the
Stage-2 implementation.

### D1 — Linked checkpoint-handle path format

A linked SST's logical path is
`<db_path>/checkpoints/<%020d checkpoint_id>/<NNNNNN.sst>` — the SAME directory
that already holds `CHECKPOINT.blob` (`incremental_checkpoint_dir`, db.rs), with
the SST's canonical working-dir basename preserved. Rationale: (a) one
self-describing namespace per checkpoint — the manifest enumerates exactly the
basenames that are linked beside it, so discard needs no extra index; (b) the
20-digit zero-padded id sorts lexicographically == numerically (reuses the
staging-GC property); (c) `parse_file_number` keeps working on linked keys.
The linked path is **metadata-only**: no physical object exists at that key.
Invariant (the zero-upload object-count assert): a link-mode checkpoint
directory physically contains exactly ONE file — `CHECKPOINT.blob` (with the
embedded mapping trailer). All SST bytes stay at their working-dir physical
keys, owned by the mapping refcounts.

### D2 — Flag gate: double-keyed, default OFF

Link mode activates only via:
1. the explicit engine API `create_incremental_checkpoint_linked(...)`
   (auto-attaches a `FileMappingManager` with journal at
   `<db_path>/MAPPING.journal` if none is attached), or
2. env `FRS_CKPT_LINK_MODE=1` **AND** a mapping already attached by the owner.

Rationale: the env key alone must never flip behavior under an unaware
consumer — today's Java `ForStRsSstUploader` reads returned paths byte-wise
(`Files.newInputStream`), and linked paths have no bytes; flipping it without
the Java zero-upload branch (Stage 3, no flink writes in Stage 2) would crash
every checkpoint. The double key means env activation becomes effective exactly
when the Stage-3 backend wires `attach_file_mapping` at open.
`create_incremental_checkpoint_noflush` NEVER routes to link mode: the
Arrow-IPC memtable artifact is per-checkpoint EXCLUSIVE state (nothing to
share/link) and is already deprecated for remote-primary (§3.3).

### D3 — Result contract (zero-upload made misuse-proof)

In link mode `IncrementalCheckpointResult` returns `new_ssts == []` and
`shared_ssts == []` (the upload lists — empty so no caller can byte-copy a
metadata-only path), plus two new fields `linked_new_ssts` /
`linked_shared_ssts` carrying the chk-namespace logical paths. The new/shared
split survives purely as the §3.1(5) registry-registration hint (classified
against the base checkpoint's manifest exactly as today). `manifest_path` is
the engine-FS blob path (no local temp staging in link mode —
`stage_checkpoint_artifacts_local` is skipped entirely). FFI keeps its ABI;
mapping `linked_*` through FFI/Java is Stage-3 work.

### D4 — JM-discard flow vs Flink SharedStateRegistry semantics

Under link mode the registry STOPS being the dedup point: every checkpoint
registers handles under its OWN `chk-k` logical paths (unique keys per
checkpoint), so registry-level identity dedup is structurally a no-op and
cross-checkpoint sharing lives ONLY in the mapping refcount layer (paper
Fig. 8 delegation — "the JM delegates the deletion to the UFS"). Consequences:
- JM discards checkpoint k → every chk-k handle's `discardState()` fires
  (registry sees no other referent) → routed to the TM-side
  `discard_linked_checkpoint(k)`: manifest-driven `unlink(<chk-k>/NNNNNN.sst)`
  loop; the physical object is deleted exactly once when refs drain to 0
  (working-dir ref + other checkpoints' refs keep it alive). The chk dir
  (blob) is then deleted directly — the TM owns it, it is not shared state.
- When the TM-side mapping is unreachable (job gone), discard degrades to the
  journal-tombstone protocol (`FileMappingManager::tombstone(physical_key)`)
  — never a direct S3 delete; drained refs or the startup `gc_sweep` consume
  the tombstone.
- Registry double-registration of the same chk-k path across job restarts is
  impossible (checkpoint ids are monotonic per job); R3's residual risk is
  covered by the Stage-1 exactly-once-delete gate + tombstone idempotence.

### D5 — Barrier ordering + crash windows

Link-mode checkpoint sequence (all under the checkpoint's pinned live set,
R31-H1, and AFTER the per-file `await_upload` barrier — the freeze-fix
no-`await_all_uploads` invariant is unchanged and gated by test):
1. `register()` every pinned live SST (identity mapping; idempotent),
2. `link()` each into `<chk-k>/` (idempotent re-link for checkpoint retry),
3. `sync_journal()` — the mapping's group-durability point at the barrier,
4. serialize manifest + embed mapping-snapshot trailer (now includes the
   chk-k links) → `write_blob`.
Crash windows: (a) between 3 and 4 → chk-k links exist in the journal but the
checkpoint was never reported: JM never acks id k, nobody discards → leaked
refs. Bounded: a RETRY of checkpoint id k re-links idempotently; an abandoned
id leaks until the Stage-3 startup sweep (journal chk-namespaces vs JM-live
checkpoint set) reaps it — recorded as Stage-3 work, leak-over-data-loss per
R1. (b) after 4 before JM ack → standard Flink unacked-checkpoint discard →
D4 flow. (c) before 3 → journal tail may lose the last records; replay
truncated-tail tolerance (Stage-1) + the blob embed of the PREVIOUS checkpoint
keep the mapping consistent.

### D6 — Dual-mode memtable durability boundary (link mode)

Stage-2 ships FLUSH-on-barrier only: barrier forces memtable→L0 (write-through
makes it durable), the L0 SST registers+links like any other (q4 evidence:
flush-is-load-bearing on 8c/32g). WAL-DELTA composes structurally (the
`wal_sync()` barrier + skip-flush override already sit in
`create_incremental_checkpoint_impl`) but remains FORBIDDEN with link mode
until WAL Phase 4 restore-replay lands (Stage 4) — a link-mode checkpoint
taken with WAL enabled would silently drop the unflushed tail on restore.
The noflush Arrow artifact path is excluded by D2.

### D7 — Restore boundary (Stage-2/3 split)

Stage-2 implements the MINIMUM restore proving the linked round-trip:
`open_from_linked_checkpoint(fs, ckpt_dir, target_dir)` — read blob, require
the mapping trailer, resolve each `<chk-k>/NNNNNN.sst` to its physical key via
the embedded snapshot (`MappingSnapshotView`), MATERIALIZE-BY-COPY into a
fresh target dir, fail LOUDLY on a missing physical (no silent empty state),
then open via the existing blob-restore path. Stage 3 replaces the copy with
`adopt()` + lazy reads through `CachedFileSystem` (instant-link restore) and
the Java `ForStRsRestoreOperation` download-loop skip. Restore does NOT
consume the journal tail in Stage-2 (blob-embedded snapshot only) — journal
tail replay is the Stage-3 restore-side trailer-consumption work.
