# ForSt Backend/Engine — Disaggregated-State Parity CONFIRMATION (forst-rs)

**Date:** 2026-06-15
**Owner:** PMC-2 (Phase-2 Disaggregated State)
**Base:** `origin/forst-rs` tip `536ac433f` (fetched + hard-reset this cycle).
**Scope:** READ-ONLY audit. No production code, no NexMark, nothing heavy run.
**Method:** feature-by-feature cross-check of the **ForSt state backend + engine implementation**
(not just the paper) against forst-rs, via grep/codegraph/read at the tip. Every row carries
`file:line` evidence verified at `536ac433f` (line numbers re-checked at this tip, not copied from
prior audits). Cross-referenced this campaign's commits (`4fd512e83`, `317c5e00f`, `307938016`,
`5192e1ba8`, `e1af132e5`, `4c57528ef`) and the two prior audits
(`2026-06-14-paper-coverage-audit.md` = paper-focused; `2026-06-14-disagg-s3-lock-assessment.md`
= the lock punch-list this campaign closed).

This doc supersedes the prior two as the **definitive engine-side parity statement**: the paper
audit checked the *paper*, the lock assessment listed *gaps*; this one confirms the gaps are
**closed in code at the tip** and states the precise residual.

---

## 0. VERDICT (headline)

**forst-rs is at FULL ForSt-backend disaggregated-state ENGINE parity, plus several extras beyond
ForSt.** Every ForSt-backend disagg capability is **COMPLETE** in the engine at `536ac433f`. The
two HIGH gaps that blocked "lockable" in the 2026-06-14 lock assessment (vlog GC/tombstone parity;
vlog rescale-clip reclamation) **landed this campaign** (`4fd512e83`) and are verified in code +
TDD. The Stage-4 WAL-DELTA per-checkpoint cost concern flagged in the brief (**"Phase-5
sealed-segment rotation not done, per-ckpt cost O(unflushed tail)"**) is **OUTDATED — Phase-5
sealed-segment rotation IS implemented and wired** (`db.rs:wal_capture_to` → `seal_and_rotate`,
flat O(bytes-since-last-ckpt) capture + WAL GC).

**The ONLY residual is cross-repo (Java/Flink side), not an engine gap:**
- The disagg **operating mode** is still local-primary in benchmarks; the Java
  `ForStRsSnapshotStrategy` zero-upload / download-skip-restore branches + the typed FFI
  `linked_*` handle surface are the Stage-3 cross-repo integration item. The engine provides every
  primitive these need (link checkpoint, instant-link restore, adopt, clip, WAL-DELTA), so this is
  **wiring on the Flink side, not a missing engine feature**.
- The QoS upload **rate-split** (`FRS_UPLOAD_RATE_SPLIT`) is engine-complete but reaches Java only
  through the **generic `frs_set_env` process feature-flag bridge** (`ffi/src/lib.rs:1185`), not a
  dedicated typed FFI/Java option — a surface nicety, not a capability gap.

**Net: engine-side parity = COMPLETE. Residual = cross-repo Java integration of the
already-built engine primitives (Stage-3) + a typed FFI surface for two flags.** No genuinely
missing ForSt **engine/backend** disagg feature was found.

---

## 1. Feature-by-feature parity matrix (ForSt backend/engine → forst-rs)

Status: **COMPLETE / PARTIAL / GAP / N-A**. Evidence verified at tip `536ac433f`.

### 1.1 Unified File System (UFS) / file-mapping

| ForSt capability | Status | Evidence (`file:line`) |
|---|---|---|
| Logical→physical file abstraction, single-mutex atomic ops | COMPLETE | `forst-rs-io/src/file_mapping.rs:498` (`Mutex<Inner>`); register `:543` |
| Hard-link sharing across DFS (link/unlink semantics) | COMPLETE | `file_mapping.rs:560` link, `:600`/`:608` unlink |
| Refcount invariant (refs derived from logical map ⇒ idempotent replay) | COMPLETE | doc `:31-33`; journal-before-mutation `:43-52` |
| UUID physical keys (rename-free link) | COMPLETE | mint `file_mapping.rs:651`; enable `:1246` `with_uuid_physical_keys`; SST-class detect `:1276` |
| Non-SST-always-local routing | COMPLETE | `router.rs:47`/`:186` `is_remote_file` (SST-family only)/`:221`; env gate `db.rs` `FRS_REMOTE_NONSST_LOCAL` |

### 1.2 Link checkpoint (zero-upload at barrier)

| ForSt capability | Status | Evidence |
|---|---|---|
| Link-based zero-upload checkpoint (Fig 8 ①②) | COMPLETE | `db.rs` `create_incremental_checkpoint_linked`; link SST + link vlog; mapping-trailer embed |
| Register linked copies, sync journal | COMPLETE | `sync_journal`; mapping trailer embed |
| JM-delegated deletion; physical delete only at refcount 0 (Fig 8 ④⑤⑥) | COMPLETE | `file_mapping.rs:600`/`:608` unlink; refcount-0 HARD-STOP `:1000`; tombstone defer `:761` |
| Incremental checkpoint (`new_ssts`/`shared_ssts` split) | COMPLETE | `db.rs:535-547` `IncrementalCheckpointResult`; `create_incremental_checkpoint` |
| Parallel multi-file checkpoint PUT (extra: upload throughput) | COMPLETE | `checkpoint.rs:358-406` `FRS_CKPT_PARALLEL_UPLOAD` (default-OFF, byte-identical); commit `307938016` |

### 1.3 Instant restore + rescale (clip + adopt)

| ForSt capability | Status | Evidence |
|---|---|---|
| Instant-link restore — SST (no download) | COMPLETE | `db.rs:8240`/`:8256` `open_from_linked_checkpoint_instant…`; adopt; tombstone gate; paranoia correlation |
| Instant-link restore — vlog/KV-sep (the corruption fix) | COMPLETE | `adopt_linked_vlog_segments` (mirrors SST adopt) called from plain + clipped restore; commit `4cd88a167` |
| Rescale-by-clip (downscale) — SST file clip + read clip | COMPLETE | clipped restore; `clip_version_to_range`; `set_clip_range`; boundary remainder → compaction |
| **Rescale-clip RECLAIM of vlog segments** (was GAP-B/H2) | **COMPLETE (this campaign)** | `db.rs:8517-8526` `clip_version_to_range` drops segments whose CF lost every SST to the clip; `live_vlog_segment_count()` residency metric; commit `4fd512e83` (P2) |
| Lazy cache warm on instant restore (paced bg fill) | COMPLETE | `db.rs:1294` `FRS_RESTORE_BG_FILL`; `background_fill.rs` token-bucket |

### 1.4 vlog / KV-separation lifecycle (GC / tombstone / clip-reclaim)

| ForSt capability | Status | Evidence |
|---|---|---|
| vlog segment GC (`live_bytes==0`-driven) | COMPLETE | `vlog.rs:325`/`:430`; lifecycle maintenance `db.rs:4759` |
| Bounded vlog reader cache (q9 OOM mitigant) | COMPLETE | `vlog.rs:348` `VlogReaderCache`, LRU, `evict_to_cap`; cap `FRS_VLOG_READER_CACHE_CAP` default 2048 |
| **vlog in FileMappingManager GC sweep** (was GAP-A/H1) | **COMPLETE (this campaign)** | `file_mapping.rs:1022` sweep filter `e == "sst" \|\| e == "vlog"`; reap/HARD-STOP/tombstone extension-agnostic; commit `4fd512e83` (P1) |
| vlog tombstone defer + reap parity | **COMPLETE (this campaign)** | tombstone reap path extension-agnostic; TDD `test_tombstone_defers_then_reaps_vlog`, `test_gc_sweep_reaps_orphan_vlog_segments` |
| Coalesced batched value-log deref (extra: KV-sep read path) | COMPLETE | `db.rs:288-328` `FRS_VLOG_COALESCE_DEREF` (default-OFF, byte-identical); commit `317c5e00f` |

### 1.5 Multi-tier cache (block cache + file cache + admission/promotion)

| ForSt capability | Status | Evidence |
|---|---|---|
| Block cache (memory, CLOCK/sharded) | COMPLETE | `cache/clock.rs:93-300`; `cache/mod.rs:88-129`; capped by `block_cache_capacity_bytes` |
| File-based secondary cache (local disk, LRU) | COMPLETE | `local_cache.rs:242` `LocalCache`, capacity bytes; LRU evict loop |
| History-Based Policy — LRU eviction | COMPLETE | `local_cache.rs` LRU evict; generation-stamped O(1) touch |
| History-Based Policy — count-to-promote + promoteLimit anti-thrash | COMPLETE | `local_cache.rs` `AdmissionTracker` (`FRS_CACHE_ADMISSION`) |
| History-Based Policy — frequency-DECAYED re-admission (extra over FIFO stand-in) | COMPLETE | `local_cache.rs:94`/`:147`/`:256-261` `epoch_evicts` + `decay_evictions` (`FRS_CACHE_ADMISSION_EPOCH`, default-OFF, byte-identical); commit `e1af132e5` |
| Pluggable cache policy | COMPLETE | `local_cache.rs` `CachePolicy::from_env` |
| Space-based cache limit (extra: dual size+space limit) | COMPLETE | `local_cache.rs:498-579` `FRS_CACHE_SPACE_LIMIT_MB` min-free-disk floor (default-OFF, byte-identical); commit `5192e1ba8` |
| Requester-class cache exemption (background/write-only non-pollution) | COMPLETE | `requester.rs:62-82` `is_background_thread`/`BackgroundScope`; `CachePolicy::background_exempt`; class hoisted to `forst-rs-io/requester_class.rs` |
| Prefetch (vector-I/O batch warm, concurrent) | COMPLETE | `cached_fs.rs:185` `prefetch_files`, `:228` `prefetch_files_concurrent`; SST-batch prefetch path |
| Background-fill scheduler (paced re-admission) | COMPLETE | `background_fill.rs:141-219`, token-bucket pacing `:112` |

### 1.6 Async flush↔upload decoupling + upload QoS

| ForSt capability | Status | Evidence |
|---|---|---|
| Async flush↔upload decoupling | COMPLETE | bg pool + opendal async upload; flush produces SST, upload paced separately |
| Upload rate-limit (throttle) | COMPLETE | `forst-rs-io/throttle.rs` `RateLimiter`/`ThrottledFileSystem::from_env`; `FRS_REMOTE_BW_MBPS` |
| **Upload QoS rate-SPLIT** (compaction sub-rate so flush/ckpt never starves) (extra) | **COMPLETE (this campaign)** | `throttle.rs:68/89/268/289` `FRS_UPLOAD_RATE_SPLIT` + `FRS_UPLOAD_COMPACTION_SHARE`; compaction sub-rate bucket; `requester_class.rs:56` `mark_thread_compaction`; `bg_pool.rs` `new_compaction`; commit `4c57528ef` |

### 1.7 WAL / durability mode

| ForSt capability | Status | Evidence |
|---|---|---|
| WAL writer/reader | COMPLETE | `wal.rs` writer + reader |
| WAL-DELTA dual-mode link checkpoint (seal/rehome/replay) | COMPLETE | `db.rs:939` `WAL_DELTA_NAME`; `wal_capture_to:3964`; FLUSH-vs-LINK boundary; replay `replay_linked_wal_delta` (Phase-4) called from plain `:8209` + clipped `:8380` restore |
| **WAL Phase-5 sealed-segment rotation (FLAT per-ckpt capture)** | **COMPLETE (NOT the GAP the brief assumed)** | `wal.rs:240/529` `seal_and_rotate` + sealed-segment type; `db.rs:3970` called in `wal_capture_to`; capture = O(bytes-since-last-ckpt), re-home-once + link prior sealed segments |
| WAL Phase-5 GC (segment release at covering checkpoint) | COMPLETE | `db.rs:4014-4054` working-unlink when every CF's flushed floor covers the segment max-seq; C3U4 precise-release for dropped CFs |
| noflush memtable-artifact checkpoint (legacy mode, still restorable) | COMPLETE | `db.rs:6655` `replay_memtable_artifacts_from_dir`; `create_incremental_checkpoint_noflush` |

### 1.8 Remote compaction

| ForSt capability | Status | Evidence |
|---|---|---|
| Remote/offloaded compaction (stateless compactor) | COMPLETE (emulated) | `compaction_executor.rs:109` `RemoteEmulatedCompactionExecutor`, offload `:142-169`; serialize round-trip `FRS_REMOTE_COMPACTION_SERIALIZE`; round-trip test `:668`. Real remote endgame user-gated (matches paper's experimental [36]) |
| Bounded background compaction (write-amp/transient bound) | COMPLETE | shared `CompactionPolicy` seam; bounded bg compaction; commit `e4d9c0644` |
| Compaction pool marked compaction-class (QoS) | COMPLETE | `bg_pool.rs` `WorkerPool::new_compaction` → `mark_thread_compaction` |

### 1.9 Coalesced/parallel remote read + prefetch + read-path

| ForSt capability | Status | Evidence |
|---|---|---|
| Coalesced batch read (group by SST, multiGet-equivalent) | COMPLETE | `db.rs:11925` `batch_get_vectorized` (coalesces by SST) |
| Parallel read-IO (batch path, depth>1 over misses) | COMPLETE (batch) | `db.rs:9626` read pool `min(cores,4)` workers, `FRS_RS_READ_IO_PARALLELISM` |
| Scan cold-prime + reader-open fanout (cold-start parallel read) | COMPLETE | `db.rs:209` `FRS_SCAN_COLD_PRIME`; `:235` `FRS_SCAN_OPEN_FANOUT` (byte-identical, timing-only); commits `1272a1f79`/`1cda0e724` |
| Cycle-3 concurrent SST-reader OPEN fanout | COMPLETE | `FRS_SCAN_OPEN_FANOUT` (above) |
| S2-pinned streaming-scan read mode (zero per-row alloc) | COMPLETE | `db.rs:14941-15242` `FRS_RS_S2_PINNED`; byte-equality + alloc-zero tests |
| Single-iterator steady-state read-IO parallelism | PARTIAL — PMC-1 (read-path), OUT OF DISAGG LANE | catalog §2.2; batch path shipped, single-iter is read-path domain — **not a disagg-backend feature** |

### 1.10 Checkpoint scoping / upload-await

| ForSt capability | Status | Evidence |
|---|---|---|
| Checkpoint await-uploads scoping (drain pinned version's SST uploads) | COMPLETE | upload-await on the checkpoint critical path; rate-split keeps flush/foreground at full rate so compaction can't starve await |

---

## 2. Async execution model (paper §4) — N-A to the engine

These are **Flink runtime** mechanisms (Java `StreamTask` / Async Execution Controller / Key
Accounting Unit / async-draining / Epoch Manager), above the JNI boundary. forst-rs is the
**storage engine** (the ForSt-equivalent); it provides exactly the primitives §4 needs
(async-friendly batch/vectorized get+scan, link checkpoint) and avoids §4's per-block-JNI CPU
sub-term by being native Rust. **Correctly N-A — no engine GAP.** (Unchanged from the prior paper
audit §2; restated for completeness.)

---

## 3. Honest PARTIAL / residual statement

This is the part that must NOT be rubber-stamped. Three items are genuinely *not* "engine
COMPLETE without caveat":

### 3.1 Cross-repo Java integration (Stage-3) — the real residual
The disagg **operating model** in all benchmarks is **local-primary** (S3 only as checkpoint
target); checkpoints still byte-copy `new_ssts` via the Java `ForStRsSstUploader`, and restore
still downloads each SST. The paper's model uploads **nothing** at checkpoint time and restores by
**linking**. The ENGINE has every primitive for the paper model — link checkpoint
(`create_incremental_checkpoint_linked`), instant-link restore (`open_from_linked_checkpoint_instant`),
adopt (SST + vlog), clip, WAL-DELTA — but the **Java `ForStRsSnapshotStrategy` zero-upload branch +
`ForStRsRestoreOperation` download-skip branch + the typed FFI `linked_*` handle surface** are the
cross-repo wiring that makes the engine primitives the live path. Evidence:
`2026-06-13-phase2-disaggregated-state-design.md:77` (model B PARTIAL — `open_remote` exists, op
model is local-primary), `:80/:82/:84` (incremental/noflush/restore rows: engine ready, Java
uploads/downloads), `:725-726` (Stage-3 residue = cross-repo). **This is a Flink-integration
residual, not a missing engine/backend feature** — but it is why "the disagg model is *built* but
not the *default operating mode*," and it must be stated plainly.

### 3.2 QoS rate-split FFI/Java surface — generic bridge only
`FRS_UPLOAD_RATE_SPLIT` / `FRS_UPLOAD_COMPACTION_SHARE` are engine-complete (§1.6) but reach Java
only through the **generic `frs_set_env` process feature-flag bridge** (`ffi/src/lib.rs:1185`),
not a dedicated typed FFI/Java option like `storageUri`. Functionally complete (the bridge sets the
process env the throttle reads), but there is no first-class Java config surface for it yet. Minor;
a surface nicety, not a capability gap.

### 3.3 WAL-DELTA per-ckpt cost — the brief's assumption is OUTDATED (now FLAT)
The brief carried the Stage-4 finding that "WAL-DELTA per-ckpt cost is O(unflushed tail) —
Phase-5 sealed-segment rotation not done." **At tip `536ac433f` this is no longer true.**
Phase-5 sealed-segment rotation IS implemented and wired: `wal_capture_to` (`db.rs:3964`) **seals**
the live segment (`seal_and_rotate`, `wal.rs:529`), **re-homes the sealed bytes ONCE**
(O(bytes-since-last-ckpt), not O(total unflushed)), **GCs** covered segments
(`db.rs:4014-4054`, with C3U4 precise dropped-CF release), and **links** prior still-live sealed
segments (O(segments) metadata, zero data movement). The remaining Stage-4 *residue* is **not** the
rotation itself but the **q4-class 5M mode-A/B validation**, which needs the **Java branch
(cross-repo, §3.1)** to drive it (`2026-06-13-...-design.md:794-796`). So: engine = COMPLETE; the
open item is cross-repo validation, the same Stage-3 dependency as §3.1.

### 3.4 Items correctly OUT OF the disagg engine lane (not residuals)
- **Single-iterator steady-state read-IO parallelism** (§1.9 PARTIAL) is the **read-path (PMC-1)**
  domain, not a disagg-backend mechanism. Batch coalesced/parallel read is COMPLETE; the
  single-iter steady-state term is a throughput lever in another lane.
- **Real remote compaction endgame** (§1.8) is **user-gated** (matches the paper's own
  "experimental feature [36]"); the emulated offload path is complete and round-trip-tested.
- **`sst_readers` hard LRU cap** (lock assessment H3, MEDIUM) — the set is bounded by the live-SST
  count (removed on retirement); a hard cap is a design decision, not a lock blocker (the driver is
  compaction, not the cache). Not a parity gap.

---

## 4. Extras BEYOND ForSt (what forst-rs added on top)

Confirmed in code at the tip — disagg-relevant levers ForSt does not have / forst-rs added:
1. **Epoch-decayed cache re-admission** (`FRS_CACHE_ADMISSION_EPOCH`) — true frequency decay vs the
   FIFO stand-in; +7–11 pp hit-rate in the state≫cache regime (prior micro-bench). §1.5.
2. **Space-based dual cache limit** (`FRS_CACHE_SPACE_LIMIT_MB`) — min-free-disk floor on top of
   the byte capacity. §1.5.
3. **Parallel multi-file checkpoint PUT** (`FRS_CKPT_PARALLEL_UPLOAD`). §1.2.
4. **Coalesced batched value-log deref** (`FRS_VLOG_COALESCE_DEREF`) — KV-sep read-path pure win. §1.4.
5. **QoS upload rate-SPLIT** (`FRS_UPLOAD_RATE_SPLIT`) — compaction sub-rate so flush/ckpt critical
   path is never starved under a throttled remote leg; 26× faster ckpt + 0 collapsed windows in the
   write-backpressure repro. §1.6.
6. **Scan cold-prime + concurrent reader-open fanout** (`FRS_SCAN_COLD_PRIME`/`FRS_SCAN_OPEN_FANOUT`)
   — cold-start parallel-read levers, byte-identical. §1.9.
7. **WAL Phase-5 precise dropped-CF segment release** (C3U4) — beyond a basic flushed-floor GC. §1.7.

All default-OFF and byte-identical when off (no per-query config divergence from ForSt's
`noflush=false`, `wbuf=1G`).

---

## 5. Verdict + precise residual

**ENGINE/BACKEND PARITY: COMPLETE.** Every ForSt disaggregated-state engine capability —
UFS/file-mapping, link/zero-upload checkpoint, instant restore, rescale (clip + adopt, SST **and**
vlog), remote compaction, multi-tier cache (block + file + admission/promotion + decay + space +
requester-exempt + prefetch + bg-fill), async flush↔upload decoupling, upload QoS (rate-limit +
**rate-split**), WAL/durability (WAL-DELTA + **Phase-5 sealed-segment rotation** + Phase-5 GC),
vlog/KV-sep lifecycle (GC/tombstone/**clip-reclaim** + bounded reader cache + coalesced deref),
coalesced/parallel remote read, prefetch, requester-class exemption, checkpoint upload-await
scoping — is **COMPLETE in forst-rs at `536ac433f`**, with `file:line` evidence and TDD. The two
HIGH lock-blocking gaps (vlog GC/tombstone, vlog rescale-clip) **closed this campaign**
(`4fd512e83`). forst-rs additionally ships **7 disagg extras beyond ForSt** (§4).

**PRECISE RESIDUAL (all cross-repo Java/Flink integration of already-built engine primitives — NOT
engine gaps):**
1. **Stage-3 Java zero-upload checkpoint + download-skip restore branches + typed FFI `linked_*`
   handle surface** — makes the engine's link-checkpoint/instant-restore the live operating mode
   (today: local-primary, byte-copy upload + download restore). §3.1.
2. **Typed FFI/Java surface for `FRS_UPLOAD_RATE_SPLIT`** (rides the generic `frs_set_env` bridge
   today). §3.2.
3. **q4-class WAL-DELTA mode-A/B validation** — blocked on (1) (needs the Java branch to drive it);
   the engine Phase-5 rotation it validates is already COMPLETE. §3.3.

**The brief's assumption that "Phase-5 sealed-segment rotation is not done" is OUTDATED — it IS
done and wired (§3.3).** The only true open work is cross-repo Flink integration to flip the
already-built engine disagg path into the default operating mode.

---

## Constraints honored
READ-ONLY (no production code, no NexMark, nothing heavy). All `file:line` re-verified at tip
`536ac433f`. No claim without evidence. Honest PARTIAL/residual stated (§3), not rubber-stamped.

## Appendix — files inspected at tip
`crates/forst-rs-io/src/{file_mapping.rs,router.rs,throttle.rs,requester_class.rs}`;
`crates/forst-rs-engine/src/{db.rs,checkpoint.rs,compaction_executor.rs,wal.rs,bg_pool.rs}`;
`crates/forst-rs-storage/src/{local_cache.rs,cached_fs.rs,requester.rs,vlog.rs,background_fill.rs}`;
`crates/forst-rs-ffi/src/lib.rs` (`frs_set_env`);
docs `2026-06-13-phase2-disaggregated-state-design.md`, `2026-06-14-paper-coverage-audit.md`,
`2026-06-14-disagg-s3-lock-assessment.md`. Campaign commits cross-checked:
`4fd512e83`, `317c5e00f`, `307938016`, `5192e1ba8`, `e1af132e5`, `4c57528ef`.
