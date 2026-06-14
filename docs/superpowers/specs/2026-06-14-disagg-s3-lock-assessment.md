# Disaggregated-State + S3 + S2-Tier — LOCK Assessment (PMC-2)

**Date:** 2026-06-14
**Owner:** PMC-2 (Phase-2 Disaggregated State)
**Branch:** `forst-rs` (base = origin tip `15436bfde`)
**Scope:** REVIEW + DESIGN + DOC only. No build/test/bench run this cycle (a perf-sensitive
NexMark pass holds the Mac). All "validation" items below are listed as follow-ups to run
once the Mac frees.
**Method:** read / grep / codegraph over the worktree at HEAD `15436bfde`. Every claim carries
`file:line` evidence; no code was executed.

---

## 0. Executive verdict

**Disagg / S3 / S2 is NOT yet lockable.** The checkpoint / restore / cache / routing / compaction
machinery is COMPLETE and well-tested for the **SST artifact class**, but the **KV-separation
`.vlog` artifact class is only half-integrated into the disaggregation lifecycle** — exactly the
"adopt one artifact class but forget the parallel one" pattern that produced the vlog-adoption
corruption bug we already caught. Two parallel-class gaps remain (vlog GC / tombstone, vlog
rescale-clip reclamation), one bounded-but-uncapped structure (`sst_readers`), and the mock-S3
certification matrix has not been run at the locked bandwidth (`6250`). See §4 for the ranked
punch-list.

The good news: each gap is **space/refcount/durability hygiene under disagg, not a read-path
correctness defect** — the restore *correctness* paths (adopt SST + vlog, tombstone/paranoia
gating, read-path clip) are complete and guarded by byte-exact ITs.

---

## 1. COMPLETENESS audit

Walking the ForSt disaggregated-state model (paper §5.1 Unified-File-System, §5.2 link
checkpoint / instant restore / rescale, §3.3 "stream once, link forever", §6.1 remote compaction)
against forst-rs. Status legend: **COMPLETE** / **PARTIAL** / **GAP**.

| # | Capability | Status | Evidence (`file:line`) | Note |
|---|---|---|---|---|
| 1 | **File-mapping layer** (logical→physical, refcounts, journal) | COMPLETE | `crates/forst-rs-io/src/file_mapping.rs:498` (`Mutex<Inner>`), `:543` register, `:560` link, `:600`/`:608` unlink, refcount invariant doc `:31`-`:33` | Single-mutex atomic ops; refs *derived* from logical map → idempotent replay. |
| 2 | **Link-based zero-upload checkpoint** | COMPLETE | `db.rs:7010` `create_incremental_checkpoint_linked` → register `:7252`, link SST `:7260`, link vlog `:7285`, `sync_journal` `:7287`, embed mapping trailer `:7295` | Bench asserts 0 upload bytes for link mode (`nexmark_disagg_s3.rs:512`). |
| 3 | **Instant-link restore (SST)** | COMPLETE | `db.rs:7975` `open_from_linked_checkpoint_instant_with_default_cf`; adopt `:8065`; tombstone gate `:8038`; paranoia correlation `:8148` | IT: `test_phase2_s3_instant_restore_zero_copy_byte_exact` `db.rs:22840`; crash-retry idempotent `:22882`. |
| 4 | **Instant-link restore (vlog / KV-sep)** — *the corruption fix* | COMPLETE (read path) | `db.rs:8113` `adopt_linked_vlog_segments` (mirrors SST adopt: resolve→tombstone→paranoia→adopt→pre-seed); called from plain `:8076` and clipped `:8303` restore | IT: `test_phase2_instant_restore_adopts_kvsep_vlog_segments` `db.rs:18203`. **Adoption** is complete; **reclamation** is not (see §1 GAP-A / §2 H1). |
| 5 | **Rescale-by-clip (downscale)** | PARTIAL | `db.rs:8202` clipped restore; layer-1 file clip `clip_version_to_range` `:8341`; layer-2 read clip `set_clip_range` `:8328`; boundary remainder → compaction (DR4) `:8181` | SST clip correct + tested (`test_phase2_c2u3_clipped_restore_in_range_exact_zero_leak` `:23180`). **vlog segments are adopted whole, never clip-reclaimed** → space leak after downscale (§2 H2). |
| 6 | **WAL-delta dual-mode durability** | COMPLETE | seal/rehome `db.rs:3686` `wal_capture_to`; FLUSH vs LINK split `:7126`/`:7128`; replay `:8520` `replay_linked_wal_delta`; dedup floor `:8595` | IT: `test_phase2_c2u3_wal_delta_restored_tail_is_clipped` `db.rs:23511`. |
| 7 | **UUID physical keys (rename-free link)** | COMPLETE | mint `file_mapping.rs:651` `mint_physical_key` (`uuid::Uuid::new_v4`); enable `:1246` `with_uuid_physical_keys`; SST-class detect `:1276` | IT: `test_phase2_c3u1_uuid_keys_rename_free_link_ckpt_restore` `db.rs:23633` (zero SST renames verified). |
| 8 | **Non-SST-always-local routing** | COMPLETE | `router.rs:47` `FileSystemRouter`, `:186` `is_remote_file` (`.sst` family only), `:221` `route` (non-SST → local); env gate `db.rs:862` `remote_nonsst_local_env` (`FRS_REMOTE_NONSST_LOCAL`) | Tests `router.rs:687`,`:724`,`:742`. |
| 9 | **Bounded local cache (LRU file cache over remote state)** | COMPLETE | `local_cache.rs:242` `LocalCache`, `capacity_bytes:244`, LRU evict loop `:1043`-`:1061`; FdCache bounded `:287`/`:314` | Tests `:1444`,`:1465`,`:1538`. |
| 10 | **Requester-class cache exemption + write-only admission + count-to-promote** | PARTIAL | `requester.rs:44` `mark_thread_background`, `:51` `is_background_thread`, `:58` `BackgroundScope`; admission/promote policy lives in `LocalCache::CachePolicy` (`background_exempt` default OFF) | The thread-marker + exemption exist; the **count-to-promote / promoteLimit** numeric policy referenced in the brief was not located as a named, tested unit — verify it is wired & tested (§4 P5). |
| 11 | **Background-fill scheduler** | COMPLETE | `background_fill.rs:141` `BackgroundFill`, `start :150`-`:219`, params `:55`, pacing token-bucket `:112` | Tests `:309`,`:333`,`:354`,`:468` (budget cap, evict-order, pacing+cancel). |
| 12 | **Bounded vlog reader cache** (q9 OOM fix) | COMPLETE | `vlog.rs:348` `VlogReaderCache`, LRU deque `:358`, `evict_to_cap :441`; cap `db.rs:134` `vlog_reader_cache_cap` (`FRS_VLOG_READER_CACHE_CAP`, default 2048) | Tests `vlog.rs:677`,`:704`. **Note: cap bounds OOM but per memory the q9 KV-sep run still OOMs (`15436bfde`) — bounding the reader set was necessary but not sufficient; the residual is the un-reclaimed vlog *segment* set (GAP-A).** |
| 13 | **Remote / offloaded compaction** | COMPLETE | `compaction_executor.rs:73` `CompactionMergeExecutor`, `:84` Local, `:109` `RemoteEmulatedCompactionExecutor`, offload `:142`-`:169`; serialize round-trip `FRS_REMOTE_COMPACTION_SERIALIZE` `:111` | Tests `:668` (descriptor round-trip), `:695`,`:703`. Emulated only — real remote endgame is user-gated. |
| 14 | **Scan cold-prime + reader-open fanout** | COMPLETE (timing-only) | `db.rs:209` `cold_prime_enabled` (`FRS_SCAN_COLD_PRIME`); `:235` `open_fanout_enabled` (`FRS_SCAN_OPEN_FANOUT`) | Byte-identical ON/OFF → guarded by byte-identity ITs, no separate unit test (acceptable: pure timing). |
| 15 | **S2-pinned tier (streaming-scan read path)** | COMPLETE | `db.rs:14941`-`:14980` `s2_pinned_enabled` / `S2_PINNED_OVERRIDE` (`FRS_RS_S2_PINNED`); pinned-block replenish `:15218`-`:15242` (zero per-row alloc) | Tests: byte-equality `db.rs:26501`, alloc-counter-zero `:26575`, diag `tests/s2_probe_diag.rs:31`,`:110`. **S2 is a read-path iteration *mode*, not a persistent storage tier** — see §3. |
| 16 | **GC sweep (orphan + tombstone reaping)** | PARTIAL | `file_mapping.rs:983` `gc_sweep` — reaps `.sst` orphans; HARD-STOP on refs>0 `:1000`; tombstone defer `:761` | **Only `.sst` is swept (`:991` `e == "sst"`); `.vlog` orphans/tombstones are never reaped** (§2 H1). |

### COMPLETENESS gaps that block "locked"

- **GAP-A (vlog not in the GC/tombstone lifecycle).** `file_mapping.rs` is SST-centric end-to-end:
  `gc_sweep` filters `extension == "sst"` (`:991`), and there is no `.vlog` path in journal records,
  orphan reap, or JM-discard tombstone handling. vlog segments are *adopted* and *refcounted via
  link/unlink* (link site `db.rs:7285`), but a JM-side discard or an orphaned vlog physical (crash
  between journal append and write) is never reclaimed. Under KV-separation this is a monotonic
  remote-space leak.
- **GAP-B (vlog rescale-clip reclamation).** Clipped restore (`db.rs:8303`) adopts **all** vlog
  segments regardless of clip; the read-path clip only *hides* out-of-range pointers. vlog GC is
  `live_bytes==0`-driven (`vlog.rs:325` doc, `:430`) with **no clip awareness**, so a segment whose
  live pointers all fell outside the clip never reaches 0 and is never reclaimed after a downscale.

---

## 2. ROBUSTNESS / correctness-hazard hunt

Patterned on the vlog-adoption bug: look for any restore/checkpoint/rescale/GC path that adopts
one artifact class but forgets a parallel one, plus any unbounded disagg structure. Each hazard
lists the regression test that should guard it.

### H1 — vlog orphans/tombstones never reaped (GAP-A) — **HIGH**
`gc_sweep` (`file_mapping.rs:983`) only reaps `.sst` (`:991`). vlog physicals orphaned by a crash
between journal-append and physical-write, or marked for JM-discard, accumulate forever on the DFS.
This is the *space* analog of the corruption bug: vlog was added to the adopt path but not to the
reclaim path.
- **Guard test (to add):** `test_gc_sweep_reaps_orphan_vlog_segments` — register+unlink a `.vlog`
  to refs==0, leave its physical on disk, assert `gc_sweep` reaps it (mirror
  `test_gc_sweep_reaps_orphans_never_live_refs` `file_mapping.rs:2018`).
- **Guard test (to add):** `test_tombstone_defers_then_reaps_vlog` (mirror `:1779`).

### H2 — vlog space leak after downscale rescale (GAP-B) — **HIGH**
Clipped restore adopts all vlog segments (`db.rs:8303`) but never clip-reclaims segments whose
live pointers are all out-of-clip; `vlog.rs` GC has no clip dimension. After a fleet of downscale
rescales the vlog set grows without bound even though logical state shrank — re-derives the q9
OOM regime through a different door (the bounded *reader* cap from §1.12 does not cap *segments*).
- **Guard test (to add):** `test_phase2_c2u3_clipped_restore_vlog_segments_reclaimable` — clip a
  KV-sep checkpoint to a sub-range, assert segments referenced ONLY by clipped-out key-groups are
  either not adopted or become GC-eligible (live_bytes decremented by the clip), and assert the
  adopted segment set's byte footprint scales with the clip, not the full state.

### H3 — `sst_readers` is bounded-by-live-files but has no hard cap — **MEDIUM**
`sst_readers: ArcSwap<HashMap<FileNumber, Arc<SstReaderImpl>>>` (`db.rs:978`) is removed on file
retirement (compaction expiry `db.rs:4230`-`:4236`; restore `evict_all_sst_readers` `:12872`), so
it is bounded by the **live SST count**, not unbounded the way vlog_readers was. BUT: under a
garbage-laden / under-budget-compaction regime (the documented q9/q20 "decay") the live-file count
itself balloons, and each reader holds an open handle + index/footer. The garbage-drain mechanism
(`db.rs:273`+ `FRS-GARBAGE-DRAIN`) is the mitigant, but there is no independent LRU cap as a
backstop. This is a *latent* OOM contributor, lower severity than H1/H2.
- **Guard test (to add):** a soak/bound assertion that `sst_readers.len()` tracks
  `version.live_sst_files().len()` and never exceeds it by more than in-flight opens (catches a
  future remove-site regression). A hard LRU cap is OPTIONAL (file count is the real driver; fix
  compaction, not the cache) — list as design-decision, not a blocker.

### H4 — `gc_sweep` lists outside the lock; vlog not double-checked — **LOW (TOCTOU bounded)**
`gc_sweep` does `list_dir` before taking the mutex (`:984` vs `:986`). A file *created* after the
listing is simply absent from the stale listing (safe). A physical at refs==0 in the map is deleted
under the lock, and any concurrent `adopt`/`link` blocks on the same mutex and would have set
refs>0 before release if it ran first (safe). The window is benign **for SST**; once vlog enters
the sweep (H1 fix) the same invariant must be preserved.
- **Guard test (to add when H1 lands):** `test_gc_sweep_concurrent_link_does_not_reap_live` — spawn
  a link racing a sweep, assert the linked physical is never reaped (refs>0 HARD-STOP holds).

### H5 — requester-class promote policy unverified — **LOW**
The count-to-promote / promoteLimit numeric admission policy (§1.10) was not located as a named,
tested unit; only the background-thread marker + `CachePolicy::background_exempt` were found. If the
promote policy is implicit/un-tested, a regression could silently disable write-only admission.
- **Guard test (to verify/add):** confirm a `#[test]` exercises count-to-promote crossing the
  promoteLimit and an exempted background read NOT polluting the cache.

### Maps audited and confirmed BOUNDED (no hazard)
`cfs` / `cf_name_to_id` (CF-count) `db.rs:967`/`:969`; `cf_max_seqs` (per-ckpt) `:839`;
`wal_flushed_floors` (per-CF) `:1189`; `pending_deletions` (transient) `:985`; `lifecycle_merged`
(pruned on drop) `:993`; `file_mapping` holder (singleton) `:1020`; `block_cache`
(`ShardedClockCache`, capped by `block_cache_capacity_bytes` `:1275`); `LocalCache` (§1.9);
`VlogReaderCache` (§1.12). FileMappingManager `logical`/`physical` maps grow with the **live file
set** (bounded by the same compaction lifecycle as `sst_readers`; H3 reasoning applies, same
severity).

---

## 3. S2 second-tier + FileMappingManager lifecycle + mock-S3 matrix

### S2-pinned tier — what it actually is
The "S2-pinned tier" is a **streaming-scan read-path mode** (`db.rs:14941`), not a persistent
storage tier: when `FRS_RS_S2_PINNED=1` the scan replenish fills `SstBlockBuf` with pinned decoded
blocks and emits offset-only row refs (`:15218`-`:15242`), giving zero per-row heap allocation.
Pinning is per-block, decided once at iterator build (`:9588` et al.), released at block boundary.
Correctness is guarded by byte-equality (`db.rs:26501`) and the alloc-counter-zero gate (`:26575`).
**This is COMPLETE and lock-clean.** It does not interact with the checkpoint/restore/GC lifecycle
(it pins in-RAM decoded blocks of already-open readers), so it carries no disagg-lifecycle hazard.

### FileMappingManager link/refcount/tombstone under concurrent ckpt+compaction+GC
- **Atomicity:** every mutation (register/link/unlink/adopt/tombstone/gc_sweep/sync_journal) takes
  the single `inner` mutex (`file_mapping.rs:498`, lock sites `:549`,`:561`,`:601`,`:748`,`:765`,
  `:986`,`:1023`). Operations are serializable; refs are *derived* from the logical map, so a replay
  or a racing op cannot double-count.
- **Durability ordering:** journal append precedes physical mutation (doc `:43`-`:52`; `append_journal`
  `:1032` flushes per record), so a crash leaves an orphan that gc reaps — for SST. **For vlog this
  invariant has no reaper (H1)** → the ordering guarantee is incomplete for the vlog class.
- **Tombstone correctness:** JM-discard writes a `Tombstone` record, deletes on refs-drain or
  immediately if already 0 (`:761`); restore refuses to adopt a tombstoned physical (SST `db.rs:8038`,
  vlog `:8139`). Correct for both classes on the *adopt* side; incomplete on the *reap* side for vlog.
- **Verdict:** SST lifecycle is concurrency-correct under ckpt+compaction+GC. vlog lifecycle is
  correct for adopt/link/unlink but **missing GC + tombstone reaping** (H1) and **missing
  rescale-clip reclamation** (H2).

### Mock-S3 certification matrix (run once the Mac frees — DO NOT run now)
Harness: `crates/forst-rs-bench/src/bin/nexmark_disagg_s3.rs`. It runs four query shapes
(q5 window-agg `:262`, q7 interval-join `:303`, q9/q20 long-scan+amplify `:342`, q4 merge-state
`:379`), each as OFF (re-upload) vs ON (link+instant-restore) arms (`:676`-`:691`), asserts
ON==OFF correctness 4/4 (`:680`), and emits S3-bytes / S3-wall tables at the modeled bandwidth
(`S3Model::from_env` `:126`; `transfer_ms` `:142`; `FRS_MODEL_BW_MBPS`, `FRS_MODEL_RTT_MS`).

**Locked-certification runs (each = one child-agent invocation once the Mac is free):**

| Arm | Command knobs | Asserts | Why |
|---|---|---|---|
| M1 baseline 50 Gb/s | `FRS_MODEL_BW_MBPS=6250` (full scale) | 4/4 correctness; ON S3-bytes ≤ OFF (the 5–10× claim) | Re-confirm the headline at the *locked* bandwidth, not the 10 MiB/s dev default. |
| M2 KV-sep ON | M1 + `FRS_KV_SEPARATION=1` | 4/4 correctness on q7/q9 with vlog active; ON restore still byte-exact | Certifies the vlog adopt path under the locked BW (the class with the open gaps). |
| M3 vlog reader cap | M2 + `FRS_VLOG_READER_CACHE_CAP=2048` (default) and `=0` (legacy) A/B | 4/4 correctness both; bounded arm RSS bounded | Confirms §1.12 bound does not change results. |
| M4 rescale | `disagg_vs_forst.rs` 1×/4×/16× (`:256`) + a clipped-restore correctness arm | clipped restore byte-exact; **vlog footprint scales with clip (H2 — currently expected to FAIL until GAP-B fixed)** | This is the arm that exposes the rescale leak; it is the lock gate for rescale. |
| M5 GC | (new micro-bench) link→discard→gc_sweep over a KV-sep dir | **vlog orphan reaped (H1 — currently expected to FAIL until GAP-A fixed)** | Lock gate for the GC class. |

M1–M3 should pass today (SST + adopt paths are complete). M4/M5 are the gates that **define** the
remaining work — they are expected to fail until GAP-A/GAP-B close, which is precisely why they are
the lock criteria.

---

## 4. LOCK verdict + ranked punch-list

**Verdict: NOT lockable now.** SST disaggregation (checkpoint / restore / rescale-SST / routing /
cache / remote-compaction / WAL-delta / UUID keys / S2 read mode) is COMPLETE and well-tested. The
KV-separation `.vlog` artifact class is integrated on the *adopt/read* side but **not on the
*reclaim* side** (GC, tombstone, rescale-clip). Until the vlog class reaches lifecycle parity with
SST, "locked" would silently leak remote space and re-open the q9 OOM regime through rescale/GC.

### Ranked punch-list (do in order)

1. **[P1 — GAP-A / H1] Bring `.vlog` into the FileMappingManager GC + tombstone lifecycle.**
   Extend `gc_sweep` (`file_mapping.rs:983`) to sweep `.vlog` (not just `.sst` `:991`); ensure
   link/unlink/tombstone records and the orphan-reap journal-ordering apply to vlog physicals.
   Tests: `test_gc_sweep_reaps_orphan_vlog_segments`, `test_tombstone_defers_then_reaps_vlog`.
   *Highest leverage: closes the monotonic remote-space leak and completes the durability ordering
   guarantee for the second artifact class — the direct descendant of the corruption bug.*

2. **[P2 — GAP-B / H2] Clip-reclaim vlog segments on downscale rescale.**
   In clipped restore (`db.rs:8303`) either skip adopting segments with no in-clip live pointer, or
   decrement `live_bytes` by the clipped-out pointers so normal vlog GC (`vlog.rs:430`) reclaims them.
   Test: `test_phase2_c2u3_clipped_restore_vlog_segments_reclaimable`.
   *Lock gate for rescale durability; without it a downscaled fleet leaks vlog space.*

3. **[P3 — Validation] Run mock-S3 M1–M3 at `FRS_MODEL_BW_MBPS=6250`** (once the Mac frees, via
   child agents). Re-confirm 4/4 correctness + the 5–10× S3-traffic win at the *locked* bandwidth,
   incl. KV-sep ON and the vlog-cap A/B. These should pass today and bank the headline evidence.

4. **[P4 — Validation, gates P1/P2] Run mock-S3 M4 (rescale) + M5 (GC).** These are expected to
   fail until P1/P2 land; passing them IS the lock signal for the vlog class.

5. **[P5 — Verify] Confirm requester-class count-to-promote / promoteLimit is wired AND tested
   (H5).** Locate or add a `#[test]` for the promote threshold + background-exempt non-pollution.
   If absent, add it; if present, cite it and mark §1.10 COMPLETE.

6. **[P6 — Hardening, optional] `sst_readers` / FileMappingManager-map backstop (H3).** No hard cap
   needed if compaction keeps the live-file set bounded; add a soak assertion that
   `sst_readers.len()` tracks `live_sst_files().len()` to catch a future remove-site regression. A
   hard LRU cap is a design decision, not a lock blocker (the real driver is compaction, not the
   cache). Also add `test_gc_sweep_concurrent_link_does_not_reap_live` (H4) when P1 lands.

### Constraints carried for any of the above impl (per user directives)
- E2E vectorized / batch / zero-copy; **no `byte[]` / memory-copy, no per-key/record execution**.
- Config matches ForSt: `noflush=false`, `wbuf=1G`; **NO per-query config** — same config for all
  queries, optimize only via dynamic/adaptive engine behavior (user directive 2026-06-14).
- Remote S3 endgame stays user-gated; mock-S3 (`FRS_MODEL_BW_MBPS`) only.

---

## Appendix — files reviewed
`crates/forst-rs-io/src/file_mapping.rs` (2528 L), `.../router.rs`; `crates/forst-rs-engine/src/db.rs`
(disagg sections), `.../checkpoint.rs`, `.../compaction_executor.rs`, `.../wal.rs`;
`crates/forst-rs-storage/src/{local_cache.rs,background_fill.rs,requester.rs,vlog.rs,cached_fs.rs}`;
`crates/forst-rs-bench/src/bin/{nexmark_disagg_s3.rs,disagg_vs_forst.rs}`. No code executed.
