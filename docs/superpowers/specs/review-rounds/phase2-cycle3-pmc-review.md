# Phase-2 Disaggregated State — Cycle-3 PMC Review (Round 1)

Reviewer angle: adversarial pass over the four cycle-3 unit commits before
merge to `forst-rs` (restoring the per-unit review discipline cycle-2 ran
inline). Scope = the unit diffs only; base `f15d86477`.

Units under review:
- **C3U1** `7db0449a3` — UUID physical keys (uuid-mode `MappedFileSystem`)
- **C3U2** `2d4b2311e` — non-SST-always-local routing (+ router barrier fixes)
- **C3U3** `c0598a936` — post-restore background-fill scheduler
- **C3U4** `83c016d0e` — WAL GC precise pin release

Files examined (absolute paths):
- `/…/ForSt/crates/forst-rs-io/src/file_mapping.rs`
- `/…/ForSt/crates/forst-rs-io/src/router.rs`
- `/…/ForSt/crates/forst-rs-storage/src/{local_cache,cached_fs,background_fill}.rs`
- `/…/ForSt/crates/forst-rs-engine/src/db.rs` (wal_capture_to GC, flush floor,
  replay_linked_wal_delta, open_remote*/instant_remote wiring, C3U* gates)

## Summary

| ID | Sev | Unit | Finding | Resolution |
|----|-----|------|---------|------------|
| R1-H1 | HIGH | C3U3 | `fill_file_cold`'s headroom check is a READ-then-PUT race: a concurrent demand fill can consume headroom between the check and `put_cold`, whose eviction loop then **evicts live entries** — violating the unit's stated budget-capped/never-evicts invariant. | **FIXED** (`6747a4313`): the no-evict decision moved INSIDE `put_inner` under the cache mutex — a cold insert of a new key that would exceed capacity removes the staged file and returns `Ok(false)` (mapped to `SkippedBudget`). Regression UT `test_c3u3_r1h1_cold_put_never_evicts_atomically`. |
| R1-M1 | MED | C3U3 | `put_cold` racing a just-completed demand fill of the SAME key treated it as an update and re-inserted at the COLD end — **demoting a foreground-hot entry** to first eviction victim. | **FIXED**: cold update of an existing key keeps hot ordering (`push_back`). Accepted residue: the entry's recency is *refreshed* by the race (overstated, not understated) — benign for write-once SSTs. Regression UT `test_c3u3_r1m1_cold_update_does_not_demote_hot_entry`. |
| R1-M2 | MED | C3U1 | `rename_logical(a, a)` (POSIX self-rename, a legal caller move) took the same-key branch and **unlinked the sole reference — deleting the physical at refs==1** (data loss on a degenerate but legal op). | **FIXED**: `src == dst` early-returns `Ok(())`. Regression UT `test_c3u1_r1m2_rename_logical_to_self_is_noop`. |
| R1-L1 | LOW | C3U1 | uuid-mode `delete_file` is `is_registered` + `unlink` (not atomic): two racing deleters → second gets `NotFound`. | ACCEPTED: matches the trait contract ("delete of a missing file = NotFound"); engine delete sites ignore errors. |
| R1-L2 | LOW | C3U1 | Crashed mint (journal Register durable, object bytes never written) leaves a mapping to a missing object until file-number reuse rebinds or `gc_sweep`/staging-sweep reaps. | ACCEPTED: leak-over-data-loss per design R1; staging (`.tmp`) case is precisely reaped by `sweep_temp_logicals` at uuid-mode mount (crash IT covers it). |
| R1-L3 | LOW | C3U2 | With `FRS_REMOTE_NONSST_LOCAL=1`, `CHECKPOINT.blob` is local-only — cross-node restore depends on the consumer shipping the blob (Flink does: the snapshot strategy uploads the manifest to checkpoint storage). | ACCEPTED + documented in the env doc; flag default-OFF. |
| R1-L4 | LOW | C3U2 | `supports_atomic_rename` leg-AND changes behavior for existing tiered-router users with rename-less remotes. | ACCEPTED: pre-fix those stacks FAILED FAST on the first SST rename (opendal `Unsupported`); leg-AND makes them take the proven direct-write convention. Strictly an improvement; UT pins it. |
| R1-N1 | NOTE | C3U4 | Never-flushing-CF pins are RETAINED while a tail is genuinely unflushed — correct (the segment is the tail's only durable copy in WAL-DELTA mode), bounded by WBM flush cadence per the design. The unit's "precise release" = dropped-CF + floor-regression cases. | No change; recorded so the residue line in §8 can be marked closed-as-specified. |
| R1-N2 | NOTE | C3U4 | CF-id reuse would break the dropped-CF-covered rule; verified `next_cf_id` is a monotonic `AtomicU32` — ids are never reused within an engine lifetime. | No change. |

Verification after fixes: io 245/0, storage 450/0, engine 359/0; clippy 0.

## What was verified clean (per unit)

- **C3U1**: journal-before-bytes ordering at mint; CreateOrTruncate never
  truncates a shared physical in place (re-mints; refcount decides old
  bytes' fate); checkpoint/`mapping_register_live_ssts` rebind guards;
  `restore_snapshot` Register-first rewrite preserves sizes; journal-handle
  drop after sync (object-store writers close on sync — post-checkpoint
  appends reopen). Default-OFF: `MappedFileSystem::new` byte-identical.
- **C3U2**: `await_upload` routing closes a REAL pre-existing hole (the
  checkpoint durability barrier no-opped through the router's trait
  default); `await_all_uploads` fan-out dedups by `Arc::ptr_eq` incl. the
  scheme registry; list_dir union / orphan-scan semantics untouched.
- **C3U3**: pacing is global-schedule (exact in aggregate), cancel-prompt
  (20 ms catnaps); `BackgroundFill: Drop` = cancel+join so a closing engine
  cannot leak fill threads (bounded by one in-flight fetch); workers are
  background-class (no LRU/stat pollution); `read_remote_whole` reuses the
  short-read-refusing fetch verbatim (R75-M2 preserved).
- **C3U4**: monotonic `wal_flushed_floors` advances only after
  `version_set.apply` (the install point); coverage takes max(live-SST,
  tracked); replay skip is restricted to CFs absent from the restored
  manifest and skipped records are NOT re-logged into the
  chain-of-restores WAL.
