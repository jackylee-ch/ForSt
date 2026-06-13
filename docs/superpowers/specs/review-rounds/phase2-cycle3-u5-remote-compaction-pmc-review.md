# Phase-2 Disaggregated State — Cycle-3 Unit-5 PMC Review (Remote Compaction, pillar 6b)

Reviewer angle: adversarial pass over the remote-compaction unit (C3U5) before
merge to `forst-rs`. Scope = the unit diff only. This is the LAST paper pillar;
the bar is the byte-identical falsifier and "default OFF changes nothing".

Design: `docs/superpowers/specs/2026-06-13-remote-compaction-design.md`.

Files examined (absolute paths):
- `/…/ForSt/crates/forst-rs-engine/src/compaction_executor.rs` (new)
- `/…/ForSt/crates/forst-rs-engine/src/db.rs` (executor field + setter + 2 seams + env default)
- `/…/ForSt/crates/forst-rs-engine/src/compaction.rs` (`KvGcSpec` derive)
- `/…/ForSt/crates/forst-rs-storage/src/version/mod.rs` (`VersionEdit` derive)
- `/…/ForSt/crates/forst-rs-engine/tests/remote_compaction_it.rs` (new)
- `/…/ForSt/crates/forst-rs-bench/src/bin/remote_compaction_offload.rs` (new)

## Summary

| ID | Sev | Finding | Resolution |
|----|-----|---------|------------|
| R1-H1 | HIGH | Trait name `CompactionExecutor` COLLIDES with the existing `flush.rs::CompactionExecutor` (the bg-worker callback `run_compaction`) that `DbImpl` already implements — 59 build errors, and silently it could have shadowed the wrong trait. | **FIXED**: renamed the new trait to `CompactionMergeExecutor` (it executes the *merge* of a picked job; the flush.rs one schedules). All refs updated; builds clean. |
| R1-H2 | HIGH | The byte-identical falsifier originally compared two SEPARATE engines' raw SST bytes — which differ in the footer `creation_time` (`SystemTime::now()`, `sst/writer.rs:499`) + its CRC even for IDENTICAL content. A naive `assert_eq!(bytes)` is a FALSE falsifier (fails on wall-clock, not on divergence). | **FIXED**: the falsifier now asserts (a) full logical content equality (`scan` — wall-clock-free, authoritative) AND (b) raw bytes identical modulo ≤12 bytes inside the footer tail (`assert_sst_bytes_identical_modulo_creation_time`). A real merge divergence fails (a) and shows out-of-footer diffs in (b). |
| R1-M1 | MED | `arc_swap::ArcSwap<Arc<dyn CompactionMergeExecutor>>` does not compile — `ArcSwapAny`'s `RefCnt` bound rejects an unsized `dyn` payload (the design draft said "ArcSwap"). | **FIXED**: `Mutex<Arc<dyn _>>` with a `current_compaction_executor()` that clones the `Arc` under the lock then releases it so `execute` runs lock-free. Compaction is heavy + infrequent ⇒ one lock/clone is negligible (documented on the field). |
| R1-M2 | MED | Worker panic handling: if the offload job panics, does the caller hang? | VERIFIED SAFE: `bg_pool::worker_loop` `catch_unwind`s the job; the `mpsc::Sender` is dropped during unwind ⇒ the caller's `recv()` returns `Disconnected`, mapped to a hard `ForstError::internal` (no install, version untouched, re-pick next cycle). This is the documented `bg_pool` H1 contract; the same mechanism the parallel batch paths rely on. No new hang surface. |
| R1-M3 | MED | Does the offload change MVCC / drop semantics? Could the worker drop a version the primary would keep? | VERIFIED SAFE: the worker runs the SAME `job.run()` with the SAME `min_active_snapshot` carried in the job; `mvcc::should_drop` is applied identically. The byte-identical falsifier (logical content equality) is the empirical proof — any horizon divergence would change the surviving versions. |
| R1-L1 | LOW | `CompactionJobDescriptor` rebuilds the merge operator BY NAME via `merge_operator_by_name`; a custom operator not in the registry can't be reconstructed by a true remote worker. | ACCEPTED + documented: matches RocksDB (`CompactionService` ships cf-options factory config). The in-process emulation passes the original `Arc` to `rebuild` (registry is only the fallback for the true-remote case), so it is unaffected. CompactionFilter has no registry ⇒ `rebuild` hard-errors if a filter is set but not supplied (loud, not silent). |
| R1-L2 | LOW | The descriptor encodes `physical_path` derived from `output_path.parent()`; if inputs ever lived in a different dir than outputs this would mis-locate them. | ACCEPTED: `compaction_output_path == sst_file_path` (compaction.rs:1804) — inputs and outputs share the working dir by construction. The descriptor round-trip IT (which reopens readers from these paths) is the regression guard. |
| R1-L3 | LOW | The offload executor BLOCKS the caller on `recv` — so it does not (yet) overlap compaction with the write path; it only moves the CPU/IO off the calling thread. | ACCEPTED by design: the correctness contract + the measured property is "TM does ~0 compaction CPU" (mini-bench: 13.0→0.1 ms, ~180×). True async (caller does not wait) is a Phase-3 transport concern, called out in the design §3.3. |
| R1-N1 | NOTE | The mini-bench raises `FRS_L0_*_TRIGGER` to keep the whole merge inside the measured `compact_all` (else the bg worker merges during populate and compact_all measures nothing). | Recorded so the numbers are reproducible; same-session A/B, n≥3, macOS system-allocator, no cross-machine comparison (matches `compaction_throughput.rs` methodology). |
| R1-N2 | NOTE | Composition with V3 trivial-move: does remote SUBSUME link-compaction? | NO — verified: `db.rs:8584` short-circuits qualifying rollups to a metadata-only edit BEFORE building any job, so they never reach the executor. V3 runs first; remote offloads only residual real merges. `test_cycle1_kvsep_and_trivial_move_compose_metadata_only_and_exact` still green. |

## What was verified clean

- **Default OFF = no behaviour change.** `default_compaction_executor_from_env`
  returns `LocalCompactionExecutor` unless `FRS_REMOTE_COMPACTION=1`;
  `LocalCompactionExecutor::execute` IS `job.run()`. Both seams call
  `current_compaction_executor().execute(job)?` — identical to the prior
  `job.run()?` when Local. Engine lib 377/0 (unchanged behaviour, +new tests).
- **Additive-only to owned/shared files.** `file_mapping.rs` UNTOUCHED (PMC-1's
  file, contract: PMC-2 additive-only — and here, zero edits). `VersionEdit` /
  `KvGcSpec` gained only `PartialEq, Eq` derives (purely additive). No deletion,
  no signature change to any existing public API.
- **`CompactionJob` is `Send`.** Every field is owned or an `Arc<dyn _>` over a
  `Send + Sync` trait (FileSystem / MergeOperator / CompactionFilter) or
  `Arc<SstReaderImpl>` (already shared across engine threads) — so moving the
  whole job onto the pool thread is sound (compiler-checked).
- **Orphan-on-reject path unchanged.** Both seams keep the existing
  stale-edit (`Busy`) orphan-cleanup arm; the executor only changes WHERE the
  edit is produced, not how it is installed or cleaned up.
- **Descriptor encode/decode is canonical + truncation-safe.** UTs:
  round-trip identity (incl. kv-gc), re-encode is byte-stable, bad magic /
  truncation / empty all error (no panic).
- **opendal-fs E2E.** `remote_compaction_correct_through_opendal_fs` runs the
  offload over a `memory://` remote FS — exercises the real remote read/write
  path (the contract's emulation), correctness exact.

## Verification after fixes

- `tests/remote_compaction_it.rs`: 4/4.
- Module UTs (`compaction_executor::tests`): 4/4 (descriptor round-trip, decode
  rejects, kind asserts).
- Suites: engine lib 377/0, io 245/0, storage 453/0; relevant ITs
  (`mvcc_compaction_it`, `remote_storage_it`) green.
- `cargo clippy -p forst-rs-engine -p forst-rs-storage -p forst-rs-io -p forst-rs-bench --all-targets`: 0.
- `cargo fmt --check`: clean. `forst-rs-ffi` (engine consumer) builds.
- Mini-bench `remote_compaction_offload` (dev Mac, n≥3): caller-thread
  compaction CPU Local 13.0 ms → Remote 0.1 ms (~180× cut), wall 20.1/19.3 ms.

## Termination

H=0, M=0 after 1 round (R1-H1/H2 and R1-M1/M2/M3 all resolved or verified-safe;
3 LOW accepted+documented; 2 notes). The byte-identical falsifier is the
standing gate for any future change to the offload path.
