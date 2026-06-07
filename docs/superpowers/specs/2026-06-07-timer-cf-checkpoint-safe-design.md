# Checkpoint-safe dedicated timer column family (FRS-TIMER-CF)

**Date:** 2026-06-07
**Status:** Implemented + checkpoint-safe + tested, but DEFAULT OFF — **CF isolation REFUTED as a
perf lever** (see "Refutation" below). Kept flag-gated (`-Dforst.rs.timer.cf=1`) as correct infra.

> ## Refutation (2026-06-07) — read this first
>
> The motivating −118s (q11 529→411) was a **single, unrepeated A/B pair = variance**. A faithful
> default-on reproduction measured q11 **518s and 457s** (61s spread between two identical runs), with
> the spike's 411 sitting inside that noise band. TIMER_DIAG with the dedicated CF on showed the
> timer-scan access pattern is **unchanged** vs the shared-CF baseline:
>
> | metric (at polls=500K) | shared CF (baseline) | dedicated timer CF |
> |---|---|---|
> | refills | 5291 | 5357 |
> | entriesPerRefill | 993 | 989 |
> | refillMs | 171s | 229s (≥ baseline) |
>
> Identical `refills` and `entriesPerRefill` prove isolation changes nothing. The q11/q12 cost is
> **diffuse LSM range-scan read-amplification over the GROWING timer state** — the same q4/q9-class
> engine gap — driven by the refill/cache-invalidation churn in
> `ForStRsKeyGroupedInternalPriorityQueue.readRangeIntoCache`, NOT timer/state interleaving and NOT
> delete tombstones (the code comment at the `flushPollDeletes()` call already states this). A separate
> CF removes none of it. **Consequence:** q11/q19/q20/q9/q4 all collapse into ONE root cause — engine
> LSM range-scan per-entry cost — so the real #2 lever is speeding up that scan path, which lifts all
> of them at once. There is no quick timer-specific shortcut.
>
> What is kept (correct + checkpoint-safe, flag-gated `-Dforst.rs.timer.cf=1`, default off): the
> `dbOpenOrCreateCf` helper, the `timerCf` routing, and the engine round-trip test. Production timers
> stay in `defaultCf` (the verified all-pass baseline).

## Original design rationale (kept for context; perf premise refuted above)
**Scope:** forst-rs Flink backend (`flink-statebackend-forst-rs`) only. No engine behavior change
(the engine already supports multi-CF checkpoint/restore; this promotes a measured spike to a
production, restore-safe default and adds an engine regression test).

## Problem

NexMark Phase-1 prerequisite #2 (forst-rs total NexMark time < rocksdb) is unmet: forst-rs 4351s vs
rocksdb 4117s (1.057× slower). The largest single contributor is **q11** (529s vs rocksdb 123s) and
**q12**, both timer-heavy. Root cause: the engine-backed timer queue
(`ForStRsKeyGroupedInternalPriorityQueue`) stored timers in the **shared default column family**,
interleaved with window/join state. Timer range scans then read-amplify over SSTs that also contain
state rows, and session-window merge produces tombstone churn (≈5M timer keys vs ≈500K final, via
delete+add) that the timer scan must walk.

A property-gated spike (`-Dforst.rs.timer.cf.spike=1`) routed timers to a dedicated CF and measured
**q11 529s → 411s (−118s, full 92M run, clean)**. The spike was NOT wired into checkpoint/restore,
so it could not ship: a restored job would call `dbCreateCf("frs_timers_spike")` on a DB where the
engine had already re-registered that CF from the manifest → "column family already exists", or
(if it had used a name the engine didn't restore) silently lose all timer state.

## What was already true (engine, no change needed)

The engine is fully multi-CF checkpoint/restore-safe in BOTH checkpoint modes:

- **Checkpoint capture** (`create_incremental_checkpoint_impl`, `db.rs`): the VersionSet is global and
  every live SST is tagged by `cf_id`; `collect_cf_descriptors()` records every CF (name + merge-op).
- **No-flush memtable capture** (`snapshot_memtables_to_dir`, `db.rs:4158`): iterates **all** CFs and
  writes one Arrow-IPC artifact per CF, `memtable-cf<cf_id>.arrow`.
- **Restore** (`open_from_checkpoint_with_default_cf`, `db.rs:4743-4792`, reached by
  `open_from_incremental` → the Java `dbOpenFromIncremental` restore path): re-registers every
  non-default CF from the manifest descriptors **preserving its original `cf_id`** and merge operator;
  `replay_memtable_artifacts_from_dir` replays each CF's memtable by `cf_id`.

So a dedicated timer CF's SSTs **and** its unflushed memtable already round-trip; the only gap was the
Java side creating-vs-opening the CF.

## Design

Promote the spike to a default-on, checkpoint-safe feature, entirely in the Java backend.

1. **`ForStRsLinker.dbOpenOrCreateCf(db, arena, name)`** — idempotent resolve: try `dbOpenCf`; on the
   engine's `FrsStatus.NOT_FOUND`, `dbCreateCf`. Any other status propagates. This is the create-on-
   fresh-DB / open-on-restore contract: on restore the CF already exists (engine re-registered it), so
   we open by name and inherit its restored state and original `cf_id`.

2. **`ForStRsAsyncKeyedStateBackend`** — `timerCfSpike` → `timerCf`, resolved lazily on the first
   `create()` via `dbOpenOrCreateCf(db, arena, "frs_timers")` and cached (multiple timer services in
   one backend share one handle). **Default ON**; `-Dforst.rs.timer.cf=0` falls back to the legacy
   shared `defaultCf` (for restoring a pre-feature snapshot whose timer rows live in the default CF).
   The handle is closed in `releaseNativeResources()`.

The timer CF carries no merge operator (timers are plain put/delete), so its descriptor round-trips as
`merge_op_name == ""` and restores as a plain CF.

## Why correctness holds

- **Single-run**: timers written to `frs_timers`; the queue uses that CF for every op including the
  snapshot-time pending-buffer drain (Phase 1.e in `snapshot()`), so the checkpoint sees the drained
  timers in the timer CF's memtable/SSTs.
- **Restore within a run** (failover): checkpoint captures the timer CF (descriptor + SSTs +
  no-flush memtable artifact); restore re-registers it and replays its memtable; the backend's first
  `create()` opens it by name → all timers present.
- **cf_id stability**: artifact filenames key on `cf_id`; restore preserves it; open-by-name returns
  whatever id the engine restored. Name-based resolution never assumes a fixed id, so creation order
  on a fresh DB is irrelevant.

## Verification

- **Engine regression test** (NEW): `db::tests::test_noflush_checkpoint_restore_non_default_cf_round_trip`
  — creates `frs_timers`, writes SST-resident + live-memtable data, no-flush checkpoints, restores via
  `open_from_incremental` (the Java restore path), and asserts: the timer CF's memtable artifact is
  captured; the CF is re-registered by name with its original `cf_id`; and both SST- and memtable-
  resident keys survive. PASS.
- **Timer unit suite**: `ForStRsKeyGroupedInternalPriorityQueue*` — 9 run / 0 fail (11 skipped).
- **Perf**: q11 forst-rs at full scale with the feature default-on (no `-D` flag) — re-confirming the
  −118s the spike measured, now in a restore-safe configuration. (In progress.)

## Files

- `flink/.../ffm/ForStRsLinker.java` — `dbOpenOrCreateCf`.
- `flink/.../keyed/ForStRsAsyncKeyedStateBackend.java` — `timerCf` field + javadoc; `create()` routing
  (default-on, open-or-create); `releaseNativeResources()` close.
- `ForSt/crates/forst-rs-engine/src/db.rs` — new engine round-trip test (test-only; no cdylib change).
