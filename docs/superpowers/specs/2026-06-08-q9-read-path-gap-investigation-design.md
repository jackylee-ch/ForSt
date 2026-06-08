# q9 Read-Path Gap Investigation (forst-rs vs ForSt) — design

**Date:** 2026-06-08
**Status:** Design (approved — profile-first). Sibling to the SST-write gap (already FIXED via
coalescing: q17 817→76.7s = parity). q9 is a DIFFERENT dimension: the join READ path.

## Grounding (measured this session)
- q9 (interval join) with the committed config (noflush=false, 6 GB budget) + the SST-write
  coalescing fix is STILL slow: ~46.7M / 963s (~48 K/s) → DNF-level. The write-side fix did NOT
  help q9 → q9 is **read-bound** (join probes re-walking growing state), not write-bound.
- **CPU = 375%** on the 8-core (`--cpus=8`) container during q9. With parallelism=4 (4 slots),
  that's ~4 slots each ~fully CPU-busy and **~4 cores IDLE**. So: per-slot reads are CPU-bound
  (not idle-waiting), AND there is idle-core headroom that parallel reads could use.
- RocksDB q9 is ALSO slow (~1100s, ~59 K/s) — the join is hard for both engines; the relative gap
  is modest (~2-3×), unlike q17's 11×. (ForSt q9 8c/32g not yet measured.)

## The discriminator (why profile-first)
375% CPU is consistent with BOTH "per-op read-amp within 4 busy slots" AND "serialization within
slots". The deciding datum is **SSTs-walked-per-probe (read-amp magnitude)** + whether the per-op
read is CPU (walking SSTs/blooms) vs waiting. Earlier note: parallel-executor (M1) was REFUTED for
q19 → MUST confirm q9 is read-amp-bound (per-op CPU) vs serialization before building parallel reads.

## Phase A — read-path counters (FRS_PROF_DIAG, COARSE to avoid perturbing per-key gets)
| metric | instrument at | reveals |
|---|---|---|
| read_ns + read_ops | `batch_get` (per-BATCH, not per-key) | join-probe ns/op |
| sst_probes_total / read_ops | the SST-reader consult loop in `get_internal` (1 relaxed inc per SST consulted) | **read-amp = SSTs walked per probe** |
| batch_size histogram | batch_get entry | is coalescing real or degenerate (size 1)? |
Reuse the existing `prof_add` / FRS_MEM_DIAG plumbing + `sst_writer_prof_ns` pattern.

## Phase B — CPU utilization (no code)
`docker stats` CPU% during q9 (got 375% once). Sustained ~375-400% with 4 idle cores = parallel
reads have headroom; near 800% = already saturated (parallelism won't help, must cut read-amp).

## Phase C — compare to ForSt + verdict
ForSt: parallel readThreads pool (`read-io-parallelism=3`) per executor + native coalesced
`multiGetAsList`. forst-rs: single VectorizedExecutor/slot + `batch_get`. Map the dominant q9 read
sub-cost:
- If **sst_probes/probe is high** (read-amp) → fix = reduce SSTs walked (LSM shape / bloom / fewer
  L0) — per-op CPU reduction.
- If **per-op is low but cores idle / executor serializes** → fix = parallel read execution (use the
  idle cores, like ForSt's readThreads), AND/OR coalesced multiGet so one batch resolves many keys.
Likely BOTH (read-amp per probe × single-threaded-per-slot). Confirm magnitudes before building.

## Output
Append the q9 read verdict + ranked fix module to the gap-map RESULTS doc; that module gets its own
plan→implement→verify (out_rows == RocksDB + q9 wall vs RocksDB ~1100s / ForSt).

## Non-goals
The read-path fix itself. This spec only measures + ranks. write_buffer_size stays 1024mb (tuned).
The SST-write coalescing fix (separate, DONE) stays.
