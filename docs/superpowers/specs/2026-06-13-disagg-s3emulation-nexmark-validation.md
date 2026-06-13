# Disaggregated-state NexMark-shaped S3-emulation validation (Phase 2)

**Date:** 2026-06-13
**Owner:** PMC-2 (Phase 2 — Disaggregated State)
**Status:** Functional + partial-benchmark artifact (Phase-2 scope). The real-S3
NexMark race is Phase 3 (online box, user-gated). Nothing here uses the network.
**Bench:** `crates/forst-rs-bench/src/bin/nexmark_disagg_s3.rs`
(`cargo run -p forst-rs-bench --release --bin nexmark_disagg_s3 [-- --smoke]`)

---

## 1. Goal

Produce the strongest "beat ForSt on S3" evidence obtainable **before** the
online box, by driving **NexMark-query-shaped state workloads** — not just raw
checkpoint/restore ops — through the **full disaggregated path** on opendal-fs
(LocalFileSystem) emulation, comparing disagg-**ON** vs disagg-**OFF**
end-to-end at query scale, and projecting the S3 wall at a single explicit
bandwidth knob (dev-Mac→BOS 10 MiB/s default; ≥50 Gb/s online box via
`FRS_MODEL_BW_MBPS=6250`).

This complements the existing `disagg_vs_forst` minibench (which measures the
three disaggregation-critical ops on a synthetic load) by exercising those same
ops **plus** steady-state throughput and physical write-amp under the four
NexMark state access patterns the heavy queries actually stress.

## 2. The four query shapes (state access patterns)

| shape   | NexMark query | state pattern (modelled)                                   | CF |
|---------|---------------|------------------------------------------------------------|----|
| q5      | hopping-window agg | per-(window,key) MERGE-state accumulators + window rotation (key churn) | merge-op |
| q7      | interval-join | append bids under a join-key prefix, prefix-scan probe per auction | plain |
| q9/q20  | output-amplifying join | large-keyspace full-range scan emitting N output rows per scanned key | plain |
| q4      | merge-state aggregate | per-category MERGE accumulators (auction→category), then read-back | merge-op |

Each shape is driven by a **seeded, deterministic** xorshift64\* stream, so the
disagg-ON and disagg-OFF arms write byte-identical workloads and the correctness
oracle is exact.

### CF choice is load-bearing (a design constraint, surfaced by this bench)

The engine **deliberately disables KV-separation on any CF that owns a merge
operator** (`db.rs::kv_sep_spec_for`: merge operands can't be cleanly
separated). So:
- **q7/q9 use a PLAIN CF** (no merge operator) → the disagg-ON arm actually
  exercises KV-separation. These are the value-carrying join payloads KV-sep
  targets.
- **q5/q4 use a merge-operator CF** (raw-concat, the real ForSt-RS Flink backend
  config) → KV-sep correctly does **not** apply; merge-operand collapse is their
  lever, not KV-sep.

If every shape had used the merge-operator CF (the naïve choice), KV-sep would
have been silently disabled on ALL shapes and the ON/OFF arms would have been
byte-identical — the bench would have "passed" while testing nothing. Catching
this is why the bench asserts the lever actually changes the SST/vlog layout.

## 3. disagg-ON vs disagg-OFF — what the arms toggle

Both arms run the **real** forst-rs engine on `LocalFileSystem` (fs-emulation).
They differ ONLY in the disaggregation + write-amp lever stack:

| lever                | disagg-ON                              | disagg-OFF              |
|----------------------|----------------------------------------|-------------------------|
| checkpoint           | `create_incremental_checkpoint_linked` (stream-once, 0 upload bytes) | `create_incremental_checkpoint` (re-upload new SSTs) |
| restore              | `open_from_linked_checkpoint_instant` (adopt + lazy reads) | local re-open (model supplies download) |
| KV separation        | ON (`set_kv_separation_override(Some(true))`) | OFF |
| trivial-move compact | ON (`set_trivial_move_override(Some(true))`)  | OFF |
| SST compression      | LZ4 (engine default)                   | LZ4 (held identical)    |

KV-sep and trivial-move are **process-global** overrides resolved at write time,
so the arms run **strictly sequentially** (set → run → reset), never
concurrently. LZ4 is held identical so byte deltas are attributable to
KV-sep/trivial-move, not codec.

## 4. MEASURED vs MODELED (label discipline)

- **MEASURED** (real engine on fs): steady-state put/merge/scan throughput,
  physical on-disk SST+vlog footprint (write-amp proxy), link-checkpoint wall,
  instant-restore wall, bytes-the-caller-must-upload per checkpoint.
- **MODELED** (one explicit bandwidth knob): the S3 wall each arm's
  bytes-to-remote would cost at `FRS_MODEL_BW_MBPS` (default 10 MiB/s recorded
  dev-Mac→BOS 2026-06-01; `=6250` for ≥50 Gb/s online box) + per-file RTT
  (`FRS_MODEL_RTT_MS`, default 23 ms).

fs-emulation has **no** network latency/throughput cap — the model supplies
those. Link/adopt are metadata ops whose cost is network-independent, so the fs
wall **is** the real disagg wall; the byte projections are where bandwidth
enters. The same state-size facts drive every column, so the comparison is
apples-to-apples on one channel assumption.

### Model assumptions (stated)

- Single channel at the modelled bandwidth (no parallel multipart upload).
- A **steady state** for the cumulative tables: no new flushes between the N
  checkpoints — the worst case for the re-upload model, exactly the regime the
  paper's Fig. 9 measures.
- The three architectures projected:
  - **forst-rs disagg**: stream the ON-arm physical footprint ONCE (at flush),
    then N link checkpoints @ 0 bytes.
  - **ForSt disagg**: SAME link mechanism (mechanism parity); streams the
    OFF-arm footprint once. Honest: it lacks forst-rs's KV-sep/trivial-move
    levers, and on a single incompressible first-stream those levers do not
    help (see §6), so ForSt's first stream is actually *smaller* here. The
    forst-rs disagg headline is the **link/instant mechanism**, not a
    first-stream byte win on this load.
  - **RocksDB + S3-ckpt**: non-disaggregated → re-uploads new-SST bytes every
    checkpoint.

## 5. Correctness gate

Every shape asserts the disagg-ON result `(row_count, aggregate_checksum)` is
**byte-identical** to the disagg-OFF oracle. Fast-but-wrong is worthless. The
bench panics on any mismatch.

## 6. The bug this validation found (and fixed)

Running the bench immediately surfaced a real **disagg × KV-separation
interaction bug**: a link checkpoint of KV-separated state, instant-restored,
failed with `Corruption("checkpoint references missing vlog segment")` on the
first deref of a separated value.

**Root cause:** `open_from_linked_checkpoint_instant[_clipped]` adopted the live
**SST** physicals into the restore target's mapping but **never adopted the
`.vlog` segments** — even though the link checkpoint records them at
chk-namespace logical paths exactly like SSTs. No existing test exercised
instant-restore with KV-separation, so the gap was latent.

**Fix (additive, `db.rs`):** a new `adopt_linked_vlog_segments` helper mirrors
the SST adopt loop (resolve the chk-namespace linked path through the embedded
mapping snapshot → honour the live journal's tombstone/paranoia truths → `adopt`
+ lazy-warm pre-seed), called from BOTH the plain and clipped instant-restore
paths. A checkpoint with no separated values has an empty segment table → no-op
(byte-identical to the pre-fix non-KV path).

**Regression guard:** `test_phase2_instant_restore_adopts_kvsep_vlog_segments`
(TDD: written red, confirmed failing on the exact corruption, then green).

This is the strongest kind of pre-remote evidence: an end-to-end emulation that
caught a correctness defect the unit tests missed.

## 7. Honest finding — KV-sep does not win first-stream bytes here

On these single-pass incompressible loads, KV-separation **increases** the
physical footprint slightly (q7: 33.7→36.2 MB, write-amp 1.05→1.13×; more vlog
files) rather than reducing it. This is expected and matches the prior campaign
finding: KV-sep's win is **avoiding value rewrite during SST compaction** under
sustained churn — it does not help a single incompressible first stream with no
compaction-rewrite to save. The disagg headline (link checkpoint = 0 upload
bytes; instant restore) is **independent** of this and robust.

## 8. Results — see the companion results doc

`docs/superpowers/specs/2026-06-13-disagg-s3emulation-nexmark-results.md`

Headlines (full scale, dev-Mac 10 MiB/s model):
- **Correctness:** all 4 shapes PASS (ON == OFF oracle).
- **Checkpoint:** link mode = **0** upload bytes; fs wall 2–17× faster than the
  re-upload arm.
- **Restore:** instant-adopt **28–298×** faster than modelled download-all.
- **Cumulative S3 (10 ckpts):** forst-rs disagg ships **~9.3–10× fewer S3
  bytes** than RocksDB re-upload; the wall ratio holds at **~5–10×** at the
  ≥50 Gb/s online-box bandwidth.

## 9. Reproduce

```
cargo run -p forst-rs-bench --release --bin nexmark_disagg_s3            # full
cargo run -p forst-rs-bench --release --bin nexmark_disagg_s3 -- --smoke  # CI
FRS_MODEL_BW_MBPS=6250 cargo run -p forst-rs-bench --release --bin nexmark_disagg_s3  # online-box projection
```

## 10. What this is NOT

- NOT a real-S3 race (no network; fs-emulation only).
- NOT a Flink end-to-end NexMark run (these are state-access-shaped microloads
  through the engine, not the full SQL pipeline).
- The ForSt column is a model, not a run (the ForSt C++ engine is not built
  here). The bandwidth knob is where the network enters; the disagg
  link/instant walls are measured and network-independent.
