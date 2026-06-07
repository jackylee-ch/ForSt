# forst-rs checkpoint-staging GC fix (FRS-CKPT-STAGE-GC)

**Date:** 2026-06-06
**Scope:** `crates/forst-rs-engine/src/db.rs` `stage_checkpoint_artifacts_local`
+ `scripts/measure-sql.sh` (harness belt-and-suspenders).

## Problem (proven, disk filled to 100%)

Every incremental checkpoint stages its manifest blob + **new** SSTs to a local
temp dir so the Java `ForStRsSstUploader` can read them via NIO and upload:

```
std::env::temp_dir()/forst-rs-ckpt-stage/<db_id>/<checkpoint_id:020>/
```

The staged copies were **never deleted**. With a 30 s checkpoint interval, ~12
keyed-state `DbImpl` slots, and a multi-query Nexmark sweep, this accumulated
**296 GB** of `forst-rs-ckpt-stage` under `$TMPDIR` and drove the disk to 100%
(1.8 GiB free of 926). The symptom downstream was heavy forst-rs queries (q9,
q18, q11) stalling at ~92M/100M with `rate=0/s` — writes blocking on the
near-full disk — i.e. the leak masqueraded as an engine perf regression.

## Root cause

`stage_checkpoint_artifacts_local` created a fresh `<checkpoint_id>` subdir per
checkpoint and returned its paths, but had no lifecycle for prior checkpoints'
staged bytes. The Java uploader consumes each checkpoint's staged artifacts
**synchronously** (reads the returned local paths, uploads, returns) before the
next checkpoint is created — and with the default `max-concurrent-checkpoints=1`
checkpoint N-1's async phase fully completes before N begins. So every staging
dir older than the current checkpoint was dead weight.

## Fix (one step, complete)

Before staging checkpoint N for a db, prune every sibling staging dir under
`forst-rs-ckpt-stage/<db_id>/` whose id is numerically `< N`. The 20-digit
zero-padded ids sort lexicographically == numerically, so a string `<` compare
is exact. This bounds staging to the current (and any in-flight) checkpoint —
O(1) dirs instead of O(checkpoints × queries).

```rust
let stage_db_root = std::env::temp_dir().join("forst-rs-ckpt-stage")
    .join(format!("{}", self.db_id.0));
let keep = format!("{:020}", checkpoint_id);
if let Ok(entries) = std::fs::read_dir(&stage_db_root) {
    for entry in entries.flatten() {
        if entry.file_name().to_string_lossy().as_ref() < keep.as_str() {
            let _ = std::fs::remove_dir_all(entry.path()); // dead: uploader consumed it
        }
    }
}
let stage_root = stage_db_root.join(&keep);
```

Harness belt-and-suspenders (cross-run, since `db_id` may restart per JVM):
`measure-sql.sh` forst-rs case also `rm -rf "${TMPDIR:-/tmp}/forst-rs-ckpt-stage"
/tmp/forst-rs-ckpt-stage` at run start, alongside the existing
`/tmp/nexmark-checkpoints-forst-rs` cleanup (also added this session — that dir
likewise leaked across runs).

## Accuracy verification

- `cargo build --release -p forst-rs-ffi` — clean.
- `cargo test --release -p forst-rs-engine checkpoint` — 2 passed (r49 M1 copies
  SSTs before blob, H2 blob persists CF metadata).
- `cargo test --release -p forst-rs-engine incremental` —
  `test_incremental_checkpoint_awaits_only_referenced_ssts` passed.
- The prune only removes strictly-older sibling dirs under a db-specific path; it
  cannot touch the current checkpoint's staging nor any other db's.

## Performance verification

Functional: the 3-backend Nexmark sweep (`sweep3.sh`) now runs without the disk
refilling. Pre-fix one sweep accumulated 296 GB; post-fix staging stays bounded
to the live checkpoint per db. Confirmation that the previously-stalled heavy
queries (q9/q18) finish under the fix is captured in the sweep3 result matrix.

## Relation to Phase 2

This is the first concrete step toward the ForSt UFS checkpoint model: staging is
a copy-based bridge to the uploader. Phase 2 replaces the copy with zero-copy
registration / refcounted linking of already-materialized SSTs (Fig. 8 hard-link
model), at which point per-checkpoint staging copies disappear entirely rather
than merely being GC'd.
