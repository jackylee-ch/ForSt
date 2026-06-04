# OPS / RELEASE NOTE — KV data-block format is now the WRITE DEFAULT (C)

**Date:** 2026-06-04 · **Branch:** `forst-rs` (commit f375ef9d6+) · **Audience:** ops / release / on-call.

## What changed
forst-rs now writes the **v2 KV data-block format** by default (was v1 Arrow-IPC). The reader auto-detects
per block, so v1 and v2 SSTs coexist — **no migration, old SSTs still read.** This shipped the heavy-join
read-path wins (decode-side cost → 0; read_at −12–14%; q4 floor 197K→230K).

## ⟲ REVERT SWITCH (the interim safety valve)
Set the environment variable on the TaskManager/JobManager processes:

```
FRS_SST_KV_BLOCK_FORMAT=0      # forces v1 Arrow blocks (the pre-2026-06-04 behaviour)
```

Any value other than `0`/`false`/`FALSE` (or unset) = v2 default. The switch only affects NEWLY-WRITTEN
SSTs; already-written blocks of either format keep reading correctly. This is the immediate rollback if a
v2-related issue is suspected in production.

## ⚠ PRODUCTION-RELYING PREREQUISITE (not yet done)
The default flip was gated on a 5/5 **performance** pass over the scan/iter-heavy risk queries
(q5/q7/q8/q11/q15) + q4, and on full **correctness** proof of the format itself (dual-version byte-identical
reads, engine ground-truth, all suites green both modes). It was **NOT** yet gated on a full **q0–q22
correctness diff** — the close-out for the 13 untested queries' rarely-exercised code paths.

**Before relying on the v2 default in production, run the full q0–q22 v1-vs-v2 output diff** (tracked as
task #34 / see `2026-06-04-q4-decay-autopsy.md` §"#31"). A 2026-06-04 cold-path code audit found NO
production path that assumes Arrow blocks (all reads go through the central `read_decoded_block` dispatch;
`read_block_at` has no production caller), so this is **low-risk confirmation, not discovery** — but it is
the necessary gate. Until it runs, the `FRS_SST_KV_BLOCK_FORMAT=0` revert above is the interim protection.

## Where else this lives (so it isn't single-pointed)
- Code: `crates/forst-rs-storage/src/sst/kv_block.rs` → `sst_write_kv_format()` doc-comment.
- Investigation record + falsification table + #31 results: `2026-06-04-q4-decay-autopsy.md`.
- PMC decision (no unsafe lever) + correctness close-out rationale: `2026-06-04-PMC-q4-arena-skiplist-DECISION.md`.
