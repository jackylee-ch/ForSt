# Phase-2 Cycle-2 Unit-3 (rescale-by-clip) — PMC self-review

**Date:** 2026-06-13
**Scope:** the rescale-by-clip diff (engine `db.rs` + `column_family.rs`;
design §10). Reviewed every read surface for out-of-range leaks, the DR1
overlap math, default-OFF discipline, clip-set ordering, and the iterator
emit-boundary rewrite.

## Round 1 — findings + dispositions

### R1-H1 (FIXED) — out-of-range leak via the S2 PINNED scan path
`PrefixScanStream::fill_into`'s **pinned** branch drives `next_step_pinned()`
and emits from `last_emitted_buf` — it NEVER calls `next`/`next_with_value`,
so the iterator-level clip filter never saw it. Because the prefix builder
deliberately does NOT thread the clip into source bounds (it relies on the
emit-boundary filter), a clipped CF served by a PINNED prefix scan
(`FRS_RS_S2_PINNED=1`, default OFF) leaked a boundary SST's out-of-range
keys. **Fix:** clip-skip at the top of the pinned branch
(`!clip.contains(last_emitted_buf) ⇒ continue`). **Regression test:**
`test_phase2_c2u3_pinned_prefix_scan_no_leak` — drives the pinned stream
DIRECTLY (`prefix_scan_stream_with_mode(.., pinned=true)`, no env dependency);
**confirmed a real falsifier** (FAILS with the skip removed, PASSES with it).
(Range pinned scans were already safe: the range builder intersects the clip
into the per-source `lower`/`upper`.)

### R1-M1 (FIXED) — remote restore had no clipped variant
`open_from_linked_checkpoint_instant_remote` delegated to the UNclipped
local helper; the production disaggregated rescale path (OpenDAL/S3) had no
way to clip. **Fix:** `open_from_linked_checkpoint_instant_clipped_remote`
(same CachedFileSystem stack + C3U2 non-SST-local wrap + C3U3 background-fill
opt-in as the unclipped remote variant) routing to the clipped local helper.

### R1-L1 (FIXED) — `set_clip_range` swallowed an empty range silently
An empty range (computed by mistake) was cleared to "full state" with only a
comment. **Fix:** `tracing::warn!` on the empty-clear path (the restore entry
point already rejects empty loudly; this is the post-open setter).

### Verified correct (no action) — every other read surface
`get_internal` (covers `get`/`get_arc`/`batch_get`), `get_at_cf` (snapshot),
`get_pinned` (zero-copy FFI), `batch_get_vectorized` (pre-pass +
skip-guard in ALL phases — memtable/imm/resident/L0/L1+ — confirmed never
clobbered), `batch_get_arrow` (inline-cache gate), the range-builder
half-open intersection (`max(lower,start)` / `min(upper,end)` + disjoint
early-return), the `next`/`next_with_value` emit-boundary filter (inner
methods renamed, no dangling refs, progress guaranteed by `last_emitted`
advance), DR1 overlap math (`smallest < end && start <= largest`, all
boundary cases), clip-set BEFORE any read (set after open, before the
WAL-replay WRITE path), default-OFF byte-identity. No panic/unwrap/borrow
issues.

## Post-fix gates

engine 372/0 (+8 C2U3 ITs incl. the pinned falsifier), storage 453/0,
io 245/0 (untouched), ffi builds; clippy 0. Termination: 1 round, H=0 M=0
after fixes.
