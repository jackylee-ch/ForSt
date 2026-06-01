# 2026-05-30 — Shared cross-instance decoded-block cache

## Status
IMPLEMENTED. Builds clean. Engine cross-DB correctness tests + q9 re-measure pending.

## Problem
The decoded-block cache (prior spec) was created **per DB instance** at 256 MiB.
A Flink TaskManager runs many keyed-state DBs — one per stateful operator ×
subtask (q9: Join ×4 + Rank ×4 ≈ 8 DBs). So the block-cache budget is fragmented
into 8 small pools; a single heavy query whose hot working set lives in a few of
those DBs only ever gets ~256 MiB, evicting q9's multi-GB join working set fast.
That capped the decoded-block cache's q9 win at ~14–25%.

## Fix
One **process-wide shared** `ShardedClockCache` for all DB instances in the TM:
- `static SHARED_BLOCK_CACHE: OnceLock<Arc<ShardedClockCache>>` + `shared_block_cache(bytes)`
  in db.rs; both `DbImpl` open paths now take the shared cache instead of
  `Arc::new(ShardedClockCache::with_capacity(..))`.
- Sized once on first open at `max(configured, 2 GiB)`. **Memory-neutral**: 2 GiB
  shared ≈ the prior 256 MiB × ~8 stateful DBs total — but one pool concentrates
  the budget on whoever is hot → higher hit rate.

### Correctness: db_id-qualified keys (the critical part)
Every DB instance assigns file numbers from 1, so in a SHARED cache two DBs'
file `000001` would collide on `CacheKey{file_number, block_offset}` and serve one
DB's block to another — silent cross-DB corruption. Fix: `SstReaderImpl::with_block_cache`
now takes `db_id` and packs the key's file-number component as
`(db_id << 40) | (file_number & (2^40-1))`. `db_id < 2^24` (few instances per TM)
and `file_number < 2^40` (≤ 1 T SSTs) in any real deployment, so the packed id is
collision-free. `CacheKey` itself is unchanged (it is used by ~15 cache unit
tests; encoding into the existing field avoids churning them).

`get_or_open_sst_reader` passes `self.db_id.0`.

## Why this is safe to do as one change
- Memory-neutral (2 GiB shared vs ~2 GiB fragmented).
- The shared cache is the SAME tested `ShardedClockCache`; only its ownership
  (global vs per-instance) and key qualification changed.
- Cross-DB correctness is exercised directly by the engine test suite: it spins
  up many `DbImpl` instances (now sharing the global cache); any key collision
  would surface as a wrong-value read and fail the existing value assertions.

## Verification
- `cargo build -p forst-rs-storage -p forst-rs-engine`: clean.
- engine suite (multi-DB, shared global cache): expect 254 pass, 0 fail.
- Re-measure q9 on S3: expect a larger speedup than the per-instance 256 MiB
  cache gave (more of q9's hot blocks stay resident in the 2 GiB shared pool).
  Then G2 (q3/q7/q8/q9) → full q0–q22 sweep for the 3× total.

## Cross-refs
- docs/superpowers/specs/2026-05-30-decoded-block-cache-prefix-iterator-perf.md
- docs/superpowers/specs/2026-05-30-writeback-needs-local-first-architectural.md
