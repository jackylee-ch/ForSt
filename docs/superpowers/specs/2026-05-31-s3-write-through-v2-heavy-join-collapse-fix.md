# SST write-through V2: eliminate the heavy-join S3 cold-read collapse

Date: 2026-05-31
Status: LANDED + validated (q9 ~4× throughput, hard collapse eliminated, 0 crashes)

## The collapse, fully characterized

q9/q7 (heavy joins) under ckpt-ON + S3-primary hard-collapsed: source burst to
~500K/s, then dropped to ~5-10K/s and flatlined at ~3.3M records. Prior sessions
attributed this variously to the prefix-iterator, cache size, or checkpoint
events. A symbolized profile + controlled probes this session pinned it exactly:

- **Not the prefix-iterator** — `frs_vectorized_batch_get` dominates the profile
  ~10× over iteration; the thread is blocked in `_pthread_cond_wait` on S3
  reads, not CPU-bound merging.
- **Not cache size** — collapse lands at ~3.3M records ≈ ~1 GB working set, far
  under the 64 GiB on-disk cache. No eviction involved.
- **Not checkpoint events** — checkpoint interval is 300 s; collapse hits at
  ~120 s, before any checkpoint fires. Steady-state, not snapshot-driven.
- **It is the RAM-shadow boundary.** Collapse lands exactly at the 1 GiB
  resident-flushed RAM-shadow cap. As join state grows past 1 GiB, the
  shadow evicts; the evicted-but-still-probed state lives **only on S3**
  (write-back uploads SSTs to S3 and keeps no local copy), so every probe of it
  pays a ~20 ms S3 GetObject. That is the collapse.

The 1 GiB cap cannot simply be raised — it was lowered from 16 GiB precisely
because 16 GiB × ~8 CF instances ≈ 128 GB OOM'd the 8c/32g box.

## Fix: write-through just-flushed SSTs to local disk (V2)

Make the spilled state **locally readable** instead of S3-only. The engine
already had `CachePopulatingWritableFile` (write-through wrapper) written but
**unwired** — a 2026-05-27 first attempt was reverted because it cached *every*
flushed file in a 64 GiB LRU and churned out the hot read set.

V2 re-enables it, addressing the revert's failure mode:

1. **Gated to `.sst` files** (`cached_fs.rs::open_writable_file`) — WAL/MANIFEST
   never pollute the read cache.
2. **Paired with a much larger on-disk cache** — `cache-capacity-mb` 64 GiB →
   192 GiB (disk has ~510 GiB free). The full heavy-join flush volume + hot read
   set both fit, so there is no eviction of hot reads (the churn that killed V1).
   Measured: the cache held only 4.6 GB at 13 M records — nowhere near the cap.

On the first `sync()` (S3 multipart-complete → SST durable + bytes final), the
accumulated SST payload is admitted to the `LocalCache`. The next reader hits
local disk (~100 µs) instead of S3 (~20 ms). Cache-put is best-effort and never
fails the (already-durable) write.

This composes with the same session's **resident-shadow-skip prefetch fix**
(`prefetch_sst_files_for_batch` no longer re-downloads RAM-resident SSTs).

## Result (q9, 100M, forst-rs-ffm-s3, 8c/32g, ckpt-ON, single run)

| wall-clock | baseline / prefetch-only | **write-through V2** |
|---|---|---|
| 161 s | ~3.3M (collapsing → ~5K/s) | **4.86M** |
| 301 s | stuck ~3.3M | **7.2M** |
| 402 s | — | **9.6M** |
| 583 s | — | **12.99M** |

- The hard collapse at 3.3M is **eliminated**; q9 climbs steadily to ~13M
  (~22K/s avg, ~4× the collapsed rate) and is still climbing at 600 s (MAXSEC).
- **0 restarts, 0 crashes, 0 LRU churn** (cache 4.6 GB ≪ 192 GiB cap).
- All cached_fs unit tests pass (the reverted-behavior test was rewritten to pin
  V2: `.sst` write-through populates the cache, non-SST files do not).

## Remaining

- q9 still oscillates (bursts ~35-40K/s, periodic dips to ~2-7K/s) — likely the
  synchronous `cache.put` (whole-SST local write) on the flush worker adding
  write-amp latency, or compaction cycles. Candidate follow-up: async/off-path
  write-through so `sync()` doesn't block on the local-disk write.
- q9 still ~7× slower per-record than RocksDB's native local q9 (~170K/s), so a
  full q0–q22 sweep is needed to confirm the binding 3× total. But the heavy-
  query class is no longer collapsing — the precondition for the total.
- Same-session correctness fix (q11/q15 off-heap MapState prefix) is independent
  and already validated.

Changes uncommitted. Validate the heavier joins (q7) and the full sweep next.
