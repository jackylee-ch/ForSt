# 2026-05-30 — Decoded-block cache: wire the shared L1 cache into SstReaderImpl

## Status
IMPLEMENTED. Builds clean. q9 perf re-measurement pending.

## Problem (the perf wall after q9 correctness was fixed)
With all 5 write-back→S3 correctness fixes in, q9 runs clean but stalls in fits
(~18K/s avg, rate→0 for long stretches) — never fast enough for 3×. A symbolized
`sample` of the TaskManager during a stall showed the cost is in the prefix
iterator's SST tier:
```
96 RangeCachedRandomAccessFile::serial_read_at
72 LocalCache::put          53 LocalCache::get
51 SstReaderImpl::get_versions
44 decode_data_block        37 read_data_block        34 decompress
77 build_lazy_prefix_key_stream   49 prefix_scan_cursor
```
`SstReaderImpl::read_data_block` (reader.rs) read the block bytes
(`read_at_exact` → cache/S3) and `decode_data_block` (decompress + decode to an
Arrow `RecordBatch`) on EVERY call — there was **no decoded-block cache**. The
streaming join's per-key prefix scans re-probe the same SST data blocks across
records, so the same block was re-read and re-decompressed repeatedly.

## Key find: the cache already existed, just unwired
`crate::cache` already provides `CacheEntry::DecodedBatch(Arc<RecordBatch>)`,
`CacheKey{file_number, block_offset}`, and `ShardedClockCache: BlockCache`
(`get`/`insert`, byte-charged, sharded clock eviction). `DbImpl` already holds a
shared `block_cache: Arc<ShardedClockCache>` (sized by
`block_cache_capacity_bytes`) — built explicitly "so SST readers can wire onto the
same cache instance" — but `SstReaderImpl` never used it.

## Fix
- `SstReaderImpl` gains `block_cache: Option<Arc<dyn BlockCache>>` + `file_number`,
  set via a `with_block_cache(cache, file_number)` builder (the bare `open` stays
  un-cached for tests / other callers — no signature break).
- `read_data_block`: on entry, look up `CacheKey(file_number, block_offset)`; a
  `DecodedBatch` hit returns `(**batch).clone()` (cheap — Arc-shared columns),
  skipping the read + decompress + decode entirely. On miss, read+decode as
  before, then `insert(DecodedBatch, charge, CachePriority::Low)`.
- `DbImpl::get_or_open_sst_reader` wires the shared `block_cache` +
  `meta.file_number` into every reader it opens — the path the prefix iterator's
  Tier-3 SST sources (and point gets) flow through.

**Correctness:** SSTs are write-once with globally-unique file numbers, so a
cached decoded block is immutable for the file's life — NO invalidation on
delete/rename/compaction is required (a deleted SST's entries simply age out of
the clock cache). This is the same write-once invariant the chunk cache relies on.

## Why this should move the needle
Eliminates the profiled `serial_read_at` + `decode_data_block` + `decompress`
cost on every repeat block access. For a streaming join whose hot key-ranges are
probed many times, the decoded blocks stay resident → probes become near-memtable
speed. Unlike the reverted write-through SST caching, this caches DATA BLOCKS
(decoded, bounded by the existing block-cache budget) on the READ path — it does
not churn the chunk LRU with write-side whole SSTs.

## Verification
- `cargo build -p forst-rs-storage -p forst-rs-engine`: clean.
- storage + engine test suites: green (un-cached path unchanged; cache path
  exercised end-to-end by the q9 run).
- Re-measure q9 on S3 (expect the iterator stalls to shrink dramatically), then
  G2 (q3/q7/q8/q9), then the full q0–q22 sweep for the 3× total.

## Cross-refs
- docs/superpowers/specs/2026-05-30-writeback-needs-local-first-architectural.md
  (the 5 correctness fixes that unblocked reaching this perf wall)
- [[project_q9_opendal_panic_fix_2026-05-29]]
