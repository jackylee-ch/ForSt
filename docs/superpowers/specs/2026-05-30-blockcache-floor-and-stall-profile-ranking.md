# 2026-05-30 — Per-instance block-cache floor + symbolized stall-profile ranking

## Status
IMPLEMENTED (768 MiB per-instance decoded-block-cache floor). Clean stripped
release deployed. Full q0–q22 sweep (binding 3× total) RUNNING.

## Symbolized stall profile (the decisive evidence)
`sample` of the TaskManager during a q9 rate-stall (this run had L0 trigger=40,
so L0 was large), top forst-rs frames, grouped:

**SST read + decode (~50%)** — the dominant cluster:
```
79 cached_fs::RangeCachedRandomAccessFile::serial_read_at
78 local_cache::LocalCache::put      49 LocalCache::get
46 sst::reader::SstReaderImpl::get_versions
41 sst::data_block::decode_data_block
38 sst::reader::SstReaderImpl::read_data_block
26 sst::compression::decompress
```
The HIGH `LocalCache::put` (78, populating) vs `get` (49) means these are
**cache MISSES on newly-flushed SSTs** — i.e. *first-touch* reads.

**Iterator / memtable (~50%)**:
```
69 db::DbImpl::batch_get_vectorized   68 build_lazy_prefix_key_stream
65 memtable::sharded::ShardedMemTable::get
37 ShardedMemTable::prefix_scan_cursor   25 prefix_scan_keys
22 column_family::ColumnFamilyData::resident_flushed_visible_entries
20 memtable::sharded::MemTierCursor::new
```

**Flush / write**: `batch_insert_with_explicit_seqs_via_indices` (51),
`merge_unsorted_to_sorted` (31).

## What the profile rules IN and OUT
- **OUT — compaction-lock contention.** No `compact_l0_for_cf` frames appear.
  `compact_l0_for_cf` does hold both the engine-global `compaction_mutex` and the
  per-CF `flush_mutex` across its S3 read+merge+write (a real serialization
  hazard), but it is NOT what the stall is spending time in. Background-compaction
  + flush_mutex decoupling is therefore deprioritized — it would not move this.
- **IN — first-touch S3 read-back of just-flushed join state.** q9 writes join
  state → flushes to L0 SSTs → immediately prefix-scans it back. When the working
  set exceeds the 16 GiB resident-flushed RAM shadow (Design A), the flushed
  memtables are evicted and the read-back must fetch + decompress + decode the SST
  block from S3 (first touch, cache miss). `resident_flushed_visible_entries` (22)
  confirms the resident path IS consulted, but `serial_read_at` (79) confirms many
  reads fall through to S3. This is **structural**: state > RAM ⇒ S3 reads, where
  RocksDB hits local disk.
- **IN — per-probe iterator constant.** `build_lazy_prefix_key_stream` (68) is
  rebuilt per prefix-scan open; q9's ROW_NUMBER rank re-iterates partition state
  per record. A genuine but diffuse constant.

## The change landed this round
Per-instance decoded-block `ShardedClockCache` **floor of 768 MiB** (db.rs, both
open sites). Rationale:
- A *shared* cross-instance pool (prior spec) regressed heavy queries ~40% via
  cross-DB shard-lock contention — REVERTED. Per-instance avoids that.
- The cache is native-resident and charged by ACTUAL bytes inserted, so a higher
  floor costs light/small-state queries nothing; only heavy DBs grow into it.
- Converts the profiled `decode_data_block` + `decompress` cost to hits on
  **re-probed** blocks (q9 rank re-iteration). Does NOT help first-touch.

## Why the TOTAL is the metric that decides this
First-touch S3 reads for state > RAM are a structural per-query handicap vs
RocksDB-local for the heavy joins (q4/q5/q7/q9/q15–q19). The 3× goal is a
**q0–q22 total** — it holds iff the light/medium wins (forst-rs historically wins
16/23) outweigh the heavy losses. The last full sweep measured 0.75×, but that
PREDATED the 5 q9 write-back→S3 correctness fixes — heavy queries were
crash-looping / timing out, dragging the total down. The sweep now running
(clean dylib, all fixes) is the first valid measurement of the binding metric.

## Verification
- `cargo build -p forst-rs-ffi --release`: clean. Stripped release deployed.
- Block-cache floor is a pure capacity increase (clock cache, byte-charged) — no
  correctness surface.
- RUNNING: `CONFIGS="rocksdb forst-rs-ffm-s3" MAXSEC=1500 scripts/sweep-sql.sh`
  → /tmp/full-sweep.log + /tmp/sweep-*/results.tsv. Reports per-query table +
  common-FINISHED ratio (base/target).

## Cross-refs
- 2026-05-30-decoded-block-cache-prefix-iterator-perf.md (the per-instance cache)
- 2026-05-30-shared-cross-instance-block-cache.md (the reverted shared pool)
- [[project_q9_opendal_panic_fix_2026-05-29]] (the 5 correctness fixes)
