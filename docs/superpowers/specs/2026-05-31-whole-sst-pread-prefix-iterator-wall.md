# Whole-SST positional read: eliminate the prefix-iterator local-read wall

Date: 2026-05-31
Status: LANDED in engine (unit-tested: 323 storage + 254 engine pass); end-to-end
q4 validation in progress.

## What a live profile of the q4 stall actually showed

The full sweep (8c/32g, ckpt-ON, 200 GiB disk) hard-stalled on q4 at ~33.6 M /
100 M, oscillating 0–880 rec/s — exactly the "heavy query collapses" symptom.
My own prior memory hypothesized this was the **aggregate / merge read path**
hitting cold accumulator SSTs on S3 (the analogue of the q9 join collapse).

A live `sample` of the TaskManager during the stall **overturned that**:

- **JM backpressure metrics localized the bottleneck to `Join[10]`** (subtasks
  0,1 at `busyRatio=1.0`), NOT either GroupAggregate (both idle, bp=ok). q4's
  wall is the auction⋈bid **join**, not the aggregate.
- The busy join threads' dominant native frame was **`frs_vec_iter_prefix_open`**
  (~5000 inclusive samples each), an order of magnitude over `batch_get`.
- Under it, the leaves were **local syscalls**: `read` (~2100) + `open` (~780) +
  `fstat`/`close`. **Zero `opendal` / S3 / `cond_wait` frames.** The data was
  already local.
- The on-disk cache held only 2.4 GB / 128 GiB (1181 chunk files) — **no
  eviction, no churn**. Write-through V2 *was* populating the cache.

So q4's collapse was **not** cold S3 reads (write-through already made the state
local), and **not** the aggregate/merge path. It was a **local-disk read
inefficiency in the prefix iterator**.

## Root cause: 64× read amplification + a dead write-through cache

Two facts compounded:

1. **The block-read path materializes a whole 1 MiB chunk to return one
   ~16 KiB block.** `RangeCachedRandomAccessFile::read_at` → `serial_read_at` →
   `chunk_bytes(idx)` → `LocalCache::get("{path}#c{idx}")` → `fs::read` of the
   **entire 1 MiB chunk file** (a 1 MiB heap alloc + 1 MiB in-kernel memcpy +
   open/fstat/close), then slices out the block. A join probe is a *different*
   key each time → a *different* block → the decoded-block cache misses → a
   fresh whole-chunk `fs::read`, per overlapping SST, per probe, × millions of
   probes. That is the `read`(2100)+`open`(780) profile.

2. **Write-through V2's cache entries were never read.** Write-through admits the
   whole SST under the key `cache_key(path)` (no suffix). But the block-read path
   queries `cache_key(path)#c{idx}`. **Disjoint keys** — the whole-SST bytes
   write-through paid to store locally were dead weight; reads re-fetched/re-read
   per-chunk anyway. (The cache dir confirmed it: both `…/000013.sst` whole-file
   entries AND `…/000013.sst#c0` chunk entries coexisted for the same SST.)

RocksDB has neither problem: it opens each SST once, keeps the fd, and `pread`s
exactly the block bytes from a page-cache-resident file. forst-rs was paying
~4 syscalls + a 1 MiB copy per 16 KiB block.

## Fix: pread the exact block range from the write-through whole-SST entry

`read_at` now, before the per-chunk path, tries to serve the block by preading
**exactly the requested range** from the whole-SST cache entry (the same
`path_key` write-through already populated):

- New `LocalCache::get_range(key, offset, len)` — a positional `pread(2)`
  (Unix `FileExt::read_at`; seek+read fallback elsewhere) that reads only `len`
  bytes without materializing the file. Bumps the LRU like `get`. Returns
  `None` on miss / vanished file so the caller falls back.
- `RangeCachedRandomAccessFile::read_at` tries `get_range(self.path_key, offset,
  total)` first. Hit (full-length) → copy to the caller's buffer, done. Miss or
  short read → fall through to the existing ranged per-chunk path (cold restore
  reads, or SSTs not write-through resident, still work).

This (a) makes the write-through bytes actually serve reads, (b) drops the read
from 1 MiB → block size (no 1 MiB alloc/memcpy), (c) mirrors RocksDB's local-SST
+ block-pread model. **Soundness:** SSTs are write-once, so a fixed
`(key, offset)` maps to immutable bytes — a positional read is always
consistent; no invalidation needed (matches the existing chunk-cache rationale).

Remaining overhead: `get_range` still `open`s the file per call (the `open`(780)
samples). A held-open fd cache is the obvious follow-up but is deferred — the
1 MiB→block read-size cut is the dominant win and is the lower-risk half.

## Tests

- `local_cache::get_range_preads_exact_subrange` — exact sub-range, zero-length,
  EOF-clamped short read, miss→None, full-range equivalence.
- `cached_fs::read_at_serves_from_whole_sst_writethrough_entry` — with the whole
  SST resident and the **remote deleted**, every block read is byte-exact and
  **no `#c` chunk entries are created** (proving the fast path served it).
- Full suites green: 323 forst-rs-storage + 254 forst-rs-engine.

## Validation (measured, fixed dylib, q4 100M, 8c/32g, ckpt-ON, cold cache)

| wall-clock | old code | **pread fix** |
|---|---|---|
| reach 33 M records | ~600 s (then frozen) | **262 s** (~5× faster, smooth climb at 100–240 K/s) |
| past 33 M | ~hundreds/s, capped | still collapses (see below) |

The pread fix is a genuine, standalone win — confirmed by re-profiling: the
prefix-iterator's `open`/`read` syscalls are GONE (`open` 782→24, a `pread`
path appears), the on-disk cache holds 137 whole-SST / **0** chunk entries
(proving the fast path serves every block), and the pre-spill phase is ~5×
faster. 577 unit tests green, zero crashes/restarts (correctness clean).

## The remaining collapse is STRUCTURAL (not the read path)

q4 still collapses sharply at ~32–33 M. A symbol-rich profile AT the collapse
(not the earlier startup sample) named the new dominant cost on the busy Join
thread: `frs_vec_iter_prefix_open → fill_chunk_from_iter → next → next →
SstReaderImpl::read_data_block` split as **`decompress` (1949)** +
**`decode_data_block → arrow_ipc RecordBatchDecoder::create_array /
ArrayDataBuilder::build` (710)** + **`_xzm_malloc_large_huge → madvise` (276)**.
The I/O (`get_range` pread) is now 2–3 samples — fully fixed.

So past the RAM-shadow spill boundary the wall is **per-probe block decompress +
Arrow-IPC RecordBatch decode on decoded-block-cache misses**, with two
amplifiers:

1. **Working-set ≫ cache.** The in-RAM decoded-block `ShardedClockCache` is
   256 MiB/instance (the FFM backend passes `block_cache_capacity_bytes=0` and
   the deployed JAR has no config key for it). At 32 M the on-disk cache already
   holds 1.4 GB *compressed* SSTs → ~3–6 GB *decoded* working-set. A test with a
   new `FRS_BLOCK_CACHE_MB=2048` engine hook did **not** move the collapse point
   (working-set > 2 GB; and at 100 M it is far larger than any RAM-feasible
   cache on a 32 GiB box already carrying 8 GiB JVM + 6 GiB WBM memtables).
2. **L0 fan-out.** 142 overlapping L0 SSTs at 32 M (64 MiB memtable + 6 GiB WBM
   → frequent flushes, compaction not collapsing them). Each prefix probe opens
   a source per overlapping SST and decodes a block from each → an O(num_L0)
   decode multiplier per probe, independent of cache size.

This is the S3-primary + Arrow-columnar + 8c/32g-RAM ceiling. RocksDB avoids it
because its blocks are plain byte slices (no Arrow columnar decode — the
`create_array`/`build` cost is an EXTRA tax forst-rs pays) and it leans on the
OS page cache for compressed blocks with a much simpler scan path. Closing it
needs a deeper change — lazy/partial column decode for scans (decode only the
key column to locate the range, defer value decode to matched rows), a cheaper
scan-only key index, or fewer/larger L0 files — each requiring careful
validation against the zero-tolerance correctness bar, not a blind mid-run
deploy.

## Three levers measured (instrument, don't assume)

| Lever | Change | q4 result |
|---|---|---|
| Read amplification | `LocalCache::get_range` whole-SST pread | ✅ pre-spill ~5× faster (33M in 262s vs ~600s); I/O gone from profile |
| Decode-cache size | `FRS_BLOCK_CACHE_MB=2048` env hook | ❌ collapse unmoved (~32M) — decoded working-set > 2 GB |
| L0 fan-out prune | `SstReaderImpl::may_contain_range` (decode-free, block-index) | ❌ collapse unmoved (~32.3M) — q4's dense SSTs put the prefix *inside* a block, which the block-granularity prune cannot skip |

The prune is correctness-safe and free (in-memory index only; skips solely when
the range is provably empty), and helps sparse-SST / block-boundary-aligned
workloads — but for q4's pattern (each 64 MiB L0 SST holds a random *sorted*
subset of the auction-id space, so an absent prefix almost always lands within a
populated block) it fires too rarely to matter. **Kept anyway: zero downside.**

The common case — "is prefix X present *within* a block" — needs a **prefix
bloom** (the existing per-full-key bloom can't answer a prefix-existence query)
or lazy/partial column decode. Both are SST format/reader changes requiring
versioning + correctness validation; out of scope for a blind autonomous deploy.

## Disposition

- **KEEP** the `get_range` pread fix (validated win, tests green), the
  `FRS_BLOCK_CACHE_MB` hook (harmless no-op unless set), and the
  `may_contain_range` L0 prune (correctness-safe, free, helps sparse workloads).
- The binding 3× total remains out of reach for heavy scattered-join queries on
  this config: their working-set exceeds RAM and S3-backed Arrow blocks cost
  more per access than RocksDB's local page-cached plain blocks. The fix helps
  the light/medium queries and the pre-spill phase of heavy ones, but does not
  overcome the structural ceiling.

Changes uncommitted.
