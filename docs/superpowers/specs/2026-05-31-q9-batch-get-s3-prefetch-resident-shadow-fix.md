# q9 perf: batch_get S3 prefetch was re-downloading RAM-resident SSTs

Date: 2026-05-31
Status: LANDED + measured (q9 trajectory improved; redundant prefetch path
eliminated from the profile). Remaining cold-read cost identified for follow-up.

## Method: profile, don't guess

The accumulated theory (multiple prior sessions) was that q9's heavy-join wall
was the **prefix-iterator** per-probe constant
(`build_lazy_prefix_key_stream`). A symbolized `sample` of the TaskManager
during q9's documented rate≈0 stall (3.3M/~120s, 8c/32g, forst-rs-ffm-s3)
**overturned that**:

| FFI root | inclusive samples |
|---|---|
| `frs_vectorized_batch_get` | **37,878** |
| `frs_vec_iter_prefix_open` | 3,839 |
| everything else | < 400 |

batch GET — not iteration — dominates by ~10×. The release profile strips
symbols (`strip="symbols"`); rebuilding with `--config profile.release.debug=1
--config profile.release.strip=false` (identical lto/opt-level, so
perf-representative) symbolized the hot stack:

```
frs_vectorized_batch_get (lib.rs:2931)
└ DbImpl::batch_get_vectorized
  └ DbImpl::prefetch_sst_files_for_batch          ← 58% of this call site
    └ CachedFileSystem::fetch_through_cache        (whole-SST download)
      └ OpendalRandomAccessFile::read_at
        └ opendal BlockingWrapper::read → tokio park::Inner::park
          └ _pthread_cond_wait → __psynch_cvwait   ← BLOCKED on S3 round-trip
```

The operator thread was **blocked on synchronous, whole-file S3 downloads**
inside the batch-get prefetch.

## Root cause

`batch_get_vectorized` resolves keys in phases: 1 active memtable → 2 **SST
prefetch** → 3 immutable memtables → 4 **resident-flushed memtables (RAM)** →
5 SST reads. Phase 4 serves keys from resident-flushed memtables that are
byte-identical to their just-flushed L0 SSTs, and Phase 5 only reads SSTs for
keys *still* pending after Phase 4.

But `prefetch_sst_files_for_batch` (Phase 2) had three problems:

1. **It ignored Phase 4.** It downloaded — from S3 — exactly the SSTs whose
   data was already resident in RAM and about to be served by Phase 4. For q9's
   join state (lives in the memtable + resident-flushed RAM) that is a wholly
   redundant S3 round-trip on every batch.
2. **It ignored its `keys` argument** (both params were `_`-prefixed) and
   prefetched *every* uncached SST in the whole version, including SSTs the
   batch never reads.
3. **It fetched serially** — one whole SST at a time, blocking.

## Fix (`db.rs::prefetch_sst_files_for_batch` + `cached_fs.rs` + `filesystem.rs`)

1. **Resident-shadow skip (the big one):** build the resident-shadowed file-
   number set (`resident_flushed_visible_entries`) and skip those SSTs — mirrors
   `build_lazy_prefix_key_stream`. Reads for those keys come from RAM in Phase 4.
2. **Range-filter:** compute `[batch_min, batch_max]` and skip SSTs whose
   `[smallest,largest]` does not overlap.
3. **Concurrent fetch:** new `FileSystem::prefetch_concurrent` (default serial;
   `CachedFileSystem` overrides with `prefetch_files_concurrent`) fans the
   surviving cache-miss fetches out across bounded scoped threads, reusing
   `fetch_through_cache` UNCHANGED (its short-read/size-verify/`await_upload`
   correctness logic is preserved verbatim). K misses → ≈1 round-trip.

## Result (measured, 8c/32g, forst-rs-ffm-s3, single runs — S3 noise applies)

- Source-throughput trajectory to 3.26M records:
  - baseline (serial whole-version prefetch): **141 s**
  - resident-shadow skip + range-filter + concurrent: **101 s** (~40% faster to
    the same point; sustained ~39K/s through 101s before the first stall).
- Re-sampled at the stall: the `prefetch_sst_files_for_batch →
  fetch_through_cache` blocking path is **entirely gone** (0 frames). The
  redundant whole-file S3 downloads were eliminated.
- All engine + storage unit suites pass (no correctness regression).

## Remaining (next lever, identified not yet fixed)

After removing the redundant prefetch, `batch_get`'s residual S3 blocking
shifted to the **genuine** cold path: Phase 5
`SstReaderImpl::get_versions → read_data_block →
RangeCachedRandomAccessFile::serial_read_at → opendal read_at → cond_wait`
— on-demand ranged block reads for keys NOT covered by resident-flushed RAM
(e.g. evicted under the 1 GiB resident cap, or L1+ compaction output). This is
a smaller, legitimate per-block read (not a whole file), but still serial +
blocking across the L0 fan-out. Candidate follow-ups:
- Parallelize Phase 5's L0 block reads (read blocks for all pending keys × L0
  files concurrently).
- Re-tune the resident-flushed cap (lowered to 1 GiB for the 8c/32g OOM fix) —
  bigger cap = more Phase 4 RAM hits, but OOM risk; needs measurement.
- Verify the decoded/range block cache covers hot blocks across batches.

3× total is not yet established; this removes one concrete, measured class of
redundant S3 reads on the heaviest query. A full q0–q22 sweep on the fixed code
is still required to measure the binding ratio.
