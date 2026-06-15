# FRS-MULTIGET-COALESCE — coalesced SST multi-key block read (the disagg `RetrieveMultipleBlocks` lever)

Date: 2026-06-15
Owner: PMC-2 (Phase-2, Disaggregated State)
Status: SHIPPED — flag-gated **default-OFF**, byte-identical OFF, mini-benched, suites green.

## 1. The gap (evidence-grounded, ForSt C++ vs forst-rs)

ForSt's C++ engine reads the data blocks for a `MultiGet`'s keys with ONE
vectored `Env::MultiRead()` call:

- `table/block_based/block_based_table_reader_sync_and_async.h:32` —
  `DEFINE_SYNC_AND_ASYNC(void, BlockBasedTable::RetrieveMultipleBlocks)`:
  *"This function reads multiple data blocks from disk using `Env::MultiRead()`
  … It uses the scratch buffer provided by the caller, which is contiguous."*
  Line ~146: `s = file->MultiRead(opts, &read_reqs[0], read_reqs.size(), …)`.

forst-rs's `batch_get_vectorized` (`crates/forst-rs-engine/src/db.rs`, the L1+
walk) already groups pending keys **by file** (one reader open per file —
`get_or_open_sst_reader`), but then resolves each key with an INDEPENDENT
`reader.get_versions(k)` call, and `get_versions`
(`crates/forst-rs-storage/src/sst/reader.rs`) reads its candidate block via a
per-call `read_decoded_block`. So **N keys landing in N distinct cold blocks of
one SST pay N separate block reads.**

In the disaggregated regime each cold block read is a local-cache pread (on a
local-cache miss, a remote GET). A scattered point-lookup batch (Flink
ValueState / MapState lookups, the q4/q7/q9-class probe shape) whose keys spread
across M blocks of one SST issues M round-trips that could be ONE vectored read.
This is the last remaining FS-level read-coalescing gap: the vlog (blob) deref
side is already coalesced (FRS-VLOG-COALESCE, group-by-segment + offset-sort +
fan-out), and the scan side is covered by the `BlockPrefetcher` windows — but the
**point-get SST data-block** side was per-key.

## 2. Design

A PURE PREFETCH that warms the decoded-block cache, leaving every byte of the
resolution logic untouched.

`SstReaderImpl::prefetch_blocks_for_keys(&self, keys: &[&[u8]])`
(`crates/forst-rs-storage/src/sst/reader.rs`):

1. Compute the candidate data-block set for all keys using the EXACT predicate
   `get_versions` uses (footer range, full bloom, sparse-index seek, then the
   forward walk over blocks whose per-block `[min_key,max_key]` still contains
   the key). Dedup + order.
2. Drop blocks already decoded-cache-resident (cache-first window splitting,
   same as `fetch_window`).
3. Group the remaining (missing) blocks into runs of physically-contiguous file
   ranges (the writer lays blocks back-to-back) → ONE `read_block_regions`
   vectored read (io_uring single submission on Linux, serial preads on the
   portable fallback — bit-identical).
4. Decode each block from its slice and `cache_insert_decoded(Low)` — the SAME
   priority `read_decoded_block` uses.

Then the existing per-key `get_versions` loop runs UNCHANGED; each call now finds
its block warm in the cache. No-op (cheap return) when there is no block cache,
or no missing blocks remain after the filters.

Engine wiring (`db.rs`, L1+ per-file group loop, flag
`FRS_MULTIGET_COALESCE`, default OFF): when ON and >1 pending key landed in the
file, call `reader.prefetch_blocks_for_keys(file_keys)` before the per-key loop.

### Why byte-identical OFF and ON

The prefetch only POPULATES the cache, over exactly the blocks the per-key path
would read, at the same `Low` priority. The resolved values, merge-chain
handling, sequence ordering, op-type handling, and BlobRef-deferral are all
unchanged. The only difference is HOW the blocks reach the cache (one vectored
read vs N per-block reads). With the flag OFF the prefetch is never called.

## 3. Correctness

- `db.rs::test_multiget_coalesce_byte_identical_l1_multiblock` — over a compacted
  L1 SST with `block_size=512` (many blocks), a scattered 300-key+miss batch:
  `batch_get_vectorized` ON == OFF == N independent `get()` calls, value-for-value;
  empty + single-key batches no-op cleanly.
- `db.rs::test_multiget_coalesce_flag_default_off_and_override` — flag/override
  semantics.
- `reader.rs::prefetch_blocks_for_keys_coalesces_io_then_get_versions_is_free` —
  a counting `RandomAccessFile` proves (a) the prefetch issues `reads <= key
  count` (contiguous runs collapse), (b) the following per-key `get_versions`
  loop issues **0** file reads (all warm), (c) a no-block-cache reader no-ops
  cleanly.
- Full suites green: `forst-rs-storage` 484/0, `forst-rs-engine` 435/0.

## 4. Mini-bench (sim-S3, RTT-per-GET)

`crates/forst-rs-bench/benches/multiget_coalesce.rs`. Each iteration opens a
FRESH reader (COLD block cache) over a `RandomAccessFile` that sleeps a fixed
per-read RTT (`FRS_BENCH_GET_RTT_US`, default 200 µs — the remote GET round-trip
fixed cost; a byte-rate throttle cannot capture it because coalescing collapses
ROUND-TRIPS, not bytes). `is_local()` reports remote. SST: `block_size=512`,
2000 keys; probe = scattered batch.

- `off` — per-key `get_versions` over the cold reader (one RTT read per block).
- `on`  — `prefetch_blocks_for_keys` (ONE coalesced vectored read) then per-key
  `get_versions` (warm).

`FRS_BENCH_GET_RTT_US=200`, 20 samples:

| batch | off | on | speedup |
|------:|----:|---:|--------:|
|  32   | 9.00 ms | 7.94 ms | 1.13× |
| 128   | 30.22 ms | 20.16 ms | 1.50× |
| 256   | 47.82 ms | 19.09 ms | **2.50×** |

The win scales with batch size exactly as the `RetrieveMultipleBlocks` model
predicts: more keys → more distinct cold blocks → the coalesce collapses more
round-trips into the file's handful of contiguous runs. At 256 keys the cold
batch is 2.5× faster. On a real remote tier (ms-class RTT) the absolute delta is
strictly larger (this bench's RTT is a conservative 200 µs).

## 5. Scope / honesty

- Helps the **cold, scattered, same-SST multi-key** point-get batch on the disagg
  tier — the only case where the per-key path pays multiple distinct cold block
  reads. Warm batches (blocks cache-resident) do ZERO extra I/O (no-op).
- Orthogonal to FRS-VLOG-COALESCE (that coalesces the blob deref AFTER the SST
  block read resolves a `BlobRef`; this coalesces the SST block read itself).
- Default OFF pending a live-box e2e A/B (the local Docker box's local-cache
  preads are µs-class, so the realized NEXMark win is gated on the real remote
  tier where the RTT is ms-class — same gating as the other Phase-2 read-IO
  levers).
