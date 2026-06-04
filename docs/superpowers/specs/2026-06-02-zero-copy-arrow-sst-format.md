# Zero-copy Arrow SST format (mmap, no decompress/parse/copy on read)

**Status:** DESIGN — for review. **Date:** 2026-06-02. **Branch:** forst-rs.

## Motivation (user, 2026-06-02)

> Why decode Arrow after reading from disk? All should be Arrow format; the file on
> disk or S3 should be Arrow-friendly so we can use Arrow offsets to read directly.

forst-rs is Arrow-native end-to-end EXCEPT at the SST boundary, where every block read
pays three transforms. This spec removes them for the local (NVMe/cache) path.

## Current format (the cost)

- **Write** (`sst/data_block.rs::encode_data_block`): `RecordBatch` → Arrow IPC **StreamWriter**
  (serialize) → **LZ4** compress (`sst/compression.rs`, default `Lz4`).
- **Read** (`sst/reader.rs::read_data_block`): read raw bytes → **LZ4 decompress** → Arrow IPC
  **StreamReader** parse → **owned `RecordBatch`** (columns copied out of the scratch buffer).

So a block-cache MISS = decompress + IPC parse + column copy. (A cache HIT returns the cached
decoded `RecordBatch` and skips all three — which is why the in-RAM block cache exists.)

### Why it's currently decode-on-read

1. **LZ4** shrinks bytes on disk/S3 — a real win on the ~10–18 MB/s S3 uplink, at decompress CPU cost.
2. **IPC Stream format** is sequential (length-prefixed messages) — not random-access / mmap-friendly.
3. **Owned copy** lets the per-thread read scratch buffer be reused across block reads.

## Measured relevance (honest)

The q7 backpressure re-diagnosis (2026-06-02) showed q7's wall is the **Join operator CPU across all
cores**, NOT block I/O/decode — a 2 GiB block cache (which avoids decode on hits) gave ~0% on q7. So
**this format change is NOT a q7 fix.** Its real payoff:
- **Cold S3 reads** (restore, evicted entries) — avoid the decode on the slowest path.
- **Memory** — a zero-copy block cache holds `Arc<mmap-slice>` views instead of owned decoded batches.
- **Read-heavy keyed queries** that DO bottleneck on block decode (to be confirmed per-query).

## Target design

**Store SST data blocks as uncompressed, 8/64-byte-aligned Arrow IPC FILE-format messages; on read,
mmap the local file and construct the `RecordBatch` as a ZERO-COPY view over the mapped bytes.**

1. **On-disk block = Arrow IPC encapsulated message, uncompressed, padded to 64B.** Arrow's zero-copy
   contract: array buffers must be in-place and aligned; the IPC message body already lays columns out
   contiguously, so a `RecordBatch` can borrow them without copying when the backing bytes are aligned
   and uncompressed.
2. **mmap the local SST/cache file** (via `memmap2`; requires lifting the crate's `forbid(unsafe_code)`
   in a single audited module, or an `unsafe`-isolated helper). The `LocalCache` already keeps hot SSTs
   on NVMe (write-through), so the bytes are local and mmap-able. (S3 objects are NOT mmap-able — the
   cold-read path still downloads, but writes through to the local cache, after which reads are mmap'd.)
3. **Zero-copy `RecordBatch`**: use `arrow::ipc::reader::read_record_batch` over an `arrow::buffer::Buffer`
   constructed from the mmap slice (`Buffer::from_custom_allocation` / from a borrowed `&[u8]` with a
   lifetime tied to the mmap `Arc`). No decompress, no parse-into-owned, no column copy.
4. **Block cache stores zero-copy views**: `CacheEntry::MmapBatch(Arc<Mmap>, RecordBatch)` — the batch
   borrows the mmap; eviction drops the `Arc<Mmap>`. Far less RAM than owned decoded batches, and a hit
   is a pointer, not a decode.

## Compression strategy (keep the S3 win without paying decode on hot reads)

- **Tiered**: keep LZ4 for **upper LSM levels / cold** (S3-bound, bytes matter); store **L0 + hot**
  blocks uncompressed + mmap-able (read-bound, CPU matters). Compaction transcodes on level change.
- Or **uncompressed everywhere on local**, LZ4 only for the S3-uploaded copy (the LocalCache write-through
  copy is uncompressed + mmap'd; the remote copy is compressed for transfer). Cleaner read path; more
  local disk (the 192 GiB NVMe cache has room).
- Decision gated on a per-query measurement of decode cost vs S3-transfer cost.

## Risks

- **`unsafe` / mmap lifecycle**: the crate is `forbid(unsafe_code)`; mmap needs a single audited unsafe
  module (or `memmap2`). munmap-on-eviction + outstanding zero-copy `RecordBatch` views must be
  refcounted (`Arc<Mmap>`) so a borrowed batch never outlives its mapping.
- **Alignment**: Arrow zero-copy requires 8/64-byte-aligned buffers; the writer must pad. Mis-alignment
  silently falls back to a copy (or panics) — needs a test asserting zero-copy actually happened.
- **Endianness / schema-per-block**: IPC File format carries the schema; keep one schema per SST (already
  true) and write it once in the file footer, not per block, to avoid per-block schema overhead.
- **S3 path unchanged** (not mmap-able) — design must degrade gracefully to download+decode for cold
  remote reads, mmap only the local write-through copy.

## Phasing (each TDD + benchmarked)

1. **Writer**: add an uncompressed, aligned IPC-File block-encoding option behind a `CompressionType::None`
   + alignment flag; round-trip test.
2. **Reader mmap path**: `RangeCachedRandomAccessFile` gains a zero-copy `read_block_mmap` that returns a
   `RecordBatch` borrowing an `Arc<Mmap>`; assert no copy (buffer ptr inside the mmap).
3. **Block cache**: `CacheEntry::MmapBatch` variant; eviction drops the mapping.
4. **Compaction transcode** for the tiered strategy.
5. **Benchmark**: per-query decode-cost delta; confirm the local-read path is now decode-free.

## Relationship to other specs

- Complements [lock-free engine](2026-06-02-lock-free-engine-design.md) (lock-free reads) and the
  fd-cache (55187ba86, fewer open() syscalls). Together: read path = mmap (no open per block via fd-cache)
  + zero-copy Arrow (no decode) + lock-free (no read lock).
- NOT a q7 fix (q7 = Join CPU across cores); q7's lever is per-record CPU reduction (batch execution /
  cache locality) — separate track.
