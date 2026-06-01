# Streaming memtable snapshot — halves checkpoint-without-flush peak memory

Date: 2026-06-01
Status: LANDED (engine + unit tests). Cluster validation pending (sweep using
the cluster). Lever for: keep heavy-query state RESIDENT in a larger memtable
without OOM → finish q4/q7/q9 at resident speed.

## Problem

`Db::snapshot_memtables_to_dir` (the checkpoint-without-flush artifact writer)
did, per CF:

1. `snapshot_batches_bounded(...)` → builds the WHOLE memtable as a
   `Vec<RecordBatch>` (~memtable size, e.g. 4 GB).
2. `serialize_memtable_batches(&batches)` → builds a SECOND full copy as a
   `Vec<u8>` (~4 GB) before any bytes hit disk.
3. `std::fs::write(tmp, &bytes)`.

So peak = live memtable (4 GB) + batches Vec (4 GB) + serialized bytes (4 GB)
= ~3× the memtable. With 4 join subtasks checkpointing concurrently on one
TaskManager, 4 × 12 GB = 48 GB → OOM on a 32 GB box. This is why a 4 GB memtable
OOM'd q9 (NoResourceAvailableException = TM died), forcing the 2 GB memtable
that spills heavy-query state to S3 SSTs and re-triggers the decode collapse.

## Fix (Win 1: eliminate the serialized-bytes copy)

New `memtable::serialize_memtable_batches_to_writer(batches: impl IntoIterator,
writer: W)` streams the Arrow IPC stream straight into the staging file,
`drop`-ing each batch immediately after `StreamWriter::write`. No intermediate
`Vec<u8>`. `snapshot_memtables_to_dir` now `std::mem::take`s the batches Vec into
the streamer over a `BufWriter<File>` (BufWriter coalesces the many small IPC
writes into large sequential file writes), with an explicit `into_inner().flush()`
so ENOSPC/I-O errors surface instead of being swallowed by BufWriter's drop-flush
(which would otherwise rename a truncated artifact into place).

Peak now = memtable (4 GB) + batches Vec (4 GB) = ~2× → 4 × 8 GB = 32 GB,
right at the edge but feasible (a 3 GB memtable → 4 × 6 GB = 24 GB, comfortable).

Byte-for-byte identical to the buffered serializer (unit test
`streaming_writer_matches_buffered_serializer_byte_for_byte`), so the same
`deserialize_memtable_batches` reader round-trips it on restore. Empty-memtable
parity covered (`streaming_writer_empty_iter_produces_empty_stream`).

## Tests

- forst-rs-storage: `memtable::artifact` 4/4 pass (incl. byte-parity + empty).
- forst-rs-engine: `snapshot_memtables_to_dir_round_trips_through_artifact_and_replay`
  pass.

## Win 2 (LANDED): lazy batch building → peak = memtable + one batch

`build_sorted_batches_bounded` (vectorized.rs) was refactored to share its loop
with a new `for_each_sorted_batch_bounded(batch_size, max_seq, |batch| ...)` that
builds → emits → DROPS one ~1.5 GiB batch at a time (the old fn now just collects
into a Vec via the same path — flush path unchanged). Exposed up the stack:

- `VectorizedMemTable::snapshot_batches_bounded_for_each` (folds unsorted, streams)
- `ShardedMemTable::snapshot_batches_bounded_for_each` (single-shard = production
  default: true streaming; multi-shard: concat+lexsort then emit — no streaming
  benefit there, but it's not the OOM case)
- `memtable::MemtableArtifactWriter<W>` — incremental Arrow-IPC writer with lazy
  header init (schema from first non-empty batch; all-empty memtable writes
  nothing), explicit `finish()` flush, and a `wrote_any` return.

`snapshot_memtables_to_dir` now streams the active + each immutable memtable into
one `MemtableArtifactWriter<BufWriter<File>>`, so peak memory is ONE batch, not
the whole memtable. Peak = live memtable (4 GB) + one batch (~1.5 GB) = ~5.5 GB
→ 4 × 5.5 = 22 GB, comfortable on a 32 GB box for a 4 GB memtable. This is the
enabler for holding heavy-query state RESIDENT (q4 already showed 554 K/s >
RocksDB while resident) without the spill-to-S3-SST decode collapse.

Tests: storage memtable 88/88, engine snapshot round-trip + 26 snapshot tests,
artifact byte-parity + empty-stream, FFI clean compile.

## Validate after the sweep

Re-raise `writebuffer.size` toward 4096mb (was reverted to 2048mb to dodge the
OOM) and re-run the heavy queries; confirm no `NoResourceAvailableException` and
that q4/q7 stay resident (no spill-flush rate collapse).
