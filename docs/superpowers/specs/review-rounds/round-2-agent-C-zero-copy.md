# Round 2 — Agent C — End-to-End Zero-Copy (re-audit)

**Reviewer:** Agent C
**Round:** 2 (post C-H4 fix)
**Date:** 2026-05-22
**Methodology:** Verified C-H4 landing on the listed files; rescanned `crates/forst-rs-storage/`, `crates/forst-rs-engine/`, `crates/forst-rs-io/`, and `crates/forst-rs-ffi/` for NEW violations not in Round 1 (C1–C17).

---

## C-H4 verification — FIX LANDED CLEANLY

**`crates/forst-rs-engine/src/list_merge.rs`** (lines 47–67):
- Two new methods added: `combine_slices(&[&[u8]])` and `combine_with_base_slices(&[u8], &[&[u8]])`. Both pre-size the output `Vec` to `total = sum(len)` and do a single `extend_from_slice` per operand. No intermediates. Output `Vec<u8>` is the only allocation, and it is the final merged value that goes straight to `db_ref.put(cf_ref, key, &merged)`. **This is fine — the put consumes a slice, but the merged blob must materialize once.** No avoidable allocation here.
- Legacy `combine` / `combine_with_base` retained for callers that already own `Vec<u8>` (no caller hot-path I could find still uses them outside tests).

**`crates/forst-rs-ffi/src/lib.rs:4022-4055`** (`frs_vec_merge_append_batch`):
- `HashMap<&[u8], Vec<&[u8]>>` — both key and operand storage are now borrowed slices over the caller's stable `keys_data` / `ops_data` buffers. The `Vec<&[u8]>` only stores fat pointers, not bytes.
- Per-group dispatch uses `combiner.combine_slices(ops.as_slice())` / `combine_with_base_slices(&existing, ops.as_slice())`. **No `to_vec()` per operand remains.**

**Round 1 estimate confirmed:** at Q19's ~3 LIST_ADD/event × 100M events = 300M per-operand clone+memcpy cycles eliminated. C-H4 closed.

---

## NEW findings (not in Round 1)

| Sev | # | Location | One-line |
|-----|---|----------|----------|
| H | C-R2-H1 | `cached_fs.rs:182-198` `fetch_through_cache` | 64-KiB-chunked `extend_from_slice` loop with no `Vec::with_capacity(remote.stat().size)` — amortized 3-5× O(N) memcpy on every SST cold-load |
| H | C-R2-H2 | `sst/reader.rs:319, 396, 404` | `values.value(row).to_vec()` per SST read row — Arrow `BinaryArray` value is a `&[u8]` view into the IPC payload; `to_vec()` heap-clones every value returned from `get` / `scan` |
| H | C-R2-H3 | `memtable/vectorized.rs:642-643, 1293` | `value_at(idx.offset).map(|s| s.to_vec())` + `key.to_vec()` per `ScanRow` and per `GetResult` — same anti-pattern as C-R2-H2 but on the memtable iterator path (V1-sync `forward_prefix` and snapshot-aware reads) |
| M | C-R2-M1 | `db.rs:985` `get_at` (snapshot-aware GET) | `mvcc::get_at(...).map(\|s\| s.to_vec())` — every snapshot read clones the value out of the memtable/SST view; called per Java-side recovery key |
| M | C-R2-M2 | `sst/writer.rs:293-295` | `key_array.value(num_rows-1).to_vec()` × 3 per flushed data block (last_key, min_key, max_key) — per-block (not per-row), but compounds C11 |
| M | C-R2-M3 | `compaction.rs:125` | `all[i].key.clone()` per distinct key while consolidating versions — per-distinct-key during every compaction pass |

---

## H-level NEW

### C-R2-H1 — `CachedFileSystem::fetch_through_cache` grows Vec with no capacity hint

`crates/forst-rs-storage/src/cached_fs.rs:182-199`:

```rust
let mut reader = self.remote.open_sequential_file(path)?;
let mut bytes = Vec::new();                       // ← no Vec::with_capacity
let mut chunk = vec![0u8; 64 * 1024];
loop {
    let n = reader.read(&mut chunk)?;
    if n == 0 { break; }
    bytes.extend_from_slice(&chunk[..n]);         // ← amortized memcpy
}
```

Why H: every cold SST load (the S3-prefetch hot path the doc literally markets) drives `Vec` from 0 → file_size via repeated doubling. For a 64 MiB SST that's `0 → 32 KiB → 64 KiB → 128 KiB → ... → 64 MiB`, each grow memcpys the prior contents. Total memcpy ≈ 2× file_size on top of the 64 KiB scratch's `extend_from_slice` for each chunk. Combined with C-R1-H2's downstream `OpendalRandomAccessFile` copies, S3 → engine is now **4-5×** the file size in memcpy traffic per cold load.

**Fix shape:** `SequentialFile::file_size()` (which exists — see `RandomAccessFile::file_size`) → `Vec::with_capacity(file_size)`. One-line change; eliminates the doubling. Also: the scratch `chunk` could `read` directly into `bytes` via `read_exact` + `bytes.reserve(file_size).set_len(...)` (unsafe-but-mechanical), removing both intermediate copies.

### C-R2-H2 — SST reader copies every value out of Arrow on read

`crates/forst-rs-storage/src/sst/reader.rs:319, 396, 404`:

```rust
// get():
Some(values.value(best_row).to_vec())              // ← line 319

// scan():
Some(values.value(row).to_vec())                   // ← line 396
...
out.push((key.to_vec(), value, sequences.value(row), op));  // ← line 404
```

Why H: the data block IS an Arrow `RecordBatch` (Arrow IPC encoded, see `sst/data_block.rs:15-31`). `BinaryArray::value(i)` returns `&[u8]` that aliases the IPC payload's flat `data: u8[]` buffer. By calling `.to_vec()` we eagerly copy every value out, defeating the entire Arrow zero-copy reuse story. For a `scan` returning N rows, this is N value-clones + N key-clones — all of them avoidable because the `RecordBatch` Arc is held by the caller's `LookupResult`/`SstScanRow`.

**Fix shape:** change `LookupResult.value: Option<Vec<u8>>` → `Option<arrow::buffer::Bytes>` (zero-copy `Bytes` slice into the underlying Arrow buffer), or simply hand back the `Arc<RecordBatch>` + row index. The current Rust → FFI shape (`FrsBytes`) eventually requires a contiguous byte pointer, but the conversion can be one `Arc`-clone instead of a memcpy. This is also the **lever for the "Arrow zero-copy analytics" claim**: external Parquet/DataFusion consumers can ingest the SST `RecordBatch` directly via `arrow::ipc::reader::StreamReader`.

### C-R2-H3 — Memtable scan/get clones key+value per row

`crates/forst-rs-storage/src/memtable/vectorized.rs:642-643, 1293`:

```rust
// collect_range_entries() — V1-sync forward_prefix path:
let v = self.value_at(idx.offset).map(|s| s.to_vec());
out.push((key.to_vec(), v, idx.sequence, idx.op_type));

// find_latest() — snapshot-aware get:
best.map(|idx| GetResult {
    value: self.value_at(idx.offset).map(|v| v.to_vec()),
    ...
})
```

Why H: the memtable's `value_at` returns `&[u8]` referencing the underlying Arrow `BinaryArray` buffer that the memtable owns and **outlives every scan call**. The clone-per-row is the symmetric Rust-side companion to **C1 (Java VectorizedExecutor `new byte[len]` per GET result)** — fixing one without the other is half a win. For Q3/Q5/Q19 prefix scans this is `O(emitted_rows) × value_size` of pure memcpy on top of the FFI hand-off.

**Fix shape:** return a borrowed view (`&'a [u8]` tied to a `&'a VectorizedMemTable`) from the internal API; only materialize the `Vec` at the FFI boundary where ownership transfer is genuinely required. Even there, prefer the `FrsBytes` pattern of leaking an `Arc<Bytes>` slice rather than allocating a fresh `Vec`.

---

## M-level NEW (briefly)

- **C-R2-M1** `db.rs:985`: `.map(|s| s.to_vec())` on every snapshot-aware GET. Recovery/checkpoint-restore path; not per-record in steady state but heavy during restart.
- **C-R2-M2** `sst/writer.rs:293-295`: 3× `to_vec()` per flushed data block (for last_key / min_key / max_key tracking). Per-block, not per-row, so 1-3 MiB SSTs amortize fine; flagged because it's the same anti-pattern as C11.
- **C-R2-M3** `compaction.rs:125`: `all[i].key.clone()` per distinct key during version-walking. Per compaction pass; compaction is already off the hot path, but flagged for completeness.

---

## Arrow zero-copy reuse — design assessment

**Storage format IS Arrow IPC.** `sst/data_block.rs:49-66` writes via `arrow::ipc::writer::StreamWriter` and `sst/reader.rs` decodes via `StreamReader`. **The on-disk + in-memory representation is already in Arrow's columnar binary layout.** That's a meaningful architectural choice that opens these downstream zero-copy paths:

1. **DataFusion / Polars / Parquet integration:** an external reader can mmap an SST data block, parse the `BlockHeader`, and feed the compressed IPC payload directly into `StreamReader` without going through any forst-rs API. Zero-copy from SST → analytics frame.
2. **Arrow Flight / Flight SQL servers** can stream SST blocks as Flight messages with no transcoding.
3. **Snapshot export to Parquet:** a sweep can call `arrow::parquet::arrow::ArrowWriter::write(batch)` on the same `RecordBatch` the SST reader already produced — no row-by-row materialization.

**However**, the *internal* API surface (`LookupResult`, `SstScanRow`, `GetResult`, `ScanRow`) discards this advantage by eagerly `to_vec()`-ing every value out of the Arrow buffer (see C-R2-H2, C-R2-H3). The format is Arrow-zero-copy-ready; the reader API is not. **There is no documented design choice rationalizing this** — no comments at the offending lines indicate it was deliberate. It reads as an early-development "make it work" shape that should now move to a borrow-or-Arc-share return.

**Recommendation (cross-cutting):** introduce a `RowView<'a>` (or `BorrowedScanRow<'a>`) that holds `(arrow::buffer::Buffer, key_range, value_range, seq, op)` and gate `to_vec()` behind an explicit `RowView::to_owned()` only at the FFI boundary. Match the upstream `FrsBytes` shape: leak an `Arc<Bytes>` slice instead of moving a `Vec<u8>`. This closes both H findings and unlocks the Arrow analytics theme that the data-block layout has *already paid for*.

---

## Out of scope this round

- **Java FFM bridge files** (`ForStRsLinker.java`, `VectorizedExecutor.java`, `AppendMergeBatchBuffer.java`, `ForStRsMapStateV2.java`): not present in this repo tree (only `nexmark/nexmark-flink/` Java sources are co-located). Round 1's C1, C5, C6, C7 are documented and remain open; cannot re-verify their state from this worktree.
- **MemorySegment → byte[] copy hotspots in snapshot/iterator paths (Java)**: same — the Java-side iterator decode and snapshot serialization paths live outside this repo; flagged by reference only.
- **Arrow IPC mmap path:** the storage layer reads via `RandomAccessFile::read_at` into an owned `Vec<u8>` (`sst/reader.rs:228-230`); switching to a true mmap is a larger refactor and out of scope.

---

## Summary

- C-H4 fix verified clean — no remaining `Vec<Vec<u8>>` path in `frs_vec_merge_append_batch`.
- `combine_slices` impl is minimal: one output `Vec` (necessary), zero intermediates.
- **3 NEW HIGH findings** in the Rust storage/engine path that no prior round caught:
  - C-R2-H1: cached_fs prefetch grows Vec without capacity hint → 4-5× memcpy per cold SST load.
  - C-R2-H2: SST reader `to_vec()` per row defeats Arrow IPC zero-copy.
  - C-R2-H3: Memtable scan/get `to_vec()` per row — symmetric Rust-side companion to C1.
- **Arrow analytics opportunity confirmed**: storage IS Arrow IPC; the *internal API* discards the advantage at the read boundary. Not a documented design choice — likely an early-stage shape that should move to borrow-or-Arc-share returns.

**End of Round 2 — Agent C**
