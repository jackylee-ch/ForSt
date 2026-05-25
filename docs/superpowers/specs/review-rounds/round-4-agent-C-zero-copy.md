# Round 4 — Agent C — Zero-Copy (verification + NEW findings)

**Reviewer:** Agent C
**Round:** 4
**Date:** 2026-05-22
**Scope:** Verify PR-D1, PR-D2, PR-B1, Cleanup-A2/C1/C3, PR-F3, PR-E4 against on-disk code; scan all 27 landed PRs for new zero-copy regressions.
**Method:** Read-only code review on `forst-rs` branch (HEAD = `6f7ae6b19`).

---

## Per-PR verification status

| PR | Status | Evidence (file:line) |
|----|--------|----------------------|
| PR-D1 (opendal Bytes) | VERIFIED | `crates/forst-rs-io/src/opendal_backend.rs:347` field `bytes: Bytes`; `:404` `buffer.copy_to_slice` (no `to_vec`); `:507` `std::mem::take(&mut self.buffer).into()` zero-copy Vec→Bytes; `:554` `buffer.to_bytes()` in `open_sequential_file`. Closes Z3-2/3/4. |
| PR-D1 (cached fs Bytes) | VERIFIED | `crates/forst-rs-storage/src/cached_fs.rs:174-218` `fetch_through_cache(...) -> ForstResult<Bytes>`; `Bytes::from(vec)` zero-copy hand-off at line 183 + 217. Closes Z3-9. |
| PR-D2 (Bytes min/max/last-key) | VERIFIED | `crates/forst-rs-storage/src/sst/writer.rs:118-124` fields `global_min_key/global_max_key/last_added_key: Option<Bytes>`; `:240-251` conditional update (allocs only on bound change); `:256-259` `last_added_key` set under `debug_assert!` only. Compile-time-pinned by test `sst_writer_min_max_key_uses_bytes` (`:978-1006`). Closes C-R3-H3. |
| PR-D2 (streaming compaction) | VERIFIED | `crates/forst-rs-engine/src/compaction.rs:161` `SstWriterImpl::with_options(...).streaming(&mut *wf)`; output SST streams block-by-block. Closes C-R3-H1 **output side**; **input side** still buffers (see new finding below — flagged as known/documented partial close). |
| PR-D2 (streaming flush) | VERIFIED | `crates/forst-rs-engine/src/flush.rs:116` `writer_inner.streaming(&mut *writable)`; `:124-160` row-stream via `writer.add`. No `writer.finish() -> Vec<u8>` materialisation. Closes C-R3-H2. |
| PR-B1 (Java GET-result zero-copy decode) | NOT VERIFIABLE | Java sources for `ForStRsValueStateV2` / `ForStRsKeyedStateBackend` etc. are out of tree (only `nexmark/` Java present). Status unchanged from Round 3. |
| Cleanup-A2 (ValueStateV2 single-sweep + Slot[]) | NOT VERIFIABLE | Out of tree (same as PR-B1). |
| Cleanup-C1 (MapStateArrowBuffer TOMBSTONE) | NOT VERIFIABLE | Out of tree. |
| Cleanup-C3 (Reducing/Aggregating cache zero-alloc) | NOT VERIFIABLE | Out of tree. |
| PR-F3 (MapStateCache off-heap + reusable Lookup) | NOT VERIFIABLE | Out of tree. |
| PR-E4 (4 V1-sync `getCopyOfBuffer` sites) | NOT VERIFIABLE | Out of tree. |

**Verifiable scope:** 5/11 items (Rust side). **Out-of-tree (Java):** 6/11 — same constraint reported in Round 2 and Round 3.

---

## NEW HIGH findings (count = 0)

No NEW zero-copy HIGH findings in the 27-PR landing set.

### Scans run

| Scan | Sites checked | Result |
|------|--------------|--------|
| `byte[] = new byte[]` on per-event paths | n/a (Java OOT) | Cannot scan |
| `getCopyOfBuffer()` added | n/a (Java OOT) | Cannot scan |
| `Bytes::to_vec` / `.clone()` of large buffers in Rust | `sst/writer.rs`, `flush.rs`, `compaction.rs`, `opendal_backend.rs`, `cached_fs.rs`, `sst/reader.rs` | All remaining `.to_vec()` / `.clone()` sites are either (a) once-per-SST footer materialisation, (b) once-per-block min/max/last-key (bounded by block count, NOT row count), (c) the single documented per-row CompactionEntry materialisation that survives by design, or (d) test-only — no regressions vs. Round 3. |

### Notable but non-HIGH observations

1. **Per-block alloc count drift in `SstWriterImpl::flush_block`** (`sst/writer.rs:464-466`): doc-comment says "2 allocations per block" but code does 3 (`last_key`, `min_key`, `max_key` each `to_vec()` from `key_array.value(...)`). Cost is O(block_count), not O(rows) — within the PR-D2 envelope, doc-comment is the only drift. Logged as L (cosmetic).
2. **C-R3-H1 input side still materialises** (`compaction.rs:84` `all: Vec<CompactionEntry>` with per-row `to_vec()` at `:93-94`): PR-D2 description explicitly states "closes ... C-R3-H1/H2/H3 — partial" and the in-code comment at `:79-99` acknowledges this as the single unavoidable materialisation point (sort buffer outlives each input block's `RecordBatch`). Not a NEW finding — it's the documented partial close. Carrying forward to the H-catalogue would require a streaming k-way merge over `read_block_at(...)`; tracked separately (not in this 27-PR batch).
3. **`open_writable_file` Append-mode `to_vec()`** (`opendal_backend.rs:589`): one alloc per file open in `WriteMode::Append` (not per-row, not per-event). Acceptable; SST/WAL writers use `CreateNew`, not `Append`.

---

## Round-3 delta verification

| R3 H | Status |
|------|--------|
| C-R3-H1 (compaction materialises full input + full output) | PARTIAL CLOSE — output streamed (verified); input buffer remains by design (documented in code) |
| C-R3-H2 (flush serialises whole memtable to `Vec<u8>` before I/O) | CLOSED — streaming flush verified at `flush.rs:116-166` |
| C-R3-H3 (`SstWriterImpl::add` per-row `key.to_vec()` x3) | CLOSED — `Bytes` trackers + conditional update verified at `writer.rs:240-259`; compile-time-pinned by test |

---

## Bottom line

- 5/11 items code-verified on disk; all 5 PASS.
- 6/11 items (Java side) un-verifiable — same OOT constraint as R2/R3.
- 0 NEW HIGH zero-copy regressions across the 27 landed PRs.
- 1 partial-close (C-R3-H1 input materialisation) remains, **documented and not a regression**.

**End of Round 4 — Agent C**
