# Round 3 — Agent C — Zero-Copy (NEW findings only)

**Reviewer:** Agent C
**Round:** 3
**Date:** 2026-05-22
**Scope:** WAL, bloom/index loading, compaction read+write, restore, Java Round-2 fix.

---

## Focus-area inventory

- **WAL:** no dedicated `wal*.rs` exists in `crates/forst-rs-engine/` or `crates/forst-rs-storage/`. The engine relies on the imm-queue + checkpoint blob; no streaming WAL path to audit.
- **Bloom/index loading** (`sst/reader.rs:145-178`): `vec![0u8; size]` + `file.read_at(offset, &mut buf)` for footer, bloom, index. Bounded heap-alloc-then-fill; same anti-pattern as C-R2-H2, no new HIGH.
- **Restore** (`engine/src/checkpoint.rs:65-138`): `copy_file` streams via 64 KiB scratch (good); `read_blob` heap-allocates once with hard 100 MiB cap (acceptable). No new HIGH.
- **Java backend Round 2 fix code:** out of tree — same as Round 2, only `nexmark/` Java sources present. Cannot review.

## NEW HIGH findings (count = 3)

| # | Path:line | One-line |
|---|-----------|----------|
| C-R3-H1 | `crates/forst-rs-engine/src/compaction.rs:73-86, 157, 174` | Compaction fully materialises ALL input rows into `Vec<CompactionEntry>` then `writer.finish()` returns whole SST `Vec<u8>` written in one `append` — peak RSS = sum(inputs) + output_SST for every L0→L1 pass |
| C-R3-H2 | `crates/forst-rs-engine/src/flush.rs:133, 150` | `let (bytes, info) = writer.finish()?; writable.append(&bytes)` — frozen memtable serialised entirely into a heap `Vec<u8>` before any I/O; symmetric to C-R3-H1 on flush path, hit every L0 flush |
| C-R3-H3 | `crates/forst-rs-storage/src/sst/writer.rs:175, 178, 188` | `SstWriterImpl::add` does `key.to_vec()` for `last_added_key` on **every** row + min/max key clones — per-row alloc during flush AND compaction, ~1M clones per Q19 flush, compounds C-R2-H3 |

No new findings in WAL (n/a), bloom/index loading, restore, or Java (out of tree).

**End of Round 3 — Agent C**
