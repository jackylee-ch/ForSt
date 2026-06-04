# C — SST data-block format vNext: raw sorted-KV + restart points (with lazy Arrow)

**Date:** 2026-06-04
**Status:** BUILD (audit done; implementing test-first). Policy-clean (no unsafe, no correctness waiver).
Removes the read-path **decode side (~13%)**. C and A (mmap, 43% read_at copy) are **largely orthogonal**
— C does NOT absorb A's 43%; price A from the post-C measured baseline (§5).

## 1. Why (from the profile)

Heavy-join probes are **point/narrow reads**, but each SST data block is one **Arrow-IPC `RecordBatch`**.
Reading any subset forces a per-block `StreamDecoder` (FlatBuffers schema parse + build all column arrays
+ `validate_offsets_full`) — and the join path then **throws the columnar form away** and re-flattens to
the row-wire FFI buffer (`[u32 klen][u32 vlen][key][value]`). Measured per-block read-path cost:
read_at 43% + crc32c 10% + Arrow array-build 10% + offset-validation 2%. B1 removes crc (10%). C targets
the **decode side specifically** (~13%: array-build + offset-validation + decoder setup) — it is
orthogonal to read_at's 43% copy cost (that's A/mmap's domain; see §5). C does not claim A's 43%.

Crucially, the **engine→Java boundary for joins is already row-wire, not Arrow** (`frsVecIterPrefixOpen`
→ `openVecIterIntoBuf` → `chunkBuf`). So a KV block needs **no Arrow on the hot path** — extract rows →
wire. Arrow is the boundary only for (a) Java→engine writes (`batch_put_arrow`, memtable-side,
unaffected) and (b) a separate Arrow-C-Data-Interface read path (`to_ffi`, batch get / columnar scan).

## 2. Proposed block format (per data block)

```
[block header (16B, unchanged: type, compression, sizes, crc)]
[restart-point count : u32]
[restart offsets     : u32 × R]   // every K-th row (K≈16), offset into the rows region
[rows region]: per row, sorted by (user_key ASC, seq DESC):
    [shared_prefix_len u16][unshared_key_len u16][value_len u32][seq u64][op u8]
    [unshared key bytes][value bytes]      // prefix-compressed key vs previous row (RocksDB-style)
```

- **Intra-block point/narrow seek:** binary-search the restart offsets (decode-free), then linear-scan
  ≤K rows decoding only key prefixes — read only the matching row(s). No whole-block decode, no
  FlatBuffers, no array build, no offset validation.
- **Key prefix compression** shrinks blocks (sorted keys share prefixes) → fewer bytes read.
- Compression (none/lz4/zstd) + crc unchanged (B1's skip flag still applies).

## 3. Consumer plan — AUDIT RESULT (2026-06-04): the SST block's Arrow encoding is 100% reader-internal

**Finding (audited):** the SST-decoded `RecordBatch` is consumed in exactly ONE module — `reader.rs`:
point `get` value-extraction (reader.rs:416/517) and `read_block_at` → `for_each_row_in_batch` scan
(reader.rs:653/694). **It never escapes the reader as a `RecordBatch`.** The Arrow-C-Data-Interface
*exports* (`frs_batch_get_arrow` → `db.batch_get_arrow` db.rs:7205; `frs_prefix_scan_arrow`) build their
OWN output arrays by streaming borrowed **value bytes** into a fresh `BinaryBuilder` via `ValueSink`
(db.rs:7223-7245) — they do NOT slice the SST block. So they are **decoupled** from the block's internal
encoding. The join hot path is row-wire (`frsVecIterPrefixOpen` → `fill_chunk_from_iter` → `chunkBuf`),
also block-format-agnostic.

**Therefore C needs NO lazy-Arrow adapter** (the earlier assumption was over-cautious). C touches only:
- **Read:** `reader.rs` — `get` reads the matched KV row directly; `for_each_row_in_batch` walks KV rows
  instead of Arrow columns (its `RowView` callback signature is unchanged → all callers unaffected).
- **Write (flush):** serialize the vectorized memtable's columns into KV rows (cheap — memtable already
  has key/value arenas + seq/op vecs). Replaces the Arrow-IPC `StreamWriter` in the data-block writer.
- **Compaction:** already iterates rows (`CompactionEntry`); reads KV → merge → writes KV. Natural fit.
- **Arrow exports:** UNCHANGED — they consume `get`/scan value bytes, which C still provides.

This makes C strictly a `reader.rs` + data-block encode/decode change with no downstream Arrow surface —
smaller blast radius than originally scoped.

## 4. Migration

- Bump the block-format version in the SST footer. Reader dispatches: v1 → Arrow-IPC decode (existing),
  v2 → KV decode. Writer emits v2. Old SSTs still readable → no rewrite needed; they convert on
  compaction. No data migration, no downtime.

## 5. Expected payoff — C and A are LARGELY ORTHOGONAL (do not conflate)

- **C cuts the DECODE side (~13%):** Arrow array-build (~10%) + `validate_offsets_full` (~2%) +
  decoder/schema setup (~1%) → replaced by a pointer-walk over KV rows. (crc's ~10% is already gone via
  B1.) Block I/O is unchanged: C still `pread`s the whole block (the block is the I/O unit); the
  restart-point seek avoids *decoding* the whole block, not *reading* it. Prefix compression shrinks
  block bytes somewhat, giving a *minor* secondary read_at reduction — not a substitute for A.
- **A cuts the READ side (43%):** the `vec![0u8;blk]` zero-fill + userspace pread copy/memmove. mmap
  makes the block a slice of the mapped file → no copy, no zero-fill. This is a DIFFERENT mechanism from
  C and is NOT removed by C.
- **⇒ "C absorbs A's 43%" is UNVERIFIED and mechanistically dubious — explicitly rejected here.** C does
  not touch the copy that dominates read_at. Do not let an optimistic absorption claim pre-emptively
  kill A's unsafe debate. **Price A from the post-C *measured* baseline** (re-profile after C lands; read
  the residual read_at %), not from a projection.
- B1 (crc skip) composes with C (still gated by the same flag); B1's win is independent of both.

## 5a. HARD PREREQUISITE before flipping C from env-flag to DEFAULT-on

C has only been profiled on the **narrow-probe** path (q4 interval join). Its effect on **scan-heavy /
non-narrow-probe** paths (full-scan, large-range, compaction-dominated queries) is UNMEASURED. KV decode
is cheaper per-narrow-probe, but for a full sequential block scan the v1 Arrow path builds columns once and
walks them vectorised, whereas v2 reconstructs prefix keys per row — that could be neutral or a regression
on wide scans. So: **before changing the writer default to v2, run a full-spectrum NexMark v1-vs-v2
*performance* comparison (q0–q22, especially scan-heavy queries) and confirm no regression.** Correctness
is already proven (§3, §7); this is purely the perf-coverage gate for default-on. (Tracked as a task.)

## 5b. A (mmap) spike — PRE-COMMITTED decision gate (lock before measuring)

mmap removes the userspace **copy** (`vec![0u8;blk]` zero-fill + pread memcpy), NOT the **page-in** (bytes
still fault from page cache / disk into the process). So the recoverable portion of read_at's 43% is the
copy+zero-fill ONLY, not the whole frame. **Gate:** spike-measure (microbench mirroring q4's scattered
8 KiB block reads) how much of read_at is copy-vs-page-in, and whether page faults dominate. The WARM
(page-cache-resident) regime is the UPPER BOUND on mmap's win (it isolates the copy; cold only adds shared
fault cost, shrinking mmap's *relative* edge). **Take A to the PMC as its own item ONLY if the spike shows
the copy is a large, recoverable fraction in a regime q4 actually hits** — otherwise reject A (the unsafe
fight isn't worth a few percent). Do not implement on the main line regardless; the spike is throwaway.

**SPIKE DONE (2026-06-04, see `2026-06-04-A-mmap-spike-findings.md`):** WARM 8 KiB scattered reads —
current pread-fresh-vec ~865 ns/read, pread-reused ~745, **mmap-no-copy ~4.5**, mmap+copy ~378; zero major
faults (warm). So warm `read_at` ≈ syscall (~375) + copy (~375), and mmap removes BOTH (bigger than the
"copy only" framing). Verdict: A **clears the warm bar → PMC as its own item**, carrying (1) win is
warm-bounded, cold-regime payoff capped by the unremoved page-in fault — UNMEASURED, the real risk;
(2) unsafe-in-`memmap2` policy call; (3) a free policy-clean pre-A win exists — **buffer reuse** removes
the alloc+zero-fill (~85–170 ns/read, the (a)−(b) gap) with no mmap. Did NOT implement A on the main line.

## 6. Risks / effort

- Largest effort of the options (new encode/decode + restart-point seek + lazy-Arrow adapter + footer
  version + dual-read). Correctness-critical (it's the on-disk format) → heavy TDD (round-trip, prefix
  compression edge cases, restart boundaries, MVCC seq/op fidelity, crc, dual-version read).
- Mitigation: land behind a writer flag (v1 default until v2 is proven), so it's reversible; validate
  with the existing SST + engine + q0-q22 correctness suites before flipping the default.

## 7. Sequence
1. **B1** (crc-skip) — LANDED, re-profiled (crc 648→0; floor +6–15%). ✅
2. **C audit** — Arrow-FFI vs row-wire read inventory. ✅ DONE (§3): SST block encoding is reader-internal;
   no Arrow consumer touches it → no lazy-Arrow adapter needed; blast radius = `reader.rs` + block codec.
3. **C build** — ✅ DONE (test-first). New `sst/kv_block.rs` codec (encode RecordBatch→KV, prefix
   compression, restart points, iterate + binary-search seek + point `lookup` + `collect_versions`); 10
   unit tests. Reader dispatches per-block via `DecodedBlock {Arrow|Kv}` on the header `block_type` byte
   (0x01/0x02); `CacheEntry::DecodedKv` added; get/get_versions/scan_borrowed/`for_each_row_in_block` all
   format-agnostic; engine hot prefix-scan switched to `for_each_row_in_block`. Writer emits KV behind
   `FRS_SST_KV_BLOCK_FORMAT` (v1 default) + a per-writer `force_kv_block_format` for tests. **Validated:**
   storage 350, engine 262 + ffi 96 + all integration tests GREEN both default-v1 AND with v2 forced (every
   flush/compaction/MVCC/snapshot/recovery path); plus a dual-version compat test (v1 & v2 SSTs read
   byte-identically via get/get_versions/scan).
   - **Bug caught by the dual-version/v2-forced suite (would have shipped otherwise):** `seek_restart`
     used `key <= target`; with many versions of one user key (all restart keys == target) it overshot to
     the last restart and skipped the newest versions (on-disk order is key ASC, **seq DESC** → newest is
     the first occurrence). Point read returned a stale version after compaction. Fixed to strict `<`
     (last restart with key < target) + a unit regression test. This is exactly why old-format + v2-forced
     correctness testing was required, not just new-format round-trips.
4. **Re-profile (q4, KV on)** — ✅ DONE (`FRS_SST_KV_BLOCK_FORMAT=1` + B1, symbolized `sample` at 121s,
   `/tmp/q4C.sample.txt`). **Decode side = 0 samples** (`create_array`/`create_primitive_array`/
   `validate_offsets_full`/`StreamDecoder`/`RecordBatchDecoder` all GONE) and **crc32c = 0** (B1). The KV
   pointer-walk (`KvBlock::parse_entry`/`decode`) is a small fraction of the old Arrow decode.
   **`read_at` (`LocalFirstSstFile::read_at` → std `FileExt::read_at`) + `_platform_memmove` are now the
   unambiguous #1 cost** — the `vec![0;blk]` zero-fill + pread copy = exactly A's domain, unchanged by C
   (orthogonality confirmed empirically, not just argued). q4 floor (B1 on throughout): baseline 159K → B1
   183K → **B1+C 197K @141s** (+24% over baseline; C adds ~+8% on the floor on top of B1).
5. **A spike** — price mmap from THIS post-C measured baseline. read_at is now provably the dominant
   residual, so A's headline is real here — but its payoff still hinges on the page-cache-hit regime
   (mmap removes the *copy*, not the page-in) and the unsafe lives in `memmap2` (skirts forbid's spirit).
   Spike-measure, then PMC design-doc item. A and C are orthogonal (measured) — C did NOT absorb A's 43%.
   Do not implement on the main line. (NEXT)
