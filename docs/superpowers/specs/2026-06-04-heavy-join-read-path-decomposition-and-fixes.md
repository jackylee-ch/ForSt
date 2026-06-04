# Heavy-join (q4/q7/q9) read-path cost decomposition + fix options — PMC design doc

**Date:** 2026-06-04
**Status:** DESIGN / PMC review. Profiling is solid; fixes are gated on policy (unsafe / correctness) or
are architectural — none is a free clean win, so they need an explicit decision.

## 1. Confirmed problem (not a memory problem, not a cache-capacity problem)

Heavy joins (q4 interval-join, q7 global-max join, q9 dedup) decay from a ~500K/s burst to a low,
slowly-declining floor (~100-130K/s local; RocksDB is flat ~237K). Instrumented run (TPS + RSS + swap +
heap/GC + SST-file-count sampled together) shows the decay tracks **SST accumulation** (ssts 30→140),
with **RSS flat ~19-26 GB on a 64 GB box, swap flat, no full GC, caches capped** → memory is NOT the
driver. A 16 GB vs 4 GB decoded-block-cache A/B changed throughput only ~12% → the cache misses are
**inherent, not capacity** (the join streams through state as windows slide, so each block is read ~once).

Mechanism: each narrow probe touches **~9 SST blocks** (fan-out = accumulated L0 + one per level; grows
2→9 as the LSM fills = the "decay"), and for each block forst-rs pays a per-block **read + decode**.
RocksDB has the *same* fan-out but a far cheaper per-block path — that per-block tax × fan-out is the gap.

## 2. CPU decomposition (symbolized `sample`, current config: 1024 MiB memtables, 8 KiB blocks,
compression=none, 4 GB cache). Under `frs_vec_iter_prefix_open` (6550 samples):

| cost | samples | % of hot path | nature |
|---|---|---|---|
| `read_at` (block read from local SST cache: `vec![0u8;blk]` zero-fill + pread copy/memmove) | 2846 | 43% | I/O + copy |
| crc32c checksum over the whole block (every cache-miss read) | 648 | 10% | redundant safety |
| Arrow IPC array build (`create_array`/`create_primitive_array`, key+value+op) | ~625 | 10% | needed |
| `ArrayData::validate_offsets_full` (re-validate binary offsets per decode) | 143 | 2% | redundant safety |
| StreamDecoder setup + per-block schema parse | ~80 | 1% | redundant |

Scan and value-get **share one decode per block** (both go through the cached `read_data_block`:
`SstReaderImpl::get` reader.rs:412, scan `read_block_at`), so there is no double-decode to remove, and
projecting the scan to key-only would either corrupt the shared cache entry or force the get to re-read
(adding to read_at, the #1 cost). The decode's big costs (crc32c, offset-validation) are exactly the
safety re-work done on every miss.

## 3. Fix options (each priced against the profile; tradeoff in bold)

- **A — mmap zero-copy block read.** Kills `read_at`'s copy (2846 / 43%, the #1 cost): a block becomes a
  slice of the mmap'd (immutable) SST cache file; the Arrow `Buffer` wraps it (no `vec![0u8;…]`, no zero-fill,
  no pread copy). **Tradeoff:** needs `memmap2` (safe API, but the unsafe lives in the dep — skirts the spirit
  of `#![forbid(unsafe_code)]`); payoff also depends on the page-cache-hit regime (unmeasured). PMC decision.
- **B1 — crc32c skip on trusted local storage.** Kills 648 / 10%. **Tradeoff:** removes per-read corruption
  detection (forst-rs wrote the file + it's on local disk; but it's a real safety net). Make it config-gated
  (verify on first read / compaction input only). PMC decision (correctness).
- **B2 — offset-validation skip.** Kills 143 / 2%. **Tradeoff:** arrow only exposes this via an `unsafe`
  flag (`with_skip_validation`) → touches the unsafe-policy line. PMC decision.
- **B3 — per-SST schema cache / decoder reuse.** Kills ~80 / 1%. **Clean, safe, no policy line — but tiny.**
  Not worth a standalone change; fold into any larger read-path refactor.
- **B4 — key-only projection / lazy materialization.** Evaluated and REJECTED: scan↔get share one cached
  decode, so projecting corrupts the cache or doubles read_at. No clean win.
- **C — lighter block format (raw sorted-KV + restart points, RocksDB-style), instead of one Arrow-IPC
  RecordBatch per block.** Biggest *non-gated* decode win: intra-block binary search reads only the needed
  rows; decode is a pointer walk (no FlatBuffers, no array build, no offset validation). **Tradeoff:** SST
  format vNext + migration; the flush/compaction/full-scan paths still want Arrow, so the format must serve
  both (or convert at the boundary). Largest effort, largest ceiling. PMC / design.
- **D — fan-out reduction (the multiplier, already proven).** Bigger memtables (256→1024 MiB) measured **~2×**
  (45M vs 23M events @201s, 3× fewer SSTs) — LANDED in the local config. Keep L0 minimal (subcompaction;
  avoid whole-L1 rewrite in `compact_l0_for_cf`). Pure tuning + compaction design, no policy line.

## 4. Recommendation (SPLIT — do NOT bundle A+B1+B2 into one PMC decision)

1. **Keep D** (bigger memtables; landed) — the only proven, policy-clean lever (~2×).
2. **B1 — fast-approved on its own.** Skip crc32c is a certain ~10%, fully reversible (env flag, default
   verify=on), no unsafe, write-time checksums always computed. It does NOT need to wait on A. LANDED +
   gated (`FRS_SST_SKIP_READ_CHECKSUM=1`); re-profiled immediately to measure the floor gain. Decoupled
   from A's unsafe-policy question entirely.
3. **A (mmap) — spike-measure FIRST, then PMC as its own design-doc item.** Its 43% headline hinges on an
   unmeasured page-cache-hit regime and the unsafe lives in a dep (skirts `forbid(unsafe_code)`'s spirit).
   Price it with a throwaway spike before asking the PMC anything. Do NOT implement on the main line yet.
4. **C (block-format vNext) — scope in parallel NOW** (see `2026-06-04-C-kv-block-format-vnext.md`). It's
   policy-clean and removes the **decode side (~13%)** — array-build + offset-validation + decoder setup.
   C and A are **largely orthogonal**: C cuts decode, A cuts the read_at copy (43%). C does NOT absorb A's
   43% (that claim is unverified/mechanistically dubious — it doesn't touch the pread copy). **Price A from
   the post-C measured baseline**, not a projection. Audit done (read inventory): no Arrow consumer.
5. **B2 (offset-validation skip, 2%)** — touches the unsafe flag for only 2%; fold into C, don't ship alone.
6. **B3 alone is not worth shipping** (~1%); fold into C.

## 4b. B1 measured result (re-profiled 2026-06-04, `FRS_SST_SKIP_READ_CHECKSUM=1`)

- **crc32c eliminated from the hot path: 648 → 0 samples** (symbolized `sample` at 122s, `/tmp/q4b1.sample.txt`).
- Floor gain grows as the read mix shifts to cache-miss reads (where crc was paid):
  102s 228K→247K (+8%), 122s 184K→195K (+6%), 142s 159K→**183K (+15%)**. Matches the predicted "certain ~10%".
- Tests: `zerocopy_decode_checksum_gate` (corrupt byte 12 → verify=true rejects, verify=false decodes intact
  payload) + full storage/engine/ffi suites green (341/262/96). No unsafe; write-time checksums always computed;
  reversible by unsetting the env (default verify=on).

## 5. Evidence index
- Instrumented decay run (TPS↔ssts, RSS/swap/GC flat): memory `project_q4_real_bottleneck_join_cpu_2026-06-04` UPDATE-e.
- 16 GB vs 4 GB cache control (≈12%): same.
- Symbolized samples: `/tmp/q4prof2.sample.txt` (current config), earlier `/tmp/q4prof.sample.txt` (Lz4).
- Sweep status (q3/q5/q8 FINISH; q4/q7/q9 stable burst→floor, cap-hit at 180s, zero crashes).
