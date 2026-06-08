# ForSt ↔ forst-rs Architecture Gap-Map — RESULTS (root cause)

**Date:** 2026-06-08
**Method:** in-engine wall-time attribution counters (FRS_PROF_DIAG via FRS_MEM_DIAG), committed
config (noflush=false, 6 GB WBM budget, true backpressure). Target q17 (the 11× merge-agg case).

## ★ ROOT CAUSE: flush throughput is ~100× too slow (merge-operand collapse on flush)

q17 attribution at ~81s wall (source throttled to 40 K/s):
```
wbm_memtable_MB=6144 (pinned at budget)  rss_MB=21838
stall_ms=116408   (cumulative across writer threads — backpressure)
flush_ms=88745  flush_MB=438  flush_cnt=2  flush_ms/MB=202.61
comp_ms=0 comp_cnt=0   (compaction hasn't even started — flush is the wall)
```

**The dominant gap is FLUSH: 202.6 ms/MB.** RocksDB/ForSt flush at ~1–2 ms/MB → forst-rs is
**~100× slower per MB on flush**. Consequence chain (all downstream of slow flush):
1. flush drains only 438 MB while 6 GB accumulates → memtable pins at the budget;
2. true backpressure stalls writers (stall_ms 116 s) — *correctly* bounding memory;
3. source throttles to 40 K/s → q17 takes 817 s vs RocksDB 74.5 s = **11×**.

So the 11× is NOT diffuse here — it is **one dominant architectural defect (flush) with a clear
cause**, exactly what the profiler was built to isolate. The memory model (backpressure) is doing
its job; it just exposes the slow flush instead of OOMing.

## Cause of the slow flush: the SST-WRITER / memtable-iteration path (NOT merge-collapse)

**CORRECTED (verified by reading flush.rs):** `FlushJob::run` does NOT collapse merge operands — it
iterates the frozen memtable and feeds rows straight into `SstWriterImpl::streaming()` via
`add_batch` (keys/values/seqs/ops), no full_merge. So the 202 ms/MB is the **flush write path
itself** — some combination of:
- **Oversized memtables:** with write_buffer_size=1024mb + 6 GB budget, each flushed memtable is
  ~1 GB → each flush is a ~22-44s MONOLITHIC operation; only 2-4 completed in 141s on 2 flush
  threads → flush cannot keep pace. RocksDB uses ~64 MB memtables → small, fast, parallel flushes.
- **SST-writer per-byte cost:** the sharded-memtable sorted merge-iteration + block encode + bloom
  build + lz4 + write, at ~50-200 ms/MB of output, is far above RocksDB's ~1-5 ms/MB.
The original merge-collapse hypothesis is REFUTED for the flush path (verified in code). (Merge
collapse may still cost on the READ/compaction path — separate, to be measured.)

NEXT to pinpoint within flush (sub-instrument or perf flamegraph of FlushJob::run): split
memtable-iteration vs SST-encode/bloom vs I/O, and A/B write_buffer_size 1024mb→128mb (smaller,
parallel flushes). Do this BEFORE the fix spec — do not assume which sub-part dominates.

## Gap-map verdict (ranked, q17)
| dim | gap | measured | contribution |
|-----|-----|----------|--------------|
| 3 | **flush write throughput (SST-writer + oversized memtables)** | **202 ms/MB, ~50-100× RocksDB** | **DOMINANT — the 11×** |
| 1 | backpressure stall | 116-210 s | downstream of slow flush (not a root cause) |
| 4 | compaction | 0 (never reached) | flush is the wall before compaction even runs |
| 2 | read coalescing | n/a for q17 (write-bound) | (matters for q9 join read-amp) |

## Fix module (next spec) — to be confirmed by sub-profiling
Two candidate levers, ranked, **pending the within-flush sub-profile** (do not pick blind):
1. **Right-size memtables (write_buffer_size 1024mb → ~64-128mb, RocksDB-like)** so flushes are
   small, fast, and parallel across the flush pool → flush keeps pace. Likely the biggest, lowest-
   risk win; A/B directly. Pairs with the committed backpressure + budget.
2. **Speed the SST writer** (memtable sorted-iteration + block encode + bloom + lz4) if the
   per-byte cost stays high even at small memtable size → vectorize/batch the hot encode loop.
Verify accuracy (out_rows == RocksDB) + re-measure flush ms/MB (target ≤ a few ms/MB) + q17 wall.
NOTE: merge-operand combine-on-write is NO LONGER the candidate for the FLUSH gap (flush doesn't
collapse merges — verified); reconsider it only if the READ path profile shows merge-chain cost.

## A/B RESULT (2026-06-08): write_buffer_size 1024mb → 128mb on q17
| metric | 1024mb (committed) | 128mb |
|--------|--------------------|-------|
| flush_cnt @~early | 2-4 | **39** |
| flush_ms/MB | 202 | **141** (still ~30 ms/MB-input) |
| stall_ms | 116-210 s | **0** (memtable ~1.2GB ≪ 6GB budget) |
| comp_cnt | 0 (never ran) | **14** (compaction runs) |
| RSS | 21.8 GB | **8 GB** |
| rate | 40-275 K/s | ~142 K/s |

**Verdict (CORRECTED — 128mb is NOT a strict win):** 128mb eliminates the WBM stall + lowers RSS,
but later in the run **compaction OVERWHELMS** — comp thread-time 554s > flush 391s, 79 tiny SSTs,
L0 fills → `l0_stop_trigger` → pipeline PAUSES (rate=0). So memtable size merely TRADES failure
modes: **big memtables → flush-stall; small memtables → compaction-overload.** Neither fixes
throughput.

**★ DEFINITIVE ROOT CAUSE: the SST-WRITER per-byte cost.** Both flush AND compaction write SSTs via
the same `SstWriterImpl` (memtable/merge sorted-iteration + block-encode + bloom + lz4), at ~30
ms/MB-input (~6-15× RocksDB). That single slow component is the wall on BOTH paths — which is why it
looks like "a large architecture problem" and why no config knob (memtable size, budget, threads)
fixes it. **THE Phase-1 throughput module = make the SST writer fast (vectorize the hot encode/
bloom/lz4/iteration loop).** Next: perf flamegraph within FlushJob::run + compact_l0_for_cf to rank
the SST-writer sub-costs, then vectorize. Memtable size becomes a secondary balance (e.g. 256mb)
once the writer is fast. Memory model (done) + SST-writer throughput (the boss) = the multi-module
upgrade; merge-combine and read-amp (q9) are separate, smaller modules for their query families.

## ★ Confirmed: SST writer `add_batch` is a PER-ROW inner loop (the per-key anti-pattern)
`StreamingSstWriter::add_batch` (writer.rs:641) takes Arrow columns but loops `for i in 0..rows {
add_internal(key,value,seq,op) }` (679-690). Per-row `add_internal` (225):
- re-appends each cell into 4 NEW Arrow builders (key/value/seq/op) — a redundant per-row COPY,
  even though the INPUT is already columnar Arrow (zero-copy slices available);
- `Sbbf::hash_key(key)` bloom hash PER KEY (push to key_hashes);
- per-row bound checks; periodic `flush_block` (encode_data_block + lz4).
**FIX MODULE (batch/vectorized SST writer):** build data blocks + bloom directly from the input
Arrow columns in bulk — batch the key-hashing (SIMD), avoid the per-row builder re-append (operate
on Arrow slices / zero-copy), batch the block encode. Same applies to the compaction writer path
(compaction also feeds SSTs via this writer → fixing it speeds flush AND compaction). This is the
goal's end-to-end-vectorization / batch-only mandate applied to the hottest write loop. Verify:
out_rows == RocksDB + flush ms/MB drop (target ≤ a few) + q17 wall. write_buffer_size stays 1024mb
(tuned for other queries — reverted; NOT the lever).

## ★★★ FIX IMPLEMENTED + VERIFIED (2026-06-08): SST-write coalescing → q17 PARITY

Profiler sub-cost split (writer.rs sstbuf/sstenc/sstwrite) proved the SST-writer cost is ~95%
**sink-write I/O** (q17: sstwrite 504s vs encode 17s + buffer 8s), NOT per-key CPU. Cause
(opendal_backend.rs:856): `OpendalWritableFile::append` did `block_on(opendal write)` PER ~8 KiB
block → ~27K block_on round-trips per SST. FIX: a `coalesce: Vec<u8>` buffer in OpendalWritableFile
accumulates streaming block appends and flushes to opendal in 4 MiB chunks (drained in close_writer/
sync + Drop backstop so SSTs never truncate).

**RESULT (8c/32g, noflush=false, 6 GB budget):**
| metric | original | +6GB budget | **+coalescing** | RocksDB |
|--------|----------|-------------|-----------------|---------|
| q17 wall | 817s | 220s | **76.7s** | 74.5s |
| q17 vs RocksDB | 11× | 3× | **1.03× = PARITY ✓** | 1.0 |
| sstwrite_ms | 504s | 504s | **9.9s (~50×)** | — |
| out_rows | — | — | **92M == RocksDB ✓** | 92M |

q17 PASSES the bar (≤1.25×, +2.2s ≤+50s, < ForSt host 157s). Tests: io 209 + storage + engine 277
green (coalescing preserves SST correctness). The fix is in the SHARED SST write path (flush AND
compaction) → expected to lift ALL write-heavy queries. The per-key→batch principle generalized to
I/O (per-block→per-chunk).

## Still to measure (other query shapes)
- q9 (join, Put/MapState — NOT merge): expect flush ms/MB much lower → confirms the gap is
  merge-specific, and the join's gap is read-amp (dim 2) not flush. Run q9 with FRS_PROF_DIAG.
- Compaction ms/MB once flush is fixed and compaction becomes the next wall.
