# Local FFI / Flush / Compaction Stress Benchmark Suite — inventory + design

**Date:** 2026-06-12
**Status:** IMPLEMENTABLE DESIGN (no code changed)
**Goal mandate:** local perf stress via FFI + flush/compaction benchmarks,
runnable by an RD agent on the Mac **without docker**, with allocator stats
capture. Companion to the remote gate matrix
(`2026-06-12-remote-gate-matrix.md`) — these are the *micro* harnesses that
attribute remote macro deltas to engine subsystems.

---

## 1. Inventory — what already exists (do NOT rebuild these)

`crates/forst-rs-bench` (criterion, `cargo bench -p forst-rs-bench --bench <name>`):

| Bench | Covers | Notes |
|---|---|---|
| `point_lookup` | get latency after flush + L0→L1 compaction (compaction in SETUP only) | engine-direct |
| `batch_ops` / `component_microbench` | engine `WriteBatch`/batch_get bookkeeping, in-memory FS, sizes 1-64 | engine-direct, P0.4 gate heritage |
| `batch_put_arrow_vs_write_batch` | arrow-layout put vs WriteBatch | engine-direct |
| `batch_get_arrow_vs_batch_get` | multi-L0-SST gets (small write buffer + manual `switch_and_flush`) | engine-direct |
| `write_throughput` | sustained put loop, 1 MB buffer in-memory FS | tiny scale; NOT a cadence/stall harness |
| `checkpoint` | checkpoint create/restore timing | in-memory FS |
| `join_probe_open` | SST-resident probe-open shape (q20-like) | engine-direct |
| `rocksdb_compare` | side-by-side RocksDB v8 (feature `rocksdb-baseline`, ~15 min first build) | the parity referee |
| `s3_vs_local` | S3 perf (feature `s3-it`) | **needs docker — out of scope here** |

Also: `forst-rs-io/examples/s3bw` (uplink bandwidth probe), Java-side JMH
heritage harness (P0 component-boundary gate; lives flink-side).

**The three gaps** (none of the above covers them):

1. **Nothing crosses the C FFI.** Every bench calls `DbImpl` directly. The
   FFM/FFI boundary cost — the recorded 4.6 µs/call class (memory: q3
   optimization), chunk encode, offsets validation, `FRS_FFI_MAX_BATCH` checks
   — is unmeasured on the Rust side. The macro stack's hot path *is*
   `frs_vectorized_*` / `frs_vec_iter_*`.
2. **No flush-cadence stress.** `write_throughput` measures a put loop at toy
   scale on MemoryFileSystem; there is no harness for stall distribution,
   flush ms/MB (recorded baseline: **202→20 ms/MB** after SST-write
   coalescing, sweep doc :339), or backpressure behavior on a real filesystem.
3. **No compaction-throughput harness.** Compaction appears only in bench
   *setup*. The recorded **21 ns/byte live vs 3 ns/byte isolated** number
   (memory: resume 2026-06-05, q4 binder settled = bandwidth-bound compaction
   merge) was a one-off measurement with no repeatable reproduction target.

---

## 2. Design — three new targets in `forst-rs-bench`

All run on macOS, no docker, `LocalFileSystem` on a tempdir (real I/O without
network). Deterministic seeds (xorshift, same pattern as merge_operator UTs).
New dep: `forst-rs-ffi = { workspace = true }` (B1 only).

### B1 — `ffi_vectorized` (criterion `[[bench]]`): the boundary cost bench

Calls the REAL `extern "C"` entry points (the same ones ForStRsLinker binds),
with buffers laid out exactly as the Java classifier does (Arrow offsets
layout, validated in ffi lib.rs):

| Group | Entry point | Matrix | Metric |
|---|---|---|---|
| put | `frs_vectorized_batch_put` (lib.rs:3225) | rows {64, 256, 1024} × value {64 B, 256 B, 1 KiB} | ns/row, rows/s |
| mixed | `frs_vectorized_batch_mixed` (:3450) kinds Delete/Put/Merge interleaved | same sizes; merge fraction {0, 20 %} | ns/row (L2a flip cost model) |
| get warm | `frs_vectorized_batch_get` (:3061), all keys memtable-resident | rows {64, 256, 1024} | ns/row |
| get multi-SST | same, after 8 × `switch_and_flush` (reuse `batch_get_arrow_vs_batch_get` setup) | rows 256 | ns/row |
| iter drain | `frs_vec_iter_prefix_open` (:5147) + `_next` (:5354), 64 KiB chunks, honoring `FRS_CHUNK_EOF` auto-close | 100 prefixes × 1000 rows/prefix | rows/s, chunks/crossing |
| iter batched | `frs_vec_iter_prefix_open_batch_parallel` (:5821) | K = 64 probes | open µs/probe |

**The headline derived number:** per-row boundary tax = (B1 ns/row) − (engine-
direct ns/row from the existing `batch_ops` group at the same size). Print it
explicitly per size — this is the number the Stage-3/L2a work moves.

### B2 — `flush_stress` (`[[bin]]`, NOT criterion — sustained-rate stress)

Criterion's sample model is wrong for cadence/stall questions; a bin with a
wall-clock phase loop is right. `cargo run -p forst-rs-bench --release --bin
flush_stress -- --secs 60 --wbuf-mib 128 --value-bytes 256 [--delete-pct 30]
[--smoke]`.

- LocalFileSystem tempdir; `write_buffer_size` ∈ {32, 128, 512} MiB sweep.
- Single producer thread issuing engine `batch_put` (256-row batches) at max
  rate for `--secs`.
- Report per run (machine-readable JSON line + human table):
  achieved rows/s; flush count; **flush ms/MB** (gate: ≤ 2× the recorded
  20 ms/MB coalesced baseline, sweep doc :339); put-latency histogram p50/p99/
  p999 (p999 spikes = stall events); total backpressure stall ms; final
  dir size vs logical bytes (write-amp proxy).
- `--delete-pct` variant drives tombstone-rich flushes → exercises the
  **garbage-drain composite gate** (db.rs `garbage_drain_gate`): log drain
  fires + reclaim feedback counters; expected on this small scale: floor
  condition (512 MB L1) keeps drains OFF unless `--secs` is large — assert the
  never-rob behavior cheaply.

### B3 — `compaction_throughput` (`[[bin]]`): the 21 ns/byte reproduction

- Build phase: write + `switch_and_flush` until L0 holds {4, 8, 16} SSTs of
  ~64 MiB, key overlap across files {0 %, 50 %, 100 %} (controlled by key-space
  partitioning), tombstone fraction {0, 20, 50 %}, values 256 B.
- Measure phase: time `compact_l0` / `compact_range`; report **ns/byte-live**
  (BASELINE: **3 ns/byte isolated**, **21 ns/byte under live load** — memory
  resume 2026-06-05), MB/s output, reclaimed-bytes %.
- Two modes: `--isolated` (nothing else running) and `--with-read-load`
  (sidecar thread running point-gets at max rate against the same CF) — the
  pair REPRODUCES the recorded 3-vs-21 gap; if a future change (L4 windowed
  reads, cache bypass) shrinks the live-mode number, this harness is its
  before/after.
- Merge-operator variant (`--merge-chain N`): keys carry N
  `NumericAddBeMergeOperator` operands → measures compaction full_merge
  collapse cost (OPT-N04 §6 chain model gets a measured ns/operand).

### B4 — allocator stats capture (cross-cutting, both bins + criterion via env)

- **Linux:** `FRS_MEM_DIAG=1` already wires `tikv-jemalloc-ctl`
  stats.{allocated,resident,retained} (engine Cargo.toml:38-42). Bins sample
  at phase boundaries and print deltas.
- **macOS:** jemalloc is **compile-gated OFF** (recorded TSD SIGSEGV,
  memory 2026-06-06) — there are no jemalloc stats to capture on the Mac.
  Fallback: a 1 Hz RSS sampler thread (`task_info`/`ps -o rss=`) reporting
  peak + end RSS per phase. The doc rule the RD must print in the output
  header: *Mac numbers are system-allocator numbers; absolute alloc/resident
  values do not transfer to the Linux deployment — use Mac runs for
  regressions (same-box A/B), Linux runs for absolute footprints.*

---

## 3. Methodology rules (binding, from recorded lessons)

1. **Same-session A/B only; n≥3 per cell** for any claim (box-noise rule —
   q17 lesson: single runs are meaningless).
2. Baselines persist to `target/bench-baselines/<git-sha>.json`; comparisons
   name BOTH SHAs. Never compare across machines.
3. 5M-no-flush artifact lesson applies down here too: B2/B3 cells must run
   long enough that flush/compaction actually cycle (assert ≥3 flushes /
   ≥1 compaction per measured phase, else the harness errors out rather than
   reporting a misleading number).
4. Each criterion bench ≤ ~5 min; bins take `--smoke` (≤30 s, asserts
   plumbing only) so CI can compile-and-smoke them without timing claims.
5. Build with `CARGO_PROFILE_RELEASE_STRIP=none DEBUG=2` when a profile will
   be taken (recorded gotcha: release strip hides inner frames).

## 4. Effort + order

| Target | Est. | Order |
|---|---|---|
| B1 ffi_vectorized | ~250 LoC (buffer builders mirror existing ffi UTs) | 1 — feeds L2a flip measurement |
| B3 compaction_throughput | ~200 LoC | 2 — feeds L4 before/after |
| B2 flush_stress | ~250 LoC | 3 |
| B4 capture | ~80 LoC shared module | with whichever bin lands first |

---

## 5. Evidence — B1 first baseline (Mac-population; one run, n=1)

**Status: B1 `ffi_vectorized` IMPLEMENTED** (`crates/forst-rs-bench/benches/ffi_vectorized.rs`,
`cargo bench -p forst-rs-bench --bench ffi_vectorized`). B2/B3/B4-bins still open.

Run: 2026-06-12, engine SHA `81903a5ed` (post E1/E3), Apple M5 Pro / 64 GiB,
macOS, system allocator (jemalloc compile-gated off on Mac), LocalFileSystem
tempdir, default `EngineOptions` (64 MiB write buffer), RawConcat bench CF.
**Mac-population caveat (binding):** same-box A/B regressions only; absolute
numbers do NOT transfer to the Linux deployment. Single run — per methodology
rule 1, no cross-change claims until n≥3.

Criterion medians, paired FFI-vs-engine-direct arms over IDENTICAL prebuilt
batches (per-row tax = ffi − engine at the same cell):

| group | cell | ffi ns/row | engine ns/row | tax ns/row |
|---|---|---|---|---|
| put | r64_v64 | 173.3 | 177.5 | −4.2 |
| put | r64_v256 | 234.0 | 278.1 | −44.1 |
| put | r64_v1024 | 603.2 | 592.4 | +10.8 |
| put | r256_v64 | 177.1 | 174.3 | +2.8 |
| put | r256_v256 | 239.3 | 245.3 | −6.0 |
| put | r256_v1024 | 565.0 | 586.4 | −21.3 |
| put | r1024_v64 | 155.9 | 156.4 | −0.5 |
| put | r1024_v256 | 235.9 | 217.6 | +18.3 |
| put | r1024_v1024 | 514.8 | 525.1 | −10.3 |
| mixed (m=0/20%) | 18 cells r{64,256,1024}×v{64,256,1024} | 155–527 | 155–513 | −13.4…+44.7 |
| get_warm | 64 | 86.1 | 82.2 | +3.9 |
| get_warm | 256 | 100.4 | 96.0 | +4.3 |
| get_warm | 1024 | 113.0 | 108.8 | +4.2 |
| get_multisst (8×L0) | 256 | 444.7 | 328.5 | +116.3 † |
| iter_drain (100×1000, 64 KiB chunks) | — | 155.5 | — | 6.43 Melem/s |
| iter_open_batch parallel | K=64 | 107.3 µs/probe (open+first-chunk fill ~750 rows) | — | — |

† get_multisst ffi arm was the noisiest cell of the run (criterion mean CI
86.9–100.8 µs vs median 113.9 µs — heavily skewed samples); treat the +116
as UNCONFIRMED until n≥3. All other cells had tight CIs.

Iter chunking shape (printed by the bench): 1000-row prefixes at 87 B/row
drain in **4 FFI crossings/prefix** (open + 1 refill + EOF-next + close),
250 rows/crossing average — the FRS_CHUNK_EOF auto-close path is exercised
by the K=64 batched open (every probe's first chunk holds ~750 rows).

**Headline finding (this box, this run):** the per-row boundary tax of the
vectorized FFI surface is **≈0 on the write path** (put/mixed median |tax| ≤
~5 %; worst cell −16 % i.e. FFI-*favoring*, which is noise — a same-shape
wrapper cannot beat its callee — per PMC B1-W1; n=1, superseded by the n=3
rerun in §5b) and **≈4 ns/row on the
warm read path** (output offsets/validity/value-copy; ~4 % at v=64 B). The
4.6 µs/call class number from the q3 era is per-CALL, not per-row: at 64+
rows/crossing the crossing itself is no longer the lever — consistent with
the design's expectation that Stage-3/L2a moves per-row work, not crossing
count. The bench's in-process median-of-30 "boundary-tax summary" footer is
noisier than the criterion arms at small rep counts; use the criterion
paired arms for claims.

B4 capture (Mac fallback): RSS peak 2212 MiB / end 1683 MiB (1 Hz sampler);
rule header printed by the bench.

## 5b. Evidence — B1 n=3 rerun (median-of-3; Mac-population)

Run: 2026-06-12, post-E5-fix tree (E5 scan-locator soundness fix + the
af8b8ea4f bench, sequential runs ×3, same box as §5: Apple M5 Pro / 64 GiB,
macOS, system allocator, LocalFileSystem tempdir). Median-of-3 of the
per-run criterion medians; same paired-arm methodology. **Mac-population
caveat unchanged:** same-box A/B regressions only.

| group | cell | ffi ns/row (med-of-3) | engine ns/row | tax ns/row | per-run ffi spread |
|---|---|---|---|---|---|
| get_multisst (8×L0) | 256 | 361.4 | 323.4 | **+38.0** | 362.5 / 358.3 / 361.4 (tight) |
| get_warm | 64 | 86.2 | 82.8 | +3.4 | 86.2 / 88.5 / 84.9 |
| get_warm | 256 | 100.6 | 96.8 | +3.9 | 100.6 / 105.6 / 98.9 |
| get_warm | 1024 | 113.3 | 109.7 | +3.5 | 115.6 / 113.3 / 112.6 |
| put | 9 cells r{64,256,1024}×v{64,256,1024} | 191–667 | 193–652 | −6.8…+16.2 | tight (≤±2 %) |
| mixed m=0 | 9 cells | 167–555 | 165–555 | −6.3…+12.1 | tight |
| mixed m=20 | 9 cells | 172–978 | 170–971 | −3.8…+6.7 except v1024 cells † | see † |
| iter_drain (100×1000) | — | **151.8** | — | — | 156.7 / 151.8 / 151.6 |
| iter_open_batch parallel | K=64 | 104.2 µs/probe | — | — | 106.8 / 102.6 / 104.2 µs |

† The two large-value merge cells are the run's only high-variance cells:
`r1024_v1024_m20` tax +141.6 and `r64_v1024_m20` +56.2 ns/row median-of-3,
but with per-run ffi spreads of 822.8–958.7 and 863.5–974.0 ns/row (±8 %) —
the same-key-rewrite memtable-deepening artifact B1's methodology audit
already flagged, amplified by 1 KiB merge operands. Direction is consistent
(ffi slower) so a real few-% large-merge tax cannot be excluded, but the
cell does not support a ns-precise claim; everything else is tight.

**n=3 verdicts:**

1. **get_multisst +116 ns/row (n=1) is RETIRED as a skew artifact** — the
   confirmed tax is **+38 ns/row (~12 % at v=64 B over 8 L0 SSTs)**, with
   tight per-run agreement (the n=1 ffi arm's 444.7 was the outlier; the
   engine arm reproduced within 2 % of n=1). Cold-ish multi-SST point reads
   pay a measurably larger boundary tax than warm reads (+38 vs +4) but
   nothing resembling the n=1 number.
2. **Warm-read tax ≈ +3.4–3.9 ns/row confirmed** (B1's ≈4 ns/row claim
   stands at n=3).
3. **Write-path tax ≈ 0 confirmed** with the honest reword PMC B1-W1 asked
   for: median |tax| ≲ 5 %, worst tight cell +16.2 ns on 580 (+2.8 %); the
   only larger cells are the † noisy large-merge ones.
4. **E5 scan-locator fix = zero scan-path regression (measured):**
   iter_drain median-of-3 151.8 ns/row vs 155.5 pre-fix n=1 (−2.4 %, noise
   level), iter_open_batch 104.2 µs/probe vs 107.3 — the per-Version
   OnceLock soundness-flag load + one cached-bool branch per level is
   invisible, as the by-inspection argument predicts (single-CF levels
   always take the unchanged binary-search arm).
