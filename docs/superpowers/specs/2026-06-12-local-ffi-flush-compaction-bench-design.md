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
