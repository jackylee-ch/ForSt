# Disagg read-I/O pool — combined-stack composition & contention validation (Phase-2)

Date: 2026-06-15
Owner: PMC-2 (Phase-2, Disaggregated State)
Status: VALIDATED (mini-bench, mock-S3 timing model) — fix SHIPPED flag-gated default-OFF

Companion to `2026-06-15-disagg-combined-stack-validation.md`. That doc validates
the KV-sep + coalesce/deref/fanout + write-path (rate-split / async-flush /
byte-budget) + WAL-DELTA link-ckpt stack composing through the real
`DbImpl::open_remote` path. THIS doc closes the one surface that bin does NOT
exercise: the **shared read-I/O pool** carrying the three NEW read-side
latency-hiding levers **concurrently**, and whether they compose cleanly or
contend.

## The three levers under test (all read-I/O-pool consumers)

Each was previously validated only in ISOLATION, against its own private pool:

| lever | flag | pool role |
|---|---|---|
| scan readahead depth-D (+ adaptive) | `FRS_VLOG_SCAN_READAHEAD_DEPTH`, `_ADAPTIVE` | FOREGROUND: launches window `k+1..k+D`'s coalesced derefs while consuming window `k` |
| compaction input reader-warm | `FRS_COMPACT_INPUT_WARM` | BACKGROUND: fires the next drain's K input-reader OPENs fire-and-forget |
| compaction input data-prime | `FRS_COMPACT_INPUT_WARM_DATA` | BACKGROUND: also primes each input's first DATA block |

(The write-side QoS lever `FRS_UPLOAD_FLUSH_RESERVED` lives on a SEPARATE pool —
the upload semaphore, not the read-I/O pool — and was validated by
`upload_flush_qos`. It does not share this contention surface.)

All three read levers submit to the ONE process-global pool
(`prefetch.rs::read_io_pool`): a `Mutex<VecDeque<Job>>` + Condvar served by
`clamp(cores/2, 2, 6)` workers, jobs run to completion, **no priority, no
fairness**. So the composition question is real: a foreground scan window
submitted just behind a burst of background warm-up jobs waits the whole burst
out.

## Method

`crates/forst-rs-bench/src/bin/disagg_readpool_contention.rs` — a pure timing
model (no engine state; correctness-safe) that mirrors the real pool faithfully
(one FIFO `VecDeque`, fixed worker count, run-to-completion) and runs the three
consumers CONCURRENTLY on it:

- **scan** (foreground): depth-D readahead over `Wn` windows; each window one
  modeled coalesced remote read (`RTT + bytes/FRS_REMOTE_BW_MBPS`); records
  per-window JOIN-WAIT latency (the foreground-latency signal).
- **compact-warm** (background): a chain of compaction jobs; before each merge it
  fires the next job's K input opens (+ optional first-data-block primes) onto
  the pool.

It reports (A) isolated per-lever win, (B) combined wall + per-lever marginal,
(C) foreground scan slowdown under FIFO contention vs isolated, and a 2-class
foreground-first fairness pool, and (3) the forst-rs-vs-ForSt-equivalent
headline (ForSt-equiv = 1 read thread, depth-1, serial warm). Each cell is the
mean of 5 runs (2 in `--smoke`). Run:

```
cargo run -p forst-rs-bench --bin disagg_readpool_contention --release            # 256 MiB/s (BOS-class)
FRS_REMOTE_BW_MBPS=6250 cargo run -p forst-rs-bench --bin disagg_readpool_contention --release   # 50 Gb/s online box
```

## Results

Pool width 6, RTT 10 ms, 96 scan windows (window-read ≫ per-window consume ⇒
read-bound, the regime depth pays in), 32 compaction jobs × 4 inputs.

### 1. The levers DO compose — combined wall win

| bandwidth | all-OFF combined | ALL-ON (FIFO) | ALL-ON (2-class fair) |
|---|---|---|---|
| 256 MiB/s | 1920.9 ms | 560.4 ms (3.43×) | 464.9 ms (**4.13×**) |
| 6250 MiB/s | 1630.8 ms | 720.2 ms (2.26×) | 434.2 ms (**3.76×**) |

Isolated scan-readahead win (no background): **4.42–4.46×** (depth-1 → opt
depth, the optimum `ceil(rtt/consume)` clamped to pool width). The combined stack
keeps a large net win at both bandwidths.

### 2. Contention IS present under the plain FIFO — and it is the headline finding

The levers do **NOT compose cleanly on the FIFO pool**. Adding the background
warm + data-prime jobs measurably slows the FOREGROUND scan because background
jobs head-of-line-block foreground scan windows:

| bandwidth | scan isolated | scan under FIFO contention | foreground slowdown |
|---|---|---|---|
| 256 MiB/s | 433.0 ms | 539.2 ms | **1.25×** (join p99 inflated ~7000×) |
| 6250 MiB/s | 372.7 ms | 710.6 ms | **1.91×** (join p99 8.9× inflated) |

The slowdown is **sharper at higher bandwidth**: the warm-up jobs become
RTT-dominated (transfer time → 0), so more of them queue per unit time and the
FIFO interleaves them ahead of foreground windows more often. The combined win
collapses from the would-be ~3.8× toward 2.26× at 6250 MiB/s purely from this
interference. This is exactly the "combined win < sum of isolated wins" the
directive asked to quantify — and it is material.

### 3. The fairness fix removes it — `FRS_RS_READ_POOL_FAIRNESS`

A 2-class **foreground-first** read-I/O pool — foreground reads (scan readahead
windows, scan cold-prime opens) served before background warm-ups, work-
conserving (a worker takes a background job only when foreground is empty) —
removes the regression with no loss to background throughput:

| bandwidth | foreground slowdown FIFO → 2-class | combined wall FIFO → 2-class |
|---|---|---|
| 256 MiB/s | 1.25× → **1.03×** | 560.4 ms → 464.9 ms |
| 6250 MiB/s | 1.91× → **1.13×** | 720.2 ms → 434.2 ms |

The fix is the read-pool analogue of the upload-side `FRS_UPLOAD_FLUSH_RESERVED`
reserved-lane QoS: latency-critical work is never starved by background work that
can be paced.

### 4. forst-rs-vs-ForSt-equivalent headline (Phase-2 goal metric, mini-bench)

ForSt-equivalent disagg read path = a SINGLE read thread, one-window (depth-1)
readahead, serial input warm-up (no concurrent open, no data prime):

| bandwidth | ForSt-equiv | forst-rs combined (fair pool) | ratio |
|---|---|---|---|
| 256 MiB/s | 3700.8 ms | 464.9 ms | **7.96×** |
| 6250 MiB/s | 3346.2 ms | 434.2 ms | **7.71×** |

forst-rs's depth-D pipeline (parallel read threads) + concurrent input warm-up +
data-prime, scheduled fairly, is ~7.7–8.0× the ForSt-equivalent serial
single-read-thread model on this read-bound disagg shape (mini-bench timing
model; the absolute factor is regime-dependent — it is dominated by the
pool-width / pipeline-depth difference, which is the real architectural gap).

## The fix as shipped (engine code)

`crates/forst-rs-storage/src/sst/prefetch.rs`:
- `ReadIoPool` now holds a 2-tuple `(foreground, background)` `VecDeque`; workers
  drain foreground fully before background (work-conserving).
- New `enum ReadJobClass { Foreground, Background }` + `submit(class, job)`.
- New public `submit_read_job_background(job)`; `submit_read_job(job)` stays
  foreground (scan readahead launch); `prime_opens_concurrent` and the
  in-merge window submits are foreground.
- Gated by `FRS_RS_READ_POOL_FAIRNESS=1`, **DEFAULT OFF**. With the flag OFF the
  background queue is never populated (every job is pushed foreground), so the
  pool is byte-AND-schedule-IDENTICAL to the pre-fix single FIFO.

`crates/forst-rs-engine/src/db.rs`: the compaction-input-warm + data-prime
fire-and-forget submit now uses `submit_read_job_background` (the only genuinely
background read-pool consumer — no operator blocks on it).

### Correctness / safety

- Flag default-OFF ⇒ single FIFO, byte-AND-schedule-identical to pre-fix. No data
  path changed; this is a pool SCHEDULING policy only.
- Work-conserving: background warm-ups are deprioritised, never dropped or
  starved — proved by the `read_pool_background_and_foreground_jobs_all_run`
  unit test (every fg + bg job completes exactly once, flag in either state).
- Suites green: `forst-rs-storage` lib 483/0, `forst-rs-engine` lib 433/0
  (incl. the two `compact_input_warm` engine tests under the new background
  submit), 26/0 in `sst::prefetch` (incl. the new fairness test). fmt + clippy +
  `RUSTDOCFLAGS="-D warnings" cargo doc` clean on storage + engine.

## Honest negatives / scope

- This is a TIMING MODEL, not a 100M NEXMark run. It faithfully mirrors the pool
  (FIFO, fixed width, run-to-completion) and the per-job remote service time, but
  not real object-store RTT variance or multi-tenant noise. The contention
  finding is structural (FIFO + a background burst ⇒ head-of-line block) so it
  holds regardless of the exact constants; the absolute slowdown factor is
  regime-dependent.
- The headline ratio is dominated by the pool-width / pipeline-depth difference
  (forst-rs many parallel reads vs ForSt-equiv one), which is the legitimate
  architectural lever, not a tuning artifact. The decisive confirmation remains
  a real-BOS / online-box NEXMark run once Phase-1 is released.
- `FRS_RS_READ_POOL_FAIRNESS` ships default-OFF: the contention only bites when
  the background warm levers (`FRS_COMPACT_INPUT_WARM[_DATA]`) AND a foreground
  scan-readahead-heavy query run together on a bandwidth-rich channel. Enabling
  it is recommended whenever the warm levers are enabled; it is harmless
  otherwise (work-conserving).

## Verdict

The three read-pool latency-hiding levers **compose** (large combined win at both
bandwidths) but do **NOT** compose cleanly on the plain FIFO pool: background
compaction warm-ups head-of-line-block foreground scan windows for a 1.25–1.91×
foreground slowdown, worst on a fast channel. The shipped **foreground-first
2-class read-I/O pool** (`FRS_RS_READ_POOL_FAIRNESS`, default-OFF, byte-identical
OFF) removes the regression (slowdown → 1.03–1.13×) and restores clean
composition (combined 3.76–4.13×), work-conserving, suites green.
