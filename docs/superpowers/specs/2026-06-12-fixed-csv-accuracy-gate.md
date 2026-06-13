# Fixed-CSV Accuracy Gate (local Mac, 1M events) — 2026-06-12

Deterministic NEXMark accuracy verification of the CURRENT tips on a FIXED
1M-event CSV input, forst-rs vs RocksDB, byte-level output comparison.
This re-establishes (locally, dockerized) the remote fixed-CSV harness that
verified the q5 namespace fix (`80015d2cfaa`, 54/54 hash-equal) — that harness
lived only on the remote box; this one is committed in-repo.

## Binding under test

| Component | Version |
| --- | --- |
| flink (forst-rs backend jar) | `72607e11097` (timer index + P0 drain + alloc fixes), rebuilt via `scripts/run-8c32g.sh jar` |
| ForSt engine (.so) | repo tip `698949db5` workspace (crates @ `c921e297b`), rebuilt via `scripts/run-8c32g.sh build` |
| Runtime | forst-bench:arm64 docker, 8c/32g, single container, templates `scripts/templates-linux/` (parallelism 4, ckpt 30 s) |
| RocksDB reference | flink-statebackend-rocksdb-2.2.0, JDK17, same container/limits |

## Harness (committed: `scripts/accuracy-gate/`)

1. **`gen-fixed-csv.sh`** (in-container, run once): seeded nexmark datagen
   (deterministic `SplittableRandom` seeds patched in `nexmark/` generator
   sources) → filesystem CSV via `EXECUTE STATEMENT SET`, parallelism 1,
   streaming mode (nexmark source is unbounded — batch runtime rejects it)
   with 2 s checkpoints + hidden-part-file promotion.
   - **`GEN_TPS=10000` is load-bearing**: datagen event TIME comes from the
     rate (`baseTime + i/rate`). At the perf-default 10M tps, 1M events span
     ~0.1 s of event time → ONE 10 s window → windowed queries degenerate
     (q5 = 5 rows). At 10k tps the dataset spans ~100 s → ~50 hop windows
     (q5 = 54 rows, same profile as the remote baseline dataset).
   - Output: `$WORKENV/frs-tmp/nexmark-fixed-csv-1m/{person,auction,bid}`
     (= container `/tmp/nexmark-fixed-csv-1m`), person 20,000 / auction
     60,000 / bid 920,000 rows. Per-dir sha256 recorded in the run log;
     the dataset is generated ONCE and reused for every run of both backends,
     so input is byte-identical by construction.
2. **`measure-print.sh`** (in-container, per query × config): replays the CSV
   dirs as bounded filesystem sources (same DDL + 4 s watermark as the remote
   harness), swaps the query's `blackhole` sink for the **`print`** connector,
   runs the bounded job to FINISHED, then extracts every changelog row
   (`+I/+U/-U/-D`, optional `N> ` subtask prefix) from the TM `.out` into a
   `.rows` capture file. A run is INVALID (no verdict) on restart, exception
   history, FAILED, or timeout. Print-sink capture sidesteps the
   streaming-file-sink commit problem and preserves retractions — the remote
   harness needed a custom `accuracy-file` connector for this; `print` is
   built-in and equivalent at 1M output volumes.
   - Gotcha fixed here: `queries/qX.sql` files have NO trailing newline;
     sql-client silently drops the final unterminated line (q5's INSERT was
     never submitted). The script appends a newline.
   - Gotcha: the bench image's login profile breaks bash `local var="$1"`
     under `set -u` — scripts avoid `local`.
   - q12 auto-enables MONITOR_MODE: `source.monitor-interval = 1 s` keeps the
     job alive so 10 s wall-clock proctime windows can actually fire (a
     bounded run ends before any window closes → 0 rows on BOTH backends),
     stop = print-row-count plateau (6×5 s stable, ≥60 s) then REST cancel.
3. **`compare-print.py`**: materializes each capture by changelog semantics
   (+I/+U add, −U/−D subtract) into a multiset, compares exactly; verdict
   hash = sha256 of the sorted (count, row) stream. Negative materialized
   counts fail the run. **q12 is PROCTIME-windowed** (wall-clock window
   boundaries, nondeterministic by design) → row-equality is replaced by the
   established invariant: `sum(bid_count) == 920000` on BOTH sides AND
   identical bidder sets.
4. **`run-matrix.sh`** (host driver): docker invocations mirroring
   `run-8c32g.sh` (same mounts, cpus/mem limits, .so deploy), loops
   queries × {forst-rs-ffm-local, rocksdb}, then compares.

### Repro

```bash
scripts/run-8c32g.sh build && scripts/run-8c32g.sh jar   # current tips
bash scripts/accuracy-gate/run-matrix.sh                  # gen (if missing) + q3 q5 q8 q11 q12
# results: target-linux/accuracy-gate-results/<tag>/{*.rows,*.log,verdicts.txt,compare.txt}
QUERIES="q5" FORCE_GEN=1 bash scripts/accuracy-gate/run-matrix.sh   # narrower / regen
```

## Results — 2026-06-12, ALL PASS (5/5)

Dataset: `nexmark-fixed-csv-1m` @ GEN_TPS=10000 (person sha256
`0609b8056f11…`, auction `457f1a576f40…`, bid `5762772b06bc…`; ~100 s
event-time span). Runs: `target-linux/accuracy-gate-results/{gate1,gate1-q12,determinism}`.
All 12 runs reached FINISHED (q12: PLATEAU+cancel) with zero restarts and
zero exception history.

| Query | Verdict | frs rows | rdb rows | diff rows | frs sha256[:16] | rdb sha256[:16] | Note |
| --- | --- | ---: | ---: | ---: | --- | --- | --- |
| q3 | **EQUAL** | 6,658 | 6,658 | 0 | `a3a8b693ddfac8c3` | `a3a8b693ddfac8c3` | materialized hash |
| q5 | **EQUAL** | 56 | 56 | 0 | `05026dee79db127b` | `05026dee79db127b` | materialized hash (remote-baseline profile was 54 — different dataset bytes, same ~50-window shape) |
| q8 | **EQUAL** | 8,379 | 8,379 | 0 | `9442c96b1cfe16c4` | `9442c96b1cfe16c4` | materialized hash |
| q11 | **EQUAL** | 19,919 | 19,919 | 0 | `c2bf43b36f0bb26f` | `c2bf43b36f0bb26f` | materialized hash |
| q12 | **PASS_INVARIANT** | 19,919 | 19,919 | n/a | — | — | proctime: sum(bid_count)=920,000 BOTH sides, identical 19,919-bidder sets, 0 malformed; row hashes differ only in wall-clock window bounds (by design) |

**Determinism control**: q8 re-run (fresh cluster + state dirs, both
backends) reproduced the materialized hash exactly — frs run1 == frs run2 ==
rdb run1 == rdb run2 == `9442c96b1cfe16c4`. The gate's verdicts are
run-to-run stable, not single-run luck.

No DIFF was found, so no row-level investigation was required; on a DIFF the
comparator prints up to 20 over-represented rows per side automatically.

## 100K scale (CI) + GHA wiring — 2026-06-13

Two homes, by faithfulness vs. runner cost:

1. **Engine-level gate (hosted GHA, every push)** —
   `.github/workflows/accuracy-gate.yml` runs
   `crates/forst-rs-bench/tests/accuracy_gate.rs` under the `rocksdb-baseline`
   feature. It replays a DETERMINISTIC fixed-seed, NEXMark-windowed-state-shaped
   workload (per-key aggregation churn + window-expiry tombstones + a
   full-range scan at every window-fire — the q5 hop / q11 session read+write
   path) through BOTH the ForSt-RS engine and the vendored RocksDB baseline on
   byte-identical input, then compares the materialized `(key,value)` state and
   FAILS on any divergence. Default scale = **100K events / 2000 keys / window
   2000** (≈50 window boundaries, the same ~100 s-span profile the CSV harness
   targets). Runs in ~3 s after the (cached) RocksDB build. Verified locally:
   frs_sha == rdb_sha (`0xc114001b61bfd9de`, 1998 keys) at 100K; a deliberate
   `+1` perturbation on the RocksDB arm was caught (DIFF rows printed, job
   failed) — the gate is non-vacuous. Tunable via `FRS_ACC_{EVENTS,KEYS,WINDOW,SEED}`
   or `workflow_dispatch` inputs. This is the smallest faithful variant that
   still catches engine accuracy regressions without a Flink cluster.

2. **Full Flink harness (self-hosted / box, on demand)** — the `scripts/`
   harness now takes `EVENTS_NUM`:
   ```bash
   EVENTS_NUM=100000 bash scripts/accuracy-gate/run-matrix.sh   # 100K CI scale
   EVENTS_NUM=1000000 bash scripts/accuracy-gate/run-matrix.sh  # full (default)
   ```
   `gen-fixed-csv.sh` AUTO-DERIVES `GEN_TPS = EVENTS_NUM / 100` so the event-time
   span stays ~100 s and windows populate at any scale (100K → GEN_TPS=1000 →
   ~50 hop windows; pin `GEN_TPS` to override). CSV dirs are scale-tagged
   (`nexmark-fixed-csv-100k` vs `…-1m`) so datasets never collide, and the q12
   invariant's expected bid count scales with `EVENTS_NUM` (BP=46/50 → 92000 at
   100K). This needs a Flink 2.2.1 dist + Hadoop + sql-gateway + JDK25 cluster,
   so it does NOT run on hosted ubuntu-latest — it is the on-demand faithful
   E2E gate for a self-hosted / box runner.

## Scope and caveats

- This is a CORRECTNESS gate, not perf: 1M events, local Mac docker. Wall
  times here are meaningless.
- Coverage is the windowed/timer-sensitive set most affected by this week's
  timer + alloc changes (q3 join, q5 hop window, q8 window join, q11 session
  window, q12 proctime window). The harness accepts any `QUERIES=` list whose
  sink is blackhole-declared in `queries/*.sql`.
- The capture is the print sink at parallelism 4; arrival order across CSV
  splits is not deterministic, but all event-time queries are order-insensitive
  modulo watermark lateness (sources are time-ordered per split; min-watermark
  aggregation prevents late drops). q12 is handled by invariant instead.
- Blackhole-sink queries ARE capturable with this harness (the sink is
  swapped pre-submit); queries with non-blackhole sinks (q10 file sink, q13
  side input) would need the remote harness's extra rewrites — out of scope
  here.
