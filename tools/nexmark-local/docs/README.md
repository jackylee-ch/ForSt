# tools/nexmark-local — NexMark reproducibility package

Self-contained assets to reproduce the forst-rs NexMark sweep **and** the LOCAL
S3-simulation disaggregated-state test on one box. Everything here is a copy or
thin wrapper of the canonical assets under `scripts/`, `scripts/templates-linux/`,
and `docs/superpowers/specs/` — kept together so a fresh checkout can run the
local sweep + the S3-sim test without hunting across the tree.

```
tools/nexmark-local/
  scripts/
    run-best.sh                  NEW — per-query BEST-config driver (<query>|sweep|print)
    run-s3sim.sh                 NEW — local S3-simulation driver (smoke | sweep)
    run-8c32g.sh                 copy of scripts/run-8c32g.sh + ONE delta:
                                   forwards FRS_REMOTE_BW_MBPS into the containers
    run-remote-nexmark-v3.sh     copy — uniform-config NexMark sweep wrapper
    pick-disk.sh                 copy — picks one scratch disk for a whole sweep
  configs/
    best-config.tsv              NEW — per-query EMPIRICALLY-BEST config table
    config-forst-rs-s3sim.yaml.tpl   NEW — disagg config: S3 dir + local dir + throttle
    config-forst-rs-local.yaml.tpl   copy — forst-rs uniform config (LocalFS arm)
    config-rocksdb.yaml              copy — RocksDB baseline config
    config-forst.yaml.tpl            copy — ForSt (C++) baseline config
  docs/
    README.md                    this file
    DOCS-INDEX.md                pointers to the design/runbook specs
```

---

## 1. Two run modes — read first

This package supports **two intentionally-distinct** ways to run the sweep:

| Mode | Script | Config policy | Question it answers |
|---|---|---|---|
| **Uniform-config** | `run-remote-nexmark-v3.sh` | SAME config for every query | research: how good is forst-rs under ONE config (matching ForSt)? |
| **Best-per-query** | `run-best.sh` | each query at its EMPIRICALLY-BEST config | reproduction: the best wall forst-rs achieves per query |

**Both are legitimate; they answer different questions — do not conflate them.**

- **Uniform-config (research).** SAME config for ALL queries; per-query tuning is
  out of scope for that mode. The only allowed optimization is *dynamic /
  adaptive engine behavior under one config*. forst-rs matches ForSt:
  `noflush=false`, writebuffer `1G`, WBM `4G`, LZ4. See `docs/DOCS-INDEX.md` →
  remote-nexmark-config-v3.
- **Best-per-query (reproduction).** Per-query config is **INTENTIONAL** here —
  the same-config constraint was **reversed 2026-06-14**. Each query runs with
  the knobs the sweep data shows are best for it (KV-separation ON for the
  write / value-carrying joins, OFF for the read-bound / OOM-prone queries, plus
  the R2a routing-adaptive executor where it helps). This is the
  best-PERFORMANCE reproduction package — see §3b and `configs/best-config.tsv`.

The **8 priority queries**: `q4 q7 q9 q11 q12 q17 q19 q20`.
The **3 backends/arms**: `forst-rs-ffm-local`, `rocksdb`, `forst-local`.

**Mock-S3 only.** The NexMark perf arm runs on LocalFS (or the LOCAL S3
simulation below). The real S3 endpoint is gated OFF for every run in this
package.

---

## 2. Build (once, and on engine changes)

```bash
# forst-rs Linux .so (for the Dockerized 8c/32g harness):
bash tools/nexmark-local/scripts/run-8c32g.sh build

# engine unit/integration tests (no Docker), incl. the S3-sim throttle smoke:
cargo test -p forst-rs-io throttle
cargo test -p forst-rs-engine --test remote_bw_throttle_it
```

(For the full image / .so / jar build sequence on the remote box, follow the
E2E runbook — see `docs/DOCS-INDEX.md`.)

---

## 3. Run the uniform-config sweep (8 queries × 3 backends)

```bash
TOPO=split REPO=/path/to/ForSt WORKENV=~/workenv FLINK=~/workenv/flink-2.2.1 \
  IMG=forst-bench:x86 PLAT=linux/amd64 \
  NEXMARK_HOME=~/workenv/nexmark-flink \
  bash tools/nexmark-local/scripts/run-remote-nexmark-v3.sh
```

Subset overrides: `QUERIES="q4 q9"`, `ARMS="forst-rs-ffm-local rocksdb"`,
`MAXSEC=3600`. The wrapper pins the uniform config, picks ONE scratch disk via
`pick-disk.sh`, and drives `run-8c32g.sh` strictly serially (one cluster at a
time).

---

## 3b. Run the BEST-per-query sweep (best-performance reproduction)

Per-query config is **intentional** here (§1). `run-best.sh` reads each query's
row from `configs/best-config.tsv`, exports the per-query forst-rs knobs, and
drives the unmodified `run-8c32g.sh` (forst-rs arm, `TOPO=split`).

```bash
# build once (and on engine changes), exactly as for the uniform sweep:
bash tools/nexmark-local/scripts/run-8c32g.sh build      # + jar on the box

# show the resolved best config for one query / all queries (no run):
bash tools/nexmark-local/scripts/run-best.sh print q19
bash tools/nexmark-local/scripts/run-best.sh print

# run ONE query at its best config:
REPO=/path/to/ForSt WORKENV=~/workenv FLINK=~/workenv/flink-2.2.1 \
  IMG=forst-bench:x86 PLAT=linux/amd64 NEXMARK_HOME=~/workenv/nexmark-flink \
  bash tools/nexmark-local/scripts/run-best.sh q19

# run the full 8-query best-config sweep (serial):
REPO=/path/to/ForSt WORKENV=~/workenv FLINK=~/workenv/flink-2.2.1 \
  IMG=forst-bench:x86 PLAT=linux/amd64 NEXMARK_HOME=~/workenv/nexmark-flink \
  bash tools/nexmark-local/scripts/run-best.sh sweep

# subset / overrides:
QUERIES="q4 q9" bash tools/nexmark-local/scripts/run-best.sh sweep
MAXSEC=3600 bash tools/nexmark-local/scripts/run-best.sh q7
```

`run-best.sh` only touches the **forst-rs** arm (`ARM=forst-rs-ffm-local`); the
RocksDB / ForSt baselines have their own defaults and run via the uniform sweep
or `run-8c32g.sh run <q> rocksdb|forst-local`.

### Per-query best config + expected walls

8c/32g Mac, TOPO=split (2 TM 4c/16g + 1 JM 2c/4g), @100M, serial. Walls are
forst-rs only. Source: the `V3 FULL 8-QUERY` (flag-ON) vs `V3 FLAG-OFF` same-pass
A/B in `docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md` (plus
the `/tmp/v3-q17` routing-adaptive capture for q17's 83.7s). **Never
cross-compare these Mac numbers with the REMOTE-x86 pins.**

| query | KV-sep | KV_MIN_BLOB | TRIVIAL_MOVE | S2_PINNED | RS_EXECUTOR | wall (s) | status | why |
|-------|--------|-------------|--------------|-----------|-------------|----------|--------|-----|
| q4  | ON  | 256 | on | 1 | (inline) | **311**  | measured | KV-sep ON wins (vs 384 OFF, +23%); beats RDB 1.09×; ForSt DNF |
| q7  | ON  | 256 | on | 1 | (inline) | **695**  | measured | KV-sep ON wins big (vs 937 OFF, +35%); PASS RDB 1.16× |
| q9  | OFF | –   | –  | – | (inline) | **1828** | measured | MUST be OFF on 35g Mac — ON **OOMs/DNFs** (×2). Beats ForSt 2002 by finishing |
| q11 | OFF | –   | –  | – | (inline) | **119**  | measured | KV-sep OFF wins (vs 216 ON, −45%); flips both-FAIL→both-PASS |
| q12 | (def OFF) | – | – | – | (inline) | **41** | measured | source-bound; KV-sep neutral (40.6 ON ≈ 41.6 OFF) |
| q17 | OFF | –   | –  | – | **routing-adaptive** | **83.7** | **needs-confirm** | best = OFF-regime + R2a; plain OFF/inline = **110.7 (measured fallback)**; ON = 150.7 |
| q19 | ON  | 256 | on | 1 | (inline) | **216**  | measured | #1 KV-sep beneficiary (vs 464 OFF, +115%); **BEATS BOTH** (0.82× RDB, 1.18× ForSt) |
| q20 | ON  | 256 | on | 1 | (inline) | **824**  | measured | KV-sep ON wins (vs 956 OFF, +16%); NEAR RDB 1.26× (busy-disk noise); PASS ForSt 1.63× |

(KV-sep ON also implies `FRS_VLOG_COMPRESSION=inherit`; `FRS_SST_COMPRESSION=lz4`
is set for every query — engine default and the fair match vs RocksDB/ForSt.)

**Measured vs needs-confirm.** All cells are MEASURED from the captured runs
**except q17**, which is **NEEDS-CONFIRM**: the 83.7s best comes from a separate
`/tmp/v3-q17` routing-adaptive capture, not the clean serial flag-OFF pass. The
table encodes that best config but flags it — treat 83.7s as an estimate and
re-confirm `KV-sep-OFF + routing-adaptive` on a quiet box; the MEASURED fallback
is plain `KV-sep-OFF / inline` at 110.7s. Two further combos are **unmeasured**
and called out in `best-config.tsv` notes rather than guessed into the table:
q11 with `routing-adaptive` (historically 318.9→135.7s, but the OFF+R2a wall is
unmeasured — the cell keeps the measured plain-OFF 118.8s) and q20 with the R1
adaptive-S2 knob `FRS_S2_FANOUT_MIN` (a candidate deep-probe lever, unmeasured).

**Lever summary (the read-path shape).** KV-separation is not globally good or
bad — interval-join + Top-N + heavy windowed-JOIN (q4/q7/q19/q20) want it **ON**
(write-amp / value-carrying read path); windowed-AGG-drain (q11/q17) wants it
**OFF**; the memory-bound q9 **must** run OFF on a ≤16g/TM box regardless of its
read-path preference (ON OOMs the cgroup). R2a (`routing-adaptive`) helps q17
(and historically q11).

---

## 4. Run the LOCAL S3 simulation (disaggregated state)

### The recipe (2026-06-14 directive)

> No real S3 available. Simulate S3 access — use one directory as the **S3
> directory** and another as the **local directory**; when accessing the S3
> directory, limit bandwidth to **50 Gb/s**.

- **Two distinct directories.** `$S3_DIR` is the remote "S3" leg (SSTs only,
  via OpenDAL `file://`); `$LOCAL_DIR` is the local store (LRU SST cache,
  Flink io.tmp, checkpoints).
- **50 Gb/s throttle.** The engine env knob `FRS_REMOTE_BW_MBPS` (MiB/s) caps
  the remote leg. `6250` MiB/s = 50 Gb/s. `0`/unset = OFF (byte-identical).
- **Remote leg only.** The throttle wraps the object-store leg *beneath* the
  local cache, so cache hits and the local store stay at native speed —
  exactly like a real disaggregated deployment.

### Smoke (fast, no Docker — the validated path)

```bash
bash tools/nexmark-local/scripts/run-s3sim.sh smoke
```

Runs the engine integration smoke `remote_bw_throttle_it`, which asserts:
1. **Correctness** — every key round-trips byte-exactly through a `file://`
   disagg engine with the throttle ON (`FRS_REMOTE_BW_MBPS=6250`).
2. **Throttle active, remote-only** — a `ThrottledFileSystem` over a real local
   "S3" dir paces an 8 MiB read at a 4 MiB/s cap (~2s) while a sibling
   unthrottled local dir serves the same 8 MiB in ~1 ms.

Expected output:
```
[io-seam] remote(4MiB/s) read 8MiB = ~2.0s (... charged=16777216B); local read 8MiB = ~0.001s ...
test result: ok. 1 passed; 0 failed
-- smoke OK: rows correct + throttle active on the remote leg --
```

### Full @100M S3-sim sweep (DEFERRED — next step)

```bash
FRS_REMOTE_BW_MBPS=6250 QUERIES="q7 q9" \
  bash tools/nexmark-local/scripts/run-s3sim.sh sweep
```

This drives the NexMark wall with `config-forst-rs-s3sim.yaml.tpl` and the
throttle forwarded into the containers. **Run it only on a clean Mac or the
remote box** — a heavy NexMark wall must not contend with a perf-sensitive
build. The package's `run-8c32g.sh` copy is the only one wired to forward
`FRS_REMOTE_BW_MBPS` into the TM/JM containers (the canonical
`scripts/run-8c32g.sh` does not).

---

## 5. How the throttle works (implementation)

| Piece | Location |
|---|---|
| Token-bucket rate limiter + FS decorator | `crates/forst-rs-io/src/throttle.rs` (`RateLimiter`, `ThrottledFileSystem`) |
| Env knob | `FRS_REMOTE_BW_MBPS` (MiB/s; `0`/unset = OFF) — `throttle.rs:REMOTE_BW_MBPS_ENV` |
| Wiring (wraps the remote leg) | `crates/forst-rs-engine/src/db.rs` → `wrap_remote_bw_throttle()`, applied in `open_remote_with_default_cf` + the two `open_from_linked_checkpoint_instant_*_remote` restore paths |
| Smoke test | `crates/forst-rs-engine/tests/remote_bw_throttle_it.rs` |

The decorator charges bytes on `read`/`read_at`/`read_ranges`/`append` and
sleeps the caller to hold the configured rate. When `FRS_REMOTE_BW_MBPS` is
unset/0 it returns the inner handles unwrapped, so the FS stack is
byte-identical and there is zero overhead.

---

## 6. Interpreting results

- A NexMark run prints a `RESULT:` line (events/s + wall) per query. The sweep
  wrappers `tee` each run to `/tmp/<tag>-<q>-<cfg>.out` and echo the RESULT line.
- For the S3-sim arm, the throttle's effect shows up as higher read wall on
  cold/remote-bound queries (q7/q9/q19/q20) relative to the unthrottled local
  arm — the disaggregated read-amp cost the cap is meant to model.
- Compare same-config across backends; a forst-rs time *below* RocksDB means
  forst-rs is faster (the perf target).
