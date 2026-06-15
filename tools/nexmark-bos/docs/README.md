# tools/nexmark-bos — NexMark-on-BOS (remote disaggregated state)

The **REMOTE/BOS sibling** of `tools/nexmark-local`. It runs the NexMark sweep
with forst-rs state **disaggregated onto BOS** (Baidu Object Storage = the real
remote S3): the engine's OpenDAL remote store points at the LIVE BOS endpoint,
so this exercises the full disaggregated path — remote SST + vlog, link-mode
(WAL-DELTA) checkpoints, instant restore, and non-SST-local routing.

Where `tools/nexmark-local`'s `run-s3sim.sh` stands BOS in with a **second local
directory + a software bandwidth throttle** (`FRS_REMOTE_BW_MBPS`), this package
talks to a **real BOS bucket**. The structure, scripts, and config style
deliberately mirror `tools/nexmark-local` so a reader who knows one knows both.

```
tools/nexmark-bos/
  scripts/
    run-bos.sh        NEW — BOS disagg driver (<query> | sweep | print)
    run-8c32g.sh      copy of the portable runner, patched to forward the REAL
                        BOS S3 creds + the Phase-2 remote disagg knobs into the
                        TM/JM containers (the nexmark-local copy pins creds to `x`)
    pick-disk.sh      copy — portable least-busy scratch-disk picker
  configs/
    config-forst-rs.yaml.tpl   NEW — BOS disagg template (OpenDAL s3:// remote
                                 store at the BOS endpoint; KV-sep + disagg on)
  docs/
    README.md         this file
```

> **Real-BOS perf is GATED** on the ≥50 Gb/s online box. Creating + dry-running
> this package needs **no** runs; firing it at a real bucket is the gated step.
> Until the good-S3 box is online, use the LOCAL S3 simulation
> (`tools/nexmark-local/scripts/run-s3sim.sh`) or model a link with
> `FRS_REMOTE_BW_MBPS`. **The harness is ready to fire.**

---

## 1. Quick start (dry-run — no creds, no runs)

```bash
# resolve + print the BOS config + every optimization knob (no BOS, no Docker):
bash tools/nexmark-bos/scripts/run-bos.sh print
bash tools/nexmark-bos/scripts/run-bos.sh print q9
```

`print` requires **no** creds and starts **no** run — it just shows the resolved
`storage.uri`, the masked creds, and the recommended knob values.

---

## 2. Run NexMark-on-BOS on the origin box

The origin Linux box is the perf authority (NexMark = remote only) and the box
that should have the fast BOS link. The checkout lives at **`/ssd2/$USER/ForSt`**
(documented, not hardcoded).

```bash
# 0) bridge auth + deploy: pull the package to the box checkout (origin/forst-rs):
cd /ssd2/$USER/ForSt && git fetch origin && git checkout forst-rs && git pull

# 1) build the forst-rs Linux .so (once + on engine changes) and the jar:
bash tools/nexmark-bos/scripts/run-8c32g.sh build
bash tools/nexmark-bos/scripts/run-8c32g.sh jar         # host maven, JAVA_HOME auto

# 2) export the BOS bucket + creds (REQUIRED for a real run):
export S3_ENDPOINT=https://s3.bj.bcebos.com    # the BOS S3-compatible endpoint
export S3_BUCKET=my-nexmark-bucket
export S3_ACCESS_KEY=...                        # BOS AK
export S3_SECRET_KEY=...                        # BOS SK
export S3_REGION=bj                             # optional (default us-east-1)
export S3_PREFIX=nexmark-bos                    # optional (default nexmark-bos)

# 3) run ONE priority query, or the full priority sweep, against BOS:
bash tools/nexmark-bos/scripts/run-bos.sh q9
bash tools/nexmark-bos/scripts/run-bos.sh sweep

# subset / overrides:
QUERIES="q7 q9" bash tools/nexmark-bos/scripts/run-bos.sh sweep
MAXSEC=3600     bash tools/nexmark-bos/scripts/run-bos.sh q7
```

**Platform behavior is auto-detected** (shared with `tools/nexmark-local`): on
Linux `REPO=/ssd2/$USER/ForSt`, `IMG=forst-bench:x86`, `PLAT=linux/amd64`,
jemalloc `LD_PRELOAD` **ON**, io_uring/seccomp **ON** (required, else q7 DNFs),
scratch disk chosen by `pick-disk.sh`, physical RAM from `/proc/meminfo`. On
macOS the arm64 container, jemalloc-OFF, io_uring-OFF defaults apply. All values
are env-overridable (see `tools/nexmark-local/docs/README.md` §9 for the full
knob table — the same `REPO/WORKENV/IMG/PLAT/SPLIT_*/SINGLE_*` knobs apply here).

The priority queries are `q4 q7 q9 q11 q12 q17 q19 q20`; the sweep is serial
(one cluster at a time), `TOPO=split` (2 TM 4c/16g + 1 JM 2c/4g) by default.

---

## 3. The recommended BOS disagg config

The forst-rs state backend points its OpenDAL remote store at the BOS bucket
(`config-forst-rs.yaml.tpl`). The shape, distilled:

| Layer | Where it lives | Why |
|---|---|---|
| SSTs (+ vlog blobs) | **BOS** `s3://$S3_BUCKET/$S3_PREFIX/forst-rs-data-$RUN_ID/` | the disaggregated working state |
| LRU SST cache | **local NVMe** `/tmp/flink-forst-rs-cache` (128 GiB) | after first touch, reads hit local disk not BOS |
| MANIFEST/CURRENT/OPTIONS/WAL/journal | **local** (when `FRS_REMOTE_NONSST_LOCAL=1`) | no chatty small-file S3 metadata traffic |
| checkpoint copy | **local** `file:///tmp/nexmark-checkpoints-forstrs` | link-mode links BOS SSTs; ckpt holds only metadata |

YAML highlights (matches the ForSt uniform config so it's a fair comparison):
write buffer `1 GiB` × 4, WBM `4 GiB`, block cache `2 GiB`, LZ4 SST compression,
`incremental: true`, `EXACTLY_ONCE`, checkpoint interval `30 s`, async-state ON.

The remote endpoint/bucket/creds come **entirely from env** (`S3_ENDPOINT`,
`S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `S3_PREFIX`) and are
substituted into the template by `measure-sql.sh`'s `envsubst` — exactly the
same `$S3VARS` set the canonical `forst-rs-ffm-s3` arm uses (VERIFIED against
`scripts/measure-sql.sh:S3VARS`). Nothing is hardcoded.

---

## 4. Optimization knobs (the remote read/write disagg levers)

These are the reason this harness exists. They are **process env vars** read by
the Rust engine (via `getenv`/`std::env::var`), forwarded into the TM/JM
containers by this package's `run-8c32g.sh`, and set to recommended defaults by
`run-bos.sh`. Every one is **env-overridable**. (For an in-process Flink
backend, the engine reads these through the FFI `frs_set_env` bridge —
`crates/forst-rs-ffi/src/lib.rs` — because the JVM's system-property table does
not feed `getenv`.)

| Knob | Default | What it does | When to tune |
|---|---|---|---|
| `FRS_KV_SEPARATION` | `true` | values → vlog blobs (KV separation) | OFF for read-bound / memory-tight queries (q9/q11) where vlog derefs add remote reads |
| `FRS_KV_MIN_BLOB_SIZE` | `256` | min value bytes routed to the vlog | raise if small values cause vlog churn; lower to separate more |
| `FRS_VLOG_COALESCE_DEREF` | `1` | coalesce **N vlog derefs → 1 remote GET** | keep ON for KV-sep value reads — the N→1 remote-GET win; no-op when KV-sep OFF |
| `FRS_CKPT_LINK_MODE` | `1` | **WAL-DELTA / LINK checkpoint**: link BOS SSTs instead of re-uploading | keep ON; the cheap-incremental-ckpt lever over a slow link |
| `FRS_REMOTE_NONSST_LOCAL` | `1` | pin MANIFEST/CURRENT/OPTIONS/WAL/journal **local**; only SSTs go to BOS | keep ON — kills chatty S3 metadata ops; OFF only to A/B the cost |
| `FRS_UPLOAD_RATE_SPLIT` | `1` | QoS-split upload bandwidth so a compaction burst can't starve flush→checkpoint | keep ON on a bandwidth-constrained link; OFF on a fat link |
| `FRS_UPLOAD_COMPACTION_SHARE` | `0.5` | compaction's fraction of the remote write rate | lower (e.g. `0.3`) if checkpoints time out under compaction load |
| `FRS_RESTORE_BG_FILL` | `1` | instant link-restore: paced background warm of adopted physicals | keep ON for fast restore; OFF for pure lazy-warm |
| `FRS_RESTORE_BG_FILL_WORKERS` | `2` | restore read-pool size | raise on a fat link to warm faster |
| `FRS_RESTORE_BG_FILL_PACE_MB` | `64` | restore warm pacing (MiB/s; `0` = unpaced) | raise/`0` on a fat link; lower to protect foreground I/O |
| `FRS_VLOG_RESIDENT_BUDGET_MB` | `512` | resident vlog-reader BYTE budget | raise on a big TM; lower if the cgroup OOMs under KV-sep |
| `FRS_CACHE_SPACE_LIMIT_MB` | `8192` | free-disk floor for the local LRU SST cache | raise on a small scratch disk to evict earlier |
| `FRS_REMOTE_BW_MBPS` | `0` | remote-leg bandwidth cap (MiB/s); **`0` = the LIVE BOS link's real bandwidth** | set a number (e.g. `6250` = 50 Gb/s) ONLY to MODEL a slower link on a fast box |
| `FRS_REMOTE_COMPACTION` | unset | route compaction to the remote store | experimental; off by default |
| `FRS_SST_COMPRESSION` / `FRS_VLOG_COMPRESSION` | `lz4` / `inherit` | SST / vlog compression | LZ4 is the engine default AND the fair match vs RocksDB/ForSt |

**Reading the levers.** On a real BOS link the dominant costs are (a) remote GET
count on the read path and (b) remote upload bandwidth on the write/checkpoint
path. `FRS_VLOG_COALESCE_DEREF` attacks (a); `FRS_CKPT_LINK_MODE` +
`FRS_UPLOAD_RATE_SPLIT` + `FRS_REMOTE_NONSST_LOCAL` attack (b). KV-separation is
the read-path shape lever — ON for interval-join / Top-N / heavy windowed-JOIN
(q4/q7/q19/q20), OFF for windowed-AGG-drain / memory-tight (q9/q11).

---

## 5. Correctness / canary checks (before trusting a wall)

Run these BEFORE believing any BOS wall — fast-but-wrong is worthless:

```bash
# 1) the disagg engine smoke (no Docker, no BOS): rows round-trip byte-exactly
#    through a remote OpenDAL stack with the throttle ON, throttle remote-only:
cargo test -p forst-rs-engine --test remote_bw_throttle_it
cargo test -p forst-rs-io throttle

# 2) the link-mode / instant-restore / disagg-write paths:
cargo test -p forst-rs-engine --test disagg_write_backpressure_it

# 3) dry-run the config-resolution path (no BOS, no run):
bash tools/nexmark-bos/scripts/run-bos.sh print
```

For a NexMark run, the canary is **output correctness vs the RocksDB baseline on
the same seeded datagen** (compare row counts / sampled rows per query), then the
`RESULT:` line (events/s + wall). The runner `tee`s each run to
`/tmp/<tag>-<q>-forst-rs-ffm-s3.out` and echoes the RESULT line. A forst-rs wall
*below* RocksDB means forst-rs is faster (the perf target).

---

## 6. Gating note — real-BOS perf

Real-BOS perf numbers are valid **only on the online box with a ≥50 Gb/s link**
(see `MEMORY.md`: the dev Mac's BOS uplink is ~10 MB/s to BOS-Beijing, so any
write/checkpoint-heavy wall measured there is a dev-machine artifact, not an
engine property). Until that box is online:

- use `tools/nexmark-local/scripts/run-s3sim.sh smoke|sweep` (mock-S3 + the
  `FRS_REMOTE_BW_MBPS` throttle) for the disagg read/write regime, or
- set `FRS_REMOTE_BW_MBPS` here to MODEL a link bandwidth on a fast box.

This package is **script/config/doc only** and is ready to fire at a real bucket
the moment the good-S3 box is available.
