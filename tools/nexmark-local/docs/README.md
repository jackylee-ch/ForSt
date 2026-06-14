# tools/nexmark-local — NexMark reproducibility package

Self-contained assets to reproduce the forst-rs NexMark sweep **and** the LOCAL
S3-simulation disaggregated-state test on one box. Everything here is a copy or
thin wrapper of the canonical assets under `scripts/`, `scripts/templates-linux/`,
and `docs/superpowers/specs/` — kept together so a fresh checkout can run the
local sweep + the S3-sim test without hunting across the tree.

```
tools/nexmark-local/
  scripts/
    run-s3sim.sh                 NEW — local S3-simulation driver (smoke | sweep)
    run-8c32g.sh                 copy of scripts/run-8c32g.sh + ONE delta:
                                   forwards FRS_REMOTE_BW_MBPS into the containers
    run-remote-nexmark-v3.sh     copy — uniform-config NexMark sweep wrapper
    pick-disk.sh                 copy — picks one scratch disk for a whole sweep
  configs/
    config-forst-rs-s3sim.yaml.tpl   NEW — disagg config: S3 dir + local dir + throttle
    config-forst-rs-local.yaml.tpl   copy — forst-rs uniform config (LocalFS arm)
    config-rocksdb.yaml              copy — RocksDB baseline config
    config-forst.yaml.tpl            copy — ForSt (C++) baseline config
  docs/
    README.md                    this file
    DOCS-INDEX.md                pointers to the design/runbook specs
```

---

## 1. Uniform-config policy (read first)

**SAME config for ALL queries. Per-query tuning is FORBIDDEN.** The only allowed
optimization is *dynamic / adaptive engine behavior under one config*. The
forst-rs config matches ForSt: `noflush=false`, writebuffer `1G`, WBM `4G`, LZ4
compression. See `docs/DOCS-INDEX.md` → remote-nexmark-config-v3.

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
