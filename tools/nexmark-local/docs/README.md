# tools/nexmark-local — NexMark reproducibility package

Self-contained assets to reproduce the forst-rs NexMark sweep **and** the LOCAL
S3-simulation disaggregated-state test on one box. Everything here is a copy or
thin wrapper of the canonical assets under `scripts/`, `scripts/templates-linux/`,
and `docs/superpowers/specs/` — kept together so a fresh checkout can run the
local sweep + the S3-sim test without hunting across the tree.

**Portable across two platforms.** Every script detects the OS (`uname -s`) and
branches only where behavior differs, so the SAME package reproduces the results
on BOTH:

- **macOS** (Apple-Silicon dev box) — arm64 Linux containers under Docker Desktop.
- **the origin Linux box** (x86_64, checkout at `/ssd2/$USER/ForSt`) — amd64
  containers, NVMe scratch disks, `LD_PRELOAD`ed jemalloc, io_uring.

All machine-specific values (paths, image tag, platform, RAM-derived sizing,
jemalloc `.so`, scratch disk, io_uring) are auto-detected with per-OS defaults
and are **fully env-overridable** — nothing is hardcoded to one machine. Jump to
**§7 Reproduce on macOS** or **§8 Reproduce on the origin Linux box**.

```
tools/nexmark-local/
  scripts/
    run-best.sh                  NEW — per-query BEST-config driver (<query>|sweep|print)
    run-s3sim.sh                 NEW — local S3-simulation driver (smoke | sweep)
    run-8c32g.sh                 portable copy of scripts/run-8c32g.sh: forwards
                                   FRS_REMOTE_BW_MBPS + JVM process.size into the
                                   containers, and adds OS-aware defaults
                                   (paths/image/jemalloc/io_uring/RAM sizing)
    run-remote-nexmark-v3.sh     uniform-config NexMark sweep wrapper (portable)
    pick-disk.sh                 portable scratch-disk picker (Linux NVMe %util;
                                   macOS $TMPDIR; FRS_CTMP_BASE override)
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
| **Full-stack-ON validate** | `run-best.sh validate ...` | every built lever ON per query, vs rocksdb + forst | the goal proof: do priority queries beat BOTH backends with the full stack? |

**All three are legitimate; they answer different questions — do not conflate them.**
The validate mode (`run-best.sh validate print|<query>|sweep`, `validate-ab <query>`) and
its A/B + canary protocol are specified in
`docs/superpowers/specs/2026-06-15-levers-on-validation-plan.md`.

- **Uniform-config (research).** SAME config for ALL queries; per-query tuning is
  out of scope for that mode. The only allowed optimization is *dynamic /
  adaptive engine behavior under one config*. forst-rs matches ForSt:
  `noflush=false`, writebuffer `1G`, WBM `4G`, LZ4. See `docs/DOCS-INDEX.md` →
  remote-nexmark-config-v3.
- **Best-per-query (reproduction).** Per-query config is **INTENTIONAL** here —
  the same-config constraint was **reversed 2026-06-14**. Each query runs with
  the knobs the sweep data shows are best for it (KV-separation ON for the
  write / value-carrying / heavy-JOIN queries — q4/q7/q9/q19/q20; OFF only where
  it MEASURABLY hurts — the windowed-AGG-drain q11/q17, and neutral q12; plus
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
the `/tmp/v3-q17` adaptive capture as a non-default A/B candidate). **Never
cross-compare these Mac numbers with the REMOTE-x86 pins.**

| query | KV-sep | KV_MIN_BLOB | TRIVIAL_MOVE | S2_PINNED | RS_EXECUTOR | wall (s) | status | why |
|-------|--------|-------------|--------------|-----------|-------------|----------|--------|-----|
| q4  | ON  | 256 | on | 1 | (inline) | **311**  | measured | KV-sep ON wins (vs 384 OFF, +23%); beats RDB 1.09×; ForSt DNF |
| q7  | ON  | 256 | on | 1 | (inline) | **695**  | measured | KV-sep ON wins big (vs 937 OFF, +35%); PASS RDB 1.16× |
| q9  | ON  | 256 | on | 1 | (inline) | **1463** | measured | KV-sep ON FITS the 2×4c/16g split via the `process.size=10240m` native-headroom carve-out (efdc5997a). FINISHED 1463.3s, **EXACT** out_rows 91,813,372, peak per-TM ~15.2 GiB. (Pre-carve-out at 12288m it OOM'd; OFF/1828s is the fallback for tighter boxes.) See **§3a Q9 + KV-separation**. |
| q11 | OFF | –   | –  | – | (inline) | **119**  | measured | KV-sep OFF wins (vs 216 ON, −45%); flips both-FAIL→both-PASS |
| q12 | (def OFF) | – | – | – | (inline) | **41** | measured | source-bound; KV-sep neutral (40.6 ON ≈ 41.6 OFF) |
| q17 | OFF | –   | –  | – | (inline) | **110.7** | measured | stable measured default; OFF+adaptive had a separate 83.7s capture but is not counted until confirmed on a quiet box |
| q19 | ON  | 256 | on | 1 | (inline) | **216**  | measured | #1 KV-sep beneficiary (vs 464 OFF, +115%); **BEATS BOTH** (0.82× RDB, 1.18× ForSt) |
| q20 | ON  | 256 | on | 1 | (inline) | **824**  | measured | KV-sep ON wins (vs 956 OFF, +16%); NEAR RDB 1.26× (busy-disk noise); PASS ForSt 1.63× |

(KV-sep ON also implies `FRS_VLOG_COMPRESSION=inherit`; `FRS_SST_COMPRESSION=lz4`
is set for every query — engine default and the fair match vs RocksDB/ForSt.)

**Measured vs candidate.** The default table only counts measured cells from the
captured runs. q17 deliberately keeps the measured `KV-sep-OFF / inline` cell at
110.7s; the separate `KV-sep-OFF + adaptive` 83.7s capture remains a targeted A/B
candidate and must be confirmed on a quiet box before it replaces the default.
Two further combos are called out in `best-config.tsv` notes rather than guessed
into the table: q11 with `adaptive` and q20 with the R1 adaptive-S2 knob
`FRS_S2_FANOUT_MIN`.

**Lever summary (the read-path shape).** KV-separation is not globally good or
bad — interval-join + Top-N + heavy windowed-JOIN (q4/q7/q9/q19/q20) want it
**ON** (write-amp / value-carrying read path); windowed-AGG-drain (q11/q17) wants
it **OFF**. The memory-bound **q9** wants KV-sep ON too, and now **fits the same
2×4c/16g split as every other query** via the `process.size=10240m` native-headroom
carve-out (no single TM, no per-query topology — see §3a). R2a (`routing-adaptive`)
helps q17 (and historically q11).

> **Topology directive (2026-06-15): EVERY query runs on the uniform 2×4c/16g
> split (`TOPO=split`). No single-TM topology for any query, q9 included.**
>
> **KV-sep policy (2026-06-15): prefer KV-sep ON wherever it performs better.**
> q4/q7/q9/q19/q20 are ON (q9 flipped this session — it now fits the split, §3a).
> The only OFF queries are where a same-pass A/B MEASURED ON to be *slower*:
> **q11** (ON 215.8 vs OFF 118.8s, +82%) and **q17** (ON 150.7 vs OFF 110.7s,
> +36%) — both windowed-AGG-drain — plus neutral **q12**. ⚠ Those q11/q17 OFF-wins
> predate the current read-path lever stack; **re-measure KV-sep ON for q11/q17 on
> the current tip and flip them if ON is now ≥ OFF** (open action — not yet
> re-measured, so the table keeps the last MEASURED OFF winner).

---

## 3a. Q9 + KV-separation — the 16g/TM fit (READ FIRST for q9)

**TL;DR:** q9 @100M with **KV-separation ON** fits the uniform **2×4c/16g split**
— FINISHED **1463.3s**, **EXACT** `out_rows = 91,813,372`, peak per-TM **~15.2 GiB**
(never hit 16). The single lever that makes it fit is lowering Flink's
`taskmanager.memory.process.size` **12288m → 10240m** (commit `efdc5997a`, already
in `scripts/templates-linux/config-forst-rs-local.yaml.tpl`).

**Command (forst-rs arm only, the winning launcher):**

```bash
# from a checkout/worktree root; REPO auto-detected, override for the box.
bash tools/nexmark-local/scripts/q9-procsize.sh q9fit 10240m 2700
# or via the per-query driver (KV-sep ON + the join stack at the split):
bash tools/nexmark-local/scripts/run-best.sh q9-split
# or the full 3-backend beat-both validate at the split:
bash tools/nexmark-local/scripts/run-best.sh validate q9
```

**Expected result (forst-rs arm):**

| metric | value |
|---|---|
| status | FINISHED |
| `out_rows` | **91,813,372** (exact) |
| `wall_ms` | ~**1,463,338** (≈1463.3s) |
| peak per-TM cgroup RSS | ~**15.2 GiB** (oscillates 12–15.2, never 16) |
| topology | `TOPO=split` (2 TM × 4c/16g + 1 JM 4g) |

**Root cause (why the carve-out is needed).** Flink's
`taskmanager.memory.process.size` budgets **only the JVM** (heap + managed +
network + overhead). The forst-rs engine allocates its state, block cache,
memtables and compaction buffers in **native memory** through the `.so`
(jemalloc) — Flink does **not** account for those bytes; they live in the cgroup
**on top of** `process.size`. At `process.size=12288m`, JVM (~12 GiB) + engine
native (~5–6 GiB at the q9 join peak) ≈ 17–18 GiB > the 16g/TM cgroup → an
end-of-run OOM-kill (q9 died ~0.4M rows short). Lowering `process.size` to
`10240m` carves ~2 GiB of the cgroup back for the engine native, leaving ~6 GiB
headroom; the JVM still gets ~4.25 GiB heap + ~3.5 GiB managed (forst-rs keeps
state in engine-native memory, not Flink managed, so managed is ample). This is a
**pure budget re-partition — no RAM added** — and it is **uniform across all
forst-rs queries** (the others have smaller native peaks, so the extra headroom
is harmless). It is set in the template, so every query already gets it.

The KV-sep resident vlog readers are additionally **bounded** (count cap 2048 +
`FRS_VLOG_RESIDENT_BUDGET_MB=256` byte budget + `FRS_KV_ADAPTIVE_PRESSURE=1`
back-off) so resident vlog bytes stay inside that ~6 GiB headroom regardless of
q9's scattered-death segment pattern.

**Do NOT do (refuted levers, kept as repro scripts).**

- **Aggressive jemalloc decay** (`_RJEM_MALLOC_CONF=dirty_decay_ms:1000,muzzy_decay_ms:0`,
  via `q9-decay.sh`): returns freed pages faster but **speeds ingestion past
  compaction** → more uncompacted state → OOMs **earlier** (~59.5M). The 10s
  default's slower re-faulting actually paces ingestion. **Worse, not better.**
- **Fewer compaction threads** (`FRS_BG_COMPACT_THREADS=1`, via `q9-fit2.sh`): a
  single compaction's working set is already ~5 GiB (the spike is working-set,
  not concurrency), and fewer threads → L0 buildup → **higher** base. **Worse.**

**Env note.** The macOS Docker Desktop VM is ~35 GiB total, so the 2×16g split +
4g JM (≈36 GiB) **barely** fits — the carve-out fit was validated under exactly
that constraint. The remote x86_64 Linux box has more headroom, so the same split
config has more slack there. (The retired 8c/36g single-TM q9 profile required a
≥40 GiB Docker VM and is no longer used — every query is split-only.)

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

---

## 7. Reproduce on macOS (dev box)

macOS is the development/UT/micro-bench box. The arm64 Linux container runs under
Docker Desktop. Defaults are auto-detected; you normally only set `REPO` if your
checkout is not at the documented path.

**Platform behavior (auto):** `IMG=forst-bench:arm64`, `PLAT=linux/arm64`, JDK17
path `…-arm64`; jemalloc `LD_PRELOAD` defaults **OFF** (the known macOS jemalloc
TSD crash — see `MEMORY.md`); io_uring/seccomp defaults **OFF** (the engine falls
back to blocking I/O inside the Docker-Desktop VM); scratch base defaults under
`$TMPDIR`; physical RAM auto-detected via `sysctl hw.memsize`.

```bash
# 0) build the forst-rs Linux .so (once + on engine changes) and the jar:
bash tools/nexmark-local/scripts/run-8c32g.sh build
bash tools/nexmark-local/scripts/run-8c32g.sh jar       # host maven, JAVA_HOME auto

# 1) show / run the per-query BEST config (forst-rs arm):
bash tools/nexmark-local/scripts/run-best.sh print            # all 8
bash tools/nexmark-local/scripts/run-best.sh q19              # one query
bash tools/nexmark-local/scripts/run-best.sh sweep           # all 8, serial

# 2) q9 + KV-sep ON at the uniform 2×4c/16g split (§3a; process.size=10240m carve-out):
bash tools/nexmark-local/scripts/run-best.sh q9-split
#    (or the winning launcher directly:)
bash tools/nexmark-local/scripts/q9-procsize.sh q9fit 10240m 2700

# 3) the uniform-config research sweep (8 queries x 3 backends):
bash tools/nexmark-local/scripts/run-remote-nexmark-v3.sh

# 4) the LOCAL S3-simulation smoke (no Docker):
bash tools/nexmark-local/scripts/run-s3sim.sh smoke
```

---

## 8. Reproduce on the origin Linux box (x86_64)

The origin box is the perf authority (NexMark = remote only). The checkout lives
at **`/ssd2/$USER/ForSt`** (documented, not hardcoded). Deploy with a plain
`git pull`:

```bash
# deploy: pull the package to the box's checkout (origin/forst-rs):
cd /ssd2/$USER/ForSt && git fetch origin && git checkout forst-rs && git pull
```

**Platform behavior (auto):** `REPO=/ssd2/$USER/ForSt`, `WORKENV=$HOME/workenv`,
`IMG=forst-bench:x86`, `PLAT=linux/amd64`, JDK17 path `…-amd64`; jemalloc
`LD_PRELOAD` defaults **ON** (the box's bundled `libjemalloc-preload.so`;
override the `.so` with `FRS_JEMALLOC_SO=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2`
if the image lacks the bundle); io_uring/seccomp defaults **ON**
(`--security-opt seccomp=unconfined` — **required**, else q7 DNFs); scratch disk
chosen by `pick-disk.sh` across the NVMe mounts (`/ssd2|/ssd1|/tmp` under `$USER`,
least-busy by an `iostat -x` %util sample); physical RAM auto-detected via
`/proc/meminfo`.

```bash
# 0) build the x86 image + .so + jar per the E2E runbook (DOCS-INDEX), then .so:
bash tools/nexmark-local/scripts/run-8c32g.sh build

# 1) per-query BEST config sweep (forst-rs arm):
bash tools/nexmark-local/scripts/run-best.sh sweep

# 2) q9 + KV-sep ON at the uniform 2×4c/16g split (§3a; process.size=10240m carve-out):
bash tools/nexmark-local/scripts/run-best.sh q9-split

# 3) uniform-config research sweep (8 queries x 3 backends, serial):
bash tools/nexmark-local/scripts/run-remote-nexmark-v3.sh
```

If the box's core/RAM counts differ from the 8c/32g budget, size the topology
with the resource knobs below.

---

## 9. Platform env knobs (override any default)

| Knob | macOS default | Linux default | Meaning |
|---|---|---|---|
| `REPO` | `/Users/.../ForSt` | `/ssd2/$USER/ForSt` | engine checkout (absolute host path; the harness uses absolute paths) |
| `WORKENV` | `~/Downloads/workenv` | `$HOME/workenv` | Flink/Hadoop/NexMark workenv root |
| `IMG` / `PLAT` | `forst-bench:arm64` / `linux/arm64` | `forst-bench:x86` / `linux/amd64` | container image + platform |
| `FRS_TM_JEMALLOC` | `0` (OFF) | `1` (ON) | LD_PRELOAD jemalloc into the TM JVM |
| `FRS_JEMALLOC_SO` | (n/a) | `/usr/local/lib/libjemalloc-preload.so` | the preload `.so` (override to the box's libjemalloc.so.2) |
| `FRS_IO_URING` | `0` (no-op) | `1` (seccomp=unconfined) | enable the io_uring path (q7 needs it on Linux) |
| `FRS_CTMP_BASE` | `$TMPDIR/jackylee/...` | `pick-disk.sh` (NVMe) | scratch base; explicit override wins on both |
| `FRS_DISK_CANDIDATES` | `$TMPDIR/jackylee` | `/ssd2 /ssd1 /tmp` under `$USER` | candidate scratch dirs for `pick-disk.sh` |
| `SINGLE_TM_CPUS` / `SINGLE_TM_MEM` | `8` / `32g` | `8` / `32g` | TOPO=single resources — generic harness knob; **no NexMark query uses single-TM** (all run the split, §3a) |
| `SPLIT_TM_CPUS` / `SPLIT_TM_MEM` | `4` / `16g` | `4` / `16g` | TOPO=split per-TM resources (2 TMs) |
| `SPLIT_JM_CPUS` / `SPLIT_JM_MEM` | `2` / `4g` | `2` / `4g` | TOPO=split JM resources |
| `FRS_TM_PROCESS_SIZE` / `FRS_JM_PROCESS_SIZE` | (unset) | (unset) | Flink JVM process.size override (the template default is `10240m` for the q9 native-headroom carve-out, §3a) |
| `FLINK_SRC` / `JAVA25_HOME` | `$REPO/../flink` / java_home | `$REPO/../flink` / `$JAVA_HOME` | `jar` build inputs |

Physical RAM is auto-detected (macOS `sysctl hw.memsize`, Linux `/proc/meminfo`)
and used only to **warn** (not fail) when the requested container memory + ~4 GiB
OS headroom exceeds it. Every NexMark query runs the 2×4c/16g split (≈36 GiB total
with the JM); on the macOS Docker VM (~35 GiB) that barely fits — see §3a for the
q9 native-headroom carve-out that keeps q9+KV-sep inside 16g/TM.
