# Remote-Linux NexMark reproduction — the SINGLE UNIFORM config (PMC-1, 2026-06-17)

This is the authoritative runbook to reproduce the forst-rs NexMark sweep on a
remote x86_64 Linux box, using **ONE uniform config for every query** (per-query
config is forbidden) with matched RocksDB and ForSt baselines for a fair A/B.

The harness is the same portable Docker driver used on the macOS dev box; it
detects the OS (`uname -s`) and every machine-specific value is env-overridable.
On the remote box the only thing you set is `REPO`/`WORKENV` (or accept the
`/ssd2/$USER/ForSt` + `$HOME/workenv` defaults).

---

## 0. TL;DR

```bash
# On the remote Linux box, in the checkout:
export REPO=/ssd2/$USER/ForSt WORKENV=$HOME/workenv      # or accept defaults
bash tools/nexmark-local/scripts/run-8c32g.sh build      # build the Linux .so (once)
bash tools/nexmark-local/scripts/run-8c32g.sh jar        # build+deploy the forst-rs jar (once)

# 3-backend fair sweep, UNIFORM config for every query:
QUERIES="q4 q7 q9 q11 q12 q17 q19 q20" \
  ARMS="forst-rs-ffm-local rocksdb forst-local" \
  bash tools/nexmark-local/scripts/pmc1-uniform-sweep.sh

# or one query at a time (forst-rs arm only):
bash tools/nexmark-local/scripts/run-best.sh q9
# or the 3-backend A/B for one query:
bash tools/nexmark-local/scripts/run-best.sh validate q9
```

`run-best.sh print` shows the exact uniform config. There are **no per-query
branches** in `run-best.sh` or `pmc1-uniform-sweep.sh`; both read the single `*`
row of `tools/nexmark-local/configs/best-config.tsv`.

---

## 1. The UNIFORM config (SAME for ALL queries)

### 1a. Topology (every query, every backend)

`TOPO=split` — **2 TaskManagers @ 4c/16g + 1 JobManager @ 2c/4g** (the committed
8c/32g TM-budget split). `parallelism.default: 4`, `numberOfTaskSlots: 4`, and
**`taskmanager.load-balance.mode: SLOTS`** in every backend template.

`SLOTS` is critical: Flink 2.x defaults to `load-balance.mode=NONE`, which packs
a job into the fewest TMs. At parallelism 4 that put all 4 join subtasks onto
`tm1`'s 4 slots, so `tm1` held 100% of the join state (~15.9 GiB) and was
OOM-killed at the 16g/TM cgroup while `tm2` sat idle (<1 GiB) — a *distribution*
bug, not a memory ceiling. `SLOTS` spreads the 4 subtasks 2+2 across both TMs.
Set identically in `config-forst-rs-local.yaml.tpl`, `config-rocksdb.yaml`, and
`config-forst-local.yaml.tpl` (and the S3/bench mirrors) for a fair topology.

### 1b. forst-rs engine env (the single `*` row)

```
FRS_KV_SEPARATION=true        # KV-sep ON uniformly (one S3 format for all queries)
FRS_KV_MIN_BLOB_SIZE=256
FRS_TRIVIAL_MOVE=true
FRS_RS_S2_PINNED=1
FRS_VLOG_COALESCE_DEREF=1     # batched value-log deref for SCAN-shaped joins
FRS_SST_COMPRESSION=lz4       # engine default AND the ForSt match
FRS_MEM_MANAGER=1             # never-OOM controller (un-throttled on a >=40 GiB box)
# FRS_VLOG_POINT_DEREF        LEFT UNSET -> auto-follows KV-sep (db.rs:467)
FRS_VLOG_READER_CACHE_CAP=2048
FRS_VLOG_RESIDENT_BUDGET_MB=256
FRS_KV_ADAPTIVE_PRESSURE=1
```

**The engine auto-selects the read path by query SHAPE under this one config:**

- **Windowed point-RMW (q11/q17/q8/q18):** `FRS_VLOG_POINT_DEREF` auto-follows
  KV-sep (it is left unset; `db.rs:445-470` defaults it to `kv_separation_enabled()`).
  A ≤1-pointer deref then reads *exactly* the accumulator bytes via `get_point`
  instead of a 64 KiB chunk fill — this is what made "KV-sep ON for the windowed
  queries too" perf-clean and removed the old q11/q17 KV-sep-OFF exception.
- **Scan/coalesceable joins (q4/q7/q19/q20):** `FRS_VLOG_COALESCE_DEREF` batches
  the deferred `BlobRef` derefs per segment; multi-pointer groups take
  `get_coalesced`. Both paths come from the *same* env; the engine picks per deref
  by locality. Both are byte-identical to the legacy read path (only the read SIZE
  changes).

`FRS_MEM_MANAGER=1` derives ONE engine-native budget from the cgroup and bounds
blockcache/wbm/shadow/vlog/compaction, arms the compaction-admission semaphore +
the windowed levers + a proactive jemalloc purge. On a box with real headroom
(≥40 GiB) it runs **un-throttled** and is perf-neutral — it only sheds under
genuine cgroup pressure. (The Mac VM's ~30% throttle is a 35 GiB-overcommit
artifact, not present on the remote box.)

### 1c. process.size carve-out (forst-rs)

`config-forst-rs-local.yaml.tpl` sets `taskmanager.memory.process.size: 10240m`
(not 16g). Flink's `process.size` budgets ONLY the JVM; forst-rs's NATIVE
(jemalloc-in-the-`.so`) allocation lives in the 16g/TM cgroup *on top of it*.
Carving the JVM down to 10240m leaves ~6 GiB of cgroup free for the engine
native, so q9 + KV-sep ON fits 16g/TM. Uniform across all forst-rs queries —
lighter queries simply have smaller native peaks, so the headroom is harmless.
Override at run time with `FRS_TM_PROCESS_SIZE` (matched by `FRS_JVM_RESERVED_MB`)
if you change the TM size; the controller auto-scales its budget with the TM.

### 1d. ForSt-base match (fair A/B)

forst-rs matches the ForSt baseline on the shared knobs: `noflush=false`,
`write_buffer_size: 1024mb`, `WriteBufferManager: 4096mb`, lz4. The RocksDB and
ForSt arms ignore all `FRS_*` env and run the SAME 2×4c/16g split + SLOTS +
parallelism 4 (reuse the stable rdb/forst baselines per the V3 rule).

### 1e. Levers EXCLUDED from the uniform default (and why)

These were per-query experimental levers; none is uniformly safe, so none is in
the uniform config:

| Lever | Why excluded |
|---|---|
| `FRS_RS_EXECUTOR=routing-adaptive` (R2a) | Helped q11/q17 historically, but under uniform KV-sep ON + auto point-deref their read path is already recovered. Left at the inline default (no per-query executor switch). |
| `FRS_S2_FANOUT_MIN=8` (R1 adaptive-S2) | Join-only lever, never A/B-confirmed harmless for windowed/source-bound queries. |
| `FRS_RS_PROBE_BLOOM_PRUNE`, `FRS_RS_LEVELED_HOT_CF`, `FRS_PERSISTENT_PROBE_ITER` | Experimental join-read-amp levers; not shown beneficial-or-neutral across all shapes. |
| `FRS_RS_MERGE_RMW` | Windowed-only RMW lever; not validated uniform. |
| per-query `process.size` / topology / KV-sep OFF (the old q9/q11/q17 splits) | FORBIDDEN — every query uses the same split + 10240m carve-out + KV-sep ON. |

The uniform config keeps only the levers proven beneficial-or-neutral across
shapes: KV-sep + coalesce + auto point-deref + the vlog resident bounds + the
never-OOM controller.

---

## 2. Prerequisites on the remote Linux box

- **Docker** (the harness runs each cluster as 3 containers on a per-run network).
- **A bench image** tagged `forst-bench:x86` (override with `IMG=`), containing
  JDK 25 at `/opt/java/openjdk` and **JDK 17** at
  `/usr/lib/jvm/java-17-openjdk-amd64` (RocksDB/ForSt arms use JDK 17; forst-rs
  uses JDK 25). The bundled `libjemalloc-preload.so` at `/usr/local/lib` is
  `LD_PRELOAD`ed for ALL backend TM JVMs (a box property, uniform/fair).
- **Hadoop 3.4.3** unpacked at `$WORKENV/hadoop-3.4.3` (`HADOOP_HOME`) — required
  by the forst-rs JDK 25 UGI path (the HadoopModuleFactory is dropped on JDK 25;
  3.4.3 is the supported version).
- **The prebuilt Flink 2.2.1 dist** at `$WORKENV/flink-2.2.1` (`FLINK_HOME`).
- **The nexmark-flink jar** at
  `$REPO/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink`
  (`NEXMARK_HOME`) — build it from the `nexmark/` submodule.
- **The forst-rs `.so`** at `$REPO/target-linux/release/libforst_rs_ffi.so`
  (built by `run-8c32g.sh build`) and **the forst-rs Flink jar**
  (deployed into `$FLINK/lib` by `run-8c32g.sh jar`).
- **Scratch disk** with headroom: each heavy query writes ~36–45 GB of SST +
  cache + checkpoint-noflush artifacts to the per-cluster scratch. Point
  `FRS_CTMP_BASE` at a big NVMe volume (default `$WORKENV/frs-tmp`). The
  scratch-cleanup trap (below) keeps it bounded.

Mock-S3 only: the perf arm runs on LocalFS. The real S3 endpoint stays OFF.

---

## 3. Build steps

```bash
export REPO=/ssd2/$USER/ForSt WORKENV=$HOME/workenv
# 1) forst-rs Linux .so (re-run on engine changes):
bash tools/nexmark-local/scripts/run-8c32g.sh build
# 2) forst-rs Flink jar (host maven, deployed into $FLINK/lib):
bash tools/nexmark-local/scripts/run-8c32g.sh jar
# 3) nexmark-flink jar (from the submodule), if not already built:
#    (cd nexmark/nexmark-flink && mvn -q -DskipTests package)
```

The `.so` is copied into each container at run time (both `/usr/local/lib` and
`$FLINK/lib`). See the **.so-copy race caveat** in §6.

---

## 4. Run each query — UNIFORM config, every backend

### One query, forst-rs arm:
```bash
bash tools/nexmark-local/scripts/run-best.sh q9      # uniform config; TOPO=split
```

### One query, 3-backend fair A/B (forst-rs uniform vs rocksdb vs forst):
```bash
bash tools/nexmark-local/scripts/run-best.sh validate q9
# baselines ignore FRS_*; SAME 2x4c/16g split + SLOTS + parallelism 4
```

### Lever attribution (uniform config ON vs all-OFF):
```bash
bash tools/nexmark-local/scripts/run-best.sh validate-ab q9
```

### Full 3-backend sweep, recorded incrementally to a TSV:
```bash
QUERIES="q4 q7 q9 q11 q12 q17 q19 q20" \
  ARMS="forst-rs-ffm-local rocksdb forst-local" \
  bash tools/nexmark-local/scripts/pmc1-uniform-sweep.sh
# results -> tools/nexmark-local/pmc1-uniform-results.tsv
```

`MAXSEC` is a wall-clock timeout (default: heavy joins q7/q9/q20 = 2700s, all
others 1500s) — it is NOT a config difference. `EVENTS_NUM=100000000`,
`TPS=10000000` are the standard 100M-event load.

---

## 5. Expected correctness (exact `out_rows`, 100M events)

These are the recorded `out_rows` (the forst-rs arm; a FINISHED run must match):

| query | out_rows | notes |
|---|---|---|
| q4  | 25,843,878 (forst-rs cadence) | retract-changelog cadence differs from RocksDB's 177,629,788 — a documented count-cadence difference, not a defect |
| q7  | 92,000,002 | |
| q9  | 91,813,372 | exact across all 3 backends |
| q11 | 92,000,000 | |
| q12 | 92,000,000 | |
| q17 | 92,000,000 | |
| q19 | 92,000,000 | |
| q20 | 93,201,404 | |
| q8  | ~3,064,4xx | small cross-backend cadence jitter (3,064,481 / 413 / 421) |
| q18 | 92,000,000 | |
| q0/q1/q2/q10/q13/q14 | 100,000,000 | source-bound |
| q3  | 2,201,068 | |
| q15/q16 | 92,000,000 | |

A run is correct iff it FINISHED (a `RESULT: ... FINISHED` line) AND its
`out_rows` matches the table.

---

## 6. Operational caveats

### 6a. The `.so`-copy race (concurrent cluster starts)

Each TM and the JM copy the freshly-built `.so` into **two** places at startup:
`/usr/local/lib/libforst_rs_ffi.so` (per-container, private — this is the
`-Dforstrs.native.libpath` the JVM loads) **and** `$FLINK/lib/libforst_rs_ffi.so`.
The `$FLINK/lib` copy targets the **shared** `$WORKENV/flink-2.2.1/lib` mount,
which is the SAME file for every concurrent cluster. If two clusters start at the
same time they race on that shared write, and a JVM can `dlopen` a half-written
`.so` → `UnsatisfiedLinkError`.

Mitigations:
- Run heavy queries **serially** (the sweep drivers do this), OR
- Give each concurrent cluster its own `$FLINK` (copy the dist per namespace) /
  its own image so `$FLINK/lib` is not shared, OR
- Rely on the per-container `/usr/local/lib` copy (the libpath the JVM actually
  uses) and drop the `$FLINK/lib` copy for your concurrency model.

The private `/usr/local/lib` copy is per-container and never races; only the
shared-`$FLINK/lib` copy does. Document/operate accordingly when running
clusters in parallel.

### 6b. Scratch-cleanup trap (disk-full prevention)

`run-8c32g.sh` (the package copy) installs a `trap _frs_cleanup EXIT INT TERM`
(commit `4adad736a`) that, on ANY exit (success OR crash/OOM-kill/SIGINT), tears
down the cluster's containers + network AND `rm -rf`s **only that run's**
per-cluster scratch (`$CTMP`), never the shared base. Without it, a killed run
left ~36–45 GB behind per query and repeated runs filled the volume (347 GB
observed). Set `FRS_KEEP_SCRATCH=1` to retain `$CTMP` for a post-mortem.
Confirmed present in `tools/nexmark-local/scripts/run-8c32g.sh`.

### 6c. The Mac VM vs the remote box (never-OOM honesty)

On the 37.77 GiB Mac VM, two co-resident 16g TMs overcommit the VM, so q9/q5/q19
can OOM from GLOBAL VM pressure (jemalloc RETAINED rides RSS over the cgroup; the
LIVE set always fits the budget). **On a host whose VM actually fits 2×16g
(≥40 GiB — the remote box), `FRS_MEM_MANAGER` runs un-throttled → never-OOM AND
within-bar.** The Mac VM forces a throttle-vs-OOM tradeoff that the remote box
does not. q5 is the one genuine live-state outlier (its in-flight window
accumulator is real application state ~8 GiB; it needs ≥16g/TM or the deferred
window-pane spill). Full empirical detail:
`docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md` (the PMC-1
"@16g/TM" sections).

---

## 7. Portability

The same package reproduces on macOS arm64 (Apple-Silicon dev box, arm64
containers) and the remote x86_64 Linux box (amd64 containers). OS is detected
via `uname -s`; `REPO`/`WORKENV`/`IMG`/`PLAT`/`NEXMARK_HOME`/`FLINK`/
`FRS_CTMP_BASE` are all env-overridable with per-OS defaults — nothing is
hardcoded to one machine.
