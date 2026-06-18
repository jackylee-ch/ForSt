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
FRS_TM_JEMALLOC=1             # Linux: eager-decay jemalloc over the TM JVM — the OOM
                              #   amplifier fix (=0 is STRICTLY WORSE; the manager
                              #   force-enables it anyway, but set it explicitly)
FRS_MEM_PURGE_AT=elevated     # proactive build-peak jemalloc purge fire threshold
                              #   (use `high` on a >=40 GiB box with spike headroom)
# FRS_VLOG_POINT_DEREF        LEFT UNSET -> auto-follows KV-sep (db.rs:467)
FRS_VLOG_READER_CACHE_CAP=2048
FRS_VLOG_RESIDENT_BUDGET_MB=256
FRS_KV_ADAPTIVE_PRESSURE=1
```

`FRS_TM_JEMALLOC=1` (Linux only) is the key never-OOM lever for the heavy joins.
With `=0` the JVM's native off-heap (FFM/Panama state buffers + AEC in-flight,
which **double** at parallelism 8) runs on plain **glibc** malloc, which *retains*
freed arenas — a TM then crests the 16g cgroup at the q9 join-build peak and
restart-loops (reproduced at ~59M events). Eager-decay jemalloc *returns* freed
pages, so the JVM-side drops back to its p4 footprint. When `FRS_MEM_MANAGER=1` is
armed, `run-8c32g.sh` **force-enables** eager jemalloc over the TM JVM even if you
set `FRS_TM_JEMALLOC=0` (loud WARN) because the controller's never-OOM guarantee
depends on the JVM actually returning memory; we still set `=1` explicitly so the
intent is visible (commit `e61aac893`). `FRS_TM_JEMALLOC_ALLOW_OFF=1` honours an
explicit OFF (then only the engine's glibc-retention cushion defends — slower,
marginal at p8). macOS ignores this knob (host-allocator TSD-crash caveat).

`FRS_MEM_PURGE_AT=elevated` drives the proactive build-peak jemalloc purge at the
**Elevated** pressure level (≥0.75 ≈ 12.3 GiB of 16) with a 250ms sampler, so the
~5 GiB MADV_FREE/dirty join-build transient is returned to the OS *before* the
sub-second build-peak spike crosses the 16g cgroup cliff (commit `44d3616b0`).
Purge only returns already-freed pages — byte-identical, zero correctness impact.
Use `FRS_MEM_PURGE_AT=high` on an ample (≥40 GiB) box where the spike has headroom
and you prefer fewer purges. Both knobs are armed by `FRS_MEM_MANAGER=1`.

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
# 1) forst-rs Linux .so — engine at origin/forst-rs TIP (re-run on engine changes):
bash tools/nexmark-local/scripts/run-8c32g.sh build
# 2) forst-rs Flink jar (host maven, deployed into $FLINK/lib) — see note below:
bash tools/nexmark-local/scripts/run-8c32g.sh jar
# 3) nexmark-flink jar (from the submodule), if not already built:
#    (cd nexmark/nexmark-flink && mvn -q -DskipTests package)
```

**Build at the engine tip + the R-2 backend jar (REQUIRED).** Step 1 must build
the `.so` from the **origin/forst-rs tip** — it carries the never-OOM levers
(proactive purge `44d3616b0`, glibc-retention reserve `e61aac893`, q20
live-files-hoist `72dae3a02`, q5 write-stall/merge-CF fixes). Step 2 must build and
deploy the **current `flink-statebackend-forst-rs` jar**, which carries the **R-2
ValueState write-back RMW cache** (flink-side commit `01ee09c8ec2`) — this is what
takes q17 from 236.9s to 47.7s (now BEATS RocksDB) and q11 to 211.8s. A stale jar
will reproduce the *old* (pre-R-2) windowed numbers; rebuild the jar whenever you
pull the flink submodule.

The `.so` is copied into each container at run time to **three** paths
(`/usr/local/lib`, `/usr/lib`, `$FLINK/lib`). The `/usr/lib` copy is on the JVM's
`java.library.path` so `System.loadLibrary` resolves it even when the
`-Dforstrs.native.libpath` property is not seen (commit `7faa2874a`). See the
**.so-copy race caveat** in §6.

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
| q4  | 25,843,878 (forst-rs cadence) | retract-changelog cadence differs from RocksDB's 177,629,788 — a documented count-cadence difference, not a defect (forst-rs runs jitter 25.83M–25.85M) |
| q7  | 92,000,002 | |
| q9  | 91,813,372 | exact across all 3 backends |
| q11 | 92,000,000 | |
| q12 | 92,000,000 | |
| q17 | 92,000,000 | |
| q19 | 92,000,000 | |
| q20 | 93,201,404 | |
| q8  | ~3,064,4xx | small cross-backend cadence jitter (3,064,481 / 413 / 421 / 465 / 401) |
| q18 | 92,000,000 | |
| q0/q1/q2/q10/q13/q14 | 100,000,000 | source-bound |
| q3  | 2,201,068 | |
| q15/q16 | 92,000,000 | |

A run is correct iff it FINISHED (a `RESULT: ... FINISHED` line) AND its
`out_rows` matches the table.

### 5b. Per-query verdicts (the recorded uniform-config A/B, latest jar)

The current standing verdicts under the SINGLE uniform config (forst-rs arm vs the
RocksDB baseline, recorded in `tools/nexmark-local/pmc1-uniform-results.tsv`):

| query | forst-rs | rocksdb | verdict |
|---|---|---|---|
| q4  | 400.1s | 503.0s | **BEATS RocksDB** (0.80×); forst-local DNF |
| q7  | 1176.1s | DNF (join-scaling wall) | **BEATS RocksDB** (rocksdb stalled ~50M @ 2.9K/s) |
| q17 | **47.7s** (R-2 jar) | 73.9s | **BEATS RocksDB** (0.65×) — R-2 write-back RMW cache |
| q8  | 47.7s | 44.3s | ForSt-parity (source-rate + RowData-serde ceiling, unwinnable) |
| q12 | 46.6s | 39.6s | ForSt-parity (same source ceiling) |
| q11 | 211.8s | 107.0s | 1.98× — STRUCTURAL (inline scattered point-get, not KV-sep deref) |
| q20 | 1146.5s | 661.7s | 1.73× — STRUCTURAL constant-factor join read-path gap |
| q9  | see §6d | 1057.4s | exact 91,813,372; never-OOM at p4@16g; **p8 fit pending — §6d** |
| q18 | 470.4s | 360.4s | finishes correct |
| q19 | 565.5s | 305.5s | finishes; needs a ≥40 GiB host to run 2×16g un-throttled — §6c |
| q5  | OOM box-limit | — | needs a host that fits 2×16g un-throttled (≥40 GiB) — §6c |

q4/q7/q17 BEAT RocksDB; q8/q12 are at the ForSt/source-rate parity ceiling; q11
(1.98×) and q20 (1.73×) are STRUCTURAL constant-factor gaps; **q9/q18/q19/q5 need a
host that actually fits two un-throttled 16g TMs (≥40 GiB VM)** — on the 37.77 GiB
Mac VM they hit the global-VM-overcommit ceiling (§6c).

---

## 6. Operational caveats

### 6a. The `.so`-copy race (concurrent cluster starts)

Each TM and the JM copy the freshly-built `.so` into **three** places at startup:
`/usr/local/lib/libforst_rs_ffi.so` (per-container, private — this is the
`-Dforstrs.native.libpath` the JVM loads), `/usr/lib/libforst_rs_ffi.so` (commit
`7faa2874a` — `/usr/lib` IS on the JVM's `java.library.path`, so
`System.loadLibrary("forst_rs_ffi")` resolves it when the `-Dforstrs.native.libpath`
property is not seen at runtime; without this the split-TM run hit
`UnsatisfiedLinkError` and restart-looped at `src_out~2000`), **and**
`$FLINK/lib/libforst_rs_ffi.so`.

The `$FLINK/lib` copy targets the **shared** `$WORKENV/flink-2.2.1/lib` mount,
which is the SAME file for every concurrent cluster. If two clusters start at the
same time they race on that shared write, and a JVM can `dlopen` a half-written
`.so` → `UnsatisfiedLinkError`. (The `/usr/local/lib` and `/usr/lib` copies are
per-container and never race.)

Mitigations:
- Run heavy queries **serially** (the sweep drivers do this), OR
- Give each concurrent cluster its own `$FLINK` (copy the dist per namespace) /
  its own image so `$FLINK/lib` is not shared, OR
- Rely on the per-container `/usr/local/lib` copy (the libpath the JVM actually
  uses) and drop the `$FLINK/lib` copy for your concurrency model.

The per-container `/usr/local/lib` and `/usr/lib` copies never race; only the
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

On the 37.77 GiB Mac VM, two co-resident 16g TMs overcommit the VM, so the heavy
state queries (q9 at p8, q5, q19, q18) can OOM from GLOBAL VM pressure (jemalloc
RETAINED rides RSS over the cgroup; the LIVE set always fits the budget). **On a
host whose VM actually fits 2×16g
(≥40 GiB — the remote box), `FRS_MEM_MANAGER` runs un-throttled → never-OOM AND
within-bar.** The Mac VM forces a throttle-vs-OOM tradeoff that the remote box
does not. q5 is the one genuine live-state outlier (its in-flight window
accumulator is real application state ~8 GiB; it needs ≥16g/TM or the deferred
window-pane spill). Full empirical detail:
`docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md` (the PMC-1
"@16g/TM" sections).

### 6d. q9 parallelism guidance (p4 fits 16g; p8 needs more — fit pending)

q9 is **never-OOM at parallelism 4 @ 16g/TM** under the uniform config: it FINISHES
exact (91,813,372) with the full KV-sep stack ON, the never-OOM controller bounding
the engine-native, and eager jemalloc + proactive purge holding the build-peak
transient under the cgroup. The docker driver (`run-best.sh` / `pmc1-uniform-sweep.sh`)
uses parallelism 4 with `taskmanager.load-balance.mode=SLOTS` to spread the 4 join
subtasks 2+2 across both TMs.

**Parallelism 8 (4 join operators per TM) is the open fit.** At p8 the Flink-side
off-heap (FFM/Panama state buffers + AEC in-flight) **doubles** per TM, so a TM
crests the 16g cgroup at the join-build peak even with the engine-native fully
bounded — this is JVM-side, not engine-native, retention. `FRS_TM_JEMALLOC=1` is
**mandatory** on Linux at p8 (it lets the JVM-side actually return freed pages); the
proactive purge + glibc-retention cushion close most of the rest, but at p8 the
JVM-side term is large. Two levers to fit p8 @ 16g:
- run with **>16g/TM** (e.g. 20–24g) if the host has the RAM, OR
- **lower `FRS_TM_PROCESS_SIZE`** (carve the JVM heap down further so the cgroup
  leaves more room for the JVM-side native off-heap; matched by `FRS_JVM_RESERVED_MB`).
The bare-metal Linux-local runner (`run-linux-local-forstrs.sh`) pins p8 uniformly,
so it is the runner where this fit matters.

<!-- TODO(q9-p8-fit, in-flight agent /tmp/frs-q9p8fit): DROP THE EXACT p8 @16g/TM
     never-OOM RECIPE HERE once the q9 p8-fit agent reports — i.e. the precise
     FRS_TM_PROCESS_SIZE / FRS_JVM_RESERVED_MB (or the >16g/TM size) + any
     FRS_MEM_JEMALLOC_OFF_RESERVE_MB value that makes q9 FINISH exact at
     parallelism 8 without OOM. Until then: use p4 @16g (proven), or p8 only on a
     >16g/TM host. This placeholder is the single drop-in line for that result. -->

---

## 7. Portability

The same package reproduces on macOS arm64 (Apple-Silicon dev box, arm64
containers) and the remote x86_64 Linux box (amd64 containers). OS is detected
via `uname -s`; `REPO`/`WORKENV`/`IMG`/`PLAT`/`NEXMARK_HOME`/`FLINK`/
`FRS_CTMP_BASE` are all env-overridable with per-OS defaults — nothing is
hardcoded to one machine.

---

## 8. The bare-metal (non-docker) Linux-local runner — ALSO uniform now

`tools/nexmark-local/scripts/run-linux-local-forstrs.sh` is the standing
**bare-metal / `/ssd2` origin-box** entrypoint (its distinct mechanism vs. the
docker driver above). As of **2026-06-17 (PMC-1)** it is folded onto the SAME
single-uniform-config rule:

- It reads the ONE `*` row from `configs/best-config-linux-local.tsv` (now a 9-col
  table that **mirrors** the docker `best-config.tsv` `*` row exactly:
  `KV_SEPARATION=true KV_MIN_BLOB_SIZE=256 TRIVIAL_MOVE=true RS_S2_PINNED=1
  VLOG_COALESCE_DEREF=1 SST_COMPRESSION=lz4 MEM_MANAGER=1`, with
  `VLOG_POINT_DEREF` left unset to auto-follow KV-sep and the vlog resident bounds
  `READER_CACHE_CAP=2048 / RESIDENT_BUDGET_MB=256 / KV_ADAPTIVE_PRESSURE=1`).
- Its `apply_profile` now applies that ONE row to **every** query (the same
  `apply_uniform` pattern as `run-best.sh`). The old per-query rows (q9 bloom/
  hot-CF, q11/q12/q16/q17/q18 KV-sep OFF, q7/q20 persistent-probe, q3/q8
  adaptive-S2, the `PROFILE_OVERRIDE_ENVS` per-query escape hatch, the dual TSV
  format-detection) are **PURGED**. There are no per-query config branches.
- `run-linux-local-forstrs-one.sh` (the single-query nohup wrapper) just forces
  `QUERIES=<q>` and delegates — it carries no per-query config.

### 8a. Honest exception: bare-metal topology is parallelism 8, not 4

The bare-metal runner keeps its long-standing topology envelope of the **8c/32g TM
budget as 2 TaskManagers × 4c/16g, parallelism 8, 4 task slots per TM**
(`FRS_FLINK_PARALLELISM=8`, `FRS_TM_SLOTS=4`, pinned for all queries). The docker
driver uses **parallelism 4** (slot-skew fixed via
`taskmanager.load-balance.mode=SLOTS`). This is the one value that differs between
the two runners — but it is a **per-runner topology choice applied identically to
EVERY query** within the bare-metal runner, **not** a per-query config difference.
It is preserved so the bare-metal runner's previously-measured numbers stay
self-comparable. The uniform-config rule ("no different config for different
queries") is fully satisfied in both runners.

Per-query values that remain in the bare-metal runner are NOT config:
`maxsec_for`/`MAXSEC` (wall-clock timeout only — the docker `run-best.sh` has the
same per-query timeout) and `source_min_for` (a per-query correctness/completeness
floor for the FINISHED gate — analogous to the per-query `out_rows` table in §5).
q6 stays UNSUPPORTED in both runners (a Flink-SQL availability fact).
