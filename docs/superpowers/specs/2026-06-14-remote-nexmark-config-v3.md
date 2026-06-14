# Remote NexMark Config v3 — canonical UNIFORM config companion (2026-06-14)

**Scope.** This is the *config companion* to the E2E runbook
(`docs/superpowers/specs/2026-06-13-nexmark-e2e-test-runbook.md`). It does **NOT
supersede** that runbook — the runbook owns the build/fire/record procedure; this
doc owns the **one uniform config** every NexMark run on the remote box must use,
plus the deploy + concurrency etiquette. When the two disagree on a *value*, this
doc is the newer source for the config; for *procedure* defer to the runbook.

It pins the config + flag values used by the companion wrapper
`scripts/run-remote-nexmark-v3.sh` and documents how those values map to the
harness (`scripts/run-8c32g.sh`) and the Flink/engine templates
(`scripts/templates-linux/`). Read it before launching anything on the shared
remote box (`sshdata00` → the `/ssd2/jackylee` checkout).

---

## 0. The ONE policy that overrides everything: uniform config, no per-query tuning

**SAME config for ALL queries. Per-query config is FORBIDDEN.** The 8 priority
queries (q4, q7, q9, q11, q12, q17, q19, q20) and the full q0–q22 set run under
*one* config. The only sanctioned optimization is **dynamic / adaptive engine
behavior under that one config** — i.e. the engine adapts at runtime (adaptive
KV-separation, adaptive S2 pinned-merge, adaptive executor parallelism), never a
human hand-picking flags per query.

Consequence for this doc: there is exactly one column of values below. There is
no "q9 config" vs "q4 config". The wrapper script exports one env set and reuses
it for every query × backend.

---

## 1. Topology — `TOPO=split` (the official remote topology)

The 8c/32g budget is **TaskManager-only**. `run-8c32g.sh` with `TOPO=split`
launches, per cluster:

| Container | cpus | memory | role |
|---|---|---|---|
| `$CLUSTER-tm1` | 4 | 16g (`--memory-swap=16g`) | TaskManager (counts toward budget) |
| `$CLUSTER-tm2` | 4 | 16g (`--memory-swap=16g`) | TaskManager (counts toward budget) |
| `$CLUSTER-jm`  | 2 | 4g  | JobManager + SqlGateway + client (NOT part of the 8c/32g budget) |

So the measured resource = **2 × 4c/16g TM = 8c/32g**. The JM box is overhead and
deliberately outside the envelope. `parallelism.default: 4`,
`numberOfTaskSlots: 4` per TM (8 slots total across 2 TMs).

`TOPO=single` (legacy 1 × 8c/32g) exists but is **not** used here — all remote v3
runs are split.

---

## 2. Backends / arms

Three arms, each its own Flink template (selected by `measure-sql.sh` from the
`CONFIG` value the harness passes):

| Arm (`CONFIG`) | Template | JDK | Storage |
|---|---|---|---|
| `forst-rs-ffm-local` | `config-forst-rs-local.yaml.tpl` | 25 | LocalFileSystem on the NVMe (`file:///tmp/...` → per-cluster scratch) |
| `rocksdb` | `config-rocksdb.yaml` | 17 | local disk |
| `forst-local` | `config-forst-local.yaml.tpl` | 17 | (community ForSt; local) |

**The forst-rs perf arm is `forst-rs-ffm-local`, NOT `forst-rs-ffm-s3`.** See §8
(MOCK S3). The S3 template `config-forst-rs.yaml.tpl` is real-S3 and stays unused
for perf.

---

## 3. Engine / Flink params — verified against the templates (the ForSt-matching values)

These are the values baked into the templates the harness uses. The forst-rs
column is from **`config-forst-rs-local.yaml.tpl`** (the arm actually run);
relevant ForSt-match points are called out.

### 3.1 Memory / process

| param | forst-rs (local) | rocksdb | forst (community) |
|---|---|---|---|
| `taskmanager.memory.process.size` | 12288m | 8192m | 12288m |
| `jobmanager.memory.process.size` | 4096m | 4096m | 4096m |
| `parallelism.default` | 4 | 4 | 4 |
| `taskmanager.numberOfTaskSlots` | 4 | 4 | 4 |

> Note: the TM JVM `process.size` (12288m) is the JVM heap+overhead; forst-rs
> native off-heap (memtables/WBM/block-cache/cache-dir) stacks ON TOP and is
> bounded by the engine params below. The container hard cap is `--memory=16g`
> per TM (§1). RocksDB's 8192m is intentional — its block cache lives in Flink
> managed memory within that budget; forst-rs's caches are native/off-heap, hence
> the larger JVM number against the same 16g container cap. This matches ForSt's
> 12288m so forst-rs and ForSt share the JVM envelope.

### 3.2 forst-rs engine knobs (the ForSt-matching set) — `config-forst-rs-local.yaml.tpl`

| param | value | ForSt-match / note |
|---|---|---|
| `state.backend.forst-rs.writebuffer.size` | **1024mb (= 1G)** | **MATCHES the directive `write_buffer_size=1G`** |
| `...writebuffer.count` | 4 | up to 4 immutable memtables |
| `...writebuffer.manager.capacity` | 4096mb | WBM caps cross-CF memtable sum |
| `...compaction.max-background` | 8 | |
| `...flush.max-background` | 4 | |
| `...cache.block.capacity` | 2048mb | block cache |
| `...storage.cache-dir` | `/tmp/flink-forst-rs-cache` (→ per-cluster scratch) | on-disk SST LRU |
| `...storage.cache-capacity-mb` | 65536 (64 GiB) | local NVMe SST cache |
| `...storage.uri` | `file:///tmp/flink-forst-rs-data/` | **LocalFileSystem (mock-S3 local arm)** |
| timer service | `FORSTRS` (`-Dforst.rs.timer-service.factory=FORSTRS`) | engine timer queue |
| **checkpoint noflush** | **`false`** (`-Dforst.rs.checkpoint.noflush=false`) | **MATCHES the directive `noflush=false`** |
| GC | `-XX:+UseG1GC` (G1, NOT ZGC; NOT CompactObjectHeaders) | validated config |

> The S3 template (`config-forst-rs.yaml.tpl`) uses different numbers
> (writebuffer.size 2048mb, WBM 8gb, cache 131072mb, FORSTRS→HEAP timer) — that
> template is the real-S3 arm and is **out of scope** here. The numbers above are
> the local arm that v3 runs.

### 3.3 Checkpointing / execution (forst-rs local arm)

| param | value |
|---|---|
| `execution.checkpointing.interval` | 30 s |
| `execution.checkpointing.mode` | EXACTLY_ONCE |
| `execution.checkpointing.tolerable-failed-checkpoints` | 1000 |
| `state.checkpoints.dir` | `file:///tmp/nexmark-checkpoints-forst-rs` (per-cluster scratch) |
| `execution.async-state.in-flight-records-limit` | 60000 |
| `execution.async-state.buffer-size` | 16000 |
| `execution.async-state.buffer-timeout` | 5000 |
| `table.exec.async-state.enabled` | true |
| `table.exec.mini-batch.enabled` | false (forces the async-state join path) |
| `table.exec.state.ttl` | 0 ms (forst-rs MapState TTL unsupported) |

RocksDB / ForSt arms checkpoint every 30 s, EXACTLY_ONCE, async-state on,
mini-batch off, RocksDB ttl 1h, ForSt ttl 1h (their templates).

### 3.4 Workload

100M events default; `TPS=10M`; per-query `MAXSEC=3600`. (`EVENTS_NUM=1000000`
only for smokes.)

---

## 4. forst-rs lever flags — UNIFORM values + the dynamic-optimization note

`run-8c32g.sh` **forwards** these lever flags into the TM/JM containers (this was
previously a bug — the harness did NOT forward them; it is now fixed, so setting
them on the `run` line takes effect inside the engine). Under the **no-per-query**
policy they all take **one** value for every query:

| env | UNIFORM value | meaning |
|---|---|---|
| `FRS_SST_COMPRESSION` | **`lz4`** (harness default) | fair vs ForSt/RocksDB engine default; also the forst-rs engine default (`crates/forst-rs-common` config.rs:268) |
| `FRS_VLOG_COMPRESSION` | `inherit` (default) | follows SST compression |
| `FRS_KV_SEPARATION` | **OFF** (unset) | KV-separation is part of the in-progress *adaptive* layer; uniform-safe default is OFF |
| `FRS_KV_MIN_BLOB_SIZE` | OFF (unset) | only meaningful with KV-sep on |
| `FRS_TRIVIAL_MOVE` | **OFF** (unset) | adaptive layer |
| `FRS_RS_S2_PINNED` | **OFF** (unset) | adaptive S2 pinned-merge; adaptive layer |
| `FRS_REMOTE_COMPACTION` | **OFF** (unset) | no remote-compaction worker under mock S3 |

**Why OFF and not the "PMC-1 ON stack".** The runbook's §5.1 ON stack
(`FRS_KV_SEPARATION=1 FRS_KV_MIN_BLOB_SIZE=256 FRS_TRIVIAL_MOVE=1
FRS_RS_S2_PINNED=1`) is a *hand-picked* per-arm optimization. The current policy
forbids hand-picked tuning; the only allowed optimization is **dynamic/adaptive
engine behavior under one config**. The adaptive versions of exactly these levers
— **adaptive KV-separation, adaptive S2 pinned-merge, adaptive executor** — are
being built; once shipped they activate **under the SAME config, with no
per-query flags**. Until then the safe uniform default is **levers OFF except
lz4 compression**. `lz4` stays ON because it is both the fairness baseline (ForSt
and RocksDB use a compressed engine default) and the forst-rs engine default — it
is a property of the config, not a per-query tweak.

> When the adaptive layer lands, this table does not change shape: the flags stay
> at their uniform values (OFF / lz4) and adaptivity happens inside the engine.

---

## 5. jemalloc on the remote box

The TM JVM uses **jemalloc via `LD_PRELOAD`**, applied uniformly to ALL backends
(it is a property of the box, like the kernel). `run-8c32g.sh` under `TOPO=split`
sets `-e LD_PRELOAD=/usr/local/lib/libjemalloc-preload.so` on each TM when
`FRS_TM_JEMALLOC` is `1` (the default). The engine's own jemalloc is statically
bundled inside `libforst_rs_ffi.so` with prefixed symbols and is unaffected by the
preload.

- The preload library must exist in the bench image at
  `/usr/local/lib/libjemalloc-preload.so` (the remote x86 image
  `docker/bench-remote.Dockerfile` bakes `libjemalloc2`; confirm the preload path
  resolves to the system `libjemalloc.so` there).
- To disable for an A/B: `FRS_TM_JEMALLOC=0`.

---

## 6. Disk / scratch — pick-disk + distinct bases

- `BASE=$(bash scripts/pick-disk.sh)` echoes the least-IO-utilized candidate from
  `/ssd2/jackylee /ssd1/jackylee /tmp/jackylee` (override via
  `FRS_DISK_CANDIDATES`). Pass `FRS_CTMP_BASE=$BASE/frs-bench-tmp`.
- The harness namespaces per-cluster scratch under
  `$FRS_CTMP_BASE/$CLUSTER` (and binds the base into the containers so the
  host-absolute Flink conf dir resolves inside them).
- **Both arms of an A/B pair use the SAME disk** (call pick-disk once per pair).
  Concurrent *different* clusters should use **distinct** bases.

---

## 7. Deployment to the remote `/ssd2/jackylee` — git pull (the only path right now)

The relay bridge that would let us write directly to the remote box is
**fingerprint-gated and currently down** (see §9). So deployment of these new
files is **version-control + pull**: commit + push on `forst-rs`, then on the
remote box pull under the checkout.

**One-line deploy (run on the remote box, under `/ssd2/jackylee`):**

```bash
git -C /ssd2/jackylee/ForSt fetch origin forst-rs && git -C /ssd2/jackylee/ForSt reset --hard origin/forst-rs
```

Then make the wrapper executable (first deploy only; git preserves the mode after
that):

```bash
chmod +x /ssd2/jackylee/ForSt/scripts/run-remote-nexmark-v3.sh
```

After pulling, follow the runbook §0 (b)–(c) for image/.so/jar build if those are
stale, then invoke the v3 wrapper (§8.1 below).

---

## 8. MOCK S3 only — no real endpoint for perf

Real S3 perf in this environment is bad (bad-S3-env), so **all perf validation
uses MOCK S3**: the NexMark perf arm is `forst-rs-ffm-local` (LocalFileSystem
state dir on the NVMe). Do **not** select `forst-rs-ffm-s3` for any perf run.

- For the *disagg* projection (a separate minibench, not the NexMark harness), the
  online box is modeled with **`FRS_MODEL_BW_MBPS=6250` (≈ 50 Gb/s)** — consumed
  by `crates/forst-rs-bench/src/bin/disagg_vs_forst.rs` /
  `nexmark_disagg_s3.rs`, which drive the real link/adopt-checkpoint paths over
  `LocalFileSystem` and cost the comparison at that bandwidth. The NexMark
  `run-8c32g.sh` harness does NOT read `FRS_MODEL_BW_MBPS` (it is local-FS), so
  the v3 wrapper exports it only as the documented disagg-projection value; it has
  no effect on the local NexMark arm.
- Real-S3 E2E perf is a user-gated item for the co-located online box — record
  nothing under it until then.

### 8.1 Invoking the v3 wrapper

```bash
TOPO=split REPO=/ssd2/jackylee/ForSt WORKENV=~/workenv FLINK=~/workenv/flink-2.2.1 \
  IMG=forst-bench:x86 PLAT=linux/amd64 NEXMARK_HOME=~/workenv/nexmark-flink \
  bash /ssd2/jackylee/ForSt/scripts/run-remote-nexmark-v3.sh
```

The wrapper pins the uniform config (the §4 flag values + lz4 + jemalloc default),
picks a disk via pick-disk.sh, and runs the 8 priority queries × 3 backends
**strictly serial, one cluster at a time**, each invoking the unmodified
`run-8c32g.sh run ... 3600`. See its header for usage / overrides.

---

## 9. Relay-bridge auth note (fingerprint-gated)

The remote control channel is:

```
/tmp/relay-bridge.exp → /tmp/relay-cmd.fifo → /tmp/relay-out.log
relay-cli -t fp <fingerprint>      # requires the user's physical fingerprint touch
```

It is **BLOCKED on the user's physical fingerprint touch** and is **down right
now**, which is *why* deployment is by `git pull` (§7), not a direct remote write.
When live, verify before use:

```bash
echo 'whoami; hostname' > /tmp/relay-cmd.fifo ; tail -5 /tmp/relay-out.log
```

---

## 10. Concurrency etiquette (shared box)

- **≤ 3 concurrent remote services** on the box. For this sweep run **STRICTLY
  SERIAL** — one NexMark cluster at a time (self-contention corrupts perf
  numbers). The v3 wrapper enforces serial.
- **Per-cluster namespacing**: every run has its own `CLUSTER=` →
  `$CLUSTER-{jm,tm1,tm2}` containers, `$CLUSTER-net` network, conf dir, `/tmp`
  scratch. Cleanup must stay namespaced — never `docker rm -f` a bare name that
  could hit another tenant's cluster.
- **Distinct `FRS_CTMP_BASE` disks** for any clusters that DO run alongside this
  sweep (pick-disk spreads across `/ssd2 /ssd1 /tmp jackylee`). A/B pairs reuse
  the SAME disk but still run serially.
- **Never collide with other tenants' NexMark**: check the box for existing
  `*-jm/*-tm*` containers and `uptime` load before launching; wait for idle. The
  `$FLINK/lib` .so / jar is SHARED — concurrent clusters must be the same build.

---

## 11. Summary — the canonical uniform config in one block

```
TOPO                       = split   (2× TM 4c/16g + 1× JM 2c/4g)
backends                   = forst-rs-ffm-local (JDK25, LocalFS) | rocksdb (JDK17) | forst-local (JDK17)
priority queries           = q4 q7 q9 q11 q12 q17 q19 q20   (same config for all; q0-q22 too)
MAXSEC                     = 3600   ;   100M events ; TPS=10M

# forst-rs engine (config-forst-rs-local.yaml.tpl) — ForSt-matching:
writebuffer.size           = 1024mb (= 1G)     # matches write_buffer_size=1G
checkpoint.noflush         = false             # matches noflush=false
writebuffer.count          = 4 ; WBM capacity = 4096mb
compaction.max-background  = 8 ; flush.max-background = 4
cache.block.capacity       = 2048mb ; storage.cache-capacity-mb = 65536
timer-service.factory      = FORSTRS ; GC = G1GC
checkpoint interval        = 30 s, EXACTLY_ONCE ; async-state on ; mini-batch off ; ttl 0
TM process.size            = 12288m (forst-rs & forst) / 8192m (rocksdb)

# UNIFORM forst-rs lever flags (no per-query tuning):
FRS_SST_COMPRESSION        = lz4        # ON (fairness + engine default)
FRS_VLOG_COMPRESSION       = inherit
FRS_KV_SEPARATION          = OFF        # adaptive version in progress (same config)
FRS_TRIVIAL_MOVE           = OFF        # adaptive version in progress
FRS_RS_S2_PINNED           = OFF        # adaptive version in progress
FRS_REMOTE_COMPACTION      = OFF        # no remote worker under mock S3
FRS_TM_JEMALLOC            = 1          # jemalloc LD_PRELOAD on all backends

# mock S3:
NexMark arm                = forst-rs-ffm-local (LocalFS) ; real S3 OFF
FRS_MODEL_BW_MBPS          = 6250 (≈50Gb/s)  # disagg minibench projection ONLY
```

Deploy: `git -C /ssd2/jackylee/ForSt fetch origin forst-rs && git -C /ssd2/jackylee/ForSt reset --hard origin/forst-rs`
