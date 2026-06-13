# NexMark End-to-End Performance Test Runbook (forst-rs / RocksDB / ForSt)

Self-contained guide: anyone with repo access + a Linux box (or Apple-Silicon
Mac) can build, run, and interpret the 3-backend NexMark @100M benchmark.

## 0. REMOTE FIRE SEQUENCE (x86 Linux box — copy/paste, top to bottom)
This is THE staged sequence for the binding REMOTE-x86 @100M run. It is
**fire-ready the instant (a) is unblocked**. Sections §2-§9 are the reference
detail behind each step. Run it on the remote box (sshdata00 → yq01), NOT the
Mac. **MOCK S3 only — never the real endpoint (§11). STRICTLY SERIAL — one
cluster at a time (§12).**

```
(a) BRIDGE AUTH  ── BLOCKED on user physical fingerprint touch ──
    /tmp/relay-bridge.exp  →  /tmp/relay-cmd.fifo  →  /tmp/relay-out.log
    relay-cli -t fp <fingerprint>          # needs the user's physical touch.
    Verify the channel is live before anything else:
      echo 'whoami; hostname' > /tmp/relay-cmd.fifo ; tail -5 /tmp/relay-out.log
    Everything below runs THROUGH this bridge on the remote box.

(b) PULL WORKTREE to /ssd2/jackylee  (the NVMe; repo path the harness expects)
      cd /ssd2/jackylee && git -C ForSt fetch origin forst-rs \
        && git -C ForSt checkout forst-rs && git -C ForSt reset --hard origin/forst-rs
      git -C flink fetch origin forst-rs-jdk25 && git -C flink checkout forst-rs-jdk25 \
        && git -C flink reset --hard origin/forst-rs-jdk25     # backend jar source

(c) BUILD image + in-image .so + jar   (3 commands, see §3/§4)
    # c1: bench image (docker 19.03-safe; JDK25 copied from build ctx, libjemalloc2 baked):
      mkdir -p /ssd2/jackylee/frs-bench/imgctx
      cp -a ~/workenv/jdk25.0.2-linux_x64_gcc12 /ssd2/jackylee/frs-bench/imgctx/jdk25
      cp /ssd2/jackylee/ForSt/docker/bench-remote.Dockerfile /ssd2/jackylee/frs-bench/imgctx/Dockerfile
      docker build -t forst-bench:x86 /ssd2/jackylee/frs-bench/imgctx
    # c2: engine .so — built NATIVELY on the host (host glibc ≤ jammy 2.35);
    #     verify it loads in the image ONCE: docker run --rm -v <so>:/t/x.so forst-bench:x86 bash -c 'ldd /t/x.so'
      cd /ssd2/jackylee/ForSt && CARGO_TARGET_DIR=$PWD/target-linux cargo build --release -p forst-rs-ffi
    # c3: backend jar (host maven, JDK25) → copied into $WORKENV/flink-2.2.1/lib/
      REPO=/ssd2/jackylee/ForSt WORKENV=~/workenv FLINK=~/workenv/flink-2.2.1 \
        IMG=forst-bench:x86 PLAT=linux/amd64 bash /ssd2/jackylee/ForSt/scripts/run-8c32g.sh jar

(d) SWEEP  q4 q7 q11 q19 q20 × {forst-rs-ffm-local, rocksdb, forst-local}
    SPLIT TOPO, @100M, STRICTLY SERIAL (one cluster at a time — self-contention
    was a real local failure mode). Pick a FREE disk first; reuse it for the
    whole sweep so populations don't mix:
      BASE=$(bash /ssd2/jackylee/ForSt/scripts/pick-disk.sh)     # e.g. /ssd2/jackylee
    Per (query, arm) — wait for the previous cluster to fully tear down before
    the next `run` returns (the harness rm's its own containers/network):
      Q=q9; CFG=forst-rs-ffm-local; TAG=rx86-$Q-$CFG
      TOPO=split REPO=/ssd2/jackylee/ForSt WORKENV=~/workenv FLINK=~/workenv/flink-2.2.1 \
        IMG=forst-bench:x86 PLAT=linux/amd64 NEXMARK_HOME=~/workenv/nexmark-flink \
        FRS_CTMP_BASE=$BASE/frs-bench-tmp CLUSTER=$TAG \
        FRS_KV_SEPARATION=1 FRS_KV_MIN_BLOB_SIZE=256 FRS_TRIVIAL_MOVE=1 FRS_RS_S2_PINNED=1 \
        bash /ssd2/jackylee/ForSt/scripts/run-8c32g.sh run $Q $CFG 3600 $TAG
      # rocksdb / forst-local arms: DROP the FRS_* lever flags (engines ignore them).
      # io_uring: TMs run with seccomp=unconfined under split-topo on this box (q7
      #   needs io_uring or it DNFs); FRS_IO_URING falls back to pread silently.
    Loop the 5 queries × 3 arms = 15 clusters, serial, with a ledger (§8) so a
    re-run fills only missing tags. Canaries to sanity-check (§10): q8 rows in
    [3.064M, 3.066M]; q9 frs out_rows canonical 91,813,372.

(e) RECORD into docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md
    Append each result to the REMOTE scoreboard, tagged **REMOTE-x86** with the
    disk used. This is a SEPARATE population — NEVER cross-compare with the Mac
    pins in that file (rule §9.10). Update CURRENT STATUS at the top.
```

## 1. Repos & branches
| Repo | Branch | Role |
|---|---|---|
| github.com/jackylee-ch/ForSt | `forst-rs` | Rust engine + bench harness (THIS repo) |
| github.com/jackylee-ch/flink | `forst-rs-jdk25` | Flink 2.2.1 fork + forst-rs backend (module `flink-state-backends/flink-statebackend-forst-rs`) |

## 2. Host prerequisites (the "workenv")
A directory (default `~/Downloads/workenv` on Mac, `~/workenv` on Linux; override
via `WORKENV=`) containing:
- `flink-2.2.1/` — Flink dist. **REQUIRED PATCH**: `bin/sql-client.sh` must be a
  wrapper rerouting `embedded` → `sql-client.sh.orig gateway --endpoint
  localhost:8083` (nexmark hardcodes `embedded`; the harness starts a SqlGateway
  on 8083). 4-line wrapper; see §9.1.
- `nexmark-flink/` — built nexmark distribution (`queries/*.sql`, `lib/*.jar`;
  copy nexmark jar into `flink-2.2.1/lib/`).
- JDK25 (forst-rs backend) + JDK17 (RocksDB/ForSt backends).
- `frs-tmp/` — scratch root (auto-created; see §6 disk policy).
- For ForSt runs: `flink-statebackend-forst-2.2.1.jar` + `forstjni-0.1.8.jar`
  in `flink-2.2.1/lib/`.
Toolchains: docker (≥19.03 works), maven 3.9+, rust stable (for host .so builds).

## 3. Bench image
- **Apple-Silicon Mac**: `docker build -t forst-bench:arm64 -f docker/bench.Dockerfile .`
- **x86 Linux** (esp. docker 19.03 which cannot pull modern OCI manifests):
  `docker/bench-remote.Dockerfile` — base `docker.m.daocloud.io/eclipse-temurin:17-jre-jammy`,
  JDK25 COPIED from build context (header documents the 3-command build).
- Fallback (no internet in `docker build`): run a base container, install
  python3/gettext-base/curl/procps/libjemalloc2 via a reachable mirror inside
  it, `docker commit` it as the bench image.

## 4. Build artifacts
```bash
# Engine .so — EITHER in-image (Mac/arm64):
scripts/run-8c32g.sh build          # → target-linux/release/libforst_rs_ffi.so
# OR on an x86 Linux host directly (glibc must be ≤ image's; verify with ldd):
CARGO_TARGET_DIR=$PWD/target-linux cargo build --release -p forst-rs-ffi

# Backend jar (host maven, JDK25):
scripts/run-8c32g.sh jar            # builds + copies into $WORKENV/flink-2.2.1/lib/
# (manual equivalent: mvn -DskipTests -Denforcer.skip=true -Dcheckstyle.skip=true \
#  -Dspotless.check.skip=true -Drat.skip=true -Dmaven.javadoc.skip=true clean package
#  in flink-state-backends/flink-statebackend-forst-rs, then cp target/*.jar)
```

## 5. Running a benchmark
```bash
# Single run:  scripts/run-8c32g.sh run <query> <config> <maxsec> [tag]
TOPO=split REPO=$PWD WORKENV=$HOME/workenv FLINK=$HOME/workenv/flink-2.2.1 \
  IMG=forst-bench:x86 PLAT=linux/amd64 NEXMARK_HOME=$HOME/workenv/nexmark-flink \
  FRS_KV_SEPARATION=1 FRS_KV_MIN_BLOB_SIZE=256 FRS_TRIVIAL_MOVE=1 FRS_RS_S2_PINNED=1 \
  scripts/run-8c32g.sh run q9 forst-rs-ffm-local 3600 mytag
```
- Configs: `forst-rs-ffm-local` (JDK25) | `rocksdb` (JDK17) | `forst-local`
  (JDK17 + ForSt jars) | `forst-rs-ffm-s3` (S3 creds via S3_* envs — **PERF: DO
  NOT USE**, see §11 MOCK-S3-ONLY).

### 5.1 forst-rs flag-ON lever stack (PMC-1 validated locally; carry to REMOTE)
run-8c32g.sh now FORWARDS these into the TM/JM containers; pass them on the
`run` line for the forst-rs arm. The PMC-1-validated stack:
| env | value | what |
|---|---|---|
| `FRS_SST_COMPRESSION` | `lz4` (HARNESS DEFAULT) | fair vs ForSt/RocksDB engine default; also the forst-rs engine default (common config.rs:268) |
| `FRS_KV_SEPARATION` | `1` | KV-separation (blob) — keeps big values out of the LSM, cuts compaction write-amp |
| `FRS_KV_MIN_BLOB_SIZE` | `256` | min value bytes to separate into the blob/vlog |
| `FRS_TRIVIAL_MOVE` | `1` | trivial-move compaction (no rewrite when key ranges don't overlap) |
| `FRS_RS_S2_PINNED` | `1` | S2 pinned-rows + loser-tree merge (q7/join lever; micro: join_probe_open 9.2× on 128-SST) |
- `FRS_SST_COMPRESSION=lz4` is the harness default — only override it (`=none`)
  for the zero-copy read-path A/B. The other four are OFF by default (merged
  flag-OFF) and MUST be set explicitly on the forst-rs arm.
- `FRS_VLOG_COMPRESSION` defaults to `inherit` (follows SST compression).
- `FRS_REMOTE_COMPACTION` is also forwarded but stays OFF for this sweep (mock
  S3, no remote-compaction worker provisioned).
- **rocksdb / forst-local arms IGNORE these flags** (different engines) — set
  them ONLY on `forst-rs-ffm-local`. The lz4 default already makes SST
  compression fair across all three (prior remote write-amp numbers were
  frs-uncompressed-vs-compressed — invalid).
- `TOPO=split` (RECOMMENDED, the official topology): 2 TM containers 4c/16g +
  JM container 2c/4g (8c/32g budget = TM-only). `TOPO=single` = legacy 1×8c/32g.
- Every run is namespaced (`CLUSTER=frs-<tag>`): own containers, network, conf,
  scratch — **concurrent runs are safe** (same .so/jar version per box).
- Workload: 100M events default (`EVENTS_NUM=1000000` for smokes), TPS=10M.
- Result: `RESULT: <q> FINISHED wall_ms=... src_out=... out_rows=...` (also in
  `$scratch/<tag>-<q>-<cfg>.out`). DNF prints `MAXSEC ... giving up`.

## 6. Disk / IO policy (multi-run boxes)
- `BASE=$(bash scripts/pick-disk.sh)` picks the least-IO-utilized state dir;
  pass `FRS_CTMP_BASE=$BASE/frs-bench-tmp`. **Both arms of an A/B pair use the
  SAME disk**; record the disk with every result.
- If the host's frs-tmp is a separate mount dockerd won't traverse (symptom:
  conf invisible in containers), `FRS_CTMP_BASE` to a dockerd-visible path
  fixes it (the script binds the base into containers).

## 7. ForSt-backend specifics (hard-won)
- The bench image may bake a STALE `libforstjni.so` on `LD_LIBRARY_PATH`,
  shadowing the jar's bundled native → `UnsatisfiedLinkError` crash-loop.
  FIX: add `-e LD_LIBRARY_PATH=` to the JM+TM `docker run`s (wrapper-script
  precedent: `integ-forst-run.sh` pattern — copy run-8c32g.sh, add the env).
- `forst-local` template expects JDK17 at the image's `env.java.home`; override
  with `JDK17_IN_IMG=` if the image differs.

## 8. Full sweep (q0-q22, both backends)
Pattern (proven): a box-side driver script run under **setsid+nohup** that loops
queries × arms with `MAXSEC=3600`, appends `RESULT-INT: <tag> ...` lines to a
ledger file, and SKIPS tags already present (`grep -q "RESULT-INT: $tag "`)
— restart-safe: rerun the driver to fill gaps; delete a line to force a redo.
Wait-for-idle + load-gate (<22 on 28c) between runs. Reference implementation
lives on the bench box at `/ssd2/jackylee/frs-bench/run-integrity.sh` (ledger
`logs/SUMMARY-INTEGRITY.md`).

## 9. Known landmines (each cost hours; read before debugging)
1. `bin/sql-client.sh` wrapper missing → nexmark submit hangs (§2).
2. nohup'd scripts lose docker from PATH on some hosts →
   `env PATH=/home/work/dockerd/bin:$PATH` (adjust to the host).
3. Docker IPv4 pool exhaustion → `docker network create` fails silently →
   pre-create with explicit `--subnet` (harness fallback: `FRS_NET_SUBNET`).
4. `queries/qX.sql` without trailing newline → sql-client silently drops the
   last line (the INSERT!) — the harness appends a newline; keep it.
5. `out_rows` is sink read-records sampled at FINISHED → ±~200-row jitter on
   BOTH backends; src_out is exact. Use the fixed-CSV accuracy gate
   (`scripts/accuracy-gate/run-matrix.sh`, 1M deterministic input, hash compare)
   for byte-level correctness.
6. Drivers killed by session teardown → always `setsid` + nohup + a resumable
   ledger.
7. Concurrent operators on one control channel WILL kill each other's clusters
   — one operator per box, or strictly namespaced clusters (the harness
   namespacing makes runs safe; cleanup must stay namespaced too).
8. io_uring needs `--security-opt seccomp=unconfined` on TMs under older
   docker; the engine probes and falls back to pread silently (`FRS_IO_URING`).
9. Datagen TPS fixes event-time span: small-event accuracy runs need low
   `GEN_TPS` or windows degenerate (accuracy gate handles this).
10. Perf comparisons: same-session pairs on the same disk only; never compare
    across boxes/populations (Mac vs Linux numbers differ wildly per query).

## 10. Interpreting results / bars
- Per-query bar: forst-rs ≤1.25× RocksDB wall (≥0.8× speed) AND faster than
  ForSt AND rows consistent.
- Canaries: q8 out_rows ∈ [3.064M, 3.066M]; q9 out_rows canonical 91,813,372
  (frs, exact); q17/q5 rows = 92M/30M-class.
- All historical results + methodology: docs/superpowers/specs/
  2026-06-08-8c32g-3backend-sweep-results.md (CURRENT STATUS at top).

## 11. ★ MOCK S3 ONLY — do NOT touch the real S3 endpoint for perf
Real S3 credentials (`S3_ENDPOINT/_ACCESS_KEY/_SECRET_KEY/_BUCKET/_REGION/
_PREFIX`) ARE present in the environment and the harness wires them through
(run-8c32g.sh passes `-e S3_*`, measure-sql.sh `envsubst`s them into the
`forst-rs-ffm-s3` / `config-forst-rs.yaml.tpl` template). **They stay UNUSED for
perf** — the current env's S3 connect perf is bad, so real-S3 numbers are
meaningless until the user green-lights the ≥50 Gb/s co-located online box.
- **All perf validation uses MOCK S3** = LocalFileSystem / latency-emulated FS
  with `FRS_MODEL_BW_MBPS` (project the online box at **6250** ≈ 50 Gb/s).
  The disagg minibench `crates/forst-rs-bench/src/bin/disagg_vs_forst.rs`
  already does this: it drives the REAL engine link/adopt-checkpoint paths over
  `LocalFileSystem` and costs the ForSt comparison column at
  `FRS_MODEL_BW_MBPS` / `FRS_MODEL_RTT_MS` (defaults 10 MB/s / 23 ms = the
  recorded dev-Mac→BOS baseline; override to 6250 for the online-box projection).
- **The NexMark sweep arm is `forst-rs-ffm-local`** (LocalFileSystem state dir
  on the NVMe), NOT `forst-rs-ffm-s3`. Do not select the s3 config for any perf
  run. Real-S3 E2E perf is a user-gated Phase-3 item — record nothing under it
  until then.

## 12. Concurrency etiquette (shared box — don't collide with other tenants)
- **≤3 concurrent remote services** total on the box. For THIS sweep, run
  STRICTLY SERIAL (one NexMark cluster at a time) — self-contention on one box
  was a real local failure mode and corrupts perf numbers.
- **Per-cluster namespacing**: every `run` gets its own `CLUSTER=` (own
  containers `$CLUSTER-{jm,tm1,tm2}`, network `$CLUSTER-net`, conf dir, and
  `/tmp` scratch). Cleanup MUST stay namespaced — never `docker rm -f` by a
  bare name that could hit another tenant's cluster.
- **Distinct `FRS_CTMP_BASE` disks for concurrent clusters**: if anything DOES
  run alongside this sweep, point each cluster's scratch at a DIFFERENT disk via
  `FRS_CTMP_BASE` (pick-disk.sh spreads across `/ssd2 /ssd1 /tmp jackylee`).
  A/B pairs (same query, two arms) reuse the SAME disk so the comparison is fair
  — but they still run serially here.
- **Never collide with other tenants' NexMark**: check the box for existing
  `*-jm/*-tm*` containers and load (`uptime`) before launching; wait for idle.
  The bench .so / jar in `$FLINK/lib` is SHARED — concurrent clusters must be the
  same engine build.

### 9.1 sql-client wrapper (verbatim)
```bash
cd $WORKENV/flink-2.2.1/bin && mv sql-client.sh sql-client.sh.orig && cat > sql-client.sh <<'EOF'
#!/usr/bin/env bash
if [ "${1:-}" = "embedded" ]; then shift
  exec "$(dirname "$0")/sql-client.sh.orig" gateway --endpoint localhost:8083 "$@"
fi
exec "$(dirname "$0")/sql-client.sh.orig" "$@"
EOF
chmod +x sql-client.sh
```
