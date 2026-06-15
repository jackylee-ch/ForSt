# NexMark End-to-End Performance Test Runbook (forst-rs / RocksDB / ForSt)

Self-contained guide: anyone with repo access + a Linux box (or Apple-Silicon
Mac) can build, run, and interpret the 3-backend NexMark @100M benchmark.

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
  scripts/run-8c32g.sh run q9 forst-rs-ffm-local 3600 mytag
```
- Configs: `forst-rs-ffm-local` (JDK25) | `rocksdb` (JDK17) | `forst-local`
  (JDK17 + ForSt jars) | `forst-rs-ffm-s3` (S3 creds via S3_* envs).
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
