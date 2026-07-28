#!/usr/bin/env bash
# 8c/32g TRUE-resource benchmark driver — BOS / REMOTE DISAGG variant.
# PORTABLE across macOS (Apple Silicon dev box, arm64 container) AND the origin
# x86_64 Linux box (amd64 container).
#
# DELTA vs tools/nexmark-local/scripts/run-8c32g.sh: this copy forwards the REAL
# BOS S3 creds (S3_ENDPOINT/S3_ACCESS_KEY/S3_SECRET_KEY/S3_BUCKET/S3_REGION/
# S3_PREFIX) into the TM/JM containers — the nexmark-local copy pins them to `x`
# because it only runs LocalFS / mock-S3 — and it forwards the Phase-2 REMOTE
# write-path / disagg optimization knobs (FRS_UPLOAD_RATE_SPLIT,
# FRS_UPLOAD_COMPACTION_SHARE, FRS_CKPT_LINK_MODE, FRS_REMOTE_NONSST_LOCAL,
# FRS_RESTORE_BG_FILL*, FRS_CACHE_SPACE_LIMIT_MB, FRS_MODEL_*). Everything else
# (platform detection, topology, jemalloc/io_uring) is identical.
#
#   scripts/run-8c32g.sh build              # build the forst-rs Linux .so (once + on engine changes)
#   scripts/run-8c32g.sh run <q> <cfg> <maxsec> [tag]
#       e.g. run-8c32g.sh run q19 forst-rs-ffm-local 900 c32
#            run-8c32g.sh run q4  rocksdb            900 c32
#   scripts/run-8c32g.sh jar                # rebuild + redeploy the forst-rs jar (host maven), then it's mounted
#
# Hard limits: --cpus=8 --memory=32g (split = 2 TM 4c/16g + 1 JM 2c/4g). Mounts
# the repo + workenv at their SAME host paths so the harness's absolute paths
# resolve; only JDK/template paths are overridden to Linux. The Linux .so lives
# at target-linux/release and is copied into FLINK_HOME/lib at run time.
#
# PLATFORM PORTABILITY (see docs/README.md "Reproduce on ..."):
#   - OS detected via `uname -s` (Darwin = macOS dev, Linux = origin box).
#   - Defaults for REPO/WORKENV, the container PLAT/IMG, the jemalloc preload
#     path, and physical-RAM-derived sizing all branch on OS but are FULLY
#     env-overridable. Nothing is hardcoded to one machine.
#   - Physical RAM is auto-detected (macOS sysctl hw.memsize; Linux
#     /proc/meminfo MemTotal) and used only to sanity-warn that the requested
#     TM/JM container memory fits with OS headroom — it never forces a size.
set -u

DOCKER_BIN="${DOCKER_BIN:-/home/work/dockerd/bin/docker}"
if [ -x "$DOCKER_BIN" ]; then
  export PATH="$(dirname "$DOCKER_BIN"):$PATH"
fi

# --- platform detection (branch only where behavior differs) ---
OS="$(uname -s)"        # Darwin (macOS) | Linux (origin box)

# Physical RAM in MiB (used to size/sanity-check the single-TM profile; never
# hardcode 64GiB). Portable: macOS sysctl vs Linux /proc/meminfo|free.
detect_ram_mib() {
  case "$OS" in
    Darwin) local b; b="$(sysctl -n hw.memsize 2>/dev/null)"; [ -n "$b" ] && echo $(( b / 1024 / 1024 )) || echo 0 ;;
    Linux)
      if [ -r /proc/meminfo ]; then
        awk '/^MemTotal:/ {print int($2/1024)}' /proc/meminfo
      else
        local b; b="$(free -b 2>/dev/null | awk '/^Mem:/ {print $2}')"; [ -n "$b" ] && echo $(( b / 1024 / 1024 )) || echo 0
      fi ;;
    *) echo 0 ;;
  esac
}
PHYS_RAM_MIB="${PHYS_RAM_MIB:-$(detect_ram_mib)}"

# Parse a docker --memory value ("36g"/"32g"/"512m") into MiB for the headroom check.
mem_to_mib() {
  local v="$1"; local n="${v%[gGmM]}"; case "$v" in
    *g|*G) echo $(( n * 1024 )) ;; *m|*M) echo "$n" ;; *) echo $(( n / 1024 / 1024 )) ;;
  esac
}

# All env-overridable so the SAME script drives macOS and the remote Linux box.
# Per-OS defaults: macOS dev checkout under /Users/...; Linux origin checkout
# under /ssd2/$USER (documented, NOT hardcoded — REPO/WORKENV override both).
if [ "$OS" = "Darwin" ]; then
  REPO="${REPO:-/Users/lijunqing/Code/stczwd/ForSt}"
  WORKENV="${WORKENV:-/Users/lijunqing/Downloads/workenv}"
  IMG="${IMG:-forst-bench:arm64}"
  PLAT="${PLAT:-linux/arm64}"
else
  # Linux origin box: checkout lives at /ssd2/$USER/ForSt; x86_64 container.
  REPO="${REPO:-/ssd2/$USER/ForSt}"
  WORKENV="${WORKENV:-$HOME/workenv}"
  IMG="${IMG:-nexmark-bos:x86}"
  PLAT="${PLAT:-linux/amd64}"
fi
FLINK="${FLINK:-$WORKENV/flink-2.2.1}"
HADOOP_HOME="${HADOOP_HOME:-$WORKENV/hadoop-3.3.6}"
BOS_HADOOP_FS_JAR="${BOS_HADOOP_FS_JAR:-$HADOOP_HOME/share/hadoop/common/lib/bos-hadoop-fs-2.0.0.jar}"

DKR_PLATFORM=()
if [ "$OS" = "Darwin" ] || [ "${USE_DOCKER_PLATFORM:-0}" = "1" ]; then
  DKR_PLATFORM=(--platform "$PLAT")
fi

DKR_COMMON=(${DKR_PLATFORM[@]+"${DKR_PLATFORM[@]}"}
  -v "$REPO:$REPO" -v "$WORKENV:$WORKENV"
  # FRS-SCRATCH (2026-06-08): the container's /tmp is a ~59 GB overlay on Docker.raw
  # (the Docker-Desktop VM disk), NOT the host's 425 GB volume. q9/q20/q4 write
  # ~36-45 GB of SST data + cache + checkpoint-noflush memtable artifacts to /tmp and
  # fill that 59 GB → "No space left on device" → job crash (mis-read earlier as OOM).
  # Bind-mount /tmp to a host dir on the big volume so all engine scratch uses it.
  -v forst-cargo:/cargo-cache
  -e CARGO_HOME=/cargo-cache
  # JDK17 path INSIDE the container depends on the container arch (the .deb
  # package suffix), not the host: arm64 image -> ...-arm64, amd64 -> ...-amd64.
      -e JDK17="${JDK17_IN_IMG:-/usr/lib/jvm/java-17-openjdk-$([ "$PLAT" = "linux/amd64" ] && echo amd64 || echo arm64)}"
      -e JDK25=/opt/java/openjdk
      -e TEMPLATES="${TEMPLATES:-$REPO/scripts/templates-linux}"
      -e FRS_DISABLE_HADOOP_CLASSPATH="${FRS_DISABLE_HADOOP_CLASSPATH:-}"
      -e HADOOP_HOME="$HADOOP_HOME"
      -e BOS_HADOOP_FS_JAR="$BOS_HADOOP_FS_JAR"
  -e NEXMARK_HOME="$REPO/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink"
  -e FLINK_HOME="$FLINK"
  -w "$REPO")

# /tmp mount is per-invocation: single/build use the shared frs-tmp; TOPO=split
# namespaces it per-cluster (CONCURRENT NexMark clusters, 2026-06-12 directive).
TMP_MOUNT=(-v "$WORKENV/frs-tmp:/tmp")

cmd="${1:?build|run|jar}"; shift || true

case "$cmd" in
  build)
    echo "== building forst-rs Linux .so into target-linux/release =="
    if command -v cargo >/dev/null 2>&1; then
      (cd "$REPO" && CARGO_TARGET_DIR="$REPO/target-linux" cargo build --release -p forst-rs-ffi && ls -la target-linux/release/libforst_rs_ffi.so)
    else
      docker run --rm "${DKR_COMMON[@]}" "${TMP_MOUNT[@]}" -e CARGO_TARGET_DIR="$REPO/target-linux" "${BUILD_IMG:-$IMG}" \
        bash -lc 'cargo build --release -p forst-rs-ffi && ls -la target-linux/release/libforst_rs_ffi.so'
    fi
    ;;
  run)
    Q="${1:?query}"; CFG="${2:?config}"; MS="${3:-900}"; TAG="${4:-c32}"
    SO="$REPO/target-linux/release/libforst_rs_ffi.so"
    [ -f "$SO" ] || { echo "missing $SO — run: $0 build"; exit 1; }
    [ -f "$BOS_HADOOP_FS_JAR" ] || { echo "missing $BOS_HADOOP_FS_JAR — set BOS_HADOOP_FS_JAR"; exit 1; }
    LOCAL_LOG_BASE="${LOCAL_LOG_BASE:-/tmp/jackylee/nexmark-bos-logs}"
    mkdir -p "$LOCAL_LOG_BASE"
    OUT="$LOCAL_LOG_BASE/${TAG}-$Q-$CFG.out"
    echo "== 8c/32g run: $Q [$CFG] MAXSEC=$MS tag=$TAG =="
    # FRS_PERF (2026-06-11): native-frame CPU profiling of the TM. Needs
    # perf_event_open which Docker's default seccomp profile blocks → relax
    # seccomp only when profiling is requested (benchmarks stay confined).
    PERF_OPTS=()
    [ -n "${FRS_PERF:-}" ] && PERF_OPTS=(--security-opt seccomp=unconfined --cap-add SYS_ADMIN)

    # io_uring / seccomp (PORTABLE, parameterized):
    #   The engine's async I/O path uses io_uring on Linux; Docker's DEFAULT
    #   seccomp profile blocks the io_uring_* syscalls, so q7 (and other
    #   io_uring-dependent read paths) DNF on the origin Linux box unless the
    #   container runs with seccomp relaxed. On macOS the engine falls back to
    #   blocking I/O inside the Docker-Desktop Linux VM, so this is a NO-OP.
    #   FRS_IO_URING: 1|true => add --security-opt seccomp=unconfined (default
    #   ON for Linux, OFF/no-op for macOS). Set FRS_IO_URING=0 to force OFF.
    URING_OPTS=()
    FRS_IO_URING_DEFAULT=0
    [ "$OS" = "Linux" ] && FRS_IO_URING_DEFAULT=1
    case "${FRS_IO_URING:-$FRS_IO_URING_DEFAULT}" in
      1|true|TRUE|yes) URING_OPTS=(--security-opt seccomp=unconfined) ;;
    esac
    ENVS=(
      -e QUERY="$Q" -e CONFIG="$CFG" -e MAXSEC="$MS" -e EVENTS_NUM="${EVENTS_NUM:-}" -e TPS="${TPS:-}" \
      -e RUN_ID="${RUN_ID:-}" \
      -e FRS_FLINK_PARALLELISM="${FRS_FLINK_PARALLELISM:-}" -e FRS_TM_SLOTS="${FRS_TM_SLOTS:-}" \
      # BOS DISAGG (tools/nexmark-bos): forward the REAL BOS S3 creds into the
      # TM/JM containers (the tools/nexmark-local copy pins these to `x` because
      # it only runs LocalFS / mock-S3). measure-sql.sh `envsubst`s them into
      # config-forst-rs.yaml.tpl's storage.uri + opendal-config + the s3 plugin
      # block. Default empty if unset — run-bos.sh hard-fails earlier when the
      # required ones are missing, so a real run never ships `x`.
      -e S3_ENDPOINT="${S3_ENDPOINT:-}" -e S3_ACCESS_KEY="${S3_ACCESS_KEY:-}" \
      -e S3_SECRET_KEY="${S3_SECRET_KEY:-}" -e S3_BUCKET="${S3_BUCKET:-}" \
      -e S3_REGION="${S3_REGION:-}" -e S3_PREFIX="${S3_PREFIX:-}" \
      # BOS DISAGG remote write-path / disagg optimization knobs (Phase-2). These
      # are the LEVERS the BOS harness exists to tune; run-bos.sh sets recommended
      # defaults and forwards them here so the engine inside the containers reads
      # them. See tools/nexmark-bos/docs/README.md "Optimization knobs".
      -e FRS_UPLOAD_RATE_SPLIT="${FRS_UPLOAD_RATE_SPLIT:-}" \
      -e FRS_UPLOAD_COMPACTION_SHARE="${FRS_UPLOAD_COMPACTION_SHARE:-}" \
      -e FRS_CKPT_LINK_MODE="${FRS_CKPT_LINK_MODE:-}" \
      -e FRS_REMOTE_NONSST_LOCAL="${FRS_REMOTE_NONSST_LOCAL:-}" \
      -e FRS_RESTORE_BG_FILL="${FRS_RESTORE_BG_FILL:-}" \
      -e FRS_RESTORE_BG_FILL_WORKERS="${FRS_RESTORE_BG_FILL_WORKERS:-}" \
      -e FRS_RESTORE_BG_FILL_PACE_MB="${FRS_RESTORE_BG_FILL_PACE_MB:-}" \
      -e FRS_CACHE_SPACE_LIMIT_MB="${FRS_CACHE_SPACE_LIMIT_MB:-}" \
      -e FRS_MODEL_BW_MBPS="${FRS_MODEL_BW_MBPS:-}" -e FRS_MODEL_RTT_MS="${FRS_MODEL_RTT_MS:-}" \
      # JVM process.size overrides consumed by measure-sql.sh INSIDE the container
      # (the q9 8c/36g single-TM profile sets these). Forward them so the profile
      # actually takes effect through this package's runner.
      -e FRS_TM_PROCESS_SIZE="${FRS_TM_PROCESS_SIZE:-}" -e FRS_JM_PROCESS_SIZE="${FRS_JM_PROCESS_SIZE:-}" \
      -e FRS_WRITEBUFFER_SIZE="${FRS_WRITEBUFFER_SIZE:-}" \
      -e FRS_WRITEBUFFER_COUNT="${FRS_WRITEBUFFER_COUNT:-}" \
      -e FRS_WRITEBUFFER_MANAGER_CAPACITY="${FRS_WRITEBUFFER_MANAGER_CAPACITY:-}" \
      -e FRS_BLOCK_CACHE_CAPACITY="${FRS_BLOCK_CACHE_CAPACITY:-}" \
      -e FRS_COMPACTION_MAX_BACKGROUND="${FRS_COMPACTION_MAX_BACKGROUND:-}" \
      -e FRS_FLUSH_MAX_BACKGROUND="${FRS_FLUSH_MAX_BACKGROUND:-}" \
      -e FRS_ASYNC_INFLIGHT_LIMIT="${FRS_ASYNC_INFLIGHT_LIMIT:-}" \
      -e FRS_ASYNC_BUFFER_SIZE="${FRS_ASYNC_BUFFER_SIZE:-}" \
      -e FRS_ASYNC_BUFFER_TIMEOUT="${FRS_ASYNC_BUFFER_TIMEOUT:-}" \
      # FRS-M5 FAIRNESS FIX (2026-06-13 cycle 2, PMC-1): the default was pinned
      # to `none` (a 2026-06-02 zero-copy read-path experiment), which forced
      # forst-rs to write SST blocks UNCOMPRESSED while the ForSt and RocksDB
      # bench templates (scripts/templates-linux/config-{forst,rocksdb}.yaml*)
      # set NO explicit compression → they use their engine default (Snappy/
      # LZ4). Every prior remote write-amp / disk number was thus frs-uncompressed
      # vs compressed competitors — an unfair ÷2-3 disk-bytes handicap on every
      # write-heavy query. The goal mandates "config must match Forst"; the
      # forst-rs engine default is also LZ4 (crates/forst-rs-common config.rs:268),
      # so LZ4 is BOTH the fair match AND the engine default. Override with
      # FRS_SST_COMPRESSION=none for the zero-copy read-path A/B. See survey §12.
      -e FRS_BLOCK_SIZE_KB=8 -e FRS_SST_COMPRESSION="${FRS_SST_COMPRESSION:-lz4}" \
      -e FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}" \
      -e FRS_VLOG_COALESCE_DEREF="${FRS_VLOG_COALESCE_DEREF:-}" \
      # KV-sep per-TM MEMORY BOUNDS (q9 KV-sep OOM fix, 2026-06-15 PMC-1).
      # These are the engine knobs that bound resident vlog state so KV-sep
      # fits the per-TM cgroup. Previously NOT forwarded → the bounded LRU /
      # byte-budget / adaptive-pressure machinery could not be turned on from
      # the harness, so q9 KV-sep ON could only "fit" by disabling KV-sep.
      #   FRS_VLOG_READER_CACHE_CAP   resident reader COUNT cap (default 2048)
      #   FRS_VLOG_RESIDENT_BUDGET_MB resident reader BYTE budget (default 0=off)
      #   FRS_KV_ADAPTIVE_PRESSURE    back off separation when over budget+stalled
      #   FRS_VLOG_GC_AGE_CUTOFF      vlog-GC relocation cutoff %% (default 0=off)
      -e FRS_VLOG_READER_CACHE_CAP="${FRS_VLOG_READER_CACHE_CAP:-}" \
      -e FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-}" \
      -e FRS_KV_ADAPTIVE_PRESSURE="${FRS_KV_ADAPTIVE_PRESSURE:-}" \
      -e FRS_VLOG_GC_AGE_CUTOFF="${FRS_VLOG_GC_AGE_CUTOFF:-}" \
      # FRS write-amp / disagg lever stack (2026-06-13 PMC-1 V10 local validation):
      # forwarded so the engine inside the TM/JM containers can read them.
      -e FRS_KV_SEPARATION="${FRS_KV_SEPARATION:-}" -e FRS_KV_MIN_BLOB_SIZE="${FRS_KV_MIN_BLOB_SIZE:-}" \
      -e FRS_TRIVIAL_MOVE="${FRS_TRIVIAL_MOVE:-}" -e FRS_REMOTE_COMPACTION="${FRS_REMOTE_COMPACTION:-}" \
      -e FRS_RS_S2_PINNED="${FRS_RS_S2_PINNED:-}" \
      -e FRS_BG_COMPACT_THREADS="${FRS_BG_COMPACT_THREADS:-}" -e FRS_BG_FLUSH_THREADS="${FRS_BG_FLUSH_THREADS:-}" \
      -e FRS_L0_STOP_TRIGGER="${FRS_L0_STOP_TRIGGER:-}" -e FRS_L0_COMPACTION_TRIGGER="${FRS_L0_COMPACTION_TRIGGER:-}" \
      -e FRS_DECAY_DIAG="${FRS_DECAY_DIAG:-}" -e FRS_BULK_SAMPLE="${FRS_BULK_SAMPLE:-}" -e FRS_ITER_DIAG="${FRS_ITER_DIAG:-}" \
      -e FRS_READ_AT_DIAG="${FRS_READ_AT_DIAG:-}" -e FRS_REENTRY_DIAG="${FRS_REENTRY_DIAG:-}" \
      -e FRS_DISABLE_PREFIX_BLOOM="${FRS_DISABLE_PREFIX_BLOOM:-}" -e FRS_GARBAGE_DRAIN_TOMBSTONES="${FRS_GARBAGE_DRAIN_TOMBSTONES:-}" \
      -e FRS_CURSOR_DIAG="${FRS_CURSOR_DIAG:-}" -e FRS_SCAN_STATS="${FRS_SCAN_STATS:-}" \
      -e FRS_RESIDENT_BYPASS="${FRS_RESIDENT_BYPASS:-}" -e FRS_RESIDENT_SHADOW_TOTAL_MB="${FRS_RESIDENT_SHADOW_TOTAL_MB:-}" \
      -e FRS_RS_PARALLEL_EXECUTOR="${FRS_RS_PARALLEL_EXECUTOR:-}" -e FRS_RS_READ_IO_PARALLELISM="${FRS_RS_READ_IO_PARALLELISM:-}" \
      -e FRS_RS_EXECUTOR="${FRS_RS_EXECUTOR:-}" -e FRS_RS_MAX_INFLIGHT_BATCHES="${FRS_RS_MAX_INFLIGHT_BATCHES:-}" \
      -e FRS_RS_PARALLEL_ITER="${FRS_RS_PARALLEL_ITER:-}" -e FRS_ITER_DISPATCH_DIAG="${FRS_ITER_DISPATCH_DIAG:-}" \
      -e FRS_RSS_SAMPLE="${FRS_RSS_SAMPLE:-}" -e FRS_JFR="${FRS_JFR:-}" \
      -e FRS_PERF="${FRS_PERF:-}" -e FRS_PERF_DELAY="${FRS_PERF_DELAY:-}" -e FRS_PERF_DUR="${FRS_PERF_DUR:-}" \
      -e MALLOC_CONF="${MALLOC_CONF:-}" -e _RJEM_MALLOC_CONF="${_RJEM_MALLOC_CONF:-}" \
      -e FRS_MEM_DIAG="${FRS_MEM_DIAG:-}" -e FRS_MEM_DIAG_FILE="${FRS_MEM_DIAG_FILE:-}" \
      -e FRS_DISABLE_MAPSTATE_CACHE="${FRS_DISABLE_MAPSTATE_CACHE:-}" \
      -e FRS_WBM_TOTAL_MB="${FRS_WBM_TOTAL_MB:-}" -e FRS_WBM_STALL="${FRS_WBM_STALL:-}" \
      -e FRS_WBM_HARD_MB="${FRS_WBM_HARD_MB:-}" \
      # LOCAL S3 SIMULATION (tools/nexmark-local): forward the remote-leg
      # bandwidth throttle into the TM/JM containers so the engine inside reads
      # it. Default empty/0 = OFF (byte-identical). Set FRS_REMOTE_BW_MBPS=6250
      # for the 50 Gb/s S3-sim regime. This is the ONLY delta from the canonical
      # scripts/run-8c32g.sh (which does not wire the S3-sim throttle).
      -e FRS_REMOTE_BW_MBPS="${FRS_REMOTE_BW_MBPS:-}" \
    )
    # TOPO=split (2026-06-11 user directive): the 8c/32g budget is TM-ONLY.
    # 2 TM containers x 4c/16g (the measured resource) + 1 JM container 2c/4g
    # (JM + sql-gateway + client — NOT part of the budget). Single-container
    # legacy topology remains TOPO=single (all pre-2026-06-11 pins live there).
    if [ "${TOPO:-single}" = "split" ]; then
      [ -n "${FRS_PERF:-}${FRS_RSS_SAMPLE:-}${FRS_JFR:-}" ] && echo "WARN: FRS_PERF/FRS_RSS_SAMPLE/FRS_JFR not supported under TOPO=split yet — ignored"
      # CONCURRENCY (2026-06-12): every run gets its own cluster namespace —
      # containers, network, /tmp scratch, and Flink conf dir — so multiple
      # NexMark clusters run on one box concurrently (same .so/jar version only:
      # $FLINK/lib is shared). CLUSTER defaults to the run tag.
      CLUSTER="${CLUSTER:-frs-$TAG}"
      NET="$CLUSTER-net"
      # FRS_CTMP_BASE (2026-06-12 R3 box fix): on boxes where frs-tmp is a
      # SEPARATE mount under $WORKENV, dockerd's `-v $WORKENV:$WORKENV` bind
      # does not traverse the submount — the container sees an EMPTY frs-tmp
      # and $CCONF (FLINK_CONF_DIR, resolved by HOST-absolute path inside
      # the containers) is unreachable. Point the cluster scratch base at a
      # dockerd-servable dir via FRS_CTMP_BASE; the base is ALSO bind-mounted
      # directly at its host path (below) so $CCONF resolves regardless of
      # where the base lives — including the default-under-$WORKENV case.
      CTMP_BASE="${FRS_CTMP_BASE:-$WORKENV/frs-tmp}"
      CTMP="$CTMP_BASE/$CLUSTER"
      CCONF="$CTMP/flink-conf"
      mkdir -p "$CTMP"
      rm -rf "$CCONF" && cp -r "$FLINK/conf" "$CCONF"
      # /tmp = the per-cluster scratch; the direct base mount makes the
      # host-absolute $CCONF path (and any other $CTMP path the harness
      # passes through env) visible inside the containers.
      SPLIT_TMP=(-v "$CTMP:/tmp" -v "$CTMP_BASE:$CTMP_BASE")
      # NETWORK (2026-06-12 R3 box fix): plain `docker network create` can
      # fail when the daemon's default-address-pools are exhausted or
      # conflict with host routes ("could not find an available,
      # non-overlapping IPv4 address pool") — the old `|| true` swallowed
      # that, the containers fell back to the default bridge, and
      # $CLUSTER-jm never resolved. Try the plain create first (unchanged
      # behavior on healthy boxes); on failure retry with an explicit
      # subnet: FRS_NET_SUBNET (full CIDR override) if set, else
      # FRS_NET_SUBNET_BASE (first two octets, default 172.99) with a
      # per-cluster third octet derived from the cluster name (cksum),
      # probing a few successors on residual collisions. Hard-fail if no
      # subnet works — a cluster on the wrong network wedges silently.
      if ! docker network inspect "$NET" >/dev/null 2>&1 \
         && ! docker network create "$NET" >/dev/null 2>&1; then
        if [ -n "${FRS_NET_SUBNET:-}" ]; then
          docker network create --subnet "$FRS_NET_SUBNET" "$NET" >/dev/null \
            || { echo "FATAL: docker network create $NET --subnet $FRS_NET_SUBNET failed"; exit 1; }
        else
          NET_BASE="${FRS_NET_SUBNET_BASE:-172.99}"
          NET_OCT=$(( $(printf '%s' "$CLUSTER" | cksum | cut -d' ' -f1) % 240 ))
          NET_OK=
          for NET_TRY in 0 1 2 3 4 5 6 7; do
            if docker network create --subnet "$NET_BASE.$(( (NET_OCT + NET_TRY) % 240 )).0/24" "$NET" >/dev/null 2>&1; then
              NET_OK=1; break
            fi
          done
          [ -n "$NET_OK" ] || { echo "FATAL: docker network create $NET failed even with explicit --subnet ($NET_BASE.x.0/24; set FRS_NET_SUBNET to force one)"; exit 1; }
        fi
      fi
      docker rm -f "$CLUSTER-jm" "$CLUSTER-tm1" "$CLUSTER-tm2" >/dev/null 2>&1 || true
      # TM JVM allocator: jemalloc via LD_PRELOAD inside the (Linux) container.
      # PORTABLE: the preload .so is a CONTAINER path (the image's bundled
      # libjemalloc-preload.so), independent of the host OS — but it can be
      # overridden per box via FRS_JEMALLOC_SO (e.g. the origin box's
      # /usr/lib/x86_64-linux-gnu/libjemalloc.so.2 if the image lacks the bundle).
      # Default ON for Linux hosts; OFF on macOS (the known jemalloc macOS TSD
      # crash — see MEMORY.md "jemalloc macOS TSD crash"). Note the preload runs
      # INSIDE the Linux container even on macOS, but we keep the macOS default
      # OFF to mirror the documented dev-box behavior. Set FRS_TM_JEMALLOC=1/0
      # to force. The engine's own jemalloc is statically bundled in the .so and
      # unaffected (prefixed symbols).
      FRS_TM_JEMALLOC_DEFAULT=1; [ "$OS" = "Darwin" ] && FRS_TM_JEMALLOC_DEFAULT=0
      TM_PRELOAD=()
      [ "${FRS_TM_JEMALLOC:-$FRS_TM_JEMALLOC_DEFAULT}" = "1" ] \
        && TM_PRELOAD=(-e "LD_PRELOAD=${FRS_JEMALLOC_SO:-/usr/local/lib/libjemalloc-preload.so}")
      # Split TM/JM resource profile (parameterized; sane 8c/32g default = 2 TM
      # 4c/16g + 1 JM 2c/4g). The origin box may have different core/RAM counts;
      # override SPLIT_TM_CPUS / SPLIT_TM_MEM / SPLIT_JM_CPUS / SPLIT_JM_MEM.
      SPLIT_TM_CPUS="${SPLIT_TM_CPUS:-4}"; SPLIT_TM_MEM="${SPLIT_TM_MEM:-16g}"
      SPLIT_JM_CPUS="${SPLIT_JM_CPUS:-2}"; SPLIT_JM_MEM="${SPLIT_JM_MEM:-4g}"
      for i in 1 2; do
        docker run -d --name "$CLUSTER-tm$i" --network "$NET" --cpus="$SPLIT_TM_CPUS" --memory="$SPLIT_TM_MEM" --memory-swap="$SPLIT_TM_MEM" \
          ${TM_PRELOAD[@]+"${TM_PRELOAD[@]}"} ${URING_OPTS[@]+"${URING_OPTS[@]}"} \
          "${DKR_COMMON[@]}" "${SPLIT_TMP[@]}" "${ENVS[@]}" -e FLINK_CONF_DIR="$CCONF" "$IMG" bash -lc "
            mkdir -p /usr/local/lib && cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
            cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
            cp '$BOS_HADOOP_FS_JAR' '$FLINK/lib/' &&
            exec bash '$FLINK/bin/taskmanager.sh' start-foreground
          " >/dev/null
      done
      docker run --rm --name "$CLUSTER-jm" --network "$NET" --cpus="$SPLIT_JM_CPUS" --memory="$SPLIT_JM_MEM" \
        "${DKR_COMMON[@]}" "${SPLIT_TMP[@]}" "${ENVS[@]}" -e CLUSTER_MODE=external -e EXPECT_TMS=2 -e JM_HOST="$CLUSTER-jm" -e FLINK_CONF_DIR="$CCONF" \
        "$IMG" bash -lc "
          mkdir -p /usr/local/lib && cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
          cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
          cp '$BOS_HADOOP_FS_JAR' '$FLINK/lib/' &&
          nproc && free -g | head -2 &&
          bash scripts/measure-sql.sh
        " 2>&1 | tee "$OUT"
      for i in 1 2; do
        docker logs "$CLUSTER-tm$i" 2>/dev/null | grep -h 'STREAM_STATS\|DIAG_COMPLETION' | tail -6
      done
      docker rm -f "$CLUSTER-tm1" "$CLUSTER-tm2" >/dev/null 2>&1 || true
      docker network rm "$NET" >/dev/null 2>&1 || true
      echo "--- RESULT line ---"; grep -E 'RESULT:|MAXSEC' "$OUT" | tail -1
      exit 0
    fi
    # SINGLE-TM resource budget (TOPO=single). Default = the canonical 8c/32g.
    # q9 KV-sep OOM fix (2026-06-15 PMC-1): a BIGGER single-TM profile gives q9
    # far more PER-TM headroom than the 16 g-capped TOPO=split TMs. The
    # `q9-36g` profile (see run-best.sh / docs) uses 8c/36g. Size via
    # SINGLE_TM_CPUS / SINGLE_TM_MEM; both default per the 8c/32g budget and
    # are checked against DETECTED physical RAM below (no 64GiB hardcode).
    SINGLE_TM_CPUS="${SINGLE_TM_CPUS:-8}"
    SINGLE_TM_MEM="${SINGLE_TM_MEM:-32g}"
    # RAM headroom sanity check: warn (don't fail) if the requested container
    # memory + ~4 GiB OS/Docker-VM headroom exceeds detected physical RAM.
    if [ "${PHYS_RAM_MIB:-0}" -gt 0 ]; then
      REQ_MIB="$(mem_to_mib "$SINGLE_TM_MEM")"
      if [ $(( REQ_MIB + 4096 )) -gt "$PHYS_RAM_MIB" ]; then
        echo "WARN: SINGLE_TM_MEM=$SINGLE_TM_MEM (${REQ_MIB} MiB) + 4 GiB headroom > detected RAM ${PHYS_RAM_MIB} MiB."
        echo "      The container may swap/OOM. Lower SINGLE_TM_MEM or run on a bigger box."
      fi
    fi
    # jemalloc preload for the single TM too (parameterized; default ON for
    # Linux hosts, OFF on macOS — same policy as the split path).
    FRS_TM_JEMALLOC_DEFAULT=1; [ "$OS" = "Darwin" ] && FRS_TM_JEMALLOC_DEFAULT=0
    SINGLE_PRELOAD=()
    [ "${FRS_TM_JEMALLOC:-$FRS_TM_JEMALLOC_DEFAULT}" = "1" ] \
      && SINGLE_PRELOAD=(-e "LD_PRELOAD=${FRS_JEMALLOC_SO:-/usr/local/lib/libjemalloc-preload.so}")
    echo "== TOPO=single resources: --cpus=$SINGLE_TM_CPUS --memory=$SINGLE_TM_MEM (detected RAM ${PHYS_RAM_MIB:-?} MiB) =="
    docker run --rm --cpus="$SINGLE_TM_CPUS" --memory="$SINGLE_TM_MEM" --memory-swap="$SINGLE_TM_MEM" ${PERF_OPTS[@]+"${PERF_OPTS[@]}"} ${URING_OPTS[@]+"${URING_OPTS[@]}"} ${SINGLE_PRELOAD[@]+"${SINGLE_PRELOAD[@]}"} "${DKR_COMMON[@]}" "${TMP_MOUNT[@]}" \
      "${ENVS[@]}" \
      "$IMG" bash -lc "
        cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
        mkdir -p /usr/local/lib && cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
        cp '$BOS_HADOOP_FS_JAR' '$FLINK/lib/' &&
        nproc && free -g | head -2 &&
        if [ -n \"\$FRS_RSS_SAMPLE\" ]; then ( RL=$REPO/target-linux/rss-\$QUERY.log; : > \$RL; while :; do p=\$(for j in \$(pgrep -x java); do echo \"\$(awk '/VmRSS/{print \$2}' /proc/\$j/status 2>/dev/null) \$j\"; done | sort -rn | head -1 | awk '{print \$2}'); if [ -n \"\$p\" ]; then a=\$(awk '/^Anonymous:/{an+=\$2} /^Rss:/{r+=\$2} END{print r,an}' /proc/\$p/smaps 2>/dev/null); echo \"t=\$(date +%s) pid=\$p rssKB_anonKB=\$a vmRSS=\$(awk '/VmRSS/{print \$2}' /proc/\$p/status 2>/dev/null)\" >> \$RL; n=\$((\${n:-0}+1)); if [ \$((n % 6)) -eq 0 ]; then echo \"--- NMT t=\$(date +%s) ---\" >> \$RL.nmt; jcmd \$p VM.native_memory summary 2>/dev/null | grep -E \"Total:|reserved=|- \" | head -40 >> \$RL.nmt; fi; fi; sleep 5; done & ) ; fi
        if [ -n \"\$FRS_PERF\" ]; then ( apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq linux-tools-generic >/dev/null 2>&1; PF=\$(find /usr/lib/linux-tools* -name perf 2>/dev/null | head -1); sleep \"\${FRS_PERF_DELAY:-700}\"; p=\$(for j in \$(pgrep -x java); do echo \"\$(awk '/VmRSS/{print \$2}' /proc/\$j/status 2>/dev/null) \$j\"; done | sort -rn | head -1 | awk '{print \$2}'); if [ -n \"\$p\" ] && [ -n \"\$PF\" ]; then echo \"=== FRS_PERF start pid=\$p dur=\${FRS_PERF_DUR:-180}s ===\"; \$PF record -F 199 -g --call-graph fp -o /tmp/\$QUERY-perf.data -p \$p -- sleep \${FRS_PERF_DUR:-180} 2>&1 | tail -2; \$PF report -i /tmp/\$QUERY-perf.data --stdio --no-children --percent-limit 0.3 > $REPO/target-linux/perf-\$QUERY-flat.txt 2>/dev/null; \$PF report -i /tmp/\$QUERY-perf.data --stdio --children --percent-limit 1 > $REPO/target-linux/perf-\$QUERY-graph.txt 2>/dev/null; echo \"=== FRS_PERF done ===\"; fi ) & fi
        if [ -n \"\$FRS_JFR\" ]; then ( sleep \"\${FRS_JFR_DELAY:-180}\"; p=\$(for j in \$(pgrep -x java); do echo \"\$(awk '/VmRSS/{print \$2}' /proc/\$j/status 2>/dev/null) \$j\"; done | sort -rn | head -1 | awk '{print \$2}'); if [ -n \"\$p\" ]; then echo \"=== FRS_JFR start pid=\$p dur=\${FRS_JFR_DUR:-120}s ===\"; /opt/java/openjdk/bin/jcmd \$p JFR.start duration=\${FRS_JFR_DUR:-120}s filename=/tmp/\$QUERY-flame.jfr settings=profile 2>&1; fi ) & fi
        rm -f '$FLINK'/log/*taskexecutor*.out '$FLINK'/log/*taskexecutor*.log 2>/dev/null || true
        bash scripts/measure-sql.sh
        grep -h 'STREAM_STATS\|DIAG_COMPLETION' '$FLINK'/log/*taskexecutor*.out 2>/dev/null | tail -6
      " 2>&1 | tee "$OUT"
    echo "--- RESULT line ---"; grep -E 'RESULT:|MAXSEC' "$OUT" | tail -1
    ;;
  jar)
    echo "== rebuild + redeploy forst-rs jar on host (mounted into container) =="
    # The flink checkout sits ALONGSIDE the engine repo by default (REPO/../flink);
    # override with FLINK_SRC. JAVA_HOME for the host maven build is OS-detected
    # (macOS: java_home -v 25; Linux: $JAVA_HOME or a common JDK path) and can be
    # forced via JAVA25_HOME.
    FLINK_SRC="${FLINK_SRC:-$REPO/../flink}"
    SB_DIR="$FLINK_SRC/flink-state-backends/flink-statebackend-forst-rs"
    [ -d "$SB_DIR" ] || { echo "FATAL: state-backend source not found at $SB_DIR (set FLINK_SRC)"; exit 1; }
    if [ -n "${JAVA25_HOME:-}" ]; then
      J25="$JAVA25_HOME"
    elif [ "$OS" = "Darwin" ]; then
      J25="$(/usr/libexec/java_home -v 25 2>/dev/null || echo /Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home)"
    else
      J25="${JAVA_HOME:-/usr/lib/jvm/java-25-openjdk-amd64}"
    fi
    cd "$SB_DIR" &&
    JAVA_HOME="$J25" "$FLINK_SRC/mvnw" -o -q -DskipTests \
      -Denforcer.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true \
      -Dmaven.javadoc.skip=true clean package &&
    cp target/flink-statebackend-forst-rs-2.2.0.jar "$FLINK/lib/" && echo "jar redeployed"
    ;;
  *) echo "unknown: $cmd"; exit 1 ;;
esac
