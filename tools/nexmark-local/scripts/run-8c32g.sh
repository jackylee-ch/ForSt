#!/usr/bin/env bash
# 8c/32g TRUE-resource benchmark driver — PORTABLE across macOS (Apple Silicon
# dev box, arm64 container) AND the origin x86_64 Linux box (amd64 container).
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
case "${PLAT:-}" in
  linux/amd64) JDK17_ARCH=amd64 ;;
  linux/arm64) JDK17_ARCH=arm64 ;;
  *)
    case "$OS" in
      Linux) JDK17_ARCH=amd64 ;;
      *) JDK17_ARCH=arm64 ;;
    esac
    ;;
esac

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
  IMG="${IMG:-forst-bench:x86}"
  PLAT="${PLAT:-linux/amd64}"
fi
FLINK="${FLINK:-$WORKENV/flink-2.2.1}"

DKR_COMMON=()
if [ -n "${PLAT:-}" ] && [ "$PLAT" != "auto" ]; then
  DKR_COMMON+=(--platform "$PLAT")
fi
DKR_COMMON+=(
  -v "$REPO:$REPO" -v "$WORKENV:$WORKENV"
  # FRS-SCRATCH (2026-06-08): the container's /tmp is a ~59 GB overlay on Docker.raw
  # (the Docker-Desktop VM disk), NOT the host's 425 GB volume. q9/q20/q4 write
  # ~36-45 GB of SST data + cache + checkpoint-noflush memtable artifacts to /tmp and
  # fill that 59 GB → "No space left on device" → job crash (mis-read earlier as OOM).
  # Bind-mount /tmp to a host dir on the big volume so all engine scratch uses it.
  -v forst-cargo:/cargo-cache
  -e CARGO_HOME=/cargo-cache
  -e REPO="$REPO"
  -e WORKENV="$WORKENV"
  -e FLINK="$FLINK"
  -e FLINK_HOME="$FLINK"
  # JDK17 path INSIDE the container depends on the container arch (the .deb
  # package suffix), not the host: arm64 image -> ...-arm64, amd64 -> ...-amd64.
  -e JDK17="${JDK17_IN_IMG:-/usr/lib/jvm/java-17-openjdk-$JDK17_ARCH}"
  -e JDK25=/opt/java/openjdk
  -e TEMPLATES="${TEMPLATES:-$REPO/scripts/templates-linux}"
  -e HADOOP_HOME="$WORKENV/hadoop-3.4.3"
  -e NEXMARK_HOME="${NEXMARK_HOME:-$REPO/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink}"
  -w "$REPO")

# /tmp mount is per-invocation: single/build use the shared frs-tmp; TOPO=split
# namespaces it per-cluster (CONCURRENT NexMark clusters, 2026-06-12 directive).
TMP_BASE="${FRS_CTMP_BASE:-$WORKENV/frs-tmp}"
mkdir -p "$TMP_BASE"
TMP_MOUNT=(-v "$TMP_BASE:/tmp" -v "$TMP_BASE:$TMP_BASE")

cmd="${1:?build|run|jar}"; shift || true

case "$cmd" in
  build)
    echo "== building forst-rs Linux .so into target-linux/release =="
    docker run --rm "${DKR_COMMON[@]}" "${TMP_MOUNT[@]}" -e CARGO_TARGET_DIR="$REPO/target-linux" "$IMG" \
      bash -lc 'cargo build --release -p forst-rs-ffi && ls -la target-linux/release/libforst_rs_ffi.so'
    ;;
  run)
    Q="${1:?query}"; CFG="${2:?config}"; MS="${3:-900}"; TAG="${4:-c32}"
    SO="$REPO/target-linux/release/libforst_rs_ffi.so"
    [ -f "$SO" ] || { echo "missing $SO — run: $0 build"; exit 1; }
    RUN_CLUSTER="${CLUSTER:-${NEXMARK_NAMESPACE:-frs}-$TAG}"
    RUN_TMP="$TMP_BASE/$RUN_CLUSTER"
    mkdir -p "$RUN_TMP"
    RUN_TMP_MOUNT=(-v "$RUN_TMP:/tmp" -v "$TMP_BASE:$TMP_BASE")
    OUT="/tmp/${TAG}-$Q-$CFG.out"
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
      -e S3_ENDPOINT=x -e S3_ACCESS_KEY=x -e S3_SECRET_KEY=x -e S3_BUCKET=x -e S3_REGION=x -e S3_PREFIX=x \
      # JVM process.size overrides consumed by measure-sql.sh INSIDE the container.
      # Default unset => the templates' value applies (10240m for the q9 16g/TM
      # native-headroom carve-out, §3a). Forward an override so callers can retune.
      -e FRS_TM_PROCESS_SIZE="${FRS_TM_PROCESS_SIZE:-}" -e FRS_JM_PROCESS_SIZE="${FRS_JM_PROCESS_SIZE:-}" \
      -e FRS_FLINK_PARALLELISM="${FRS_FLINK_PARALLELISM:-}" -e FRS_TM_SLOTS="${FRS_TM_SLOTS:-}" \
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
      -e FRS_VLOG_POINT_DEREF="${FRS_VLOG_POINT_DEREF:-}" \
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
      # FULL-STACK-ON validation levers (Phase-1 query-perf, run-best.sh validate).
      # All default-OFF in the engine; forwarded so the validate profile can turn
      # them ON inside the TM/JM containers. Empty => engine default (OFF).
      #   FRS_S2_FANOUT_MIN            adaptive S2 loser-tree threshold (R1; usize, default OFF) db.rs:15601
      #   FRS_RS_PROBE_BLOOM_PRUNE     metadata-resident probe bloom prune (MR-1)               db.rs:677
      #   FRS_RS_LEVELED_HOT_CF        leveled-bottom discipline on hot probe CFs (Approach-1)  db.rs:712
      #   FRS_RS_LEVELED_HOT_CF_FANOUT_MIN / _L0_TRIGGER   Approach-1 tuning (defaults 8 / 4)   db.rs:743/755
      #   FRS_PERSISTENT_PROBE_ITER    reusable per-(CF,version) probe iterator (Approach-1)     db.rs:16850
      #   FRS_VLOG_RESIDENT_BUDGET_MB  KV-sep resident vlog byte budget (already wired below via the q9 block; re-listed for the join validate stack)
      -e FRS_S2_FANOUT_MIN="${FRS_S2_FANOUT_MIN:-}" \
      -e FRS_RS_PROBE_BLOOM_PRUNE="${FRS_RS_PROBE_BLOOM_PRUNE:-}" \
      -e FRS_RS_LEVELED_HOT_CF="${FRS_RS_LEVELED_HOT_CF:-}" \
      -e FRS_RS_LEVELED_HOT_CF_FANOUT_MIN="${FRS_RS_LEVELED_HOT_CF_FANOUT_MIN:-}" \
      -e FRS_RS_LEVELED_HOT_CF_L0_TRIGGER="${FRS_RS_LEVELED_HOT_CF_L0_TRIGGER:-}" \
      -e FRS_PERSISTENT_PROBE_ITER="${FRS_PERSISTENT_PROBE_ITER:-}" \
      # Approach-2 / OPT-N04 backend merge-RMW (windowed/OVER). Engine + backend
      # (jar) side; default-OFF. FRS_RS_MERGE_RMW master gate, _STATES per-state
      # opt-in list, _CHAIN_REBASE max merge-chain before rebase (default 4096).
      -e FRS_RS_MERGE_RMW="${FRS_RS_MERGE_RMW:-}" \
      -e FRS_RS_MERGE_RMW_STATES="${FRS_RS_MERGE_RMW_STATES:-}" \
      -e FRS_RS_MERGE_CHAIN_REBASE="${FRS_RS_MERGE_CHAIN_REBASE:-}" \
      -e FRS_BG_COMPACT_THREADS="${FRS_BG_COMPACT_THREADS:-}" -e FRS_BG_FLUSH_THREADS="${FRS_BG_FLUSH_THREADS:-}" \
      -e FRS_L0_STOP_TRIGGER="${FRS_L0_STOP_TRIGGER:-}" -e FRS_L0_COMPACTION_TRIGGER="${FRS_L0_COMPACTION_TRIGGER:-}" \
      -e FRS_L0_SLOWDOWN_TRIGGER="${FRS_L0_SLOWDOWN_TRIGGER:-}" \
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
      -e FRS_DYNAMIC_SHED="${FRS_DYNAMIC_SHED:-}" -e FRS_DYN_SHED_INTERVAL_MS="${FRS_DYN_SHED_INTERVAL_MS:-}" \
      -e FRS_MEM_BUDGET_MB="${FRS_MEM_BUDGET_MB:-}" \
      # FRS-MEM-PRESSURE-PURGE (2026-06-16 PMC-1): arm ONLY the proactive
      # jemalloc purge valve (no lever shedding). Default-OFF; forwarded so the
      # never-OOM valve can be armed standalone inside the TM/JM containers.
      -e FRS_MEM_PRESSURE_PURGE="${FRS_MEM_PRESSURE_PURGE:-}" \
      # FRS-MEM-PRESSURE-PURGE build-peak threshold (2026-06-17 PMC-1 q9-purge):
      # the lowest pressure level at which the proactive purge fires.
      # critical|high|elevated; default elevated (≥0.75) when the valve is armed
      # so the ~5 GiB MADV_FREE/dirty join-build transient is returned to the OS
      # BEFORE the sub-second build-peak spike crosses the 16 g cgroup cliff.
      -e FRS_MEM_PURGE_AT="${FRS_MEM_PURGE_AT:-}" \
      # FRS-MEM-MANAGER (2026-06-16 PMC-1): the UNIFIED engine-native memory
      # controller. FRS_MEM_MANAGER=1 arms it (default-OFF, byte-identical when
      # off). It reads the cgroup limit (FRS_MEM_CGROUP_MB override else
      # /sys/fs/cgroup/memory.max), subtracts the JVM reservation
      # (FRS_JVM_RESERVED_MB == process.size), FFM (FRS_FFM_RESERVED_MB) and
      # headroom (FRS_MEM_HEADROOM_MB floor), derives ONE engine-native budget and
      # splits it across block-cache / WBM / shadow / vlog / compaction so their
      # SUM is bounded and every cap AUTO-SCALES with the configured TM size.
      # FRS_MEM_INSTANCES = assumed co-resident DB count for the per-instance
      # block-cache slice. Forwarded so the controller engages in the TM/JM.
      -e FRS_MEM_MANAGER="${FRS_MEM_MANAGER:-}" -e FRS_MEM_CGROUP_MB="${FRS_MEM_CGROUP_MB:-}" \
      -e FRS_JVM_RESERVED_MB="${FRS_JVM_RESERVED_MB:-}" -e FRS_FFM_RESERVED_MB="${FRS_FFM_RESERVED_MB:-}" \
      -e FRS_MEM_HEADROOM_MB="${FRS_MEM_HEADROOM_MB:-}" -e FRS_MEM_INSTANCES="${FRS_MEM_INSTANCES:-}" \
      # FRS-MEM-JEMALLOC-OFF-RESERVE (2026-06-17 PMC-1 q9 p8 OOM fix): forward the
      # operator's FRS_TM_JEMALLOC choice INTO the container so the engine
      # controller can tell whether the JVM-side native malloc is eager (jemalloc
      # preloaded) or plain glibc (retains freed arenas). When FRS_TM_JEMALLOC=0
      # the controller reserves a glibc-retention cushion (shrinks engine-native)
      # so the SUM stays under the cgroup despite the retained JVM-side off-heap.
      # FRS_MEM_JEMALLOC_OFF_RESERVE_MB overrides the derived cushion (0 disables).
      -e FRS_TM_JEMALLOC="${FRS_TM_JEMALLOC:-}" \
      -e FRS_MEM_JEMALLOC_OFF_RESERVE_MB="${FRS_MEM_JEMALLOC_OFF_RESERVE_MB:-}" \
      # FRS_FFM_DIAG (2026-06-16 PMC-1): periodic dump of the bounded FFM
      # off-heap working set (columnar/GET-out/iter-scratch + freed-on-grow).
      -e FRS_FFM_DIAG="${FRS_FFM_DIAG:-}" \
      -e FRS_DISABLE_MAPSTATE_CACHE="${FRS_DISABLE_MAPSTATE_CACHE:-}" \
      -e FRS_WBM_TOTAL_MB="${FRS_WBM_TOTAL_MB:-}" -e FRS_WBM_STALL="${FRS_WBM_STALL:-}" \
      -e FRS_WBM_HARD_MB="${FRS_WBM_HARD_MB:-}" \
      -e FRS_SCAN_OPEN_FANOUT="${FRS_SCAN_OPEN_FANOUT:-}" -e FRS_SCAN_COLD_PRIME="${FRS_SCAN_COLD_PRIME:-}" \
      -e FRS_S2_FANOUT_MIN="${FRS_S2_FANOUT_MIN:-}" \
      -e FRS_VLOG_GC_ADAPTIVE="${FRS_VLOG_GC_ADAPTIVE:-}" -e FRS_VLOG_GC_ADAPTIVE_CUTOFF="${FRS_VLOG_GC_ADAPTIVE_CUTOFF:-}" \
      # L4 compaction-input windowed reads (runtime_tuning.rs): bound the
      # compaction-input transient (double-buffered, cache-skip) so the
      # end-of-run compaction storm can't spike one TM over its cgroup. Default
      # empty=OFF (byte-identical). FRS_COMPACT_WINDOWED=1 turns it on.
      -e FRS_COMPACT_WINDOWED="${FRS_COMPACT_WINDOWED:-}" -e FRS_COMPACT_WINDOW_BYTES="${FRS_COMPACT_WINDOW_BYTES:-}" \
      -e FRS_COMPACT_PREFETCH_BUDGET="${FRS_COMPACT_PREFETCH_BUDGET:-}" -e FRS_COMPACT_PARALLEL="${FRS_COMPACT_PARALLEL:-}" \
      -e FRS_CACHE_ADMISSION="${FRS_CACHE_ADMISSION:-}" -e FRS_CACHE_BG_EXEMPT="${FRS_CACHE_BG_EXEMPT:-}" \
      -e FRS_CACHE_ADMISSION_EPOCH="${FRS_CACHE_ADMISSION_EPOCH:-}" -e FRS_CACHE_SPACE_LIMIT_MB="${FRS_CACHE_SPACE_LIMIT_MB:-}" \
      -e FRS_IO_URING="${FRS_IO_URING:-}" -e FRS_RS_INLINE_MAX="${FRS_RS_INLINE_MAX:-}" \
      -e FRS_RS_SYNC_DIRECT="${FRS_RS_SYNC_DIRECT:-}" \
      -e FRS_RS_MIXED_BATCH="${FRS_RS_MIXED_BATCH:-}" -e FRS_TIMER_INDEX_MAX="${FRS_TIMER_INDEX_MAX:-}" \
      -e FRS_RS_BLOCK_PREFETCH="${FRS_RS_BLOCK_PREFETCH:-}" -e FRS_RS_PREFETCH_THREADS="${FRS_RS_PREFETCH_THREADS:-}" \
      -e FRS_PERSISTENT_PROBE_ITER="${FRS_PERSISTENT_PROBE_ITER:-}" \
      -e FRS_LIFECYCLE_COHORT_TRIGGER="${FRS_LIFECYCLE_COHORT_TRIGGER:-}" \
      -e FRS_LIFECYCLE_DROP_IGNORE_SNAPSHOTS="${FRS_LIFECYCLE_DROP_IGNORE_SNAPSHOTS:-}" \
      -e FRS_LIFECYCLE_SEGMENTS="${FRS_LIFECYCLE_SEGMENTS:-}" -e FRS_LIFECYCLE_STAMPED_CEILING="${FRS_LIFECYCLE_STAMPED_CEILING:-}" \
      -e FRS_CKPT_LINK_MODE="${FRS_CKPT_LINK_MODE:-}" -e FRS_CKPT_PARALLEL_UPLOAD="${FRS_CKPT_PARALLEL_UPLOAD:-}" \
      -e FRS_COMPACT_CONCURRENT="${FRS_COMPACT_CONCURRENT:-}" -e FRS_COMPACT_DIAG="${FRS_COMPACT_DIAG:-}" \
      -e FRS_COMPACT_DRAIN_L1="${FRS_COMPACT_DRAIN_L1:-}" -e FRS_COMPACT_RELEASE_LOCK="${FRS_COMPACT_RELEASE_LOCK:-}" \
      -e FRS_REMOTE_COMPACTION_SERIALIZE="${FRS_REMOTE_COMPACTION_SERIALIZE:-}" -e FRS_REMOTE_NONSST_LOCAL="${FRS_REMOTE_NONSST_LOCAL:-}" \
      -e FRS_RESIDENT_BLOOM_SKIP="${FRS_RESIDENT_BLOOM_SKIP:-}" -e FRS_RESIDENT_SHADOW="${FRS_RESIDENT_SHADOW:-}" \
      -e FRS_RESIDENT_SHADOW_MB="${FRS_RESIDENT_SHADOW_MB:-}" \
      -e FRS_RESTORE_BG_FILL="${FRS_RESTORE_BG_FILL:-}" -e FRS_RESTORE_BG_FILL_PACE_MB="${FRS_RESTORE_BG_FILL_PACE_MB:-}" \
      -e FRS_RESTORE_BG_FILL_WORKERS="${FRS_RESTORE_BG_FILL_WORKERS:-}" \
      -e S3_DIR="${S3_DIR:-}" -e LOCAL_DIR="${LOCAL_DIR:-}" \
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
      CLUSTER="${CLUSTER:-${NEXMARK_NAMESPACE:-frs}-$TAG}"
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
      # FRS-SCRATCH-TRAP (2026-06-17 PMC-1): the per-cluster scratch ($CTMP)
      # accumulates the engine's SST/cache/checkpoint-noflush artifacts (~36-45
      # GB/query). If a run is killed (exit-137 OOM, SIGINT, crash) BEFORE the
      # normal teardown below, the old code left $CTMP behind — repeated runs
      # piled up frs-tmp until the host volume hit 100% (347 GB observed). This
      # trap tears down the cluster (containers + network) AND rm -rf's the
      # per-cluster scratch on ANY exit (success OR crash/kill), so frs-tmp
      # stays bounded. It removes ONLY this run's $CTMP, never the shared base.
      # Set FRS_KEEP_SCRATCH=1 to retain $CTMP for post-mortem (containers still
      # torn down). The trap fires once (cleared at the start to be idempotent).
      _frs_cleanup() {
        trap - EXIT INT TERM
        docker rm -f "$CLUSTER-jm" "$CLUSTER-tm1" "$CLUSTER-tm2" >/dev/null 2>&1 || true
        docker network rm "$NET" >/dev/null 2>&1 || true
        if [ "${FRS_KEEP_SCRATCH:-0}" = "1" ]; then
          echo "FRS_KEEP_SCRATCH=1 — retaining cluster scratch $CTMP"
        elif [ -n "${CTMP:-}" ] && [ "$CTMP" != "$CTMP_BASE" ] && [ -d "$CTMP" ]; then
          echo "trap: rm -rf cluster scratch $CTMP"
          rm -rf "$CTMP" || true
        fi
      }
      trap _frs_cleanup EXIT INT TERM
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
      JM_ALIAS="${JM_ALIAS:-jm}"
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
      TM_JEMALLOC_EFF="${FRS_TM_JEMALLOC:-$FRS_TM_JEMALLOC_DEFAULT}"
      # FRS-MEM-JEMALLOC-OFF-FORCE (2026-06-17 PMC-1 q9 p8 OOM fix): the engine
      # mem-manager bounds engine-native allocations, but it CANNOT bound the
      # JVM-side native off-heap (FFM/Panama state buffers + AEC in-flight) when
      # that runs on plain glibc malloc — glibc RETAINS freed arenas, so with
      # FRS_TM_JEMALLOC=0 the JVM-side RSS balloons ~2 GiB past process.size and a
      # TM crests the 16g cgroup at the q9 p8 join peak (reproduced: a TM RESTARTed
      # at ~59M). The controller's glibc-retention cushion helps but at p8 the
      # JVM-side retention (~12 GiB) is too large to fully offset by shrinking the
      # engine alone. The decisive never-OOM fix is to give the JVM an allocator
      # that RETURNS pages: when the mem-manager is armed (FRS_MEM_MANAGER=1) we
      # FORCE the eager-decay jemalloc preload over the JVM even if the operator
      # set FRS_TM_JEMALLOC=0 (WARN), because the controller's never-OOM guarantee
      # depends on the JVM-side actually returning freed memory. Inside the (Linux)
      # container the preload is always valid (the macOS TSD-crash caveat is a
      # host-allocator concern, not the containerized JVM). Set
      # FRS_TM_JEMALLOC_ALLOW_OFF=1 to honour an explicit OFF anyway (then the
      # engine cushion is the only defense — slower, marginal at p8).
      if [ "$TM_JEMALLOC_EFF" != "1" ] \
         && [ "${FRS_MEM_MANAGER:-}" = "1" ] \
         && [ "${FRS_TM_JEMALLOC_ALLOW_OFF:-0}" != "1" ]; then
        echo "WARN: FRS_TM_JEMALLOC=0 with FRS_MEM_MANAGER=1 — FORCING eager jemalloc on the TM JVM (the controller cannot bound glibc retention; see FRS-MEM-JEMALLOC-OFF-FORCE). Set FRS_TM_JEMALLOC_ALLOW_OFF=1 to override."
        TM_JEMALLOC_EFF=1
      fi
      # When the JVM-side jemalloc preload IS on, make it EAGER-decay (return freed
      # pages to the OS promptly) — matching the engine's own compiled-in
      # dirty_decay_ms:0,muzzy_decay_ms:0. Without this the preload used jemalloc's
      # non-eager defaults, so even jemalloc-ON retained freed JVM-side off-heap.
      # An explicit MALLOC_CONF (operator pin) still wins.
      TM_MALLOC_CONF_EFF="${MALLOC_CONF:-}"
      if [ "$TM_JEMALLOC_EFF" = "1" ]; then
        TM_PRELOAD=(-e "LD_PRELOAD=${FRS_JEMALLOC_SO:-/usr/local/lib/libjemalloc-preload.so}")
        # Eager-decay config for the JVM-side jemalloc. NOTE: the ENVS array also
        # forwards `-e MALLOC_CONF=${MALLOC_CONF:-}` (possibly empty) AFTER
        # TM_PRELOAD in the docker run, so a MALLOC_CONF placed in TM_PRELOAD would
        # be CLOBBERED by that empty later one. We therefore carry the effective
        # value here and re-apply it as a LATER `-e` (after ENVS) on the TM run so
        # the eager config actually wins. Operator MALLOC_CONF pin still wins.
        [ -z "$TM_MALLOC_CONF_EFF" ] \
          && TM_MALLOC_CONF_EFF="background_thread:true,dirty_decay_ms:0,muzzy_decay_ms:0"
      fi
      # Split TM/JM resource profile (parameterized; sane 8c/32g default = 2 TM
      # 4c/16g + 1 JM 2c/4g). The origin box may have different core/RAM counts;
      # override SPLIT_TM_CPUS / SPLIT_TM_MEM / SPLIT_JM_CPUS / SPLIT_JM_MEM.
      SPLIT_TM_CPUS="${SPLIT_TM_CPUS:-4}"; SPLIT_TM_MEM="${SPLIT_TM_MEM:-16g}"
      SPLIT_JM_CPUS="${SPLIT_JM_CPUS:-2}"; SPLIT_JM_MEM="${SPLIT_JM_MEM:-4g}"
      for i in 1 2; do
        docker run -d --name "$CLUSTER-tm$i" --network "$NET" --cpus="$SPLIT_TM_CPUS" --memory="$SPLIT_TM_MEM" --memory-swap="$SPLIT_TM_MEM" \
          ${TM_PRELOAD[@]+"${TM_PRELOAD[@]}"} ${URING_OPTS[@]+"${URING_OPTS[@]}"} \
          "${DKR_COMMON[@]}" "${SPLIT_TMP[@]}" "${ENVS[@]}" -e FRS_TM_JEMALLOC="$TM_JEMALLOC_EFF" -e MALLOC_CONF="$TM_MALLOC_CONF_EFF" -e FLINK_CONF_DIR="$CCONF" "$IMG" bash -lc "
            mkdir -p /usr/local/lib && cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
            cp '$SO' /usr/lib/libforst_rs_ffi.so &&
            cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
            for t in \$(seq 1 150); do curl -sf http://$JM_ALIAS:8081/overview >/dev/null 2>&1 && break; sleep 2; done
            exec bash '$FLINK/bin/taskmanager.sh' start-foreground
          " >/dev/null
      done
      docker run --rm --name "$CLUSTER-jm" --network "$NET" --network-alias "$JM_ALIAS" --cpus="$SPLIT_JM_CPUS" --memory="$SPLIT_JM_MEM" \
        "${DKR_COMMON[@]}" "${SPLIT_TMP[@]}" "${ENVS[@]}" -e CLUSTER_MODE=external -e EXPECT_TMS=2 -e JM_HOST="$JM_ALIAS" -e FLINK_CONF_DIR="$CCONF" \
        "$IMG" bash -lc "
          mkdir -p /usr/local/lib && cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
          cp '$SO' /usr/lib/libforst_rs_ffi.so &&
          cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
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
    # NOTE (2026-06-15 user directive): NexMark now runs EVERY query on the
    # 2×4c/16g split (TOPO=split), q9 included (it fits 16g/TM via the
    # process.size=10240m carve-out). This TOPO=single path is retained only as a
    # generic harness capability; no NexMark query routes to it. Size via
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
    SINGLE_NAME="$RUN_CLUSTER-jm"
    docker rm -f "$SINGLE_NAME" >/dev/null 2>&1 || true
    echo "== TOPO=single resources: --cpus=$SINGLE_TM_CPUS --memory=$SINGLE_TM_MEM (detected RAM ${PHYS_RAM_MIB:-?} MiB) =="
    docker run --rm --name "$SINGLE_NAME" --cpus="$SINGLE_TM_CPUS" --memory="$SINGLE_TM_MEM" --memory-swap="$SINGLE_TM_MEM" ${PERF_OPTS[@]+"${PERF_OPTS[@]}"} ${URING_OPTS[@]+"${URING_OPTS[@]}"} ${SINGLE_PRELOAD[@]+"${SINGLE_PRELOAD[@]}"} "${DKR_COMMON[@]}" "${RUN_TMP_MOUNT[@]}" \
      "${ENVS[@]}" \
      "$IMG" bash -lc "
        cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
        mkdir -p /usr/local/lib && cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
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
