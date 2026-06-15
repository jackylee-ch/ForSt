#!/usr/bin/env bash
# 8c/32g TRUE-resource benchmark driver (arm64 Linux container, native on Apple Silicon).
#
#   scripts/run-8c32g.sh build              # build the forst-rs Linux .so (once + on engine changes)
#   scripts/run-8c32g.sh run <q> <cfg> <maxsec> [tag]
#       e.g. run-8c32g.sh run q19 forst-rs-ffm-local 900 c32
#            run-8c32g.sh run q4  rocksdb            900 c32
#   scripts/run-8c32g.sh jar                # rebuild + redeploy the forst-rs jar (host maven), then it's mounted
#
# Hard limits: --cpus=8 --memory=32g. Mounts the repo + workenv at their SAME
# host paths so the harness's absolute paths resolve; only JDK/template paths
# are overridden to Linux. The Linux .so lives at target-linux/release and is
# copied into FLINK_HOME/lib at run time.
set -u
# All env-overridable so the SAME script drives the remote Linux box
# (x86_64, repos under ~/code/stczwd, workenv under ~/workenv).
REPO="${REPO:-/Users/lijunqing/Code/stczwd/ForSt}"
WORKENV="${WORKENV:-/Users/lijunqing/Downloads/workenv}"
FLINK="${FLINK:-$WORKENV/flink-2.2.1}"
IMG="${IMG:-forst-bench:arm64}"
PLAT="${PLAT:-linux/arm64}"

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
  -e JDK17="${JDK17_IN_IMG:-/usr/lib/jvm/java-17-openjdk-arm64}"
  -e JDK25=/opt/java/openjdk
  -e TEMPLATES="${TEMPLATES:-$REPO/scripts/templates-linux}"
  -e HADOOP_HOME="$WORKENV/hadoop-3.4.3"
  -e NEXMARK_HOME="$REPO/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink"
  -w "$REPO")

# /tmp mount is per-invocation: single/build use the shared frs-tmp; TOPO=split
# namespaces it per-cluster (CONCURRENT NexMark clusters, 2026-06-12 directive).
TMP_MOUNT=(-v "$WORKENV/frs-tmp:/tmp")

cmd="${1:?build|run|jar}"; shift || true

case "$cmd" in
  build)
    echo "== building forst-rs Linux .so (arm64) into target-linux/release =="
    docker run --rm "${DKR_COMMON[@]}" "${TMP_MOUNT[@]}" -e CARGO_TARGET_DIR="$REPO/target-linux" "$IMG" \
      bash -lc 'cargo build --release -p forst-rs-ffi && ls -la target-linux/release/libforst_rs_ffi.so'
    ;;
  run)
    Q="${1:?query}"; CFG="${2:?config}"; MS="${3:-900}"; TAG="${4:-c32}"
    SO="$REPO/target-linux/release/libforst_rs_ffi.so"
    [ -f "$SO" ] || { echo "missing $SO — run: $0 build"; exit 1; }
    OUT="/tmp/${TAG}-$Q-$CFG.out"
    echo "== 8c/32g run: $Q [$CFG] MAXSEC=$MS tag=$TAG =="
    # FRS_PERF (2026-06-11): native-frame CPU profiling of the TM. Needs
    # perf_event_open which Docker's default seccomp profile blocks → relax
    # seccomp only when profiling is requested (benchmarks stay confined).
    PERF_OPTS=()
    [ -n "${FRS_PERF:-}" ] && PERF_OPTS=(--security-opt seccomp=unconfined --cap-add SYS_ADMIN)
    ENVS=(
      -e QUERY="$Q" -e CONFIG="$CFG" -e MAXSEC="$MS" -e EVENTS_NUM="${EVENTS_NUM:-}" -e TPS="${TPS:-}" \
      -e S3_ENDPOINT=x -e S3_ACCESS_KEY=x -e S3_SECRET_KEY=x -e S3_BUCKET=x -e S3_REGION=x -e S3_PREFIX=x \
      -e FRS_BLOCK_CACHE_MB="${FRS_BLOCK_CACHE_MB:-}" \
      -e FRS_SST_KV_BLOCK_FORMAT="${FRS_SST_KV_BLOCK_FORMAT:-}" -e FRS_SST_SKIP_READ_CHECKSUM="${FRS_SST_SKIP_READ_CHECKSUM:-}" \
      -e FRS_DYNAMIC_LEVELS="${FRS_DYNAMIC_LEVELS:-}" -e FRS_FADVISE="${FRS_FADVISE:-}" \
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
      # KV-sep per-TM MEMORY BOUNDS (q9 KV-sep OOM fix, 2026-06-15 PMC-1) — the
      # engine knobs that bound resident vlog state so KV-sep fits the cgroup.
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
      -e FRS_DISABLE_MAPSTATE_CACHE="${FRS_DISABLE_MAPSTATE_CACHE:-}" \
      -e FRS_WBM_TOTAL_MB="${FRS_WBM_TOTAL_MB:-}" -e FRS_WBM_STALL="${FRS_WBM_STALL:-}" \
      -e FRS_WBM_HARD_MB="${FRS_WBM_HARD_MB:-}" \
      -e FRS_SCAN_OPEN_FANOUT="${FRS_SCAN_OPEN_FANOUT:-}" -e FRS_SCAN_COLD_PRIME="${FRS_SCAN_COLD_PRIME:-}" \
      -e FRS_S2_FANOUT_MIN="${FRS_S2_FANOUT_MIN:-}" \
      -e FRS_VLOG_READER_CACHE_CAP="${FRS_VLOG_READER_CACHE_CAP:-}" \
      -e FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-}" \
      -e FRS_VLOG_COALESCE_DEREF="${FRS_VLOG_COALESCE_DEREF:-}" \
      -e FRS_KV_ADAPTIVE_PRESSURE="${FRS_KV_ADAPTIVE_PRESSURE:-}" \
      -e FRS_VLOG_GC_ADAPTIVE="${FRS_VLOG_GC_ADAPTIVE:-}" -e FRS_VLOG_GC_ADAPTIVE_CUTOFF="${FRS_VLOG_GC_ADAPTIVE_CUTOFF:-}" \
      -e FRS_VLOG_GC_AGE_CUTOFF="${FRS_VLOG_GC_AGE_CUTOFF:-}" \
      -e FRS_COMPACT_WINDOWED="${FRS_COMPACT_WINDOWED:-}" -e FRS_COMPACT_WINDOW_BYTES="${FRS_COMPACT_WINDOW_BYTES:-}" \
      -e FRS_COMPACT_PREFETCH_BUDGET="${FRS_COMPACT_PREFETCH_BUDGET:-}" -e FRS_COMPACT_PARALLEL="${FRS_COMPACT_PARALLEL:-}" \
      -e FRS_CACHE_ADMISSION="${FRS_CACHE_ADMISSION:-}" -e FRS_CACHE_BG_EXEMPT="${FRS_CACHE_BG_EXEMPT:-}" \
      -e FRS_CACHE_ADMISSION_EPOCH="${FRS_CACHE_ADMISSION_EPOCH:-}" -e FRS_CACHE_SPACE_LIMIT_MB="${FRS_CACHE_SPACE_LIMIT_MB:-}" \
      -e FRS_IO_URING="${FRS_IO_URING:-}" -e FRS_RS_INLINE_MAX="${FRS_RS_INLINE_MAX:-}" \
      -e FRS_RS_SYNC_DIRECT="${FRS_RS_SYNC_DIRECT:-}" \
      -e FRS_RS_MIXED_BATCH="${FRS_RS_MIXED_BATCH:-}" -e FRS_TIMER_INDEX_MAX="${FRS_TIMER_INDEX_MAX:-}" \
      -e FRS_RS_BLOCK_PREFETCH="${FRS_RS_BLOCK_PREFETCH:-}" -e FRS_RS_PREFETCH_THREADS="${FRS_RS_PREFETCH_THREADS:-}" \
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
      # TM JVM allocator: jemalloc via LD_PRELOAD (uniform across ALL backends —
      # an environment property of the box, like the kernel). The engine's own
      # jemalloc is statically bundled in the .so and unaffected (prefixed symbols).
      TM_PRELOAD=()
      [ "${FRS_TM_JEMALLOC:-1}" = "1" ] && TM_PRELOAD=(-e LD_PRELOAD=/usr/local/lib/libjemalloc-preload.so)
      for i in 1 2; do
        docker run -d --name "$CLUSTER-tm$i" --network "$NET" --cpus=4 --memory=16g --memory-swap=16g \
          ${TM_PRELOAD[@]+"${TM_PRELOAD[@]}"} \
          "${DKR_COMMON[@]}" "${SPLIT_TMP[@]}" "${ENVS[@]}" -e FLINK_CONF_DIR="$CCONF" "$IMG" bash -lc "
            mkdir -p /usr/local/lib && cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
            cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
            jm_ip=\$(getent hosts '$CLUSTER-jm' | awk '{print \$1; exit}')
            [ -z \"\$jm_ip\" ] && jm_ip='$CLUSTER-jm'
            for t in \$(seq 1 150); do timeout 2 bash -lc \"</dev/tcp/\$jm_ip/8081\" >/dev/null 2>&1 && break; sleep 2; done
            exec bash '$FLINK/bin/taskmanager.sh' start-foreground
          " >/dev/null
      done
      docker run --rm --name "$CLUSTER-jm" --network "$NET" --cpus=2 --memory=4g \
        "${DKR_COMMON[@]}" "${SPLIT_TMP[@]}" "${ENVS[@]}" -e CLUSTER_MODE=external -e EXPECT_TMS=2 -e JM_HOST="$CLUSTER-jm" -e FLINK_CONF_DIR="$CCONF" \
        "$IMG" bash -lc "
          mkdir -p /usr/local/lib && cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
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
    # q9 KV-sep OOM fix (2026-06-15 PMC-1): SINGLE_TM_CPUS/SINGLE_TM_MEM size a
    # BIGGER single TM (e.g. 8c/36g) that gives q9 KV-sep far more per-TM
    # headroom than the 16g-capped TOPO=split TMs. Unset = byte-identical 8c/32g.
    SINGLE_TM_CPUS="${SINGLE_TM_CPUS:-8}"
    SINGLE_TM_MEM="${SINGLE_TM_MEM:-32g}"
    echo "== TOPO=single resources: --cpus=$SINGLE_TM_CPUS --memory=$SINGLE_TM_MEM =="
    docker run --rm --cpus="$SINGLE_TM_CPUS" --memory="$SINGLE_TM_MEM" --memory-swap="$SINGLE_TM_MEM" ${PERF_OPTS[@]+"${PERF_OPTS[@]}"} "${DKR_COMMON[@]}" "${TMP_MOUNT[@]}" \
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
    cd "$REPO/../flink/flink-state-backends/flink-statebackend-forst-rs" &&
    JAVA_HOME="${JAVA_HOME:-$WORKENV/jdk25.0.2-linux_x64_gcc12}" ../../mvnw -o -q -DskipTests \
      -Denforcer.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true \
      -Dmaven.javadoc.skip=true clean package &&
    cp target/flink-statebackend-forst-rs-2.2.0.jar "$FLINK/lib/" && echo "jar redeployed"
    ;;
  *) echo "unknown: $cmd"; exit 1 ;;
esac
