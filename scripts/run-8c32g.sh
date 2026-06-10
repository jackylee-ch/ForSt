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
REPO=/Users/lijunqing/Code/stczwd/ForSt
WORKENV=/Users/lijunqing/Downloads/workenv
FLINK=$WORKENV/flink-2.2.1
IMG=forst-bench:arm64
PLAT=linux/arm64

DKR_COMMON=(--platform "$PLAT"
  -v "$REPO:$REPO" -v "$WORKENV:$WORKENV"
  # FRS-SCRATCH (2026-06-08): the container's /tmp is a ~59 GB overlay on Docker.raw
  # (the Docker-Desktop VM disk), NOT the host's 425 GB volume. q9/q20/q4 write
  # ~36-45 GB of SST data + cache + checkpoint-noflush memtable artifacts to /tmp and
  # fill that 59 GB → "No space left on device" → job crash (mis-read earlier as OOM).
  # Bind-mount /tmp to a host dir on the big volume so all engine scratch uses it.
  -v "$WORKENV/frs-tmp:/tmp"
  -v forst-cargo:/cargo-cache
  -e CARGO_HOME=/cargo-cache
  -e JDK17=/usr/lib/jvm/java-17-openjdk-arm64
  -e JDK25=/opt/java/openjdk
  -e TEMPLATES="${TEMPLATES:-$REPO/scripts/templates-linux}"
  -e HADOOP_HOME="$WORKENV/hadoop-3.4.3"
  -e NEXMARK_HOME="$REPO/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink"
  -w "$REPO")

cmd="${1:?build|run|jar}"; shift || true

case "$cmd" in
  build)
    echo "== building forst-rs Linux .so (arm64) into target-linux/release =="
    docker run --rm "${DKR_COMMON[@]}" -e CARGO_TARGET_DIR="$REPO/target-linux" "$IMG" \
      bash -lc 'cargo build --release -p forst-rs-ffi && ls -la target-linux/release/libforst_rs_ffi.so'
    ;;
  run)
    Q="${1:?query}"; CFG="${2:?config}"; MS="${3:-900}"; TAG="${4:-c32}"
    SO="$REPO/target-linux/release/libforst_rs_ffi.so"
    [ -f "$SO" ] || { echo "missing $SO — run: $0 build"; exit 1; }
    OUT="/tmp/${TAG}-$Q-$CFG.out"
    echo "== 8c/32g run: $Q [$CFG] MAXSEC=$MS tag=$TAG =="
    docker run --rm --cpus=8 --memory=32g --memory-swap=32g "${DKR_COMMON[@]}" \
      -e QUERY="$Q" -e CONFIG="$CFG" -e MAXSEC="$MS" -e EVENTS_NUM="${EVENTS_NUM:-}" -e TPS="${TPS:-}" \
      -e S3_ENDPOINT=x -e S3_ACCESS_KEY=x -e S3_SECRET_KEY=x -e S3_BUCKET=x -e S3_REGION=x -e S3_PREFIX=x \
      -e FRS_BLOCK_SIZE_KB=8 -e FRS_SST_COMPRESSION="${FRS_SST_COMPRESSION:-none}" \
      -e FRS_BG_COMPACT_THREADS="${FRS_BG_COMPACT_THREADS:-}" -e FRS_BG_FLUSH_THREADS="${FRS_BG_FLUSH_THREADS:-}" \
      -e FRS_L0_STOP_TRIGGER="${FRS_L0_STOP_TRIGGER:-}" -e FRS_L0_COMPACTION_TRIGGER="${FRS_L0_COMPACTION_TRIGGER:-}" \
      -e FRS_DECAY_DIAG="${FRS_DECAY_DIAG:-}" -e FRS_BULK_SAMPLE="${FRS_BULK_SAMPLE:-}" -e FRS_ITER_DIAG="${FRS_ITER_DIAG:-}" \
      -e FRS_READ_AT_DIAG="${FRS_READ_AT_DIAG:-}" \
      -e FRS_CURSOR_DIAG="${FRS_CURSOR_DIAG:-}" -e FRS_SCAN_STATS="${FRS_SCAN_STATS:-}" \
      -e FRS_RESIDENT_BYPASS="${FRS_RESIDENT_BYPASS:-}" -e FRS_RESIDENT_SHADOW_TOTAL_MB="${FRS_RESIDENT_SHADOW_TOTAL_MB:-}" \
      -e FRS_RS_PARALLEL_EXECUTOR="${FRS_RS_PARALLEL_EXECUTOR:-}" -e FRS_RS_READ_IO_PARALLELISM="${FRS_RS_READ_IO_PARALLELISM:-}" \
      -e FRS_RS_EXECUTOR="${FRS_RS_EXECUTOR:-}" \
      -e FRS_RS_PARALLEL_ITER="${FRS_RS_PARALLEL_ITER:-}" -e FRS_ITER_DISPATCH_DIAG="${FRS_ITER_DISPATCH_DIAG:-}" \
      -e FRS_RSS_SAMPLE="${FRS_RSS_SAMPLE:-}" -e FRS_JFR="${FRS_JFR:-}" \
      -e MALLOC_CONF="${MALLOC_CONF:-}" -e _RJEM_MALLOC_CONF="${_RJEM_MALLOC_CONF:-}" \
      -e FRS_MEM_DIAG="${FRS_MEM_DIAG:-}" -e FRS_MEM_DIAG_FILE="${FRS_MEM_DIAG_FILE:-}" \
      -e FRS_DISABLE_MAPSTATE_CACHE="${FRS_DISABLE_MAPSTATE_CACHE:-}" \
      -e FRS_WBM_TOTAL_MB="${FRS_WBM_TOTAL_MB:-}" -e FRS_WBM_STALL="${FRS_WBM_STALL:-}" \
      -e FRS_WBM_HARD_MB="${FRS_WBM_HARD_MB:-}" \
      "$IMG" bash -lc "
        cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
        mkdir -p /usr/local/lib && cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
        nproc && free -g | head -2 &&
        if [ -n \"\$FRS_RSS_SAMPLE\" ]; then ( RL=$REPO/target-linux/rss-\$QUERY.log; : > \$RL; while :; do p=\$(for j in \$(pgrep -x java); do echo \"\$(awk '/VmRSS/{print \$2}' /proc/\$j/status 2>/dev/null) \$j\"; done | sort -rn | head -1 | awk '{print \$2}'); if [ -n \"\$p\" ]; then a=\$(awk '/^Anonymous:/{an+=\$2} /^Rss:/{r+=\$2} END{print r,an}' /proc/\$p/smaps 2>/dev/null); echo \"t=\$(date +%s) pid=\$p rssKB_anonKB=\$a vmRSS=\$(awk '/VmRSS/{print \$2}' /proc/\$p/status 2>/dev/null)\" >> \$RL; n=\$((\${n:-0}+1)); if [ \$((n % 6)) -eq 0 ]; then echo \"--- NMT t=\$(date +%s) ---\" >> \$RL.nmt; jcmd \$p VM.native_memory summary 2>/dev/null | grep -E \"Total:|reserved=|- \" | head -40 >> \$RL.nmt; fi; fi; sleep 5; done & ) ; fi
        if [ -n \"\$FRS_JFR\" ]; then ( sleep \"\${FRS_JFR_DELAY:-180}\"; p=\$(for j in \$(pgrep -x java); do echo \"\$(awk '/VmRSS/{print \$2}' /proc/\$j/status 2>/dev/null) \$j\"; done | sort -rn | head -1 | awk '{print \$2}'); if [ -n \"\$p\" ]; then echo \"=== FRS_JFR start pid=\$p dur=\${FRS_JFR_DUR:-120}s ===\"; /opt/java/openjdk/bin/jcmd \$p JFR.start duration=\${FRS_JFR_DUR:-120}s filename=/tmp/\$QUERY-flame.jfr settings=profile 2>&1; fi ) & fi
        bash scripts/measure-sql.sh
      " 2>&1 | tee "$OUT"
    echo "--- RESULT line ---"; grep -E 'RESULT:|MAXSEC' "$OUT" | tail -1
    ;;
  jar)
    echo "== rebuild + redeploy forst-rs jar on host (mounted into container) =="
    cd "$REPO/../flink/flink-state-backends/flink-statebackend-forst-rs" &&
    JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home ../../mvnw -o -q -DskipTests \
      -Denforcer.skip=true -Dcheckstyle.skip=true -Dspotless.check.skip=true -Drat.skip=true \
      -Dmaven.javadoc.skip=true clean package &&
    cp target/flink-statebackend-forst-rs-2.2.0.jar "$FLINK/lib/" && echo "jar redeployed"
    ;;
  *) echo "unknown: $cmd"; exit 1 ;;
esac
