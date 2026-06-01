#!/usr/bin/env bash
# 4-way Nexmark sweep — refresh for docs/superpowers/specs/2026-05-22-rocksdb-refresh.md.
# Configs:
#   1. rocksdb          | JDK17 | local FS
#   2. forst-s3         | JDK17 | S3 (community ForSt Java backend, bundled forst native)
#   3. forst-rs-jni-s3  | JDK17 | S3 (community ForSt backend, native swapped to forst-rs via
#                                      JNI-compat: libforst.dylib on java.library.path wins the
#                                      first System.loadLibrary("forst") before the jar fallback)
#   4. forst-rs-ffm-s3  | JDK25 | S3 (forst-rs ForStRsStateBackendFactory + FFM dylib)
#
# Fresh-cluster-per-query (orphan RESTARTING jobs otherwise starve later queries).
# S3 creds come from env (S3_ENDPOINT/ACCESS_KEY/SECRET_KEY/BUCKET/REGION/PREFIX) — never printed.
#
# Usage:
#   QUERIES="q1" CONFIGS="rocksdb forst-s3 forst-rs-jni-s3 forst-rs-ffm-s3" ./bench-4way-s3.sh   # smoke
#   ./bench-4way-s3.sh                                                                            # full Q0-Q23
set -uo pipefail

export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"

JDK17=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home

# Unique per-run S3 prefix segment so reruns never collide.
export RUN_ID="${RUN_ID:-v5-$(date +%Y%m%d-%H%M%S)}"

# compat-jni dylib for config 3 — must export Java_org_forstdb_* (built with --features compat-jni).
JNI_DIR=/tmp/forstrs-jni
mkdir -p "$JNI_DIR"
cp /Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib "$JNI_DIR/libforst.dylib"

QUERIES="${QUERIES:-q0 q1 q2 q3 q4 q5 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22 q23}"
CONFIGS="${CONFIGS:-rocksdb forst-s3 forst-rs-jni-s3 forst-rs-ffm-s3}"
QUERY_TIMEOUT="${QUERY_TIMEOUT:-720}"   # 12 min hard cap per query
OUTDIR="${OUTDIR:-/tmp/v5-bench/$RUN_ID}"
mkdir -p "$OUTDIR"
SUMMARY="$OUTDIR/summary.csv"
[ -f "$SUMMARY" ] || echo "config,query,time_s,throughput,errors" > "$SUMMARY"

# envsubst only these — leaves --add-opens etc. untouched.
S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'

echo "RUN_ID=$RUN_ID  OUTDIR=$OUTDIR"
echo "CONFIGS=$CONFIGS"
echo "QUERIES=$QUERIES"

write_sql_gateway_config() {
    cat >> "$1" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF
}

cancel_all_jobs() {
    local jobs
    jobs=$(curl -s http://localhost:8081/jobs 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    for j in d['jobs']:
        if j['status'] not in ('FINISHED','CANCELED','FAILED'):
            print(j['id'])
except: pass
" 2>/dev/null)
    for jid in $jobs; do
        curl -s -X PATCH "http://localhost:8081/jobs/$jid" >/dev/null 2>&1 || true
    done
    sleep 2
}

setup_config() {
    local cfg="$1"
    local CONF="$FLINK_HOME/conf/config.yaml"
    case "$cfg" in
        rocksdb)
            cp "$FLINK_HOME/conf/templates/config-rocksdb.yaml" "$CONF"
            rm -rf /tmp/flink-rocksdb-io /tmp/nexmark-checkpoints-rocksdb
            ;;
        forst-s3)
            envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst.yaml.tpl" > "$CONF"
            rm -rf /tmp/flink-forst-io
            ;;
        forst-rs-jni-s3)
            envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst.yaml.tpl" > "$CONF"
            # Inject java.library.path so System.loadLibrary("forst") loads our compat-jni
            # libforst.dylib (forst-rs engine) before NativeLibraryLoader falls back to the
            # bundled forst native inside flink-dist.
            perl -i -pe 's{^(env\.java\.opts\.all: )(.*)$}{${1}-Djava.library.path='"$JNI_DIR"' ${2}}' "$CONF"
            rm -rf /tmp/flink-forst-io
            ;;
        forst-rs-ffm-s3)
            envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs.yaml.tpl" > "$CONF"
            rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache
            ;;
        forst-rs-ffm-local)
            # 2026-05-29 DIAGNOSTIC: forst-rs with LOCAL storage (not S3) to isolate
            # whether the v3.8 regression is S3-config or code. v3.8 q4=46.88s/q7=54.62s
            # were measured on LOCAL FS.
            envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs-local.yaml.tpl" > "$CONF"
            rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data /tmp/nexmark-checkpoints-forst-rs
            ;;
        *) echo "unknown config: $cfg"; return 1 ;;
    esac
    write_sql_gateway_config "$CONF"
}

full_cluster_restart() {
    local jdk="$1"
    "$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1
    "$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
    pkill -9 -f Benchmark 2>/dev/null || true
    pkill -9 -f TaskManagerRunner 2>/dev/null || true
    pkill -9 -f StandaloneSession 2>/dev/null || true
    pkill -9 -f SqlGateway 2>/dev/null || true
    sleep 4
    JAVA_HOME="$jdk" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
    sleep 6
    JAVA_HOME="$jdk" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
    sleep 8
    # Readiness: JM REST must answer AND >=1 TaskManager must be registered.
    # Pre-fix this only checked /jobs, so a JM-up-but-TM-down cluster passed and
    # the nexmark client then spun on "Current Cores=0 (0 TMs)" for the whole
    # QUERY_TIMEOUT, recording a phantom timeout (e.g. q7 ckpt-on 2026-05-27).
    for i in $(seq 1 20); do
        if curl -sf http://localhost:8081/jobs >/dev/null 2>&1; then
            local tms
            tms=$(curl -sf http://localhost:8081/overview 2>/dev/null | grep -oE '"taskmanagers":[0-9]+' | grep -oE '[0-9]+$')
            [ -n "$tms" ] && [ "$tms" -ge 1 ] && return 0
        fi
        sleep 3
    done
    return 1
}

run_query() {
    local cfg="$1" query="$2" jdk="$3"
    local outfile="$OUTDIR/${cfg}-${query}.out"
    echo "=== [$(date +%H:%M:%S)] $cfg / $query (timeout ${QUERY_TIMEOUT}s) ==="

    full_cluster_restart "$jdk" || { echo "    CLUSTER FAILED to come up"; echo "$cfg,$query,,,cluster-fail" >> "$SUMMARY"; return; }
    cancel_all_jobs

    JAVA_HOME="$jdk" "$NEXMARK_HOME"/bin/run_query.sh oa "$query" > "$outfile" 2>&1 &
    local PID=$! elapsed=0
    while [[ $elapsed -lt $QUERY_TIMEOUT ]]; do
        kill -0 $PID 2>/dev/null || break
        # GOAL-CKPT-MEASURE early-break: the cluster is fresh-per-query, so its job
        # history holds only this query's two jobs — the Nexmark warmup job then the
        # measurement job. When >=2 jobs have reached a terminal state
        # (FINISHED/CANCELED/FAILED), the measurement job is done; break instead of
        # blocking the full QUERY_TIMEOUT on a hung Nexmark monitor thread (the
        # "Current Cores=0 (0 TMs)" hang seen under ckpt-on). The JM-duration
        # fallback below then recovers the wall-clock from /jobs/overview.
        local term
        term=$(curl -sf http://localhost:8081/jobs 2>/dev/null \
            | tr ',' '\n' | grep -cE '"status":"(FINISHED|CANCELED|FAILED)"')
        if [[ "${term:-0}" -ge 2 ]]; then
            echo "    [EARLY-BREAK] measurement job terminal at ~${elapsed}s (>=2 terminal jobs)"
            break
        fi
        sleep 10; elapsed=$((elapsed + 10))
    done
    if kill -0 $PID 2>/dev/null; then
        echo "    WATCHDOG: killing $PID at ${elapsed}s"
        kill -9 $PID 2>/dev/null; pkill -9 -f Benchmark 2>/dev/null; cancel_all_jobs; sleep 5
    fi

    local result_line time_s="" throughput="" time_src="nexmark"
    result_line=$(grep "^|$query " "$outfile" 2>/dev/null | head -1)
    if [[ -n "$result_line" ]]; then
        time_s=$(echo "$result_line" | awk -F'|' '{gsub(/^ +| +$/,"",$5); print $5}')
        throughput=$(echo "$result_line" | awk -F'|' '{gsub(/^ +| +$/,"",$7); print $7}')
    fi
    # GOAL-CKPT-MEASURE: the Nexmark MetricReporter monitor thread can hang at
    # "Current Cores=0 (0 TMs)" under ckpt-on even though the cluster is healthy
    # and the measurement job runs to completion (observed q7 ckpt-on 2026-05-27:
    # job RUNNING, all slots busy, checkpoints completed=2 failed=0, but no time
    # printed). The job still reaches FINISHED on the JM, so recover the wall-clock
    # from JM /jobs/overview before the per-query cluster restart wipes history.
    # We take the FINISHED job with the latest end-time (= the measurement job;
    # the warmup job ends earlier or is CANCELED). JM duration includes ~3s deploy
    # ramp on top of Nexmark's processing window, so it is a CONSERVATIVE (slightly
    # slower) number for forst-rs — any speedup computed from it is a lower bound.
    if [[ -z "$time_s" || "$time_s" == "" ]]; then
        local jm_ms
        jm_ms=$(curl -sf http://localhost:8081/jobs/overview 2>/dev/null \
            | tr ',' '\n' | grep -E '"(state|duration|end-time)"' \
            | awk -F'[:"]' '
                # Flink JobDetails JSON emits state -> end-time -> duration per job,
                # so evaluate at the duration line when all three are known.
                /"state"/      {st=$5}
                /"end-time"/   {et=$5}
                /"duration"/   {dur=$5; if(st=="FINISHED" && et>best_et){best_et=et; best=dur}}
                END{if(best!="")print best}')
        if [[ -n "$jm_ms" && "$jm_ms" -gt 0 ]]; then
            time_s=$(awk "BEGIN{printf \"%.3f\", $jm_ms/1000}")
            time_src="jm-duration"
            echo "    [JM-FALLBACK] Nexmark monitor produced no time; using JM job duration ${time_s}s"
        fi
    fi
    local errors
    errors=$(grep -c "IllegalStateException\|Exception in thread\|UnsatisfiedLinkError" "$FLINK_HOME"/log/flink-*-taskexecutor-*.log 2>/dev/null | awk -F: '{s+=$NF} END{print s+0}')
    time_s=$(echo "$time_s" | tr -d '\n' | tr -s ' ')
    throughput=$(echo "$throughput" | tr -d '\n' | tr -s ' ')
    echo "$cfg,$query,$time_s,$throughput,$errors,$time_src" >> "$SUMMARY"
    echo "    → time=${time_s}s throughput=${throughput} errors=${errors} src=${time_src}"
}

for cfg in $CONFIGS; do
    case "$cfg" in
        rocksdb|forst-s3|forst-rs-jni-s3) jdk="$JDK17" ;;
        forst-rs-ffm-s3|forst-rs-ffm-local) jdk="$JDK25" ;;
        *) echo "skip unknown $cfg"; continue ;;
    esac
    echo ""; echo "======== Config: $cfg (jdk=$(basename $(dirname $(dirname $jdk)))) ========"
    setup_config "$cfg" || continue
    for q in $QUERIES; do
        run_query "$cfg" "$q" "$jdk"
    done
    "$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1
    "$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
done

echo ""; echo "======== Sweep complete. Summary: ========"
cat "$SUMMARY"
