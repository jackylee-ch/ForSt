#!/usr/bin/env bash
# v3 benchmark report — runs Nexmark Q0-Q23 sequentially on each of rocksdb / forst / forst-rs
# v3 improvements: hard per-query timeout + job-cancel + cluster-restart between problematic queries
set -uo pipefail

export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"

JDK17=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home

OUTDIR=/tmp/v3-bench-results
mkdir -p "$OUTDIR"
SUMMARY=$OUTDIR/summary.csv
echo "backend,query,time_s,throughput,errors" > "$SUMMARY"

# Hard cap per query: 5 minutes. Most queries finish in 30s-2min; this catches SESSION-window
# restart loops or hung Nexmark clients without throwing away legitimate runs (Q7 was 7.8min on
# v2 — we accept it gets cut here if it persists; better than blocking the sweep for hours).
QUERY_TIMEOUT=300

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
    local jobs=$(curl -s http://localhost:8081/jobs 2>/dev/null | python3 -c "
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

start_cluster() {
    local backend="$1"
    local jdk="$2"
    "$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null
    "$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null
    pkill -9 -f Benchmark 2>/dev/null || true
    sleep 3

    case "$backend" in
        rocksdb)
            cp "$FLINK_HOME"/conf/templates/config-rocksdb.yaml "$FLINK_HOME"/conf/config.yaml
            write_sql_gateway_config "$FLINK_HOME"/conf/config.yaml
            rm -rf /tmp/flink-rocksdb-io /tmp/nexmark-checkpoints-rocksdb
            ;;
        forst)
            cp "$FLINK_HOME"/conf/templates/config-forst.yaml.tpl "$FLINK_HOME"/conf/config.yaml
            sed -i '' 's|primary-dir: s3://${S3_BUCKET}/${S3_PREFIX}/forst-data-${RUN_ID}|primary-dir: file:///tmp/flink-forst-data|;s|dir: s3://${S3_BUCKET}/${S3_PREFIX}/forst-checkpoints-${RUN_ID}|dir: file:///tmp/nexmark-checkpoints-forst|' "$FLINK_HOME"/conf/config.yaml
            write_sql_gateway_config "$FLINK_HOME"/conf/config.yaml
            rm -rf /tmp/flink-forst-data /tmp/nexmark-checkpoints-forst
            ;;
        forst-rs)
            cp "$FLINK_HOME"/conf/templates/config-forst-rs-local.yaml.tpl "$FLINK_HOME"/conf/config.yaml
            sed -i '' 's/-XX:+UseG1GC -XX:+UseCompactObjectHeaders/-XX:+UseG1GC/g' "$FLINK_HOME"/conf/config.yaml
            sed -i '' 's/-XX:+UseZGC/-XX:+UseG1GC/g' "$FLINK_HOME"/conf/config.yaml
            write_sql_gateway_config "$FLINK_HOME"/conf/config.yaml
            rm -rf /tmp/flink-forst-rs-data /tmp/flink-forst-rs-cache /tmp/nexmark-checkpoints-forst-rs
            ;;
    esac

    JAVA_HOME="$jdk" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
    sleep 5
    JAVA_HOME="$jdk" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
    sleep 8
    return 0
}

run_query() {
    local backend="$1"
    local query="$2"
    local jdk="$3"
    local outfile="$OUTDIR/${backend}-${query}.out"
    echo "=== [$(date +%H:%M:%S)] $backend / $query (timeout ${QUERY_TIMEOUT}s) ==="

    # Cancel any stale jobs from previous runs
    cancel_all_jobs

    # Use gtimeout (brew install coreutils) or fallback to background+sleep+kill
    JAVA_HOME="$jdk" "$NEXMARK_HOME"/bin/run_query.sh oa "$query" > "$outfile" 2>&1 &
    local PID=$!
    local elapsed=0
    while [[ $elapsed -lt $QUERY_TIMEOUT ]]; do
        if ! kill -0 $PID 2>/dev/null; then
            break
        fi
        sleep 10
        elapsed=$((elapsed + 10))
    done
    if kill -0 $PID 2>/dev/null; then
        echo "    WATCHDOG: killing $PID at ${elapsed}s"
        kill -9 $PID 2>/dev/null
        pkill -9 -f Benchmark 2>/dev/null
        # Cancel any restarting jobs to free slots
        cancel_all_jobs
        sleep 5
    fi

    # Parse: look for the Nexmark results table line: "|qN ..."
    local result_line=$(grep "^|$query " "$outfile" 2>/dev/null | head -1)
    local time_s=""
    local throughput=""
    if [[ -n "$result_line" ]]; then
        # Columns split on |. Skipping the leading empty, columns are:
        # query, EventsNum, Cores, Time(s), Cores*Time, Throughput, Throughput/Cores
        time_s=$(echo "$result_line" | awk -F'|' '{gsub(/^ +| +$/,"",$5); print $5}')
        throughput=$(echo "$result_line" | awk -F'|' '{gsub(/^ +| +$/,"",$7); print $7}')
    fi
    local errors=$(grep -c "IllegalStateException\|Exception in thread" "$FLINK_HOME"/log/flink-lijunqing-taskexecutor-0-*.log 2>/dev/null || echo 0)

    # Strip newlines + extra spaces
    time_s=$(echo "$time_s" | tr -d '\n' | tr -s ' ')
    throughput=$(echo "$throughput" | tr -d '\n' | tr -s ' ')

    echo "$backend,$query,$time_s,$throughput,$errors" >> "$SUMMARY"
    echo "    → time=${time_s}s throughput=${throughput} errors=${errors}"
}

QUERIES="q0 q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22 q23"

for backend in rocksdb forst forst-rs; do
    case "$backend" in
        rocksdb|forst) jdk="$JDK17" ;;
        forst-rs) jdk="$JDK25" ;;
    esac

    echo ""
    echo "========================================"
    echo "  Backend: $backend"
    echo "========================================"

    start_cluster "$backend" "$jdk"

    for q in $QUERIES; do
        run_query "$backend" "$q" "$jdk"
    done

    "$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null
    "$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null
done

echo ""
echo "========================================"
echo "  All benchmarks complete."
echo "========================================"
cat "$SUMMARY"
