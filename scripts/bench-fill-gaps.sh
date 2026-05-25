#!/usr/bin/env bash
# Fill the Q4-Q23 gaps in /tmp/v3-bench-results/summary.csv by re-running the queries with
# empty time_s. Uses a 12-min per-query watchdog (Nexmark monitor.duration is 10 min).
# Runs forst-rs first (largest gap), then forst, then rocksdb.
set -uo pipefail

export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"

JDK17=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home

OUTDIR=/tmp/v3-bench-results
SUMMARY=$OUTDIR/summary.csv
QUERY_TIMEOUT=720  # 12 min — Nexmark monitor.duration is 10 min

# Q6 is not in Nexmark suite — skip.
SKIPPED_QUERIES="q6"

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
}

run_query() {
    local backend="$1"
    local query="$2"
    local jdk="$3"
    local outfile="$OUTDIR/${backend}-${query}.out"
    echo "=== [$(date +%H:%M:%S)] $backend / $query ==="

    cancel_all_jobs

    JAVA_HOME="$jdk" "$NEXMARK_HOME"/bin/run_query.sh oa "$query" > "$outfile" 2>&1 &
    local PID=$!
    local elapsed=0
    while [[ $elapsed -lt $QUERY_TIMEOUT ]]; do
        if ! kill -0 $PID 2>/dev/null; then break; fi
        # Check if the Nexmark results table appeared — exit early.
        if grep -q "^|$query " "$outfile" 2>/dev/null; then
            sleep 3  # Let it flush stdout
            break
        fi
        sleep 15
        elapsed=$((elapsed + 15))
    done
    if kill -0 $PID 2>/dev/null; then
        echo "    WATCHDOG: killing at ${elapsed}s"
        kill -9 $PID 2>/dev/null
        pkill -9 -f Benchmark 2>/dev/null
        cancel_all_jobs
        sleep 5
    fi

    local result_line=$(grep "^|$query " "$outfile" 2>/dev/null | head -1)
    local time_s=""
    local throughput=""
    if [[ -n "$result_line" ]]; then
        time_s=$(echo "$result_line" | awk -F'|' '{gsub(/^ +| +$/,"",$5); print $5}' | tr -d '\n')
        throughput=$(echo "$result_line" | awk -F'|' '{gsub(/^ +| +$/,"",$7); print $7}' | tr -d '\n')
    fi
    local errors=$(grep -c "IllegalStateException\|Exception in thread" "$FLINK_HOME"/log/flink-lijunqing-taskexecutor-0-*.log 2>/dev/null || echo 0)

    # Replace existing row in CSV (the script writes 73 rows total — sort + dedup on (backend,query)).
    # Use grep -v to remove old row, append new one.
    grep -v "^$backend,$query," "$SUMMARY" > "$SUMMARY.tmp"
    mv "$SUMMARY.tmp" "$SUMMARY"
    echo "$backend,$query,$time_s,$throughput,$errors" >> "$SUMMARY"

    echo "    → time=${time_s}s throughput=${throughput} errors=${errors}"
}

# For each backend, find queries with empty time_s and re-run them
process_backend() {
    local backend="$1"
    local jdk="$2"

    # Collect queries that are empty or missing
    local TO_RUN=""
    for q_num in $(seq 0 23); do
        local q="q${q_num}"
        # Skip Q6 — not in Nexmark suite
        if echo "$SKIPPED_QUERIES" | grep -qw "$q"; then continue; fi
        # Check current CSV for time_s
        local current_row=$(grep "^$backend,$q," "$SUMMARY" 2>/dev/null | head -1)
        local current_time=$(echo "$current_row" | awk -F, '{print $3}' | tr -d ' \n')
        if [[ -z "$current_time" || "$current_time" == "0.000" ]]; then
            TO_RUN="$TO_RUN $q"
        fi
    done

    if [[ -z "$(echo $TO_RUN | tr -d ' ')" ]]; then
        echo "[$backend] no queries to re-run"
        return
    fi

    echo ""
    echo "========================================"
    echo "  Backend: $backend"
    echo "  Re-running:$TO_RUN"
    echo "========================================"

    start_cluster "$backend" "$jdk"

    for q in $TO_RUN; do
        run_query "$backend" "$q" "$jdk"
    done

    "$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null
    "$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null
}

# Process forst-rs first (biggest gap), then forst, then rocksdb
process_backend "forst-rs" "$JDK25"
process_backend "forst" "$JDK17"
process_backend "rocksdb" "$JDK17"

echo ""
echo "========================================"
echo "  All gap-fills complete. Final CSV:"
echo "========================================"
cat "$SUMMARY" | sort -t, -k1,1 -k2,2V
