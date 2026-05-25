#!/usr/bin/env bash
# Fresh-cluster-per-query strategy for forst-rs Nexmark sweep.
# Earlier sweeps fail because orphan RESTARTING jobs from previous queries accumulate and starve
# new queries of cluster slots. Solution: stop + start cluster between each query. ~12s overhead
# per query is acceptable for the reliability gain.
set -uo pipefail

export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home

OUTDIR=/tmp/v3-bench-results
SUMMARY=$OUTDIR/summary.csv
QUERY_TIMEOUT=720  # 12 min

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

setup_forst_rs_config() {
    cp "$FLINK_HOME"/conf/templates/config-forst-rs-local.yaml.tpl "$FLINK_HOME"/conf/config.yaml
    sed -i '' 's/-XX:+UseG1GC -XX:+UseCompactObjectHeaders/-XX:+UseG1GC/g' "$FLINK_HOME"/conf/config.yaml
    sed -i '' 's/-XX:+UseZGC/-XX:+UseG1GC/g' "$FLINK_HOME"/conf/config.yaml
    write_sql_gateway_config "$FLINK_HOME"/conf/config.yaml
}

full_cluster_restart() {
    "$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null
    "$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null
    pkill -9 -f Benchmark 2>/dev/null || true
    pkill -9 -f TaskManagerRunner 2>/dev/null || true
    pkill -9 -f StandaloneSession 2>/dev/null || true
    pkill -9 -f SqlGateway 2>/dev/null || true
    sleep 4
    rm -rf /tmp/flink-forst-rs-data /tmp/flink-forst-rs-cache /tmp/nexmark-checkpoints-forst-rs

    "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
    sleep 6
    "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
    sleep 8

    # Verify cluster is healthy before submitting
    for i in 1 2 3 4 5; do
        if curl -sf http://localhost:8081/jobs >/dev/null 2>&1; then
            return 0
        fi
        sleep 3
    done
    return 1
}

run_single_query_fresh() {
    local query="$1"
    local outfile="$OUTDIR/forst-rs-${query}.out"
    echo "=== [$(date +%H:%M:%S)] forst-rs / $query (fresh cluster) ==="

    if ! full_cluster_restart; then
        echo "    ERROR: cluster failed to start for $query"
        return 1
    fi

    "$NEXMARK_HOME"/bin/run_query.sh oa "$query" > "$outfile" 2>&1 &
    local PID=$!
    local elapsed=0
    while [[ $elapsed -lt $QUERY_TIMEOUT ]]; do
        if ! kill -0 $PID 2>/dev/null; then break; fi
        # Early-exit when results table appears
        if grep -q "^|$query " "$outfile" 2>/dev/null; then
            sleep 3
            break
        fi
        sleep 15
        elapsed=$((elapsed + 15))
    done
    if kill -0 $PID 2>/dev/null; then
        echo "    WATCHDOG: killing at ${elapsed}s"
        kill -9 $PID 2>/dev/null
        pkill -9 -f Benchmark 2>/dev/null
        sleep 3
    fi

    local result_line=$(grep "^|$query " "$outfile" 2>/dev/null | head -1)
    local time_s=""
    local throughput=""
    if [[ -n "$result_line" ]]; then
        time_s=$(echo "$result_line" | awk -F'|' '{gsub(/^ +| +$/,"",$5); print $5}' | tr -d '\n')
        throughput=$(echo "$result_line" | awk -F'|' '{gsub(/^ +| +$/,"",$7); print $7}' | tr -d '\n')
    fi

    # Replace existing row in CSV
    grep -v "^forst-rs,$query," "$SUMMARY" > "$SUMMARY.tmp"
    mv "$SUMMARY.tmp" "$SUMMARY"
    echo "forst-rs,$query,$time_s,$throughput,0" >> "$SUMMARY"

    echo "    → time=${time_s}s throughput=${throughput}"
}

# Initial config setup
setup_forst_rs_config

# Run queries that are currently empty in CSV (skip Q6 — not in Nexmark suite)
QUERIES_TO_RUN=""
for q_num in 5 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23; do
    q="q${q_num}"
    current_row=$(grep "^forst-rs,$q," "$SUMMARY" 2>/dev/null | head -1)
    current_time=$(echo "$current_row" | awk -F, '{print $3}' | tr -d ' \n')
    if [[ -z "$current_time" || "$current_time" == "0.000" ]]; then
        QUERIES_TO_RUN="$QUERIES_TO_RUN $q"
    fi
done

# Also re-run Q5 (suspiciously slow result of 563s)
QUERIES_TO_RUN="q5 $QUERIES_TO_RUN"

echo "Queries to run with fresh-cluster strategy: $QUERIES_TO_RUN"

for q in $QUERIES_TO_RUN; do
    run_single_query_fresh "$q"
done

# Final cluster shutdown
"$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null
"$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null

echo ""
echo "========================================"
echo "  forst-rs fresh-cluster sweep complete."
echo "========================================"
grep "^forst-rs," "$SUMMARY" | sort -t, -k2,2V
