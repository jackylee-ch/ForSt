#!/usr/bin/env bash
# Re-run forst-rs queries that timed out in the v3 sweep, with 12-min timeout to give
# Nexmark monitor.duration (10 min) full window.
set -uo pipefail

export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"

JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
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

# Restart cluster fresh
"$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null
"$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null
pkill -9 -f Benchmark 2>/dev/null || true
sleep 3

cp "$FLINK_HOME"/conf/templates/config-forst-rs-local.yaml.tpl "$FLINK_HOME"/conf/config.yaml
sed -i '' 's/-XX:+UseG1GC -XX:+UseCompactObjectHeaders/-XX:+UseG1GC/g' "$FLINK_HOME"/conf/config.yaml
sed -i '' 's/-XX:+UseZGC/-XX:+UseG1GC/g' "$FLINK_HOME"/conf/config.yaml
write_sql_gateway_config "$FLINK_HOME"/conf/config.yaml
rm -rf /tmp/flink-forst-rs-data /tmp/flink-forst-rs-cache /tmp/nexmark-checkpoints-forst-rs

JAVA_HOME="$JDK25" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
sleep 5
JAVA_HOME="$JDK25" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
sleep 8

# Queries to re-run (forst-rs timed out on these)
QUERIES="q4 q5 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22 q23"

for q in $QUERIES; do
    outfile="$OUTDIR/forst-rs-${q}.out"
    echo "=== [$(date +%H:%M:%S)] forst-rs / $q (timeout ${QUERY_TIMEOUT}s) ==="

    # Cancel any stale jobs
    jobs=$(curl -s http://localhost:8081/jobs 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    for j in d['jobs']:
        if j['status'] not in ('FINISHED','CANCELED','FAILED'):
            print(j['id'])
except: pass
")
    for jid in $jobs; do
        curl -s -X PATCH "http://localhost:8081/jobs/$jid" >/dev/null 2>&1 || true
    done
    sleep 2

    JAVA_HOME="$JDK25" "$NEXMARK_HOME"/bin/run_query.sh oa "$q" > "$outfile" 2>&1 &
    PID=$!
    elapsed=0
    while [[ $elapsed -lt $QUERY_TIMEOUT ]]; do
        if ! kill -0 $PID 2>/dev/null; then break; fi
        sleep 15
        elapsed=$((elapsed + 15))
    done
    if kill -0 $PID 2>/dev/null; then
        echo "    WATCHDOG: killing at ${elapsed}s"
        kill -9 $PID 2>/dev/null
        pkill -9 -f Benchmark 2>/dev/null
        sleep 5
    fi

    # Parse + update summary CSV
    result_line=$(grep "^|$q " "$outfile" 2>/dev/null | head -1)
    time_s=""
    throughput=""
    if [[ -n "$result_line" ]]; then
        time_s=$(echo "$result_line" | awk -F'|' '{gsub(/^ +| +$/,"",$5); print $5}' | tr -d '\n')
        throughput=$(echo "$result_line" | awk -F'|' '{gsub(/^ +| +$/,"",$7); print $7}' | tr -d '\n')
    fi
    errors=$(grep -c "IllegalStateException\|Exception in thread" "$FLINK_HOME"/log/flink-lijunqing-taskexecutor-0-*.log 2>/dev/null || echo 0)

    # Replace existing row in CSV
    grep -v "^forst-rs,$q," "$SUMMARY" > "$SUMMARY.tmp"
    echo "forst-rs,$q,$time_s,$throughput,$errors" >> "$SUMMARY.tmp"
    mv "$SUMMARY.tmp" "$SUMMARY"

    echo "    → time=${time_s}s throughput=${throughput} errors=${errors}"
done

"$FLINK_HOME"/bin/stop-cluster.sh 2>/dev/null >/dev/null
"$FLINK_HOME"/bin/sql-gateway.sh stop 2>/dev/null >/dev/null
