#!/usr/bin/env bash
# Run forst-rs-local q7 to completion with the current (tuned) template and
# report wall time + throughput. Baseline to beat: rocksdb-local q7 = 941 s.
set -uo pipefail
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
Q="${Q:-q7}"
OUT=/tmp/validate-$Q; rm -rf "$OUT"; mkdir -p "$OUT"
export RUN_ID="val-$(date +%s)"
S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'

"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f "nexmark.flink.Benchmark" 2>/dev/null; pkill -f TaskManagerRunner 2>/dev/null; pkill -f StandaloneSession 2>/dev/null; sleep 3
rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data /tmp/nexmark-checkpoints-forst-rs
envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs-local.yaml.tpl" > "$FLINK_HOME/conf/config.yaml"
cat >> "$FLINK_HOME/conf/config.yaml" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF
JAVA_HOME="$JDK25" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf http://localhost:8081/jobs >/dev/null 2>&1 && break; sleep 1; done
JAVA_HOME="$JDK25" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf http://localhost:8083/v1/info >/dev/null 2>&1 && break; sleep 1; done

echo "=== running $Q to completion (cap ~1600s); rocksdb baseline 941s ==="
JAVA_HOME="$JDK25" "$NEXMARK_HOME"/bin/run_query.sh oa "$Q" > "$OUT/run.out" 2>&1 &
RUNPID=$!
# Bounded wait without `timeout(1)` (absent on macOS): poll for run_query exit
# up to the cap, then hard-kill if still running.
CAP=1600; waited=0
while kill -0 "$RUNPID" 2>/dev/null; do
  sleep 10; waited=$((waited+10))
  if [ "$waited" -ge "$CAP" ]; then echo "CAP $CAP s reached — killing"; kill "$RUNPID" 2>/dev/null; break; fi
done
echo "=== q7 result (waited ${waited}s) ==="
grep -iE "Total|Throughput|Cores|finished|elapsed|,q7,|exception|error|Job Runtime" "$OUT/run.out" | head -40

echo "=== verify config was applied (grep effective config.yaml, no secrets) ==="
grep -A2 "writebuffer\|cache:" "$FLINK_HOME/conf/config.yaml" | head -20

"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -f TaskManagerRunner 2>/dev/null; pkill -9 -f "nexmark.flink.Benchmark" 2>/dev/null
echo "=== DONE $Q ==="
