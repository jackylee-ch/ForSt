#!/usr/bin/env bash
# Poor-man's sampling profiler for a forst-rs-local NexMark query.
# Starts a fresh forst-rs cluster, submits the query, samples the TaskManager
# thread stacks every 3s for ~90s, then aggregates the hottest frames.
# Usage: Q=q7 ./profile-q7-jstack.sh
set -uo pipefail
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
Q="${Q:-q7}"
OUT=/tmp/profile-$Q
rm -rf "$OUT"; mkdir -p "$OUT"
S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'
export RUN_ID="prof-$(date +%s)"

# fresh cluster, forst-rs-local config
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f "nexmark.flink.Benchmark" 2>/dev/null; pkill -f TaskManagerRunner 2>/dev/null; pkill -f StandaloneSession 2>/dev/null; sleep 3
rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data /tmp/nexmark-checkpoints-forst-rs
envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs-local.yaml.tpl" > "$FLINK_HOME/conf/config.yaml"
# run_query.sh submits via the SQL gateway (port 8083) — must be configured + started.
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

# submit query in background
JAVA_HOME="$JDK25" "$NEXMARK_HOME"/bin/run_query.sh oa "$Q" > "$OUT/run.out" 2>&1 &
RUNPID=$!

# wait for a RUNNING job
for i in $(seq 1 40); do
  st=$(curl -sf http://localhost:8081/jobs 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);print(next((j['status'] for j in d.get('jobs',[])),'NONE'))" 2>/dev/null)
  [ "$st" = "RUNNING" ] && break; sleep 2
done
echo "job status=$st (after wait)"

# find the TaskManager pid
TMPID=$(jps 2>/dev/null | grep -iE "TaskManagerRunner" | awk '{print $1}' | head -1)
echo "TM pid=$TMPID"
# let it warm up
sleep 20
# sample 25 times every 3s (~75s of steady-state)
for i in $(seq 1 25); do
  "$JDK25/bin/jstack" "$TMPID" >> "$OUT/stacks.txt" 2>/dev/null || true
  echo "---SAMPLE $i---" >> "$OUT/stacks.txt"
  sleep 3
done

# stop everything
kill "$RUNPID" 2>/dev/null
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
pkill -f TaskManagerRunner 2>/dev/null; pkill -9 -f "nexmark.flink.Benchmark" 2>/dev/null

echo "=== TOP forst-rs / engine frames in TM task threads ==="
grep -E "org.apache.flink.state.forstrs|forst_rs|jdk.internal.foreign|java.lang.foreign|Native Method" "$OUT/stacks.txt" \
  | sed -E 's/\(.*//; s/^[[:space:]]*at //' | sort | uniq -c | sort -rn | head -40
echo "=== TOP overall frames (Source/Process/state) in 'Source'+'Window'+'Join' threads ==="
grep -E "at " "$OUT/stacks.txt" | sed -E 's/\(.*//; s/^[[:space:]]*at //' | grep -vE "^java\.|^jdk\.|^sun\.|^scala\.|Unsafe|park|epoll|Native Method$" | sort | uniq -c | sort -rn | head -30
