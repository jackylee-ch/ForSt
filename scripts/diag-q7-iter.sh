#!/usr/bin/env bash
# Time-boxed forst-rs-local q7 run with FRS_ITER_DIAG=1 so the engine logs any
# prefix-iterator OPEN whose build exceeds 1ms (with mem/sst source counts).
# Decisive I/O-vs-CPU signal for the join-probe bottleneck: if builds rarely
# exceed 1ms the open is fast (cost is the get-per-key drain / probe volume);
# if many builds log with high sst_sources the cost is SST fan-out I/O.
set -uo pipefail
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
Q=q7
OUT=/tmp/diag-$Q; rm -rf "$OUT"; mkdir -p "$OUT"
export RUN_ID="diag-$(date +%s)"
# THE signal: make the engine log slow iterator builds.
export FRS_ITER_DIAG=1
S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'

"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f "nexmark.flink.Benchmark" 2>/dev/null; pkill -f TaskManagerRunner 2>/dev/null; pkill -f StandaloneSession 2>/dev/null; sleep 3
rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data /tmp/nexmark-checkpoints-forst-rs
rm -f "$FLINK_HOME"/log/*taskexecutor*.out "$FLINK_HOME"/log/*taskexecutor*.log 2>/dev/null
envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs-local.yaml.tpl" > "$FLINK_HOME/conf/config.yaml"
cat >> "$FLINK_HOME/conf/config.yaml" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF
JAVA_HOME="$JDK25" FRS_ITER_DIAG=1 "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf http://localhost:8081/jobs >/dev/null 2>&1 && break; sleep 1; done
JAVA_HOME="$JDK25" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf http://localhost:8083/v1/info >/dev/null 2>&1 && break; sleep 1; done

JAVA_HOME="$JDK25" "$NEXMARK_HOME"/bin/run_query.sh oa "$Q" > "$OUT/run.out" 2>&1 &
RUNPID=$!
for i in $(seq 1 40); do
  st=$(curl -sf http://localhost:8081/jobs 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);print(next((j['status'] for j in d.get('jobs',[])),'NONE'))" 2>/dev/null)
  [ "$st" = "RUNNING" ] && break; sleep 2
done
echo "job status=$st"
# let it process well past warmup so state flushes to SSTs (the regime we care about)
sleep 200

echo "=== FRS-ITER-DIAG line count (builds > 1ms) ==="
grep -c "FRS-ITER-DIAG" "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null
echo "=== histogram of sst_sources on slow builds ==="
grep -ho "sst_sources=[0-9]*" "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null | sort | uniq -c | sort -rn | head -20
echo "=== histogram of mem_sources ==="
grep -ho "mem_sources=[0-9]*" "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null | sort | uniq -c | sort -rn | head -20
echo "=== slowest builds (top us) ==="
grep -ho "FRS-ITER-DIAG build_lazy_prefix us=[0-9]* .*" "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null | sort -t= -k2 -rn | head -15
echo "=== sample raw lines ==="
grep "FRS-ITER-DIAG" "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null | head -8

kill "$RUNPID" 2>/dev/null
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -f TaskManagerRunner 2>/dev/null; pkill -9 -f "nexmark.flink.Benchmark" 2>/dev/null
echo "=== DONE ==="
