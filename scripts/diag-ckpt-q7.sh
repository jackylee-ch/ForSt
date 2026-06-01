#!/usr/bin/env bash
# Pinpoint WHICH checkpoint sub-phase is slow on forst-rs-local q7.
# Runs q7, waits for >=2 completed checkpoints, dumps per-subtask
# sync/async/alignment/start-delay durations from /checkpoints/details.
set -uo pipefail
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
Q=q7; OUT=/tmp/diag-ckpt; rm -rf "$OUT"; mkdir -p "$OUT"
export RUN_ID="ckpt-$(date +%s)"
S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f "nexmark.flink.Benchmark|TaskManagerRunner|StandaloneSession" 2>/dev/null; sleep 3
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
JAVA_HOME="$JDK25" "$NEXMARK_HOME"/bin/run_query.sh oa "$Q" > "$OUT/run.out" 2>&1 &
JID=""
for i in $(seq 1 40); do
  JID=$(curl -sf http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);r=[j for j in d.get('jobs',[]) if j['state']=='RUNNING'];print(r[0]['jid'] if r else '')" 2>/dev/null)
  [ -n "$JID" ] && break; sleep 2
done
echo "JID=$JID"
# Wait until >=2 checkpoints complete (or cap 600s)
for i in $(seq 1 60); do
  n=$(curl -sf "http://localhost:8081/jobs/$JID/checkpoints" 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin).get('counts',{}).get('completed',0))" 2>/dev/null)
  echo "[$((i*10))s] completed_checkpoints=$n"
  [ "${n:-0}" -ge 2 ] && break; sleep 10
done
echo "=== checkpoint summary ==="
curl -sf "http://localhost:8081/jobs/$JID/checkpoints" 2>/dev/null | python3 -m json.tool 2>/dev/null | grep -A4 -iE "summary|end_to_end|state_size|persisted|processed" | head -40
echo "=== latest checkpoint DETAILS (per-subtask sync/async/alignment) ==="
LATEST=$(curl -sf "http://localhost:8081/jobs/$JID/checkpoints" 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);c=d.get('latest',{}).get('completed') or {};print(c.get('id',''))" 2>/dev/null)
echo "latest completed id=$LATEST"
curl -sf "http://localhost:8081/jobs/$JID/checkpoints/details/$LATEST" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('id',d.get('id'),'e2e_ms',d.get('end_to_end_duration'),'state_size',d.get('state_size'),'processed_data',d.get('processed_data'),'persisted_data',d.get('persisted_data'))
for t in d.get('tasks',{}).values() if isinstance(d.get('tasks'),dict) else []:
    pass
ts=d.get('tasks',{})
for tid,tv in (ts.items() if isinstance(ts,dict) else []):
    s=tv.get('summary',{})
    print('--- task',tv.get('name','?')[:60])
    print('   sync_ms',s.get('checkpoint_duration',{}).get('sync'),'async_ms',s.get('checkpoint_duration',{}).get('async'))
    print('   alignment_ms',s.get('alignment',{}).get('duration'),'start_delay_ms',s.get('start_delay'))
    print('   state_size',s.get('state_size'),'persisted',s.get('persisted_data'))
" 2>/dev/null || echo "(details parse failed; raw below)"
curl -sf "http://localhost:8081/jobs/$JID/checkpoints/details/$LATEST" 2>/dev/null > "$OUT/ckpt-details.json"
echo "raw saved to $OUT/ckpt-details.json ($(wc -c < $OUT/ckpt-details.json 2>/dev/null) bytes)"
echo "=== backpressure: which vertex is busy/backpressured ==="
for v in $(curl -sf "http://localhost:8081/jobs/$JID" 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);[print(x['id'],x['name'][:40]) for x in d.get('vertices',[])]" 2>/dev/null | awk '{print $1}'); do
  bp=$(curl -sf "http://localhost:8081/jobs/$JID/vertices/$v/backpressure" 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);print(d.get('status'),d.get('backpressure-level','?'))" 2>/dev/null)
  echo "$v $bp"
done
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f "nexmark.flink.Benchmark|TaskManagerRunner|StandaloneSession" 2>/dev/null
echo "=== DONE ==="
