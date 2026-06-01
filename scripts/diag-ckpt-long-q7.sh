#!/usr/bin/env bash
# Long q7 run that tracks per-checkpoint e2e over time to catch the DEGRADATION
# (early ckpts ~1s, later ~170-350s), dumps raw details JSON for the slowest
# checkpoint, records backpressure over time, and on exit tails the TM/JM logs
# for the failover root cause. ~700s.
set -uo pipefail
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
Q=q7; OUT=/tmp/diag-ckpt-long; rm -rf "$OUT"; mkdir -p "$OUT"
export RUN_ID="ckptlong-$(date +%s)"
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
SRCV=$(curl -sf "http://localhost:8081/jobs/$JID" 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);[print(x['id']) for x in d.get('vertices',[]) if 'ource' in x['name'] or 'datagen' in x['name']]" 2>/dev/null | head -1)
JOINV=$(curl -sf "http://localhost:8081/jobs/$JID" 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);[print(x['id']) for x in d.get('vertices',[]) if 'Join' in x['name']]" 2>/dev/null | head -1)
echo "src_vertex=$SRCV join_vertex=$JOINV"
# Track checkpoints + backpressure for ~700s
for i in $(seq 1 70); do
  t=$((i*10))
  cp=$(curl -sf "http://localhost:8081/jobs/$JID/checkpoints" 2>/dev/null)
  line=$(echo "$cp" | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except: print('na'); sys.exit()
c=d.get('counts',{}); lc=d.get('latest',{}).get('completed') or {}
print('completed=%s failed=%s inprog=%s | latest id=%s e2e_ms=%s size=%s'%(c.get('completed'),c.get('failed'),c.get('in_progress'),lc.get('id'),lc.get('end_to_end_duration'),lc.get('state_size')))
" 2>/dev/null)
  bp=""
  if [ -n "$JOINV" ]; then bp=$(curl -sf "http://localhost:8081/jobs/$JID/vertices/$JOINV/backpressure" 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);print('join_bp=%s'%d.get('backpressure-level','?'))" 2>/dev/null); fi
  st=$(curl -sf "http://localhost:8081/jobs/$JID" 2>/dev/null | python3 -c "import json,sys;print('jobstate='+json.load(sys.stdin).get('state','?'))" 2>/dev/null)
  echo "[${t}s] $st $line $bp"
  # If a checkpoint e2e exceeded 30s, dump its raw details and stop early.
  slow=$(echo "$cp" | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except: sys.exit()
lc=d.get('latest',{}).get('completed') or {}
print(lc.get('id','') if (lc.get('end_to_end_duration') or 0)>30000 else '')
" 2>/dev/null)
  if [ -n "$slow" ]; then
    echo ">>> SLOW checkpoint $slow (e2e>30s) — dumping details"
    curl -sf "http://localhost:8081/jobs/$JID/checkpoints/details/$slow" 2>/dev/null > "$OUT/slow-ckpt-$slow.json"
    echo "saved $OUT/slow-ckpt-$slow.json ($(wc -c < $OUT/slow-ckpt-$slow.json) bytes)"
    break
  fi
  [ "$st" = "jobstate=FINISHED" ] && { echo "FINISHED"; break; }
  sleep 10
done
echo "=== SLOW checkpoint sub-phase (per-subtask) ==="
for f in "$OUT"/slow-ckpt-*.json; do
  [ -f "$f" ] || continue
  python3 -c "
import json
d=json.load(open('$f'))
print('ckpt',d.get('id'),'e2e_ms',d.get('end_to_end_duration'),'size',d.get('state_size'))
for vid,tv in (d.get('tasks') or {}).items():
    nm=tv.get('name','?')[:45]
    sub=tv.get('subtasks') or []
    for s in sub:
        cd=s.get('checkpoint',{}) ; al=s.get('alignment',{}) if isinstance(s.get('alignment'),dict) else {}
        print('  ',nm,'idx',s.get('index'),'ack_ms',s.get('end_to_end_duration'),
              'sync',(s.get('checkpoint') or {}).get('sync_duration') if isinstance(s.get('checkpoint'),dict) else None,
              'async',(s.get('checkpoint') or {}).get('async_duration') if isinstance(s.get('checkpoint'),dict) else None,
              'align',al.get('duration'),'start_delay',s.get('start_delay'))
" 2>/dev/null || { echo "raw keys:"; python3 -c "import json;d=json.load(open('$f'));print(list(d.keys()));t=d.get('tasks') or {};[print(k,list(v.keys())) for k,v in list(t.items())[:1]]"; }
done
echo "=== run.out tail (failover / exceptions) ==="
grep -iE "exception|caused by|fail|restart|error|cancel" "$OUT/run.out" 2>/dev/null | head -15
echo "=== TM log: first task FAILED root cause ==="
ls -t $FLINK_HOME/log/*taskexecutor*.log | head -1 | xargs grep -nE "switched from RUNNING to (FAILED|CANCELED)|Caused by:|OutOfMemory|java.lang.|ForstError|panic" 2>/dev/null | grep -ivE "shutting down|Disconnect from JobManager|already in state|Attempting to fail" | head -20
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f "nexmark.flink.Benchmark|TaskManagerRunner|StandaloneSession" 2>/dev/null
echo "=== DONE ==="
