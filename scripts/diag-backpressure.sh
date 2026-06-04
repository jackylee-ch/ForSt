#!/usr/bin/env bash
# Per-operator bottleneck diagnostic: start the cluster for CONFIG, submit QUERY,
# and while the measurement job is RUNNING poll each vertex's backpressure
# endpoint (busyRatio / backpressuredRatio / idleRatio). The bottleneck vertex
# has busyRatio≈1 & backpressuredRatio≈0; if that's the SOURCE, the query is
# source/pipeline-bound (no state backend can speed it up).
# Usage: QUERY=q3 CONFIG=forst-rs-ffm-local SAMPLES=8 bash diag-backpressure.sh
set -u
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK17=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
QUERY="${QUERY:-q3}"; CONFIG="${CONFIG:-forst-rs-ffm-local}"; SAMPLES="${SAMPLES:-8}"
CONF="$FLINK_HOME/conf/config.yaml"
case "$CONFIG" in
  rocksdb) cp "$FLINK_HOME/conf/templates/config-rocksdb.yaml" "$CONF"; JDK="$JDK17"; rm -rf /tmp/flink-rocksdb-io /tmp/nexmark-checkpoints-rocksdb;;
  forst-rs-ffm-local) cp "$FLINK_HOME/conf/templates/config-forst-rs-local.yaml.tpl" "$CONF"; JDK="$JDK25"; rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data;;
  *) echo "unknown CONFIG $CONFIG"; exit 1;;
esac
cat >> "$CONF" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1; "$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'TaskManagerRunner|StandaloneSession|SqlGateway' 2>/dev/null; sleep 3
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1; sleep 6
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1; sleep 8
echo "cluster up; submitting $QUERY ($CONFIG)"
JAVA_HOME="$JDK" "$NEXMARK_HOME"/bin/run_query.sh oa "$QUERY" > /tmp/diag-bp-$QUERY.out 2>&1 &
# Wait for the measurement (largest-duration RUNNING) job, then poll backpressure.
for i in $(seq 1 "$SAMPLES"); do
  sleep 4
  jid=$(curl -s --max-time 6 http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except: sys.exit()
r=[j for j in d.get('jobs',[]) if j.get('state')=='RUNNING']
if r: print(max(r,key=lambda x:x.get('duration',0))['jid'])" 2>/dev/null)
  [ -z "$jid" ] && { echo "[$i] no RUNNING job yet"; continue; }
  echo "=== sample $i (job $jid) ==="
  curl -s --max-time 6 "http://localhost:8081/jobs/$jid" 2>/dev/null | python3 -c "
import json,sys,urllib.request
d=json.load(sys.stdin); jid='$jid'
for v in d.get('vertices',[]):
    vid=v['id']; name=v['name'][:46]
    try:
        bp=json.load(urllib.request.urlopen('http://localhost:8081/jobs/%s/vertices/%s/backpressure'%(jid,vid),timeout=6))
        subs=bp.get('subtasks',[])
        busy=sum(s.get('busyRatio',0) or 0 for s in subs)/max(len(subs),1)
        bpr=sum(s.get('backpressureRatio',0) or 0 for s in subs)/max(len(subs),1)
        idle=sum(s.get('idleRatio',0) or 0 for s in subs)/max(len(subs),1)
        lvl=bp.get('backpressure-level',bp.get('backpressureLevel','?'))
        print('  %-48s busy=%.2f backpressured=%.2f idle=%.2f lvl=%s'%(name,busy,bpr,idle,lvl))
    except Exception as e:
        print('  %-48s (bp fetch err: %s)'%(name,e))
" 2>/dev/null
done
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1; "$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'TaskManagerRunner|StandaloneSession|SqlGateway|run_query' 2>/dev/null
echo "DIAG DONE"
