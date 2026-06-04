#!/usr/bin/env bash
# q5 STALLS (zero forward progress at ~2.2M while RUNNING). Capture WHERE it is
# stuck: submit q5, detect the stall (src_out flat), then jstack the TaskManager
# twice (5s apart) so we can see which task/state threads are blocked.
set -u
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
CONF="$FLINK_HOME/conf/config.yaml"
cp "$FLINK_HOME/conf/templates/config-forst-rs-local.yaml.tpl" "$CONF"
rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data
cat >> "$CONF" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1; "$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'TaskManagerRunner|StandaloneSession|SqlGateway|run_query' 2>/dev/null; sleep 3
JAVA_HOME="$JDK25" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1; sleep 6
JAVA_HOME="$JDK25" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1; sleep 8
echo "cluster up; submitting q5"
JAVA_HOME="$JDK25" "$NEXMARK_HOME"/bin/run_query.sh oa q5 > /tmp/diag-q5js.out 2>&1 &
prev=-1; flat=0; dumped=0
for i in $(seq 1 25); do  # 25 x 6s = 150s (stall well before any restart)
  sleep 6
  jid=$(curl -s --max-time 6 http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except: sys.exit()
r=[j for j in d.get('jobs',[]) if j['state']=='RUNNING']
if r: print(max(r,key=lambda x:x.get('duration',0))['jid'])" 2>/dev/null)
  src=0
  [ -n "${jid:-}" ] && src=$(curl -s --max-time 6 "http://localhost:8081/jobs/$jid" 2>/dev/null | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except: print(0); sys.exit()
print(sum(v.get('metrics',{}).get('write-records',0) or 0 for v in d.get('vertices',[]) if 'Source' in v.get('name','')))" 2>/dev/null)
  echo "[$((i*6))s] src_out=${src:-0}"
  if [ "${src:-0}" = "$prev" ] && [ "${src:-0}" != "0" ]; then flat=$((flat+1)); else flat=0; fi
  prev="${src:-0}"
  if [ "$flat" -ge 2 ] && [ "$dumped" -eq 0 ]; then
    pid=$(pgrep -f TaskManagerRunner | head -1)
    echo "######## STALL CONFIRMED (src flat at $src) — jstack TM pid=$pid ########"
    for d in 1 2; do
      echo "===== JSTACK #$d (pid $pid) ====="
      "$JDK25"/bin/jstack -l "$pid" > /tmp/q5-jstack-$d.txt 2>&1 && echo "ok -> /tmp/q5-jstack-$d.txt" || echo "jstack failed"
      sleep 5
    done
    dumped=1
    break
  fi
done
echo "=== task/state threads (BLOCKED/WAITING) from dump #1 ==="
grep -A14 -iE "\"(Source: datagen|GlobalWindowAggregate|LocalWindowAggregate|WindowJoin).*\"" /tmp/q5-jstack-1.txt 2>/dev/null | grep -iE "^\"|java.lang.Thread.State|forstrs|forst_rs|ForStRs|checkpoint|snapshot|park|wait|lock|Native Method|FFM|Arena|Linker" | head -60
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1; "$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'TaskManagerRunner|StandaloneSession|SqlGateway|run_query' 2>/dev/null
echo "DIAG-Q5-JSTACK DONE"
