#!/usr/bin/env bash
# q5 STALLS at ~2.2M records (rate->0) with a restart at ~140s. Capture the
# root cause: start cluster, submit q5, poll src_out + job state, and when a
# restart/stall is seen dump the JM exception history + TM native stderr.
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
JAVA_HOME="$JDK25" "$NEXMARK_HOME"/bin/run_query.sh oa q5 > /tmp/diag-q5.out 2>&1 &
prev_src=-1; stall_cnt=0; dumped=0
for i in $(seq 1 30); do   # 30 x 10s = 300s
  sleep 10
  read state jid dur src < <(curl -s --max-time 6 http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except: print('NONE 0 0 0'); sys.exit()
js=d.get('jobs',[]); run=[j for j in js if j['state'] in ('RUNNING','RESTARTING','FAILING','FAILED','CREATED')]
if not run: print('NONE 0 0 0'); sys.exit()
j=max(run,key=lambda x:x.get('duration',0)); print(j['state'], j['jid'], j.get('duration',0), '0')" 2>/dev/null)
  # src_out from the job's source vertex
  if [ -n "${jid:-}" ] && [ "$jid" != "0" ]; then
    src=$(curl -s --max-time 6 "http://localhost:8081/jobs/$jid" 2>/dev/null | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except: print(0); sys.exit()
tot=0
for v in d.get('vertices',[]):
    if 'Source' in v.get('name',''): tot+=v.get('metrics',{}).get('write-records',0) or 0
print(tot)" 2>/dev/null)
  fi
  echo "[$((i*10))s] state=$state dur=${dur}ms src_out=${src}"
  # detect stall (src unchanged & >0) or non-running state
  if [ "${src:-0}" = "${prev_src}" ] && [ "${src:-0}" != "0" ]; then stall_cnt=$((stall_cnt+1)); else stall_cnt=0; fi
  prev_src="${src:-0}"
  if { [ "$stall_cnt" -ge 3 ] || [ "$state" = "RESTARTING" ] || [ "$state" = "FAILED" ] || [ "$state" = "FAILING" ]; } && [ "$dumped" -eq 0 ] && [ -n "${jid:-}" ] && [ "$jid" != "0" ]; then
    echo "######## STALL/RESTART DETECTED (state=$state stall_cnt=$stall_cnt) — dumping diagnostics ########"
    echo "=== JM exceptions ==="
    curl -s --max-time 8 "http://localhost:8081/jobs/$jid/exceptions" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
rt=d.get('root-exception')
if rt: print('ROOT:',rt[:2500])
for e in d.get('exceptionHistory',{}).get('entries',[])[:3]:
    print('---',e.get('exceptionName'),'@',e.get('taskName'))
    print((e.get('stacktrace') or '')[:1500])" 2>/dev/null
    echo "=== TM native .out tail ==="; ls -t "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null | head -1 | xargs tail -25 2>/dev/null
    echo "=== TM .log: exceptions ==="; ls -t "$FLINK_HOME"/log/*taskexecutor*.log 2>/dev/null | head -1 | xargs grep -iE "exception|caused by|error|stall|deadlock|timeout" 2>/dev/null | grep -ivE "INFO|^Picked" | tail -15
    dumped=1
  fi
done
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1; "$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'TaskManagerRunner|StandaloneSession|SqlGateway|run_query' 2>/dev/null
echo "DIAG-Q5 DONE"
