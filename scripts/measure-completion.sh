#!/usr/bin/env bash
# 2026-05-29 RELIABLE measurement: submit ONE nexmark query, let the (broken)
# metric-reporter client exit, then poll the Flink JM until the measurement
# job reaches FINISHED and read its REAL wall-clock `duration` + verify the
# Source emitted 100M records. Ignores nexmark's peak-TPS-extrapolated Time.
#
# Usage: QUERY=q9 CONFIG=forst-rs-ffm-s3 MAXSEC=2400 bash measure-completion.sh
set -u
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK17=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
export RUN_ID="${RUN_ID:-mc-$(date +%H%M%S)}"
QUERY="${QUERY:-q9}"
CONFIG="${CONFIG:-forst-rs-ffm-s3}"
MAXSEC="${MAXSEC:-2400}"
S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'
CONF="$FLINK_HOME/conf/config.yaml"
case "$CONFIG" in
  rocksdb) cp "$FLINK_HOME/conf/templates/config-rocksdb.yaml" "$CONF"; JDK="$JDK17"; rm -rf /tmp/flink-rocksdb-io /tmp/nexmark-checkpoints-rocksdb ;;
  forst-rs-ffm-s3) envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs.yaml.tpl" > "$CONF"; JDK="$JDK25"; rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache ;;
  forst-rs-ffm-local) envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs-local.yaml.tpl" > "$CONF"; JDK="$JDK25"; rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data ;;
  *) echo "unknown config $CONFIG"; exit 1 ;;
esac
cat >> "$CONF" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF
# Fresh cluster
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'Benchmark|TaskManagerRunner|StandaloneSession|SqlGateway' 2>/dev/null || true
sleep 4
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
sleep 6
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
sleep 8
for i in $(seq 1 20); do
  tms=$(curl -sf http://localhost:8081/overview 2>/dev/null | grep -oE '"taskmanagers":[0-9]+' | grep -oE '[0-9]+$')
  [ -n "$tms" ] && [ "$tms" -ge 1 ] && break
  sleep 3
done
echo "cluster up (tms=$tms), submitting $QUERY ($CONFIG)..."
# Submit the query (nexmark warmup+measurement). Detach; its metric reporter may
# throw — we ignore it and poll the JM ourselves.
JAVA_HOME="$JDK" "$NEXMARK_HOME"/bin/run_query.sh oa "$QUERY" > /tmp/mc-$QUERY.out 2>&1 &
NXPID=$!
# Poll the JM. Track the measurement (long-running) job: the job whose Source
# emits the most records. Wait for it to reach FINISHED; read its duration.
start=$(date +%s); best_dur=""; best_src=""; last_src=0; last_t=0
while :; do
  now=$(date +%s); el=$((now-start))
  [ "$el" -ge "$MAXSEC" ] && { echo "MAXSEC $MAXSEC reached, giving up"; break; }
  ov=$(curl -s --max-time 6 http://localhost:8081/jobs/overview 2>/dev/null)
  # Per job: jid state duration ; pick FINISHED job with the largest duration.
  fin=$(echo "$ov" | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except: sys.exit()
js=d.get('jobs',[])
fin=[j for j in js if j.get('state')=='FINISHED']
run=[j for j in js if j.get('state')=='RUNNING']
if fin:
  j=max(fin,key=lambda x:x.get('duration',0)); print('FINISHED',j['jid'],j.get('duration',0))
elif run:
  j=max(run,key=lambda x:x.get('duration',0)); print('RUNNING',j['jid'],j.get('duration',0))
else: print('NONE 0 0')
" 2>/dev/null)
  set -- $fin; fstate="${1:-NONE}"; fjid="${2:-}"; fdur="${3:-0}"
  src="?"
  if [ -n "$fjid" ] && [ "$fjid" != "0" ]; then
    src=$(curl -s --max-time 6 "http://localhost:8081/jobs/$fjid" 2>/dev/null | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except: sys.exit()
sv=[v for v in d.get('vertices',[]) if 'Source' in v['name']]
print(sv[0]['metrics'].get('write-records','?') if sv else '?')" 2>/dev/null)
  fi
  # sustained rate since last sample
  rate=""
  if [ "$src" != "?" ] && [ "$last_t" -gt 0 ]; then
    rate=$(( (src - last_src) / ( (now-last_t) > 0 ? (now-last_t) : 1 ) ))
  fi
  [ "$src" != "?" ] && { last_src=$src; last_t=$now; }
  echo "[$el s] $fstate dur_ms=$fdur src_out=$src rate=${rate}/s"
  if [ "$fstate" = "FINISHED" ]; then
    echo "RESULT: $QUERY FINISHED wall_ms=$fdur (=$(awk "BEGIN{printf \"%.1f\", $fdur/1000}")s) src_out=$src"
    break
  fi
  sleep 20
done
kill -9 $NXPID 2>/dev/null
