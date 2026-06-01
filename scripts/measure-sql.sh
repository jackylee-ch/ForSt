#!/usr/bin/env bash
# 2026-05-29 RELIABLE measurement via DIRECT sql-client submission — bypasses
# nexmark's fragile warmup + MetricReporter + FlinkRestClient (whose connection
# pool leaks/blocks under heavy S3 load, killing runs before the measurement job
# is even submitted). This reproduces EXACTLY what nexmark's QueryRunner does
# internally (`sql-client.sh embedded` fed the concatenated DDL+query on stdin),
# minus the warmup and the broken monitoring. We then poll the Flink JM
# /jobs/<id> until the bounded 100M job reaches FINISHED and read its REAL
# wall-clock `duration` + Source write-records.
#
# Usage: QUERY=q9 CONFIG=forst-rs-ffm-s3 MAXSEC=3000 bash measure-sql.sh
set -u
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK17=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
export RUN_ID="${RUN_ID:-msql-$(date +%H%M%S)}"
QUERY="${QUERY:-q9}"
CONFIG="${CONFIG:-forst-rs-ffm-s3}"
MAXSEC="${MAXSEC:-3000}"
# nexmark default workload: tps=10M, eventsNum=100M, bid:46 auction:3 person:1
TPS="${TPS:-10000000}"; EVENTS_NUM="${EVENTS_NUM:-100000000}"
PERSON_PROPORTION=1; AUCTION_PROPORTION=3; BID_PROPORTION=46
S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'
CONF="$FLINK_HOME/conf/config.yaml"
case "$CONFIG" in
  rocksdb) cp "$FLINK_HOME/conf/templates/config-rocksdb.yaml" "$CONF"; JDK="$JDK17"; rm -rf /tmp/flink-rocksdb-io /tmp/nexmark-checkpoints-rocksdb ;;
  forst-rs-ffm-s3) envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs.yaml.tpl" > "$CONF"; JDK="$JDK25"; rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache ;;
  forst-rs-ffm-local) envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs-local.yaml.tpl" > "$CONF"; JDK="$JDK25"; rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data ;;
  *) echo "unknown config $CONFIG"; exit 1 ;;
esac
# The repo's sql-client.sh is a WRAPPER that reroutes nexmark's hardcoded
# `embedded` to `sql-client.sh.orig gateway --endpoint localhost:8083`, so a
# SqlGateway daemon MUST be running at 8083. Append its endpoint config + start it.
cat >> "$CONF" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF
# Fresh cluster + sql-gateway
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'Benchmark|TaskManagerRunner|StandaloneSession|SqlGateway|SqlClient' 2>/dev/null || true
sleep 4
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
sleep 6
for i in $(seq 1 20); do
  tms=$(curl -sf http://localhost:8081/overview 2>/dev/null | grep -oE '"taskmanagers":[0-9]+' | grep -oE '[0-9]+$')
  [ -n "$tms" ] && [ "$tms" -ge 1 ] && break
  sleep 3
done
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
# Wait for the gateway REST port to accept connections.
for i in $(seq 1 20); do
  curl -sf "http://localhost:8083/v1/info" >/dev/null 2>&1 && break
  sleep 2
done
echo "cluster up (tms=$tms) + gateway, building SQL for $QUERY ($CONFIG)..."
# Build the SQL exactly like nexmark QueryRunner.initializeAllSqlLines:
# ddl_gen + ddl_kafka + ddl_views + <query>, with ${VAR} substitution.
QDIR="$NEXMARK_HOME/queries"
SQLFILE="/tmp/measure-$QUERY-$RUN_ID.sql"
: > "$SQLFILE"
# Substitutions needed by file-sink / side-input queries (q10/q13) so they run
# under this harness exactly as the full nexmark runner would.
NEXMARK_DIR="${NEXMARK_DIR:-/tmp/nexmark-qout}"
SUBMIT_TIME="${SUBMIT_TIME:-$RUN_ID}"
mkdir -p "$NEXMARK_DIR/data/output/$SUBMIT_TIME" 2>/dev/null || true
# q13 side-input file: create an empty one so the legacy filesystem source opens.
mkdir -p "$FLINK_HOME/data" 2>/dev/null || true
[ -f "$FLINK_HOME/data/side_input.txt" ] || : > "$FLINK_HOME/data/side_input.txt"
for f in ddl_gen.sql ddl_kafka.sql ddl_views.sql "$QUERY.sql"; do
  [ -f "$QDIR/$f" ] || continue
  sed -e "s/\${TPS}/$TPS/g" \
      -e "s/\${EVENTS_NUM}/$EVENTS_NUM/g" \
      -e "s/\${PERSON_PROPORTION}/$PERSON_PROPORTION/g" \
      -e "s/\${AUCTION_PROPORTION}/$AUCTION_PROPORTION/g" \
      -e "s/\${BID_PROPORTION}/$BID_PROPORTION/g" \
      -e "s/\${NEXMARK_TABLE}/datagen/g" \
      -e "s/\${BOOTSTRAP_SERVERS}//g" \
      -e "s#\${NEXMARK_DIR}#$NEXMARK_DIR#g" \
      -e "s#\${SUBMIT_TIME}#$SUBMIT_TIME#g" \
      -e "s#\${FLINK_HOME}#$FLINK_HOME#g" \
      "$QDIR/$f" >> "$SQLFILE"
  echo "" >> "$SQLFILE"
done
# Submit via sql-client embedded (detached streaming INSERT). It prints "Job ID:"
# and exits on stdin EOF; the job keeps running on the cluster.
echo "submitting via sql-client embedded..."
CLIENTOUT="/tmp/measure-$QUERY-$RUN_ID.client.out"
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/sql-client.sh embedded < "$SQLFILE" > "$CLIENTOUT" 2>&1
JID=$(grep -oE "Job ID: [A-Za-z0-9]{32}" "$CLIENTOUT" | head -1 | awk '{print $3}')
if [ -z "$JID" ]; then
  echo "FAILED to submit: no Job ID. Client output (masked):"
  grep -v -iE 'access_key|secret|endpoint|bucket|s3\.' "$CLIENTOUT" | tail -25
  exit 1
fi
echo "submitted JID=$JID, polling JM until FINISHED..."
start=$(date +%s); last_src=0; last_t=0; restart_zero=0
while :; do
  now=$(date +%s); el=$((now-start))
  [ "$el" -ge "$MAXSEC" ] && { echo "MAXSEC $MAXSEC reached, giving up"; break; }
  jj=$(curl -s --max-time 6 "http://localhost:8081/jobs/$JID" 2>/dev/null)
  st=$(echo "$jj" | python3 -c "import json,sys
try: d=json.load(sys.stdin)
except: print('?'); sys.exit()
print(d.get('state','?'))" 2>/dev/null)
  dur=$(echo "$jj" | python3 -c "import json,sys
try: d=json.load(sys.stdin)
except: print(0); sys.exit()
print(d.get('duration',0))" 2>/dev/null)
  src=$(echo "$jj" | python3 -c "import json,sys
try: d=json.load(sys.stdin)
except: print('?'); sys.exit()
sv=[v for v in d.get('vertices',[]) if 'Source' in v['name']]
print(sv[0]['metrics'].get('write-records','?') if sv else '?')" 2>/dev/null)
  rate=""
  if [ "$src" != "?" ] && [ -n "$src" ] && [ "$last_t" -gt 0 ]; then
    rate=$(( (src - last_src) / ( (now-last_t) > 0 ? (now-last_t) : 1 ) ))
  fi
  [ "$src" != "?" ] && [ -n "$src" ] && { last_src=$src; last_t=$now; }
  echo "[$el s] $st dur_ms=$dur src_out=$src rate=${rate}/s"
  # Crash-loop early-abort: a job stuck RESTARTING with zero source output is
  # not going to recover (e.g. malformed file-sink path on q10/q19/q20/q23).
  # Bail after ~160s of this instead of burning the full MAXSEC cap.
  if [ "$st" = "RESTARTING" ] && { [ "$src" = "0" ] || [ "$src" = "?" ]; }; then
    restart_zero=$((restart_zero+1))
    if [ "$restart_zero" -ge 8 ]; then
      echo "RESULT: $QUERY ENDED state=RESTARTING dur_ms=$dur src_out=$src — crash-loop (no progress), aborting early"; break
    fi
  else
    restart_zero=0
  fi
  case "$st" in
    FINISHED) echo "RESULT: $QUERY FINISHED wall_ms=$dur (=$(awk "BEGIN{printf \"%.1f\", $dur/1000}")s) src_out=$src"; break ;;
    FAILED|CANCELED) echo "RESULT: $QUERY ENDED state=$st dur_ms=$dur src_out=$src — NOT a clean finish"; break ;;
  esac
  sleep 20
done
