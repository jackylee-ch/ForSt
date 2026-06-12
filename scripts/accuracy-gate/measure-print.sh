#!/usr/bin/env bash
# Fixed-CSV accuracy measurement for ONE (query, config) — runs INSIDE the
# forst-bench container (or directly on a box with FLINK_HOME/TEMPLATES set).
#
# Replays the FIXED 1M nexmark CSV (person/auction/bid dirs, generated once by
# gen-fixed-csv.sh) as the filesystem source, replaces the query's blackhole
# sink with the 'print' connector, runs the BOUNDED job to FINISHED, then
# extracts every emitted changelog row (+I/+U/-U/-D) from the TaskManager .out
# into $OUT_FILE for deterministic offline comparison.
#
# Env: QUERY CONFIG OUT_FILE [CSV_DIR=/tmp/nexmark-fixed-csv-1m] [MAXSEC=900]
set -u
export FLINK_HOME="${FLINK_HOME:-/Users/lijunqing/Downloads/workenv/flink-2.2.1}"
NEXMARK_HOME="${NEXMARK_HOME:-/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink}"
export HADOOP_CLASSPATH="$(find "${HADOOP_HOME:-/Users/lijunqing/Downloads/workenv/hadoop-3.4.3}/share/hadoop" -name '*.jar' 2>/dev/null | tr '\n' ':')"
JDK17="${JDK17:-/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home}"
JDK25="${JDK25:-/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home}"
QUERY="${QUERY:?q3|q5|...}"
CONFIG="${CONFIG:?rocksdb|forst-rs-ffm-local}"
OUT_FILE="${OUT_FILE:?path for extracted sink rows}"
CSV_DIR="${CSV_DIR:-/tmp/nexmark-fixed-csv-1m}"
MAXSEC="${MAXSEC:-900}"
RUN_ID="acc-$QUERY-$CONFIG-$(date +%H%M%S)"
# q12 is PROCTIME-windowed: a bounded job finishes before any 10 s wall-clock
# window fires -> 0 output. Keep the source alive via monitor-interval and stop
# on OUTPUT PLATEAU (print-row count in TM .out unchanged for N polls), then
# cancel. Same approach as the remote harness (csv_monitor="1 s" for q12).
MONITOR_MODE="${MONITOR_MODE:-}"
[ "$QUERY" = "q12" ] && MONITOR_MODE="${MONITOR_MODE:-1}"
OUT_STABLE_POLLS="${OUT_STABLE_POLLS:-6}"
MIN_PLATEAU_SEC="${MIN_PLATEAU_SEC:-60}"

[ -d "$CSV_DIR/person" ] && [ -d "$CSV_DIR/auction" ] && [ -d "$CSV_DIR/bid" ] || {
  echo "VERDICT_TSV	$QUERY	$CONFIG	CSV_MISSING	0	-"; exit 1; }

S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'
export RUN_ID
CONF="$FLINK_HOME/conf/config.yaml"
TEMPLATES="${TEMPLATES:-$FLINK_HOME/conf/templates}"
case "$CONFIG" in
  rocksdb)
    cp "$TEMPLATES/config-rocksdb.yaml" "$CONF"; JDK="$JDK17"
    rm -rf /tmp/flink-rocksdb-io /tmp/nexmark-checkpoints-rocksdb ;;
  forst-rs-ffm-local)
    envsubst "$S3VARS" < "$TEMPLATES/config-forst-rs-local.yaml.tpl" > "$CONF"; JDK="$JDK25"
    rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data \
           /tmp/nexmark-checkpoints-forst-rs "${TMPDIR:-/tmp}/forst-rs-ckpt-stage" /tmp/forst-rs-ckpt-stage ;;
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

"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'TaskManagerRunner|StandaloneSession|SqlGateway|SqlClient' 2>/dev/null || true
sleep 3
# Isolate THIS run's TM stdout (the print sink writes there).
rm -f "$FLINK_HOME"/log/* 2>/dev/null || true
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
sleep 5
tms=""
for i in $(seq 1 30); do
  tms=$(curl -sf http://localhost:8081/overview 2>/dev/null | grep -oE '"taskmanagers":[0-9]+' | grep -oE '[0-9]+$' || true)
  [ -n "$tms" ] && [ "$tms" -ge 1 ] && break
  sleep 3
done
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
for i in $(seq 1 20); do
  curl -sf "http://localhost:8083/v1/info" >/dev/null 2>&1 && break
  sleep 2
done
echo "cluster up (tms=$tms), building SQL for $QUERY ($CONFIG), csv=$CSV_DIR"

MON_OPT=""
[ -n "$MONITOR_MODE" ] && MON_OPT=", 'source.monitor-interval' = '1 s'"
SQLFILE="/tmp/acc-$QUERY-$RUN_ID.sql"
cat > "$SQLFILE" <<SQL_CSV
CREATE TABLE person (
  id BIGINT, name VARCHAR, emailAddress VARCHAR, creditCard VARCHAR,
  city VARCHAR, state VARCHAR, \`dateTime\` TIMESTAMP(3), extra VARCHAR,
  WATERMARK FOR \`dateTime\` AS \`dateTime\` - INTERVAL '4' SECOND
) WITH ('connector' = 'filesystem', 'path' = 'file://$CSV_DIR/person', 'format' = 'csv'$MON_OPT);

CREATE TABLE auction (
  id BIGINT, itemName VARCHAR, description VARCHAR, initialBid BIGINT, reserve BIGINT,
  \`dateTime\` TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT, category BIGINT, extra VARCHAR,
  WATERMARK FOR \`dateTime\` AS \`dateTime\` - INTERVAL '4' SECOND
) WITH ('connector' = 'filesystem', 'path' = 'file://$CSV_DIR/auction', 'format' = 'csv'$MON_OPT);

CREATE TABLE bid (
  auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR,
  \`dateTime\` TIMESTAMP(3), extra VARCHAR,
  WATERMARK FOR \`dateTime\` AS \`dateTime\` - INTERVAL '4' SECOND
) WITH ('connector' = 'filesystem', 'path' = 'file://$CSV_DIR/bid', 'format' = 'csv'$MON_OPT);
SQL_CSV
# Query SQL with the blackhole sink swapped for 'print' (rows land in TM .out,
# prefixed with their changelog kind — captures retractions too).
sed -e "s/'connector' = 'blackhole'/'connector' = 'print'/" \
    "$NEXMARK_HOME/queries/$QUERY.sql" >> "$SQLFILE"
# qX.sql files have no trailing newline — sql-client drops the final
# unterminated line (q5's INSERT never executed). Terminate it.
echo "" >> "$SQLFILE"

echo "submitting via sql-client..."
CLIENTOUT="/tmp/acc-$QUERY-$RUN_ID.client.out"
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/sql-client.sh embedded < "$SQLFILE" > "$CLIENTOUT" 2>&1
JID=$(grep -oE "Job ID: [A-Za-z0-9]{32}" "$CLIENTOUT" | head -1 | awk '{print $3}' || true)
if [ -z "$JID" ]; then
  echo "FAILED to submit: no Job ID:"; tail -40 "$CLIENTOUT"
  echo "VERDICT_TSV	$QUERY	$CONFIG	SUBMIT_FAILED	0	-"; exit 1
fi
echo "submitted JID=$JID, polling to FINISHED (monitor_mode=${MONITOR_MODE:-0})..."

start=$(date +%s); restart_seen=0; final_state="?"; last_cnt=-1; stable_out=0
while :; do
  now=$(date +%s); el=$((now-start))
  jj=$(curl -s --max-time 6 "http://localhost:8081/jobs/$JID" 2>/dev/null)
  st=$(printf '%s' "$jj" | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('state','?'))
except Exception: print('?')" 2>/dev/null)
  cnt=$(grep -achE '^([0-9]+> )?[+-][IUD]\[' "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null \
        | awk '{s+=$1} END{print s+0}')
  echo "[$el s] state=$st out_rows=$cnt stable_out=$stable_out"
  [ "$st" = "RESTARTING" ] && restart_seen=1
  case "$st" in
    FINISHED|FAILED|CANCELED) final_state="$st"; break ;;
  esac
  if [ -n "$MONITOR_MODE" ]; then
    if [ "$cnt" = "$last_cnt" ] && [ "$cnt" -gt 0 ]; then stable_out=$((stable_out+1)); else stable_out=0; fi
    last_cnt="$cnt"
    if [ "$el" -ge "$MIN_PLATEAU_SEC" ] && [ "$stable_out" -ge "$OUT_STABLE_POLLS" ]; then
      final_state="PLATEAU"
      echo "output plateau reached (rows=$cnt), canceling job..."
      curl -s -X PATCH "http://localhost:8081/jobs/$JID?mode=cancel" >/dev/null 2>&1
      break
    fi
  fi
  if [ "$el" -ge "$MAXSEC" ]; then final_state="TIMEOUT"; break; fi
  sleep 5
done

if { [ "$final_state" != "FINISHED" ] && [ "$final_state" != "PLATEAU" ]; } || [ "$restart_seen" = "1" ]; then
  echo "=== JM EXCEPTIONS ==="
  curl -s --max-time 6 "http://localhost:8081/jobs/$JID/exceptions" 2>/dev/null | head -c 4000; echo
  echo "VERDICT_TSV	$QUERY	$CONFIG	${final_state}_restart=${restart_seen}	0	$JID"
  exit 1
fi
# Exception history (even on FINISHED) invalidates the run — fail loudly.
if curl -s --max-time 6 "http://localhost:8081/jobs/$JID/exceptions" 2>/dev/null \
   | python3 -c "import json,sys
d=json.load(sys.stdin)
ents=(d.get('exceptionHistory',{}) or {}).get('entries') or []
sys.exit(0 if ents or (d.get('root-exception') or '').strip() else 1)" 2>/dev/null; then
  echo "VERDICT_TSV	$QUERY	$CONFIG	EXCEPTION_HISTORY	0	$JID"; exit 1
fi

# Extract print-sink changelog rows from TM stdout. Optional "N> " prefix when
# sink parallelism > 1; comparator strips it.
mkdir -p "$(dirname "$OUT_FILE")"
grep -ahE '^([0-9]+> )?[+-][IUD]\[' "$FLINK_HOME"/log/*taskexecutor*.out > "$OUT_FILE" || true
ROWS=$(wc -l < "$OUT_FILE" | tr -d ' ')
echo "extracted $ROWS sink rows -> $OUT_FILE"
echo "VERDICT_TSV	$QUERY	$CONFIG	$final_state	$ROWS	$JID"
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1
exit 0
