#!/usr/bin/env bash
# Run one fixed-CSV NexMark accuracy query inside a BOS benchmark container.
set -euo pipefail

FLINK_HOME="${FLINK_HOME:?set FLINK_HOME}"
NEXMARK_HOME="${NEXMARK_HOME:?set NEXMARK_HOME}"
QUERY="${QUERY:?set QUERY=q4}"
BACKEND="${BACKEND:?set BACKEND=forstrs|rocksdb}"
RUN_ID="${RUN_ID:?set RUN_ID}"
CSV_DIR="${CSV_DIR:?set CSV_DIR}"
OUT_DIR="${OUT_DIR:?set OUT_DIR}"
REMOTE_PARENT="${REMOTE_PARENT:-bos://tal-poc-namespace/jackylee/test}"
LOCAL_BASE="${LOCAL_BASE:-/tmp/jackylee/nexmark-bos-accuracy}"
NEXMARK_PARALLELISM="${NEXMARK_PARALLELISM:-8}"
MAXSEC="${MAXSEC:-900}"
JDK17="${JDK17:?set JDK17}"
JDK25="${JDK25:?set JDK25}"
TEMPLATES="${TEMPLATES:-$PWD/tools/nexmark-bos/configs}"
ACCURACY_DIR="${ACCURACY_DIR:-$PWD/tools/nexmark-bos/accuracy}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-${HADOOP_HOME:-}/etc/hadoop}"

CONF="${FLINK_CONF_DIR:-$FLINK_HOME/conf}/config.yaml"
LOG_DIR="${FLINK_LOG_DIR:-$FLINK_HOME/log}"
mkdir -p "$OUT_DIR"

export HADOOP_CONF_DIR
export NEXMARK_PARALLELISM
case "$BACKEND" in
  forstrs)
    CONFIG_NAME="forst-rs"
    export S3_BUCKET="${S3_BUCKET:?set S3_BUCKET}"
    export S3_ENDPOINT="${S3_ENDPOINT:?set S3_ENDPOINT}"
    export S3_ACCESS_KEY="${S3_ACCESS_KEY:?set S3_ACCESS_KEY}"
    export S3_SECRET_KEY="${S3_SECRET_KEY:?set S3_SECRET_KEY}"
    export S3_REGION="${S3_REGION:-us-east-1}"
    export S3_PREFIX="${S3_PREFIX:-jackylee/test/nexmark-bos-accuracy}/$RUN_ID/forst-rs-$QUERY-bos"
    envsubst '${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}' \
      < "$TEMPLATES/config-forst-rs.yaml.tpl" > "$CONF"
    JDK="$JDK25"
    rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/nexmark-checkpoints-forstrs
    ;;
  rocksdb)
    CONFIG_NAME="rocksdb"
    if [ -n "${HADOOP_HOME:-}" ] && [ -d "$HADOOP_HOME/share/hadoop" ]; then
      export HADOOP_CLASSPATH="${HADOOP_CLASSPATH:-$(find "$HADOOP_HOME/share/hadoop" -name '*.jar' 2>/dev/null | tr '\n' ':')}"
    fi
    export ROCKSDB_LOCAL_DIR="$LOCAL_BASE/rocksdb-local/$RUN_ID/$QUERY"
    export ROCKSDB_CHECKPOINT_URI="$REMOTE_PARENT/nexmark-bos-accuracy/$RUN_ID/rocksdb-$QUERY-bos/checkpoints"
    envsubst '${JDK17} ${HADOOP_CONF_DIR} ${NEXMARK_PARALLELISM} ${ROCKSDB_LOCAL_DIR} ${ROCKSDB_CHECKPOINT_URI}' \
      < "$ACCURACY_DIR/config-rocksdb-bos.yaml.tpl" > "$CONF"
    JDK="$JDK17"
    rm -rf "$ROCKSDB_LOCAL_DIR" /tmp/flink-rocksdb-tmp
    ;;
  *)
    echo "unknown BACKEND=$BACKEND" >&2
    exit 1
    ;;
esac

cat >> "$CONF" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF

if [ "${CLUSTER_MODE:-}" = "external" ]; then
  JMH="${JM_HOST:-nexmark-bos-accuracy-jm}"
  python3 - "$CONF" "$JMH" <<'PYEOF'
import sys
p, jmh = sys.argv[1], sys.argv[2]
out, sect = [], None
for ln in open(p):
    s = ln.rstrip("\n")
    if s and not s[0].isspace() and s.endswith(":"):
        sect = s[:-1]
    if s == "  bind-host: localhost":
        s = "  bind-host: 0.0.0.0"
    elif s == "    address: localhost" and sect == "jobmanager":
        s = "    address: " + jmh
    elif s == "  host: localhost" and sect == "taskmanager":
        continue
    elif s == "      size: 4096m" and sect == "jobmanager":
        s = "      size: 1600m"
    elif s == "  bind-address: localhost" and sect == "rest":
        s = "  bind-address: 0.0.0.0"
    out.append(s + "\n")
open(p, "w").writelines(out)
PYEOF
fi

echo "ACCURACY_CONFIG_OK: backend=$BACKEND query=$QUERY run_id=$RUN_ID parallelism=$NEXMARK_PARALLELISM"

"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f 'TaskManagerRunner|StandaloneSession|SqlGateway|SqlClient' 2>/dev/null || true
sleep 4
mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*taskexecutor*.out "$LOG_DIR"/*taskexecutor*.log 2>/dev/null || true

if [ "${CLUSTER_MODE:-}" = "external" ]; then
  JAVA_HOME="$JDK" "$FLINK_HOME"/bin/jobmanager.sh start >/tmp/"$RUN_ID-$BACKEND-$QUERY-jm-start.log" 2>&1
else
  JAVA_HOME="$JDK" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
fi
sleep 6

EXPECT_TMS="${EXPECT_TMS:-1}"
for _ in $(seq 1 60); do
  tms="$(curl -sf http://localhost:8081/overview 2>/dev/null | grep -oE '"taskmanagers":[0-9]+' | grep -oE '[0-9]+$' || true)"
  [ -n "${tms:-}" ] && [ "$tms" -ge "$EXPECT_TMS" ] && break
  sleep 3
done
if [ -z "${tms:-}" ] || [ "$tms" -lt "$EXPECT_TMS" ]; then
  printf 'ACCURACY_TSV\t%s\t%s\tNO_TMS\t0\t-\n' "$QUERY" "$BACKEND"
  exit 1
fi

JAVA_HOME="$JDK" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
for _ in $(seq 1 30); do
  curl -sf "http://localhost:8083/v1/info" >/dev/null 2>&1 && break
  sleep 2
done

MON_OPT=""
if [ "${QUERY}" = "q12" ] || [ -n "${MONITOR_MODE:-}" ]; then
  MON_OPT=", 'source.monitor-interval' = '1 s'"
fi

SQLFILE="/tmp/$RUN_ID-$BACKEND-$QUERY.sql"
cat > "$SQLFILE" <<SQL
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
SQL

if [ "$QUERY" = "q10" ]; then
  # q10's stock SQL uses a partitioned filesystem sink. For accuracy, keep the
  # projection but use print so changelog capture and comparison stay uniform.
  cat >> "$SQLFILE" <<'SQL_Q10'
CREATE TABLE nexmark_q10 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR,
  dt VARCHAR,
  hm VARCHAR
) WITH ('connector' = 'print');

INSERT INTO nexmark_q10
SELECT auction, bidder, price, `dateTime`, extra, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm')
FROM bid;
SQL_Q10
elif [ "$QUERY" = "q13" ]; then
  mkdir -p "$FLINK_HOME/data"
  [ -f "$FLINK_HOME/data/side_input.txt" ] || : > "$FLINK_HOME/data/side_input.txt"
  sed -e "s/'connector' = 'blackhole'/'connector' = 'print'/" \
      -e "s#\${FLINK_HOME}#$FLINK_HOME#g" \
      "$NEXMARK_HOME/queries/$QUERY.sql" >> "$SQLFILE"
else
  sed -e "s/'connector' = 'blackhole'/'connector' = 'print'/" \
      "$NEXMARK_HOME/queries/$QUERY.sql" >> "$SQLFILE"
fi
echo "" >> "$SQLFILE"

CLIENTOUT="/tmp/$RUN_ID-$BACKEND-$QUERY.client.out"
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/sql-client.sh embedded < "$SQLFILE" > "$CLIENTOUT" 2>&1
JID="$(grep -oE "Job ID: [A-Za-z0-9]{32}" "$CLIENTOUT" | head -1 | awk '{print $3}' || true)"
if [ -z "$JID" ]; then
  printf 'ACCURACY_TSV\t%s\t%s\tSUBMIT_FAILED\t0\t-\n' "$QUERY" "$BACKEND"
  grep -viE 'access[_\.-]?key|secret|credential|authorization|fs\.bos|Classpath:' "$CLIENTOUT" | tail -80
  exit 1
fi

printf 'ACCURACY_TSV\t%s\t%s\tSUBMITTED\t0\t%s\n' "$QUERY" "$BACKEND" "$JID"
start="$(date +%s)"
last_cnt=-1
stable_out=0
final_state="?"
restart_seen=0
while :; do
  now="$(date +%s)"
  elapsed=$((now - start))
  jj="$(curl -s --max-time 6 "http://localhost:8081/jobs/$JID" 2>/dev/null || true)"
  state="$(printf '%s' "$jj" | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('state','?'))
except Exception: print('?')" 2>/dev/null)"
  cnt="$(grep -achE '^([0-9]+> )?[+-][IUD]\[' "$LOG_DIR"/*taskexecutor*.out 2>/dev/null || true)"
  cnt="$(printf '%s\n' "$cnt" | awk '{s+=$1} END{print s+0}')"
  printf 'ACCURACY_POLL\t%s\t%s\telapsed_s=%s\tstate=%s\tout_rows=%s\tstable=%s\n' "$QUERY" "$BACKEND" "$elapsed" "$state" "$cnt" "$stable_out"
  [ "$state" = "RESTARTING" ] && restart_seen=1
  case "$state" in
    FINISHED|FAILED|CANCELED)
      final_state="$state"
      break
      ;;
  esac
  if [ "$QUERY" = "q12" ] || [ -n "${MONITOR_MODE:-}" ]; then
    if [ "$cnt" = "$last_cnt" ] && [ "$cnt" -gt 0 ]; then
      stable_out=$((stable_out + 1))
    else
      stable_out=0
    fi
    last_cnt="$cnt"
    if [ "$elapsed" -ge "${MIN_PLATEAU_SEC:-60}" ] && [ "$stable_out" -ge "${OUT_STABLE_POLLS:-6}" ]; then
      final_state="PLATEAU"
      curl -s -X PATCH "http://localhost:8081/jobs/$JID?mode=cancel" >/dev/null 2>&1 || true
      break
    fi
  fi
  if [ "$elapsed" -ge "$MAXSEC" ]; then
    final_state="TIMEOUT"
    break
  fi
  sleep 5
done

if { [ "$final_state" != "FINISHED" ] && [ "$final_state" != "PLATEAU" ]; } || [ "$restart_seen" = "1" ]; then
  printf 'ACCURACY_TSV\t%s\t%s\t%s_restart=%s\t0\t%s\n' "$QUERY" "$BACKEND" "$final_state" "$restart_seen" "$JID"
  curl -s --max-time 6 "http://localhost:8081/jobs/$JID/exceptions" 2>/dev/null \
    | grep -viE 'access[_\.-]?key|secret|credential|authorization|fs\.bos|Classpath:' \
    | head -c 4000
  echo
  exit 1
fi

grep -ahE '^([0-9]+> )?[+-][IUD]\[' "$LOG_DIR"/*taskexecutor*.out > "$OUT_DIR/print.rows" || true
python3 "$ACCURACY_DIR/materialize-changelog.py" materialize \
  --input "$OUT_DIR/print.rows" \
  --raw-csv "$OUT_DIR/raw-changelog.csv" \
  --final-csv "$OUT_DIR/final-materialized.csv"
rows="$(wc -l < "$OUT_DIR/print.rows" | tr -d ' ')"

printf 'ACCURACY_TSV\t%s\t%s\t%s\t%s\t%s\n' "$QUERY" "$BACKEND" "$final_state" "$rows" "$JID"
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
