#!/usr/bin/env bash
set -u
export FLINK_HOME="${FLINK_HOME:-/opt/flink}"
NEXMARK_HOME="${NEXMARK_HOME:-/src/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink}"
export HADOOP_CLASSPATH="$(find "${HADOOP_HOME:-/opt/hadoop}/share/hadoop" -name '*.jar' 2>/dev/null | tr '\n' ':')"
JDK17="${JDK17:-/opt/java/openjdk}"
JDK25="${JDK25:-/opt/java/openjdk}"
export RUN_ID="${RUN_ID:-msql-$(date +%H%M%S)}"
QUERY="${QUERY:-q12}"
CONFIG="${CONFIG:-forst-local}"
MAXSEC="${MAXSEC:-3600}"
TPS="${TPS:-10000000}"
EVENTS_NUM="${EVENTS_NUM:-100000000}"
PERSON_PROPORTION="${PERSON_PROPORTION:-1}"
AUCTION_PROPORTION="${AUCTION_PROPORTION:-3}"
BID_PROPORTION="${BID_PROPORTION:-46}"
DONE_ON_SRC="${DONE_ON_SRC:-0}"
DONE_ON_PLATEAU="${DONE_ON_PLATEAU:-1}"
POLL_SEC="${POLL_SEC:-5}"
PLATEAU_POLLS="${PLATEAU_POLLS:-6}"
OUT_STABLE_POLLS="${OUT_STABLE_POLLS:-3}"
MIN_PLATEAU_SEC="${MIN_PLATEAU_SEC:-60}"
SRC_DONE_GRACE_SEC="${SRC_DONE_GRACE_SEC:-60}"
SRC_DONE_OUT_STABLE_POLLS="${SRC_DONE_OUT_STABLE_POLLS:-8}"
FAIL_ON_RESTART="${FAIL_ON_RESTART:-1}"
FAIL_ON_EXCEPTIONS="${FAIL_ON_EXCEPTIONS:-1}"
EXPECTED_SRC="${EXPECTED_SRC:-}"
TOTAL_PROPORTION=$((PERSON_PROPORTION + AUCTION_PROPORTION + BID_PROPORTION))
if [ -z "$EXPECTED_SRC" ]; then
  case "$QUERY" in
    q3|q8) EXPECTED_SRC=$((EVENTS_NUM * (PERSON_PROPORTION + AUCTION_PROPORTION) / TOTAL_PROPORTION)) ;;
    q4|q6|q9|q20) EXPECTED_SRC=$((EVENTS_NUM * (AUCTION_PROPORTION + BID_PROPORTION) / TOTAL_PROPORTION)) ;;
    q0|q1|q2|q5|q7|q10|q11|q12|q13|q14|q15|q16|q17|q18|q19|q21|q22) EXPECTED_SRC=$((EVENTS_NUM * BID_PROPORTION / TOTAL_PROPORTION)) ;;
    *) EXPECTED_SRC="$EVENTS_NUM" ;;
  esac
fi

emit_result() {
  local mode="$1" wall_ms="$2" src_out="$3" expected_src="$4" out_rows="$5" state="$6" elapsed_s="$7" jid="$8" note="${9:-}"
  echo "RESULT: $QUERY $mode wall_ms=$wall_ms src_out=$src_out expected_src=$expected_src out_rows=$out_rows state=$state elapsed_s=$elapsed_s note=$note"
  printf 'RESULT_TSV\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$QUERY" "$mode" "$wall_ms" "$src_out" "$expected_src" "$out_rows" "$state" "$elapsed_s" "$jid" "$note"
}

is_uint() {
  case "${1:-}" in ''|*[!0-9]*) return 1 ;; *) return 0 ;; esac
}

job_snapshot() {
  python3 -c "import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    print('?\t0\t?\t?')
    sys.exit(0)
state=d.get('state','?')
dur=d.get('duration',0) or 0

def metric_int(v, name, default='?'):
    m=(v.get('metrics') or {}).get(name, default)
    try:
        return int(m)
    except Exception:
        return None
src_total=0; src_seen=False
out_total=0; out_seen=False
for v in d.get('vertices',[]) or []:
    name=v.get('name','')
    if 'Source' in name:
        val=metric_int(v,'write-records')
        if val is not None:
            src_total += val; src_seen=True
    if 'Sink' in name or 'Writer' in name:
        val=metric_int(v,'read-records')
        if val is not None:
            out_total += val; out_seen=True
src=str(src_total) if src_seen else '?'
out=str(out_total) if out_seen else '?'
print(f'{state}\t{dur}\t{src}\t{out}')" 2>/dev/null
}

print_exceptions() {
  local jid="$1"
  echo "=== JM EXCEPTIONS ==="
  curl -s --max-time 6 "http://localhost:8081/jobs/$jid/exceptions" 2>/dev/null \
    | python3 -c "import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    print('(no exceptions json)'); sys.exit(0)
ents=(d.get('exceptionHistory',{}) or {}).get('entries') or []
print('--- recent failures ---')
for e in ents[-3:]:
    print('EXC:', e.get('exceptionName',''), '@', e.get('taskName',''))
    for ln in (e.get('stacktrace') or '').splitlines()[:25]: print(ln)
print('--- root-exception ---')
for ln in (d.get('root-exception') or '').splitlines()[:20]: print(ln)" 2>/dev/null \
    | grep -viE 'access[_-]?key|secret|s3\.|endpoint|bucket' | head -80
}


has_exception_history() {
  local jid="$1"
  curl -s --max-time 6 "http://localhost:8081/jobs/$jid/exceptions" 2>/dev/null \
    | python3 -c "import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    sys.exit(1)
ents=(d.get('exceptionHistory',{}) or {}).get('entries') or []
root=(d.get('root-exception') or '').strip()
sys.exit(0 if ents or root else 1)" 2>/dev/null
}

emit_success_or_evidence() {
  local mode="$1" wall_ms="$2" src_out="$3" expected_src="$4" out_rows="$5" state="$6" elapsed_s="$7" jid="$8" note="${9:-}"
  if [ "$FAIL_ON_RESTART" = "1" ] && [ "${restart_seen:-0}" = "1" ]; then
    print_exceptions "$jid"
    emit_result "RESTARTED" "$wall_ms" "$src_out" "$expected_src" "$out_rows" "$state" "$elapsed_s" "$jid" "restart_seen_before_${mode};${note}"
    return 0
  fi
  if [ "$FAIL_ON_EXCEPTIONS" = "1" ] && has_exception_history "$jid"; then
    print_exceptions "$jid"
    emit_result "EXCEPTION_HISTORY" "$wall_ms" "$src_out" "$expected_src" "$out_rows" "$state" "$elapsed_s" "$jid" "exception_history_before_${mode};${note}"
    return 0
  fi
  emit_result "$mode" "$wall_ms" "$src_out" "$expected_src" "$out_rows" "$state" "$elapsed_s" "$jid" "$note"
}

S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'
CONF="$FLINK_HOME/conf/config.yaml"
TEMPLATES="${TEMPLATES:-$FLINK_HOME/conf/templates}"
case "$CONFIG" in
  rocksdb) cp "$TEMPLATES/config-rocksdb.yaml" "$CONF"; JDK="$JDK17" ;;
  forst-rs-ffm-s3) envsubst "$S3VARS" < "$TEMPLATES/config-forst-rs.yaml.tpl" > "$CONF"; JDK="$JDK25" ;;
  forst-rs-ffm-local) envsubst "$S3VARS" < "$TEMPLATES/config-forst-rs-local.yaml.tpl" > "$CONF"; JDK="$JDK25" ;;
  forst-local) cp "$TEMPLATES/config-forst-local.yaml.tpl" "$CONF"; JDK="$JDK17" ;;
  *) echo "unknown config $CONFIG"; exit 1 ;;
esac
cat >> "$CONF" <<'EOF_CONF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF_CONF

echo "bench policy: query=$QUERY events=$EVENTS_NUM tps=$TPS expected_src=$EXPECTED_SRC done_on_src=$DONE_ON_SRC done_on_plateau=$DONE_ON_PLATEAU poll=$POLL_SEC plateau_polls=$PLATEAU_POLLS out_stable_polls=$OUT_STABLE_POLLS min_plateau_sec=$MIN_PLATEAU_SEC src_done_grace_sec=$SRC_DONE_GRACE_SEC fail_on_restart=$FAIL_ON_RESTART fail_on_exceptions=$FAIL_ON_EXCEPTIONS"
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f 'Benchmark|TaskManagerRunner|StandaloneSession|SqlGateway|SqlClient' 2>/dev/null || true
sleep 4
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
sleep 6
tms=""
for i in $(seq 1 20); do
  tms=$(curl -sf http://localhost:8081/overview 2>/dev/null | grep -oE '"taskmanagers":[0-9]+' | grep -oE '[0-9]+$' || true)
  [ -n "$tms" ] && [ "$tms" -ge 1 ] && break
  sleep 3
done
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
for i in $(seq 1 20); do
  curl -sf "http://localhost:8083/v1/info" >/dev/null 2>&1 && break
  sleep 2
done
echo "cluster up (tms=$tms) + gateway, building SQL for $QUERY ($CONFIG)..."

QDIR="$NEXMARK_HOME/queries"
SQLFILE="/tmp/measure-$QUERY-$RUN_ID.sql"
: > "$SQLFILE"
NEXMARK_DIR="${NEXMARK_DIR:-/tmp/nexmark-qout}"
SUBMIT_TIME="${SUBMIT_TIME:-$RUN_ID}"
CSV_SOURCE_DIR="${CSV_DIR:-/bench/csv/${CSV_LABEL:-nexmark-${EVENTS_NUM}}}"
CSV_MONITOR_OPT=""
if [ -n "${CSV_SOURCE_MONITOR_INTERVAL:-}" ]; then
  CSV_MONITOR_OPT=",
  'source.monitor-interval' = '${CSV_SOURCE_MONITOR_INTERVAL}'"
  echo "csv source monitor interval: $CSV_SOURCE_MONITOR_INTERVAL"
fi
if [ ! -d "$CSV_SOURCE_DIR/person" ] || [ ! -d "$CSV_SOURCE_DIR/auction" ] || [ ! -d "$CSV_SOURCE_DIR/bid" ]; then
  echo "CSV source directory missing expected person/auction/bid dirs: $CSV_SOURCE_DIR" >&2
  emit_result "CSV_MISSING" 0 0 "$EXPECTED_SRC" 0 "CSV_MISSING" 0 "-" "csv_missing"
  exit 1
fi
mkdir -p "$NEXMARK_DIR/data/output/$SUBMIT_TIME" 2>/dev/null || true
mkdir -p "$FLINK_HOME/data" 2>/dev/null || true
python3 - <<'PY_SIDE' > "$FLINK_HOME/data/side_input.txt"
for i in range(10000):
    print(f"{i},value-{i}")
PY_SIDE
cat > "$SQLFILE" <<SQL_CSV
CREATE TABLE person (
  id BIGINT,
  name VARCHAR,
  emailAddress VARCHAR,
  creditCard VARCHAR,
  city VARCHAR,
  state VARCHAR,
  \`dateTime\` TIMESTAMP(3),
  extra VARCHAR,
  WATERMARK FOR \`dateTime\` AS \`dateTime\` - INTERVAL '4' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = 'file://$CSV_SOURCE_DIR/person',
  'format' = 'csv'$CSV_MONITOR_OPT
);

CREATE TABLE auction (
  id BIGINT,
  itemName VARCHAR,
  description VARCHAR,
  initialBid BIGINT,
  reserve BIGINT,
  \`dateTime\` TIMESTAMP(3),
  expires TIMESTAMP(3),
  seller BIGINT,
  category BIGINT,
  extra VARCHAR,
  WATERMARK FOR \`dateTime\` AS \`dateTime\` - INTERVAL '4' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = 'file://$CSV_SOURCE_DIR/auction',
  'format' = 'csv'$CSV_MONITOR_OPT
);

CREATE TABLE bid (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel VARCHAR,
  url VARCHAR,
  \`dateTime\` TIMESTAMP(3),
  extra VARCHAR,
  WATERMARK FOR \`dateTime\` AS \`dateTime\` - INTERVAL '4' SECOND
) WITH (
  'connector' = 'filesystem',
  'path' = 'file://$CSV_SOURCE_DIR/bid',
  'format' = 'csv'$CSV_MONITOR_OPT
);
SQL_CSV
if { [ "${Q3_FILE_OUTPUT:-0}" = "1" ] || [ "${Q3_PRINT_OUTPUT:-0}" = "1" ]; } && [ "$QUERY" = "q3" ]; then
  if [ "${Q3_PRINT_OUTPUT:-0}" = "1" ]; then
    Q3_CONNECTOR="'connector' = 'print'"
    echo "q3 print output enabled"
  else
    Q3_OUTPUT_DIR="/bench/results/csv-output/$RUN_ID/q3"
    rm -rf "$Q3_OUTPUT_DIR"
    mkdir -p "$Q3_OUTPUT_DIR"
    Q3_CONNECTOR="'connector' = 'filesystem',
  'path' = 'file://$Q3_OUTPUT_DIR',
  'format' = 'csv'"
    echo "q3 file output dir: $Q3_OUTPUT_DIR"
  fi
  cat >> "$SQLFILE" <<SQL_Q3_FILE
CREATE TABLE nexmark_q3 (
  name VARCHAR,
  city VARCHAR,
  state VARCHAR,
  id BIGINT
) WITH (
  $Q3_CONNECTOR
);

INSERT INTO nexmark_q3
SELECT
    P.name, P.city, P.state, A.id
FROM
    auction AS A INNER JOIN person AS P on A.seller = P.id
WHERE
    A.category = 10 and (P.state = 'OR' OR P.state = 'ID' OR P.state = 'CA');
SQL_Q3_FILE
else
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
      "$QDIR/$QUERY.sql" >> "$SQLFILE"
fi
if [ "${FIX_Q6_SQL:-1}" = "1" ] && [ "$QUERY" = "q6" ]; then
  python3 - "$SQLFILE" <<'PY_Q6'
from pathlib import Path
import sys
path = Path(sys.argv[1])
s = path.read_text()
start = s.find('CREATE TABLE nexmark_q6')
if start < 0:
    raise SystemExit('q6 table DDL not found')
prefix = s[:start]
replacement = """CREATE TABLE nexmark_q6 (
  seller BIGINT,
  avg_price BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q6
SELECT
    Q.seller,
    CAST(AVG(Q.price) OVER
        (PARTITION BY Q.seller ORDER BY Q.bidTime, Q.id ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS BIGINT)
FROM (
    SELECT id, seller, price, bidTime
    FROM (
        SELECT A.id, A.seller, B.price, CAST(B.`dateTime` AS TIMESTAMP(3)) AS bidTime,
            ROW_NUMBER() OVER (PARTITION BY A.id, A.seller ORDER BY B.price DESC, B.`dateTime` ASC, B.bidder ASC) AS rownum
        FROM auction AS A,
            bid AS B
        WHERE A.id = B.auction
            AND B.`dateTime` BETWEEN A.`dateTime` AND A.expires
    ) T
    WHERE rownum <= 1
) Q;
"""
path.write_text(prefix + replacement)
PY_Q6
  echo "q6 SQL rewritten with nested rownum filter for Flink validation"
fi

if [ "${ACCURACY_FILE_OUTPUT:-0}" = "1" ] && [ "$QUERY" = "q10" ]; then
  python3 - "$SQLFILE" <<'PY_Q10'
from pathlib import Path
import sys
path = Path(sys.argv[1])
s = path.read_text()
start = s.find('CREATE TABLE nexmark_q10')
insert = s.find('INSERT INTO nexmark_q10', start)
if start < 0 or insert < 0:
    raise SystemExit('q10 DDL/INSERT not found')
prefix = s[:start]
suffix = s[insert:]
replacement = """CREATE TABLE nexmark_q10 (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  `dateTime` TIMESTAMP(3),
  extra VARCHAR,
  dt STRING,
  hm STRING
) WITH (
  'connector' = 'blackhole'
);

"""
path.write_text(prefix + replacement + suffix)
PY_Q10
  echo "q10 filesystem sink rewritten to accuracy-compatible sink schema"
fi

if [ "${ACCURACY_FILE_OUTPUT:-0}" = "1" ]; then
  ACCURACY_OUTPUT_DIR="${ACCURACY_OUTPUT_DIR:-/bench/results/accuracy-output/$RUN_ID/$QUERY}"
  ACCURACY_FLUSH_EVERY="${ACCURACY_FLUSH_EVERY:-1024}"
  rm -rf "$ACCURACY_OUTPUT_DIR"
  mkdir -p "$ACCURACY_OUTPUT_DIR"
  python3 - "$SQLFILE" "$ACCURACY_OUTPUT_DIR" "$ACCURACY_FLUSH_EVERY" <<'PY_ACC'
from pathlib import Path
import sys
sql = Path(sys.argv[1])
out = sys.argv[2]
flush_every = sys.argv[3]
s = sql.read_text()
old = "'connector' = 'blackhole'"
new = "'connector' = 'accuracy-file',\n  'path' = 'file://%s',\n  'flush-every' = '%s'" % (out, flush_every)
if old not in s:
    print('accuracy-file replacement skipped: no blackhole connector found', file=sys.stderr)
else:
    s = s.replace(old, new)
    sql.write_text(s)
PY_ACC
  echo "accuracy file output dir: $ACCURACY_OUTPUT_DIR flush_every=$ACCURACY_FLUSH_EVERY"
fi

echo "" >> "$SQLFILE"
echo "csv source dir: $CSV_SOURCE_DIR"

echo "submitting via sql-client embedded..."
CLIENTOUT="/tmp/measure-$QUERY-$RUN_ID.client.out"
JAVA_HOME="$JDK" "$FLINK_HOME"/bin/sql-client.sh embedded < "$SQLFILE" > "$CLIENTOUT" 2>&1
JID=$(grep -oE "Job ID: [A-Za-z0-9]{32}" "$CLIENTOUT" | head -1 | awk '{print $3}' || true)
if [ -z "$JID" ]; then
  echo "FAILED to submit: no Job ID. Client output (masked):"
  grep -v -iE 'access_key|secret|endpoint|bucket|s3\.' "$CLIENTOUT" | tail -60
  emit_result "SUBMIT_FAILED" 0 0 "$EXPECTED_SRC" 0 "SUBMIT_FAILED" 0 "-" "no_job_id"
  exit 1
fi

echo "submitted JID=$JID, polling JM..."
start=$(date +%s)
last_src=""; last_out=""; stable_src=0; stable_out=0; restart_zero=0; restart_seen=0
src_done_seen=0; src_done_dur=0; src_done_elapsed=0; src_done_stable_out=0
last_state="?"; last_dur=0; last_src_print="?"; last_out_print="?"; last_elapsed=0
while :; do
  now=$(date +%s); el=$((now-start)); last_elapsed=$el
  jj=$(curl -s --max-time 6 "http://localhost:8081/jobs/$JID" 2>/dev/null)
  snap=$(printf '%s' "$jj" | job_snapshot)
  st=$(printf '%s' "$snap" | cut -f1)
  dur=$(printf '%s' "$snap" | cut -f2)
  src=$(printf '%s' "$snap" | cut -f3)
  outrows=$(printf '%s' "$snap" | cut -f4)
  last_state="$st"; last_dur="$dur"; last_src_print="$src"; last_out_print="$outrows"
  rate=""
  if is_uint "$src" && is_uint "${last_src:-}"; then
    delta=$((src - last_src))
    rate=$((delta / (POLL_SEC > 0 ? POLL_SEC : 1)))
  fi
  if is_uint "$src"; then
    if [ "${last_src:-}" = "$src" ]; then stable_src=$((stable_src+1)); else stable_src=0; fi
    last_src="$src"
  fi
  if is_uint "$outrows"; then
    if [ "${last_out:-}" = "$outrows" ]; then stable_out=$((stable_out+1)); else stable_out=0; fi
    last_out="$outrows"
  fi
  echo "[$el s] $st dur_ms=$dur src_out=$src out_rows=$outrows rate=${rate}/s stable_src=$stable_src stable_out=$stable_out"

  if [ "$el" -ge "$MAXSEC" ]; then
    print_exceptions "$JID"
    emit_result "TIMEOUT" "$dur" "$src" "$EXPECTED_SRC" "$outrows" "$st" "$el" "$JID" "maxsec"
    break
  fi

  if [ "$st" = "RESTARTING" ]; then
    restart_seen=1
  fi

  if [ "$st" = "RESTARTING" ] && { [ "$src" = "0" ] || [ "$src" = "?" ]; }; then
    restart_zero=$((restart_zero+1))
    if [ "$restart_zero" -ge 8 ]; then
      print_exceptions "$JID"
      emit_result "RESTARTING" "$dur" "$src" "$EXPECTED_SRC" "$outrows" "$st" "$el" "$JID" "crash_loop_no_progress"
      break
    fi
  else
    restart_zero=0
  fi

  case "$st" in
    FINISHED)
      emit_success_or_evidence "FINISHED" "$dur" "$src" "$EXPECTED_SRC" "$outrows" "$st" "$el" "$JID" "finished"
      break
      ;;
    FAILED|CANCELED)
      print_exceptions "$JID"
      emit_result "$st" "$dur" "$src" "$EXPECTED_SRC" "$outrows" "$st" "$el" "$JID" "terminal"
      break
      ;;
  esac

  if [ "$DONE_ON_SRC" = "1" ] && is_uint "$src" && [ "$src" -ge "$EXPECTED_SRC" ]; then
    if [ "$src_done_seen" = "0" ]; then
      src_done_seen=1
      src_done_dur="$dur"
      src_done_elapsed="$el"
      src_done_stable_out=0
      echo "source target reached at elapsed=${src_done_elapsed}s dur_ms=${src_done_dur}; waiting for output stability grace"
    else
      if is_uint "$outrows" && [ "$stable_out" -ge "$SRC_DONE_OUT_STABLE_POLLS" ]; then
        src_done_stable_out=1
      fi
      if [ "$src_done_stable_out" = "1" ] || [ $((el-src_done_elapsed)) -ge "$SRC_DONE_GRACE_SEC" ]; then
        emit_success_or_evidence "SOURCE_DONE" "$src_done_dur" "$src" "$EXPECTED_SRC" "$outrows" "$st" "$el" "$JID" "src_done_elapsed=${src_done_elapsed};end_dur_ms=${dur};out_stable=${src_done_stable_out}"
        break
      fi
    fi
  fi

  if [ "$DONE_ON_PLATEAU" = "1" ] && [ "$el" -ge "$MIN_PLATEAU_SEC" ] && is_uint "$src" && [ "$src" -gt 0 ] && [ "$stable_src" -ge "$PLATEAU_POLLS" ] && [ "$stable_out" -ge "$OUT_STABLE_POLLS" ]; then
    emit_success_or_evidence "SOURCE_PLATEAU" "$dur" "$src" "$EXPECTED_SRC" "$outrows" "$st" "$el" "$JID" "stable_src_polls=${stable_src};stable_out_polls=${stable_out}"
    break
  fi
  sleep "$POLL_SEC"
done
