#!/usr/bin/env bash
# Generate a fixed NexMark CSV input dataset for BOS accuracy tests.
#
# The script runs inside the benchmark container. It uses NexMark datagen once
# and writes bounded CSV files to $CSV_DIR/{person,auction,bid}. The default
# scale is 100K events with a 100s event-time span.
set -euo pipefail

FLINK_HOME="${FLINK_HOME:?set FLINK_HOME}"
NEXMARK_HOME="${NEXMARK_HOME:?set NEXMARK_HOME}"
JDK17="${JDK17:?set JDK17}"
CSV_DIR="${CSV_DIR:-/tmp/jackylee/nexmark-bos-accuracy/input-100k}"
EVENTS_NUM="${EVENTS_NUM:-100000}"
GEN_TPS="${GEN_TPS:-$(( EVENTS_NUM / 100 ))}"
[ "$GEN_TPS" -gt 0 ] || GEN_TPS=1

PERSON_PROPORTION="${PERSON_PROPORTION:-1}"
AUCTION_PROPORTION="${AUCTION_PROPORTION:-3}"
BID_PROPORTION="${BID_PROPORTION:-46}"
RUN_ID="nexmark-bos-accuracy-gencsv-$(date +%Y%m%d-%H%M%S)"
CONF="${FLINK_CONF_DIR:-$FLINK_HOME/conf}/config.yaml"
TEMPLATES="${TEMPLATES:-$FLINK_HOME/conf/templates}"

rm -rf "$CSV_DIR"
mkdir -p "$CSV_DIR"
cp "$TEMPLATES/config-rocksdb.yaml" "$CONF"
cat >> "$CONF" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF

"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f 'TaskManagerRunner|StandaloneSession|SqlGateway|SqlClient' 2>/dev/null || true
sleep 3

JAVA_HOME="$JDK17" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
sleep 5
for _ in $(seq 1 40); do
  tms=$(curl -sf http://localhost:8081/overview 2>/dev/null | grep -oE '"taskmanagers":[0-9]+' | grep -oE '[0-9]+$' || true)
  [ -n "${tms:-}" ] && [ "$tms" -ge 1 ] && break
  sleep 2
done
JAVA_HOME="$JDK17" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
for _ in $(seq 1 30); do
  curl -sf "http://localhost:8083/v1/info" >/dev/null 2>&1 && break
  sleep 2
done

SQLFILE="/tmp/$RUN_ID.sql"
: > "$SQLFILE"
cat >> "$SQLFILE" <<SQL
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '2 s';
SQL

for src in ddl_gen.sql ddl_views.sql; do
  sed -e "s/\${TPS}/$GEN_TPS/g" \
      -e "s/\${EVENTS_NUM}/$EVENTS_NUM/g" \
      -e "s/\${PERSON_PROPORTION}/$PERSON_PROPORTION/g" \
      -e "s/\${AUCTION_PROPORTION}/$AUCTION_PROPORTION/g" \
      -e "s/\${BID_PROPORTION}/$BID_PROPORTION/g" \
      -e "s/\${NEXMARK_TABLE}/datagen/g" \
      "$NEXMARK_HOME/queries/$src" >> "$SQLFILE"
  echo "" >> "$SQLFILE"
done

cat >> "$SQLFILE" <<SQL
CREATE TABLE person_out (
  id BIGINT, name VARCHAR, emailAddress VARCHAR, creditCard VARCHAR,
  city VARCHAR, state VARCHAR, \`dateTime\` TIMESTAMP(3), extra VARCHAR
) WITH ('connector'='filesystem','path'='file://$CSV_DIR/person','format'='csv');

CREATE TABLE auction_out (
  id BIGINT, itemName VARCHAR, description VARCHAR, initialBid BIGINT, reserve BIGINT,
  \`dateTime\` TIMESTAMP(3), expires TIMESTAMP(3), seller BIGINT, category BIGINT, extra VARCHAR
) WITH ('connector'='filesystem','path'='file://$CSV_DIR/auction','format'='csv');

CREATE TABLE bid_out (
  auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR,
  \`dateTime\` TIMESTAMP(3), extra VARCHAR
) WITH ('connector'='filesystem','path'='file://$CSV_DIR/bid','format'='csv');

EXECUTE STATEMENT SET BEGIN
INSERT INTO person_out SELECT * FROM person;
INSERT INTO auction_out SELECT * FROM auction;
INSERT INTO bid_out SELECT * FROM bid;
END;
SQL

CLIENTOUT="/tmp/$RUN_ID.client.out"
JAVA_HOME="$JDK17" "$FLINK_HOME"/bin/sql-client.sh embedded < "$SQLFILE" > "$CLIENTOUT" 2>&1
JID="$(grep -oE "Job ID: [A-Za-z0-9]{32}" "$CLIENTOUT" | head -1 | awk '{print $3}' || true)"
if [ -z "$JID" ]; then
  printf 'GEN_TSV\tSUBMIT_FAILED\t0\n'
  tail -50 "$CLIENTOUT"
  exit 1
fi

printf 'GEN_TSV\tSUBMITTED\t%s\tevents=%s\tgen_tps=%s\tcsv=%s\n' "$JID" "$EVENTS_NUM" "$GEN_TPS" "$CSV_DIR"
start=$(date +%s)
while :; do
  elapsed=$(( $(date +%s) - start ))
  state="$(curl -s --max-time 6 "http://localhost:8081/jobs/$JID" 2>/dev/null | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('state','?'))
except Exception: print('?')" 2>/dev/null)"
  printf 'GEN_TSV\tPOLL\telapsed_s=%s\tstate=%s\n' "$elapsed" "$state"
  case "$state" in
    FINISHED) break ;;
    FAILED|CANCELED)
      curl -s --max-time 6 "http://localhost:8081/jobs/$JID/exceptions" 2>/dev/null \
        | grep -viE 'access[_\.-]?key|secret|credential|authorization|fs\.bos|Classpath:' \
        | head -c 4000
      echo
      exit 1 ;;
  esac
  [ "$elapsed" -ge "${GEN_MAXSEC:-900}" ] && { printf 'GEN_TSV\tTIMEOUT\t%s\n' "$elapsed"; exit 1; }
  sleep 5
done

for d in person auction bid; do
  find "$CSV_DIR/$d" -type f -name '.part-*' | while read -r p; do
    mv "$p" "$(dirname "$p")/$(basename "$p" | sed 's/^\.//;s/\.inprogress\..*$//;s/\.pending$//')"
  done
done

"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true

total=0
for d in person auction bid; do
  rows="$(cat "$CSV_DIR/$d"/* 2>/dev/null | wc -l | tr -d ' ')"
  sha="$(cat "$CSV_DIR/$d"/* 2>/dev/null | sha256sum | awk '{print $1}')"
  printf 'GEN_TSV\t%s\trows=%s\tsha256=%s\n' "$d" "$rows" "$sha"
  total=$((total + rows))
done
printf 'GEN_TSV\ttotal\trows=%s\texpected=%s\n' "$total" "$EVENTS_NUM"
[ "$total" = "$EVENTS_NUM" ] || { printf 'GEN_TSV\tROW_COUNT_MISMATCH\n'; exit 1; }
printf 'GEN_TSV\tOK\t%s\n' "$CSV_DIR"
