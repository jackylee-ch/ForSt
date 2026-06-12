#!/usr/bin/env bash
# Generate the FIXED 1M nexmark CSV dataset ONCE (person/auction/bid dirs)
# from the seeded nexmark datagen (deterministic SplittableRandom seeds are
# patched into nexmark/nexmark-flink generator sources). Runs INSIDE the
# forst-bench container. Output: $CSV_DIR (container /tmp = host $WORKENV/frs-tmp).
#
# Both backends then replay this byte-identical dataset, so any output diff is
# a backend defect — not datagen noise.
#
# Env: [CSV_DIR=/tmp/nexmark-fixed-csv-1m] [EVENTS_NUM=1000000]
set -u
export FLINK_HOME="${FLINK_HOME:-/Users/lijunqing/Downloads/workenv/flink-2.2.1}"
NEXMARK_HOME="${NEXMARK_HOME:-/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink}"
export HADOOP_CLASSPATH="$(find "${HADOOP_HOME:-/Users/lijunqing/Downloads/workenv/hadoop-3.4.3}/share/hadoop" -name '*.jar' 2>/dev/null | tr '\n' ':')"
JDK17="${JDK17:-/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home}"
CSV_DIR="${CSV_DIR:-/tmp/nexmark-fixed-csv-1m}"
EVENTS_NUM="${EVENTS_NUM:-1000000}"
# GEN_TPS sets the datagen rate, which ALSO fixes the EVENT-TIME span of the
# dataset (timestamps = baseTime + i/rate). At 10M tps, 1M events span only
# ~0.1s of event time -> a single 10s window -> windowed queries degenerate
# (q5 emitted 5 rows). 10k tps -> ~100s span -> ~50 hop windows, matching the
# remote fixed-CSV baseline profile (q5 = 54 rows). Generation wall time ~100s.
TPS="${GEN_TPS:-10000}"; PP=1; AP=3; BP=46
RUN_ID="gencsv-$(date +%H%M%S)"

rm -rf "$CSV_DIR"; mkdir -p "$CSV_DIR"
CONF="$FLINK_HOME/conf/config.yaml"
TEMPLATES="${TEMPLATES:-$FLINK_HOME/conf/templates}"
cp "$TEMPLATES/config-rocksdb.yaml" "$CONF"
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
JAVA_HOME="$JDK17" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
sleep 5
for i in $(seq 1 30); do
  tms=$(curl -sf http://localhost:8081/overview 2>/dev/null | grep -oE '"taskmanagers":[0-9]+' | grep -oE '[0-9]+$' || true)
  [ -n "$tms" ] && [ "$tms" -ge 1 ] && break; sleep 3
done
JAVA_HOME="$JDK17" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
for i in $(seq 1 20); do curl -sf "http://localhost:8083/v1/info" >/dev/null 2>&1 && break; sleep 2; done
echo "cluster up, generating $EVENTS_NUM events -> $CSV_DIR"

build_sql() {  # $1 = runtime mode (batch|streaming)
  # NOTE: no `local x="$1"` here — the bench image's login profile breaks
  # `local` + `set -u` ("unbound variable" even when the arg is passed).
  mode="$1"; f="/tmp/gencsv-$RUN_ID-$mode.sql"
  : > "$f"
  echo "SET 'parallelism.default' = '1';" >> "$f"
  if [ "$mode" = "batch" ]; then
    echo "SET 'execution.runtime-mode' = 'batch';" >> "$f"
  else
    echo "SET 'execution.checkpointing.interval' = '2 s';" >> "$f"
  fi
  for src in ddl_gen.sql ddl_views.sql; do
    sed -e "s/\${TPS}/$TPS/g" -e "s/\${EVENTS_NUM}/$EVENTS_NUM/g" \
        -e "s/\${PERSON_PROPORTION}/$PP/g" -e "s/\${AUCTION_PROPORTION}/$AP/g" \
        -e "s/\${BID_PROPORTION}/$BP/g" -e "s/\${NEXMARK_TABLE}/datagen/g" \
        "$NEXMARK_HOME/queries/$src" >> "$f"
    echo "" >> "$f"
  done
  cat >> "$f" <<SQL
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
  echo "$f"
}

submit_and_wait() {  # $1 = sqlfile ; returns 0 on FINISHED
  f="$1"; out="/tmp/gencsv-client-$RUN_ID.out"
  JAVA_HOME="$JDK17" "$FLINK_HOME"/bin/sql-client.sh embedded < "$f" > "$out" 2>&1
  jid=$(grep -oE "Job ID: [A-Za-z0-9]{32}" "$out" | head -1 | awk '{print $3}' || true)
  if [ -z "$jid" ]; then echo "submit failed:"; tail -30 "$out"; return 1; fi
  echo "gen job $jid submitted, polling..."
  start=$(date +%s)
  while :; do
    el=$(( $(date +%s) - start ))
    st=$(curl -s --max-time 6 "http://localhost:8081/jobs/$jid" 2>/dev/null | python3 -c "import json,sys
try: print(json.load(sys.stdin).get('state','?'))
except Exception: print('?')" 2>/dev/null)
    echo "[$el s] gen state=$st"
    case "$st" in
      FINISHED) return 0 ;;
      FAILED|CANCELED) curl -s "http://localhost:8081/jobs/$jid/exceptions" | head -c 2000; echo; return 1 ;;
    esac
    [ "$el" -ge 600 ] && { echo "gen TIMEOUT"; return 1; }
    sleep 5
  done
}

# Streaming mode only: the nexmark source declares itself UNBOUNDED, so batch
# runtime rejects it ("Querying an unbounded table ... in batch mode").
submit_and_wait "$(build_sql streaming)" || { echo "GEN FAILED"; exit 1; }
# Streaming sink may leave committed-but-hidden part files: promote them.
for d in person auction bid; do
  find "$CSV_DIR/$d" -type f -name '.part-*' | while read -r p; do
    mv "$p" "$(dirname "$p")/$(basename "$p" | sed 's/^\.//;s/\.inprogress\..*$//;s/\.pending$//')"
  done
done
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1

echo "=== fixed CSV dataset summary ==="
total=0
for d in person auction bid; do
  n=$(cat "$CSV_DIR/$d"/* 2>/dev/null | wc -l | tr -d ' ')
  h=$(cat "$CSV_DIR/$d"/* 2>/dev/null | shasum -a 256 2>/dev/null | awk '{print $1}')
  [ -n "$h" ] || h=$(cat "$CSV_DIR/$d"/* 2>/dev/null | sha256sum | awk '{print $1}')
  echo "GEN_TSV	$d	rows=$n	sha256=$h"
  total=$((total + n))
done
echo "GEN_TSV	total	rows=$total	expected=$EVENTS_NUM"
[ "$total" = "$EVENTS_NUM" ] || { echo "GEN ROW-COUNT MISMATCH"; exit 1; }
echo "GEN OK: $CSV_DIR"
