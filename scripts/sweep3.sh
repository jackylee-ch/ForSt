#!/usr/bin/env bash
# 3-way NEXMark sweep driver: runs the full query suite for one CONFIG at one
# EVENTS_NUM, capturing real JM wall_ms per query + the suite total.
# Usage: CONFIG=rocksdb EVENTS_NUM=10000000 MAXSEC=300 bash scripts/sweep3.sh <tag>
set -u
TAG="${1:-run}"
CONFIG="${CONFIG:-rocksdb}"
EVENTS_NUM="${EVENTS_NUM:-10000000}"
MAXSEC="${MAXSEC:-300}"
# q6 excluded: unsupported in Flink SQL (Column 'rownum' not found) for ALL backends.
QUERIES="q0 q1 q2 q3 q4 q5 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22"
TSV="/tmp/sweep3-$TAG.tsv"
: > "$TSV"
total=0
for Q in $QUERIES; do
  rid="$TAG-$Q-$(date +%H%M%S)"
  QUERY="$Q" CONFIG="$CONFIG" EVENTS_NUM="$EVENTS_NUM" MAXSEC="$MAXSEC" RUN_ID="$rid" \
    bash scripts/measure-sql.sh > "/tmp/sweep3-$TAG-$Q.log" 2>&1
  ms=$(grep -oE "FINISHED wall_ms=[0-9]+" "/tmp/sweep3-$TAG-$Q.log" | head -1 | grep -oE "[0-9]+$")
  if [ -n "$ms" ]; then
    st=FIN; total=$((total + ms))
  else
    ms=$((MAXSEC * 1000)); st=TIMEOUT; total=$((total + ms))
  fi
  printf '%s\t%s\t%s\n' "$Q" "$ms" "$st" | tee -a "$TSV"
done
printf 'TOTAL\t%s\t%.1fs\n' "$total" "$(awk "BEGIN{print $total/1000}")" | tee -a "$TSV"
echo "SWEEP3-DONE tag=$TAG config=$CONFIG events=$EVENTS_NUM"
