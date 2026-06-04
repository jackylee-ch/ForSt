#!/usr/bin/env bash
# Sweep q0-q22 for ONE backend config via the reliable measure-completion.sh
# (fresh cluster per query, JM-polled FINISHED wall-clock). Appends one CSV row
# per query to $OUT. Usage: CONFIG=rocksdb OUT=/tmp/bench/rocksdb.csv bash sweep-config.sh
set -u
CONFIG="${CONFIG:?set CONFIG (rocksdb|forst-rs-ffm-local|forst-rs-ffm-s3)}"
OUT="${OUT:?set OUT csv path}"
MAXSEC="${MAXSEC:-1200}"
MC=/Users/lijunqing/Code/stczwd/ForSt/scripts/measure-completion.sh
mkdir -p "$(dirname "$OUT")"
[ -f "$OUT" ] || echo "config,query,wall_ms,wall_s,src_out,status" > "$OUT"
# q6 is unsupported in Flink SQL itself (both backends) — skip. Run q0..q22.
for n in $(seq 0 22); do
  [ "$n" = "6" ] && { echo "$CONFIG,q6,,,SKIP_UNSUPPORTED" >> "$OUT"; continue; }
  q="q$n"
  # already done? (resume-safe)
  grep -q "^$CONFIG,$q," "$OUT" && { echo "[$CONFIG $q] already in csv, skip"; continue; }
  echo "=== [$CONFIG $q] start $(date +%H:%M:%S) ==="
  QUERY="$q" CONFIG="$CONFIG" MAXSEC="$MAXSEC" bash "$MC" > "/tmp/sweep-$CONFIG-$q.log" 2>&1
  line=$(grep -E "RESULT:|MAXSEC" "/tmp/sweep-$CONFIG-$q.log" | tail -1)
  # measure-completion echoes the RESULT line twice on one physical line, so
  # take only the FIRST match of each field (head -1) to avoid multi-value vars.
  wall_ms=$(echo "$line" | grep -oE 'wall_ms=[0-9]+' | grep -oE '[0-9]+' | head -1); wall_ms=${wall_ms:-}
  src=$(echo "$line" | grep -oE 'src_out=[0-9]+' | grep -oE '[0-9]+' | head -1); src=${src:-}
  if [ -n "$wall_ms" ]; then
    ws=$(awk "BEGIN{printf \"%.1f\", $wall_ms/1000}")
    echo "$CONFIG,$q,$wall_ms,$ws,$src,FINISHED" >> "$OUT"
    echo "[$CONFIG $q] FINISHED ${ws}s src=$src"
  else
    echo "$CONFIG,$q,,,,$( [ -n "$(echo "$line"|grep MAXSEC)" ] && echo TIMEOUT || echo NORESULT)" >> "$OUT"
    echo "[$CONFIG $q] NO RESULT (timeout/err)"
  fi
done
echo "=== SWEEP DONE $CONFIG -> $OUT ==="
awk -F, 'NR>1 && $4!=""{s+=$4} END{printf "TOTAL %s q0-q22 (finished only) = %.1f s\n", "'"$CONFIG"'", s}' "$OUT"
