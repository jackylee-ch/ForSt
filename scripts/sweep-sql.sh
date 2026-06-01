#!/usr/bin/env bash
# 2026-05-29 Full q0..q22 sweep via the reliable measure-sql.sh bypass (no nexmark
# monitoring). Runs each query to FINISHED on a fresh cluster, reads the real JM
# wall-clock, and tallies the q0~q22 TOTAL per config — the ONE metric that matters
# for the 3x goal. Run BOTH configs then compare totals.
#
# bash 3.2 compatible (macOS default — no associative arrays): results are written
# to $OUT/results.tsv ("cfg<TAB>q<TAB>ms<TAB>state") and tallied with awk.
#
# Secrets come from the environment only (S3_ENDPOINT/ACCESS_KEY/SECRET_KEY/
# BUCKET/REGION/PREFIX) — even local config uses S3 for checkpoints. Bridge inline:
#   export S3_SECRET="${S3_SECRET_KEY}"
#
# Usage:
#   CONFIGS="rocksdb forst-rs-ffm-s3" QUERIES="q0..q22" MAXSEC=1500 bash sweep-sql.sh
#   (QUERIES defaults to q0..q22; override e.g. QUERIES="q3 q7 q8 q9" for a group)
set -u
HERE="$(cd "$(dirname "$0")"; pwd)"
CONFIGS="${CONFIGS:-rocksdb forst-rs-ffm-s3}"
MAXSEC="${MAXSEC:-1500}"
if [ "${QUERIES:-q0..q22}" = "q0..q22" ]; then
  QUERIES=""
  for i in $(seq 0 22); do QUERIES="$QUERIES q$i"; done
fi
OUT="/tmp/sweep-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT"
RES="$OUT/results.tsv"
: > "$RES"
echo "sweep → $OUT  | configs: $CONFIGS | maxsec: $MAXSEC"

for cfg in $CONFIGS; do
  total=0; missing=""
  for q in $QUERIES; do
    echo "==== $cfg / $q ===="
    QUERY="$q" CONFIG="$cfg" MAXSEC="$MAXSEC" bash "$HERE/measure-sql.sh" > "$OUT/$cfg-$q.log" 2>&1
    line=$(grep "^RESULT:" "$OUT/$cfg-$q.log" | tail -1)
    ms=$(echo "$line" | grep -oE "wall_ms=[0-9]+" | grep -oE "[0-9]+" | head -1)
    st=$(echo "$line" | grep -oE "FINISHED|ENDED" | head -1)
    if [ -n "$ms" ] && [ "$st" = "FINISHED" ]; then
      total=$((total+ms))
      printf '%s\t%s\t%s\tFINISHED\n' "$cfg" "$q" "$ms" >> "$RES"
      printf '  %s %s = %.1fs\n' "$cfg" "$q" "$(awk "BEGIN{print $ms/1000}")"
    else
      stx=$(echo "$line" | grep -oE 'state=[A-Z]+' || echo 'NA')
      printf '%s\t%s\tNA\t%s\n' "$cfg" "$q" "$stx" >> "$RES"
      missing="$missing $q"
      echo "  $cfg $q = NA ($stx)"
    fi
  done
  echo "TOTAL[$cfg] = $(awk "BEGIN{printf \"%.1f\", $total/1000}")s  (missing/non-finished:${missing:- none})"
done

echo "================= SUMMARY ================="
awk -F'\t' -v configs="$CONFIGS" -v queries="$QUERIES" '
  { ms[$1"-"$2]=$3 }
  END {
    nc=split(configs,C," "); nq=split(queries,Q," ");
    printf "%-8s","query"; for(i=1;i<=nc;i++) printf "%-22s",C[i]; print "";
    for(j=1;j<=nq;j++){
      printf "%-8s",Q[j];
      for(i=1;i<=nc;i++){ v=ms[C[i]"-"Q[j]];
        if(v=="" || v=="NA") printf "%-22s","NA";
        else printf "%-22s",sprintf("%.1fs",v/1000); }
      print "";
    }
    print "------------------------------------------";
    for(i=1;i<=nc;i++){ tot[C[i]]=0;
      for(j=1;j<=nq;j++){ v=ms[C[i]"-"Q[j]]; if(v!="" && v!="NA") tot[C[i]]+=v; }
      printf "TOTAL[%s] = %.1fs\n", C[i], tot[C[i]]/1000;
    }
    base="rocksdb";
    for(i=1;i<=nc;i++){ if(C[i]==base) continue;
      bt=0; tt=0; common=0;
      for(j=1;j<=nq;j++){ b=ms[base"-"Q[j]]; t=ms[C[i]"-"Q[j]];
        if(b!="" && b!="NA" && t!="" && t!="NA"){ bt+=b; tt+=t; common++; } }
      if(tt>0) printf "%s vs %s (over %d common-FINISHED q): %.2fx (base %.1fs / target %.1fs)\n", C[i], base, common, bt/tt, bt/1000, tt/1000;
    }
  }' "$RES"
echo "logs in $OUT"
