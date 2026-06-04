#!/usr/bin/env bash
# q7 timed out at the sweep's MAXSEC=1200 (rocksdb confirmed; forst-rs likely
# too). The user needs q7's real execution time. This waits for the in-flight
# sweep to release the local cluster, then reruns q7 for both backends with a
# large cap (finishes-when-done; cap only bounds a true hang) and patches the
# q7 row into each CSV.
set -u
MC=/Users/lijunqing/Code/stczwd/ForSt/scripts/measure-completion.sh
Q7MAX="${Q7MAX:-10800}"   # 3h cap
mkdir -p /tmp/bench
# 1. Wait for any running sweep to finish (shared local cluster).
while pgrep -f 'sweep-config' >/dev/null 2>&1; do sleep 30; done
echo "=== sweep released cluster; q7 reruns start $(date) (Q7MAX=${Q7MAX}s) ==="
pkill -9 -f 'TaskManagerRunner|StandaloneSession|SqlGateway' 2>/dev/null; sleep 3

run_q7() {  # $1=config  $2=csv
  local cfg="$1" csv="$2" log="/tmp/bench/q7-rerun-$1.log"
  echo "=== q7 rerun [$cfg] $(date) ==="
  QUERY=q7 CONFIG="$cfg" MAXSEC="$Q7MAX" bash "$MC" > "$log" 2>&1
  local line ms src
  line=$(grep -E "RESULT:|MAXSEC" "$log" | tail -1)
  ms=$(echo "$line" | grep -oE 'wall_ms=[0-9]+' | grep -oE '[0-9]+' | head -1)
  src=$(echo "$line" | grep -oE 'src_out=[0-9]+' | grep -oE '[0-9]+' | head -1)
  # Drop any existing q7 row(s) (clean single-line TIMEOUT/placeholder), append fresh.
  [ -f "$csv" ] && { grep -v "^$cfg,q7," "$csv" > "$csv.tmp" 2>/dev/null; mv "$csv.tmp" "$csv"; }
  if [ -n "$ms" ]; then
    echo "$cfg,q7,$ms,$(awk "BEGIN{printf \"%.1f\",$ms/1000}"),${src:-},FINISHED" >> "$csv"
    echo "[$cfg q7] FINISHED ${ms}ms src=${src:-?}"
  else
    echo "$cfg,q7,,,,DNF_${Q7MAX}s" >> "$csv"
    echo "[$cfg q7] DID NOT FINISH within ${Q7MAX}s"
  fi
}

run_q7 rocksdb /tmp/bench/rocksdb-local.csv
run_q7 forst-rs-ffm-local /tmp/bench/forst-rs-local.csv
echo "=== Q7 RERUNS DONE $(date) ==="
