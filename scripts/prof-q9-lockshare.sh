#!/usr/bin/env bash
# Phase-3 GATE measurement (2026-06-02): profile a q9 heavy-join (LOCAL) at
# steady state to decide whether the lock-free memtable phase 3 (remove the
# per-shard RwLock) is worth its risk, and which fork (A skiplist-only vs B
# concurrent hash_index) to take. Captures, from one macOS `sample` of the
# TaskManager native stacks:
#   (1) RwLock / lock-contention share in the Join thread  -> phase-3 go/no-go
#   (2) point-read (get/hash_index) vs scan (prefix/range/skiplist) share -> A/B
# Also records the HEAD q9 wall-clock for the "did phase 2 move runtime" check.
set -u
FLINK=/Users/lijunqing/Downloads/workenv/flink-2.2.1
SRC=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib
OUT=/tmp/q9-prof-head.txt
LOG=/tmp/q9-prof-run.log
TPL="$FLINK/conf/templates/config-forst-rs-local.yaml.tpl"
# MEASUREMENT EXPEDIENT: the FORSTRS timer currently fails to init for
# timer-using queries (stateName '/' bug — tracked separately as a directive-3
# blocker). The memtable lock-share we are profiling is independent of the
# timer backend, so flip the timer to HEAP for this run ONLY and restore
# FORSTRS on exit so the committed config intent is preserved.
cp "$TPL" "$TPL.bak-prof"
trap 'cp "$TPL.bak-prof" "$TPL"; rm -f "$TPL.bak-prof"; echo "restored FORSTRS timer template"' EXIT
sed -i '' 's/timer-service.factory=FORSTRS/timer-service.factory=HEAP/g' "$TPL"
echo "timer flipped to HEAP for measurement: $(grep -o 'timer-service.factory=[A-Z]*' "$TPL" | sort -u | tr '\n' ' ')"
cp "$SRC" "$FLINK/lib/libforst_rs_ffi.dylib"
echo "deployed symboled dylib ($(date)) sha=$(cd /Users/lijunqing/Code/stczwd/ForSt && git rev-parse --short HEAD)"
QUERY=q9 CONFIG=forst-rs-ffm-local MAXSEC=900 bash /Users/lijunqing/Code/stczwd/ForSt/scripts/measure-completion.sh > "$LOG" 2>&1 &
RUNPID=$!
# Wait for a warm join regime: measurement-job dur_ms>=120s and src_out>=5M.
warm=0
for i in $(seq 1 150); do
  sleep 8
  line=$(grep -E '\] (RUNNING|FINISHED) ' "$LOG" 2>/dev/null | tail -1)
  d=$(echo "$line" | grep -oE 'dur_ms=[0-9]+' | grep -oE '[0-9]+'); d=${d:-0}
  s=$(echo "$line" | grep -oE 'src_out=[0-9]+' | grep -oE '[0-9]+'); s=${s:-0}
  kill -0 $RUNPID 2>/dev/null || { echo "run exited early before warm"; break; }
  echo "wait: dur_ms=$d src_out=$s"
  echo "$line" | grep -q ' FINISHED ' && { echo "FINISHED before warm-sample"; break; }
  if [ "$d" -ge 120000 ] && [ "$s" -ge 5000000 ]; then warm=1; break; fi
done
if [ "$warm" = 1 ]; then
  PID=$(pgrep -f TaskManagerRunner | head -1)
  echo "WARM reached, sampling TM pid=$PID for 30s"
  /usr/bin/sample "$PID" 30 -file "$OUT" -mayDie 2>&1 | tail -2
  echo "SAMPLE -> $OUT"
fi
wait $RUNPID 2>/dev/null
echo "=== RESULT ==="; grep -E 'RESULT|MAXSEC' "$LOG" | tail -3
"$FLINK"/bin/stop-cluster.sh >/dev/null 2>&1
"$FLINK"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'Benchmark|TaskManagerRunner|StandaloneSession|SqlGateway' 2>/dev/null
echo "STOPPED ($(date))"
