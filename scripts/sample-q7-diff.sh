#!/usr/bin/env bash
# Differential profile: sample the q7 Join thread at EARLY (~70s, fast) and
# LATE (~340s, decayed) to reveal which per-record CPU frame GROWS with state.
set -u
FLINK=/Users/lijunqing/Downloads/workenv/flink-2.2.1
SRC=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib
cp "$SRC" "$FLINK/lib/libforst_rs_ffi.dylib"
echo "deployed symboled dylib"
QUERY=q7 CONFIG=forst-rs-ffm-local MAXSEC=560 bash /Users/lijunqing/Code/stczwd/ForSt/scripts/measure-completion.sh > /tmp/q7-diff-run.log 2>&1 &
RUNPID=$!
sample_at() {  # $1 = label, waits until measurement-job dur reaches $2 seconds
  local label="$1" target="$2" e
  while :; do
    sleep 8
    e=$(grep 'RUNNING' /tmp/q7-diff-run.log 2>/dev/null | tail -1 | grep -oE 'dur_ms=[0-9]+' | grep -oE '[0-9]+')
    e=${e:-0}; e=$((e/1000))
    kill -0 $RUNPID 2>/dev/null || { echo "run exited before $label"; return 1; }
    [ "$e" -ge "$target" ] && break
  done
  local pid; pid=$(pgrep -f TaskManagerRunner | head -1)
  echo "[$label] measurement-job dur=${e}s TMpid=$pid"
  /usr/bin/sample "$pid" 20 -file "/tmp/q7-sample-$label.txt" -mayDie 2>&1 | tail -1
  echo "[$label] -> /tmp/q7-sample-$label.txt"
}
# EARLY: measurement job ~30s in (state still small, high rate).
sample_at early 30
# LATE: measurement job ~330s in (decayed).
sample_at late 330
kill -9 $RUNPID 2>/dev/null
"$FLINK"/bin/stop-cluster.sh >/dev/null 2>&1
"$FLINK"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'Benchmark|TaskManagerRunner|StandaloneSession|SqlGateway' 2>/dev/null
echo "DIFF PROFILE DONE"
