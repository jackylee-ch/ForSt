#!/usr/bin/env bash
# Deploy symboled dylib, run q7 to the decay regime, then macOS `sample` the
# TaskManager's native stacks for 30s to capture the build_lazy_prefix hot frame.
set -u
FLINK=/Users/lijunqing/Downloads/workenv/flink-2.2.1
SRC=/Users/lijunqing/Code/stczwd/ForSt/target/release/libforst_rs_ffi.dylib
cp "$SRC" "$FLINK/lib/libforst_rs_ffi.dylib"
echo "deployed symboled dylib"
# Run q7 (NO FRS_ITER_DIAG — clean perf path; symbols still present for sampling).
QUERY=q7 CONFIG=forst-rs-ffm-local MAXSEC=560 bash /Users/lijunqing/Code/stczwd/ForSt/scripts/measure-completion.sh > /tmp/q7-sample-run.log 2>&1 &
RUNPID=$!
# Wait for the measurement job to reach the decay regime (elapsed>=320s, src>=28M).
e=0; src=0
for i in $(seq 1 130); do
  sleep 10
  e=$(grep 'RUNNING' /tmp/q7-sample-run.log 2>/dev/null | tail -1 | grep -oE '^\[[0-9]+' | grep -oE '[0-9]+')
  src=$(grep 'RUNNING' /tmp/q7-sample-run.log 2>/dev/null | tail -1 | grep -oE 'src_out=[0-9]+' | grep -oE '[0-9]+')
  e=${e:-0}; src=${src:-0}
  if [ "$e" -ge 320 ] && [ "$src" -ge 28000000 ]; then break; fi
  # bail if the run died
  kill -0 $RUNPID 2>/dev/null || { echo "run exited early"; break; }
done
TMPID=$(pgrep -f TaskManagerRunner | head -1)
echo "decay reached (elapsed=${e}s src=${src}), TM pid=${TMPID}"
if [ -n "$TMPID" ]; then
  /usr/bin/sample "$TMPID" 30 -file /tmp/q7-sample.txt -mayDie 2>&1 | tail -3
  echo "SAMPLE DONE -> /tmp/q7-sample.txt"
else
  echo "ERROR: no TaskManager pid found"
fi
kill -9 $RUNPID 2>/dev/null
"$FLINK"/bin/stop-cluster.sh >/dev/null 2>&1
"$FLINK"/bin/sql-gateway.sh stop >/dev/null 2>&1
pkill -9 -f 'Benchmark|TaskManagerRunner|StandaloneSession|SqlGateway' 2>/dev/null
echo "STOPPED"
