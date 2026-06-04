#!/usr/bin/env bash
# q4 CPU-split GATING profile (gates the whole B_resident direction).
# Samples the forst-rs-local q4 TaskManager thread stacks and classifies every
# RUNNABLE application frame into one of three buckets:
#   ENGINE     — the native read path (FFM downcall → libforst: build_lazy_prefix
#                / get / scan), incl. java.lang.foreign / jdk.internal.foreign
#                trampolines and the forstrs backend state access.
#   JOIN       — Flink interval-join operator CPU (IntervalJoin / Temporal join /
#                MapState / findRow / drainInflight / CopyingChainingOutput /
#                KeyedProcess / processElement / watermark).
#   CHECKPOINT — snapshot / upload / checkpoint async work.
# The ENGINE:JOIN:CHECKPOINT ratio is the veto: only pursue an engine read-path
# (B_resident) fix if ENGINE dominates; else pivot to JOIN or CHECKPOINT.
# Usage: ./profile-q4-cpusplit.sh
set -uo pipefail
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
Q=q4
OUT=/tmp/profile-q4-cpusplit
rm -rf "$OUT"; mkdir -p "$OUT"
S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'
export RUN_ID="prof-$(date +%s)"
# Same q4 regime as the bench/attribution runs so the split reflects the floor.
export FRS_BLOCK_SIZE_KB=8 FRS_BLOCK_CACHE_MB=4096 FRS_SST_COMPRESSION=none FRS_RESIDENT_SHADOW_TOTAL_MB=2048

"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f "nexmark.flink.Benchmark" 2>/dev/null; pkill -f TaskManagerRunner 2>/dev/null; pkill -f StandaloneSession 2>/dev/null; sleep 3
rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data /tmp/nexmark-checkpoints-forst-rs
envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs-local.yaml.tpl" > "$FLINK_HOME/conf/config.yaml"
cat >> "$FLINK_HOME/conf/config.yaml" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF
JAVA_HOME="$JDK25" "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf http://localhost:8081/jobs >/dev/null 2>&1 && break; sleep 1; done
JAVA_HOME="$JDK25" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf http://localhost:8083/v1/info >/dev/null 2>&1 && break; sleep 1; done

JAVA_HOME="$JDK25" "$NEXMARK_HOME"/bin/run_query.sh oa "$Q" > "$OUT/run.out" 2>&1 &
RUNPID=$!
for i in $(seq 1 40); do
  st=$(curl -sf http://localhost:8081/jobs 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);print(next((j['status'] for j in d.get('jobs',[])),'NONE'))" 2>/dev/null)
  [ "$st" = "RUNNING" ] && break; sleep 2
done
echo "job status=$st"
TMPID=$(jps 2>/dev/null | grep -iE "TaskManagerRunner" | awk '{print $1}' | head -1)
echo "TM pid=$TMPID"
# Warm past the burst into the decay floor (where A/B were attributed), then
# sample across >1 compaction-stall period (~120s).
sleep 90
for i in $(seq 1 100); do
  "$JDK25/bin/jstack" "$TMPID" >> "$OUT/stacks.txt" 2>/dev/null || true
  echo "---SAMPLE $i---" >> "$OUT/stacks.txt"
  sleep 1.5
done

kill "$RUNPID" 2>/dev/null
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
pkill -f TaskManagerRunner 2>/dev/null; pkill -9 -f "nexmark.flink.Benchmark" 2>/dev/null

# --- classify RUNNABLE application frames into ENGINE / JOIN / CHECKPOINT ---
# Only count threads that are RUNNABLE (on-CPU); park/wait/epoll are idle.
python3 - "$OUT/stacks.txt" <<'PY'
import sys, re
stacks = open(sys.argv[1], encoding="utf-8", errors="ignore").read()
# Split into per-thread blocks (a thread block starts with a quoted name line).
blocks = re.split(r'\n(?=")', stacks)
ENGINE = re.compile(r'forst_rs|java\.lang\.foreign|jdk\.internal\.foreign|VarHandle|state\.forstrs', re.I)
JOIN   = re.compile(r'IntervalJoin|RowtimeJoin|TemporalRowTimeJoin|TimeIntervalJoin|MapStateCache|findRow|drainInflight|CopyingChainingOutput|KeyedProcess|processElement|processWatermark|RowDataSerializer|BinaryRow', re.I)
CKPT   = re.compile(r'[Cc]heckpoint|[Ss]napshot|uploadFiles|AsyncSnapshot|RocksDBStateUpload|persist|notifyCheckpointComplete', re.I)
counts = {"ENGINE":0,"JOIN":0,"CHECKPOINT":0,"OTHER_RUNNABLE":0}
task_threads = 0
for b in blocks:
    if '"' not in b: continue
    head = b.splitlines()[0]
    # only the pipeline task / checkpoint / source threads do query work
    if not re.search(r'(Source|Join|Process|Window|Sink|Map|Calc|Operator|Checkpoint|AsyncOperations|flink)', head, re.I):
        continue
    if 'java.lang.Thread.State: RUNNABLE' not in b:
        continue
    task_threads += 1
    # find the highest-priority category present in this RUNNABLE stack
    if CKPT.search(b):      counts["CHECKPOINT"] += 1
    elif ENGINE.search(b):  counts["ENGINE"] += 1
    elif JOIN.search(b):    counts["JOIN"] += 1
    else:                   counts["OTHER_RUNNABLE"] += 1
tot = sum(counts.values()) or 1
print(f"=== q4 CPU-split (RUNNABLE task/ckpt thread samples, n={tot}) ===")
for k in ("ENGINE","JOIN","CHECKPOINT","OTHER_RUNNABLE"):
    print(f"  {k:16s} {counts[k]:6d}  {100*counts[k]/tot:5.1f}%")
print("VETO: pursue B (engine read-path) ONLY if ENGINE dominates; else pivot to JOIN/CHECKPOINT.")
PY
echo "(raw stacks: $OUT/stacks.txt)"
