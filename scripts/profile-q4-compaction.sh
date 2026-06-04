#!/usr/bin/env bash
# Attribute the q4 periodic COMPACTION STALL (the identified wall-clock binder).
# Samples the "forst-rs-compact" thread (and the pipeline) during q4 and
# classifies the compaction thread's RUNNABLE frames into:
#   MERGE   — full_merge / collect_merge_operands / MergeOperator (operand-chain
#             collapse; an ENGINE lever, not machine-bound)
#   IO      — read_at / pread / write / StreamingSstWriter / upload / get_range
#             (disk/object-store; machine-bound on this dev Mac)
#   DECODE  — decode_data_block / decompress / Arrow / kv_block
#   KEYCMP  — InternalKey / compare / search_index / BTree
#   OTHER
# If MERGE/DECODE/KEYCMP dominate → the stall is CPU/engine work (a real lever).
# If IO dominates → disk-bound (machine-bound → needs the co-located cloud box).
set -uo pipefail
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
Q=q4
OUT=/tmp/profile-q4-compaction
rm -rf "$OUT"; mkdir -p "$OUT"
S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'
export RUN_ID="prof-$(date +%s)"
export FRS_BLOCK_SIZE_KB=8 FRS_BLOCK_CACHE_MB=4096 FRS_SST_COMPRESSION=none FRS_RESIDENT_SHADOW_TOTAL_MB=2048
export FRS_COMPACT_DIAG=1

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
TMPID=$(jps 2>/dev/null | grep -iE "TaskManagerRunner" | awk '{print $1}' | head -1)
echo "TM pid=$TMPID status=$st"
# Sample densely from t≈80s..240s (covers >1 compaction burst). Also sample
# iostat to see if the disk saturates during bursts.
sleep 70
( iostat -d -w 2 disk0 > "$OUT/iostat.txt" 2>/dev/null & echo $! > "$OUT/iostat.pid" )
for i in $(seq 1 160); do
  "$JDK25/bin/jstack" "$TMPID" >> "$OUT/stacks.txt" 2>/dev/null || true
  echo "---SAMPLE $i $(date +%s)---" >> "$OUT/stacks.txt"
  sleep 1
done
kill "$(cat "$OUT/iostat.pid")" 2>/dev/null || true
kill "$RUNPID" 2>/dev/null
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
pkill -f TaskManagerRunner 2>/dev/null; pkill -9 -f "nexmark.flink.Benchmark" 2>/dev/null

python3 - "$OUT/stacks.txt" <<'PY'
import sys, re
from collections import Counter
txt = open(sys.argv[1], encoding="utf-8", errors="ignore").read()
blocks = re.split(r'\n(?=")', txt)
MERGE  = re.compile(r'full_merge|collect_merge_operands|MergeOperator|merge_operands|peel_merges|RawConcat', re.I)
IO     = re.compile(r'read_at|pread|read_exact|StreamingSstWriter|write_all|flush_block|upload|get_range|open_random|read_block|serial_read', re.I)
DECODE = re.compile(r'decode_data_block|decompress|RecordBatch|arrow|kv_block|DecodedBlock|for_each_row', re.I)
KEYCMP = re.compile(r'InternalKey|search_index|find_lower_bound|BTree|\bcmp\b|compare|partition_point', re.I)
cat=Counter(); topframe=Counter(); states=Counter(); samples=0
for b in blocks:
    if not b.startswith('"forst-rs-compact'): continue
    samples += 1
    st = re.search(r'java\.lang\.Thread\.State: (\w+)', b)
    states[st.group(1) if st else "?"] += 1
    if 'RUNNABLE' not in b:
        continue
    if   MERGE.search(b):  cat["MERGE"]+=1
    elif DECODE.search(b): cat["DECODE"]+=1
    elif IO.search(b):     cat["IO"]+=1
    elif KEYCMP.search(b): cat["KEYCMP"]+=1
    else:                  cat["OTHER"]+=1
    # top forst_rs frame
    for line in b.splitlines():
        line=line.strip()
        if line.startswith("at ") and ('forst_rs' in line or 'forstrs' in line):
            topframe[line[3:].split("(")[0]] += 1; break
print(f"=== forst-rs-compact thread: {samples} samples, states={dict(states)} ===")
run = sum(cat.values()) or 1
print(f"--- RUNNABLE classification (n={run}) ---")
for k,v in cat.most_common(): print(f"  {k:8s} {v:5d}  {100*v/run:5.1f}%")
print("--- top forst_rs frames in compact thread ---")
for f,c in topframe.most_common(15): print(f"  {c:4d}  {f}")
PY
echo "=== iostat during run (disk0 MB/s) — saturation during bursts? ==="
awk 'NR<=3 || NR%5==0' "$OUT/iostat.txt" 2>/dev/null | tail -25
echo "=== COMPACT_DIAG durations ==="
grep -h COMPACT_DIAG "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null | grep -vE "l0 0->0" | tail -12
