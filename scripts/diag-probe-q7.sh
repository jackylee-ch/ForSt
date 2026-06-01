#!/usr/bin/env bash
# Time-boxed q7 with FRS_PROBE_DIAG=1 to split per-probe frs_vec_iter_prefix_open
# latency into BUILD (merge construction / reader opens) vs FILL (get_arc drain).
# Runs ~480s (past the ~21-23M stall) then aggregates the slow-probe breakdown.
set -uo pipefail
export FLINK_HOME=/Users/lijunqing/Downloads/workenv/flink-2.2.1
export NEXMARK_HOME=/Users/lijunqing/Code/stczwd/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink
export HADOOP_CLASSPATH="$(find /Users/lijunqing/Downloads/workenv/hadoop-3.4.3/share/hadoop -name '*.jar' | tr '\n' ':')"
JDK25=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
Q=q7; OUT=/tmp/diag-probe; rm -rf "$OUT"; mkdir -p "$OUT"
export RUN_ID="probe-$(date +%s)"
export FRS_PROBE_DIAG=1
S3VARS='${S3_ENDPOINT} ${S3_ACCESS_KEY} ${S3_SECRET_KEY} ${S3_BUCKET} ${S3_REGION} ${S3_PREFIX} ${RUN_ID}'
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f "nexmark.flink.Benchmark|TaskManagerRunner|StandaloneSession" 2>/dev/null; sleep 3
rm -rf /tmp/flink-forst-rs-io /tmp/flink-forst-rs-cache /tmp/flink-forst-rs-data /tmp/nexmark-checkpoints-forst-rs
rm -f "$FLINK_HOME"/log/*taskexecutor*.out
envsubst "$S3VARS" < "$FLINK_HOME/conf/templates/config-forst-rs-local.yaml.tpl" > "$FLINK_HOME/conf/config.yaml"
cat >> "$FLINK_HOME/conf/config.yaml" <<'EOF'

sql-gateway:
  endpoint:
    rest:
      address: localhost
      port: 8083
      bind-port: 8083
EOF
JAVA_HOME="$JDK25" FRS_PROBE_DIAG=1 "$FLINK_HOME"/bin/start-cluster.sh >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf http://localhost:8081/jobs >/dev/null 2>&1 && break; sleep 1; done
JAVA_HOME="$JDK25" "$FLINK_HOME"/bin/sql-gateway.sh start >/dev/null 2>&1
for i in $(seq 1 30); do curl -sf http://localhost:8083/v1/info >/dev/null 2>&1 && break; sleep 1; done
JAVA_HOME="$JDK25" "$NEXMARK_HOME"/bin/run_query.sh oa "$Q" > "$OUT/run.out" 2>&1 &
echo "running ~480s to reach the stall..."
sleep 480
TO=$(ls -t "$FLINK_HOME"/log/*taskexecutor*.out 2>/dev/null | head -1)
echo "=== FRS-PROBE-DIAG slow-probe count ==="
grep -c "FRS-PROBE-DIAG" "$TO" 2>/dev/null
echo "=== build_us vs fill_us distribution (which dominates slow probes?) ==="
grep "FRS-PROBE-DIAG" "$TO" 2>/dev/null | python3 -c "
import sys,re
builds=[]; fills=[]; totals=[]
for line in sys.stdin:
    m=re.search(r'total_us=(\d+) build_us=(\d+) fill_us=(\d+)',line)
    if m:
        t,b,f=int(m.group(1)),int(m.group(2)),int(m.group(3))
        totals.append(t); builds.append(b); fills.append(f)
n=len(totals)
if n==0: print('no slow probes logged'); sys.exit()
def pct(x,p): xs=sorted(x); return xs[min(len(xs)-1,int(len(xs)*p/100))]
print(f'slow probes: {n}')
print(f'total_us  p50={pct(totals,50)} p90={pct(totals,90)} max={max(totals)}')
print(f'build_us  p50={pct(builds,50)} p90={pct(builds,90)} max={max(builds)} sum={sum(builds)}')
print(f'fill_us   p50={pct(fills,50)} p90={pct(fills,90)} max={max(fills)} sum={sum(fills)}')
bd=sum(builds); fl=sum(fills); tot=bd+fl
print(f'BUILD share={100*bd//max(1,tot)}%  FILL share={100*fl//max(1,tot)}%')
" 2>/dev/null
echo "=== sample slow-probe lines ==="
grep "FRS-PROBE-DIAG" "$TO" 2>/dev/null | tail -6
echo "=== FRS-ITER-DIAG (build internals) if any ==="
grep "FRS-ITER-DIAG" "$TO" 2>/dev/null | tail -4
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f "nexmark.flink.Benchmark|TaskManagerRunner|StandaloneSession" 2>/dev/null
echo "=== DONE ==="
