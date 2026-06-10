#!/usr/bin/env bash
set -euo pipefail

VARIANT="${VARIANT:-forst-local}"
QUERY="${QUERY:-q4}"
EVENTS_NUM="${EVENTS_NUM:-100000000}"
TPS="${TPS:-10000000}"
MAXSEC="${MAXSEC:-3600}"
RUN_ID="${RUN_ID:-${VARIANT}-${QUERY}-${EVENTS_NUM}-$(date +%Y%m%d%H%M%S)}"
PARALLELISM="${PARALLELISM:-8}"
TM_SLOTS="${TM_SLOTS:-$PARALLELISM}"
RUNTIME_MODE="${RUNTIME_MODE:-STREAMING}"
CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL:-30 s}"
BENCH_DIR="/bench"
RUN_DIR="$BENCH_DIR/state/$RUN_ID"
LOG_DIR="$BENCH_DIR/logs/$RUN_ID"
RESULT_LOG="$BENCH_DIR/results/$RUN_ID.log"
TEMPLATE_DIR="$BENCH_DIR/templates/$RUN_ID"

mkdir -p "$RUN_DIR" "$LOG_DIR" "$BENCH_DIR/results" "$TEMPLATE_DIR" "$BENCH_DIR/tmp/$RUN_ID/java"
exec > >(tee -a "$RESULT_LOG") 2>&1

echo "=== run metadata ==="
date -Is
echo "variant=$VARIANT query=$QUERY events=$EVENTS_NUM tps=$TPS maxsec=$MAXSEC parallelism=$PARALLELISM slots=$TM_SLOTS runtime_mode=$RUNTIME_MODE checkpoint_interval=$CHECKPOINT_INTERVAL run_id=$RUN_ID"
echo "image=$(cat /etc/os-release | grep PRETTY_NAME | cut -d= -f2-) java=$(java -version 2>&1 | head -1)"
echo "cpu_limit=$(cat /sys/fs/cgroup/cpu.max 2>/dev/null || true) mem_limit=$(cat /sys/fs/cgroup/memory.max 2>/dev/null || true)"

case "$VARIANT" in
  forst-local)
    export LD_LIBRARY_PATH=""
    unset JAVA_TOOL_OPTIONS || true
    EXTRA_JAVA_OPTS=""
    ;;
  forst-rs-lib)
    test -f "$BENCH_DIR/native/libforstjni.so"
    chmod +x "$BENCH_DIR/native/libforstjni.so" || true
    export LD_LIBRARY_PATH="$BENCH_DIR/native"
    export JAVA_TOOL_OPTIONS="-Djava.library.path=$BENCH_DIR/native"
    EXTRA_JAVA_OPTS="-Djava.library.path=$BENCH_DIR/native"
    ;;
  *)
    echo "unknown VARIANT=$VARIANT" >&2
    exit 2
    ;;
esac
export MALLOC_CONF="background_thread:true,dirty_decay_ms:10000,muzzy_decay_ms:10000"
JEMALLOC_LIB="${JEMALLOC_LIB:-/lib/x86_64-linux-gnu/libjemalloc.so.2}"
if [ -f "$JEMALLOC_LIB" ]; then
  export LD_PRELOAD="${LD_PRELOAD:+$LD_PRELOAD:}$JEMALLOC_LIB"
else
  echo "WARN: jemalloc not found at $JEMALLOC_LIB"
fi
export FLINK_HOME="/opt/flink"
export HADOOP_HOME="/opt/hadoop"
export JDK17="/opt/java/openjdk"
export JDK25="/opt/java/openjdk"
export NEXMARK_HOME="/src/ForSt/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink"
export TEMPLATES="$TEMPLATE_DIR"
export CONFIG="forst-local"
export RUN_ID QUERY EVENTS_NUM TPS MAXSEC
export FLINK_LOG_DIR="$LOG_DIR"

if [ ! -d "$NEXMARK_HOME/queries" ]; then
  echo "NEXMARK_HOME missing queries: $NEXMARK_HOME" >&2
  exit 3
fi

echo "=== install matching community forstjni jar ==="
rm -f "$FLINK_HOME"/lib/forstjni-0.1.8.jar
cp "$BENCH_DIR"/native/forstjni-0.1.8-community.jar "$FLINK_HOME"/lib/forstjni-0.1.8.jar
sha256sum "$FLINK_HOME"/lib/forstjni-0.1.8.jar

echo "=== install nexmark connector jar ==="
rm -f "$FLINK_HOME"/lib/nexmark-flink-*.jar
cp "$NEXMARK_HOME"/lib/nexmark-flink-0.3-SNAPSHOT.jar "$FLINK_HOME"/lib/
ls -lh "$FLINK_HOME"/lib/nexmark-flink-0.3-SNAPSHOT.jar
if [ -f "$BENCH_DIR/lib/accuracy-file-sink.jar" ]; then
  echo "=== install accuracy file sink jar ==="
  cp "$BENCH_DIR/lib/accuracy-file-sink.jar" "$FLINK_HOME"/lib/
  ls -lh "$FLINK_HOME"/lib/accuracy-file-sink.jar
fi

echo "LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}"
echo "LD_PRELOAD=${LD_PRELOAD:-}"
echo "JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS:-}"
if [ "$VARIANT" = "forst-rs-lib" ]; then
  echo "forst-rs compat lib sha256=$(sha256sum $BENCH_DIR/native/libforstjni.so | awk '{print $1}')"
  if command -v nm >/dev/null 2>&1; then
    nm -D "$BENCH_DIR/native/libforstjni.so" | grep -m1 Java_org_forstdb_RocksDB_version || true
  else
    echo "nm unavailable inside container; skip symbol check"
  fi
fi

rm -rf "$RUN_DIR" "$BENCH_DIR/tmp/$RUN_ID"
mkdir -p "$RUN_DIR/primary" "$RUN_DIR/checkpoints" "$BENCH_DIR/tmp/$RUN_ID/io" "$BENCH_DIR/tmp/$RUN_ID/java" "$LOG_DIR"

if [ "$CHECKPOINT_INTERVAL" = "disabled" ] || [ "$CHECKPOINT_INTERVAL" = "none" ]; then
  CHECKPOINT_CONFIG=""
else
  CHECKPOINT_CONFIG="  checkpointing:
    interval: $CHECKPOINT_INTERVAL
    mode: EXACTLY_ONCE
    tolerable-failed-checkpoints: 1000"
fi

FORST_TUNING_CONFIG=""
if [ "${FORST_TUNING_PROFILE:-}" = "write-heavy" ]; then
  FORST_TUNING_CONFIG="state.backend.forst.thread.num: ${FORST_THREAD_NUM:-8}
state.backend.forst.memory.fixed-per-tm: ${FORST_MEMORY_FIXED_PER_TM:-20gb}
state.backend.forst.memory.write-buffer-ratio: ${FORST_WRITE_BUFFER_RATIO:-0.7}
state.backend.forst.writebuffer.size: ${FORST_WRITEBUFFER_SIZE:-256mb}
state.backend.forst.writebuffer.count: ${FORST_WRITEBUFFER_COUNT:-4}
state.backend.forst.writebuffer.number-to-merge: ${FORST_WRITEBUFFER_NUMBER_TO_MERGE:-2}
state.backend.forst.write-batch-size: ${FORST_WRITE_BATCH_SIZE:-16mb}
state.backend.forst.compaction.level.target-file-size-base: ${FORST_TARGET_FILE_SIZE_BASE:-256mb}
state.backend.forst.compaction.level.max-size-level-base: ${FORST_MAX_SIZE_LEVEL_BASE:-1gb}
state.backend.forst.executor.inline-write: ${FORST_EXECUTOR_INLINE_WRITE:-false}
state.backend.forst.executor.write-io-parallelism: ${FORST_EXECUTOR_WRITE_IO_PARALLELISM:-4}
state.backend.forst.executor.read-io-parallelism: ${FORST_EXECUTOR_READ_IO_PARALLELISM:-4}
"
fi

cat > "$TEMPLATE_DIR/config-forst-local.yaml.tpl" <<CFG
# Generated by /bench/run-one.sh for $RUN_ID
# ForSt backend, local primary/checkpoint storage, JDK17.
env.java.home: /opt/java/openjdk
env.java.opts.all: $EXTRA_JAVA_OPTS --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED -Djava.io.tmpdir=$BENCH_DIR/tmp/$RUN_ID/java

jobmanager:
  bind-host: localhost
  rpc:
    address: localhost
    port: 6123
  memory:
    process:
      size: 2048m
  execution:
    failover-strategy: region

taskmanager:
  bind-host: localhost
  host: localhost
  numberOfTaskSlots: $TM_SLOTS
  memory:
    process:
      size: 32768m

parallelism:
  default: $PARALLELISM

state:
  backend:
    type: forst
    forst:
      primary-dir: file://$RUN_DIR/primary
      cache:
        size-based-limit: 8gb
  checkpoints:
    dir: file://$RUN_DIR/checkpoints

$FORST_TUNING_CONFIG
execution:
  runtime-mode: $RUNTIME_MODE
$CHECKPOINT_CONFIG

table:
  exec:
    mini-batch:
      enabled: ${MINI_BATCH_ENABLED:-false}
      allow-latency: ${MINI_BATCH_ALLOW_LATENCY:-1 s}
      size: ${MINI_BATCH_SIZE:-5000}
    async-state:
      enabled: ${ASYNC_STATE_ENABLED:-true}
    state:
      ttl: 0 ms

rest:
  port: 8081
  bind-port: 8081
  address: localhost
  bind-address: localhost

io:
  tmp:
    dirs: $BENCH_DIR/tmp/$RUN_ID/io
CFG

echo "=== generated flink config ==="
sed -n '1,220p' "$TEMPLATE_DIR/config-forst-local.yaml.tpl"

echo "=== install sql-client gateway wrapper ==="
if [ ! -f "$FLINK_HOME/bin/sql-client.sh.orig" ]; then
  mv "$FLINK_HOME/bin/sql-client.sh" "$FLINK_HOME/bin/sql-client.sh.orig"
  cat > "$FLINK_HOME/bin/sql-client.sh" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
if [ "${1:-}" = "embedded" ]; then
  shift
  tmp="$(mktemp /tmp/sql-client-gateway-XXXXXX.sql)"
  cat > "$tmp"
  exec "$DIR/sql-client.sh.orig" gateway --endpoint localhost:8083 -f "$tmp" "$@"
fi
exec "$DIR/sql-client.sh.orig" "$@"
SH
  chmod +x "$FLINK_HOME/bin/sql-client.sh"
fi

echo "=== start measure-sql ==="
set +e
bash "${MEASURE_SCRIPT:-/src/ForSt/scripts/measure-sql.sh}"
MEASURE_RC=$?
set -e

echo "=== loaded native libraries ==="
for p in $(pgrep -f 'TaskManagerRunner|StandaloneSession|SqlGateway|SqlClient' || true); do
  echo "--- pid=$p cmd=$(tr '\0' ' ' < /proc/$p/cmdline | cut -c1-220)"
  grep -hE 'libforstjni|libforst_rs|forstjni' /proc/$p/maps 2>/dev/null | head -20 || true
done

echo "=== stop flink daemons ==="
"$FLINK_HOME"/bin/stop-cluster.sh >/dev/null 2>&1 || true
"$FLINK_HOME"/bin/sql-gateway.sh stop >/dev/null 2>&1 || true
pkill -9 -f 'Benchmark|TaskManagerRunner|StandaloneSession|SqlGateway|SqlClient' 2>/dev/null || true

echo "=== disk usage ==="
du -sh "$RUN_DIR" "$LOG_DIR" 2>/dev/null || true
echo "run_id=$RUN_ID result_log=$RESULT_LOG"
exit "$MEASURE_RC"
