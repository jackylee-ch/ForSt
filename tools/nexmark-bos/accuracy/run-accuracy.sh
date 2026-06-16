#!/usr/bin/env bash
# Host-side driver for BOS fixed-CSV NexMark correctness checks.
set -euo pipefail

OS="$(uname -s)"
if [ "$OS" = "Darwin" ]; then
  REPO="${REPO:-/Users/lijunqing/Code/stczwd/ForSt}"
  WORKENV="${WORKENV:-/Users/lijunqing/Downloads/workenv}"
  IMG="${IMG:-forst-bench:arm64}"
  PLAT="${PLAT:-linux/arm64}"
else
  REPO="${REPO:-$PWD}"
  WORKENV="${WORKENV:-$HOME/workenv}"
  IMG="${IMG:-nexmark-bos:forst-rs-q4-latest-20260615}"
  PLAT="${PLAT:-linux/amd64}"
fi

FLINK="${FLINK:-$WORKENV/flink-2.2.1}"
HADOOP_HOME="${HADOOP_HOME:-$WORKENV/hadoop-3.3.6}"
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
CORE_SITE="${CORE_SITE:-$HADOOP_CONF_DIR/core-site.xml}"
BOS_HADOOP_FS_JAR="${BOS_HADOOP_FS_JAR:-$HADOOP_HOME/share/hadoop/common/lib/bos-hadoop-fs-2.0.0.jar}"
DOCKER_BIN="${DOCKER_BIN:-/home/work/dockerd/bin/docker}"
LOCAL_BASE="${LOCAL_BASE:-/tmp/jackylee/nexmark-bos-accuracy}"
REMOTE_PARENT="${REMOTE_PARENT:-bos://tal-poc-namespace/jackylee/test}"
EVENTS_NUM="${EVENTS_NUM:-100000}"
GEN_TPS="${GEN_TPS:-$(( EVENTS_NUM / 100 ))}"
NEXMARK_PARALLELISM="${NEXMARK_PARALLELISM:-8}"
MAXSEC="${MAXSEC:-900}"
SPLIT_TM_CPUS="${SPLIT_TM_CPUS:-4}"
SPLIT_TM_MEM="${SPLIT_TM_MEM:-16g}"
SPLIT_JM_CPUS="${SPLIT_JM_CPUS:-2}"
SPLIT_JM_MEM="${SPLIT_JM_MEM:-4g}"
BACKENDS="${BACKENDS:-forstrs rocksdb}"
QUERIES="${QUERIES:-q0 q1 q2 q3 q4 q5 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22}"
RUN_TAG="${RUN_TAG:-nexmark-bos-accuracy-$(date +%Y%m%d-%H%M%S)}"
RUN_ROOT="$LOCAL_BASE/$RUN_TAG"
CSV_DIR="$RUN_ROOT/input-100k"
RESULTS="$RUN_ROOT/results"
ACCURACY_DIR="$REPO/tools/nexmark-bos/accuracy"
CONFIGS_DIR="$REPO/tools/nexmark-bos/configs"
SO="$REPO/target-linux/release/libforst_rs_ffi.so"
cmd="${1:-sweep}"

if [ -x "$DOCKER_BIN" ]; then
  export PATH="$(dirname "$DOCKER_BIN"):$PATH"
fi

load_bos_creds_for_script_only() {
  [ -r "$CORE_SITE" ] || { echo "FATAL: core-site.xml is not readable by script"; exit 1; }
  eval "$(
    python3 - "$CORE_SITE" <<'PY'
import shlex
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
props = {}
for prop in root.findall("property"):
    name = prop.findtext("name")
    value = prop.findtext("value")
    if name is not None and value is not None:
        props[name.strip()] = value.strip()

mapping = {
    "S3_ACCESS_KEY": "fs.bos.access.key",
    "S3_SECRET_KEY": "fs.bos.secret.access.key",
}
missing = [src for src in mapping.values() if not props.get(src)]
if missing:
    print("echo FATAL: required BOS credential properties are missing in core-site.xml >&2")
    print("exit 1")
    raise SystemExit(0)
for env_name, prop_name in mapping.items():
    print(f"export {env_name}={shlex.quote(props[prop_name])}")
PY
  )"
}

require_file() {
  [ -f "$1" ] || { echo "FATAL: missing $1" >&2; exit 1; }
}

require_file "$SO"
require_file "$BOS_HADOOP_FS_JAR"
require_file "$ACCURACY_DIR/config-rocksdb-bos.yaml.tpl"
require_file "$CONFIGS_DIR/config-forst-rs.yaml.tpl"
[ -x "$(command -v docker 2>/dev/null || true)" ] || { echo "FATAL: docker CLI not found; set DOCKER_BIN"; exit 1; }

if echo "$BACKENDS" | grep -qw forstrs; then
  if [ -z "${S3_ACCESS_KEY:-}" ] || [ -z "${S3_SECRET_KEY:-}" ]; then
    load_bos_creds_for_script_only
  fi
  S3_ENDPOINT="${S3_ENDPOINT:-http://s3.bj.bcebos.com}"
  S3_BUCKET="${S3_BUCKET:-tal-poc-namespace}"
  S3_REGION="${S3_REGION:-bj}"
  : "${S3_ENDPOINT:?set S3_ENDPOINT; do not print secrets}"
  : "${S3_BUCKET:?set S3_BUCKET}"
  : "${S3_ACCESS_KEY:?set S3_ACCESS_KEY}"
  : "${S3_SECRET_KEY:?set S3_SECRET_KEY}"
fi

mkdir -p "$RESULTS"

DKR_COMMON=(
  -v "$REPO:$REPO"
  -v "$WORKENV:$WORKENV"
  -v "$LOCAL_BASE:$LOCAL_BASE"
  -v forst-cargo:/cargo-cache
  -e CARGO_HOME=/cargo-cache
  -e JDK17="${JDK17_IN_IMG:-/usr/lib/jvm/java-17-openjdk-$([ "$PLAT" = "linux/amd64" ] && echo amd64 || echo arm64)}"
  -e JDK25=/opt/java/openjdk
  -e TEMPLATES="$CONFIGS_DIR"
  -e ACCURACY_DIR="$ACCURACY_DIR"
  -e HADOOP_HOME="$HADOOP_HOME"
  -e HADOOP_CONF_DIR="$HADOOP_CONF_DIR"
  -e NEXMARK_HOME="$REPO/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink"
  -e FLINK_HOME="$FLINK"
  -e LOCAL_BASE="$LOCAL_BASE"
  -e REMOTE_PARENT="$REMOTE_PARENT"
  -e NEXMARK_PARALLELISM="$NEXMARK_PARALLELISM"
  -w "$REPO")
if [ "$OS" = "Darwin" ] || [ "${USE_DOCKER_PLATFORM:-0}" = "1" ]; then
  DKR_COMMON=(--platform "$PLAT" "${DKR_COMMON[@]}")
fi

FRS_ENVS=(
  -e S3_ENDPOINT="${S3_ENDPOINT:-}"
  -e S3_ACCESS_KEY="${S3_ACCESS_KEY:-}"
  -e S3_SECRET_KEY="${S3_SECRET_KEY:-}"
  -e S3_BUCKET="${S3_BUCKET:-}"
  -e S3_REGION="${S3_REGION:-us-east-1}"
  -e S3_PREFIX="${S3_PREFIX:-jackylee/test/nexmark-bos-accuracy}"
  -e FRS_KV_SEPARATION="${FRS_KV_SEPARATION:-true}"
  -e FRS_KV_MIN_BLOB_SIZE="${FRS_KV_MIN_BLOB_SIZE:-256}"
  -e FRS_TRIVIAL_MOVE="${FRS_TRIVIAL_MOVE:-true}"
  -e FRS_VLOG_COALESCE_DEREF="${FRS_VLOG_COALESCE_DEREF:-1}"
  -e FRS_CKPT_LINK_MODE="${FRS_CKPT_LINK_MODE:-1}"
  -e FRS_REMOTE_NONSST_LOCAL="${FRS_REMOTE_NONSST_LOCAL:-1}"
  -e FRS_UPLOAD_RATE_SPLIT="${FRS_UPLOAD_RATE_SPLIT:-1}"
  -e FRS_UPLOAD_COMPACTION_SHARE="${FRS_UPLOAD_COMPACTION_SHARE:-0.5}"
  -e FRS_RESTORE_BG_FILL="${FRS_RESTORE_BG_FILL:-1}"
  -e FRS_RESTORE_BG_FILL_WORKERS="${FRS_RESTORE_BG_FILL_WORKERS:-2}"
  -e FRS_RESTORE_BG_FILL_PACE_MB="${FRS_RESTORE_BG_FILL_PACE_MB:-64}"
  -e FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-512}"
  -e FRS_CACHE_SPACE_LIMIT_MB="${FRS_CACHE_SPACE_LIMIT_MB:-8192}"
  -e FRS_REMOTE_BW_MBPS="${FRS_REMOTE_BW_MBPS:-0}"
  -e FRS_SST_COMPRESSION="${FRS_SST_COMPRESSION:-lz4}"
  -e FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}"
)

sanitize_log() {
  grep -viE 'access[_\.-]?key|secret|credential|authorization|fs\.bos|Classpath:' "$1" > "$2" || true
}

run_container() {
  docker run --rm "$@" "$IMG" bash -lc "
    mkdir -p /usr/local/lib &&
    cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
    cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
    cp '$BOS_HADOOP_FS_JAR' '$FLINK/lib/' &&
    bash \"\$RUN_SCRIPT\"
  "
}

gen_csv() {
  if [ -d "$CSV_DIR/person" ] && [ -d "$CSV_DIR/auction" ] && [ -d "$CSV_DIR/bid" ] && [ -z "${FORCE_GEN:-}" ]; then
    echo "== reusing fixed CSV input: $CSV_DIR =="
    return
  fi
  echo "== generating fixed $EVENTS_NUM-event CSV input: $CSV_DIR =="
  log="$RUN_ROOT/gen.log"
  gen_conf="$RUN_ROOT/gen-conf"
  gen_log="$RUN_ROOT/gen-log"
  rm -rf "$gen_conf" "$gen_log"
  mkdir -p "$gen_conf" "$gen_log"
  cp -r "$FLINK/conf/." "$gen_conf/"
  RUN_SCRIPT="$ACCURACY_DIR/gen-fixed-csv.sh" \
    docker run --rm --cpus=8 --memory=32g --memory-swap=32g \
      "${DKR_COMMON[@]}" \
      -e TEMPLATES="$REPO/scripts/templates-linux" \
      -e FLINK_CONF_DIR="$gen_conf" -e FLINK_LOG_DIR="$gen_log" \
      -e CSV_DIR="$CSV_DIR" -e EVENTS_NUM="$EVENTS_NUM" -e GEN_TPS="$GEN_TPS" \
      -e RUN_SCRIPT="$ACCURACY_DIR/gen-fixed-csv.sh" \
      "$IMG" bash -lc "
        cp '$BOS_HADOOP_FS_JAR' '$FLINK/lib/' &&
        bash \"\$RUN_SCRIPT\"
      " > "$log" 2>&1 || { sanitize_log "$log" "$RUN_ROOT/gen.sanitized.log"; cat "$RUN_ROOT/gen.sanitized.log"; exit 1; }
  sanitize_log "$log" "$RUN_ROOT/gen.sanitized.log"
  grep -q $'GEN_TSV\tOK' "$RUN_ROOT/gen.sanitized.log" || { cat "$RUN_ROOT/gen.sanitized.log"; exit 1; }
}

create_network() {
  local net="$1"
  if docker network inspect "$net" >/dev/null 2>&1; then
    return
  fi
  if docker network create "$net" >/dev/null 2>&1; then
    return
  fi
  local base="${FRS_NET_SUBNET_BASE:-172.98}"
  local oct
  oct=$(( $(printf '%s' "$net" | cksum | cut -d' ' -f1) % 240 ))
  for try in 0 1 2 3 4 5 6 7; do
    docker network create --subnet "$base.$(( (oct + try) % 240 )).0/24" "$net" >/dev/null 2>&1 && return
  done
  echo "FATAL: cannot create docker network $net" >&2
  exit 1
}

run_query_backend() {
  local q="$1"
  local backend="$2"
  local run_id="$RUN_TAG-$q-$backend"
  local cluster="$run_id"
  local net="$cluster-net"
  local ctmp="$RUN_ROOT/tmp/$cluster"
  local cconf="$ctmp/flink-conf"
  local clog="$ctmp/flink-log"
  local out_dir="$RESULTS/$q/$backend"
  local log="$out_dir/runner.log"
  mkdir -p "$out_dir" "$ctmp" "$clog"
  rm -rf "$cconf"
  cp -r "$FLINK/conf" "$cconf"
  create_network "$net"
  docker rm -f "$cluster-jm" "$cluster-tm1" "$cluster-tm2" >/dev/null 2>&1 || true

  local uring_opts=()
  case "${FRS_IO_URING:-1}" in
    1|true|TRUE|yes) uring_opts=(--security-opt seccomp=unconfined) ;;
  esac
  local tm_preload=()
  [ "${FRS_TM_JEMALLOC:-1}" = "1" ] && tm_preload=(-e "LD_PRELOAD=${FRS_JEMALLOC_SO:-/usr/local/lib/libjemalloc-preload.so}")
  local backend_envs=()
  backend_envs=(-e NEXMARK_BOS_BACKEND="$backend")
  if [ "$backend" = "rocksdb" ]; then
    local hadoop_cp
    hadoop_cp="$(find "$HADOOP_HOME/share/hadoop" -name '*.jar' 2>/dev/null | tr '\n' ':')"
    backend_envs+=(-e HADOOP_CLASSPATH="$hadoop_cp")
  fi

  echo "== RUN $q [$backend] run_id=$run_id =="
  for i in 1 2; do
    docker run -d --name "$cluster-tm$i" --network "$net" \
      --cpus="$SPLIT_TM_CPUS" --memory="$SPLIT_TM_MEM" --memory-swap="$SPLIT_TM_MEM" \
      "${tm_preload[@]}" "${uring_opts[@]}" \
      "${DKR_COMMON[@]}" "${FRS_ENVS[@]}" "${backend_envs[@]}" \
      -v "$ctmp:/tmp" -v "$RUN_ROOT:$RUN_ROOT" \
      -e FLINK_CONF_DIR="$cconf" -e FLINK_LOG_DIR="$clog" \
      -e QUERY="$q" -e BACKEND="$backend" \
      -e RUN_ID="$run_id" -e CSV_DIR="$CSV_DIR" -e OUT_DIR="$out_dir" \
      "$IMG" bash -lc "
        mkdir -p /usr/local/lib &&
        cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
        cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
        cp '$BOS_HADOOP_FS_JAR' '$FLINK/lib/' &&
        for t in \$(seq 1 150); do curl -sf http://$cluster-jm:8081/overview >/dev/null 2>&1 && break; sleep 2; done &&
        exec bash '$FLINK/bin/taskmanager.sh' start-foreground > '$clog/flink--taskexecutor-docker-$i.out' 2>&1
      " >/dev/null
  done

  set +e
  docker run --rm --name "$cluster-jm" --network "$net" \
    --cpus="$SPLIT_JM_CPUS" --memory="$SPLIT_JM_MEM" --memory-swap="$SPLIT_JM_MEM" \
    "${uring_opts[@]}" \
    "${DKR_COMMON[@]}" "${FRS_ENVS[@]}" "${backend_envs[@]}" \
    -v "$ctmp:/tmp" -v "$RUN_ROOT:$RUN_ROOT" \
    -e FLINK_CONF_DIR="$cconf" -e FLINK_LOG_DIR="$clog" \
    -e CLUSTER_MODE=external -e EXPECT_TMS=2 -e JM_HOST="$cluster-jm" \
    -e QUERY="$q" -e BACKEND="$backend" -e RUN_ID="$run_id" -e CSV_DIR="$CSV_DIR" \
    -e OUT_DIR="$out_dir" -e MAXSEC="$MAXSEC" \
    -e RUN_SCRIPT="$ACCURACY_DIR/measure-query.sh" \
    "$IMG" bash -lc "
      mkdir -p /usr/local/lib &&
      cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
      cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
      cp '$BOS_HADOOP_FS_JAR' '$FLINK/lib/' &&
      bash \"\$RUN_SCRIPT\"
    " > "$log" 2>&1
  rc=$?
  set -e

  docker rm -f "$cluster-tm1" "$cluster-tm2" >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
  sanitize_log "$log" "$out_dir/runner.sanitized.log"
  grep -h "ACCURACY_TSV" "$out_dir/runner.sanitized.log" | tail -1 >> "$RUN_ROOT/verdicts.tsv" || true
  if [ "$rc" -ne 0 ]; then
    cat "$out_dir/runner.sanitized.log"
    return "$rc"
  fi
}

compare_query() {
  local q="$1"
  local frs="$RESULTS/$q/forstrs/print.rows"
  local rdb="$RESULTS/$q/rocksdb/print.rows"
  local out="$RESULTS/$q/compare.txt"
  if [ ! -s "$frs" ] || [ ! -s "$rdb" ]; then
    echo -e "COMPARE_TSV\t$q\tNO_DATA\tforstrs=$([ -s "$frs" ] && echo ok || echo missing)\trocksdb=$([ -s "$rdb" ] && echo ok || echo missing)" | tee "$out" >> "$RUN_ROOT/compare.tsv"
    return 1
  fi
  local expected_bids=$(( EVENTS_NUM * 46 / 50 ))
  set +e
  python3 "$ACCURACY_DIR/materialize-changelog.py" compare \
    --query "$q" \
    --left "$frs" \
    --right "$rdb" \
    --csv-dir "$CSV_DIR" \
    --expected-bids "$expected_bids" > "$out" 2>&1
  rc=$?
  set -e
  grep -h "COMPARE_TSV" "$out" | tail -1 >> "$RUN_ROOT/compare.tsv" || true
  cat "$out"
  return "$rc"
}

write_summary() {
  local summary="$RUN_ROOT/SUMMARY.md"
  {
    echo "# NexMark BOS Accuracy Summary"
    echo
    echo "- run_id: \`$RUN_TAG\`"
    echo "- input_events: \`$EVENTS_NUM\`"
    echo "- input_dir: \`$CSV_DIR\`"
    echo "- results_dir: \`$RESULTS\`"
    echo "- remote_parent: \`$REMOTE_PARENT\`"
    echo "- local_base: \`$LOCAL_BASE\`"
    echo
    echo "## Verdicts"
    echo
    echo '```text'
    [ -f "$RUN_ROOT/verdicts.tsv" ] && cat "$RUN_ROOT/verdicts.tsv"
    echo '```'
    echo
    echo "## Comparison"
    echo
    echo '```text'
    [ -f "$RUN_ROOT/compare.tsv" ] && cat "$RUN_ROOT/compare.tsv"
    echo '```'
  } > "$summary"
  echo "summary: $summary"
}

cleanup_prefix() {
  docker ps -a --format '{{.Names}}' 2>/dev/null | grep '^nexmark-bos-accuracy' | xargs -r docker rm -f >/dev/null 2>&1 || true
  docker network ls --format '{{.Name}}' 2>/dev/null | grep '^nexmark-bos-accuracy' | xargs -r docker network rm >/dev/null 2>&1 || true
}

case "$cmd" in
  gen)
    gen_csv
    ;;
  clean)
    cleanup_prefix
    ;;
  q*)
    : > "$RUN_ROOT/verdicts.tsv"
    : > "$RUN_ROOT/compare.tsv"
    gen_csv
    for backend in $BACKENDS; do
      run_query_backend "$cmd" "$backend"
    done
    compare_query "$cmd" || true
    write_summary
    ;;
  sweep)
    : > "$RUN_ROOT/verdicts.tsv"
    : > "$RUN_ROOT/compare.tsv"
    gen_csv
    for q in $QUERIES; do
      ok=1
      for backend in $BACKENDS; do
        run_query_backend "$q" "$backend" || ok=0
      done
      compare_query "$q" || ok=0
      [ "$ok" = "1" ] || echo "WARN: $q did not pass accuracy comparison"
    done
    write_summary
    ;;
  *)
    echo "usage: $0 [gen|clean|qN|sweep]" >&2
    exit 1
    ;;
esac
