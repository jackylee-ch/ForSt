#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  "") ;;
  -h|--help) echo "Usage: run-matrix.sh (configure with env: RUN_LABEL EVENTS_NUM TPS MAXSEC QUERIES VARIANTS)"; exit 0 ;;
  *) echo "unknown argument: $1" >&2; exit 2 ;;
esac
WORK_ROOT="${WORK_ROOT:-/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610}"
FORST_REPO="${FORST_REPO:-/home/users/lijunqing/code/stczwd/ForSt}"
IMAGE="${IMAGE:-flink:2.2.1-jdk17-forst-bench-tools-20260609}"
DOCKER_BIN="${DOCKER_BIN:-/home/work/dockerd/bin/docker}"
RUN_LABEL="${RUN_LABEL:-bench-$(date +%Y%m%d%H%M%S)}"
EVENTS_NUM="${EVENTS_NUM:-100000000}"
TPS="${TPS:-10000000}"
MAXSEC="${MAXSEC:-3600}"
POLL_SEC="${POLL_SEC:-5}"
DONE_ON_SRC="${DONE_ON_SRC:-1}"
DONE_ON_PLATEAU="${DONE_ON_PLATEAU:-1}"
PLATEAU_POLLS="${PLATEAU_POLLS:-6}"
OUT_STABLE_POLLS="${OUT_STABLE_POLLS:-3}"
MIN_PLATEAU_SEC="${MIN_PLATEAU_SEC:-45}"
SRC_DONE_GRACE_SEC="${SRC_DONE_GRACE_SEC:-30}"
SRC_DONE_OUT_STABLE_POLLS="${SRC_DONE_OUT_STABLE_POLLS:-2}"
CONTAINER_MEMORY="${CONTAINER_MEMORY:-40g}"
CONTAINER_CPUS="${CONTAINER_CPUS:-8}"
QUERIES_STR="${QUERIES:-q0 q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22}"
VARIANTS_STR="${VARIANTS:-forst-local forst-rs-lib}"
mkdir -p "$WORK_ROOT"/{logs,results,state,tmp,templates}
MATRIX="$WORK_ROOT/results/${RUN_LABEL}.tsv"
: > "$MATRIX"
printf 'run_label\tvariant\tquery\tdocker_rc\tmode\twall_ms\tseconds\tthroughput_src_rows_per_s\tsrc_out\texpected_src\tout_rows\tstate\telapsed_s\tjid\tnative_loaded\tresult_log\touter_log\tnote\n' >> "$MATRIX"

echo "matrix_start run_label=$RUN_LABEL events=$EVENTS_NUM tps=$TPS maxsec=$MAXSEC poll=$POLL_SEC image=$IMAGE cpus=$CONTAINER_CPUS memory=$CONTAINER_MEMORY"
echo "queries=$QUERIES_STR"
echo "variants=$VARIANTS_STR"

"$DOCKER_BIN" image inspect "$IMAGE" >/dev/null
for q in $QUERIES_STR; do
  for variant in $VARIANTS_STR; do
    run_id="${RUN_LABEL}-${variant}-${q}"
    cname="nexmark-${run_id}"
    result_log="$WORK_ROOT/results/${run_id}.log"
    outer_log="$WORK_ROOT/logs/${run_id}.docker.log"
    echo "=== RUN $run_id ==="
    "$DOCKER_BIN" rm -f "$cname" >/dev/null 2>&1 || true
    set +e
    "$DOCKER_BIN" run --rm \
      --name "$cname" \
      --security-opt seccomp=unconfined \
      --cpus="$CONTAINER_CPUS" \
      --memory="$CONTAINER_MEMORY" \
      --memory-swap="$CONTAINER_MEMORY" \
      --entrypoint /bin/bash \
      -v "$WORK_ROOT:/bench" \
      -v "$FORST_REPO:/src/ForSt:ro" \
      -e VARIANT="$variant" \
      -e QUERY="$q" \
      -e EVENTS_NUM="$EVENTS_NUM" \
      -e TPS="$TPS" \
      -e MAXSEC="$MAXSEC" \
      -e PARALLELISM="${PARALLELISM:-8}" \
      -e TM_SLOTS="${TM_SLOTS:-${PARALLELISM:-8}}" \
      -e RUNTIME_MODE="${RUNTIME_MODE:-STREAMING}" \
      -e ASYNC_STATE_ENABLED="${ASYNC_STATE_ENABLED:-true}" \
      -e MINI_BATCH_ENABLED="${MINI_BATCH_ENABLED:-false}" \
      -e MINI_BATCH_ALLOW_LATENCY="${MINI_BATCH_ALLOW_LATENCY:-1 s}" \
      -e MINI_BATCH_SIZE="${MINI_BATCH_SIZE:-5000}" \
      -e FORST_TUNING_PROFILE="${FORST_TUNING_PROFILE:-}" \
      -e JFR_ENABLED="${JFR_ENABLED:-0}" \
      -e JFR_DELAY="${JFR_DELAY:-20s}" \
      -e JFR_DURATION="${JFR_DURATION:-${MAXSEC}s}" \
      -e JFR_SETTINGS="${JFR_SETTINGS:-profile}" \
      -e CHECKPOINT_INTERVAL="${CHECKPOINT_INTERVAL:-30 s}" \
      -e RUN_ID="$run_id" \
      -e MEASURE_SCRIPT="${MEASURE_SCRIPT:-/bench/scripts/measure-sql-full.sh}" \
      -e DONE_ON_SRC="$DONE_ON_SRC" \
      -e DONE_ON_PLATEAU="$DONE_ON_PLATEAU" \
      -e POLL_SEC="$POLL_SEC" \
      -e PLATEAU_POLLS="$PLATEAU_POLLS" \
      -e OUT_STABLE_POLLS="$OUT_STABLE_POLLS" \
      -e MIN_PLATEAU_SEC="$MIN_PLATEAU_SEC" \
      -e SRC_DONE_GRACE_SEC="$SRC_DONE_GRACE_SEC" \
      -e SRC_DONE_OUT_STABLE_POLLS="$SRC_DONE_OUT_STABLE_POLLS" \
      -e NEXMARK_DIR="/bench/tmp/$run_id/nexmark-qout" \
      -e CSV_LABEL="${CSV_LABEL:-}" \
      -e CSV_DIR="${CSV_DIR:-}" \
      -e CSV_SOURCE_MONITOR_INTERVAL="${CSV_SOURCE_MONITOR_INTERVAL:-}" \
      -e Q3_FILE_OUTPUT="${Q3_FILE_OUTPUT:-}" \
      -e Q3_PRINT_OUTPUT="${Q3_PRINT_OUTPUT:-}" \
      -e ACCURACY_FILE_OUTPUT="${ACCURACY_FILE_OUTPUT:-0}" \
      -e ACCURACY_FLUSH_EVERY="${ACCURACY_FLUSH_EVERY:-1024}" \
      -e INSTALL_FLINK_FORST_BACKEND_JAR="${INSTALL_FLINK_FORST_BACKEND_JAR:-1}" \
      -e FLINK_FORST_BACKEND_JAR="${FLINK_FORST_BACKEND_JAR:-}" \
      "$IMAGE" /bench/scripts/run-one-full.sh 2>&1 | tee "$outer_log"
    rc=${PIPESTATUS[0]}
    set -e
    result_line=""
    if [ -f "$result_log" ]; then
      result_line=$(grep -a '^RESULT_TSV' "$result_log" | tail -1 || true)
    fi
    if [ -n "$result_line" ]; then
      IFS=$'\t' read -r tag rq mode wall_ms src_out expected_src out_rows state elapsed_s jid note <<< "$result_line"
    else
      rq="$q"; mode="NO_RESULT"; wall_ms="0"; src_out="0"; expected_src="0"; out_rows="0"; state="NO_RESULT"; elapsed_s="0"; jid="-"; note="missing_RESULT_TSV"
    fi
    native_loaded="unknown"
    if [ -f "$result_log" ]; then
      if grep -a -q '/bench/native/libforstjni.so' "$result_log"; then
        native_loaded="bench-native-libforstjni.so"
      elif grep -a -q 'libforstjni-linux64.so' "$result_log"; then
        native_loaded="jar-extracted-libforstjni-linux64.so"
      elif grep -a -q 'libforstjni' "$result_log"; then
        native_loaded="other-libforstjni"
      fi
    fi
    seconds="0"; throughput="0"
    if [[ "$wall_ms" =~ ^[0-9]+$ ]] && [ "$wall_ms" -gt 0 ] && [[ "$src_out" =~ ^[0-9]+$ ]]; then
      seconds=$(awk -v ms="$wall_ms" 'BEGIN{printf "%.3f", ms/1000.0}')
      throughput=$(awk -v rows="$src_out" -v ms="$wall_ms" 'BEGIN{printf "%.2f", rows/(ms/1000.0)}')
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$RUN_LABEL" "$variant" "$q" "$rc" "$mode" "$wall_ms" "$seconds" "$throughput" "$src_out" "$expected_src" "$out_rows" "$state" "$elapsed_s" "$jid" "$native_loaded" "$result_log" "$outer_log" "$note" >> "$MATRIX"
    echo "SUMMARY $run_id rc=$rc mode=$mode wall_ms=$wall_ms src_out=$src_out out_rows=$out_rows native=$native_loaded note=$note"
    "$DOCKER_BIN" run --rm --security-opt seccomp=unconfined --entrypoint /bin/bash --user root -v "$WORK_ROOT:/bench" "$IMAGE" -lc "rm -rf /bench/state/$run_id /bench/tmp/$run_id /bench/templates/$run_id" >/dev/null 2>&1 || true
    "$DOCKER_BIN" rm -f "$cname" >/dev/null 2>&1 || true
    if [ "$rc" -eq 130 ] || [ "$rc" -eq 141 ]; then
      echo "matrix_interrupted rc=$rc after $run_id" >&2
      exit "$rc"
    fi
  done
done

echo "matrix_done matrix=$MATRIX"
