#!/usr/bin/env bash
# Host driver: fixed-CSV 1M accuracy gate, forst-rs vs rocksdb, LOCAL Mac via
# the forst-bench docker image (same mounts/limits as run-8c32g.sh).
#
#   scripts/accuracy-gate/run-matrix.sh            # gen CSV if missing + q3 q5 q8 q11 q12
#   QUERIES="q5" scripts/accuracy-gate/run-matrix.sh
#   FORCE_GEN=1 ... # regenerate the fixed CSV dataset
#
# Results: target-linux/accuracy-gate-results/<tag>/{<q>-<cfg>.rows,verdicts.txt}
set -u
REPO="${REPO:-/Users/lijunqing/Code/stczwd/ForSt}"
WORKENV="${WORKENV:-/Users/lijunqing/Downloads/workenv}"
FLINK="${FLINK:-$WORKENV/flink-2.2.1}"
IMG="${IMG:-forst-bench:arm64}"
PLAT="${PLAT:-linux/arm64}"
QUERIES="${QUERIES:-q3 q5 q8 q11 q12}"
CONFIGS="${CONFIGS:-forst-rs-ffm-local rocksdb}"
TAG="${TAG:-accgate-$(date +%m%d%H%M)}"
MAXSEC="${MAXSEC:-900}"
CSV_HOST="$WORKENV/frs-tmp/nexmark-fixed-csv-1m"   # container /tmp/nexmark-fixed-csv-1m
RESULTS="$REPO/target-linux/accuracy-gate-results/$TAG"
SO="$REPO/target-linux/release/libforst_rs_ffi.so"
[ -f "$SO" ] || { echo "missing $SO — run: scripts/run-8c32g.sh build"; exit 1; }
mkdir -p "$RESULTS"

DKR_COMMON=(--platform "$PLAT"
  -v "$REPO:$REPO" -v "$WORKENV:$WORKENV"
  -v "$WORKENV/frs-tmp:/tmp"
  -e JDK17="${JDK17_IN_IMG:-/usr/lib/jvm/java-17-openjdk-arm64}"
  -e JDK25=/opt/java/openjdk
  -e TEMPLATES="$REPO/scripts/templates-linux"
  -e HADOOP_HOME="$WORKENV/hadoop-3.4.3"
  -e NEXMARK_HOME="$REPO/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink"
  -e FLINK_HOME="$FLINK"
  -e S3_ENDPOINT=x -e S3_ACCESS_KEY=x -e S3_SECRET_KEY=x -e S3_BUCKET=x -e S3_REGION=x -e S3_PREFIX=x
  -w "$REPO")

if [ -n "${FORCE_GEN:-}" ] || [ ! -d "$CSV_HOST/bid" ]; then
  echo "== generating fixed 1M CSV dataset (once) =="
  docker run --rm --cpus=8 --memory=32g "${DKR_COMMON[@]}" "$IMG" bash -lc \
    "bash scripts/accuracy-gate/gen-fixed-csv.sh" 2>&1 | tee "$RESULTS/gen.log"
  grep -q "GEN OK" "$RESULTS/gen.log" || { echo "CSV generation FAILED"; exit 1; }
else
  echo "== reusing fixed CSV dataset at $CSV_HOST =="
fi

: > "$RESULTS/verdicts.txt"
for q in $QUERIES; do
  for cfg in $CONFIGS; do
    out_c="$REPO/target-linux/accuracy-gate-results/$TAG/$q-$cfg.rows"  # same path host+container
    echo "== RUN $q [$cfg] =="
    docker run --rm --cpus=8 --memory=32g --memory-swap=32g "${DKR_COMMON[@]}" \
      -e QUERY="$q" -e CONFIG="$cfg" -e MAXSEC="$MAXSEC" -e OUT_FILE="$out_c" \
      "$IMG" bash -lc "
        mkdir -p /usr/local/lib && cp '$SO' /usr/local/lib/libforst_rs_ffi.so &&
        cp '$SO' '$FLINK/lib/libforst_rs_ffi.so' &&
        bash scripts/accuracy-gate/measure-print.sh
      " 2>&1 | tee "$RESULTS/$q-$cfg.log"
    grep -h "VERDICT_TSV" "$RESULTS/$q-$cfg.log" | tail -1 >> "$RESULTS/verdicts.txt"
  done
done

echo ""
echo "== run verdicts =="
cat "$RESULTS/verdicts.txt"
echo ""
echo "== A/B comparison =="
: > "$RESULTS/compare.txt"
for q in $QUERIES; do
  frs="$RESULTS/$q-forst-rs-ffm-local.rows"; rdb="$RESULTS/$q-rocksdb.rows"
  if [ -s "$frs" ] && [ -s "$rdb" ]; then
    python3 "$REPO/scripts/accuracy-gate/compare-print.py" "$q" "$frs" "$rdb" 2>&1 | tee -a "$RESULTS/compare.txt"
  else
    echo "COMPARE_TSV	$q	NO_DATA	frs=$([ -s "$frs" ] && echo ok || echo missing)	rdb=$([ -s "$rdb" ] && echo ok || echo missing)" | tee -a "$RESULTS/compare.txt"
  fi
done
echo ""
echo "results dir: $RESULTS"
