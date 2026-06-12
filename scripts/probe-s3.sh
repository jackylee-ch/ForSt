#!/usr/bin/env bash
# FRS-PHASE2-S0: S3-reachability probe (gate for all Phase-2 partial benches).
# Design: docs/superpowers/specs/2026-06-13-phase2-disaggregated-state-design.md §Stage-0.
#
# Wraps the forst-rs-io `s3bw` example's --probe mode (the production opendal
# write/read path) and applies the design's decision matrix:
#   upload >= 100 MB/s  AND  download >= 100 MB/s  AND  metadata RTT < 5 ms
#     -> BOS usable for Phase-2 partial benchmarks
#   otherwise -> fs-emulation only
# Also reports read-after-write visibility (design risk R2): if "eventual",
# Stage-3 restore must add a bounded-retry stat loop.
#
# Env (same vars as scripts/measure-sql.sh / bench-4way-s3.sh):
#   S3_ENDPOINT S3_ACCESS_KEY S3_SECRET_KEY S3_BUCKET S3_REGION S3_PREFIX
# Tunables: PROBE_MB (default 200), PROBE_OPS (default 20), PROBE_VIS_TRIALS
# (default 10), REPORT_FILE (optional: also append the report there).
#
# Usage:
#   scripts/probe-s3.sh            # probe $S3_ENDPOINT (BOS or any S3-compat)
#   scripts/probe-s3.sh selftest   # fs-emulation self-test (no network; checks
#                                  # the probe machinery end-to-end)
#
# No docker, no flink; runnable standalone on the co-located remote box.
set -euo pipefail

cd "$(dirname "$0")/.."

MODE="${1:-s3}"
OUT="$(mktemp)"
trap 'rm -f "$OUT"' EXIT

if [[ "$MODE" == "selftest" ]]; then
  ROOT="$(mktemp -d)"
  trap 'rm -rf "$ROOT" "$OUT"' EXIT
  echo "probe-s3: fs-emulation SELF-TEST (root=$ROOT) — validates probe machinery only;"
  echo "probe-s3: numbers are local-disk, NOT S3 evidence."
  S3BW_SCHEME=fs S3BW_ROOT="$ROOT" \
    cargo run --release -p forst-rs-io --example s3bw -- --probe | tee "$OUT"
else
  : "${S3_BUCKET:?S3_BUCKET required (plus S3_ENDPOINT/S3_REGION/S3_ACCESS_KEY/S3_SECRET_KEY/S3_PREFIX as needed)}"
  echo "probe-s3: probing S3 endpoint (bucket/prefix hidden) ..."
  S3BW_SCHEME=s3 \
    cargo run --release -p forst-rs-io --example s3bw -- --probe | tee "$OUT"
fi

# ---- decision matrix (design §Stage-0) -------------------------------------
get() { grep -o "$1=[0-9.]*" "$OUT" | head -1 | cut -d= -f2; }
RTT="$(get rtt_ms_median)"
UP="$(get upload_mbps)"
DOWN="$(get download_mbps)"
VIS="$(grep -o 'raw_visibility=[a-zA-Z]*' "$OUT" | head -1 | cut -d= -f2)"

if [[ -z "$RTT" || -z "$UP" || -z "$DOWN" ]]; then
  echo "probe-s3: ERROR — probe output incomplete (missing rtt/upload/download)"
  exit 1
fi

DECISION="fs-emulation-only"
if awk -v u="$UP" -v d="$DOWN" -v r="$RTT" 'BEGIN { exit !(u >= 100 && d >= 100 && r < 5) }'; then
  DECISION="bos-usable"
fi

REPORT="probe-s3 REPORT: date=$(date -u +%Y-%m-%dT%H:%M:%SZ) mode=$MODE host=$(hostname) \
rtt_ms_median=$RTT upload_mbps=$UP download_mbps=$DOWN raw_visibility=$VIS decision=$DECISION"
echo "$REPORT"
if [[ "$VIS" == "eventual" ]]; then
  echo "probe-s3: WARNING — eventual read-after-write visibility: Stage-3 restore needs the bounded-retry stat loop (design risk R2)."
elif [[ "$VIS" == "FAILED" ]]; then
  echo "probe-s3: ERROR — object never became visible within the retry budget; endpoint unusable."
  exit 1
fi
if [[ "$MODE" == "selftest" ]]; then
  echo "probe-s3: (self-test decision is about machinery, not BOS — run against \$S3_ENDPOINT on the remote box for the real verdict)"
fi
if [[ -n "${REPORT_FILE:-}" ]]; then
  echo "$REPORT" >> "$REPORT_FILE"
  echo "probe-s3: report appended to $REPORT_FILE"
fi
