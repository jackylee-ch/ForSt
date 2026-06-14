#!/usr/bin/env bash
# run-best.sh — apply each NexMark query's EMPIRICALLY-BEST forst-rs config.
#
# Per-query config is INTENTIONAL here: this is the best-PERFORMANCE reproduction
# package (the same-config constraint was reversed 2026-06-14). It is DISTINCT
# from run-remote-nexmark-v3.sh, which deliberately pins ONE uniform config for
# all queries (the uniform-config research question). For each query this script
# reads its row from configs/best-config.tsv, exports the per-query forst-rs
# knobs, and drives the UNMODIFIED run-8c32g.sh (TOPO=split) for the forst-rs arm.
#
# The best config per query (see configs/best-config.tsv for full provenance):
#   KV-sep ON  (write/value-carrying joins): q4 q7 q19 q20
#   KV-sep OFF (read-bound / OOM-prone):     q9 (MUST, else OOM) q11 q17
#   NEUTRAL    (source-bound):               q12
#   q17 best wall (83.7s) also needs FRS_RS_EXECUTOR=routing-adaptive (R2a).
#
# USAGE
#   run-best.sh <query>            # run ONE query at its best config (forst-rs arm)
#   run-best.sh sweep              # run ALL 8 priority queries serially at best config
#   run-best.sh print [<query>]    # print the resolved best config (no run)
#
#   Subset:  QUERIES="q4 q9" run-best.sh sweep
#   Per-run cap: MAXSEC (default per-query from the table's MAXSEC map).
#
# ENV (passed through to run-8c32g.sh; the harness uses absolute host paths):
#   REPO WORKENV FLINK IMG PLAT NEXMARK_HOME TAG_PREFIX FRS_CTMP_BASE MAXSEC
#
# This script ONLY sets env + calls run-8c32g.sh. It does NOT build or modify
# run-8c32g.sh / any template. Build first: run-8c32g.sh build (+ jar on the box).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TSV="$PKG_DIR/configs/best-config.tsv"
[ -f "$TSV" ] || { echo "FATAL: missing $TSV"; exit 1; }

# Repo root: prefer an explicit REPO (the box checkout), else the engine repo
# that contains this package (../../.. from tools/nexmark-local/scripts).
REPO="${REPO:-$(cd "$PKG_DIR/../.." && pwd)}"
# Use the package's run-8c32g.sh copy if present (forwards FRS_REMOTE_BW_MBPS),
# else the canonical one. Per-query env is identical either way.
RUNNER="$PKG_DIR/scripts/run-8c32g.sh"
[ -f "$RUNNER" ] || RUNNER="$REPO/scripts/run-8c32g.sh"
[ -f "$RUNNER" ] || { echo "FATAL: missing run-8c32g.sh (looked in $PKG_DIR/scripts and $REPO/scripts)"; exit 1; }

ARM="${ARM:-forst-rs-ffm-local}"   # best config only applies to the forst-rs arm
TAG_PREFIX="${TAG_PREFIX:-best}"

ALL_QUERIES="q4 q7 q9 q11 q12 q17 q19 q20"

# Per-query MAXSEC defaults (from the V3 passes: q11/q12/q17/q19/q4=1500;
# q7/q9/q20=2700). Override globally with MAXSEC=...
# (Plain case, not an assoc array, so this runs on macOS bash 3.2 too — the
# script runs on the HOST and shells out to docker.)
maxsec_for() {
  case "$1" in
    q7|q9|q20) echo 2700 ;;
    *)         echo 1500 ;;
  esac
}

# --- read one query's row from the TSV; returns the tab-split fields ---
# Skips comment (#) and blank lines and the header row.
lookup_row() {
  local q="$1"
  awk -F'\t' -v q="$q" '
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*$/ { next }
    $1 == "query"    { next }
    $1 == q          { print; found=1; exit }
    END { if (!found) exit 3 }
  ' "$TSV"
}

# --- export the per-query knobs from a TSV row; "-" => unset ---
# Echoes a human-readable summary. Sets the FRS_* env for run-8c32g.sh.
apply_row() {
  local row="$1"
  local query kvsep kvmin trivial s2pin s2fan exec_ comp wall status note
  IFS=$'\t' read -r query kvsep kvmin trivial s2pin s2fan exec_ comp wall status note <<<"$row"

  # Clear all per-query knobs first (so a previous query's setting never leaks).
  unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE \
        FRS_RS_S2_PINNED FRS_S2_FANOUT_MIN FRS_RS_EXECUTOR 2>/dev/null || true

  setk() { [ "$2" != "-" ] && export "$1=$2" || true; }
  setk FRS_KV_SEPARATION   "$kvsep"
  setk FRS_KV_MIN_BLOB_SIZE "$kvmin"
  setk FRS_TRIVIAL_MOVE    "$trivial"
  setk FRS_RS_S2_PINNED    "$s2pin"
  setk FRS_S2_FANOUT_MIN   "$s2fan"
  setk FRS_RS_EXECUTOR     "$exec_"
  # SST compression: lz4 is the engine default AND the fair match; always set it
  # explicitly so the run is reproducible regardless of any inherited env.
  export FRS_SST_COMPRESSION="${comp:-lz4}"
  export FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}"

  echo "  query=$query  expected_wall=${wall}s  status=$status"
  echo "  FRS_KV_SEPARATION=${FRS_KV_SEPARATION:-<unset>}  FRS_KV_MIN_BLOB_SIZE=${FRS_KV_MIN_BLOB_SIZE:-<unset>}  FRS_TRIVIAL_MOVE=${FRS_TRIVIAL_MOVE:-<unset>}"
  echo "  FRS_RS_S2_PINNED=${FRS_RS_S2_PINNED:-<unset>}  FRS_S2_FANOUT_MIN=${FRS_S2_FANOUT_MIN:-<unset>}  FRS_RS_EXECUTOR=${FRS_RS_EXECUTOR:-<unset>}"
  echo "  FRS_SST_COMPRESSION=$FRS_SST_COMPRESSION"
  [ "$status" = "NEEDS-CONFIRM" ] && echo "  *** NEEDS-CONFIRM: this exact knob combo's wall is UNMEASURED — treat $wall s as an estimate; a confirming run is required. ***"
  echo "  note: $note"
}

run_one() {
  local q="$1"
  local row; row="$(lookup_row "$q")" || { echo "FATAL: query '$q' not in $TSV"; exit 1; }
  local ms="${MAXSEC:-$(maxsec_for "$q")}"
  local tag="$TAG_PREFIX-$q-$ARM"
  echo ""
  echo "================ BEST-CONFIG RUN $q [$ARM] tag=$tag MAXSEC=$ms ================"
  ( apply_row "$row"
    export TOPO=split
    echo "  -> $RUNNER run $q $ARM $ms $tag"
    CLUSTER="$tag" bash "$RUNNER" run "$q" "$ARM" "$ms" "$tag"
  )
}

cmd="${1:-}"; [ -n "$cmd" ] && shift || true
case "$cmd" in
  print)
    q="${1:-}"
    if [ -n "$q" ]; then
      row="$(lookup_row "$q")" || { echo "FATAL: query '$q' not in $TSV"; exit 1; }
      echo "== best config for $q =="
      apply_row "$row"
    else
      for q in $ALL_QUERIES; do
        echo "== best config for $q =="
        ( apply_row "$(lookup_row "$q")" )
        echo ""
      done
    fi
    ;;
  sweep)
    QS="${QUERIES:-$ALL_QUERIES}"
    echo "== best-config sweep: $QS (arm=$ARM, serial) =="
    for q in $QS; do run_one "$q"; done
    echo ""
    echo "== best-config sweep done =="
    ;;
  q*)
    # run-best.sh <query>
    run_one "$cmd"
    ;;
  ""|-h|--help|help)
    sed -n '2,40p' "$0"
    ;;
  *)
    echo "unknown: $cmd (expected: <query> | sweep | print [<query>] | help)"; exit 1
    ;;
esac
