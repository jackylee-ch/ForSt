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
#   Approach A (coalesced vlog deref, FRS_VLOG_COALESCE_DEREF=1) is part of the
#   best config for the KV-sep queries (q4 q7 q19 q20). It is set uniformly in
#   the TSV; it is a no-op when KV-sep is OFF (q9/q11/q12/q17).
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
  local query kvsep kvmin trivial s2pin s2fan exec_ comp coalesce wall status note
  IFS=$'\t' read -r query kvsep kvmin trivial s2pin s2fan exec_ comp coalesce wall status note <<<"$row"

  # Clear all per-query knobs first (so a previous query's setting never leaks).
  unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE \
        FRS_RS_S2_PINNED FRS_S2_FANOUT_MIN FRS_RS_EXECUTOR FRS_VLOG_COALESCE_DEREF 2>/dev/null || true

  setk() { [ "$2" != "-" ] && export "$1=$2" || true; }
  setk FRS_KV_SEPARATION   "$kvsep"
  setk FRS_KV_MIN_BLOB_SIZE "$kvmin"
  setk FRS_TRIVIAL_MOVE    "$trivial"
  setk FRS_RS_S2_PINNED    "$s2pin"
  setk FRS_S2_FANOUT_MIN   "$s2fan"
  setk FRS_RS_EXECUTOR     "$exec_"
  # Coalesced batched value-log deref (Approach A): KV-sep pure-win read path,
  # part of the best config for the KV-sep queries (q4/q7/q19/q20). No-op when
  # KV-sep is OFF, so harmless to set uniformly.
  setk FRS_VLOG_COALESCE_DEREF "$coalesce"
  # SST compression: lz4 is the engine default AND the fair match; always set it
  # explicitly so the run is reproducible regardless of any inherited env.
  export FRS_SST_COMPRESSION="${comp:-lz4}"
  export FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}"

  echo "  query=$query  expected_wall=${wall}s  status=$status"
  echo "  FRS_KV_SEPARATION=${FRS_KV_SEPARATION:-<unset>}  FRS_KV_MIN_BLOB_SIZE=${FRS_KV_MIN_BLOB_SIZE:-<unset>}  FRS_TRIVIAL_MOVE=${FRS_TRIVIAL_MOVE:-<unset>}"
  echo "  FRS_RS_S2_PINNED=${FRS_RS_S2_PINNED:-<unset>}  FRS_S2_FANOUT_MIN=${FRS_S2_FANOUT_MIN:-<unset>}  FRS_RS_EXECUTOR=${FRS_RS_EXECUTOR:-<unset>}"
  echo "  FRS_SST_COMPRESSION=$FRS_SST_COMPRESSION  FRS_VLOG_COALESCE_DEREF=${FRS_VLOG_COALESCE_DEREF:-<unset>}"
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

# q9 KV-sep OOM fix (2026-06-15 PMC-1): q9 with KV-separation ON on a SINGLE
# BIG TM (8c/36g) instead of the 2×4c/16g split. The split capped each TM at
# 16g, and KV-sep's resident vlog state pushed q9 over that cgroup (DNF/OOM).
# A single TM with all 8 cores + 36g (physical RAM is 64g here, so OS+JM
# headroom is ample) gives q9 the per-TM memory the split could not. KV-sep's
# resident vlog readers are additionally BOUNDED (count cap + byte budget +
# adaptive pressure back-off) so the engine delta stays small.
#
# This is a PER-QUERY topology profile (the per-query best-config exception the
# user allowed) — it does NOT change any other query's run.
run_q9_36g() {
  local ms="${MAXSEC:-2700}"
  local tag="${TAG_PREFIX}-q9-36g-$ARM"
  echo ""
  echo "============ q9 KV-sep 8c/36g PROFILE [$ARM] tag=$tag MAXSEC=$ms ============"
  ( # KV-sep ON + coalesced deref (the per-query best read path), lz4.
    export FRS_KV_SEPARATION=true
    export FRS_KV_MIN_BLOB_SIZE="${FRS_KV_MIN_BLOB_SIZE:-256}"
    export FRS_TRIVIAL_MOVE="${FRS_TRIVIAL_MOVE:-true}"
    export FRS_RS_S2_PINNED="${FRS_RS_S2_PINNED:-1}"
    export FRS_VLOG_COALESCE_DEREF="${FRS_VLOG_COALESCE_DEREF:-1}"
    export FRS_SST_COMPRESSION="${FRS_SST_COMPRESSION:-lz4}"
    export FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}"
    # KV-sep resident MEMORY BOUNDS — the engine delta that the split's 16g
    # cgroup could not hold. Count cap is on by default (2048); add the BYTE
    # budget + adaptive pressure so resident vlog bytes are guaranteed bounded
    # regardless of q9's scattered-death segment pattern.
    export FRS_VLOG_READER_CACHE_CAP="${FRS_VLOG_READER_CACHE_CAP:-2048}"
    export FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-512}"
    export FRS_KV_ADAPTIVE_PRESSURE="${FRS_KV_ADAPTIVE_PRESSURE:-1}"
    # Single BIG TM topology: 8 cores, 36g container; JVM heap bumped from the
    # split-default 8192m so q9's on-heap join state has the headroom that two
    # split TMs gave it across their two 8g JVMs. JM 3g; the rest is native
    # off-heap (bounded vlog readers + shadow + decoded cache) + OS page cache.
    export TOPO=single
    export SINGLE_TM_CPUS="${SINGLE_TM_CPUS:-8}"
    export SINGLE_TM_MEM="${SINGLE_TM_MEM:-36g}"
    export FRS_TM_PROCESS_SIZE="${FRS_TM_PROCESS_SIZE:-16384m}"
    export FRS_JM_PROCESS_SIZE="${FRS_JM_PROCESS_SIZE:-3072m}"
    echo "  KV-sep=ON min_blob=$FRS_KV_MIN_BLOB_SIZE coalesce=$FRS_VLOG_COALESCE_DEREF"
    echo "  bounds: reader_cap=$FRS_VLOG_READER_CACHE_CAP budget_mb=$FRS_VLOG_RESIDENT_BUDGET_MB adaptive=$FRS_KV_ADAPTIVE_PRESSURE"
    echo "  topo=single cpus=$SINGLE_TM_CPUS mem=$SINGLE_TM_MEM tm_jvm=$FRS_TM_PROCESS_SIZE jm_jvm=$FRS_JM_PROCESS_SIZE"
    echo "  -> $RUNNER run q9 $ARM $ms $tag"
    CLUSTER="$tag" bash "$RUNNER" run q9 "$ARM" "$ms" "$tag"
  )
}

cmd="${1:-}"; [ -n "$cmd" ] && shift || true
case "$cmd" in
  q9-36g)
    run_q9_36g
    ;;
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
