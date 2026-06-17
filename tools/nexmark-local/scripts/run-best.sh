#!/usr/bin/env bash
# run-best.sh — apply the SINGLE UNIFORM forst-rs config to EVERY NexMark query.
#
# ★★ UNIFORM CONFIG (2026-06-17, PMC-1 — user directive). PER-QUERY CONFIG IS
# FORBIDDEN. This script reads the ONE `*` row from configs/best-config.tsv and
# exports the SAME forst-rs knobs for every query, then drives the UNMODIFIED
# run-8c32g.sh (TOPO=split, 2x4c/16g + 1x JM 2c/4g) for the forst-rs arm. There
# are NO per-query branches anywhere in this file. The engine adapts to the query
# SHAPE at runtime under the one config (point-deref auto-follows KV-sep for
# windowed RMW; coalesce for scans). See best-config.tsv for full provenance.
#
# THE UNIFORM CONFIG (from configs/best-config.tsv, row `*`):
#   FRS_KV_SEPARATION=true  FRS_KV_MIN_BLOB_SIZE=256  FRS_TRIVIAL_MOVE=true
#   FRS_RS_S2_PINNED=1      FRS_VLOG_COALESCE_DEREF=1  FRS_SST_COMPRESSION=lz4
#   FRS_MEM_MANAGER=1       (never-OOM controller; un-throttled on a >=40 GiB box)
#   FRS_VLOG_POINT_DEREF    LEFT UNSET -> auto-follows KV-sep (windowed q11/q17)
#   vlog resident bounds:   FRS_VLOG_READER_CACHE_CAP=2048
#                           FRS_VLOG_RESIDENT_BUDGET_MB=256  FRS_KV_ADAPTIVE_PRESSURE=1
#   TOPO=split (2x4c/16g + 10240m process.size carve-out + SLOTS, set in templates).
#
# USAGE
#   run-best.sh <query>            # run ONE query at the uniform config (forst-rs arm)
#   run-best.sh sweep              # run ALL priority queries serially, SAME config
#   run-best.sh print              # print the resolved uniform config (no run)
#
#   Subset:  QUERIES="q4 q9" run-best.sh sweep
#   Per-run cap: MAXSEC (default per-query wall budget from maxsec_for; this is a
#                TIMEOUT only — it is NOT a config difference).
#
# THREE-BACKEND FAIR A/B (forst-rs vs rocksdb vs forst — matched topology):
#   run-best.sh validate print              # DRY-RUN: print the uniform forst-rs config
#   run-best.sh validate <query>            # run ONE query: forst-rs (uniform config)
#                                             + rocksdb + forst-local baselines, SAME
#                                             2x4c/16g split + SLOTS + parallelism=4.
#   run-best.sh validate sweep              # all priority queries x 3 arms, serial.
#   run-best.sh validate-ab <query>         # forst-rs uniform-config (A) vs forst-rs
#                                             levers-OFF (B) — lever attribution A/B.
#   ARMS override (validate): ARMS_VALIDATE="forst-rs-ffm-local rocksdb".
#
# ENV (passed through to run-8c32g.sh; the harness uses absolute host paths and
# detects the OS — macOS arm64 dev box AND remote x86_64 Linux):
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
# Use the package's run-8c32g.sh copy if present (has the scratch-cleanup trap +
# FRS_MEM_MANAGER/process.size passthrough), else the canonical one.
RUNNER="$PKG_DIR/scripts/run-8c32g.sh"
[ -f "$RUNNER" ] || RUNNER="$REPO/scripts/run-8c32g.sh"
[ -f "$RUNNER" ] || { echo "FATAL: missing run-8c32g.sh (looked in $PKG_DIR/scripts and $REPO/scripts)"; exit 1; }

ARM="${ARM:-forst-rs-ffm-local}"   # the uniform config applies to the forst-rs arm
TAG_PREFIX="${TAG_PREFIX:-best}"

ALL_QUERIES="${ALL_QUERIES:-q4 q7 q9 q11 q12 q17 q19 q20}"

# Per-query MAXSEC defaults (a wall-clock TIMEOUT only — NOT a config difference).
# Heavy joins get a longer cap; everything else 1500s. Override with MAXSEC=...
maxsec_for() {
  case "$1" in
    q7|q9|q20) echo 2700 ;;
    *)         echo 1500 ;;
  esac
}

# --- read the single uniform `*` row from the TSV; returns the tab-split fields ---
lookup_uniform_row() {
  awk -F'\t' '
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*$/ { next }
    $1 == "query"    { next }
    $1 == "*"        { print; found=1; exit }
    END { if (!found) exit 3 }
  ' "$TSV"
}

# --- export the UNIFORM forst-rs knobs from the `*` row; "-" => unset ---
# This sets the EXACT SAME env regardless of which query is about to run.
apply_uniform() {
  local row; row="$(lookup_uniform_row)" || { echo "FATAL: no uniform '*' row in $TSV"; exit 1; }
  local query kvsep kvmin trivial s2pin coalesce comp memmgr note
  IFS=$'\t' read -r query kvsep kvmin trivial s2pin coalesce comp memmgr note <<<"$row"

  # Clear everything first so nothing leaks across invocations.
  unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE FRS_RS_S2_PINNED \
        FRS_VLOG_COALESCE_DEREF FRS_VLOG_POINT_DEREF FRS_RS_EXECUTOR FRS_S2_FANOUT_MIN \
        FRS_RS_PROBE_BLOOM_PRUNE FRS_RS_LEVELED_HOT_CF FRS_PERSISTENT_PROBE_ITER \
        FRS_RS_MERGE_RMW 2>/dev/null || true

  setk() { [ "$2" != "-" ] && export "$1=$2" || true; }
  setk FRS_KV_SEPARATION    "$kvsep"
  setk FRS_KV_MIN_BLOB_SIZE "$kvmin"
  setk FRS_TRIVIAL_MOVE     "$trivial"
  setk FRS_RS_S2_PINNED     "$s2pin"
  setk FRS_VLOG_COALESCE_DEREF "$coalesce"
  export FRS_SST_COMPRESSION="${comp:-lz4}"
  export FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}"
  # FRS_VLOG_POINT_DEREF deliberately LEFT UNSET -> auto-follows KV-sep (db.rs:467).
  # The never-OOM controller (uniform; un-throttled on a >=40 GiB box).
  setk FRS_MEM_MANAGER "$memmgr"
  # Vlog resident bounds: keep the engine native delta inside the ~6 GiB the
  # process.size=10240m carve-out leaves free in the 16g cgroup (uniform; harmless
  # for light queries whose vlog working set is small).
  export FRS_VLOG_READER_CACHE_CAP="${FRS_VLOG_READER_CACHE_CAP:-2048}"
  export FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-256}"
  export FRS_KV_ADAPTIVE_PRESSURE="${FRS_KV_ADAPTIVE_PRESSURE:-1}"

  echo "  UNIFORM config (SAME for every query):"
  echo "  FRS_KV_SEPARATION=${FRS_KV_SEPARATION:-<unset>}  FRS_KV_MIN_BLOB_SIZE=${FRS_KV_MIN_BLOB_SIZE:-<unset>}  FRS_TRIVIAL_MOVE=${FRS_TRIVIAL_MOVE:-<unset>}"
  echo "  FRS_RS_S2_PINNED=${FRS_RS_S2_PINNED:-<unset>}  FRS_VLOG_COALESCE_DEREF=${FRS_VLOG_COALESCE_DEREF:-<unset>}  FRS_VLOG_POINT_DEREF=<unset: auto-follows KV-sep>"
  echo "  FRS_SST_COMPRESSION=$FRS_SST_COMPRESSION  FRS_MEM_MANAGER=${FRS_MEM_MANAGER:-<unset>}"
  echo "  vlog bounds: reader_cap=$FRS_VLOG_READER_CACHE_CAP resident_budget_mb=$FRS_VLOG_RESIDENT_BUDGET_MB adaptive_pressure=$FRS_KV_ADAPTIVE_PRESSURE"
  echo "  topology: TOPO=split (2x4c/16g + 1x JM 2c/4g), process.size=10240m + SLOTS (templates), parallelism=4"
}

# Run ONE query under the uniform config (forst-rs arm).
run_one() {
  local q="$1"
  local ms="${MAXSEC:-$(maxsec_for "$q")}"
  local tag="$TAG_PREFIX-$q-$ARM"
  echo ""
  echo "================ UNIFORM-CONFIG RUN $q [$ARM] tag=$tag MAXSEC=$ms ================"
  ( apply_uniform
    export TOPO=split
    echo "  -> $RUNNER run $q $ARM $ms $tag"
    CLUSTER="$tag" bash "$RUNNER" run "$q" "$ARM" "$ms" "$tag"
  )
}

# Run ONE query across the requested ARMS for a fair A/B (forst-rs uniform config
# vs the rocksdb / forst baselines). The baselines ignore FRS_* env; they use the
# SAME 2x4c/16g split + SLOTS + parallelism=4 (matched topology, per the V3 rule
# that reuses the stable rdb/forst baselines).
run_validate_one() {
  local q="$1"
  local ms="${MAXSEC:-$(maxsec_for "$q")}"
  local arms="${ARMS_VALIDATE:-forst-rs-ffm-local rocksdb forst-local}"
  echo ""
  echo "################ VALIDATE $q (arms: $arms) MAXSEC=$ms ################"
  for a in $arms; do
    local tag="validate-$q-$a"
    echo ""
    echo "---------------- VALIDATE $q [$a] tag=$tag ----------------"
    if [ "$a" = "forst-rs-ffm-local" ]; then
      ( apply_uniform
        export TOPO=split
        echo "  -> $RUNNER run $q $a $ms $tag"
        CLUSTER="$tag" bash "$RUNNER" run "$q" "$a" "$ms" "$tag"
      )
    else
      # Baseline backend: no forst-rs flags. SAME split + SLOTS as the forst-rs arm.
      ( export TOPO=split
        export FRS_SST_COMPRESSION="${FRS_SST_COMPRESSION:-lz4}"
        echo "  -> $RUNNER run $q $a $ms $tag"
        CLUSTER="$tag" bash "$RUNNER" run "$q" "$a" "$ms" "$tag"
      )
    fi
  done
}

# Lever-attribution A/B: forst-rs uniform-config (A) vs forst-rs levers-OFF (B),
# SAME resource, SAME jar/.so — proves the config is the cause of any win.
run_validate_ab() {
  local q="$1"
  local ms="${MAXSEC:-$(maxsec_for "$q")}"
  echo ""
  echo "################ VALIDATE-AB $q (uniform-config vs levers-OFF) MAXSEC=$ms ################"
  ( apply_uniform
    export TOPO=split
    echo "== ARM A (uniform-config) =="
    CLUSTER="validate-ab-on-$q" bash "$RUNNER" run "$q" forst-rs-ffm-local "$ms" "validate-ab-on-$q" )
  ( unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE FRS_RS_S2_PINNED \
          FRS_VLOG_COALESCE_DEREF FRS_VLOG_POINT_DEREF FRS_MEM_MANAGER 2>/dev/null || true
    export FRS_SST_COMPRESSION=lz4
    export TOPO=split
    echo "== ARM B (levers-OFF) =="
    CLUSTER="validate-ab-off-$q" bash "$RUNNER" run "$q" forst-rs-ffm-local "$ms" "validate-ab-off-$q" )
}

cmd="${1:-}"; [ -n "$cmd" ] && shift || true
case "$cmd" in
  validate)
    sub="${1:-}"; [ -n "$sub" ] && shift || true
    case "$sub" in
      print)  echo "== UNIFORM forst-rs config (applied to EVERY query) =="; ( apply_uniform ) ;;
      sweep)
        QS="${QUERIES:-$ALL_QUERIES}"
        echo "== VALIDATE 3-backend sweep (uniform forst-rs config): $QS =="
        for q in $QS; do run_validate_one "$q"; done
        echo ""; echo "== VALIDATE sweep done =="
        ;;
      q*) run_validate_one "$sub" ;;
      ""|-h|--help) echo "usage: run-best.sh validate (print | sweep | <query>)"; exit 0 ;;
      *) echo "unknown validate subcommand: $sub"; exit 1 ;;
    esac
    ;;
  validate-ab)
    q="${1:-}"; [ -n "$q" ] || { echo "usage: run-best.sh validate-ab <query>"; exit 1; }
    run_validate_ab "$q"
    ;;
  print)
    echo "== UNIFORM forst-rs config (applied to EVERY query) =="
    apply_uniform
    ;;
  sweep)
    QS="${QUERIES:-$ALL_QUERIES}"
    echo "== uniform-config sweep: $QS (arm=$ARM, serial, SAME config) =="
    for q in $QS; do run_one "$q"; done
    echo ""; echo "== uniform-config sweep done =="
    ;;
  q*)
    run_one "$cmd"
    ;;
  ""|-h|--help|help)
    sed -n '2,40p' "$0"
    ;;
  *)
    echo "unknown: $cmd (expected: <query> | sweep | print | validate (print|sweep|<query>) | validate-ab <query> | help)"; exit 1
    ;;
esac
