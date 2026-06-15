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
# FULL-STACK-ON VALIDATION (the goal-critical "beat BOTH RocksDB and ForSt" proof)
#   run-best.sh validate print [<query>]   # DRY-RUN: print the resolved full-stack-ON
#                                            forst-rs flag set per query (NO run).
#   run-best.sh validate <query>           # run ONE query, full-stack-ON forst-rs arm
#                                            PLUS the rocksdb + forst-local baselines
#                                            (the cross-backend A vs B vs C comparison).
#   run-best.sh validate sweep             # all 8 priority queries × 3 arms, serial.
#   run-best.sh validate-ab <query>        # forst-rs full-stack-ON (A) vs forst-rs
#                                            flags-OFF (B) — the lever-attribution A/B
#                                            for the read-amp joins (q7/q9/q20).
#
#   The validate profile turns ON, PER QUERY, the levers that query wants (see the
#   VALIDATE config table below; every flag name is verified against the engine
#   source in configs/best-config.tsv and crates/forst-rs-engine/src/db.rs). It is
#   DISTINCT from the plain best-config rows above: best-config is the conservative
#   already-MEASURED per-query winner; validate is the experimental full-built-stack
#   that the e2e run is meant to CONFIRM (it may beat, match, or regress best-config).
#   ARMS (validate only): default "forst-rs-ffm-local rocksdb forst-local"; override
#   with ARMS="forst-rs-ffm-local rocksdb".
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
# A single TM with all 8 cores + 36g gives q9 the per-TM memory the split could
# not. The 36g default assumes a box with >=40 GiB physical RAM (the dev Mac and
# the origin Linux box both qualify); run-8c32g.sh auto-detects physical RAM and
# WARNS if 36g + OS headroom won't fit, so override SINGLE_TM_MEM on a smaller
# box. KV-sep's resident vlog readers are additionally BOUNDED (count cap + byte
# budget + adaptive pressure back-off) so the engine delta stays small.
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

# =====================================================================
# FULL-STACK-ON VALIDATION PROFILE (the goal-critical "beat BOTH" proof)
# =====================================================================
# Per-query full-built-stack flag set for the forst-rs arm. Each query gets the
# levers it wants (read-amp joins vs windowed/OVER vs the q17 inline carve-out).
# Flag names are verified against crates/forst-rs-engine/src/db.rs (line refs in
# configs/best-config.tsv header) and the OPT-N04 merge-RMW backend design doc.
#
# Sets the FRS_* env then defers to the SAME run_one / topology path the best
# config uses, so the docker plumbing (TOPO=split, namespacing, cross-platform
# detection) is identical. Echoes a human-readable summary for `validate print`.
#
# The shared join read-amp stack (q7/q9/q20): KV-sep + min_blob 256 + coalesced
# vlog deref (A) + S2-pinned + adaptive-S2 (R1) + probe-bloom prune (MR-1) +
# leveled-hot-CF (Approach-1) + persistent probe-iter (Approach-1). q9 ALSO needs
# the 8c/36g single-TM topology (else KV-sep OOMs the split's 16g cgroup); it
# routes through run_q9_36g's resource block with the join stack layered on.
# The windowed/OVER stack (q8/q11/q12/q18): merge-RMW (A2) + routing-adaptive (R2a).
# q17: zero-handoff inline carve-out (routing-adaptive selects the iter-free path),
# KV-sep OFF. q19: KV-sep ON (already wins).
apply_validate() {
  local q="$1"
  # Clear everything the validate/best paths may set so nothing leaks across queries.
  unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE \
        FRS_RS_S2_PINNED FRS_S2_FANOUT_MIN FRS_RS_EXECUTOR FRS_VLOG_COALESCE_DEREF \
        FRS_RS_PROBE_BLOOM_PRUNE FRS_RS_LEVELED_HOT_CF FRS_PERSISTENT_PROBE_ITER \
        FRS_RS_MERGE_RMW FRS_RS_MERGE_RMW_STATES 2>/dev/null || true

  # Always-on fairness/reproducibility baseline.
  export FRS_SST_COMPRESSION="${FRS_SST_COMPRESSION:-lz4}"
  export FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}"

  # --- the shared read-amp-join lever stack (used by q4/q7/q9/q19/q20) ---
  join_stack() {
    export FRS_KV_SEPARATION=true
    export FRS_KV_MIN_BLOB_SIZE="${FRS_KV_MIN_BLOB_SIZE:-256}"
    export FRS_TRIVIAL_MOVE="${FRS_TRIVIAL_MOVE:-true}"
    export FRS_RS_S2_PINNED="${FRS_RS_S2_PINNED:-1}"
    export FRS_S2_FANOUT_MIN="${FRS_S2_FANOUT_MIN:-8}"   # R1 adaptive S2 (deep/shallow split)
    export FRS_VLOG_COALESCE_DEREF="${FRS_VLOG_COALESCE_DEREF:-1}"      # Approach A
    export FRS_RS_PROBE_BLOOM_PRUNE="${FRS_RS_PROBE_BLOOM_PRUNE:-1}"    # MR-1
    export FRS_RS_LEVELED_HOT_CF="${FRS_RS_LEVELED_HOT_CF:-1}"          # Approach-1
    export FRS_PERSISTENT_PROBE_ITER="${FRS_PERSISTENT_PROBE_ITER:-1}"  # Approach-1
  }
  # --- the windowed/OVER lever stack (used by q8/q11/q12/q18) ---
  window_stack() {
    export FRS_RS_MERGE_RMW="${FRS_RS_MERGE_RMW:-1}"                    # Approach-2 / A2
    export FRS_RS_EXECUTOR="${FRS_RS_EXECUTOR:-routing-adaptive}"        # Approach-3 / R2a
  }

  case "$q" in
    q7|q20)  join_stack ;;                       # read-amp joins, 8c/32g split
    q9)      join_stack ;;                        # read-amp join, BUT 8c/36g (see run_validate_one)
    q4)      join_stack ;;                        # write/value-carrying join
    q19)     join_stack ;;                        # KV-sep already wins; full stack layered
    q8|q11|q12|q18)
             window_stack ;;                      # windowed / OVER
    q17)     # zero-handoff inline carve-out: routing-adaptive selects the iter-free
             # path for the unbounded group-agg; KV-sep OFF (q17 STRUCTURAL vs RDB,
             # beats ForSt 3.3x on this path). NO join stack, NO merge-RMW.
             export FRS_RS_EXECUTOR="${FRS_RS_EXECUTOR:-routing-adaptive}" ;;
    *)       echo "WARN: $q has no validate profile; running fairness baseline only" ;;
  esac

  echo "  query=$q  VALIDATE full-stack-ON (forst-rs arm)"
  echo "  KV: FRS_KV_SEPARATION=${FRS_KV_SEPARATION:-<unset>} min_blob=${FRS_KV_MIN_BLOB_SIZE:-<unset>} trivial=${FRS_TRIVIAL_MOVE:-<unset>}"
  echo "  S2: FRS_RS_S2_PINNED=${FRS_RS_S2_PINNED:-<unset>} FRS_S2_FANOUT_MIN=${FRS_S2_FANOUT_MIN:-<unset>}"
  echo "  read-amp: COALESCE_DEREF=${FRS_VLOG_COALESCE_DEREF:-<unset>} PROBE_BLOOM_PRUNE=${FRS_RS_PROBE_BLOOM_PRUNE:-<unset>} LEVELED_HOT_CF=${FRS_RS_LEVELED_HOT_CF:-<unset>} PERSISTENT_PROBE_ITER=${FRS_PERSISTENT_PROBE_ITER:-<unset>}"
  echo "  window/OVER: MERGE_RMW=${FRS_RS_MERGE_RMW:-<unset>} EXECUTOR=${FRS_RS_EXECUTOR:-<unset>}"
  echo "  compression: SST=$FRS_SST_COMPRESSION VLOG=$FRS_VLOG_COMPRESSION"
  if [ "$q" = "q9" ]; then
    echo "  topology: q9 routes through the 8c/36g single-TM resource (run_q9_36g block) with the join stack layered."
  fi
}

# Run ONE query under the validate profile across the requested ARMS (the forst-rs
# arm gets the full stack; rocksdb / forst-local are the baseline backends with NO
# forst-rs flags — they ignore FRS_* env). q9 uses the 8c/36g topology.
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
      ( apply_validate "$q"
        if [ "$q" = "q9" ]; then
          # 8c/36g single-TM: KV-sep needs the per-TM memory the split's 16g lacks.
          export TOPO=single
          export SINGLE_TM_CPUS="${SINGLE_TM_CPUS:-8}"
          export SINGLE_TM_MEM="${SINGLE_TM_MEM:-36g}"
          export FRS_TM_PROCESS_SIZE="${FRS_TM_PROCESS_SIZE:-16384m}"
          export FRS_JM_PROCESS_SIZE="${FRS_JM_PROCESS_SIZE:-3072m}"
          export FRS_VLOG_READER_CACHE_CAP="${FRS_VLOG_READER_CACHE_CAP:-2048}"
          export FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-512}"
          export FRS_KV_ADAPTIVE_PRESSURE="${FRS_KV_ADAPTIVE_PRESSURE:-1}"
          echo "  -> q9 8c/36g single-TM topology"
        else
          export TOPO=split
        fi
        echo "  -> $RUNNER run $q $a $ms $tag"
        CLUSTER="$tag" bash "$RUNNER" run "$q" "$a" "$ms" "$tag"
      )
    else
      # Baseline backend: no forst-rs flags. q9 uses the same 36g topology so the
      # comparison is at the SAME resource (apples to apples for the beat-both proof).
      ( if [ "$q" = "q9" ]; then
          export TOPO=single SINGLE_TM_CPUS="${SINGLE_TM_CPUS:-8}" SINGLE_TM_MEM="${SINGLE_TM_MEM:-36g}"
          export FRS_TM_PROCESS_SIZE="${FRS_TM_PROCESS_SIZE:-16384m}" FRS_JM_PROCESS_SIZE="${FRS_JM_PROCESS_SIZE:-3072m}"
        else
          export TOPO=split
        fi
        export FRS_SST_COMPRESSION="${FRS_SST_COMPRESSION:-lz4}"
        echo "  -> $RUNNER run $q $a $ms $tag"
        CLUSTER="$tag" bash "$RUNNER" run "$q" "$a" "$ms" "$tag"
      )
    fi
  done
}

# Lever-attribution A/B for the read-amp joins: forst-rs full-stack-ON (A) vs
# forst-rs flags-OFF (B), SAME resource, SAME jar/.so — proves the stack is the
# cause of any beat-both win (not box noise).
run_validate_ab() {
  local q="$1"
  local ms="${MAXSEC:-$(maxsec_for "$q")}"
  echo ""
  echo "################ VALIDATE-AB $q (full-stack-ON vs flags-OFF) MAXSEC=$ms ################"
  # Arm A: full stack ON
  ( apply_validate "$q"
    if [ "$q" = "q9" ]; then export TOPO=single SINGLE_TM_CPUS="${SINGLE_TM_CPUS:-8}" SINGLE_TM_MEM="${SINGLE_TM_MEM:-36g}" FRS_TM_PROCESS_SIZE="${FRS_TM_PROCESS_SIZE:-16384m}" FRS_JM_PROCESS_SIZE="${FRS_JM_PROCESS_SIZE:-3072m}" FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-512}" FRS_KV_ADAPTIVE_PRESSURE=1; else export TOPO=split; fi
    echo "== ARM A (full-stack-ON) =="
    CLUSTER="validate-ab-on-$q" bash "$RUNNER" run "$q" forst-rs-ffm-local "$ms" "validate-ab-on-$q" )
  # Arm B: all forst-rs levers OFF (engine defaults; lz4 kept for fairness)
  ( unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE FRS_RS_S2_PINNED \
          FRS_S2_FANOUT_MIN FRS_RS_EXECUTOR FRS_VLOG_COALESCE_DEREF FRS_RS_PROBE_BLOOM_PRUNE \
          FRS_RS_LEVELED_HOT_CF FRS_PERSISTENT_PROBE_ITER FRS_RS_MERGE_RMW 2>/dev/null || true
    export FRS_SST_COMPRESSION=lz4
    if [ "$q" = "q9" ]; then export TOPO=single SINGLE_TM_CPUS="${SINGLE_TM_CPUS:-8}" SINGLE_TM_MEM="${SINGLE_TM_MEM:-36g}" FRS_TM_PROCESS_SIZE="${FRS_TM_PROCESS_SIZE:-16384m}" FRS_JM_PROCESS_SIZE="${FRS_JM_PROCESS_SIZE:-3072m}"; else export TOPO=split; fi
    echo "== ARM B (flags-OFF) =="
    CLUSTER="validate-ab-off-$q" bash "$RUNNER" run "$q" forst-rs-ffm-local "$ms" "validate-ab-off-$q" )
}

cmd="${1:-}"; [ -n "$cmd" ] && shift || true
case "$cmd" in
  validate)
    sub="${1:-}"; [ -n "$sub" ] && shift || true
    case "$sub" in
      print)
        q="${1:-}"
        if [ -n "$q" ]; then
          echo "== VALIDATE full-stack-ON config for $q =="
          ( apply_validate "$q" )
        else
          for q in $ALL_QUERIES q8 q18; do
            echo "== VALIDATE full-stack-ON config for $q =="
            ( apply_validate "$q" )
            echo ""
          done
        fi
        ;;
      sweep)
        QS="${QUERIES:-q4 q7 q8 q9 q11 q12 q17 q18 q19 q20}"
        echo "== VALIDATE full-stack-ON sweep: $QS =="
        for q in $QS; do run_validate_one "$q"; done
        echo ""
        echo "== VALIDATE sweep done =="
        ;;
      q*) run_validate_one "$sub" ;;
      ""|-h|--help) echo "usage: run-best.sh validate (print [<q>] | sweep | <query>)"; exit 0 ;;
      *) echo "unknown validate subcommand: $sub"; exit 1 ;;
    esac
    ;;
  validate-ab)
    q="${1:-}"; [ -n "$q" ] || { echo "usage: run-best.sh validate-ab <query>"; exit 1; }
    run_validate_ab "$q"
    ;;
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
    echo "unknown: $cmd (expected: <query> | sweep | print [<query>] | validate ... | validate-ab <query> | q9-36g | help)"; exit 1
    ;;
esac
