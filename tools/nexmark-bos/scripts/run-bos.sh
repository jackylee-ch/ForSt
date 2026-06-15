#!/usr/bin/env bash
# run-bos.sh — drive the NexMark sweep with forst-rs state DISAGGREGATED onto
# BOS (Baidu Object Storage = the real remote S3). The REMOTE/BOS sibling of
# tools/nexmark-local/scripts/run-best.sh + run-s3sim.sh.
#
# WHAT IT DOES
#   * Selects the BOS disagg config (tools/nexmark-bos/configs/
#     config-forst-rs.yaml.tpl) by pointing TEMPLATES at this package's configs
#     dir and driving measure-sql.sh's `forst-rs-ffm-s3` arm (which `envsubst`s
#     the S3_* creds into that template's storage.uri + opendal-config). The
#     engine's OpenDAL remote store thus points at the LIVE BOS endpoint.
#   * Sets the RECOMMENDED remote disagg knobs (KV-sep ON, coalesced vlog deref,
#     link-mode checkpoint, upload-rate split, non-SST-local routing, paced
#     instant-restore, bounded resident vlog + local-cache budgets) and forwards
#     them into the TM/JM containers via this package's run-8c32g.sh.
#   * Drives the priority queries against BOS-resident state, TOPO=split, serial.
#   * Reuses the cross-platform OS / disk / jemalloc / io_uring detection from
#     run-8c32g.sh (the origin Linux box is the target; BOS is remote).
#
# USAGE
#   run-bos.sh print [<query>]   # resolve + print the BOS config + knobs (NO run)
#   run-bos.sh <query>           # run ONE priority query against BOS
#   run-bos.sh sweep             # run ALL priority queries serially against BOS
#
#   Subset:   QUERIES="q7 q9" run-bos.sh sweep
#   Per-run cap: MAXSEC (default per-query; q7/q9/q20=2700, else 1500).
#
# REQUIRED ENV (the BOS bucket + creds; `print` does NOT require them):
#   S3_ENDPOINT  S3_BUCKET  S3_ACCESS_KEY  S3_SECRET_KEY
# OPTIONAL ENV:
#   S3_REGION (default us-east-1)   S3_PREFIX (default nexmark-bos)
#
# OPTIMIZATION KNOBS (recommended defaults below; override any from the env):
#   FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_VLOG_COALESCE_DEREF
#   FRS_CKPT_LINK_MODE FRS_REMOTE_NONSST_LOCAL
#   FRS_UPLOAD_RATE_SPLIT FRS_UPLOAD_COMPACTION_SHARE
#   FRS_RESTORE_BG_FILL FRS_RESTORE_BG_FILL_WORKERS FRS_RESTORE_BG_FILL_PACE_MB
#   FRS_VLOG_RESIDENT_BUDGET_MB FRS_CACHE_SPACE_LIMIT_MB
#   FRS_REMOTE_BW_MBPS (0/unset = the LIVE BOS link's real bandwidth)
#   FRS_REMOTE_COMPACTION
# RESOURCE PROFILE (parameterized; passed through to run-8c32g.sh):
#   TOPO SPLIT_TM_CPUS SPLIT_TM_MEM SPLIT_JM_CPUS SPLIT_JM_MEM
#   SINGLE_TM_CPUS SINGLE_TM_MEM FRS_TM_PROCESS_SIZE FRS_JM_PROCESS_SIZE
#
# This script ONLY sets env + calls run-8c32g.sh. Build first:
#   bash tools/nexmark-bos/scripts/run-8c32g.sh build   (+ jar on the box)
#
# NOTE: real-BOS perf is gated on the >=50 Gb/s online box. Until then use the
# LOCAL S3 simulation (tools/nexmark-local/scripts/run-s3sim.sh) or model a link
# with FRS_REMOTE_BW_MBPS. This harness is ready to fire — see docs/README.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIGS_DIR="$PKG_DIR/configs"
[ -f "$CONFIGS_DIR/config-forst-rs.yaml.tpl" ] \
  || { echo "FATAL: missing $CONFIGS_DIR/config-forst-rs.yaml.tpl"; exit 1; }

# Repo root: prefer an explicit REPO (the box checkout), else the engine repo
# that contains this package (../../.. from tools/nexmark-bos/scripts).
REPO="${REPO:-$(cd "$PKG_DIR/../.." && pwd)}"
# Use this package's run-8c32g.sh (forwards the real S3 creds + the BOS disagg
# knobs); fall back to the canonical one only if the package copy is missing.
RUNNER="$PKG_DIR/scripts/run-8c32g.sh"
[ -f "$RUNNER" ] || RUNNER="$REPO/scripts/run-8c32g.sh"
[ -f "$RUNNER" ] || { echo "FATAL: missing run-8c32g.sh (looked in $PKG_DIR/scripts and $REPO/scripts)"; exit 1; }

# The BOS run always uses the S3 arm (config-forst-rs.yaml.tpl). Point TEMPLATES
# at this package's configs dir so measure-sql.sh picks up the BOS-tuned copy.
ARM="forst-rs-ffm-s3"
TAG_PREFIX="${TAG_PREFIX:-bos}"

ALL_QUERIES="q4 q7 q9 q11 q12 q17 q19 q20"

# Per-query MAXSEC defaults (heavy joins get more headroom; plain case so this
# runs on macOS bash 3.2 too — the script runs on the HOST and shells out).
maxsec_for() {
  case "$1" in
    q7|q9|q20) echo 2700 ;;
    *)         echo 1500 ;;
  esac
}

# --- recommended REMOTE/BOS disagg knobs (every one env-overridable) ---
# These are THE optimization levers. Defaults are the recommended starting
# point for BOS-resident state; see docs/README.md for what each tunes.
apply_bos_knobs() {
  # KV separation ON: values -> vlog blobs (write-amp / value-carrying reads).
  export FRS_KV_SEPARATION="${FRS_KV_SEPARATION:-true}"
  export FRS_KV_MIN_BLOB_SIZE="${FRS_KV_MIN_BLOB_SIZE:-256}"
  export FRS_TRIVIAL_MOVE="${FRS_TRIVIAL_MOVE:-true}"
  # Coalesce N vlog derefs into ONE remote GET — the N->1 remote-GET win.
  export FRS_VLOG_COALESCE_DEREF="${FRS_VLOG_COALESCE_DEREF:-1}"
  # WAL-DELTA / LINK checkpoint: link SSTs instead of re-uploading (cheap
  # incremental ckpt over the slow remote link).
  export FRS_CKPT_LINK_MODE="${FRS_CKPT_LINK_MODE:-1}"
  # Keep chatty small files (MANIFEST/CURRENT/OPTIONS/WAL/journal) LOCAL — only
  # SST-class objects go to BOS (no S3 metadata storms).
  export FRS_REMOTE_NONSST_LOCAL="${FRS_REMOTE_NONSST_LOCAL:-1}"
  # QoS-split the upload bandwidth so a compaction burst can't starve the
  # flush -> checkpoint critical path; compaction gets COMPACTION_SHARE.
  export FRS_UPLOAD_RATE_SPLIT="${FRS_UPLOAD_RATE_SPLIT:-1}"
  export FRS_UPLOAD_COMPACTION_SHARE="${FRS_UPLOAD_COMPACTION_SHARE:-0.5}"
  # Instant link-restore: paced background cache warm of adopted physicals.
  export FRS_RESTORE_BG_FILL="${FRS_RESTORE_BG_FILL:-1}"
  export FRS_RESTORE_BG_FILL_WORKERS="${FRS_RESTORE_BG_FILL_WORKERS:-2}"
  export FRS_RESTORE_BG_FILL_PACE_MB="${FRS_RESTORE_BG_FILL_PACE_MB:-64}"
  # Bound the KV-sep resident vlog working set (fits a small per-TM cgroup).
  export FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-512}"
  # Free-disk-headroom floor for the local LRU SST cache (0/unset = size-only).
  export FRS_CACHE_SPACE_LIMIT_MB="${FRS_CACHE_SPACE_LIMIT_MB:-8192}"
  # Remote-leg bandwidth cap (MiB/s). 0/unset = the LIVE BOS link's real
  # bandwidth (the production default); set a number ONLY to MODEL a slower
  # link on a fast box. 6250 ~= 50 Gb/s.
  export FRS_REMOTE_BW_MBPS="${FRS_REMOTE_BW_MBPS:-0}"
  # SST/vlog compression: lz4 is the engine default AND the fair match.
  export FRS_SST_COMPRESSION="${FRS_SST_COMPRESSION:-lz4}"
  export FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}"
  # Optional: route compaction to the remote store (off by default).
  export FRS_REMOTE_COMPACTION="${FRS_REMOTE_COMPACTION:-}"
}

# --- the S3/BOS connection env (creds required for a real run, not for print) ---
apply_bos_storage() {
  export S3_REGION="${S3_REGION:-us-east-1}"
  export S3_PREFIX="${S3_PREFIX:-nexmark-bos}"
  # Required for a real run. (S3_PREFIX/S3_REGION have safe defaults above.)
  export S3_ENDPOINT="${S3_ENDPOINT:-}"
  export S3_BUCKET="${S3_BUCKET:-}"
  export S3_ACCESS_KEY="${S3_ACCESS_KEY:-}"
  export S3_SECRET_KEY="${S3_SECRET_KEY:-}"
}

require_creds() {
  local missing=""
  [ -n "${S3_ENDPOINT:-}" ]   || missing="$missing S3_ENDPOINT"
  [ -n "${S3_BUCKET:-}" ]     || missing="$missing S3_BUCKET"
  [ -n "${S3_ACCESS_KEY:-}" ] || missing="$missing S3_ACCESS_KEY"
  [ -n "${S3_SECRET_KEY:-}" ] || missing="$missing S3_SECRET_KEY"
  if [ -n "$missing" ]; then
    echo "FATAL: missing required BOS env:$missing"
    echo "       export S3_ENDPOINT / S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY"
    echo "       (S3_REGION default us-east-1, S3_PREFIX default nexmark-bos)."
    echo "       Use 'run-bos.sh print' to dry-run the config without creds."
    exit 1
  fi
}

# Mask a secret for echo (show first 3 chars + length).
mask() { local v="${1:-}"; [ -z "$v" ] && { echo "<unset>"; return; }; echo "${v:0:3}…(${#v})"; }

print_resolved() {
  apply_bos_storage
  apply_bos_knobs
  echo "  arm=$ARM  templates=$CONFIGS_DIR  topo=${TOPO:-split}"
  echo "  -- BOS storage --"
  echo "  S3_ENDPOINT=${S3_ENDPOINT:-<unset>}  S3_BUCKET=${S3_BUCKET:-<unset>}  S3_REGION=$S3_REGION  S3_PREFIX=$S3_PREFIX"
  echo "  S3_ACCESS_KEY=$(mask "${S3_ACCESS_KEY:-}")  S3_SECRET_KEY=$(mask "${S3_SECRET_KEY:-}")"
  echo "  storage.uri = s3://${S3_BUCKET:-<bucket>}/${S3_PREFIX}/forst-rs-data-<RUN_ID>/"
  echo "  -- optimization knobs --"
  echo "  FRS_KV_SEPARATION=$FRS_KV_SEPARATION  FRS_KV_MIN_BLOB_SIZE=$FRS_KV_MIN_BLOB_SIZE  FRS_VLOG_COALESCE_DEREF=$FRS_VLOG_COALESCE_DEREF"
  echo "  FRS_CKPT_LINK_MODE=$FRS_CKPT_LINK_MODE  FRS_REMOTE_NONSST_LOCAL=$FRS_REMOTE_NONSST_LOCAL"
  echo "  FRS_UPLOAD_RATE_SPLIT=$FRS_UPLOAD_RATE_SPLIT  FRS_UPLOAD_COMPACTION_SHARE=$FRS_UPLOAD_COMPACTION_SHARE"
  echo "  FRS_RESTORE_BG_FILL=$FRS_RESTORE_BG_FILL  (_WORKERS=$FRS_RESTORE_BG_FILL_WORKERS _PACE_MB=$FRS_RESTORE_BG_FILL_PACE_MB)"
  echo "  FRS_VLOG_RESIDENT_BUDGET_MB=$FRS_VLOG_RESIDENT_BUDGET_MB  FRS_CACHE_SPACE_LIMIT_MB=$FRS_CACHE_SPACE_LIMIT_MB"
  echo "  FRS_REMOTE_BW_MBPS=$FRS_REMOTE_BW_MBPS (0 = live BOS bandwidth)  FRS_SST_COMPRESSION=$FRS_SST_COMPRESSION  FRS_VLOG_COMPRESSION=$FRS_VLOG_COMPRESSION"
  echo "  FRS_REMOTE_COMPACTION=${FRS_REMOTE_COMPACTION:-<unset>}"
}

run_one() {
  local q="$1"
  local ms="${MAXSEC:-$(maxsec_for "$q")}"
  local tag="$TAG_PREFIX-$q"
  echo ""
  echo "================ BOS DISAGG RUN $q [$ARM] tag=$tag MAXSEC=$ms ================"
  ( apply_bos_storage
    require_creds
    apply_bos_knobs
    export TEMPLATES="$CONFIGS_DIR"
    export TOPO="${TOPO:-split}"
    print_resolved
    echo "  -> $RUNNER run $q $ARM $ms $tag"
    CLUSTER="$tag" bash "$RUNNER" run "$q" "$ARM" "$ms" "$tag"
  )
}

cmd="${1:-}"; [ -n "$cmd" ] && shift || true
case "$cmd" in
  print)
    q="${1:-}"
    if [ -n "$q" ]; then
      echo "== resolved BOS config for $q (MAXSEC=$(maxsec_for "$q")) =="
      ( print_resolved )
    else
      echo "== resolved BOS config (applies to all priority queries) =="
      ( print_resolved )
      echo ""
      echo "  priority queries: $ALL_QUERIES"
      echo "  per-query MAXSEC: q7/q9/q20=2700, else 1500"
    fi
    ;;
  sweep)
    QS="${QUERIES:-$ALL_QUERIES}"
    echo "== BOS disagg sweep: $QS (arm=$ARM, serial) =="
    for q in $QS; do run_one "$q"; done
    echo ""
    echo "== BOS disagg sweep done =="
    ;;
  q*)
    run_one "$cmd"
    ;;
  ""|-h|--help|help)
    sed -n '2,45p' "$0"
    ;;
  *)
    echo "unknown: $cmd (expected: <query> | sweep | print [<query>] | help)"; exit 1
    ;;
esac
