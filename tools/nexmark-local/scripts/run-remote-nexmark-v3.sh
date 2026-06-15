#!/usr/bin/env bash
# run-remote-nexmark-v3.sh — UNIFORM-config remote NexMark sweep wrapper.
#
# Companion to docs/superpowers/specs/2026-06-14-remote-nexmark-config-v3.md and
# the runbook docs/superpowers/specs/2026-06-13-nexmark-e2e-test-runbook.md.
#
# WHAT IT DOES
#   Pins the ONE uniform config (per-query tuning is FORBIDDEN — the only allowed
#   optimization is dynamic/adaptive engine behavior under one config) and invokes
#   the UNMODIFIED scripts/run-8c32g.sh (TOPO=split) for the 8 priority queries ×
#   3 backends, STRICTLY SERIAL (one cluster at a time), with FRS_CTMP_BASE from
#   pick-disk.sh and a per-cluster CLUSTER namespace.
#
#   It ONLY sets env + calls run-8c32g.sh. It does NOT modify run-8c32g.sh or any
#   template, and it does NOT build (run the runbook §0 (b)-(c) for image/.so/jar).
#
# UNIFORM CONFIG ENCODED (see the v3 doc §11):
#   TOPO=split ; backends forst-rs-ffm-local | rocksdb | forst-local ; MAXSEC=3600
#   forst-rs levers: FRS_SST_COMPRESSION=lz4 (ON), all of KV_SEPARATION /
#   TRIVIAL_MOVE / RS_S2_PINNED / REMOTE_COMPACTION OFF (adaptive versions are the
#   in-progress dynamic layer that activates under the SAME config). jemalloc
#   LD_PRELOAD on (FRS_TM_JEMALLOC=1). MOCK S3 only: NexMark arm is the LocalFS
#   forst-rs-ffm-local; real S3 is OFF. FRS_MODEL_BW_MBPS=6250 is exported as the
#   documented disagg-minibench projection only (run-8c32g.sh / NexMark ignore it).
#
# USAGE (on the remote box, after `git pull` to /ssd2/jackylee — v3 doc §7):
#   TOPO=split REPO=/ssd2/jackylee/ForSt WORKENV=~/workenv \
#     FLINK=~/workenv/flink-2.2.1 IMG=forst-bench:x86 PLAT=linux/amd64 \
#     NEXMARK_HOME=~/workenv/nexmark-flink \
#     bash /ssd2/jackylee/ForSt/scripts/run-remote-nexmark-v3.sh
#
#   Make it executable once (git preserves the mode after):
#     chmod +x /ssd2/jackylee/ForSt/scripts/run-remote-nexmark-v3.sh
#
# OPTIONAL OVERRIDES (env):
#   QUERIES="q4 q9"        # subset of the 8 priority queries
#   ARMS="forst-rs-ffm-local rocksdb"   # subset of backends
#   MAXSEC=3600            # per-run cap
#   TAG_PREFIX=rv3         # cluster/tag prefix
#   FRS_CTMP_BASE=...      # skip pick-disk.sh and force a scratch base
set -euo pipefail

# Repo root (this script lives in scripts/). REPO can override (the harness uses
# absolute host paths, so REPO must match the checkout on the box).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-$(cd "$SCRIPT_DIR/.." && pwd)}"
RUNNER="$REPO/scripts/run-8c32g.sh"
PICK_DISK="$REPO/scripts/pick-disk.sh"
[ -f "$RUNNER" ] || { echo "FATAL: missing $RUNNER"; exit 1; }

# --- the uniform sweep matrix (no per-query config; same set for all) ---
QUERIES="${QUERIES:-q4 q7 q9 q11 q12 q17 q19 q20}"
ARMS="${ARMS:-forst-rs-ffm-local rocksdb forst-local}"
MAXSEC="${MAXSEC:-3600}"
TAG_PREFIX="${TAG_PREFIX:-rv3}"

# --- the ONE uniform config (exported once; reused for every query × backend) ---
# Topology: split = 2 TM 4c/16g + 1 JM 2c/4g (8c/32g = TM-only).
export TOPO=split
# forst-rs lever flags at their UNIFORM values. lz4 ON (fairness + engine
# default); the rest OFF — their adaptive versions are the in-progress dynamic
# layer that will activate under this SAME config without per-query flags.
export FRS_SST_COMPRESSION=lz4
export FRS_VLOG_COMPRESSION=inherit
unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE \
      FRS_RS_S2_PINNED FRS_REMOTE_COMPACTION 2>/dev/null || true
# jemalloc LD_PRELOAD on the TM JVM, uniform across all backends (box property).
export FRS_TM_JEMALLOC="${FRS_TM_JEMALLOC:-1}"
# Disagg-minibench projection ONLY (run-8c32g.sh / NexMark do not read this; the
# NexMark perf arm is LocalFS). Documented value for the ≈50 Gb/s online box.
export FRS_MODEL_BW_MBPS="${FRS_MODEL_BW_MBPS:-6250}"

# --- pick ONE disk for the whole sweep so populations don't mix ---
# pick-disk.sh is platform-aware (Linux NVMe %util sample; macOS $TMPDIR). On a
# Linux box the default candidates are /ssd2|/ssd1|/tmp under $USER; the origin
# checkout lives at /ssd2/$USER/ForSt (documented, not hardcoded).
if [ -z "${FRS_CTMP_BASE:-}" ]; then
  if [ -x "$PICK_DISK" ] || [ -f "$PICK_DISK" ]; then
    BASE="$(bash "$PICK_DISK")"
  else
    BASE="/tmp/$USER"
  fi
  export FRS_CTMP_BASE="$BASE/frs-bench-tmp"
fi
echo "== v3 uniform sweep =="
echo "   REPO=$REPO  TOPO=$TOPO  MAXSEC=$MAXSEC"
echo "   FRS_CTMP_BASE=$FRS_CTMP_BASE"
echo "   levers: SST_COMPRESSION=$FRS_SST_COMPRESSION (KV_SEP/TRIVIAL_MOVE/S2_PINNED/REMOTE_COMPACTION OFF)"
echo "   QUERIES=$QUERIES"
echo "   ARMS=$ARMS"

# --- strictly serial: one cluster at a time ---
for q in $QUERIES; do
  for cfg in $ARMS; do
    tag="$TAG_PREFIX-$q-$cfg"
    echo ""
    echo "================ RUN $q [$cfg] tag=$tag ================"
    # CLUSTER namespaces containers/network/conf/scratch per run. run-8c32g.sh
    # tears its own cluster down before `run` returns, so the loop is serial.
    CLUSTER="$tag" bash "$RUNNER" run "$q" "$cfg" "$MAXSEC" "$tag"
  done
done

echo ""
echo "== v3 uniform sweep done =="
