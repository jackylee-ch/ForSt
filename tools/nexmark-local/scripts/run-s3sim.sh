#!/usr/bin/env bash
# run-s3sim.sh — LOCAL S3 SIMULATION driver for forst-rs disaggregated state.
#
# Implements the 2026-06-14 user directive: "No real S3 available. Simulate S3
# access — use one directory as the S3 directory and another as the local
# directory; when accessing the S3 directory, limit bandwidth to 50 Gb/s."
#
# The bandwidth cap is the engine env knob FRS_REMOTE_BW_MBPS (MiB/s), enforced
# by a token-bucket throttle wrapping ONLY the remote/object-store leg of the
# disaggregated FS stack (crates/forst-rs-io/src/throttle.rs:ThrottledFileSystem,
# wired in crates/forst-rs-engine/src/db.rs:wrap_remote_bw_throttle). The local
# SST cache and the local store are never throttled. Default OFF (0/unset) =
# byte-identical pass-through; 6250 MiB/s = 50 Gb/s.
#
# MODES
#   smoke   (default) — runs the engine integration smoke that proves:
#                         (1) rows round-trip byte-exactly with the throttle ON,
#                         (2) the throttle paces the remote leg but NOT a sibling
#                             local dir. Fast, no Docker, no NexMark wall.
#   sweep             — drives the full NexMark uniform sweep with the S3-sim
#                       config + throttle (DEFERRED: needs a clean Mac / the
#                       remote box; a heavy NexMark wall must not contend with a
#                       perf-sensitive build). Mock-S3 only; real S3 is gated.
#
# USAGE
#   bash tools/nexmark-local/scripts/run-s3sim.sh smoke
#   FRS_REMOTE_BW_MBPS=6250 bash tools/nexmark-local/scripts/run-s3sim.sh sweep
#
# ENV
#   FRS_REMOTE_BW_MBPS   remote-leg cap in MiB/s (default 6250 = 50 Gb/s here).
#   S3_DIR / LOCAL_DIR   the two distinct dirs (default under $TMPDIR/frs-s3sim).
#   QUERIES / ARMS / MAXSEC   forwarded to the sweep (see run-remote-nexmark-v3.sh).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Package root is tools/nexmark-local; repo root is three levels up.
REPO="${REPO:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"

MODE="${1:-smoke}"

# The 50 Gb/s S3-sim regime by default; 0/unset disables the throttle.
export FRS_REMOTE_BW_MBPS="${FRS_REMOTE_BW_MBPS:-6250}"

# Two DISTINCT directories: the remote "S3" dir and the local dir.
BASE="${FRS_S3SIM_BASE:-${TMPDIR:-/tmp}/frs-s3sim}"
export S3_DIR="${S3_DIR:-$BASE/s3}"
export LOCAL_DIR="${LOCAL_DIR:-$BASE/local}"
mkdir -p "$S3_DIR" "$LOCAL_DIR"

echo "== local S3 simulation =="
echo "   REPO=$REPO"
echo "   S3_DIR    (remote, THROTTLED) = $S3_DIR"
echo "   LOCAL_DIR (local store)       = $LOCAL_DIR"
echo "   FRS_REMOTE_BW_MBPS            = $FRS_REMOTE_BW_MBPS MiB/s (50 Gb/s = 6250; 0 = OFF)"
echo "   MODE                         = $MODE"

case "$MODE" in
  smoke)
    # Engine integration smoke — proves correctness + an active throttle confined
    # to the remote leg. Runs the FRS_REMOTE_BW_MBPS arm internally.
    cd "$REPO"
    echo "-- running engine S3-sim throttle smoke (cargo test) --"
    cargo test -p forst-rs-engine --test remote_bw_throttle_it -- --nocapture
    echo "-- smoke OK: rows correct + throttle active on the remote leg --"
    ;;

  sweep)
    # Full NexMark uniform sweep with the S3-sim config + throttle. DEFERRED:
    # run only on a clean Mac or the remote box. Reuses the package's
    # run-8c32g.sh (which forwards FRS_REMOTE_BW_MBPS into the containers) via
    # run-remote-nexmark-v3.sh, but with the S3-sim config template. The S3-sim
    # template lives under tools/nexmark-local/configs; point TEMPLATES at it so
    # the harness picks up config-forst-rs-s3sim.yaml.tpl.
    echo "WARNING: the full @100M NexMark S3-sim sweep is a heavy wall. Run it on"
    echo "         a clean Mac or the remote box only (see docs/README.md §S3-sim)."
    RUNNER="$SCRIPT_DIR/run-remote-nexmark-v3.sh"
    [ -f "$RUNNER" ] || { echo "FATAL: missing $RUNNER"; exit 1; }
    # The forst-rs arm uses the S3-sim config and the package-local templates.
    ARMS="${ARMS:-forst-rs-s3sim}" \
      QUERIES="${QUERIES:-q7 q9}" \
      MAXSEC="${MAXSEC:-3600}" \
      TAG_PREFIX="${TAG_PREFIX:-s3sim}" \
      TEMPLATES="$REPO/tools/nexmark-local/configs" \
      S3_DIR="$S3_DIR" \
      LOCAL_DIR="$LOCAL_DIR" \
      REPO="$REPO" \
      bash "$RUNNER"
    ;;

  *)
    echo "unknown mode: $MODE (expected: smoke | sweep)"
    exit 1
    ;;
esac
