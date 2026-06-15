#!/usr/bin/env bash
# q9-procsize.sh <tag> <tm_process_size> <maxsec>
# q9 @100M @2x4c/16g SPLIT, KV-sep ON, full q9 lever stack + FRS_MEM_DIAG=1.
# MEMORY-FIT lever: LOWER Flink taskmanager.memory.process.size so the engine's
# native (off-heap, jemalloc) allocation — which Flink does NOT account for and
# which lives in the cgroup ON TOP of process.size — fits under the 16g/TM cgroup.
# Default Flink process.size=12288m + engine native ~5-6g = ~18g > 16g cgroup ->
# OOM. Carving process.size down to ~10240m leaves ~6g cgroup headroom for the
# engine native. Uniform (all forst-rs queries), no RAM added, global-safe.
set -u
# Portable repo root: explicit REPO wins, else the engine repo that contains this
# script (scripts -> tools/nexmark-local -> ../../.. = repo root). Works from a
# normal checkout or a worktree; overridable for the remote box.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
TAG="${1:-q9ps}"
PS="${2:-10240m}"
MS="${3:-2700}"

export EVENTS_NUM=100000000 TPS=10000000
export TOPO=split
export FRS_SST_COMPRESSION=lz4
export FRS_KV_SEPARATION=true
export FRS_KV_MIN_BLOB_SIZE=256
export FRS_TRIVIAL_MOVE=true
export FRS_RS_S2_PINNED=1
export FRS_S2_FANOUT_MIN=8
export FRS_VLOG_COALESCE_DEREF=1
export FRS_RS_PROBE_BLOOM_PRUNE=1
export FRS_RS_LEVELED_HOT_CF=1
export FRS_PERSISTENT_PROBE_ITER=1
export FRS_VLOG_READER_CACHE_CAP=2048
export FRS_VLOG_RESIDENT_BUDGET_MB=256
export FRS_KV_ADAPTIVE_PRESSURE=1
export FRS_MEM_DIAG=1
export FRS_MEM_DIAG_FILE=/tmp/q9-memdiag.log
export FRS_TM_PROCESS_SIZE="$PS"

CLUSTER="$TAG-q9-forst-rs-ffm-local" bash "$REPO/tools/nexmark-local/scripts/run-8c32g.sh" run q9 forst-rs-ffm-local "$MS" "$TAG"
