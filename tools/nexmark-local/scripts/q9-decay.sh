#!/usr/bin/env bash
# q9-decay.sh <tag> <decay_conf> <maxsec>
# q9 @100M @2x4c/16g SPLIT, KV-sep ON, full q9 lever stack + FRS_MEM_DIAG=1,
# with an OVERRIDDEN jemalloc decay via _RJEM_MALLOC_CONF (runtime, no rebuild).
# Tests whether returning freed engine pages to the OS faster shaves the
# end-of-run spike enough to fit the 16g/TM cgroup. Global-safe (allocator decay,
# uniform across all queries).
set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
TAG="${1:-q9decay}"
DECAY="${2:-background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0}"
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
export _RJEM_MALLOC_CONF="$DECAY"

CLUSTER="$TAG-q9-forst-rs-ffm-local" bash "$REPO/tools/nexmark-local/scripts/run-8c32g.sh" run q9 forst-rs-ffm-local "$MS" "$TAG"
