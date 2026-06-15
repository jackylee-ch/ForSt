#!/usr/bin/env bash
# q9-profile.sh <tag> <maxsec>
# Profiling q9 @100M at the 2x4c/16g SPLIT with KV-sep ON + full q9 lever stack +
# FRS_MEM_DIAG=1. Writes the mem-diag to the per-cluster /tmp (host CTMP) so the
# spike can be attributed (rss / jemalloc alloc-resident-retained / wbm / shadow /
# flush+compaction bytes-ns). Poll the docker logs for [FRS_MEM_DIAG] lines.
set -u
REPO=/tmp/frs-q9memfit
TAG="${1:-q9prof}"
MS="${2:-2700}"

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

CLUSTER="$TAG-q9-forst-rs-ffm-local" bash "$REPO/tools/nexmark-local/scripts/run-8c32g.sh" run q9 forst-rs-ffm-local "$MS" "$TAG"
