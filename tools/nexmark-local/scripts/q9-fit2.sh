#!/usr/bin/env bash
# q9-fit2.sh <tag> <maxsec>
# q9 @100M @2x4c/16g SPLIT, KV-sep ON, full q9 lever stack + FRS_MEM_DIAG=1,
# COMBINED memory-fit levers:
#   1. _RJEM_MALLOC_CONF aggressive decay (return freed engine pages ~1s) — kills
#      the freed-dirty retention component of the spike.
#   2. FRS_BG_COMPACT_THREADS=1 — one compaction at a time per TM, halving the
#      LIVE compaction merge working set (the ~5.6 GiB burst that drives the
#      cgroup toward 16g). Trades some L0-drain throughput for a lower memory
#      ceiling. Global-safe (uniform; engine decides).
# Goal: keep the end-of-run spike under 16g/TM so q9 finishes EXACT 91,813,372.
set -u
REPO=/tmp/frs-q9memfit
TAG="${1:-q9fit2}"
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
export _RJEM_MALLOC_CONF="background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0"
export FRS_BG_COMPACT_THREADS=1

CLUSTER="$TAG-q9-forst-rs-ffm-local" bash "$REPO/tools/nexmark-local/scripts/run-8c32g.sh" run q9 forst-rs-ffm-local "$MS" "$TAG"
