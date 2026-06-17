#!/usr/bin/env bash
# Linux local ForSt-RS NexMark sweep — the SINGLE UNIFORM config for EVERY query.
#
# ★★ UNIFORM CONFIG (2026-06-17, PMC-1 — user directive). PER-QUERY CONFIG IS
# FORBIDDEN. This bare-metal (non-docker) runner reads the ONE `*` row from
# configs/best-config-linux-local.tsv and applies the SAME forst-rs knobs to every
# query (the same apply_uniform pattern as the docker harness's run-best.sh). There
# are NO per-query branches anywhere in this file. The engine adapts to the query
# SHAPE at runtime under the one config (point-deref auto-follows KV-sep for
# windowed RMW; coalesce for scans). See configs/best-config-linux-local.tsv for
# the full provenance and the EXCLUDED-lever rationale.
#
# This script is intentionally Linux/local specific (its distinct mechanism is the
# bare-metal /ssd2 origin-box entrypoint). Keep the portable Mac/Linux runners
# untouched, and use this entrypoint for the origin-box runs. Only the PER-QUERY
# variation has been removed — the runner itself stays.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO="${REPO:-$(cd "$PKG_DIR/../.." && pwd)}"
RUNNER="${RUNNER:-$PKG_DIR/scripts/run-8c32g.sh}"
TSV="${BEST_CONFIG_TSV:-$PKG_DIR/configs/best-config-linux-local.tsv}"
LEGACY_TSV="$PKG_DIR/configs/best-config.tsv"

WORKENV="${WORKENV:-/home/users/lijunqing/workenv}"
FLINK="${FLINK:-$WORKENV/flink-2.2.1}"
# Docker 19.03 on the Linux box has experimental platform selection disabled.
# Let the daemon choose the native amd64 image unless the caller explicitly sets
# PLAT to a supported value.
PLAT="${PLAT:-auto}"
TEMPLATES="${TEMPLATES:-$PKG_DIR/configs}"
NEXMARK_HOME="${NEXMARK_HOME:-$REPO/nexmark/nexmark-flink/target/nexmark-flink-bin/nexmark-flink}"
MAXSEC="${MAXSEC:-3600}"
QUERIES="${QUERIES:-q0 q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 q14 q15 q16 q17 q18 q19 q20 q21 q22}"
STAMP="${STAMP:-$(date +%Y%m%d-%H%M%S)}"

sanitize_name() {
  printf '%s' "$1" \
    | tr '[:upper:]' '[:lower:]' \
    | sed 's/[^a-z0-9_.-]/-/g; s/^-*//; s/-*$//'
}

SCENARIO_PREFIX="$(sanitize_name "${SCENARIO_PREFIX:-nexmark-local}")"
RUN_ID="$(sanitize_name "${RUN_ID:-$SCENARIO_PREFIX-forstrs-$STAMP}")"
case "$RUN_ID" in
  "$SCENARIO_PREFIX"-*) ;;
  *) RUN_ID="$SCENARIO_PREFIX-$RUN_ID" ;;
esac
OUTROOT="${OUTROOT:-/ssd2/jackylee/frs-bench/logs/$RUN_ID}"
CTMP_BASE="${FRS_CTMP_BASE:-/ssd2/jackylee/frs-bench/tmp-$RUN_ID}"
CSV="$OUTROOT/summary.csv"
MD="$OUTROOT/SUMMARY.md"
ENV_DIR="$OUTROOT/env"
IMG="${IMG:-$SCENARIO_PREFIX-forst-bench:x86}"

PASS_ENV_KEYS=(
  EVENTS_NUM TPS
  SINGLE_TM_CPUS SINGLE_TM_MEM
  SPLIT_TM_CPUS SPLIT_TM_MEM SPLIT_JM_CPUS SPLIT_JM_MEM
  FRS_TM_PROCESS_SIZE FRS_JM_PROCESS_SIZE
  FRS_FLINK_PARALLELISM FRS_TM_SLOTS
  FRS_NET_SUBNET FRS_NET_SUBNET_BASE
  FRS_TM_JEMALLOC
  FRS_BLOCK_SIZE_KB FRS_SST_COMPRESSION FRS_VLOG_COMPRESSION
  FRS_BLOCK_CACHE_MB FRS_SST_KV_BLOCK_FORMAT FRS_SST_SKIP_READ_CHECKSUM
  FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE
  FRS_REMOTE_COMPACTION FRS_RS_S2_PINNED
  FRS_RS_PROBE_BLOOM_PRUNE
  FRS_RS_LEVELED_HOT_CF FRS_RS_LEVELED_HOT_CF_FANOUT_MIN
  FRS_RS_LEVELED_HOT_CF_L0_TRIGGER
  FRS_DYNAMIC_LEVELS FRS_FADVISE
  FRS_BG_COMPACT_THREADS FRS_BG_FLUSH_THREADS
  FRS_L0_STOP_TRIGGER FRS_L0_COMPACTION_TRIGGER
  FRS_L0_SLOWDOWN_TRIGGER
  FRS_DECAY_DIAG FRS_BULK_SAMPLE FRS_ITER_DIAG
  FRS_READ_AT_DIAG FRS_REENTRY_DIAG
  FRS_DISABLE_PREFIX_BLOOM FRS_GARBAGE_DRAIN_TOMBSTONES
  FRS_CURSOR_DIAG FRS_SCAN_STATS
  FRS_RESIDENT_BYPASS FRS_RESIDENT_SHADOW_TOTAL_MB
  FRS_RS_PARALLEL_EXECUTOR FRS_RS_READ_IO_PARALLELISM
  FRS_RS_EXECUTOR FRS_RS_MAX_INFLIGHT_BATCHES
  FRS_RS_PARALLEL_ITER FRS_ITER_DISPATCH_DIAG
  FRS_RSS_SAMPLE FRS_JFR FRS_PERF FRS_PERF_DELAY FRS_PERF_DUR
  MALLOC_CONF _RJEM_MALLOC_CONF
  FRS_MEM_DIAG FRS_MEM_DIAG_FILE
  FRS_DISABLE_MAPSTATE_CACHE
  FRS_WBM_TOTAL_MB FRS_WBM_STALL FRS_WBM_HARD_MB
  FRS_SCAN_OPEN_FANOUT FRS_SCAN_COLD_PRIME FRS_S2_FANOUT_MIN
  FRS_VLOG_READER_CACHE_CAP FRS_VLOG_RESIDENT_BUDGET_MB
  FRS_VLOG_COALESCE_DEREF
  FRS_KV_ADAPTIVE_PRESSURE
  FRS_VLOG_GC_ADAPTIVE FRS_VLOG_GC_ADAPTIVE_CUTOFF
  FRS_VLOG_GC_AGE_CUTOFF
  FRS_COMPACT_WINDOWED FRS_COMPACT_WINDOW_BYTES
  FRS_COMPACT_PREFETCH_BUDGET FRS_COMPACT_PARALLEL
  FRS_CACHE_ADMISSION FRS_CACHE_BG_EXEMPT
  FRS_CACHE_ADMISSION_EPOCH FRS_CACHE_SPACE_LIMIT_MB
  FRS_IO_URING FRS_RS_INLINE_MAX FRS_RS_SYNC_DIRECT
  FRS_RS_MIXED_BATCH FRS_TIMER_INDEX_MAX
  FRS_RS_BLOCK_PREFETCH FRS_RS_PREFETCH_THREADS
  FRS_PERSISTENT_PROBE_ITER
  FRS_RS_MERGE_RMW FRS_RS_MERGE_RMW_STATES FRS_RS_MERGE_CHAIN_REBASE
  FRS_LIFECYCLE_COHORT_TRIGGER FRS_LIFECYCLE_DROP_IGNORE_SNAPSHOTS
  FRS_LIFECYCLE_SEGMENTS FRS_LIFECYCLE_STAMPED_CEILING
  FRS_CKPT_LINK_MODE FRS_CKPT_PARALLEL_UPLOAD
  FRS_COMPACT_CONCURRENT FRS_COMPACT_DIAG FRS_COMPACT_DRAIN_L1
  FRS_COMPACT_RELEASE_LOCK
  FRS_REMOTE_COMPACTION_SERIALIZE FRS_REMOTE_NONSST_LOCAL
  FRS_RESIDENT_BLOOM_SKIP FRS_RESIDENT_SHADOW FRS_RESIDENT_SHADOW_MB
  FRS_RESTORE_BG_FILL FRS_RESTORE_BG_FILL_PACE_MB FRS_RESTORE_BG_FILL_WORKERS
  S3_DIR LOCAL_DIR FRS_REMOTE_BW_MBPS
)

EXTRA_ENV_KEYS=(
  FRS_CACHE_ADMISSION_PROMOTE FRS_CACHE_ADMISSION_EVICT_LIMIT
  FRS_CACHE_ADMISSION_TRACKER_CAP
  FRS_UPLOAD_RATE_SPLIT FRS_UPLOAD_COMPACTION_SHARE
  FRS_MEMTABLE_SHARDS
)

mkdir -p "$OUTROOT" "$ENV_DIR" "$CTMP_BASE"

export PATH=/home/work/dockerd/bin:$PATH

validate_local_forstrs_config() {
  [ -x "$RUNNER" ] || { echo "FATAL: missing runner $RUNNER"; exit 1; }
  if [ ! -f "$TSV" ]; then
    if [ "$TSV" != "$LEGACY_TSV" ] && [ -f "$LEGACY_TSV" ]; then
      echo "WARN: missing Linux-local best config $TSV; falling back to legacy $LEGACY_TSV" >&2
      TSV="$LEGACY_TSV"
    else
      echo "FATAL: missing best config $TSV"
      exit 1
    fi
  fi
  [ -d "$FLINK" ] || { echo "FATAL: missing FLINK=$FLINK"; exit 1; }
  [ -d "$NEXMARK_HOME/queries" ] || { echo "FATAL: missing NEXMARK_HOME queries at $NEXMARK_HOME"; exit 1; }
  if ! docker image inspect "$IMG" >/dev/null 2>&1; then
    if [ "$IMG" = "$SCENARIO_PREFIX-forst-bench:x86" ] \
       && docker image inspect forst-bench:x86 >/dev/null 2>&1; then
      docker tag forst-bench:x86 "$IMG"
    else
      echo "FATAL: missing Docker image IMG=$IMG"
      exit 1
    fi
  fi

  local tpl="$TEMPLATES/config-forst-rs-local.yaml.tpl"
  [ -f "$tpl" ] || { echo "FATAL: missing template $tpl"; exit 1; }
  grep -q 'org.apache.flink.state.forstrs.ForStRsStateBackendFactory' "$tpl" \
    || { echo "FATAL: $tpl is not ForSt-RS backend"; exit 1; }
  grep -q 'forst.rs.timer-service.factory=FORSTRS' "$tpl" \
    || { echo "FATAL: $tpl does not force FORSTRS timer"; exit 1; }
  if grep -q 'forst.rs.timer-service.factory=HEAP' "$tpl"; then
    echo "FATAL: $tpl contains HEAP timer; ForSt-RS local tests must use FORSTRS"
    exit 1
  fi
}

validate_local_forstrs_config

if [ ! -f "$CSV" ]; then
  printf 'query,status,wall_s,wall_ms,src_out,out_rows,topology,cluster,config_profile,log,started_at,ended_at\n' > "$CSV"
fi

export TOPO_DEFAULT=split

# Read the single uniform `*` row from the TSV; returns the tab-split fields.
# (No per-query lookup — there is exactly ONE config row applied to every query.)
lookup_uniform_row() {
  awk -F'\t' '
    /^[[:space:]]*#/ { next }
    /^[[:space:]]*$/ { next }
    $1 == "query" { next }
    $1 == "*"     { print; found=1; exit }
    END { if (!found) exit 3 }
  ' "$TSV"
}

clear_forstrs_knobs() {
  unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE \
        FRS_RS_S2_PINNED FRS_S2_FANOUT_MIN FRS_RS_EXECUTOR FRS_VLOG_COALESCE_DEREF \
        FRS_RS_PROBE_BLOOM_PRUNE FRS_RS_LEVELED_HOT_CF \
        FRS_RS_LEVELED_HOT_CF_FANOUT_MIN FRS_RS_LEVELED_HOT_CF_L0_TRIGGER \
        FRS_REMOTE_COMPACTION FRS_RS_PARALLEL_EXECUTOR \
        FRS_RS_MAX_INFLIGHT_BATCHES FRS_RS_PARALLEL_ITER \
        FRS_RS_INLINE_MAX FRS_RS_SYNC_DIRECT \
        FRS_RS_MIXED_BATCH FRS_TIMER_INDEX_MAX FRS_DISABLE_MAPSTATE_CACHE \
        FRS_RS_BLOCK_PREFETCH FRS_RS_PREFETCH_THREADS \
        FRS_PERSISTENT_PROBE_ITER \
        FRS_RS_MERGE_RMW FRS_RS_MERGE_RMW_STATES FRS_RS_MERGE_CHAIN_REBASE \
        FRS_SCAN_OPEN_FANOUT FRS_SCAN_COLD_PRIME FRS_RS_READ_IO_PARALLELISM \
        FRS_VLOG_READER_CACHE_CAP FRS_VLOG_RESIDENT_BUDGET_MB \
        FRS_KV_ADAPTIVE_PRESSURE FRS_VLOG_GC_ADAPTIVE FRS_VLOG_GC_ADAPTIVE_CUTOFF \
        FRS_VLOG_GC_AGE_CUTOFF FRS_CACHE_ADMISSION FRS_CACHE_BG_EXEMPT \
        FRS_CACHE_ADMISSION_EPOCH FRS_CACHE_SPACE_LIMIT_MB \
        FRS_COMPACT_WINDOWED FRS_COMPACT_WINDOW_BYTES FRS_COMPACT_PREFETCH_BUDGET \
        FRS_COMPACT_PARALLEL FRS_COMPACT_CONCURRENT FRS_COMPACT_DRAIN_L1 \
        FRS_COMPACT_RELEASE_LOCK FRS_CKPT_PARALLEL_UPLOAD \
        SINGLE_TM_CPUS SINGLE_TM_MEM SPLIT_TM_CPUS SPLIT_TM_MEM \
        FRS_FLINK_PARALLELISM FRS_TM_SLOTS \
        FRS_TM_PROCESS_SIZE FRS_JM_PROCESS_SIZE PROFILE_TOPOLOGY PROFILE_MAXSEC 2>/dev/null || true
  for k in "${EXTRA_ENV_KEYS[@]}"; do
    unset "$k" 2>/dev/null || true
  done
}

set_if_value() {
  local k="$1" v="$2"
  [ "$v" != "-" ] && export "$k=$v" || true
}

# Apply the SINGLE UNIFORM config to EVERY query. This reads the one `*` row from
# the TSV and exports the SAME forst-rs knobs regardless of which query is about to
# run — there are NO per-query branches. Mirrors run-best.sh's apply_uniform.
apply_profile() {
  local q="$1" row

  clear_forstrs_knobs
  PROFILE=
  PROFILE_TOPOLOGY=
  PROFILE_MAXSEC=

  # Conservative bare-metal-local defaults, UNIFORM for every query.
  export FRS_TM_JEMALLOC="${FRS_TM_JEMALLOC:-1}"
  export FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}"
  export FRS_SCAN_OPEN_FANOUT="${FRS_SCAN_OPEN_FANOUT:-1}"
  export FRS_SCAN_COLD_PRIME="${FRS_SCAN_COLD_PRIME:-1}"
  export FRS_RS_READ_IO_PARALLELISM="${FRS_RS_READ_IO_PARALLELISM:-3}"
  export FRS_IO_URING="${FRS_IO_URING:-1}"

  # Bare-metal Linux-local topology invariant (UNIFORM, every query): the 8c/32g
  # TM budget realized as 2 TaskManagers x 4c/16g, parallelism 8, 4 slots/TM. This
  # is a runner topology choice applied identically to all queries, NOT a per-query
  # config knob (see best-config-linux-local.tsv).
  export FRS_FLINK_PARALLELISM=8
  export FRS_TM_SLOTS=4
  export SPLIT_TM_CPUS=4
  export SPLIT_TM_MEM=16g

  # --- apply the uniform `*` row (identical for every query) ---
  row="$(lookup_uniform_row 2>/dev/null)" \
    || { echo "FATAL: no uniform '*' row in $TSV"; exit 1; }
  local urow ukvsep ukvmin utrivial us2pin ucoalesce ucomp umemmgr unote
  IFS=$'\t' read -r urow ukvsep ukvmin utrivial us2pin ucoalesce ucomp umemmgr unote <<<"$row"
  set_if_value FRS_KV_SEPARATION       "$ukvsep"
  set_if_value FRS_KV_MIN_BLOB_SIZE    "$ukvmin"
  set_if_value FRS_TRIVIAL_MOVE        "$utrivial"
  set_if_value FRS_RS_S2_PINNED        "$us2pin"
  set_if_value FRS_VLOG_COALESCE_DEREF "$ucoalesce"
  export FRS_SST_COMPRESSION="${ucomp:-lz4}"
  # FRS_VLOG_POINT_DEREF deliberately LEFT UNSET -> auto-follows KV-sep (db.rs:467).
  set_if_value FRS_MEM_MANAGER         "$umemmgr"
  # Vlog resident bounds: keep the engine native bounded. Uniform for every query.
  export FRS_VLOG_READER_CACHE_CAP="${FRS_VLOG_READER_CACHE_CAP:-2048}"
  export FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-256}"
  export FRS_KV_ADAPTIVE_PRESSURE="${FRS_KV_ADAPTIVE_PRESSURE:-1}"

  # Final guard: results are only comparable when every query uses the same
  # measured resource envelope. Re-pin after all defaults.
  export FRS_FLINK_PARALLELISM=8
  export FRS_TM_SLOTS=4
  export SPLIT_TM_CPUS=4
  export SPLIT_TM_MEM=16g

  PROFILE="uniform-split:KVSEP-ON:p8-2x4c16g"
}

topology_for() {
  local q="$1" var
  if [ -n "${PROFILE_TOPOLOGY:-}" ] && [ "$PROFILE_TOPOLOGY" != "-" ] && [ "$PROFILE_TOPOLOGY" != "split" ]; then
    echo "WARN: ignoring topology=$PROFILE_TOPOLOGY for $q; NexMark local must use split 2x4c16g TM resources" >&2
  fi
  var="TOPO_${q^^}"
  if [ -n "${!var:-}" ]; then
    if [ "${!var}" != "split" ]; then
      echo "WARN: ignoring $var=${!var}; NexMark local must use split 2x4c16g TM resources" >&2
    fi
    printf '%s\n' split
    return
  fi
  printf '%s\n' split
}

maxsec_for() {
  if [ -n "${PROFILE_MAXSEC:-}" ] && [ "$PROFILE_MAXSEC" != "-" ]; then
    printf '%s\n' "$PROFILE_MAXSEC"
    return
  fi
  printf '%s\n' "$MAXSEC"
}

source_min_for() {
  case "$1" in
    q4|q9) printf '%s\n' 96000000 ;;
    q7|q11|q12|q17|q18|q19|q20) printf '%s\n' 90000000 ;;
    q8) printf '%s\n' 2500000 ;;
    q5) printf '%s\n' 5000000 ;;
    q3) printf '%s\n' 2000000 ;;
    *) printf '%s\n' 0 ;;
  esac
}

metric_regressed_in_log() {
  local log="$1"
  awk '
    {
      line = $0
      while (match(line, /src_out=[0-9]+/)) {
        v = substr(line, RSTART + 8, RLENGTH - 8) + 0
        if (seen && prev >= 10000000 && v + 1000000 < prev && v < prev * 0.75) {
          bad = 1
        }
        prev = v
        seen = 1
        line = substr(line, RSTART + RLENGTH)
      }
    }
    END { exit bad ? 0 : 1 }
  ' "$log" 2>/dev/null
}

valid_finished_log() {
  local q="$1" log="$2" result src_out min_src
  result="$(grep -E '^RESULT:' "$log" 2>/dev/null | tail -1 || true)"
  printf '%s\n' "$result" | grep -q 'FINISHED' || return 1
  metric_regressed_in_log "$log" && return 1
  min_src="$(source_min_for "$q")"
  [ "$min_src" -gt 0 ] || return 0
  src_out="$(printf '%s\n' "$result" | sed -n 's/.*src_out=\([^ ]*\).*/\1/p')"
  case "$src_out" in
    ''|*[!0-9]*) return 1 ;;
    *) [ "$src_out" -ge "$min_src" ] ;;
  esac
}

valid_resource_env() {
  local q="$1" file="$ENV_DIR/$q.env"
  [ -f "$file" ] || return 1
  grep -qx 'TOPOLOGY=split' "$file" || return 1
  grep -qx 'SPLIT_TM_CPUS=4' "$file" || return 1
  grep -qx 'SPLIT_TM_MEM=16g' "$file" || return 1
  grep -qx 'FRS_FLINK_PARALLELISM=8' "$file" || return 1
  grep -qx 'FRS_TM_SLOTS=4' "$file" || return 1
}

write_env_file() {
  local q="$1" profile="$2" topology="$3" file="$ENV_DIR/$q.env"
  {
    echo "RUN_ID=$RUN_ID"
    echo "SCENARIO_PREFIX=$SCENARIO_PREFIX"
    echo "QUERY=$q"
    echo "PROFILE=$profile"
    echo "REPO=$REPO"
    echo "WORKENV=$WORKENV"
    echo "FLINK=$FLINK"
    echo "IMG=$IMG"
    echo "PLAT=$PLAT"
    echo "TEMPLATES=$TEMPLATES"
    echo "MAXSEC=$MAXSEC"
    echo "PROFILE_MAXSEC=${PROFILE_MAXSEC:-}"
    echo "CONFIG_TSV=$TSV"
    echo "TOPOLOGY=$topology"
    echo "CTMP_BASE=$CTMP_BASE"
    echo "ForSt_HEAD=$(git -C "$REPO" rev-parse HEAD 2>/dev/null || true)"
    echo "Flink_HEAD=$(git -C "$REPO/../flink" rev-parse HEAD 2>/dev/null || true)"
    for k in "${PASS_ENV_KEYS[@]}"; do
      printf '%s=%s\n' "$k" "${!k:-}"
    done
    for k in "${EXTRA_ENV_KEYS[@]}"; do
      printf '%s=%s\n' "$k" "${!k:-}"
    done
  } > "$file"
}

write_md() {
  {
    echo "# ForSt-RS best local NexMark full sweep"
    echo
    echo "- run_id: $RUN_ID"
    echo "- scenario_prefix: $SCENARIO_PREFIX"
    echo "- repo: $REPO"
    echo "- forst_head: $(git -C "$REPO" rev-parse HEAD 2>/dev/null || true)"
    echo "- flink_head: $(git -C "$REPO/../flink" rev-parse HEAD 2>/dev/null || true)"
    echo "- backend: forst-rs-ffm-local"
    echo "- maxsec_per_query: $MAXSEC"
    echo "- config_tsv: $TSV"
    echo "- topology: fixed split, 2 TaskManagers x 4c/16g; 4 slots/TM; parallelism 8"
    echo "- templates: $TEMPLATES"
    echo "- scratch: $CTMP_BASE"
    echo "- env_dir: $ENV_DIR"
    echo
    echo "## Totals"
    awk -F, '
      NR > 1 && $2 == "FINISHED" {
        all += $3; n_all++;
        if ($1 != "q4") { no_q4 += $3; n_no_q4++; }
        if ($1 != "q6") { no_q6 += $3; n_no_q6++; }
        if ($1 != "q4" && $1 != "q6") { no_q4_q6 += $3; n_no_q4_q6++; }
      }
      END {
        printf "- finished_queries: %d\n", n_all + 0;
        printf "- total_finished_s: %.1f\n", all + 0;
        printf "- total_excluding_q4_s: %.1f\n", no_q4 + 0;
        printf "- total_excluding_q6_s: %.1f\n", no_q6 + 0;
        printf "- total_excluding_q4_q6_s: %.1f\n", no_q4_q6 + 0;
      }' "$CSV"
    echo
    echo "## By Query"
    echo
    echo "| query | status | wall_s | out_rows | topology | profile | log |"
    echo "|---|---:|---:|---:|---|---|---|"
    awk -F, 'NR > 1 {
      printf "| %s | %s | %s | %s | %s | %s | %s |\n", $1, $2, $3, $6, $7, $9, $10
    }' "$CSV"
  } > "$MD"
}

parse_and_record() {
  local q="$1" topology="$2" cluster="$3" profile="$4" log="$5" started="$6" ended="$7"
  local result status wall_s wall_ms src_out out_rows tmp min_src
  result="$(grep -E '^RESULT:' "$log" 2>/dev/null | tail -1 || true)"
  status="NO_RESULT"; wall_s=""; wall_ms=""; src_out=""; out_rows=""

  if printf '%s\n' "$result" | grep -q 'FINISHED'; then
    status="FINISHED"
    wall_ms="$(printf '%s\n' "$result" | sed -n 's/.*wall_ms=\([0-9][0-9]*\).*/\1/p')"
    wall_s="$(printf '%s\n' "$result" | sed -n 's/.*(=\([0-9.][0-9.]*\)s).*/\1/p')"
    src_out="$(printf '%s\n' "$result" | sed -n 's/.*src_out=\([^ ]*\).*/\1/p')"
    out_rows="$(printf '%s\n' "$result" | sed -n 's/.*out_rows=\([^ ]*\).*/\1/p')"
    min_src="$(source_min_for "$q")"
    if metric_regressed_in_log "$log"; then
      status="SUSPECT_FINISHED"
      echo "WARN: $q finished but src_out regressed during polling; preserving as suspect: $log" >&2
    elif [ "$min_src" -gt 0 ]; then
      case "$src_out" in
        ''|*[!0-9]*)
          status="SUSPECT_FINISHED"
          echo "WARN: $q finished but src_out is not numeric: $src_out" >&2
          ;;
        *)
          if [ "$src_out" -lt "$min_src" ]; then
            status="SUSPECT_FINISHED"
            echo "WARN: $q finished with src_out=$src_out below expected minimum $min_src" >&2
          fi
          ;;
      esac
    fi
  elif printf '%s\n' "$result" | grep -q 'MAXSEC'; then
    status="MAXSEC"
  elif printf '%s\n' "$result" | grep -q 'state='; then
    status="$(printf '%s\n' "$result" | sed -n 's/.*state=\([^ ]*\).*/\1/p')"
    wall_ms="$(printf '%s\n' "$result" | sed -n 's/.*dur_ms=\([0-9][0-9]*\).*/\1/p')"
    [ -n "$wall_ms" ] && wall_s="$(awk "BEGIN{printf \"%.1f\", $wall_ms/1000}")"
    src_out="$(printf '%s\n' "$result" | sed -n 's/.*src_out=\([^ ]*\).*/\1/p')"
  elif grep -q 'FAILED to submit' "$log" 2>/dev/null; then
    status="SUBMIT_FAILED"
  elif grep -q 'FATAL:' "$log" 2>/dev/null; then
    status="FATAL"
  fi

  tmp="$CSV.tmp"
  awk -F, -v q="$q" 'BEGIN{OFS=","} NR == 1 || $1 != q {print}' "$CSV" > "$tmp"
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$q" "$status" "$wall_s" "$wall_ms" "$src_out" "$out_rows" "$topology" "$cluster" "$profile" "$log" "$started" "$ended" >> "$tmp"
  mv "$tmp" "$CSV"
  write_md
  printf '[%s] %s status=%s wall_s=%s profile=%s\n' "$(date '+%F %T')" "$q" "$status" "$wall_s" "$profile"
}

cleanup_cluster() {
  local cluster="$1"
  case "$cluster" in
    "$SCENARIO_PREFIX"-*) ;;
    *)
      echo "FATAL: refusing to cleanup non-$SCENARIO_PREFIX cluster: $cluster"
      exit 1
      ;;
  esac
  docker rm -f "$cluster-jm" "$cluster-tm1" "$cluster-tm2" >/dev/null 2>&1 || true
  docker network rm "$cluster-net" >/dev/null 2>&1 || true
  case "$CTMP_BASE" in
    /ssd2/jackylee/frs-bench/tmp-"$SCENARIO_PREFIX"-*|/ssd1/jackylee/frs-bench/tmp-"$SCENARIO_PREFIX"-*|/tmp/"$SCENARIO_PREFIX"-*)
      rm -rf "$CTMP_BASE/$cluster" >/dev/null 2>&1 || true
      if [ -e "$CTMP_BASE/$cluster" ]; then
        docker run --rm --user 0:0 -v "$CTMP_BASE:$CTMP_BASE" "$IMG" \
          sh -c 'rm -rf "$1"' sh "$CTMP_BASE/$cluster" >/dev/null 2>&1 || true
      fi
      if [ -e "$CTMP_BASE/$cluster" ]; then
        echo "WARN: scratch cleanup incomplete for $CTMP_BASE/$cluster"
      fi
      ;;
    *)
      echo "WARN: skip scratch cleanup outside namespaced bench tmp: CTMP_BASE=$CTMP_BASE"
      ;;
  esac
}

wait_for_quiet_box() {
  [ "${WAIT_FOR_QUIET:-0}" = "1" ] || return 0
  while :; do
    local load containers
    read -r load _ < /proc/loadavg
    containers="$(docker ps --format '{{.Names}}' 2>/dev/null | grep -Ec "^${SCENARIO_PREFIX}-" || true)"
    if awk -v l="$load" 'BEGIN{exit !(l < 20.0)}' && [ "${containers:-0}" -eq 0 ]; then
      return 0
    fi
    echo "[gate $(date '+%F %T')] load=$load active_${SCENARIO_PREFIX}_containers=$containers; waiting"
    sleep 60
  done
}

write_md

for q in $QUERIES; do
  log="$OUTROOT/run-$q.log"
  apply_profile "$q"
  profile="$PROFILE"
  topology="$(topology_for "$q")"
  query_maxsec="$(maxsec_for)"
  cluster="$(sanitize_name "$RUN_ID-$q")"
  case "$cluster" in
    "$SCENARIO_PREFIX"-*) ;;
    *) cluster="$SCENARIO_PREFIX-$cluster" ;;
  esac
  tag="$cluster"

  if grep -q "RESULT: $q FINISHED" "$log" 2>/dev/null \
    && valid_finished_log "$q" "$log" \
    && valid_resource_env "$q"; then
    echo "record $q: existing FINISHED result in $log"
    write_env_file "$q" "$profile" "$topology"
    ended="$(date '+%F %T')"
    parse_and_record "$q" "$topology" "$cluster" "$profile" "$log" "existing" "$ended"
    cleanup_cluster "$cluster"
    continue
  elif grep -q "RESULT: $q FINISHED" "$log" 2>/dev/null; then
    echo "rerun $q: existing FINISHED result lacks valid result/resource evidence; preserving $log"
    cp -p "$log" "$log.suspect-$(date +%Y%m%d-%H%M%S)" 2>/dev/null || true
  fi

  write_env_file "$q" "$profile" "$topology"
  started="$(date '+%F %T')"

  echo "=== [$started] running $q forst-rs best profile=$profile topology=$topology maxsec=$query_maxsec cluster=$cluster ==="
  cleanup_cluster "$cluster"
  wait_for_quiet_box

  set +e
  pass_env=(
    REPO="$REPO" WORKENV="$WORKENV" FLINK="$FLINK" IMG="$IMG" PLAT="$PLAT" TEMPLATES="$TEMPLATES"
    TOPO="$topology" FRS_CTMP_BASE="$CTMP_BASE" CLUSTER="$cluster"
    NEXMARK_NAMESPACE="$SCENARIO_PREFIX"
    NEXMARK_HOME="$NEXMARK_HOME"
  )
  for k in "${PASS_ENV_KEYS[@]}"; do
    pass_env+=("$k=${!k:-}")
  done
  for k in "${EXTRA_ENV_KEYS[@]}"; do
    pass_env+=("$k=${!k:-}")
  done
  env "${pass_env[@]}" bash "$RUNNER" run "$q" forst-rs-ffm-local "$query_maxsec" "$tag" > "$log" 2>&1
  rc=$?
  set -e

  ended="$(date '+%F %T')"
  parse_and_record "$q" "$topology" "$cluster" "$profile" "$log" "$started" "$ended"
  cleanup_cluster "$cluster"
  echo "rc=$rc log=$log"
done

write_md
echo "summary: $MD"
