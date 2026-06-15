#!/usr/bin/env bash
# pmc1-uniform-sweep.sh — PMC-1 uniform-split q0-q22 sweep (2026-06-15).
#
# Drives run-8c32g.sh directly under the UNIFORM 2x4c/16g SPLIT topology with
# the established "validate" full-stack-ON forst-rs lever set per query. The ONE
# deliberate difference from run-best.sh `validate`: q9 runs in the SAME uniform
# split as every other query (NOT the 8c/36g single-TM special case), because
# commit efdc5997a (process.size 12288m->10240m) makes q9 KV-sep ON fit the 16g
# split cgroup. This is the uniform-config research arm.
#
# Records each RESULT incrementally to $RESULTS_TSV.
#
# USAGE:
#   QUERIES="q9 q7 ..." ARMS="forst-rs-ffm-local rocksdb forst-local" \
#     bash pmc1-uniform-sweep.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="${REPO:-/tmp/frs-dync}"
RUNNER="$REPO/tools/nexmark-local/scripts/run-8c32g.sh"
RESULTS_TSV="${RESULTS_TSV:-$REPO/tools/nexmark-local/dynshed-results.tsv}"

# FRS-DYN-SHED master arming (forst-rs arm only). Forwarded by the edited
# run-8c32g.sh ENVS array. Default ON here (this whole sweep validates it);
# the q7 ample-memory A/B flips it to 0 explicitly.
export FRS_DYNAMIC_SHED="${FRS_DYNAMIC_SHED:-1}"
export FRS_MEM_DIAG="${FRS_MEM_DIAG:-1}"

export EVENTS_NUM="${EVENTS_NUM:-100000000}"
export TPS="${TPS:-10000000}"
MAXSEC="${MAXSEC:-2700}"
TAG_PREFIX="${TAG_PREFIX:-pmc1u}"

QUERIES="${QUERIES:-q9}"
ARMS="${ARMS:-forst-rs-ffm-local rocksdb forst-local}"

[ -f "$RESULTS_TSV" ] || printf 'query\tarm\tstate\twall_ms\twall_s\tsrc_out\tout_rows\ttag\tts\n' > "$RESULTS_TSV"

# Apply the per-query validate full-stack-ON forst-rs levers (mirrors
# run-best.sh apply_validate; forst-rs arm only — baselines ignore FRS_*).
apply_validate() {
  local q="$1"
  unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE \
        FRS_RS_S2_PINNED FRS_S2_FANOUT_MIN FRS_RS_EXECUTOR FRS_VLOG_COALESCE_DEREF \
        FRS_RS_PROBE_BLOOM_PRUNE FRS_RS_LEVELED_HOT_CF FRS_PERSISTENT_PROBE_ITER \
        FRS_RS_MERGE_RMW FRS_RS_MERGE_RMW_STATES 2>/dev/null || true
  export FRS_SST_COMPRESSION="${FRS_SST_COMPRESSION:-lz4}"
  export FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}"
  join_stack() {
    export FRS_KV_SEPARATION=true
    export FRS_KV_MIN_BLOB_SIZE="${FRS_KV_MIN_BLOB_SIZE:-256}"
    export FRS_TRIVIAL_MOVE="${FRS_TRIVIAL_MOVE:-true}"
    export FRS_RS_S2_PINNED="${FRS_RS_S2_PINNED:-1}"
    export FRS_S2_FANOUT_MIN="${FRS_S2_FANOUT_MIN:-8}"
    export FRS_VLOG_COALESCE_DEREF="${FRS_VLOG_COALESCE_DEREF:-1}"
    export FRS_RS_PROBE_BLOOM_PRUNE="${FRS_RS_PROBE_BLOOM_PRUNE:-1}"
    export FRS_RS_LEVELED_HOT_CF="${FRS_RS_LEVELED_HOT_CF:-1}"
    export FRS_PERSISTENT_PROBE_ITER="${FRS_PERSISTENT_PROBE_ITER:-1}"
  }
  window_stack() {
    export FRS_RS_MERGE_RMW="${FRS_RS_MERGE_RMW:-1}"
    export FRS_RS_EXECUTOR="${FRS_RS_EXECUTOR:-routing-adaptive}"
  }
  case "$q" in
    q4|q5|q7|q9|q19|q20) join_stack ;;
    q8|q11|q12|q18)   window_stack ;;
    q17)              export FRS_RS_EXECUTOR="${FRS_RS_EXECUTOR:-routing-adaptive}" ;;
    *)                : ;;  # light/source-bound: fairness baseline only
  esac
  # q9 KV-sep resident bounds (so KV-sep fits the 16g split cgroup — the whole
  # point of the 10240m process.size carve-out). Harmless for other queries but
  # only set them for q9 to keep the other queries' env minimal.
  if [ "$q" = "q9" ]; then
    export FRS_VLOG_READER_CACHE_CAP="${FRS_VLOG_READER_CACHE_CAP:-2048}"
    export FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-512}"
    export FRS_KV_ADAPTIVE_PRESSURE="${FRS_KV_ADAPTIVE_PRESSURE:-1}"
  fi
}

parse_and_record() {
  local q="$1" arm="$2" tag="$3" out="$4"
  local line state wall_ms wall_s src out_rows ts
  line="$(grep -E 'RESULT:' "$out" | tail -1)"
  ts="$(date +%Y%m%dT%H%M%S)"
  if [ -z "$line" ]; then
    state="NO_RESULT"; wall_ms="-"; wall_s="-"; src="-"; out_rows="-"
    # If MAXSEC banner present, mark DNF
    grep -q 'MAXSEC .* reached' "$out" && state="DNF_MAXSEC"
  else
    state="$(echo "$line" | sed -n 's/.*FINISHED.*/FINISHED/p;s/.*ENDED state=\([A-Z]*\).*/\1/p')"
    [ -z "$state" ] && state="UNKNOWN"
    wall_ms="$(echo "$line" | sed -n 's/.*wall_ms=\([0-9]*\).*/\1/p')"; [ -z "$wall_ms" ] && wall_ms="$(echo "$line" | sed -n 's/.*dur_ms=\([0-9]*\).*/\1/p')"
    wall_s="-"; [ -n "$wall_ms" ] && wall_s="$(awk "BEGIN{printf \"%.1f\", $wall_ms/1000}")"
    src="$(echo "$line" | sed -n 's/.*src_out=\([0-9]*\).*/\1/p')"
    out_rows="$(echo "$line" | sed -n 's/.*out_rows=\([0-9]*\).*/\1/p')"
    [ -z "$out_rows" ] && out_rows="-"; [ -z "$src" ] && src="-"; [ -z "$wall_ms" ] && wall_ms="-"
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$q" "$arm" "$state" "$wall_ms" "$wall_s" "$src" "$out_rows" "$tag" "$ts" >> "$RESULTS_TSV"
  echo ">>> RECORDED: $q $arm state=$state wall_ms=$wall_ms wall_s=$wall_s out_rows=$out_rows"
}

for q in $QUERIES; do
  for arm in $ARMS; do
    tag="$TAG_PREFIX-$q-$arm"
    out="/tmp/$tag-$q-$arm.out"
    echo ""
    echo "################ UNIFORM-SPLIT RUN $q [$arm] tag=$tag MAXSEC=$MAXSEC ################"
    docker rm -f "$tag-jm" "$tag-tm1" "$tag-tm2" >/dev/null 2>&1 || true
    (
      export TOPO=split
      if [ "$arm" = "forst-rs-ffm-local" ]; then
        apply_validate "$q"
      else
        # baselines: only fairness compression; no forst-rs levers
        unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE FRS_RS_S2_PINNED \
              FRS_S2_FANOUT_MIN FRS_RS_EXECUTOR FRS_VLOG_COALESCE_DEREF FRS_RS_PROBE_BLOOM_PRUNE \
              FRS_RS_LEVELED_HOT_CF FRS_PERSISTENT_PROBE_ITER FRS_RS_MERGE_RMW 2>/dev/null || true
        export FRS_SST_COMPRESSION=lz4
      fi
      # FRS_MEM_DIAG_FILE on the SHARED base mount (not the per-cluster /tmp,
      # which the sweep deletes) so the in-container sampler's shed/level log
      # survives for post-run inspection on the host.
      if [ "$arm" = "forst-rs-ffm-local" ]; then
        export FRS_MEM_DIAG=1
        export FRS_MEM_DIAG_FILE="${DIAG_BASE:-/Users/lijunqing/Downloads/workenv/frs-tmp}/dynshed-diag-$tag.log"
        rm -f "$FRS_MEM_DIAG_FILE" 2>/dev/null || true
      fi
      echo "  levers: KV_SEP=${FRS_KV_SEPARATION:-OFF} EXEC=${FRS_RS_EXECUTOR:-default} MERGE_RMW=${FRS_RS_MERGE_RMW:-OFF} DYN_SHED=${FRS_DYNAMIC_SHED:-OFF}"
      CLUSTER="$tag" bash "$RUNNER" run "$q" "$arm" "$MAXSEC" "$tag"
    )
    parse_and_record "$q" "$arm" "$tag" "$out"
    # docker-clean between runs (safety; harness already tears its cluster down)
    docker rm -f "$tag-jm" "$tag-tm1" "$tag-tm2" >/dev/null 2>&1 || true
    docker network rm "$tag-net" >/dev/null 2>&1 || true
    # free the per-cluster scratch to reclaim disk
    rm -rf "${FRS_CTMP_BASE:-$REPO/../../Downloads/workenv/frs-tmp}/$tag" 2>/dev/null || true
    rm -rf "/Users/lijunqing/Downloads/workenv/frs-tmp/$tag" 2>/dev/null || true
  done
done

echo ""
echo "== pmc1 uniform sweep done; results in $RESULTS_TSV =="
cat "$RESULTS_TSV"
