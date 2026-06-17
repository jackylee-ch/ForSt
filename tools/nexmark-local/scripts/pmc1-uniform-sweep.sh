#!/usr/bin/env bash
# pmc1-uniform-sweep.sh — PMC-1 UNIFORM-config q0-q22 sweep (2026-06-17).
#
# Drives run-8c32g.sh under the UNIFORM 2x4c/16g SPLIT topology with the SINGLE
# uniform forst-rs config applied to EVERY query — NO per-query branches (user
# directive: per-query config is FORBIDDEN). The config is read from
# configs/best-config.tsv (the `*` row) so this driver and run-best.sh share ONE
# source of truth. The engine adapts to the query SHAPE at runtime under the one
# config (FRS_VLOG_POINT_DEREF auto-follows KV-sep for windowed point-RMW;
# FRS_VLOG_COALESCE_DEREF batches scan derefs). Records each RESULT incrementally.
#
# USAGE (REPO/WORKENV/etc are env-overridable; the harness detects the OS):
#   REPO=/path/to/checkout QUERIES="q9 q7 ..." \
#     ARMS="forst-rs-ffm-local rocksdb forst-local" bash pmc1-uniform-sweep.sh
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO="${REPO:-$(cd "$PKG_DIR/../.." && pwd)}"
RUNNER="$PKG_DIR/scripts/run-8c32g.sh"
[ -f "$RUNNER" ] || RUNNER="$REPO/scripts/run-8c32g.sh"
TSV="$PKG_DIR/configs/best-config.tsv"
RESULTS_TSV="${RESULTS_TSV:-$PKG_DIR/pmc1-uniform-results.tsv}"

export EVENTS_NUM="${EVENTS_NUM:-100000000}"
export TPS="${TPS:-10000000}"
MAXSEC="${MAXSEC:-2700}"
TAG_PREFIX="${TAG_PREFIX:-pmc1u}"

QUERIES="${QUERIES:-q4 q7 q9 q11 q12 q17 q19 q20}"
ARMS="${ARMS:-forst-rs-ffm-local rocksdb forst-local}"

[ -f "$RESULTS_TSV" ] || printf 'query\tarm\tstate\twall_ms\twall_s\tsrc_out\tout_rows\ttag\tts\n' > "$RESULTS_TSV"

# --- read the single uniform `*` row from configs/best-config.tsv ---
lookup_uniform_row() {
  awk -F'\t' '
    /^[[:space:]]*#/ { next } /^[[:space:]]*$/ { next }
    $1 == "query" { next } $1 == "*" { print; found=1; exit }
    END { if (!found) exit 3 }
  ' "$TSV"
}

# Apply the UNIFORM forst-rs config — IDENTICAL for every query (no branches).
# Mirrors run-best.sh apply_uniform; forst-rs arm only (baselines ignore FRS_*).
apply_uniform() {
  local row; row="$(lookup_uniform_row)" || { echo "FATAL: no uniform '*' row in $TSV"; exit 1; }
  local query kvsep kvmin trivial s2pin coalesce comp memmgr note
  IFS=$'\t' read -r query kvsep kvmin trivial s2pin coalesce comp memmgr note <<<"$row"
  unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE FRS_RS_S2_PINNED \
        FRS_VLOG_COALESCE_DEREF FRS_VLOG_POINT_DEREF FRS_RS_EXECUTOR FRS_S2_FANOUT_MIN \
        FRS_RS_PROBE_BLOOM_PRUNE FRS_RS_LEVELED_HOT_CF FRS_PERSISTENT_PROBE_ITER \
        FRS_RS_MERGE_RMW FRS_RS_MERGE_RMW_STATES 2>/dev/null || true
  setk() { [ "$2" != "-" ] && export "$1=$2" || true; }
  setk FRS_KV_SEPARATION    "$kvsep"
  setk FRS_KV_MIN_BLOB_SIZE "$kvmin"
  setk FRS_TRIVIAL_MOVE     "$trivial"
  setk FRS_RS_S2_PINNED     "$s2pin"
  setk FRS_VLOG_COALESCE_DEREF "$coalesce"
  setk FRS_MEM_MANAGER      "$memmgr"
  export FRS_SST_COMPRESSION="${comp:-lz4}"
  export FRS_VLOG_COMPRESSION="${FRS_VLOG_COMPRESSION:-inherit}"
  # FRS_VLOG_POINT_DEREF LEFT UNSET -> auto-follows KV-sep (db.rs:467).
  export FRS_VLOG_READER_CACHE_CAP="${FRS_VLOG_READER_CACHE_CAP:-2048}"
  export FRS_VLOG_RESIDENT_BUDGET_MB="${FRS_VLOG_RESIDENT_BUDGET_MB:-256}"
  export FRS_KV_ADAPTIVE_PRESSURE="${FRS_KV_ADAPTIVE_PRESSURE:-1}"
}

parse_and_record() {
  local q="$1" arm="$2" tag="$3" out="$4"
  local line state wall_ms wall_s src out_rows ts
  line="$(grep -E 'RESULT:' "$out" | tail -1)"
  ts="$(date +%Y%m%dT%H%M%S)"
  if [ -z "$line" ]; then
    state="NO_RESULT"; wall_ms="-"; wall_s="-"; src="-"; out_rows="-"
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
        apply_uniform
      else
        unset FRS_KV_SEPARATION FRS_KV_MIN_BLOB_SIZE FRS_TRIVIAL_MOVE FRS_RS_S2_PINNED \
              FRS_S2_FANOUT_MIN FRS_RS_EXECUTOR FRS_VLOG_COALESCE_DEREF FRS_VLOG_POINT_DEREF \
              FRS_MEM_MANAGER FRS_RS_PROBE_BLOOM_PRUNE \
              FRS_RS_LEVELED_HOT_CF FRS_PERSISTENT_PROBE_ITER FRS_RS_MERGE_RMW 2>/dev/null || true
        export FRS_SST_COMPRESSION=lz4
      fi
      echo "  uniform levers: KV_SEP=${FRS_KV_SEPARATION:-OFF} COALESCE=${FRS_VLOG_COALESCE_DEREF:-OFF} POINT_DEREF=<auto> MEM_MGR=${FRS_MEM_MANAGER:-OFF}"
      CLUSTER="$tag" bash "$RUNNER" run "$q" "$arm" "$MAXSEC" "$tag"
    )
    parse_and_record "$q" "$arm" "$tag" "$out"
    docker rm -f "$tag-jm" "$tag-tm1" "$tag-tm2" >/dev/null 2>&1 || true
    docker network rm "$tag-net" >/dev/null 2>&1 || true
    # The harness scratch-trap already rm -rf's the per-cluster scratch on exit;
    # this is a belt-and-suspenders reclaim using the SAME base it computes.
    rm -rf "${FRS_CTMP_BASE:-${WORKENV:-$HOME/workenv}/frs-tmp}/$tag" 2>/dev/null || true
  done
done

echo ""
echo "== pmc1 uniform sweep done; results in $RESULTS_TSV =="
cat "$RESULTS_TSV"
