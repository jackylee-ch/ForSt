#!/usr/bin/env bash
# pick-disk.sh — choose the least-IO-utilized state dir for a NexMark cluster.
# Usage: BASE=$(bash scripts/pick-disk.sh)  -> echoes e.g. /ssd1/jackylee
#
# PORTABLE (macOS dev + origin Linux box):
#   - Linux: candidates default to the box's NVMe mounts (/ssd2|/ssd1|/tmp,
#     under $USER) and the least-busy one is chosen by an `iostat -x` %util
#     sample over the device backing each candidate (GNU df --output=source).
#   - macOS: there is one APFS volume and `df --output` / `iostat -x` use a
#     different (BSD) flag set, so utilization sampling is skipped; a sane
#     writable scratch base under $TMPDIR is returned. Honors an explicit
#     override on both platforms (FRS_DISK_CANDIDATES / FRS_CTMP_BASE).
#
# Policy (user 2026-06-12): concurrent clusters spread across these candidates;
# both arms of an A/B pair must reuse the SAME base (caller's responsibility --
# call once per pair and pass to both arms via FRS_CTMP_BASE).
set -u

OS="$(uname -s)"

# An explicit FRS_CTMP_BASE always wins on either platform.
if [ -n "${FRS_CTMP_BASE:-}" ]; then
  mkdir -p "${FRS_CTMP_BASE}" 2>/dev/null || true
  echo "${FRS_CTMP_BASE}"
  exit 0
fi

if [ "$OS" = "Darwin" ]; then
  # macOS: single APFS volume; GNU iostat/df flags are unavailable. Pick a
  # writable scratch base under $TMPDIR (or an explicit candidate if the user
  # forced one). No utilization sampling — there is only one volume.
  for d in ${FRS_DISK_CANDIDATES:-"${TMPDIR:-/tmp}/jackylee"}; do
    if mkdir -p "$d" 2>/dev/null; then echo "$d"; exit 0; fi
  done
  echo "${TMPDIR:-/tmp}/jackylee"
  exit 0
fi

# Linux: pick the least-busy NVMe mount via an iostat %util sample.
CANDIDATES=${FRS_DISK_CANDIDATES:-"/ssd2/$USER /ssd1/$USER /tmp/$USER"}
best=""; best_util=101
for d in $CANDIDATES; do
  mkdir -p "$d" 2>/dev/null || continue
  dev=$(df --output=source "$d" 2>/dev/null | tail -1 | sed 's|/dev/||; s|p[0-9]*$||; s|[0-9]*$||')
  [ -z "$dev" ] && continue
  # 3s utilization sample for this device (iostat col %util = last field)
  util=$(iostat -x 3 2 2>/dev/null | awk -v d="$dev" '$1==d {u=$NF} END {print (u==""?100:u)}')
  # integer compare via awk (util may be float)
  better=$(awk -v a="$util" -v b="$best_util" 'BEGIN{print (a<b)?1:0}')
  [ "$better" = "1" ] && { best="$d"; best_util="$util"; }
done
# Fallback to the first candidate's directory (writable) if sampling found none.
if [ -z "$best" ]; then
  for d in $CANDIDATES; do
    if mkdir -p "$d" 2>/dev/null; then best="$d"; break; fi
  done
fi
[ -z "$best" ] && best="/tmp/$USER"
echo "$best"
