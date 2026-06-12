#!/usr/bin/env bash
# pick-disk.sh — choose the least-IO-utilized state dir for a NexMark cluster.
# Usage: BASE=$(bash scripts/pick-disk.sh)  → echoes e.g. /ssd1/jackylee
# Policy (user 2026-06-12): concurrent clusters spread across these candidates;
# both arms of an A/B pair must reuse the SAME base (caller's responsibility —
# call once per pair and pass to both arms via FRS_CTMP_BASE).
set -u
CANDIDATES=${FRS_DISK_CANDIDATES:-"/ssd2/jackylee /ssd1/jackylee /tmp/jackylee"}
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
[ -z "$best" ] && best="/ssd2/jackylee"
echo "$best"
