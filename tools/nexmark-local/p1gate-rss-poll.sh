#!/usr/bin/env bash
# p1gate-rss-poll.sh CLUSTER OUTFILE — poll docker stats for the cluster's
# tm1/tm2 containers every 3s, recording peak RSS (MiB) for each. Exits when
# both TM containers are gone. ONE arg = cluster tag.
CLUSTER="$1"; OUT="$2"
p1=0; p2=0
to_mib() {
  # input like "1.234GiB" or "567.8MiB" -> integer MiB
  awk -v v="$1" 'BEGIN{
    n=v+0; u=v; sub(/^[0-9.]+/,"",u);
    if (u ~ /GiB/) print int(n*1024);
    else if (u ~ /MiB/) print int(n);
    else if (u ~ /KiB/) print int(n/1024);
    else if (u ~ /GB/) print int(n*953.674);
    else if (u ~ /MB/) print int(n*0.953674);
    else print int(n);
  }'
}
while :; do
  s1="$(docker stats --no-stream --format '{{.MemUsage}}' "$CLUSTER-tm1" 2>/dev/null | awk '{print $1}')"
  s2="$(docker stats --no-stream --format '{{.MemUsage}}' "$CLUSTER-tm2" 2>/dev/null | awk '{print $1}')"
  alive=0
  if [ -n "$s1" ]; then m1="$(to_mib "$s1")"; [ "$m1" -gt "$p1" ] 2>/dev/null && p1="$m1"; alive=1; fi
  if [ -n "$s2" ]; then m2="$(to_mib "$s2")"; [ "$m2" -gt "$p2" ] 2>/dev/null && p2="$m2"; alive=1; fi
  printf 'peak_tm1_mib=%s peak_tm2_mib=%s\n' "$p1" "$p2" > "$OUT"
  # stop once neither TM exists AND we have recorded something
  if [ "$alive" = "0" ] && [ "$p1" -gt 0 -o "$p2" -gt 0 ] 2>/dev/null; then break; fi
  sleep 3
done
printf 'peak_tm1_mib=%s peak_tm2_mib=%s\n' "$p1" "$p2" > "$OUT"
