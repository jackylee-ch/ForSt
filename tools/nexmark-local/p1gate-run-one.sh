#!/usr/bin/env bash
# p1gate-run-one.sh QUERY [MAXSEC] — run ONE forst-rs query under the uniform
# config + split topology, with peak-RSS polling, recording to the pmc1 TSV and
# emitting a single GATE line with wall/out_rows/peak-RSS. ONE query, sequential.
set -u
SD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
Q="$1"; MS="${2:-2700}"
export REPO=/tmp/frs-p1sweep
export WORKENV=/Users/lijunqing/Downloads/workenv
export EVENTS_NUM="${EVENTS_NUM:-100000000}"
export TPS="${TPS:-10000000}"
TAG="p1g-$Q"
CLUSTER="$TAG-forst-rs-ffm-local"
RSSOUT="/tmp/$TAG-rss.txt"
echo "peak_tm1_mib=0 peak_tm2_mib=0" > "$RSSOUT"
docker rm -f "$CLUSTER-jm" "$CLUSTER-tm1" "$CLUSTER-tm2" >/dev/null 2>&1 || true
# start RSS poller in background (own process; reaped when TMs gone)
bash "$SD/p1gate-rss-poll.sh" "$CLUSTER" "$RSSOUT" >/dev/null 2>&1 &
RSSPID=$!
QUERIES="$Q" ARMS="forst-rs-ffm-local" MAXSEC="$MS" TAG_PREFIX=p1g \
  RESULTS_TSV="$SD/pmc1-uniform-results.tsv" \
  bash "$SD/scripts/pmc1-uniform-sweep.sh"
kill "$RSSPID" >/dev/null 2>&1 || true
echo "=== P1GATE RSS for $Q ==="; cat "$RSSOUT"
