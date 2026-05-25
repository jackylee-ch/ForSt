#!/usr/bin/env bash
# Parse DispatchBatch log lines from a Flink TaskManager log and emit a histogram.
#
# Usage:
#   ./parse-dispatch-batch-histogram.sh /path/to/flink-...-taskexecutor-...log
#
# Expects log lines emitted by VectorizedExecutor.executeGets / executePuts when
# the JVM property forst.rs.dispatch.batch_log_every is set to a non-zero value
# (e.g., 200). Line format:
#   DispatchBatch kind=GET n=<batchSize> latencyNs=<elapsed> totalCount=<cumulative> nsPerOp=<latency/n>

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <flink-taskexecutor-log-path>" >&2
    exit 1
fi

LOG="$1"
if [[ ! -f "$LOG" ]]; then
    echo "Log file not found: $LOG" >&2
    exit 1
fi

emit_histogram_for_kind() {
    local kind="$1"
    echo "=== kind=$kind ==="

    local sizes
    sizes=$(grep "DispatchBatch kind=$kind " "$LOG" | grep -oE 'n=[0-9]+' | sed 's/n=//')
    if [[ -z "$sizes" ]]; then
        echo "  (no samples)"
        return
    fi

    local count
    count=$(echo "$sizes" | wc -l | tr -d ' ')
    echo "  Samples: $count"

    # Mean / min / max
    echo "$sizes" | awk -v count="$count" '
        BEGIN { min=1e18; max=0; sum=0 }
        { if($1<min) min=$1; if($1>max) max=$1; sum+=$1 }
        END {
            printf "  Min/Max: %d / %d\n", min, max
            printf "  Mean:    %.2f\n", sum/NR
        }
    '

    # Median + p95 + p99 via sort
    local sorted_file
    sorted_file=$(mktemp)
    echo "$sizes" | sort -n > "$sorted_file"

    local median p95 p99
    median=$(awk -v n="$count" 'NR == int((n+1)/2) {print; exit}' "$sorted_file")
    p95=$(awk -v n="$count" 'NR == int(n*0.95+0.5) {print; exit}' "$sorted_file")
    p99=$(awk -v n="$count" 'NR == int(n*0.99+0.5) {print; exit}' "$sorted_file")
    echo "  Median:  $median"
    echo "  p95:     $p95"
    echo "  p99:     $p99"

    rm -f "$sorted_file"

    # Decile bins
    echo "  Histogram (batch-size bins, count per bin):"
    echo "$sizes" | awk '
        {
            n=$1
            if (n<=4) bin="[1,4]"
            else if (n<=16) bin="[5,16]"
            else if (n<=64) bin="[17,64]"
            else if (n<=256) bin="[65,256]"
            else bin="[257,+inf)"
            counts[bin]++
        }
        END {
            order[1]="[1,4]"; order[2]="[5,16]"; order[3]="[17,64]"; order[4]="[65,256]"; order[5]="[257,+inf)"
            for (i=1; i<=5; i++) {
                b = order[i]
                c = (b in counts) ? counts[b] : 0
                printf "    %-14s %6d\n", b, c
            }
        }
    '
    echo ""
}

emit_histogram_for_kind GET
emit_histogram_for_kind PUT

echo "Total DispatchBatch lines: $(grep -c 'DispatchBatch' "$LOG")"
