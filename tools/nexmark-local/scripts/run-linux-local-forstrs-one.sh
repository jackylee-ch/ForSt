#!/usr/bin/env bash
# Run one Linux local ForSt-RS NexMark query in a resumable result directory.
#
# This wrapper delegates profile/topology selection to run-linux-local-forstrs.sh
# while forcing QUERIES to a single query. It is useful with nohup for long
# queries so an interactive shell/session timeout cannot kill the benchmark.
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <query>"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export QUERIES="$1"

exec bash "$SCRIPT_DIR/run-linux-local-forstrs.sh"
