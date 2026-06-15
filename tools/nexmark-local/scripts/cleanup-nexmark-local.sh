#!/usr/bin/env bash
# Cleanup only the Linux local NexMark scenario namespace.
#
# This script intentionally scopes all destructive operations to names beginning
# with SCENARIO_PREFIX (default: nexmark-local). It must not touch bos-*, rv3-*,
# s3sim-*, forst-*, rdb-*, or other concurrent benchmark environments.
set -euo pipefail

SCENARIO_PREFIX="${SCENARIO_PREFIX:-nexmark-local}"
ROOT="${ROOT:-/ssd2/jackylee/frs-bench}"
CLEAN_IMAGES="${CLEAN_IMAGES:-0}"
PATH=/home/work/dockerd/bin:$PATH

case "$SCENARIO_PREFIX" in
  nexmark-local|nexmark-local-*) ;;
  *)
    echo "FATAL: refusing cleanup for non-nexmark-local prefix: $SCENARIO_PREFIX"
    exit 1
    ;;
esac

mapfile -t containers < <(
  docker ps -a --format '{{.Names}}' 2>/dev/null \
    | awk -v p="$SCENARIO_PREFIX" 'index($0, p "-") == 1 { print }'
)
if [ "${#containers[@]}" -gt 0 ]; then
  docker rm -f "${containers[@]}" >/dev/null 2>&1 || true
fi

mapfile -t networks < <(
  docker network ls --format '{{.Name}}' 2>/dev/null \
    | awk -v p="$SCENARIO_PREFIX" 'index($0, p "-") == 1 { print }'
)
if [ "${#networks[@]}" -gt 0 ]; then
  docker network rm "${networks[@]}" >/dev/null 2>&1 || true
fi

if [ -d "$ROOT" ]; then
  cleanup_img="${CLEANUP_IMG:-nexmark-local-forst-bench:x86}"
  if ! docker image inspect "$cleanup_img" >/dev/null 2>&1; then
    cleanup_img=forst-bench:x86
  fi
  while IFS= read -r dir; do
    printf '%s\n' "$dir"
    rm -rf "$dir" >/dev/null 2>&1 || true
    if [ -e "$dir" ]; then
      docker run --rm --user 0:0 -v "$ROOT:$ROOT" "$cleanup_img" \
        sh -c 'rm -rf "$1"' sh "$dir" >/dev/null 2>&1 || true
    fi
    [ ! -e "$dir" ] || echo "WARN: failed to fully remove $dir"
  done < <(find "$ROOT" -maxdepth 1 -type d -name "tmp-$SCENARIO_PREFIX-*" -print)
fi

images=()
if [ "$CLEAN_IMAGES" = "1" ]; then
  mapfile -t images < <(
    docker images --format '{{.Repository}}:{{.Tag}}' 2>/dev/null \
      | awk -v p="$SCENARIO_PREFIX" 'index($0, p "-") == 1 { print }'
  )
  if [ "${#images[@]}" -gt 0 ]; then
    docker rmi "${images[@]}" >/dev/null 2>&1 || true
  fi
fi

echo "cleaned prefix=$SCENARIO_PREFIX root=$ROOT containers=${#containers[@]} networks=${#networks[@]} images=${#images[@]}"
