#!/usr/bin/env bash
#
# Build the Nexmark baseline package — invokes the canonical harness build
# and bundles our config overrides.
#
# Usage:   ./nexmark-config/build-baseline.sh
# Output:  nexmark-baseline.tgz (containing nexmark-flink.tgz + config overrides)
#
# Authoritative spec: .planning/refactor-review/A1_reconciliation.md §4
# Plan: .planning/refactor-review/N2_nexmark_plan.md

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NEXMARK_DIR="${REPO_ROOT}/nexmark/nexmark-flink"
CONFIG_DIR="${REPO_ROOT}/nexmark-config"
OUT_DIR="${REPO_ROOT}/build/nexmark-baseline"

if [[ ! -d "${NEXMARK_DIR}" ]]; then
  echo "ERROR: ${NEXMARK_DIR} not found."
  echo ""
  echo "The nexmark/nexmark submodule is not vendored yet. Per N2 plan Task 1,"
  echo "this requires explicit user authorization to vendor external code."
  echo ""
  echo "To enable: have user run"
  echo "    git submodule add https://github.com/nexmark/nexmark.git nexmark"
  echo "    cd nexmark && git checkout 6b3646c && cd .."
  echo "    git add .gitmodules nexmark && git commit"
  echo ""
  echo "OR if submodule is registered but not initialized:"
  echo "    git submodule update --init --recursive"
  exit 1
fi

echo "[1/3] Building canonical nexmark-flink package..."
cd "${NEXMARK_DIR}"
./build.sh
cd "${REPO_ROOT}"

echo "[2/3] Bundling config overrides..."
mkdir -p "${OUT_DIR}"
cp "${NEXMARK_DIR}/nexmark-flink.tgz" "${OUT_DIR}/"
cp "${CONFIG_DIR}/flink-config-baseline.yaml" "${OUT_DIR}/"
cp "${CONFIG_DIR}/nexmark.yaml" "${OUT_DIR}/"

echo "[3/3] Creating combined tarball..."
cd "${REPO_ROOT}/build"
tar czf nexmark-baseline.tgz nexmark-baseline/
mv nexmark-baseline.tgz "${REPO_ROOT}/"
cd "${REPO_ROOT}"

echo ""
echo "✓ Build complete: ${REPO_ROOT}/nexmark-baseline.tgz"
echo "  Contents: nexmark-flink.tgz + config overrides"
echo ""
echo "Next steps (user, on cluster):"
echo "  1. scp nexmark-baseline.tgz to master node"
echo "  2. tar xzf nexmark-baseline.tgz; cd nexmark-baseline"
echo "  3. Follow .planning/nexmark/USER_RUNBOOK.md"
