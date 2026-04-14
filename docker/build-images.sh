#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <version-tag> [registry-prefix]"
  exit 1
fi

VERSION="$1"
REGISTRY_PREFIX="${2:-${REGISTRY_PREFIX:-ghcr.io/falkordb}}"

CONTROL_PLANE_IMAGE="${REGISTRY_PREFIX}/dm-sql-to-falkordb-control-plane:${VERSION}"
RUNNER_IMAGE="${REGISTRY_PREFIX}/dm-sql-to-falkordb-runner:${VERSION}"

docker build \
  -f control-plane/Dockerfile \
  -t "${CONTROL_PLANE_IMAGE}" \
  .

docker build \
  -f docker/runner.Dockerfile \
  -t "${RUNNER_IMAGE}" \
  .

echo "Built images:"
echo "  ${CONTROL_PLANE_IMAGE}"
echo "  ${RUNNER_IMAGE}"
