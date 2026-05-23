#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

bi_compose() {
  compose --profile core --profile extended --profile serving --profile bi "$@"
}

wait_for_superset() {
  for _ in $(seq 1 60); do
    if curl -fsSL "http://localhost:8088/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  echo "error: Superset did not become ready at http://localhost:8088/health" >&2
  exit 1
}

SUP_CONTAINER_ID="$(bi_compose ps -q superset)"
if [[ -z "${SUP_CONTAINER_ID}" ]]; then
  echo "error: Superset is not running. Start it with 'bash infra/scripts/up-bi.sh' first." >&2
  exit 1
fi

wait_for_superset
docker exec "${SUP_CONTAINER_ID}" python /app/bootstrap/bootstrap_superset.py

