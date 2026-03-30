#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

require_running() {
  local service="$1"
  local cid
  cid="$(service_cid "${service}")"
  if [[ -z "${cid}" ]]; then
    echo "error: service ${service} is not running." >&2
    exit 1
  fi
}

require_running minio
require_running kafka
require_running spark-master
require_running spark-worker-1

docker exec "$(service_cid kafka)" kafka-topics.sh --bootstrap-server localhost:29092 --list | grep -qx "ecom.events"

docker run --rm --network "${PROJECT_NAME}" minio/mc:RELEASE.2025-02-21T16-00-46Z /bin/sh -ec '
  mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null
  mc ls local | grep -q "bronze"
  mc ls local | grep -q "silver"
  mc ls local | grep -q "gold"
  mc ls local | grep -q "warehouse"
'

curl -fsSL http://localhost:8081 | grep -q "spark-worker-1"

if [[ -n "$(service_cid spark-worker-2)" ]]; then
  require_running spark-worker-2
  curl -fsSL http://localhost:8081 | grep -q "spark-worker-2"
fi

echo "verification ok"
