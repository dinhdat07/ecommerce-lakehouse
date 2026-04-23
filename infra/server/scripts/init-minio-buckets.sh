#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
load_server_env "${STORAGE_ENV_FILE}"

run_mc() {
  if command -v mc >/dev/null 2>&1; then
    mc "$@"
    return
  fi

  if command -v docker >/dev/null 2>&1; then
    docker run --rm --network host minio/mc:RELEASE.2025-02-21T16-00-46Z mc "$@"
    return
  fi

  fail "either mc or docker is required to initialize MinIO buckets"
}

run_mc alias set lakehouse "${S3_ENDPOINT}" "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}" >/dev/null
run_mc mb --ignore-existing "lakehouse/${S3_BUCKET_BRONZE}"
run_mc mb --ignore-existing "lakehouse/${S3_BUCKET_SILVER}"
run_mc mb --ignore-existing "lakehouse/${S3_BUCKET_GOLD}"
run_mc mb --ignore-existing "lakehouse/${S3_BUCKET_WAREHOUSE}"

log "MinIO buckets are ready."
