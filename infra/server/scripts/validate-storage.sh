#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
load_server_env "${STORAGE_ENV_FILE}"

if command -v mc >/dev/null 2>&1; then
  mc alias set lakehouse "${S3_ENDPOINT}" "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}" >/dev/null
  mc ls lakehouse
  exit 0
fi

if command -v docker >/dev/null 2>&1; then
  docker run --rm --network host \
    -e S3_ENDPOINT \
    -e S3_ACCESS_KEY \
    -e S3_SECRET_KEY \
    --entrypoint /bin/sh \
    minio/mc:RELEASE.2025-02-21T16-00-46Z \
    -ec 'mc alias set lakehouse "${S3_ENDPOINT}" "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}" >/dev/null && mc ls lakehouse'
  exit 0
fi

fail "either mc or docker is required to validate object storage connectivity"
