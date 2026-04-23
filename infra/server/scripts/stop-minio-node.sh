#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
MINIO_CLUSTER_ENV_FILE="${MINIO_CLUSTER_ENV_FILE:-${SERVER_ENV_DIR}/minio-cluster.env}"
load_server_env "${STORAGE_ENV_FILE}" "${MINIO_CLUSTER_ENV_FILE}"

compose_server "${SERVER_COMPOSE_DIR}/docker-compose.minio-node.yml" down
log "MinIO node stopped."
