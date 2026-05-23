#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
SERVER_ENV_FILE="${SERVER_ENV_FILE:-${SERVER_ENV_DIR}/server.env}"
load_server_env "${STORAGE_ENV_FILE}"
load_env_file "${SERVER_ENV_FILE}"

bash "${SCRIPT_DIR}/render-trino-catalog.sh"
compose_server "${SERVER_COMPOSE_DIR}/docker-compose.bi.yml" up -d
log "Started Trino and Superset on node1."
log "Trino HTTP: http://100.123.190.84:8085"
log "Superset HTTP: http://100.123.190.84:8088"
