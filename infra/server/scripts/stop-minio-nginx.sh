#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

MINIO_NGINX_ENV_FILE="${MINIO_NGINX_ENV_FILE:-${SERVER_ENV_DIR}/minio-nginx.env}"
load_server_env "${MINIO_NGINX_ENV_FILE}"

compose_server "${SERVER_COMPOSE_DIR}/docker-compose.minio-nginx.yml" down
log "MinIO Nginx ingress stopped."
