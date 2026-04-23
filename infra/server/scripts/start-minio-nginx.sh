#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

MINIO_NGINX_ENV_FILE="${MINIO_NGINX_ENV_FILE:-${SERVER_ENV_DIR}/minio-nginx.env}"
load_server_env "${MINIO_NGINX_ENV_FILE}"

mkdir -p "${MINIO_NGINX_CERT_DIR:-/srv/ecommerce/certs/minio}"
bash "${SCRIPT_DIR}/render-minio-nginx-conf.sh"
compose_server "${SERVER_COMPOSE_DIR}/docker-compose.minio-nginx.yml" up -d
log "MinIO Nginx ingress started on ports ${MINIO_NGINX_API_PORT}/${MINIO_NGINX_CONSOLE_PORT}."
