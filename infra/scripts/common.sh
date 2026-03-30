#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/infra/docker-compose.yml"
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-ecommerce-lakehouse-laptop}"

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" "$@"
    return
  fi

  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" "$@"
    return
  fi

  echo "error: Docker Compose is not available in this environment." >&2
  echo "Install Docker Desktop with WSL integration or the docker compose plugin first." >&2
  exit 1
}

service_cid() {
  compose ps -q "$1"
}
