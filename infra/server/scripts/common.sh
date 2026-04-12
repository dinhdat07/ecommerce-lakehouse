#!/usr/bin/env bash
set -euo pipefail

SERVER_SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_ROOT_DIR="$(cd -- "${SERVER_SCRIPT_DIR}/.." && pwd)"
ROOT_DIR="$(cd -- "${SERVER_ROOT_DIR}/../.." && pwd)"
SERVER_ENV_DIR="${SERVER_ROOT_DIR}/env"
SERVER_COMPOSE_DIR="${SERVER_ROOT_DIR}/compose"

log() {
  printf '[server] %s\n' "$*"
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

load_env_file() {
  local env_file="$1"
  [[ -f "${env_file}" ]] || fail "env file not found: ${env_file}. Copy the matching .example file first."
  set -a
  # shellcheck disable=SC1090
  source "${env_file}"
  set +a
}

load_server_env() {
  local primary="${SERVER_ENV_FILE:-${SERVER_ENV_DIR}/server.env}"
  load_env_file "${primary}"
  for extra_file in "$@"; do
    if [[ -n "${extra_file}" ]]; then
      load_env_file "${extra_file}"
    fi
  done
  mkdir -p "${SERVER_RUNTIME_DIR:-${ROOT_DIR}/run/server}"
}

compose_server() {
  local compose_file="$1"
  shift
  if docker compose version >/dev/null 2>&1; then
    docker compose -p "${SERVER_COMPOSE_PROJECT_NAME:-ecommerce-lakehouse-server}" -f "${compose_file}" "$@"
    return
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    docker-compose -p "${SERVER_COMPOSE_PROJECT_NAME:-ecommerce-lakehouse-server}" -f "${compose_file}" "$@"
    return
  fi
  fail "Docker Compose is not available"
}

pid_is_running() {
  local pid_file="$1"
  [[ -f "${pid_file}" ]] || return 1
  local pid
  pid="$(cat "${pid_file}")"
  [[ -n "${pid}" ]] || return 1
  kill -0 "${pid}" >/dev/null 2>&1
}
