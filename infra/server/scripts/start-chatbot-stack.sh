#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

CHATBOT_ENV_FILE="${CHATBOT_ENV_FILE:-${SERVER_ENV_DIR}/chatbot.env}"
load_server_env
load_env_file "${CHATBOT_ENV_FILE}"

compose_server "${SERVER_COMPOSE_DIR}/docker-compose.chatbot.yml" up -d --build
log "Started chatbot frontend on http://127.0.0.1:8089"
log "Started chatbot backend on http://127.0.0.1:8090"
