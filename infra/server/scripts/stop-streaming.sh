#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

load_server_env
STREAM_PID_FILE="${STREAM_PID_FILE:-${SERVER_RUNTIME_DIR}/streaming/${STREAM_QUERY_NAME}.pid}"

if [[ "${STREAM_USE_SYSTEMD:-0}" == "1" ]]; then
  sudo systemctl stop "${STREAM_SERVICE_NAME:-ecommerce-streaming.service}"
  log "Streaming systemd service stopped."
  exit 0
fi

if pid_is_running "${STREAM_PID_FILE}"; then
  kill "$(cat "${STREAM_PID_FILE}")"
  rm -f "${STREAM_PID_FILE}"
  log "Streaming process stopped."
  exit 0
fi

rm -f "${STREAM_PID_FILE}"
log "No running streaming pid file was found."
