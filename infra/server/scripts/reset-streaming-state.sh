#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

load_server_env
STREAM_PID_FILE="${STREAM_PID_FILE:-${SERVER_RUNTIME_DIR}/streaming/${STREAM_QUERY_NAME}.pid}"

if pid_is_running "${STREAM_PID_FILE}"; then
  fail "streaming job is still running; stop it first"
fi

rm -rf "${STREAM_CHECKPOINT_LOCATION}"
rm -f "${STREAM_PROGRESS_LOG_PATH}" "${STREAM_PID_FILE}"
if [[ "${STREAM_RESET_REMOVE_LOGS:-0}" == "1" ]]; then
  rm -f "${STREAM_LOG_FILE:-${SERVER_RUNTIME_DIR}/streaming/${STREAM_QUERY_NAME}.log}"
fi

log "Streaming checkpoint and progress state reset."
