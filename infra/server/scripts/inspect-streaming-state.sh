#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

load_server_env

STREAM_PID_FILE="${STREAM_PID_FILE:-${SERVER_RUNTIME_DIR}/streaming/${STREAM_QUERY_NAME}.pid}"
STREAM_LOG_FILE="${STREAM_LOG_FILE:-${SERVER_RUNTIME_DIR}/streaming/${STREAM_QUERY_NAME}.log}"

printf 'query_name=%s\n' "${STREAM_QUERY_NAME}"
printf 'topic=%s\n' "${KAFKA_TOPIC_EVENTS}"
printf 'checkpoint=%s\n' "${STREAM_CHECKPOINT_LOCATION}"
printf 'progress_log=%s\n' "${STREAM_PROGRESS_LOG_PATH}"

if pid_is_running "${STREAM_PID_FILE}"; then
  printf 'status=running pid=%s\n' "$(cat "${STREAM_PID_FILE}")"
else
  printf 'status=not_running\n'
fi

if [[ -d "${STREAM_CHECKPOINT_LOCATION}" ]]; then
  find "${STREAM_CHECKPOINT_LOCATION}" -maxdepth 2 -type f | sort | tail -n 20
else
  printf 'checkpoint_dir_missing\n'
fi

if [[ -f "${STREAM_PROGRESS_LOG_PATH}" ]]; then
  tail -n 20 "${STREAM_PROGRESS_LOG_PATH}"
fi

if [[ -f "${STREAM_LOG_FILE}" ]]; then
  tail -n 20 "${STREAM_LOG_FILE}"
fi
