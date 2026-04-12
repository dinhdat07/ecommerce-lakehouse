#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
# shellcheck source=infra/server/spark/submit-common.sh
source "${SERVER_ROOT_DIR}/spark/submit-common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
load_server_env "${STORAGE_ENV_FILE}"

STREAM_PID_FILE="${STREAM_PID_FILE:-${SERVER_RUNTIME_DIR}/streaming/${STREAM_QUERY_NAME}.pid}"
STREAM_LOG_FILE="${STREAM_LOG_FILE:-${SERVER_RUNTIME_DIR}/streaming/${STREAM_QUERY_NAME}.log}"
mkdir -p "$(dirname "${STREAM_PID_FILE}")" "$(dirname "${STREAM_LOG_FILE}")" "$(dirname "${STREAM_CHECKPOINT_LOCATION}")"
if [[ -n "${STREAM_PROGRESS_LOG_PATH}" ]]; then
  mkdir -p "$(dirname "${STREAM_PROGRESS_LOG_PATH}")"
fi

if pid_is_running "${STREAM_PID_FILE}"; then
  fail "streaming job already running with pid $(cat "${STREAM_PID_FILE}")"
fi

if [[ "${STREAM_RUN_IN_FOREGROUND:-0}" == "1" ]]; then
  spark_submit_streaming_job "${ROOT_DIR}/infra/jobs/kafka_stream_to_iceberg.py" \
    --mode server \
    --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS}" \
    --topic "${KAFKA_TOPIC_EVENTS}" \
    --query-name "${STREAM_QUERY_NAME}" \
    --checkpoint-location "${STREAM_CHECKPOINT_LOCATION}" \
    --progress-log-path "${STREAM_PROGRESS_LOG_PATH}" \
    --starting-offsets "${STREAM_STARTING_OFFSETS}" \
    --trigger-seconds "${STREAM_TRIGGER_SECONDS}" \
    --max-offsets-per-trigger "${STREAM_MAX_OFFSETS_PER_TRIGGER}" \
    --timeout-seconds "${STREAM_TIMEOUT_SECONDS}" \
    --stop-after-seconds "${STREAM_STOP_AFTER_SECONDS}" \
    --progress-poll-seconds "${STREAM_PROGRESS_POLL_SECONDS}" \
    --fail-on-data-loss "${STREAM_FAIL_ON_DATA_LOSS}"
  exit 0
fi

(
  spark_submit_streaming_job "${ROOT_DIR}/infra/jobs/kafka_stream_to_iceberg.py" \
    --mode server \
    --bootstrap-servers "${KAFKA_BOOTSTRAP_SERVERS}" \
    --topic "${KAFKA_TOPIC_EVENTS}" \
    --query-name "${STREAM_QUERY_NAME}" \
    --checkpoint-location "${STREAM_CHECKPOINT_LOCATION}" \
    --progress-log-path "${STREAM_PROGRESS_LOG_PATH}" \
    --starting-offsets "${STREAM_STARTING_OFFSETS}" \
    --trigger-seconds "${STREAM_TRIGGER_SECONDS}" \
    --max-offsets-per-trigger "${STREAM_MAX_OFFSETS_PER_TRIGGER}" \
    --timeout-seconds "${STREAM_TIMEOUT_SECONDS}" \
    --stop-after-seconds "${STREAM_STOP_AFTER_SECONDS}" \
    --progress-poll-seconds "${STREAM_PROGRESS_POLL_SECONDS}" \
    --fail-on-data-loss "${STREAM_FAIL_ON_DATA_LOSS}"
) >>"${STREAM_LOG_FILE}" 2>&1 &

echo "$!" >"${STREAM_PID_FILE}"
log "Streaming job started with pid $(cat "${STREAM_PID_FILE}")"
