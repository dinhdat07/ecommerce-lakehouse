#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
load_server_env "${STORAGE_ENV_FILE}"

MODE="${1:-status}"
DEMO_INPUT_CSV="${DEMO_REALTIME_INPUT_CSV:-${ROOT_DIR}/data/sample/events_streaming_demo_1500.csv}"
DEMO_MAX_ROWS="${DEMO_REALTIME_MAX_ROWS:-200}"
DEMO_WAIT_SECONDS="${DEMO_REALTIME_WAIT_SECONDS:-90}"
DEMO_POLL_SECONDS="${DEMO_REALTIME_POLL_SECONDS:-5}"
DEMO_KAFKA_IMAGE="${DEMO_REALTIME_KAFKA_IMAGE:-apache/kafka:4.1.2}"
DEMO_SOURCE_FILE="$(basename "${DEMO_INPUT_CSV}")"

usage() {
  cat <<EOF
Usage: bash infra/server/scripts/demo-realtime-proof.sh <status|run|tail>

status  Show Kafka/Spark streaming status and replay row counts.
run     Safely send a tiny bounded replay sample to Kafka and wait for Bronze rows to increase.
tail    Tail structured streaming progress/log files.

Notes:
- This uses ${DEMO_INPUT_CSV} by default.
- The bundled streaming sample carries 2020-03/2020-04 event dates, so use only if you need live proof.
- It does not reset or clean demo tables automatically.
- UI links:
  - Spark UI: http://100.123.190.84:8080
  - Trino UI: http://100.123.190.84:8085/ui/
  - Superset: http://100.123.190.84:8088
  - MinIO API/console endpoint in this setup: http://100.123.190.84:9100
EOF
}

trino_query() {
  local sql="$1"
  docker exec server-trino sh -lc \
    "trino --server http://127.0.0.1:8085 --catalog iceberg --schema demo --output-format CSV_HEADER --execute \"$sql\""
}

numeric_trino_value() {
  local sql="$1"
  trino_query "${sql}" | tail -n +2 | tr -d '"' | tr -d '\r' | head -n 1
}

stream_pid_file() {
  printf '%s\n' "${STREAM_PID_FILE:-${SERVER_RUNTIME_DIR}/streaming/${STREAM_QUERY_NAME}.pid}"
}

stream_running() {
  pid_is_running "$(stream_pid_file)"
}

ensure_streaming() {
  if stream_running; then
    log "Streaming already running with pid $(cat "$(stream_pid_file)")"
    return 0
  fi
  log "Starting streaming consumer"
  bash "${SCRIPT_DIR}/start-streaming.sh"
  sleep 5
  stream_running || fail "streaming job did not start"
}

show_status() {
  printf '[NOTE] Safe realtime-proof status only. No data will be replayed in this mode.\n'
  printf '[NOTE] If lecturer asks for live proof, run: bash infra/server/scripts/demo-realtime-proof.sh run\n'
  printf '\n'
  printf '[UI] Spark UI: %s\n' "http://100.123.190.84:8080"
  printf '[UI] Trino UI: %s\n' "http://100.123.190.84:8085/ui/"
  printf '[UI] Superset: %s\n' "http://100.123.190.84:8088"
  printf '[UI] MinIO endpoint: %s\n' "http://100.123.190.84:9100"
  printf '\n'
  printf 'sample_csv=%s\n' "${DEMO_INPUT_CSV}"
  printf 'kafka_bootstrap=%s\n' "${KAFKA_BOOTSTRAP_SERVERS}"
  printf 'topic=%s\n' "${KAFKA_TOPIC_EVENTS}"
  printf 'spark_ui=%s\n' "${SPARK_MASTER_STATUS_URL:-http://100.123.190.84:8080/json/}"
  printf 'trino_ui=%s\n' "http://100.123.190.84:8085/ui/"
  printf 'superset=%s\n' "http://100.123.190.84:8088"
  printf '\n'
  bash "${SCRIPT_DIR}/inspect-streaming-state.sh" || true
  printf '\n---\n'
  trino_query "
SELECT 'bronze_kafka_replay_rows' AS metric, count(*) AS value FROM bronze_events WHERE source_type = 'kafka_replay'
UNION ALL
SELECT 'silver_demo_source_rows', count(*) FROM silver_events WHERE source_file = '${DEMO_SOURCE_FILE}'
UNION ALL
SELECT 'bronze_total_rows', count(*) FROM bronze_events
UNION ALL
SELECT 'silver_total_rows', count(*) FROM silver_events
"
}

tail_logs() {
  local progress="${STREAM_PROGRESS_LOG_PATH}"
  local log_file="${STREAM_LOG_FILE:-${SERVER_RUNTIME_DIR}/streaming/${STREAM_QUERY_NAME}.log}"
  printf 'progress_log=%s\n' "${progress}"
  [[ -f "${progress}" ]] && tail -n 20 "${progress}" || printf 'progress_log_missing\n'
  printf '\n---\n'
  printf 'stream_log=%s\n' "${log_file}"
  [[ -f "${log_file}" ]] && tail -n 20 "${log_file}" || printf 'stream_log_missing\n'
}

replay_messages() {
  local input_csv="$1"
  local max_rows="$2"
  [[ -f "${input_csv}" ]] || fail "input csv not found: ${input_csv}"
  python3 - "${input_csv}" "${max_rows}" <<'PY' | docker run --rm -i --network host "${DEMO_KAFKA_IMAGE}" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --topic "${KAFKA_TOPIC_EVENTS}"
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

path = Path(sys.argv[1])
max_rows = int(sys.argv[2])

def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

with path.open("r", encoding="utf-8", newline="") as handle:
    reader = csv.DictReader(handle)
    for idx, row in enumerate(reader):
        if idx >= max_rows:
            break
        payload = {
            "replayed_at": now_iso(),
            "source_file": path.name,
            "source_month": row.get("event_time", "")[:7] or None,
            "payload": row,
        }
        print(json.dumps(payload, sort_keys=True))
PY
}

run_demo() {
  printf '[NOTE] Live proof mode. Script will send bounded sample rows into Kafka.\n'
  printf '[NOTE] Sample file carries 2020-03/2020-04 event dates. Use only if needed for lecturer proof.\n'
  printf '[NOTE] Suggested companion windows: Spark UI http://100.123.190.84:8080 , Trino UI http://100.123.190.84:8085/ui/ , Superset http://100.123.190.84:8088\n'
  printf '\n'
  printf '[STEP 1] Ensure streaming consumer is running\n'
  ensure_streaming
  local before_bronze before_silver current_bronze start_ts loops max_loops
  before_bronze="$(numeric_trino_value "SELECT count(*) FROM bronze_events WHERE source_type = 'kafka_replay' AND source_file = '${DEMO_SOURCE_FILE}'")"
  before_silver="$(numeric_trino_value "SELECT count(*) FROM silver_events WHERE source_file = '${DEMO_SOURCE_FILE}'")"
  start_ts="$(date -u +"%Y-%m-%d %H:%M:%S")"

  printf '[STEP 2] Capture before-counts for replay evidence\n'
  log "Before replay: bronze_kafka_replay_rows=${before_bronze} silver_kafka_replay_rows=${before_silver}"
  printf '[STEP 3] Send %s rows into Kafka topic %s\n' "${DEMO_MAX_ROWS}" "${KAFKA_TOPIC_EVENTS}"
  log "Sending ${DEMO_MAX_ROWS} events from ${DEMO_INPUT_CSV} to Kafka topic ${KAFKA_TOPIC_EVENTS}"
  replay_messages "${DEMO_INPUT_CSV}" "${DEMO_MAX_ROWS}"

  printf '[STEP 4] Poll Bronze until replay appears or timeout hits\n'
  max_loops=$(( DEMO_WAIT_SECONDS / DEMO_POLL_SECONDS ))
  if (( max_loops < 1 )); then
    max_loops=1
  fi

  for ((loops=1; loops<=max_loops; loops++)); do
    sleep "${DEMO_POLL_SECONDS}"
    current_bronze="$(numeric_trino_value "SELECT count(*) FROM bronze_events WHERE source_type = 'kafka_replay' AND source_file = '${DEMO_SOURCE_FILE}'")"
    if [[ "${current_bronze}" =~ ^[0-9]+$ ]] && [[ "${before_bronze}" =~ ^[0-9]+$ ]] && (( current_bronze > before_bronze )); then
      log "Replay observed in Bronze after $(( loops * DEMO_POLL_SECONDS ))s"
      break
    fi
  done

  printf '\n[STEP 5] Show before/after row-count proof\n'
  printf '=== BEFORE/AFTER ===\n'
  trino_query "
SELECT 'bronze_kafka_replay_rows_before' AS metric, ${before_bronze} AS value
UNION ALL
SELECT 'silver_kafka_replay_rows_before', ${before_silver}
UNION ALL
SELECT 'bronze_kafka_replay_rows_after', count(*) FROM bronze_events WHERE source_type = 'kafka_replay' AND source_file = '${DEMO_SOURCE_FILE}'
UNION ALL
SELECT 'silver_demo_source_rows_after', count(*) FROM silver_events WHERE source_file = '${DEMO_SOURCE_FILE}'
"

  printf '\n[STEP 6] Show newly ingested Bronze rows since replay start\n'
  printf '=== NEW BRONZE ROWS SINCE REPLAY START ===\n'
  trino_query "
SELECT source_month, count(*) AS rows
FROM bronze_events
WHERE source_type = 'kafka_replay'
  AND source_file = '${DEMO_SOURCE_FILE}'
  AND CAST(ingested_at AS timestamp) >= TIMESTAMP '${start_ts}'
GROUP BY 1
ORDER BY 1
"

  printf '\n[STEP 7] Tail streaming logs for operator proof\n'
  printf '=== STREAMING LOG TAIL ===\n'
  tail_logs
}

case "${MODE}" in
  status)
    show_status
    ;;
  run)
    run_demo
    ;;
  tail)
    tail_logs
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
