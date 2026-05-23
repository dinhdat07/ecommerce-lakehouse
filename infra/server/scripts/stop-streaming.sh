#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

load_server_env
STREAM_PID_FILE="${STREAM_PID_FILE:-${SERVER_RUNTIME_DIR}/streaming/${STREAM_QUERY_NAME}.pid}"
SPARK_KILL_CONTAINER="${SPARK_KILL_CONTAINER:-spark-master}"

active_streaming_apps() {
  python3 - <<'PY'
import json
import urllib.request

url = "http://127.0.0.1:8080/json/"
try:
    payload = json.load(urllib.request.urlopen(url, timeout=5))
except Exception:
    raise SystemExit(0)

for app in payload.get("activeapps", []):
    name = str(app.get("name", ""))
    if name.endswith("-streaming"):
        print(app.get("id", ""))
PY
}

if [[ "${STREAM_USE_SYSTEMD:-0}" == "1" ]]; then
  sudo systemctl stop "${STREAM_SERVICE_NAME:-ecommerce-streaming.service}"
  log "Streaming systemd service stopped."
  exit 0
fi

if pid_is_running "${STREAM_PID_FILE}"; then
  kill "$(cat "${STREAM_PID_FILE}")"
  rm -f "${STREAM_PID_FILE}"
  log "Streaming process stopped."
fi

rm -f "${STREAM_PID_FILE}"

mapfile -t stream_pids < <(pgrep -f "python3 ${ROOT_DIR}/infra/jobs/kafka_stream_to_iceberg.py" || true)
if [[ "${#stream_pids[@]}" -gt 0 ]]; then
  kill "${stream_pids[@]}" >/dev/null 2>&1 || true
fi

mapfile -t submit_pids < <(pgrep -f "org.apache.spark.deploy.SparkSubmit.*kafka_stream_to_iceberg.py" || true)
if [[ "${#submit_pids[@]}" -gt 0 ]]; then
  kill "${submit_pids[@]}" >/dev/null 2>&1 || true
fi

mapfile -t active_apps < <(active_streaming_apps)
if [[ "${#active_apps[@]}" -gt 0 ]]; then
  for app_id in "${active_apps[@]}"; do
    [[ -n "${app_id}" ]] || continue
    docker exec "${SPARK_KILL_CONTAINER}" /opt/spark/bin/spark-class org.apache.spark.deploy.Client kill "${SPARK_MASTER}" "${app_id}" >/dev/null
    log "Requested Spark master to kill application ${app_id}."
  done
  exit 0
fi

log "No running streaming pid file or active Spark streaming application was found."
