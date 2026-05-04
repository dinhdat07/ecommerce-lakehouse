#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
# shellcheck source=infra/server/spark/submit-common.sh
source "${SERVER_ROOT_DIR}/spark/submit-common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
BENCHMARK_ENV_FILE="${BENCHMARK_ENV_FILE:-${SERVER_ENV_DIR}/benchmark.env}"
load_server_env "${STORAGE_ENV_FILE}" "${BENCHMARK_ENV_FILE}"

RUN_LABEL="${BENCHMARK_RUN_LABEL:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUTPUT_JSON="${1:-${BENCHMARK_OUTPUT_ROOT}/maintenance-${RUN_LABEL}.json}"
shift $(( $# > 0 ? 1 : 0 ))
EXTRA_ARGS=("$@")
if [[ " ${EXTRA_ARGS[*]} " == *" --execute-benchmark-maintenance "* ]]; then
  log "Executing Iceberg maintenance for benchmark tables only; production tables are refused by the job."
else
  log "Planning Iceberg maintenance dry-run only; no rewrite/expire/remove procedure will be executed."
fi
spark_submit_iceberg_job "${ROOT_DIR}/infra/jobs/iceberg_maintenance_report.py" \
  --include-benchmark-tables \
  --output-json "${OUTPUT_JSON}" \
  "${EXTRA_ARGS[@]}"
log "Maintenance plan written to ${OUTPUT_JSON}"
