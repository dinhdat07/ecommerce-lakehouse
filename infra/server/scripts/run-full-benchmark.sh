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
RUN_ROOT="${BENCHMARK_OUTPUT_ROOT}/${RUN_LABEL}"
mkdir -p "${RUN_ROOT}"

spark_submit_iceberg_job "${ROOT_DIR}/infra/jobs/benchmark_system_lakehouse_vs_parquet.py" \
  --mode "${BENCHMARK_MODE:-full}" \
  --input-dir "${BENCHMARK_INPUT_DIR}" \
  --start-month 2019-10 \
  --end-month 2020-02 \
  --warehouse-uri "${ICEBERG_WAREHOUSE}" \
  --parquet-warehouse "${BENCHMARK_PARQUET_WAREHOUSE:-${RUN_ROOT}/parquet}" \
  --output-json "${RUN_ROOT}/system_benchmark.json" \
  --output-csv "${RUN_ROOT}/system_benchmark.csv"

spark_submit_iceberg_job "${ROOT_DIR}/infra/jobs/benchmark_batch_vs_streaming.py" \
  --mode "${BENCHMARK_MODE:-full}" \
  --input-dir "${STREAM_BENCHMARK_INPUT_DIR:-${BENCHMARK_INPUT_DIR}}" \
  --start-month 2020-03 \
  --end-month 2020-04 \
  --microbatch-size "${STREAM_BENCHMARK_MICROBATCH_SIZE:-50000}" \
  --output-json "${RUN_ROOT}/processing_model_benchmark.json" \
  --output-csv "${RUN_ROOT}/processing_model_benchmark.csv"

python3 - <<'PY' "${RUN_ROOT}/metadata.json" "${RUN_LABEL}" "${BENCHMARK_MODE:-full}" "${BENCHMARK_INPUT_DIR}" "${ICEBERG_WAREHOUSE}"
import json
import sys
from datetime import datetime, timezone

path, run_label, mode, input_dir, warehouse = sys.argv[1:]
with open(path, "w", encoding="utf-8") as handle:
    json.dump(
        {
            "run_label": run_label,
            "benchmark_mode": mode,
            "input_dir": input_dir,
            "iceberg_warehouse": warehouse,
            "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        handle,
        indent=2,
        sort_keys=True,
    )
PY

log "Full benchmark artifacts written to ${RUN_ROOT}"
