#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

BENCHMARK_ENV_FILE="${BENCHMARK_ENV_FILE:-${SERVER_ENV_DIR}/benchmark.env}"
load_server_env "${BENCHMARK_ENV_FILE}"

RUN_LABEL="${1:-latest}"
if [[ "${RUN_LABEL}" == "latest" ]]; then
  RUN_DIR="$(find "${BENCHMARK_OUTPUT_ROOT}" -mindepth 1 -maxdepth 1 -type d | sort | tail -n 1)"
else
  RUN_DIR="${BENCHMARK_OUTPUT_ROOT}/${RUN_LABEL}"
fi
[[ -d "${RUN_DIR}" ]] || fail "benchmark run directory not found: ${RUN_DIR}"

ARCHIVE_PATH="${RUN_DIR}.tar.gz"
tar -czf "${ARCHIVE_PATH}" -C "$(dirname "${RUN_DIR}")" "$(basename "${RUN_DIR}")"
log "Collected benchmark archive: ${ARCHIVE_PATH}"
