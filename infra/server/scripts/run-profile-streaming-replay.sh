#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
# shellcheck source=infra/server/spark/submit-common.sh
source "${SERVER_ROOT_DIR}/spark/submit-common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
BENCHMARK_ENV_FILE="${BENCHMARK_ENV_FILE:-${SERVER_ENV_DIR}/benchmark.env}"

# Preserve caller overrides before loading env files; the env files contain
# conservative defaults that should not clobber a disk-safe benchmark invocation.
REQUESTED_BENCHMARK_PROFILE="${BENCHMARK_PROFILE:-}"
REQUESTED_BENCHMARK_RUN_LABEL="${BENCHMARK_RUN_LABEL:-}"
REQUESTED_STREAM_START_MONTH="${STREAM_START_MONTH:-}"
REQUESTED_STREAM_END_MONTH="${STREAM_END_MONTH:-}"
REQUESTED_STREAM_BENCHMARK_SAMPLE_FRACTION="${STREAM_BENCHMARK_SAMPLE_FRACTION:-}"
REQUESTED_PROFILE_2MONTH_STREAM_SAMPLE_FRACTION="${PROFILE_2MONTH_STREAM_SAMPLE_FRACTION:-}"
REQUESTED_STREAM_BENCHMARK_MICROBATCH_SIZE="${STREAM_BENCHMARK_MICROBATCH_SIZE:-}"
REQUESTED_BENCHMARK_DISK_FREE_THRESHOLD_GB="${BENCHMARK_DISK_FREE_THRESHOLD_GB:-}"
REQUESTED_SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-}"
REQUESTED_SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-}"
REQUESTED_SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED="${SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED:-}"
REQUESTED_BENCHMARK_WRITE_PARTITIONS="${BENCHMARK_WRITE_PARTITIONS:-}"
REQUESTED_BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS="${BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS:-}"
REQUESTED_PROCESSING_GOLD_REFRESH_MODE="${PROCESSING_GOLD_REFRESH_MODE:-}"
REQUESTED_BENCHMARK_CACHE_DATAFRAMES="${BENCHMARK_CACHE_DATAFRAMES:-}"

load_server_env "${STORAGE_ENV_FILE}" "${BENCHMARK_ENV_FILE}"

[[ -n "${REQUESTED_BENCHMARK_PROFILE}" ]] && BENCHMARK_PROFILE="${REQUESTED_BENCHMARK_PROFILE}"
[[ -n "${REQUESTED_BENCHMARK_RUN_LABEL}" ]] && BENCHMARK_RUN_LABEL="${REQUESTED_BENCHMARK_RUN_LABEL}"
[[ -n "${REQUESTED_STREAM_START_MONTH}" ]] && STREAM_START_MONTH="${REQUESTED_STREAM_START_MONTH}"
[[ -n "${REQUESTED_STREAM_END_MONTH}" ]] && STREAM_END_MONTH="${REQUESTED_STREAM_END_MONTH}"
[[ -n "${REQUESTED_STREAM_BENCHMARK_SAMPLE_FRACTION}" ]] && STREAM_BENCHMARK_SAMPLE_FRACTION="${REQUESTED_STREAM_BENCHMARK_SAMPLE_FRACTION}"
[[ -n "${REQUESTED_PROFILE_2MONTH_STREAM_SAMPLE_FRACTION}" ]] && PROFILE_2MONTH_STREAM_SAMPLE_FRACTION="${REQUESTED_PROFILE_2MONTH_STREAM_SAMPLE_FRACTION}"
[[ -n "${REQUESTED_STREAM_BENCHMARK_MICROBATCH_SIZE}" ]] && STREAM_BENCHMARK_MICROBATCH_SIZE="${REQUESTED_STREAM_BENCHMARK_MICROBATCH_SIZE}"
[[ -n "${REQUESTED_BENCHMARK_DISK_FREE_THRESHOLD_GB}" ]] && BENCHMARK_DISK_FREE_THRESHOLD_GB="${REQUESTED_BENCHMARK_DISK_FREE_THRESHOLD_GB}"
[[ -n "${REQUESTED_SPARK_SQL_SHUFFLE_PARTITIONS}" ]] && SPARK_SQL_SHUFFLE_PARTITIONS="${REQUESTED_SPARK_SQL_SHUFFLE_PARTITIONS}"
[[ -n "${REQUESTED_SPARK_DEFAULT_PARALLELISM}" ]] && SPARK_DEFAULT_PARALLELISM="${REQUESTED_SPARK_DEFAULT_PARALLELISM}"
[[ -n "${REQUESTED_SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED}" ]] && SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED="${REQUESTED_SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED}"
[[ -n "${REQUESTED_BENCHMARK_WRITE_PARTITIONS}" ]] && BENCHMARK_WRITE_PARTITIONS="${REQUESTED_BENCHMARK_WRITE_PARTITIONS}"
[[ -n "${REQUESTED_BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS}" ]] && BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS="${REQUESTED_BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS}"
[[ -n "${REQUESTED_PROCESSING_GOLD_REFRESH_MODE}" ]] && PROCESSING_GOLD_REFRESH_MODE="${REQUESTED_PROCESSING_GOLD_REFRESH_MODE}"
[[ -n "${REQUESTED_BENCHMARK_CACHE_DATAFRAMES}" ]] && BENCHMARK_CACHE_DATAFRAMES="${REQUESTED_BENCHMARK_CACHE_DATAFRAMES}"

BENCHMARK_PROFILE="${BENCHMARK_PROFILE:-profile_2month_batch_streaming_sample}"
RUN_LABEL="${BENCHMARK_RUN_LABEL:-stream-dec-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${BENCHMARK_OUTPUT_ROOT:-/srv/ecommerce/benchmarks}/${RUN_LABEL}"
mkdir -p "${RUN_ROOT}"

STREAM_START_MONTH="${STREAM_START_MONTH:-${STREAM_BENCHMARK_START_MONTH:-2019-12}}"
STREAM_END_MONTH="${STREAM_END_MONTH:-${STREAM_BENCHMARK_END_MONTH:-2019-12}}"
STREAM_BENCHMARK_SAMPLE_FRACTION="${STREAM_BENCHMARK_SAMPLE_FRACTION:-${PROFILE_2MONTH_STREAM_SAMPLE_FRACTION:-0.10}}"
STREAM_BENCHMARK_MICROBATCH_SIZE="${STREAM_BENCHMARK_MICROBATCH_SIZE:-250000}"
BENCHMARK_INPUT_SAMPLE_ROOT="${BENCHMARK_INPUT_SAMPLE_ROOT:-s3a://warehouse/benchmarks/input_samples}"
BENCHMARK_STAGING_ROOT="${BENCHMARK_STAGING_ROOT:-s3a://warehouse/benchmarks/staging/${RUN_LABEL}}"
STREAM_BENCHMARK_CHECKPOINT_LOCATION="${STREAM_BENCHMARK_CHECKPOINT_LOCATION:-s3a://warehouse/benchmarks/checkpoints/${BENCHMARK_PROFILE}/${RUN_LABEL}/streaming_replay}"
BENCHMARK_DISK_FREE_THRESHOLD_GB="${BENCHMARK_DISK_FREE_THRESHOLD_GB:-20}"
BENCHMARK_REQUIRE_SPARK_LOCAL_DIRS="${BENCHMARK_REQUIRE_SPARK_LOCAL_DIRS:-true}"

check_free_space() {
  local host="$1"
  local free_gb
  local py='import os; s=os.statvfs("/"); print(int((s.f_bavail*s.f_frsize)//1024//1024//1024))'
  if [[ "${host}" == "local" ]]; then
    free_gb="$(python3 -c "${py}")"
  else
    free_gb="$(ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "python3 -c '${py}'" 2>/dev/null || echo 0)"
  fi
  if [[ "${free_gb}" =~ ^[0-9]+$ && "${free_gb}" -lt "${BENCHMARK_DISK_FREE_THRESHOLD_GB}" ]]; then
    fail "${host} has ${free_gb}GB free, below BENCHMARK_DISK_FREE_THRESHOLD_GB=${BENCHMARK_DISK_FREE_THRESHOLD_GB}"
  fi
}

spark_local_dirs_match() {
  local actual="$1"
  local expected="$2"
  [[ ",${actual}," == *",${expected},"* ]]
}

check_spark_local_dirs() {
  local host="$1"
  local expected="${SPARK_LOCAL_DIR:-/srv/ecommerce/spark-tmp}"
  local actual=""
  if [[ "${host}" == "local" ]]; then
    actual="$(docker exec spark-worker sh -lc 'printf "%s" "${SPARK_LOCAL_DIRS:-}"' 2>/dev/null || true)"
  else
    actual="$(ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "docker exec spark-worker sh -lc 'printf \"%s\" \"\${SPARK_LOCAL_DIRS:-}\"'" 2>/dev/null || true)"
  fi
  if spark_local_dirs_match "${actual}" "${expected}"; then
    log "Spark worker on ${host} has SPARK_LOCAL_DIRS=${actual}"
    return 0
  fi
  log "Spark worker on ${host} has SPARK_LOCAL_DIRS=${actual:-<unset>}; expected ${expected}. Standalone executors may otherwise spill to /tmp."
  if [[ "${BENCHMARK_REQUIRE_SPARK_LOCAL_DIRS}" == "true" ]]; then
    return 1
  fi
  return 0
}

log "Streaming replay profile=${BENCHMARK_PROFILE} run_id=${RUN_LABEL} months=${STREAM_START_MONTH}..${STREAM_END_MONTH} sample_fraction=${STREAM_BENCHMARK_SAMPLE_FRACTION}"
df -h / /srv/ecommerce /srv/ecommerce/spark-tmp /tmp 2>/dev/null || true
check_free_space local
check_spark_local_dirs local || fail "Spark worker local dirs are not pinned to ${SPARK_LOCAL_DIR:-/srv/ecommerce/spark-tmp}"
for host in ${BENCHMARK_SPARK_CLEANUP_HOSTS:-}; do
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "df -h / /srv/ecommerce /srv/ecommerce/spark-tmp /tmp 2>/dev/null || true" || true
  check_free_space "${host}"
  check_spark_local_dirs "${host}" || fail "Spark worker local dirs are not pinned to ${SPARK_LOCAL_DIR:-/srv/ecommerce/spark-tmp} on ${host}"
done

export BENCHMARK_PROFILE BENCHMARK_RUN_ID="${RUN_LABEL}" BENCHMARK_CACHE_DATAFRAMES="${BENCHMARK_CACHE_DATAFRAMES:-false}" GIT_COMMIT="${GIT_COMMIT:-$(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || echo unknown)}"

spark_submit_iceberg_job "${ROOT_DIR}/infra/jobs/benchmark_batch_vs_streaming.py" \
  --mode "${BENCHMARK_MODE:-full}" \
  --input-dir "${STREAM_BENCHMARK_INPUT_DIR:-${BENCHMARK_INPUT_DIR:-/srv/ecommerce/raw}}" \
  --sample-file "${STREAM_BENCHMARK_SAMPLE_FILE:-/opt/ecommerce-lakehouse/data/sample/events_streaming_demo_1500.csv}" \
  --start-month "${STREAM_START_MONTH}" \
  --end-month "${STREAM_END_MONTH}" \
  --subset-enabled true \
  --subset-mode fraction \
  --sample-fraction "${STREAM_BENCHMARK_SAMPLE_FRACTION}" \
  --sample-seed "${BENCHMARK_SAMPLE_SEED:-42}" \
  --staging-enabled "${BENCHMARK_STAGING_ENABLED:-true}" \
  --staging-path "${BENCHMARK_STAGING_ROOT}/streaming_replay_input" \
  --input-sample-enabled "${BENCHMARK_INPUT_SAMPLE_ENABLED:-true}" \
  --input-sample-root "${BENCHMARK_INPUT_SAMPLE_ROOT}" \
  --microbatch-size "${STREAM_BENCHMARK_MICROBATCH_SIZE}" \
  --gold-refresh-mode "${PROCESSING_GOLD_REFRESH_MODE:-affected_dates}" \
  --batch-enabled false \
  --streaming-checkpoint-location "${STREAM_BENCHMARK_CHECKPOINT_LOCATION}" \
  --profile "${BENCHMARK_PROFILE}" \
  --run-id "${RUN_LABEL}" \
  --output-json "${RUN_ROOT}/streaming_replay_benchmark.json" \
  --output-csv "${RUN_ROOT}/streaming_replay_benchmark.csv"

log "Streaming replay artifacts written to ${RUN_ROOT}"
df -h / /srv/ecommerce /srv/ecommerce/spark-tmp /tmp 2>/dev/null || true
