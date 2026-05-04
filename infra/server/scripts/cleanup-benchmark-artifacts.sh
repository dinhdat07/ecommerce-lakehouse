#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

DRY_RUN=true
OLDER_THAN_DAYS="${BENCHMARK_CLEANUP_OLDER_THAN_DAYS:-7}"
INCLUDE_S3=false
PROFILE="${BENCHMARK_PROFILE:-profile_2month_batch_streaming_sample}"
EXPIRE_INPUT_SAMPLES=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --execute) DRY_RUN=false ;;
    --dry-run) DRY_RUN=true ;;
    --include-s3) INCLUDE_S3=true ;;
    --profile) PROFILE="$2"; shift ;;
    --expire-input-samples) EXPIRE_INPUT_SAMPLES=true ;;
    --older-than-days) OLDER_THAN_DAYS="$2"; shift ;;
    *) fail "unknown argument: $1" ;;
  esac
  shift
done

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
BENCHMARK_ENV_FILE="${BENCHMARK_ENV_FILE:-${SERVER_ENV_DIR}/benchmark.env}"
load_server_env "${STORAGE_ENV_FILE}" "${BENCHMARK_ENV_FILE}"

log "Benchmark cleanup mode: $([[ "${DRY_RUN}" == true ]] && echo dry-run || echo execute), profile=${PROFILE}"
log "Will only target benchmark/temp artifacts; raw data, production Iceberg tables, checkpoints, and Postgres are excluded."
log "Preserved raw files: /srv/ecommerce/raw/2019-Oct.csv.gz /srv/ecommerce/raw/2019-Nov.csv.gz /srv/ecommerce/raw/2019-Dec.csv.gz"
log "Preserved S3 prefixes: s3a://warehouse/demo, s3a://warehouse/benchmarks/input_samples unless --expire-input-samples is passed."
log "Disk usage before cleanup:"
df -h / /srv/ecommerce /srv/ecommerce/spark-tmp /tmp "${BENCHMARK_OUTPUT_ROOT:-/srv/ecommerce/benchmarks}" 2>/dev/null || true
du -xsh /srv/ecommerce /srv/ecommerce/spark-tmp /tmp "${BENCHMARK_OUTPUT_ROOT:-/srv/ecommerce/benchmarks}" 2>/dev/null || true

REMOTE_HOSTS=( ${BENCHMARK_SPARK_CLEANUP_HOSTS:-} )
for host in "${REMOTE_HOSTS[@]}"; do
  log "Remote disk usage before cleanup on ${host}:"
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "df -h / /srv/ecommerce /srv/ecommerce/spark-tmp /tmp 2>/dev/null || true; du -xsh /srv/ecommerce /srv/ecommerce/spark-tmp /tmp 2>/dev/null || true" || true
done

run_or_echo() {
  if [[ "${DRY_RUN}" == true ]]; then
    printf '[dry-run] %q ' "$@"
    printf '\n'
  else
    "$@"
  fi
}

clean_local_glob() {
  local description="$1"
  shift
  log "Target: ${description}"
  "$@" -print 2>/dev/null || true
  if [[ "${DRY_RUN}" == false ]]; then
    "$@" -exec rm -rf -- {} + 2>/dev/null || true
  fi
}

clean_remote_temp() {
  local host="$1"
  log "Target on ${host}: Spark temp/spill and completed worker app dirs only"
  if [[ "${DRY_RUN}" == true ]]; then
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "find /srv/ecommerce/spark-tmp -mindepth 1 -maxdepth 1 ! -name worker -print 2>/dev/null || true; find /srv/ecommerce/spark-tmp/worker -mindepth 1 -maxdepth 1 \( -name 'app-*' -o -name 'spark-*' -o -name 'blockmgr-*' \) -print 2>/dev/null || true; find /tmp -maxdepth 1 \( -name 'spark-*' -o -name 'blockmgr-*' -o -name 'hive-*' \) -print 2>/dev/null || true" || true
  else
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "find /srv/ecommerce/spark-tmp -mindepth 1 -maxdepth 1 ! -name worker -print -exec rm -rf -- {} + 2>/dev/null || true; find /srv/ecommerce/spark-tmp/worker -mindepth 1 -maxdepth 1 \( -name 'app-*' -o -name 'spark-*' -o -name 'blockmgr-*' \) -print -exec rm -rf -- {} + 2>/dev/null || true; find /tmp -maxdepth 1 \( -name 'spark-*' -o -name 'blockmgr-*' -o -name 'hive-*' \) -print -exec rm -rf -- {} + 2>/dev/null || true" || true
  fi
}

# Old benchmark run directories only. Keeps logs and any non-directory files untouched.
if [[ -d "${BENCHMARK_OUTPUT_ROOT:-/srv/ecommerce/benchmarks}" ]]; then
  clean_local_glob "/srv/ecommerce/benchmarks/<old-run> older than ${OLDER_THAN_DAYS} days" \
    find "${BENCHMARK_OUTPUT_ROOT:-/srv/ecommerce/benchmarks}" -mindepth 1 -maxdepth 1 -type d -regextype posix-extended -regex '.*/20[0-9]{6}T[0-9]{6}Z' -mtime "+${OLDER_THAN_DAYS}"
fi

clean_local_glob "/srv/ecommerce/spark-tmp/* except worker state" find /srv/ecommerce/spark-tmp -mindepth 1 -maxdepth 1 ! -name worker
clean_local_glob "/srv/ecommerce/spark-tmp/worker completed app/spill dirs" find /srv/ecommerce/spark-tmp/worker -mindepth 1 -maxdepth 1 \( -name 'app-*' -o -name 'spark-*' -o -name 'blockmgr-*' \)
clean_local_glob "/tmp/spark-* /tmp/blockmgr-* /tmp/hive-*" find /tmp -maxdepth 1 \( -name 'spark-*' -o -name 'blockmgr-*' -o -name 'hive-*' \)

for host in "${REMOTE_HOSTS[@]}"; do
  clean_remote_temp "${host}"
done

if [[ "${INCLUDE_S3}" == true ]]; then
  S3_TARGETS=(
    "warehouse/benchmarks/staging"
    "warehouse/benchmarks/parquet"
    "warehouse/benchmarks/tmp"
    "warehouse/benchmarks/checkpoints/${PROFILE}"
  )
  if [[ "${EXPIRE_INPUT_SAMPLES}" == true ]]; then
    S3_TARGETS+=("warehouse/benchmarks/input_samples/${PROFILE}")
  fi
  log "S3/MinIO benchmark cleanup targets only:"
  printf ' - s3a://%s\n' "${S3_TARGETS[@]}"
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx minio && docker exec minio mc --version >/dev/null 2>&1; then
    docker exec minio mc alias set ecommerce "http://localhost:9000" "${S3_ACCESS_KEY:-minioadmin}" "${S3_SECRET_KEY:-minioadmin}" >/dev/null
    for target in "${S3_TARGETS[@]}"; do
      docker exec minio mc du "ecommerce/${target}" 2>/dev/null || true
      if [[ "${DRY_RUN}" == true ]]; then
        printf '[dry-run] docker exec minio mc rm --recursive --force %q\n' "ecommerce/${target}"
      else
        docker exec minio mc rm --recursive --force "ecommerce/${target}" 2>/dev/null || true
      fi
    done
  elif command -v hadoop >/dev/null 2>&1; then
    for target in "${S3_TARGETS[@]}"; do
      hadoop fs -du -h "s3a://${target}" 2>/dev/null || true
      run_or_echo hadoop fs -rm -r -skipTrash "s3a://${target}" || true
    done
  else
    log "No configured mc/hadoop client found; skipping S3A cleanup."
  fi
else
  log "Skipping s3a://warehouse/benchmarks cleanup; pass --include-s3 to include benchmark Parquet/staging paths."
fi

log "Disk usage after cleanup:"
df -h / /srv/ecommerce /srv/ecommerce/spark-tmp /tmp "${BENCHMARK_OUTPUT_ROOT:-/srv/ecommerce/benchmarks}" 2>/dev/null || true
du -xsh /srv/ecommerce /srv/ecommerce/spark-tmp /tmp "${BENCHMARK_OUTPUT_ROOT:-/srv/ecommerce/benchmarks}" 2>/dev/null || true
for host in "${REMOTE_HOSTS[@]}"; do
  log "Remote disk usage after cleanup on ${host}:"
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "df -h / /srv/ecommerce /srv/ecommerce/spark-tmp /tmp 2>/dev/null || true; du -xsh /srv/ecommerce /srv/ecommerce/spark-tmp /tmp 2>/dev/null || true" || true
done
