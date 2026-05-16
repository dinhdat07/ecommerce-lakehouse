#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

DRY_RUN=true
OLDER_THAN_DAYS="${BENCHMARK_CLEANUP_OLDER_THAN_DAYS:-7}"
INCLUDE_S3=false
PROFILE="${BENCHMARK_PROFILE:-profile_2month_batch_streaming_sample}"
RUN_LABEL="${BENCHMARK_RUN_LABEL:-}"
STAGING_SUFFIX="${BENCHMARK_CLEANUP_STAGING_SUFFIX:-}"
EXPIRE_INPUT_SAMPLES=false
INCLUDE_BENCHMARK_ICEBERG=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --execute) DRY_RUN=false ;;
    --dry-run) DRY_RUN=true ;;
    --include-s3) INCLUDE_S3=true ;;
    --profile) PROFILE="$2"; shift ;;
    --run-label) RUN_LABEL="$2"; shift ;;
    --staging-suffix) STAGING_SUFFIX="$2"; shift ;;
    --expire-input-samples) EXPIRE_INPUT_SAMPLES=true ;;
    --include-benchmark-iceberg) INCLUDE_BENCHMARK_ICEBERG=true ;;
    --older-than-days) OLDER_THAN_DAYS="$2"; shift ;;
    *) fail "unknown argument: $1" ;;
  esac
  shift
done

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
BENCHMARK_ENV_FILE="${BENCHMARK_ENV_FILE:-${SERVER_ENV_DIR}/benchmark.env}"
load_server_env "${STORAGE_ENV_FILE}" "${BENCHMARK_ENV_FILE}"

log "Benchmark cleanup mode: $([[ "${DRY_RUN}" == true ]] && echo dry-run || echo execute), profile=${PROFILE}"
log "Will only target benchmark/temp artifacts; raw data and production Iceberg tables are excluded."
log "Preserved raw files: /srv/ecommerce/raw/2019-Oct.csv.gz /srv/ecommerce/raw/2019-Nov.csv.gz /srv/ecommerce/raw/2019-Dec.csv.gz"
log "Preserved S3 prefixes: s3a://warehouse/benchmarks/input_samples unless --expire-input-samples is passed."
if [[ -n "${RUN_LABEL}" ]]; then
  log "Run-scoped S3 cleanup enabled for run_label=${RUN_LABEL}."
  if [[ -n "${STAGING_SUFFIX}" ]]; then
    log "Staging cleanup is limited to suffix=${STAGING_SUFFIX}."
  fi
else
  log "No --run-label passed; S3 benchmark cleanup targets all benchmark staging/parquet/tmp prefixes."
fi
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
  if [[ -n "${RUN_LABEL}" ]]; then
    staging_target="warehouse/benchmarks/staging/${RUN_LABEL}"
    if [[ -n "${STAGING_SUFFIX}" ]]; then
      staging_target="${staging_target}/${STAGING_SUFFIX}"
    fi
    S3_TARGETS=(
      "${staging_target}"
      "warehouse/benchmarks/parquet/${RUN_LABEL}"
      "warehouse/benchmarks/tmp/${RUN_LABEL}"
      "warehouse/benchmarks/checkpoints/${PROFILE}/${RUN_LABEL}"
    )
  else
    S3_TARGETS=(
      "warehouse/benchmarks/staging"
      "warehouse/benchmarks/parquet"
      "warehouse/benchmarks/tmp"
      "warehouse/benchmarks/checkpoints/${PROFILE}"
    )
  fi
  if [[ "${EXPIRE_INPUT_SAMPLES}" == true ]]; then
    S3_TARGETS+=("warehouse/benchmarks/input_samples/${PROFILE}")
  fi
  if [[ "${INCLUDE_BENCHMARK_ICEBERG}" == true ]]; then
    S3_TARGETS+=(
      "warehouse/demo/bench_bronze_events"
      "warehouse/demo/bench_silver_events"
      "warehouse/demo/bench_daily_revenue"
      "warehouse/demo/bench_top_products"
      "warehouse/demo/bench_conversion_funnel_daily"
      "warehouse/demo/bench_category_performance_daily"
      "warehouse/demo/bench_session_funnel"
      "warehouse/demo/bench_user_conversion_path"
      "warehouse/demo/bench_cohort_retention"
      "warehouse/demo/bench_repeat_purchase"
      "warehouse/demo/bench_product_affinity"
      "warehouse/demo/bench_time_to_conversion_distribution"
      "warehouse/demo/bench_rfm_segmentation"
    )
  fi
  log "S3/MinIO benchmark cleanup targets only:"
  printf ' - s3a://%s\n' "${S3_TARGETS[@]}"
  MINIO_CONTAINER=""
  for candidate in server-minio minio; do
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${candidate}" && docker exec "${candidate}" mc --version >/dev/null 2>&1; then
      MINIO_CONTAINER="${candidate}"
      break
    fi
  done
  if [[ -n "${MINIO_CONTAINER}" ]]; then
    docker exec "${MINIO_CONTAINER}" mc alias set ecommerce "${S3_ENDPOINT:-http://100.123.190.84:9100}" "${S3_ACCESS_KEY:-minioadmin}" "${S3_SECRET_KEY:-minioadmin}" >/dev/null
    for target in "${S3_TARGETS[@]}"; do
      docker exec "${MINIO_CONTAINER}" mc du "ecommerce/${target}" 2>/dev/null || true
      if [[ "${DRY_RUN}" == true ]]; then
        printf '[dry-run] docker exec %q mc rm --recursive --force %q\n' "${MINIO_CONTAINER}" "ecommerce/${target}"
      else
        docker exec "${MINIO_CONTAINER}" mc rm --recursive --force "ecommerce/${target}" 2>/dev/null || true
      fi
    done
    if [[ "${INCLUDE_BENCHMARK_ICEBERG}" == true ]]; then
      log "Target: JDBC Iceberg catalog rows for demo.bench_* only"
      if [[ "${DRY_RUN}" == true ]]; then
        printf '[dry-run] docker exec server-postgres psql -U iceberg -d iceberg -c %q\n' "delete from iceberg_tables where catalog_name='lakehouse' and table_namespace='demo' and table_name like 'bench_%';"
      elif docker ps --format '{{.Names}}' 2>/dev/null | grep -qx server-postgres; then
        docker exec server-postgres psql -U iceberg -d iceberg -c "delete from iceberg_tables where catalog_name='lakehouse' and table_namespace='demo' and table_name like 'bench_%';" || true
      fi
    fi
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
