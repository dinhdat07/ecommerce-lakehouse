#!/usr/bin/env bash
set -euo pipefail

SPARK_TEMPLATE_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_ROOT_DIR="$(cd -- "${SPARK_TEMPLATE_DIR}/.." && pwd)"
ROOT_DIR="$(cd -- "${SERVER_ROOT_DIR}/../.." && pwd)"

normalize_warehouse_uri() {
  printf '%s\n' "${ICEBERG_WAREHOUSE:-s3://warehouse}" | sed 's#^s3a://#s3://#'
}

spark_packages_csv() {
  local packages="$1"
  if [[ -n "${SPARK_EXTRA_PACKAGES:-}" ]]; then
    printf '%s,%s\n' "${packages}" "${SPARK_EXTRA_PACKAGES}"
    return
  fi
  printf '%s\n' "${packages}"
}

append_optional_conf() {
  local conf_key="$1"
  local conf_value="${2:-}"
  if [[ -n "${conf_value}" ]]; then
    SPARK_SUBMIT_ARGS+=(--conf "${conf_key}=${conf_value}")
  fi
}

spark_submit_command_args() {
  local spark_local_dir="$1"
  if [[ -n "${SPARK_SUBMIT_DOCKER_CONTAINER:-}" ]]; then
    SPARK_SUBMIT_COMMAND=(
      docker exec
      -e "SPARK_LOCAL_DIRS=${spark_local_dir}"
      -e "SPARK_LOCAL_DIR=${spark_local_dir}"
	      -e "S3_ENDPOINT=${S3_ENDPOINT:-}"
	      -e "GIT_COMMIT=${GIT_COMMIT:-}"
	      -e "BENCHMARK_CACHE_DATAFRAMES=${BENCHMARK_CACHE_DATAFRAMES:-false}"
	      -e "BENCHMARK_CACHE_STORAGE_LEVEL=${BENCHMARK_CACHE_STORAGE_LEVEL:-DISK_ONLY}"
	      -e "BENCHMARK_WRITE_PARTITIONS=${BENCHMARK_WRITE_PARTITIONS:-}"
	      -e "BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS=${BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS:-}"
	      -e "BENCHMARK_STAGING_WRITE_PARTITIONS=${BENCHMARK_STAGING_WRITE_PARTITIONS:-}"
	      "${SPARK_SUBMIT_DOCKER_CONTAINER}"
      "${SPARK_SUBMIT_CONTAINER_BIN:-/opt/spark/bin/spark-submit}"
    )
    return
  fi
  SPARK_SUBMIT_COMMAND=("${SPARK_SUBMIT_BIN:-spark-submit}")
}

spark_submit_base_args() {
  local packages="$1"
  local warehouse_uri
  local packages_csv
  local s3a_endpoint
  local s3a_ssl_enabled="true"
  local spark_local_dir="${SPARK_LOCAL_DIR:-/srv/ecommerce/spark-tmp}"
  warehouse_uri="$(normalize_warehouse_uri)"
  packages_csv="$(spark_packages_csv "${packages}")"
  s3a_endpoint="${S3_ENDPOINT#http://}"
  s3a_endpoint="${s3a_endpoint#https://}"
  if [[ "${S3_ENDPOINT}" == http://* ]]; then
    s3a_ssl_enabled="false"
  fi
  mkdir -p "${spark_local_dir}" 2>/dev/null || true
  export SPARK_LOCAL_DIRS="${spark_local_dir}"
  export SPARK_LOCAL_DIR="${spark_local_dir}"
  spark_submit_command_args "${spark_local_dir}"
  SPARK_SUBMIT_ARGS=(
    "${SPARK_SUBMIT_COMMAND[@]}"
    --master "${SPARK_MASTER}"
    --deploy-mode "${SPARK_DEPLOY_MODE:-client}"
    --driver-memory "${SPARK_DRIVER_MEMORY:-2g}"
    --conf "spark.jars.ivy=${SPARK_IVY_DIR:-/tmp/.ivy2}"
    --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-8}"
    --conf "spark.sql.session.timeZone=UTC"
    --conf "spark.local.dir=${spark_local_dir}"
    --conf "spark.driverEnv.SPARK_LOCAL_DIRS=${spark_local_dir}"
    --conf "spark.driverEnv.SPARK_LOCAL_DIR=${spark_local_dir}"
	    --conf "spark.driverEnv.S3_ENDPOINT=${S3_ENDPOINT:-}"
	    --conf "spark.driverEnv.GIT_COMMIT=${GIT_COMMIT:-}"
	    --conf "spark.driverEnv.BENCHMARK_CACHE_DATAFRAMES=${BENCHMARK_CACHE_DATAFRAMES:-false}"
	    --conf "spark.driverEnv.BENCHMARK_CACHE_STORAGE_LEVEL=${BENCHMARK_CACHE_STORAGE_LEVEL:-DISK_ONLY}"
	    --conf "spark.driverEnv.BENCHMARK_WRITE_PARTITIONS=${BENCHMARK_WRITE_PARTITIONS:-}"
	    --conf "spark.driverEnv.BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS=${BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS:-}"
	    --conf "spark.driverEnv.BENCHMARK_STAGING_WRITE_PARTITIONS=${BENCHMARK_STAGING_WRITE_PARTITIONS:-}"
    --conf "spark.executorEnv.SPARK_LOCAL_DIRS=${spark_local_dir}"
    --conf "spark.executorEnv.SPARK_LOCAL_DIR=${spark_local_dir}"
    --conf "spark.executorEnv.S3_ENDPOINT=${S3_ENDPOINT:-}"
    --conf "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM:-8}"
    --conf "spark.sql.adaptive.enabled=${SPARK_SQL_ADAPTIVE_ENABLED:-true}"
    --conf "spark.sql.adaptive.coalescePartitions.enabled=${SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED:-true}"
    --conf "spark.sql.adaptive.advisoryPartitionSizeInBytes=${SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE:-32m}"
    --conf "spark.sql.files.maxPartitionBytes=${SPARK_SQL_FILES_MAX_PARTITION_BYTES:-64m}"
    --conf "spark.sql.files.openCostInBytes=${SPARK_SQL_FILES_OPEN_COST_BYTES:-4194304}"
    --conf "spark.sql.iceberg.advisory-partition-size=${SPARK_SQL_ICEBERG_ADVISORY_PARTITION_SIZE:-134217728}"
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    --conf "spark.sql.defaultCatalog=${ICEBERG_CATALOG}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.type=${ICEBERG_CATALOG_TYPE}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.uri=${ICEBERG_CATALOG_URI}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.jdbc.user=${ICEBERG_CATALOG_USER}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.jdbc.password=${ICEBERG_CATALOG_PASSWORD}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.jdbc.driver=${ICEBERG_CATALOG_DRIVER}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=${warehouse_uri}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.s3.endpoint=${S3_ENDPOINT}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.s3.access-key-id=${S3_ACCESS_KEY}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.s3.secret-access-key=${S3_SECRET_KEY}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.s3.path-style-access=${S3_PATH_STYLE_ACCESS}"
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.client.region=${S3_REGION}"
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
    --conf "spark.hadoop.fs.s3a.endpoint=${s3a_endpoint}"
    --conf "spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY}"
    --conf "spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY}"
    --conf "spark.hadoop.fs.s3a.path.style.access=${S3_PATH_STYLE_ACCESS}"
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=${s3a_ssl_enabled}"
    --packages "${packages_csv}"
  )
  append_optional_conf "spark.driver.host" "${SPARK_DRIVER_HOST:-}"
  append_optional_conf "spark.driver.bindAddress" "${SPARK_DRIVER_BIND_ADDRESS:-}"
  if [[ -n "${SPARK_EXECUTOR_MEMORY:-}" ]]; then
    SPARK_SUBMIT_ARGS+=(--conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}")
  fi
  append_optional_conf "spark.executor.cores" "${SPARK_EXECUTOR_CORES:-}"
  append_optional_conf "spark.driver.maxResultSize" "${SPARK_DRIVER_MAX_RESULT_SIZE:-}"
  append_optional_conf "spark.sql.parquet.enableVectorizedReader" "${SPARK_SQL_PARQUET_VECTORIZED_READER:-}"
  append_optional_conf "spark.sql.parquet.filterPushdown" "${SPARK_SQL_PARQUET_FILTER_PUSHDOWN:-}"
  append_optional_conf "spark.sql.catalog.${ICEBERG_CATALOG}.write.distribution-mode" "${ICEBERG_WRITE_DISTRIBUTION_MODE:-}"
  append_optional_conf "spark.sql.catalog.${ICEBERG_CATALOG}.write.target-file-size-bytes" "${ICEBERG_WRITE_TARGET_FILE_SIZE_BYTES:-}"
}

spark_submit_iceberg_job() {
  local app_path="$1"
  shift
  spark_submit_base_args "${SPARK_ICEBERG_PACKAGES}"
  "${SPARK_SUBMIT_ARGS[@]}" "${app_path}" "$@"
}

spark_submit_streaming_job() {
  local app_path="$1"
  shift
  spark_submit_base_args "${SPARK_KAFKA_PACKAGE},${SPARK_ICEBERG_PACKAGES}"
  "${SPARK_SUBMIT_ARGS[@]}" "${app_path}" "$@"
}
