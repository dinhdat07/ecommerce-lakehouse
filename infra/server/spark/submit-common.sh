#!/usr/bin/env bash
set -euo pipefail

SPARK_TEMPLATE_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_ROOT_DIR="$(cd -- "${SPARK_TEMPLATE_DIR}/.." && pwd)"
ROOT_DIR="$(cd -- "${SERVER_ROOT_DIR}/../.." && pwd)"

normalize_warehouse_uri() {
  printf '%s\n' "${ICEBERG_WAREHOUSE:-s3://warehouse}" | sed 's#^s3a://#s3://#'
}

spark_submit_base_args() {
  local packages="$1"
  local warehouse_uri
  warehouse_uri="$(normalize_warehouse_uri)"
  SPARK_SUBMIT_ARGS=(
    "${SPARK_SUBMIT_BIN:-spark-submit}"
    --master "${SPARK_MASTER}"
    --deploy-mode "${SPARK_DEPLOY_MODE:-client}"
    --driver-memory "${SPARK_DRIVER_MEMORY:-2g}"
    --conf "spark.jars.ivy=${SPARK_IVY_DIR:-/tmp/.ivy2}"
    --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-8}"
    --conf "spark.sql.session.timeZone=UTC"
    --conf "spark.local.dir=${SPARK_LOCAL_DIR:-/tmp}"
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
    --packages "${packages}"
  )
  if [[ -n "${SPARK_EXECUTOR_MEMORY:-}" ]]; then
    SPARK_SUBMIT_ARGS+=(--conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}")
  fi
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
