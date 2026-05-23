#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

manual_compose() {
  compose --profile core --profile serving "$@"
}

manual_service_cid() {
  manual_compose ps -q "$1"
}

wait_for_http() {
  local url="$1"
  local name="$2"
  for _ in $(seq 1 60); do
    if curl -fsSL "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done

  echo "error: ${name} did not become ready at ${url}" >&2
  exit 1
}

require_running() {
  local service="$1"
  if [[ -z "$(manual_service_cid "${service}")" ]]; then
    echo "error: service ${service} is not running." >&2
    exit 1
  fi
}

if [[ "${MANUAL_FULL_BACKFILL:-0}" != "1" ]]; then
  echo "error: full backfill is intentionally gated." >&2
  echo "run with MANUAL_FULL_BACKFILL=1 FULL_START_MONTH=YYYY-MM FULL_END_MONTH=YYYY-MM bash infra/scripts/run-full-backfill-manual.sh" >&2
  exit 1
fi

FULL_START_MONTH="${FULL_START_MONTH:?set FULL_START_MONTH=YYYY-MM}"
FULL_END_MONTH="${FULL_END_MONTH:?set FULL_END_MONTH=YYYY-MM}"
FULL_INPUT_DIR="${FULL_INPUT_DIR:-/workspace/data/raw}"
FULL_RESUME_FROM="${FULL_RESUME_FROM:-full}"
FULL_RESET_TABLES="${FULL_RESET_TABLES:-0}"

manual_compose up -d

wait_for_http "http://localhost:8080/v1/info" "Trino"

require_running spark-master

docker exec "$(manual_service_cid spark-master)" /opt/spark/bin/spark-submit \
  --master local[2] \
  --driver-memory 1400m \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.io.compression.codec=lzf \
  --conf spark.shuffle.compress=false \
  --conf spark.shuffle.spill.compress=false \
  --conf spark.broadcast.compress=false \
  --conf spark.sql.adaptive.enabled=false \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.local.dir=/opt/spark/work-dir/spark-local \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.postgresql:postgresql:42.7.3 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.defaultCatalog=lakehouse \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=jdbc \
  --conf spark.sql.catalog.lakehouse.uri=jdbc:postgresql://postgres:5432/iceberg \
  --conf spark.sql.catalog.lakehouse.jdbc.user=iceberg \
  --conf spark.sql.catalog.lakehouse.jdbc.password=iceberg \
  --conf spark.sql.catalog.lakehouse.jdbc.driver=org.postgresql.Driver \
  --conf spark.sql.catalog.lakehouse.warehouse=s3://warehouse \
  --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.lakehouse.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.lakehouse.s3.access-key-id=minioadmin \
  --conf spark.sql.catalog.lakehouse.s3.secret-access-key=minioadmin \
  --conf spark.sql.catalog.lakehouse.s3.path-style-access=true \
  --conf spark.sql.catalog.lakehouse.client.region=us-east-1 \
  /workspace/jobs/batch_backfill_to_iceberg.py \
  --input-dir "${FULL_INPUT_DIR}" \
  --start-month "${FULL_START_MONTH}" \
  --end-month "${FULL_END_MONTH}" \
  --resume-from "${FULL_RESUME_FROM}" \
  $( [[ "${FULL_RESET_TABLES}" == "1" ]] && printf '%s' "--reset-tables" )

echo "Manual full backfill completed for ${FULL_START_MONTH}..${FULL_END_MONTH}"
