#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

sample_compose() {
  compose --profile core --profile serving "$@"
}

sample_service_cid() {
  sample_compose ps -q "$1"
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
  if [[ -z "$(sample_service_cid "${service}")" ]]; then
    echo "error: service ${service} is not running." >&2
    exit 1
  fi
}

DEMO_SAMPLE_ROWS="${DEMO_SAMPLE_ROWS:-100000}"
DEMO_SOURCE_MONTH="${DEMO_SOURCE_MONTH:-2020-04}"
DEMO_SAMPLE_FILE="${DEMO_SAMPLE_FILE:-data/sample/events_demo_${DEMO_SAMPLE_ROWS}.csv}"
DEMO_REGENERATE="${DEMO_REGENERATE:-0}"
DEMO_RESET_TABLES="${DEMO_RESET_TABLES:-1}"

if [[ ! -f "${DEMO_SAMPLE_FILE}" || "${DEMO_REGENERATE}" == "1" ]]; then
  python3 scripts/create_sample.py --rows "${DEMO_SAMPLE_ROWS}" --output "${DEMO_SAMPLE_FILE}"
fi

DEMO_SAMPLE_CONTAINER_PATH="/workspace/${DEMO_SAMPLE_FILE#./}"

sample_compose up -d

wait_for_http "http://localhost:8080/v1/info" "Trino"

require_running spark-master
require_running trino

docker exec "$(sample_service_cid spark-master)" /opt/spark/bin/spark-submit \
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
  --input-file "${DEMO_SAMPLE_CONTAINER_PATH}" \
  --source-month "${DEMO_SOURCE_MONTH}" \
  --start-month "${DEMO_SOURCE_MONTH}" \
  --end-month "${DEMO_SOURCE_MONTH}" \
  $( [[ "${DEMO_RESET_TABLES}" == "1" ]] && printf '%s' "--reset-tables" )

echo "Sample pipeline prepared."
echo "Sample file: ${DEMO_SAMPLE_FILE}"
echo "Sample rows target: ${DEMO_SAMPLE_ROWS}"
echo "Verify counts with:"
echo "  docker exec $(sample_service_cid trino) trino --execute \"SELECT count(*) FROM iceberg.demo.bronze_events\""
