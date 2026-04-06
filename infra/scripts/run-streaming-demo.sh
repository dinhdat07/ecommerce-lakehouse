#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

stream_compose() {
  compose --profile core --profile serving "$@"
}

stream_service_cid() {
  stream_compose ps -q "$1"
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
  if [[ -z "$(stream_service_cid "${service}")" ]]; then
    echo "error: service ${service} is not running." >&2
    exit 1
  fi
}

STREAM_SAMPLE_ROWS="${STREAM_SAMPLE_ROWS:-1500}"
STREAM_SAMPLE_FILE="${STREAM_SAMPLE_FILE:-data/sample/events_streaming_demo_${STREAM_SAMPLE_ROWS}.csv}"
STREAM_SAMPLE_REGENERATE="${STREAM_SAMPLE_REGENERATE:-0}"
STREAM_BATCH_SIZE="${STREAM_BATCH_SIZE:-500}"
STREAM_SLEEP_SECONDS="${STREAM_SLEEP_SECONDS:-1}"
STREAM_MAX_ROWS="${STREAM_MAX_ROWS:-}"
STREAM_TIMEOUT_SECONDS="${STREAM_TIMEOUT_SECONDS:-150}"
STREAM_TRIGGER_SECONDS="${STREAM_TRIGGER_SECONDS:-5}"
STREAM_MAX_OFFSETS_PER_TRIGGER="${STREAM_MAX_OFFSETS_PER_TRIGGER:-2000}"
STREAM_CHECKPOINT_DIR="${STREAM_CHECKPOINT_DIR:-/opt/spark/work-dir/checkpoints/phase2/kafka_to_iceberg}"
KAFKA_CONTAINER_ID=""

if [[ ! -f "${STREAM_SAMPLE_FILE}" || "${STREAM_SAMPLE_REGENERATE}" == "1" ]]; then
  python3 scripts/create_sample.py \
    --rows "${STREAM_SAMPLE_ROWS}" \
    --start-date "2020-03-01T00:00:00Z" \
    --days-span 61 \
    --output "${STREAM_SAMPLE_FILE}"
fi

stream_compose up -d

wait_for_http "http://localhost:8080/v1/info" "Trino"

require_running kafka
require_running spark-master
require_running trino

KAFKA_CONTAINER_ID="$(stream_service_cid kafka)"
SPARK_MASTER_CONTAINER_ID="$(stream_service_cid spark-master)"

docker exec "${SPARK_MASTER_CONTAINER_ID}" /bin/sh -ec \
  "rm -rf '${STREAM_CHECKPOINT_DIR:?}' && mkdir -p '${STREAM_CHECKPOINT_DIR}'"

docker exec "$(stream_service_cid kafka)" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --delete \
  --topic ecom.events \
  >/dev/null 2>&1 || true
docker exec "$(stream_service_cid kafka)" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create \
  --if-not-exists \
  --topic ecom.events \
  --partitions 2 \
  --replication-factor 1 \
  >/dev/null

docker exec "${SPARK_MASTER_CONTAINER_ID}" /opt/spark/bin/spark-submit \
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
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.postgresql:postgresql:42.7.3 \
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
  /workspace/jobs/kafka_stream_to_iceberg.py \
  --bootstrap-servers kafka:29092 \
  --topic ecom.events \
  --checkpoint-location "${STREAM_CHECKPOINT_DIR}" \
  --max-offsets-per-trigger "${STREAM_MAX_OFFSETS_PER_TRIGGER}" \
  --trigger-seconds "${STREAM_TRIGGER_SECONDS}" \
  --timeout-seconds "${STREAM_TIMEOUT_SECONDS}" \
  >/tmp/ecommerce_phase2_stream.log 2>&1 &

STREAM_DRIVER_PID=$!
sleep 8

producer_args=(
  --input-csv "${STREAM_SAMPLE_FILE}"
  --topic ecom.events
  --batch-size "${STREAM_BATCH_SIZE}"
  --sleep-seconds "${STREAM_SLEEP_SECONDS}"
  --docker-kafka-container "${KAFKA_CONTAINER_ID}"
)
if [[ -n "${STREAM_MAX_ROWS}" ]]; then
  producer_args+=(--max-rows "${STREAM_MAX_ROWS}")
fi

python3 apps/producer/replay_csv_to_kafka.py "${producer_args[@]}"

wait "${STREAM_DRIVER_PID}"

echo "Streaming demo completed."
echo "Sample file: ${STREAM_SAMPLE_FILE}"
echo "Checkpoint dir (container): ${STREAM_CHECKPOINT_DIR}"
echo "Validation SQL: infra/trino/sql/validate_streaming_phase2.sql"
