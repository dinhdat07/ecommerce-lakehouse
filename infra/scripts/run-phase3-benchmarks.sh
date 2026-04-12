#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

bench_compose() {
  compose --profile core --profile serving "$@"
}

service_cid_checked() {
  local service="$1"
  local cid
  cid="$(bench_compose ps -q "${service}")"
  if [[ -z "${cid}" ]]; then
    echo "error: service ${service} is not running" >&2
    exit 1
  fi
  printf '%s\n' "${cid}"
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

BENCH_MODE="${BENCH_MODE:-auto}"
BENCH_START_MONTH="${BENCH_START_MONTH:-2019-10}"
BENCH_END_MONTH="${BENCH_END_MONTH:-2020-02}"
STREAM_BENCH_START_MONTH="${STREAM_BENCH_START_MONTH:-2020-03}"
STREAM_BENCH_END_MONTH="${STREAM_BENCH_END_MONTH:-2020-04}"
BENCH_SAMPLE_FILE="${BENCH_SAMPLE_FILE:-/workspace/data/sample/events_sample_100k.csv}"
STREAM_BENCH_SAMPLE_FILE="${STREAM_BENCH_SAMPLE_FILE:-/workspace/data/sample/events_streaming_demo_1500.csv}"
BENCH_OUTPUT_DIR="${BENCH_OUTPUT_DIR:-/workspace/data/benchmarks}"
BENCH_INPUT_DIR="${BENCH_INPUT_DIR:-/workspace/data/raw}"
STREAM_MICROBATCH_SIZE="${STREAM_MICROBATCH_SIZE:-500}"

bench_compose up -d
wait_for_http "http://localhost:8080/v1/info" "Trino"

SPARK_MASTER_CID="$(service_cid_checked spark-master)"

docker exec "${SPARK_MASTER_CID}" mkdir -p "${BENCH_OUTPUT_DIR}"

docker exec "${SPARK_MASTER_CID}" /opt/spark/bin/spark-submit \
  --master local[2] \
  --driver-memory 1400m \
  --conf spark.jars.ivy=/tmp/.ivy2 \
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
  /workspace/jobs/benchmark_system_lakehouse_vs_parquet.py \
  --mode "${BENCH_MODE}" \
  --input-dir "${BENCH_INPUT_DIR}" \
  --start-month "${BENCH_START_MONTH}" \
  --end-month "${BENCH_END_MONTH}" \
  --sample-file "${BENCH_SAMPLE_FILE}" \
  --output-json "${BENCH_OUTPUT_DIR}/system_benchmark.json" \
  --output-csv "${BENCH_OUTPUT_DIR}/system_benchmark.csv"

docker exec "${SPARK_MASTER_CID}" /opt/spark/bin/spark-submit \
  --master local[2] \
  --driver-memory 1400m \
  --conf spark.jars.ivy=/tmp/.ivy2 \
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
  /workspace/jobs/benchmark_batch_vs_streaming.py \
  --mode "${BENCH_MODE}" \
  --input-dir "${BENCH_INPUT_DIR}" \
  --start-month "${STREAM_BENCH_START_MONTH}" \
  --end-month "${STREAM_BENCH_END_MONTH}" \
  --sample-file "${STREAM_BENCH_SAMPLE_FILE}" \
  --microbatch-size "${STREAM_MICROBATCH_SIZE}" \
  --output-json "${BENCH_OUTPUT_DIR}/processing_model_benchmark.json" \
  --output-csv "${BENCH_OUTPUT_DIR}/processing_model_benchmark.csv"

echo "Phase 3 benchmarks completed."
echo "Results directory: ${BENCH_OUTPUT_DIR}"
