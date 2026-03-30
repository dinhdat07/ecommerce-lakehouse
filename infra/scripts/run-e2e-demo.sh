#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

full_compose() {
  compose --profile core --profile extended --profile serving --profile bi "$@"
}

profiled_service_cid() {
  full_compose ps -q "$1"
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
  if [[ -z "$(profiled_service_cid "${service}")" ]]; then
    echo "error: service ${service} is not running." >&2
    exit 1
  fi
}

full_compose up -d

wait_for_http "http://localhost:8080/v1/info" "Trino"
wait_for_http "http://localhost:8088/health" "Superset"

require_running spark-master
require_running trino
require_running superset

docker exec "$(profiled_service_cid spark-master)" /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.io.compression.codec=lzf \
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
  /workspace/jobs/load_sample_to_iceberg.py

docker exec -i "$(profiled_service_cid trino)" trino < "${ROOT_DIR}/infra/trino/sql/prepare_demo_views.sql"
docker exec "$(profiled_service_cid superset)" python /app/bootstrap/bootstrap_superset.py

echo "E2E demo prepared."
echo "Superset URL: http://localhost:8088"
echo "Dashboard URL: http://localhost:8088/superset/dashboard/lakehouse-sample-dashboard/"
