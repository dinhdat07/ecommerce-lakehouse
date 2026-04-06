#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

compose --profile core --profile extended --profile serving --profile bi down

docker volume rm \
  ecommerce-lakehouse-laptop_minio_data \
  ecommerce-lakehouse-laptop_postgres_data \
  ecommerce-lakehouse-laptop_superset_home \
  ecommerce-lakehouse-laptop_spark_events \
  ecommerce-lakehouse-laptop_spark_master_data \
  ecommerce-lakehouse-laptop_spark_worker1_data \
  ecommerce-lakehouse-laptop_spark_worker2_data \
  >/dev/null 2>&1 || true

rm -rf data/lakehouse data/manifests checkpoints/local_bus checkpoints/streaming_ingest_csv checkpoints/phase2
mkdir -p data/lakehouse data/manifests data/raw checkpoints
touch data/raw/.gitkeep data/manifests/.gitkeep

echo "Demo state reset complete."
