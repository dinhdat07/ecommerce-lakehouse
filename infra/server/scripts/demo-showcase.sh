#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
load_server_env "${STORAGE_ENV_FILE}"

MODE="${1:-status}"

trino_query() {
  local sql="$1"
  docker exec server-trino sh -lc \
    "trino --server http://127.0.0.1:8085 --catalog iceberg --schema demo --output-format CSV_HEADER --execute \"$sql\""
}

print_topology() {
  printf '=== Topology ===\n'
  printf 'node1=%s\n' "100.123.190.84"
  printf 'node2=%s\n' "100.76.241.30"
  printf 'node3=%s\n' "100.90.64.86"
  printf 'spark_master=%s\n' "${SPARK_MASTER}"
  printf 'kafka_bootstrap=%s\n' "${KAFKA_BOOTSTRAP_SERVERS}"
  printf 'object_store=%s\n' "${S3_ENDPOINT}"
  printf 'trino=%s\n' "http://100.123.190.84:8085"
  printf 'superset=%s\n' "http://100.123.190.84:8088"
  printf 'chatbot=%s\n' "http://100.123.190.84:8089"
}

print_spark() {
  printf '\n=== Spark Cluster ===\n'
  python3 - <<'PY'
import json
import urllib.request

j = json.load(urllib.request.urlopen("http://127.0.0.1:8080/json/"))
print(f"master={j['url']}")
print(f"alive_workers={len(j['workers'])}")
for worker in j["workers"]:
    print(
        f"worker={worker['host']} state={worker['state']} "
        f"cores={worker['cores']} free={worker['coresfree']} "
        f"memory_mb={worker['memory']} free_mb={worker['memoryfree']}"
    )
PY
}

print_minio() {
  printf '\n=== MinIO Cluster ===\n'
  docker run --rm --network host --entrypoint /bin/sh \
    minio/mc:RELEASE.2025-02-21T16-00-46Z \
    -ec 'mc alias set local http://127.0.0.1:9100 minioadmin minioadmin >/dev/null && mc admin info local'
}

print_kafka() {
  printf '\n=== Kafka Cluster ===\n'
  bash "${SCRIPT_DIR}/validate-kafka-cluster-3node.sh"
}

print_data() {
  printf '\n=== Data Footprint ===\n'
  trino_query "
SELECT 'silver_events_rows' AS metric, count(*) AS value FROM silver_events
UNION ALL
SELECT 'daily_revenue_days', count(*) FROM daily_revenue
UNION ALL
SELECT 'category_performance_rows', count(*) FROM category_performance_daily
UNION ALL
SELECT 'rfm_rows', count(*) FROM rfm_segmentation
"

  printf '\n=== Sample Business Query ===\n'
  trino_query "
SELECT category_code, SUM(purchase_revenue) AS revenue
FROM category_performance_daily
GROUP BY 1
ORDER BY revenue DESC
LIMIT 5
"
}

print_apps() {
  printf '\n=== App Health ===\n'
  printf 'superset_health='
  curl -fsS http://127.0.0.1:8088/health || true
  printf '\nchatbot_ready='
  curl -fsS http://100.123.190.84:8090/api/ready || true
  printf '\n'
}

print_script() {
  cat <<'EOF'

=== Demo Flow ===
1. Show architecture and topology:
   bash infra/server/scripts/demo-showcase.sh status
2. Open Spark UI, Trino UI, Superset, and Chatbot side-by-side.
3. In Superset, open the dashboard and point out Gold marts.
4. In terminal, prove realtime/distributed ingestion:
   bash infra/server/scripts/demo-realtime-proof.sh run
5. In chatbot UI, ask:
   - Show the daily revenue trend and purchase volume.
   - Which categories generate the most revenue?
   - Summarize cohort retention performance.
EOF
}

case "${MODE}" in
  status)
    print_topology
    print_spark
    print_minio
    print_kafka
    print_data
    print_apps
    print_script
    ;;
  *)
    printf 'Usage: bash infra/server/scripts/demo-showcase.sh status\n' >&2
    exit 1
    ;;
esac
