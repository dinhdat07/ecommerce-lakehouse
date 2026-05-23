#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
load_server_env "${STORAGE_ENV_FILE}"

MODE="${1:-all}"
TRINO_SERVER="${TRINO_SERVER:-http://127.0.0.1:8085}"
PUBLIC_IP="${PUBLIC_IP:-100.123.190.84}"
STREAMING_SAMPLE="${DEMO_REALTIME_INPUT_CSV:-${ROOT_DIR}/data/sample/events_streaming_demo_1500.csv}"

section() {
  printf '\n\n========== %s ==========\n' "$*"
}

trino_query() {
  local sql="$1"
  docker exec server-trino sh -lc \
    "trino --server '${TRINO_SERVER}' --catalog iceberg --schema demo --output-format CSV_HEADER --execute \"$sql\""
}

json_or_python() {
  local url="$1"
  local jq_filter="$2"
  if command -v jq >/dev/null 2>&1; then
    curl -fsS "${url}" | jq "${jq_filter}"
    return
  fi
  curl -fsS "${url}"
}

print_urls() {
  section "UI URLS"
  cat <<EOF
Spark master UI:        http://${PUBLIC_IP}:8080
Trino UI:               http://${PUBLIC_IP}:8085/ui/
Superset home:          http://${PUBLIC_IP}:8088
Dashboard executive:    http://${PUBLIC_IP}:8088/superset/dashboard/lakehouse-sample-dashboard/
Dashboard retention:    http://${PUBLIC_IP}:8088/superset/dashboard/lakehouse-retention-dashboard/
Dashboard merchandising:http://${PUBLIC_IP}:8088/superset/dashboard/lakehouse-merchandising-dashboard/
Chatbot frontend:       http://${PUBLIC_IP}:8089
Chatbot backend ready:  http://${PUBLIC_IP}:8090/api/ready
MinIO console:          http://${PUBLIC_IP}:9101
EOF
}

print_health() {
  section "HEALTH CHECK"
  printf '\n-- Docker containers on node1 --\n'
  docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'

  printf '\n-- Spark master summary --\n'
  if command -v jq >/dev/null 2>&1; then
    curl -fsS "http://${PUBLIC_IP}:8080/json/" | jq '{
      url,
      worker_count:(.workers|length),
      workers:[.workers[]|{host,cores,memory,state}],
      activeapps:(.activeapps|length),
      completedapps:(.completedapps|length),
      status
    }'
  else
    python3 - <<PY
import json, urllib.request
j=json.load(urllib.request.urlopen("http://${PUBLIC_IP}:8080/json/"))
print("url=", j.get("url"))
print("worker_count=", len(j.get("workers", [])))
for w in j.get("workers", []):
    print(f"worker={w['host']} state={w['state']} cores={w['cores']} memory_mb={w['memory']}")
print("activeapps=", len(j.get("activeapps", [])))
print("completedapps=", len(j.get("completedapps", [])))
PY
  fi

  printf '\n-- Trino info --\n'
  json_or_python "http://${PUBLIC_IP}:8085/v1/info" '.'

  printf '\n-- Superset health --\n'
  curl -fsS "http://${PUBLIC_IP}:8088/health"
  printf '\n'

  printf '\n-- Chatbot readiness --\n'
  curl -fsS "http://${PUBLIC_IP}:8090/api/ready"
  printf '\n'
}

print_distributed() {
  section "DISTRIBUTED PROOF"
  printf '\n-- Spark 3-node workers --\n'
  python3 - <<PY
import json, urllib.request
j=json.load(urllib.request.urlopen("http://${PUBLIC_IP}:8080/json/"))
print(f"master={j['url']}")
print(f"alive_workers={len(j['workers'])}")
for worker in j["workers"]:
    print(
        f"worker={worker['host']} state={worker['state']} "
        f"cores={worker['cores']} free={worker['coresfree']} "
        f"memory_mb={worker['memory']} free_mb={worker['memoryfree']}"
    )
PY

  printf '\n-- MinIO distributed object store --\n'
  docker run --rm --network host \
    -e S3_ENDPOINT -e S3_ACCESS_KEY -e S3_SECRET_KEY \
    --entrypoint /bin/sh \
    minio/mc:RELEASE.2025-02-21T16-00-46Z \
    -ec 'mc alias set lakehouse "${S3_ENDPOINT}" "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}" >/dev/null && mc admin info lakehouse'

  printf '\n-- Kafka 3-node quorum and replicated topics --\n'
  bash "${SCRIPT_DIR}/validate-kafka-cluster-3node.sh"
}

print_storage() {
  section "RAW DATA AND OBJECT STORAGE"
  printf '\n-- Candidate raw data paths --\n'
  ls -lh /root/ecommerce-data 2>/dev/null || true
  ls -lh "${RAW_DATA_DIR:-/srv/ecommerce/raw}" 2>/dev/null || true
  ls -lh "${ROOT_DIR}/data/raw" 2>/dev/null || true

  printf '\n-- Bucket validation --\n'
  bash "${SCRIPT_DIR}/validate-storage.sh"

  printf '\n-- Iceberg warehouse footprint --\n'
  docker run --rm --network host \
    -e S3_ENDPOINT -e S3_ACCESS_KEY -e S3_SECRET_KEY -e S3_BUCKET_WAREHOUSE \
    --entrypoint /bin/sh minio/mc:RELEASE.2025-02-21T16-00-46Z \
    -ec 'mc alias set lakehouse "${S3_ENDPOINT}" "${S3_ACCESS_KEY}" "${S3_SECRET_KEY}" >/dev/null && mc du -r "lakehouse/${S3_BUCKET_WAREHOUSE}/demo" | tail -n 20'
}

print_data() {
  section "BRONZE -> SILVER -> GOLD DATA PROOF"
  printf '\n-- Core table row counts --\n'
  trino_query "
SELECT 'bronze_events' AS table_name, count(*) AS rows FROM bronze_events
UNION ALL
SELECT 'silver_events', count(*) FROM silver_events
UNION ALL
SELECT 'daily_revenue', count(*) FROM daily_revenue
UNION ALL
SELECT 'session_funnel', count(*) FROM session_funnel
UNION ALL
SELECT 'rfm_segmentation', count(*) FROM rfm_segmentation
"

  printf '\n-- History-wide Gold table row counts --\n'
  trino_query "
SELECT 'cohort_retention' AS table_name, count(*) AS rows FROM cohort_retention
UNION ALL
SELECT 'repeat_purchase', count(*) FROM repeat_purchase
UNION ALL
SELECT 'product_affinity', count(*) FROM product_affinity
UNION ALL
SELECT 'time_to_conversion_distribution', count(*) FROM time_to_conversion_distribution
UNION ALL
SELECT 'rfm_segmentation', count(*) FROM rfm_segmentation
"

  printf '\n-- Monthly revenue by Gold daily_revenue --\n'
  trino_query "
SELECT substr(cast(event_date AS varchar),1,7) AS month,
       count(*) AS days,
       sum(purchase_revenue) AS revenue
FROM daily_revenue
GROUP BY 1
ORDER BY 1
"

  printf '\n-- Daily revenue sample --\n'
  trino_query "
SELECT * FROM daily_revenue ORDER BY 1 LIMIT 10
"

  printf '\n-- Top category revenue sample --\n'
  trino_query "
SELECT category_code, SUM(purchase_revenue) AS revenue
FROM category_performance_daily
GROUP BY 1
ORDER BY revenue DESC
LIMIT 5
"
}

print_chatbot_script() {
  section "CHATBOT DEMO PROMPTS"
  cat <<'EOF'
Open the chatbot UI and ask these in order:

1. Show the daily revenue trend and purchase volume.
2. Which categories generate the most revenue?
3. How is the conversion funnel trending over time?
4. Summarize cohort retention performance.
5. What does the RFM segment mix look like?

Emphasize:
- visible SQL
- Trino-backed results
- chart/table output
- concise English business summary
- independent BI copilot UI, not embedded in Superset
EOF
}

print_narrative() {
  section "SUGGESTED TALK TRACK"
  cat <<EOF
1. This is a 3-node lakehouse over Tailscale:
   - Spark workers: node1, node2, node3
   - Kafka quorum: 3 brokers/controllers, topic RF=3
   - MinIO: 3 nodes, 6 drives, erasure coding
   - Trino and Superset serve Gold Iceberg marts
   - Chatbot is a separate internal analytics copilot

2. Data scale:
   - Silver events: roughly 116M+ rows
   - Gold marts include revenue, funnel, retention, merchandising, RFM

3. Live proof:
   - Run: bash infra/server/scripts/demo-realtime-proof.sh run
   - Watch Spark UI while Kafka replay is consumed and Bronze rows increase

4. Product layer:
   - Superset shows dashboards over physical Gold tables
   - Chatbot answers business questions with SQL transparency
EOF
}

run_realtime() {
  section "LIVE KAFKA -> SPARK -> ICEBERG PROOF"
  printf 'Sample replay file: %s\n' "${STREAMING_SAMPLE}"
  bash "${SCRIPT_DIR}/demo-realtime-proof.sh" run
}

usage() {
  cat <<EOF
Usage: bash infra/server/scripts/demo-complete-showcase.sh <mode>

Modes:
  all          Safe complete demo proof without replaying new Kafka data
  health       Container, Spark, Trino, Superset, chatbot readiness
  distributed Spark + MinIO + Kafka distributed proof
  storage      Raw data and Iceberg warehouse proof
  data         Bronze/Silver/Gold Trino data proof
  urls         UI links
  chatbot      Chatbot prompt script
  realtime     Live Kafka -> Spark -> Iceberg replay proof
  narrative    Speaker notes
EOF
}

case "${MODE}" in
  all)
    print_urls
    print_health
    print_distributed
    print_storage
    print_data
    print_chatbot_script
    print_narrative
    ;;
  health) print_health ;;
  distributed) print_distributed ;;
  storage) print_storage ;;
  data) print_data ;;
  urls) print_urls ;;
  chatbot) print_chatbot_script ;;
  realtime) run_realtime ;;
  narrative) print_narrative ;;
  -h|--help|help) usage ;;
  *)
    usage
    exit 1
    ;;
esac
