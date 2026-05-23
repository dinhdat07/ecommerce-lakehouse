#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

KAFKA_ENV_FILE="${KAFKA_ENV_FILE:-${SERVER_ENV_DIR}/kafka-cluster.env}"
load_server_env "${KAFKA_ENV_FILE}"

compose_server "${SERVER_COMPOSE_DIR}/docker-compose.kafka-cluster.yml" exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "kafka-1:29092" \
  --create \
  --if-not-exists \
  --topic "${KAFKA_TOPIC_EVENTS}" \
  --partitions "${KAFKA_TOPIC_PARTITIONS}" \
  --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
  --config "min.insync.replicas=${KAFKA_TOPIC_MIN_INSYNC_REPLICAS}" \
  --config "retention.ms=$(( KAFKA_RETENTION_HOURS * 60 * 60 * 1000 ))" \
  --config "retention.bytes=${KAFKA_RETENTION_BYTES}" \
  --config "max.message.bytes=${KAFKA_MESSAGE_MAX_BYTES}"

compose_server "${SERVER_COMPOSE_DIR}/docker-compose.kafka-cluster.yml" exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "kafka-1:29092" \
  --create \
  --if-not-exists \
  --topic "${KAFKA_TOPIC_DLQ}" \
  --partitions "${KAFKA_TOPIC_PARTITIONS}" \
  --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR}" \
  --config "min.insync.replicas=${KAFKA_TOPIC_MIN_INSYNC_REPLICAS}" \
  --config "retention.ms=$(( KAFKA_RETENTION_HOURS * 60 * 60 * 1000 ))" \
  --config "retention.bytes=${KAFKA_RETENTION_BYTES}" \
  --config "max.message.bytes=${KAFKA_MESSAGE_MAX_BYTES}"

log "Kafka topics are ready."
