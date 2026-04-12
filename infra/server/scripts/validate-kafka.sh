#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

KAFKA_ENV_FILE="${KAFKA_ENV_FILE:-${SERVER_ENV_DIR}/kafka-cluster.env}"
load_server_env "${KAFKA_ENV_FILE}"

compose_server "${SERVER_COMPOSE_DIR}/docker-compose.kafka-cluster.yml" exec -T kafka-1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "kafka-1:29092" --describe --topic "${KAFKA_TOPIC_EVENTS}"
