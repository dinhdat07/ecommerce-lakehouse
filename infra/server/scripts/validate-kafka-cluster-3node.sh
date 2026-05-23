#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

load_server_env

require_command ssh

SELF_IP="${KAFKA_SELF_IP:-$(tailscale ip -4 | head -n 1)}"

mapfile -t VOTERS < <(printf '%s\n' "${KAFKA_CONTROLLER_QUORUM_VOTERS}" | tr ',' '\n')
first_host="${VOTERS[0]#*@}"
first_host="${first_host%:9093}"

printf '=== Kafka Quorum ===\n'
if [[ "${first_host}" == "${SELF_IP}" ]]; then
  docker exec server-kafka /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller "${first_host}:9093" describe --status
else
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${first_host}" \
    "docker exec server-kafka /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller '${first_host}:9093' describe --status"
fi

printf '\n=== Topic %s ===\n' "${KAFKA_TOPIC_EVENTS}"
if [[ "${first_host}" == "${SELF_IP}" ]]; then
  docker exec server-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${first_host}:9092" --describe --topic "${KAFKA_TOPIC_EVENTS}"
else
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${first_host}" \
    "docker exec server-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server '${first_host}:9092' --describe --topic '${KAFKA_TOPIC_EVENTS}'"
fi

printf '\n=== Topic %s ===\n' "${KAFKA_TOPIC_DLQ}"
if [[ "${first_host}" == "${SELF_IP}" ]]; then
  docker exec server-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${first_host}:9092" --describe --topic "${KAFKA_TOPIC_DLQ}"
else
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${first_host}" \
    "docker exec server-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server '${first_host}:9092' --describe --topic '${KAFKA_TOPIC_DLQ}'"
fi

printf '\n=== Brokers ===\n'
for voter in "${VOTERS[@]}"; do
  host="${voter#*@}"
  host="${host%:9093}"
  if [[ "${host}" == "${SELF_IP}" ]]; then
    printf '%s ' "${host}"
    docker ps --format '{{.Names}} {{.Status}}' | grep '^server-kafka '
  else
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" \
      "printf '%s ' '${host}'; docker ps --format '{{.Names}} {{.Status}}' | grep '^server-kafka '"
  fi
done
