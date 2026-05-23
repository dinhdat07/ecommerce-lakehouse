#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

load_server_env

KAFKA_CLUSTER_ID="${KAFKA_CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}"
KAFKA_DATA_DIR="${KAFKA_DATA_DIR:-/srv/kafka/lakehouse}"
KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms512m -Xmx512m}"
KAFKA_REMOTE_REPO_ROOT="${KAFKA_REMOTE_REPO_ROOT:-${ROOT_DIR}}"
KAFKA_REMOTE_ENV_FILE="${KAFKA_REMOTE_ENV_FILE:-${KAFKA_REMOTE_REPO_ROOT}/infra/server/env/kafka-node.env}"
KAFKA_REMOTE_COMPOSE_FILE="${KAFKA_REMOTE_REPO_ROOT}/infra/server/compose/docker-compose.kafka-node.yml"
KAFKA_PROJECT_NAME="${KAFKA_PROJECT_NAME:-ecommerce-lakehouse-kafka}"
LOCAL_COMPOSE_FILE="${SERVER_COMPOSE_DIR}/docker-compose.kafka-node.yml"

require_command ssh
require_command scp
require_command docker

SELF_IP="${KAFKA_SELF_IP:-$(tailscale ip -4 | head -n 1)}"

mapfile -t VOTERS < <(printf '%s\n' "${KAFKA_CONTROLLER_QUORUM_VOTERS}" | tr ',' '\n')
if [[ "${#VOTERS[@]}" -ne 3 ]]; then
  fail "expected exactly 3 KAFKA_CONTROLLER_QUORUM_VOTERS entries, got ${#VOTERS[@]}"
fi

hosts=()
ids=()
for voter in "${VOTERS[@]}"; do
  ids+=("${voter%%@*}")
  hosts+=("${voter#*@}")
  hosts[-1]="${hosts[-1]%:9093}"
done

first_host="${hosts[0]}"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

for i in "${!hosts[@]}"; do
  host="${hosts[$i]}"
  node_id="${ids[$i]}"
  env_file="${tmp_dir}/kafka-node-${node_id}.env"
  cat >"${env_file}" <<EOF
KAFKA_CLUSTER_ID=${KAFKA_CLUSTER_ID}
KAFKA_NODE_ID=${node_id}
KAFKA_NODE_HOST=${host}
KAFKA_CONTROLLER_QUORUM_VOTERS=${KAFKA_CONTROLLER_QUORUM_VOTERS}
KAFKA_TOPIC_PARTITIONS=${KAFKA_TOPIC_PARTITIONS:-12}
KAFKA_TOPIC_REPLICATION_FACTOR=${KAFKA_TOPIC_REPLICATION_FACTOR:-3}
KAFKA_TOPIC_MIN_INSYNC_REPLICAS=${KAFKA_TOPIC_MIN_INSYNC_REPLICAS:-2}
KAFKA_RETENTION_HOURS=${KAFKA_RETENTION_HOURS:-72}
KAFKA_RETENTION_BYTES=${KAFKA_RETENTION_BYTES:-10737418240}
KAFKA_MESSAGE_MAX_BYTES=${KAFKA_MESSAGE_MAX_BYTES:-4194304}
KAFKA_DATA_DIR=${KAFKA_DATA_DIR}
KAFKA_HEAP_OPTS=${KAFKA_HEAP_OPTS}
EOF

  log "Preparing Kafka broker ${node_id} on ${host}"
  if [[ "${host}" == "${SELF_IP}" ]]; then
    mkdir -p "${KAFKA_DATA_DIR}" "$(dirname "${KAFKA_REMOTE_ENV_FILE}")"
    chown -R 1000:1000 "${KAFKA_DATA_DIR}"
    cp "${env_file}" "${KAFKA_REMOTE_ENV_FILE}"
    if [[ "${LOCAL_COMPOSE_FILE}" != "${KAFKA_REMOTE_COMPOSE_FILE}" ]]; then
      cp "${LOCAL_COMPOSE_FILE}" "${KAFKA_REMOTE_COMPOSE_FILE}"
    fi
    docker compose -p "${KAFKA_PROJECT_NAME}" --env-file "${KAFKA_REMOTE_ENV_FILE}" -f "${KAFKA_REMOTE_COMPOSE_FILE}" up -d
  else
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" \
      "mkdir -p '${KAFKA_DATA_DIR}' '$(dirname "${KAFKA_REMOTE_ENV_FILE}")' '$(dirname "${KAFKA_REMOTE_COMPOSE_FILE}")' && chown -R 1000:1000 '${KAFKA_DATA_DIR}'"
    scp -q -o BatchMode=yes -o StrictHostKeyChecking=no "${env_file}" "${host}:${KAFKA_REMOTE_ENV_FILE}"
    scp -q -o BatchMode=yes -o StrictHostKeyChecking=no "${LOCAL_COMPOSE_FILE}" "${host}:${KAFKA_REMOTE_COMPOSE_FILE}"
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" \
      "docker compose -p '${KAFKA_PROJECT_NAME}' --env-file '${KAFKA_REMOTE_ENV_FILE}' -f '${KAFKA_REMOTE_COMPOSE_FILE}' up -d"
  fi
done

log "Waiting for brokers to accept traffic"
sleep 10

log "Creating lakehouse topics"
if [[ "${first_host}" == "${SELF_IP}" ]]; then
  docker exec server-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${first_host}:9092" \
    --create --if-not-exists \
    --topic "${KAFKA_TOPIC_EVENTS}" \
    --partitions "${KAFKA_TOPIC_PARTITIONS:-12}" \
    --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR:-3}" \
    --config "min.insync.replicas=${KAFKA_TOPIC_MIN_INSYNC_REPLICAS:-2}" \
    --config "retention.ms=$(( ${KAFKA_RETENTION_HOURS:-72} * 3600 * 1000 ))" \
    --config "retention.bytes=${KAFKA_RETENTION_BYTES:-10737418240}"

  docker exec server-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${first_host}:9092" \
    --create --if-not-exists \
    --topic "${KAFKA_TOPIC_DLQ}" \
    --partitions "3" \
    --replication-factor "${KAFKA_TOPIC_REPLICATION_FACTOR:-3}" \
    --config "min.insync.replicas=${KAFKA_TOPIC_MIN_INSYNC_REPLICAS:-2}" \
    --config "retention.ms=$(( ${KAFKA_RETENTION_HOURS:-72} * 3600 * 1000 ))" \
    --config "retention.bytes=${KAFKA_RETENTION_BYTES:-10737418240}"
else
  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${first_host}" \
    "docker exec server-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server '${first_host}:9092' \
      --create --if-not-exists \
      --topic '${KAFKA_TOPIC_EVENTS}' \
      --partitions '${KAFKA_TOPIC_PARTITIONS:-12}' \
      --replication-factor '${KAFKA_TOPIC_REPLICATION_FACTOR:-3}' \
      --config min.insync.replicas='${KAFKA_TOPIC_MIN_INSYNC_REPLICAS:-2}' \
      --config retention.ms='$(( ${KAFKA_RETENTION_HOURS:-72} * 3600 * 1000 ))' \
      --config retention.bytes='${KAFKA_RETENTION_BYTES:-10737418240}'"

  ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${first_host}" \
    "docker exec server-kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server '${first_host}:9092' \
      --create --if-not-exists \
      --topic '${KAFKA_TOPIC_DLQ}' \
      --partitions '3' \
      --replication-factor '${KAFKA_TOPIC_REPLICATION_FACTOR:-3}' \
      --config min.insync.replicas='${KAFKA_TOPIC_MIN_INSYNC_REPLICAS:-2}' \
      --config retention.ms='$(( ${KAFKA_RETENTION_HOURS:-72} * 3600 * 1000 ))' \
      --config retention.bytes='${KAFKA_RETENTION_BYTES:-10737418240}'"
fi

log "Kafka cluster started across: ${hosts[*]}"
