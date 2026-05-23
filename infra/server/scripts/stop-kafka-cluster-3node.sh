#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

load_server_env

KAFKA_REMOTE_REPO_ROOT="${KAFKA_REMOTE_REPO_ROOT:-${ROOT_DIR}}"
KAFKA_REMOTE_ENV_FILE="${KAFKA_REMOTE_ENV_FILE:-${KAFKA_REMOTE_REPO_ROOT}/infra/server/env/kafka-node.env}"
KAFKA_REMOTE_COMPOSE_FILE="${KAFKA_REMOTE_REPO_ROOT}/infra/server/compose/docker-compose.kafka-node.yml"
KAFKA_PROJECT_NAME="${KAFKA_PROJECT_NAME:-ecommerce-lakehouse-kafka}"

require_command ssh

SELF_IP="${KAFKA_SELF_IP:-$(tailscale ip -4 | head -n 1)}"

mapfile -t VOTERS < <(printf '%s\n' "${KAFKA_CONTROLLER_QUORUM_VOTERS}" | tr ',' '\n')
for voter in "${VOTERS[@]}"; do
  host="${voter#*@}"
  host="${host%:9093}"
  log "Stopping Kafka broker on ${host}"
  if [[ "${host}" == "${SELF_IP}" ]]; then
    docker compose -p "${KAFKA_PROJECT_NAME}" --env-file "${KAFKA_REMOTE_ENV_FILE}" -f "${KAFKA_REMOTE_COMPOSE_FILE}" down || true
  else
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" \
      "docker compose -p '${KAFKA_PROJECT_NAME}' --env-file '${KAFKA_REMOTE_ENV_FILE}' -f '${KAFKA_REMOTE_COMPOSE_FILE}' down || true"
  fi
done

log "Kafka cluster stop requested on all nodes."
