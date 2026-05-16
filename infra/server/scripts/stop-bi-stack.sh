#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

compose_server "${SERVER_COMPOSE_DIR}/docker-compose.bi.yml" down
log "Stopped Trino and Superset."
