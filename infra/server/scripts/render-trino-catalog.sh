#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
load_server_env "${STORAGE_ENV_FILE}"

OUTPUT_PATH="${TRINO_RENDERED_CATALOG_PATH:-${SERVER_ROOT_DIR}/trino/etc/catalog/iceberg.properties}"
mkdir -p "$(dirname "${OUTPUT_PATH}")"

python3 - <<'PY' "${SERVER_ROOT_DIR}/trino/catalog/iceberg.properties.template" "${OUTPUT_PATH}"
import os
import sys
from string import Template

template_path, output_path = sys.argv[1:]
with open(template_path, "r", encoding="utf-8") as handle:
    content = Template(handle.read()).safe_substitute(os.environ)
with open(output_path, "w", encoding="utf-8") as handle:
    handle.write(content)
PY

log "Rendered Trino Iceberg catalog to ${OUTPUT_PATH}"
