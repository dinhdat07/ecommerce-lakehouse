#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

bash "${SCRIPT_DIR}/start-kafka-cluster-3node.sh"
bash "${SCRIPT_DIR}/validate-kafka-cluster-3node.sh"
bash "${SCRIPT_DIR}/start-bi-stack.sh"
bash "${SCRIPT_DIR}/start-chatbot-stack.sh"

cat <<'EOF'

Demo stack is up.

- Spark UI: http://100.123.190.84:8080
- Trino UI: http://100.123.190.84:8085/ui/
- Superset: http://100.123.190.84:8088
- Chatbot frontend: http://100.123.190.84:8089
- Chatbot backend: http://100.123.190.84:8090/api/ready

Next:
1. bash infra/server/scripts/demo-showcase.sh status
2. bash infra/server/scripts/demo-realtime-proof.sh run
EOF
