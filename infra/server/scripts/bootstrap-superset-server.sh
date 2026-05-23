#!/usr/bin/env bash
set -euo pipefail

until docker exec server-superset python - <<'PY'
import urllib.request
urllib.request.urlopen("http://127.0.0.1:8088/health", timeout=5)
PY
do
  sleep 2
done

docker exec server-superset python /app/bootstrap/bootstrap_superset.py
echo "Superset dashboard assets refreshed."
