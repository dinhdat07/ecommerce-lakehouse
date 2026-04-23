#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"

MINIO_NGINX_ENV_FILE="${MINIO_NGINX_ENV_FILE:-${SERVER_ENV_DIR}/minio-nginx.env}"
load_server_env "${MINIO_NGINX_ENV_FILE}"

OUTPUT_PATH="${MINIO_NGINX_CONFIG_PATH:-${SERVER_RUNTIME_DIR}/nginx/minio.conf}"
mkdir -p "$(dirname "${OUTPUT_PATH}")"

python3 - <<'PY' "${SERVER_ROOT_DIR}/nginx/minio.conf.template" "${OUTPUT_PATH}"
import os
import sys
from string import Template


def bool_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def build_upstream_block(raw: str) -> str:
    items = [item.strip() for item in raw.split(",") if item.strip()]
    if not items:
        raise SystemExit("error: at least one MinIO upstream is required")
    return "\n".join(f"    server {item} max_fails=3 fail_timeout=10s;" for item in items)


tls_enabled = bool_env("MINIO_NGINX_TLS_ENABLED", False)
api_port = os.environ.get("MINIO_NGINX_API_PORT", "9000")
console_enabled = bool_env("MINIO_NGINX_ENABLE_CONSOLE", True)
console_port = os.environ.get("MINIO_NGINX_CONSOLE_PORT", "9001")
cert_file = os.environ.get("MINIO_NGINX_TLS_CERT_FILE", "/etc/nginx/certs/tls.crt")
key_file = os.environ.get("MINIO_NGINX_TLS_KEY_FILE", "/etc/nginx/certs/tls.key")
api_upstreams = build_upstream_block(os.environ.get("MINIO_NGINX_API_UPSTREAMS", ""))

if tls_enabled:
    api_listen = f"{api_port} ssl"
    ssl_block = (
        f"\n    ssl_certificate {cert_file};\n"
        f"    ssl_certificate_key {key_file};\n"
        "    ssl_session_cache shared:SSL:10m;\n"
        "    ssl_session_timeout 10m;\n"
    )
else:
    api_listen = api_port
    ssl_block = ""

if console_enabled:
    console_upstreams = build_upstream_block(os.environ.get("MINIO_NGINX_CONSOLE_UPSTREAMS", ""))
    console_listen = f"{console_port} ssl" if tls_enabled else console_port
    console_ssl_block = ssl_block
    console_server_block = f"""
server {{
    listen {console_listen};
    server_name {os.environ.get("MINIO_NGINX_CONSOLE_SERVER_NAME", os.environ.get("MINIO_NGINX_SERVER_NAME", "_"))};
{console_ssl_block}    proxy_buffering off;
    proxy_request_buffering off;

    location / {{
        proxy_http_version 1.1;
        proxy_set_header Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_connect_timeout 5s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
        proxy_pass http://minio_console;
    }}
}}
"""
    console_upstream_decl = f"""upstream minio_console {{
{console_upstreams}
    keepalive 16;
}}"""
else:
    console_upstream_decl = ""
    console_server_block = ""

os.environ["MINIO_API_UPSTREAM_BLOCK"] = api_upstreams
os.environ["MINIO_API_LISTEN"] = api_listen
os.environ["MINIO_API_SSL_BLOCK"] = ssl_block
os.environ["MINIO_CONSOLE_UPSTREAM_DECL"] = console_upstream_decl
os.environ["MINIO_CONSOLE_SERVER_BLOCK"] = console_server_block

template_path, output_path = sys.argv[1:]
with open(template_path, "r", encoding="utf-8") as handle:
    content = Template(handle.read()).safe_substitute(os.environ)
with open(output_path, "w", encoding="utf-8") as handle:
    handle.write(content)
PY

log "Rendered MinIO Nginx config to ${OUTPUT_PATH}"
