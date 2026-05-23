#!/usr/bin/env bash
# Create Iceberg JDBC catalog tables in Postgres if missing (e.g. volume created before 02-*.sql existed).
# Safe to run multiple times (CREATE TABLE IF NOT EXISTS).

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/../postgres/init/02-iceberg-jdbc-catalog-tables.sql"

if [[ ! -f "${SQL_FILE}" ]]; then
  echo "error: missing ${SQL_FILE}" >&2
  exit 1
fi

POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-ecommerce-lakehouse-laptop-postgres-1}"

docker exec -i "${POSTGRES_CONTAINER}" psql -U postgres -v ON_ERROR_STOP=1 < "${SQL_FILE}"
echo "Iceberg JDBC catalog tables ensured in database iceberg."
