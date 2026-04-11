# Infrastructure Notes

This directory now contains a laptop-friendly Docker Compose stack for lightweight multi-node simulation.

## Included Components

- Kafka in single-broker KRaft mode
- MinIO as shared object storage
- Spark master
- Spark worker 1
- Optional Spark worker 2 through the `extended` profile
- Postgres for the Iceberg JDBC catalog and Superset metadata
- Trino for SQL serving over Iceberg
- Superset for the demo dashboard

The stack is intentionally minimal so it can run on a single laptop with constrained disk space and still be easy to tear down completely.

## Main Files

- `docker-compose.yml`: profile-based laptop simulation stack
- `scripts/up-core.sh`: start the lightweight default stack
- `scripts/up-extended.sh`: start the stack with a second Spark worker
- `scripts/up-bi.sh`: start the full local stack including Trino and Superset
- `scripts/bootstrap-superset.sh`: apply the demo Superset database, datasets, and dashboard into the running BI stack
- `scripts/run-sample-pipeline.sh`: run Bronze, Silver, and Gold materialization on a configurable sample file
- `scripts/run-e2e-demo.sh`: materialize Bronze, Silver, and Gold Iceberg tables from a configurable demo sample, then bootstrap Superset assets
- `scripts/run-streaming-demo.sh`: run the bounded Kafka -> Bronze -> Silver -> Gold streaming demo for March-April sample data
- `scripts/run-full-backfill-manual.sh`: manually gated full historical backfill entrypoint
- `scripts/reset-demo-state.sh`: remove demo warehouse/catalog state and local file-based outputs
- `scripts/verify.sh`: smoke-test the stack
- `scripts/down.sh`: stop containers while keeping volumes
- `scripts/purge.sh`: remove containers, volumes, network, and service images

## Operator Guide

Use [docs/docker_laptop_stack.md](/mnt/e/coding/learn%20data/data%20engineering/ecommerce-lakehouse/docs/docker_laptop_stack.md) for step-by-step instructions, disk expectations, verification steps, and cleanup guidance.

## Trino: `Cannot check and eventually update SQL schema`

That message wraps the **real** error from Postgres. Check Trino logs for `Caused by:` — common cases:

### A) `relation "iceberg_tables" does not exist`

The `iceberg` database has **no Iceberg JDBC metadata tables** yet. Trino’s Iceberg connector does **not** auto-create them (`initializeCatalogTables=false`). They are created on a **fresh** Postgres volume by `infra/postgres/init/02-iceberg-jdbc-catalog-tables.sql`, or when **Spark** has successfully committed to the JDBC catalog at least once.

**If your Postgres volume was created before that init script existed**, apply it once (idempotent):

```bash
bash infra/scripts/init-iceberg-jdbc-catalog.sh
```

Or from Git Bash / WSL, with the repo path correct:

```bash
docker exec -i ecommerce-lakehouse-laptop-postgres-1 psql -U postgres -v ON_ERROR_STOP=1 < infra/postgres/init/02-iceberg-jdbc-catalog-tables.sql
```

Then retry Trino (e.g. `SHOW TABLES FROM iceberg.demo`).

### B) Schema migration / half-migrated JDBC catalog

If logs show a different `PSQLException` (not “does not exist”), try resetting volumes and reloading the demo:

```bash
docker compose -f infra/docker-compose.yml --profile core --profile extended --profile serving --profile bi down -v
bash infra/scripts/up-bi.sh
bash infra/scripts/run-e2e-demo.sh
```

### C) Confirm Trino catalog flags

`docker exec ecommerce-lakehouse-laptop-trino-1 cat /etc/trino/catalog/iceberg.properties` should include `iceberg.jdbc-catalog.schema-version=V0` so Trino matches the V0 JDBC layout used by Spark in this repo.
