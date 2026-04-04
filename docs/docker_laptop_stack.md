# Laptop-Friendly Docker Compose Stack

## Goal

This stack simulates a small multi-node environment on one laptop while staying within a practical `30-40 GB` free-disk budget.

## Included Services

`core` profile:

- `minio`: shared object storage simulation
- `minio-init`: one-shot bucket bootstrap
- `kafka`: single-broker event bus in KRaft mode
- `kafka-init`: one-shot topic bootstrap
- `spark-master`: control-plane role
- `spark-worker-1`: first compute node

`extended` profile:

- `spark-worker-2`: second compute node for a more realistic multi-node simulation

`serving` profile:

- `postgres`: Iceberg JDBC catalog and Superset metadata database
- `trino`: SQL engine over the Iceberg catalog on MinIO

`bi` profile:

- `superset-init`: one-shot Superset metadata/admin bootstrap
- `superset`: BI UI and API on `http://localhost:8088`

The working demo path is:

1. `run-e2e-demo.sh` generates or reuses a sample CSV in `data/sample/`.
2. Spark appends Bronze rows, incrementally inserts Silver rows, and materializes Gold Iceberg tables.
3. Trino queries those physical Iceberg tables.
4. Superset connects to Trino and publishes a dashboard from the Gold tables.

## Expected Disk Usage

- First image pull: about `5-7 GB`
- Named volumes during the demo: about `0.5-1.5 GB`
- Safe working budget: about `8-12 GB`

With `30-40 GB` free on `C:`, this stack is realistic as long as you clean it up after testing.

## Resource Profile

- `minio`: `0.75 CPU`, `768 MB RAM`
- `kafka`: `1 CPU`, `1 GB RAM`
- `spark-master`: `1.25 CPU`, `2 GB RAM`
- `spark-worker-1`: `1.25 CPU`, `1.75 GB RAM`
- `spark-worker-2` optional: `1 CPU`, `1.25 GB RAM`
- `postgres`: `0.5 CPU`, `512 MB RAM`
- `trino`: `1 CPU`, `1.5 GB RAM`
- `superset`: `1 CPU`, `1.5 GB RAM`

Core mode should fit on a typical laptop with `16 GB` RAM. Extended mode is better on `16-24 GB`.

## Data and Cleanup Model

All persistent state is stored in named Docker volumes defined in `infra/docker-compose.yml`:

- `minio_data`
- `kafka_data`
- `spark_events`
- `spark_master_data`
- `spark_worker1_data`
- `spark_worker2_data`
- `postgres_data`
- `superset_home`

Containers also use log rotation to avoid log bloat.

## Commands

Prerequisite:

- Docker Desktop with WSL integration, or Docker Engine plus the Compose plugin

Start core stack:

```bash
bash infra/scripts/up-core.sh
```

Start extended stack:

```bash
bash infra/scripts/up-extended.sh
```

Start the full BI stack:

```bash
bash infra/scripts/up-bi.sh
```

Run the full demo pipeline:

```bash
bash infra/scripts/run-e2e-demo.sh
```

Run only Bronze, Silver, and Gold on the sample without Superset:

```bash
bash infra/scripts/run-sample-pipeline.sh
```

Run the full demo pipeline with a smaller sample:

```bash
DEMO_SAMPLE_ROWS=50000 bash infra/scripts/run-e2e-demo.sh
```

Check status:

```bash
bash infra/scripts/status.sh
```

Verify the simulation:

```bash
bash infra/scripts/verify.sh
```

Stop without deleting data:

```bash
bash infra/scripts/down.sh
```

Delete containers, network, volumes, and service images:

```bash
bash infra/scripts/purge.sh
```

Reset only demo warehouse/catalog state while keeping images:

```bash
bash infra/scripts/reset-demo-state.sh
```

Manual full historical backfill stays opt-in only:

```bash
MANUAL_FULL_BACKFILL=1 FULL_START_MONTH=2019-10 FULL_END_MONTH=2020-02 bash infra/scripts/run-full-backfill-manual.sh
```

## Step-by-Step Workflow

1. Check current Docker disk usage:
   ```bash
   docker system df
   ```
2. Start the smallest useful simulation:
   ```bash
   bash infra/scripts/up-core.sh
   ```
3. Confirm the services are up:
   ```bash
   bash infra/scripts/status.sh
   ```
4. Run the built-in infrastructure smoke checks:
   ```bash
   bash infra/scripts/verify.sh
   ```
5. If you want a more realistic worker layout, add the second worker:
   ```bash
   bash infra/scripts/up-extended.sh
   bash infra/scripts/verify.sh
   ```
6. To prepare the full dashboard demo:
   ```bash
   bash infra/scripts/up-bi.sh
   bash infra/scripts/run-e2e-demo.sh
   ```
   Optional:
   ```bash
   DEMO_SAMPLE_ROWS=50000 bash infra/scripts/run-e2e-demo.sh
   ```
7. Open Superset:
   ```text
   http://localhost:8088
   ```
   Use `admin` / `admin`, then open `/superset/dashboard/lakehouse-sample-dashboard/`.
8. After testing, stop the stack but keep data:
   ```bash
   bash infra/scripts/down.sh
   ```
9. When you want the disk space back, purge the entire stack:
   ```bash
   bash infra/scripts/purge.sh
   docker system df
   ```

## What To Verify

- Kafka topic `ecom.events` exists
- MinIO buckets `bronze`, `silver`, `gold`, and `warehouse` exist
- Spark master is reachable at `http://localhost:8081`
- Spark worker UIs are reachable at `http://localhost:8082` and optionally `http://localhost:8083`
- Spark master UI lists the registered workers
- Trino responds on `http://localhost:8080/v1/info`
- Superset health responds on `http://localhost:8088/health`
- Trino lists `iceberg.demo.bronze_events`, `silver_events`, `daily_revenue`, `top_products`, `conversion_funnel_daily`, `category_performance_daily`, `session_funnel`, and `user_conversion_path`
- Superset serves the dashboard at `http://localhost:8088/superset/dashboard/lakehouse-sample-dashboard/`
- Trino validation SQL is available in `infra/trino/sql/validate_silver_gold.sql`

## Best Practices To Avoid Disk Bloat

- Use `up-core.sh` unless you specifically need the second worker.
- Use `run-e2e-demo.sh` instead of manual commands if you want the full BI demo.
- Keep `DEMO_SAMPLE_ROWS` at `100000` or lower on laptop/WSL setups.
- Do not place large historical files in `data/raw/` for the default Docker demo workflow.
- Run `purge.sh` after testing if disk space is tight.
- Keep Kafka retention short and topic sizes small in laptop mode.
- Avoid large datasets during service smoke tests.
- Do not keep repeated stopped containers or unused images around.
- Prefer named Docker volumes over ad hoc bind mounts for service state, because they are easier to remove completely with `purge.sh`.
- If free space still looks low after purging this stack, check for unrelated Docker leftovers:
  ```bash
  docker system df
  docker image ls
  docker volume ls
  ```
