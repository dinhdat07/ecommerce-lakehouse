# ecommerce-lakehouse

Local-first e-commerce lakehouse for user behavior analytics with Bronze, Silver, and Gold data contracts.

## Current Implementation

- Phase 1 batch backfill for `2019-10` to `2020-02` into physical Bronze, Silver, and Gold Iceberg tables
- Phase 2 bounded streaming demo for `2020-03` to `2020-04` using Kafka replay and Spark Structured Streaming
- Canonical Silver materialization with normalization, null handling, and deduplication
- Gold Iceberg tables for revenue, product ranking, funnel, category, session, and user-path analytics
- Trino as the query layer over Iceberg tables
- Superset demo dashboard backed by physical Gold tables

## Target Platform

- Kafka for durable event ingestion
- Spark for batch and structured streaming execution
- MinIO for S3-compatible storage
- Iceberg for managed Silver and Gold tables
- Trino for SQL serving
- Superset for BI dashboards

## Repository Layout

- `apps/`: batch, streaming, producer, and SQL entrypoints
- `common/`: shared config, quality, manifests, storage, schemas, and transforms
- `pipelines/`: reusable Bronze, Silver, and Gold stage implementations
- `data/raw/`: historical source files
- `data/sample/`: local sample inputs
- `docs/`: architecture, pipeline, system map, and improvement notes
- `infra/`: future integrated platform bootstrap assets
- `tests/`: unit and pipeline-focused tests

## Quick Start

1. Copy `.env.example` to `.env`.
2. Bootstrap the local environment:
   ```bash
   bash scripts/bootstrap_local.sh
   ```
3. Run the local batch flow:
   ```bash
   bash scripts/clean_local_state.sh
   bash scripts/run_local_batch.sh
   ```
4. Run the streaming-friendly local replay flow:
   ```bash
   bash scripts/clean_local_state.sh
   bash scripts/run_local_streaming.sh
   ```
5. Run automated verification:
   ```bash
   python3 scripts/verify_local.py
   python3 -m pytest -q
   ```

## Docker Demo

To run the laptop-friendly Phase 1 batch demo with Iceberg, Trino, and Superset:

```bash
bash infra/scripts/up-bi.sh
bash infra/scripts/run-e2e-demo.sh
```

By default the demo script:

1. generates or reuses a local sample CSV at about `100k` rows
2. resets the demo tables
3. materializes Bronze, Silver, and Gold Iceberg tables from that sample only

Then open `http://localhost:8088`, sign in with `admin` / `admin`, and open `/superset/dashboard/lakehouse-sample-dashboard/`.

To re-apply the Superset demo assets explicitly:

```bash
bash infra/scripts/bootstrap-superset.sh
```

To change the demo size safely:

```bash
DEMO_SAMPLE_ROWS=50000 bash infra/scripts/run-e2e-demo.sh
```

To reset all demo state before rerunning:

```bash
bash infra/scripts/reset-demo-state.sh
```

To run Bronze, Silver, and Gold without Superset:

```bash
bash infra/scripts/run-sample-pipeline.sh
```

To run a manual full backfill, opt in explicitly:

```bash
MANUAL_FULL_BACKFILL=1 FULL_START_MONTH=2019-10 FULL_END_MONTH=2020-02 bash infra/scripts/run-full-backfill-manual.sh
```

To validate Silver and Gold in Trino:

```bash
docker exec -i ecommerce-lakehouse-laptop-trino-1 trino < infra/trino/sql/validate_silver_gold.sql
```

To run the bounded Phase 2 streaming demo on top of the Phase 1 foundation:

```bash
STREAM_SAMPLE_ROWS=1500 STREAM_TIMEOUT_SECONDS=150 STREAM_MAX_OFFSETS_PER_TRIGGER=500 bash infra/scripts/run-streaming-demo.sh
```

Then validate March-April replay output:

```bash
docker exec -i ecommerce-lakehouse-laptop-trino-1 trino < infra/trino/sql/validate_streaming_phase2.sql
```

The streaming path is bounded by sample size, Kafka replay batch size, Spark timeout, and a container-local checkpoint directory on the Spark work volume so it stays safe on laptop/WSL environments.

Outputs are written under `data/lakehouse/` by default. Run metadata is stored in `data/manifests/`.

## Key Documentation

- `docs/architecture.md`: visible and hidden system architecture
- `docs/pipeline.md`: Bronze, Silver, Gold contracts
- `docs/pipeline.md`: Bronze, Silver, Gold contracts and bounded Phase 2 refresh model
- `docs/gap_analysis.md`: current implementation status and remaining gaps
- `docs/local_setup.md`: local setup, sample data, and test commands
- `docs/docker_laptop_stack.md`: laptop-friendly Docker Compose multi-node simulation
- `docs/multi_node_readiness.md`: scaling path and shared-storage assumptions
- `docs/deployment_3node_ubuntu.md`: step-by-step 3-node Ubuntu deployment guide
- `docs/file_reference.md`: file roles and key classes/functions
- `docs/system_map.md`: file responsibilities
- `docs/improvements.md`: improvement and scaling recommendations
