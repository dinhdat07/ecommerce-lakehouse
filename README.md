# ecommerce-lakehouse

Local-first e-commerce lakehouse for user behavior analytics with Bronze, Silver, and Gold data contracts.

## Current Implementation

- Historical ingestion from CSV and CSV.GZ into append-only Bronze JSONL
- Canonical Silver transformation with validation, normalization, and quarantine handling
- Gold analytics outputs for user, product, funnel, category, and session metrics
- Streaming-friendly local replay bus that mirrors the future Kafka path
- Run manifests, serving SQL, and architecture/operator documentation

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

To run the laptop-friendly multi-node demo with Iceberg, Trino, and Superset:

```bash
bash infra/scripts/up-bi.sh
bash infra/scripts/run-e2e-demo.sh
```

Then open `http://localhost:8088`, sign in with `admin` / `admin`, and open `/superset/dashboard/lakehouse-sample-dashboard/`.

Outputs are written under `data/lakehouse/` by default. Run metadata is stored in `data/manifests/`.

## Key Documentation

- `docs/architecture.md`: visible and hidden system architecture
- `docs/pipeline.md`: Bronze, Silver, Gold contracts
- `docs/gap_analysis.md`: current implementation status and remaining gaps
- `docs/local_setup.md`: local setup, sample data, and test commands
- `docs/docker_laptop_stack.md`: laptop-friendly Docker Compose multi-node simulation
- `docs/multi_node_readiness.md`: scaling path and shared-storage assumptions
- `docs/deployment_3node_ubuntu.md`: step-by-step 3-node Ubuntu deployment guide
- `docs/file_reference.md`: file roles and key classes/functions
- `docs/system_map.md`: file responsibilities
- `docs/improvements.md`: improvement and scaling recommendations
