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
2. Run the local batch flow:
   ```bash
   bash scripts/run_local_batch.sh
   ```
3. Run the streaming-friendly local replay flow:
   ```bash
   bash scripts/run_local_streaming.sh
   ```

Outputs are written under `data/lakehouse/` by default. Run metadata is stored in `data/manifests/`.

## Key Documentation

- `docs/architecture.md`: visible and hidden system architecture
- `docs/pipeline.md`: Bronze, Silver, Gold contracts
- `docs/system_map.md`: file responsibilities
- `docs/improvements.md`: improvement and scaling recommendations
