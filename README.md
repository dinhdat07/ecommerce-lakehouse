# ecommerce-lakehouse

Production-inspired Data Engineering scaffold for real-time and historical E-commerce behavior analytics.

## Scope

This repository is a starter skeleton for:
- Historical backfill from monthly `.csv.gz` files (Oct 2019 to Apr 2020)
- Streaming simulation for newer events
- Bronze / Silver / Gold data model
- Local-first development with small sample data

## Planned Stack

- Kafka: event ingestion
- Spark: batch and structured streaming jobs
- MinIO: S3-compatible object storage
- Iceberg: table format for Silver/Gold
- Trino: SQL query engine
- Superset: dashboards

## Repository Layout

- `docs/`: project definition, architecture, and pipeline design notes
- `data/raw/`: large source files (local/server mounted, generally not committed)
- `data/sample/`: tiny sample data for local development
- `configs/`: environment and runtime config files
- `scripts/`: helper scripts for local setup and job execution
- `apps/producer/`: Kafka producer/replay app
- `apps/batch/`: Spark batch pipeline entrypoints
- `apps/streaming/`: Spark structured streaming entrypoints
- `apps/sql/`: SQL assets for Trino/Spark SQL
- `common/`: reusable schema, transforms, constants, and logging
- `tests/`: unit tests for shared logic

## Quick Start (Local Skeleton)

1. Copy `.env.example` to `.env` and update values.
2. Generate sample input:
   ```bash
   python scripts/create_sample.py
   ```
3. Run local batch placeholders:
   ```bash
   bash scripts/run_local_batch.sh
   ```

## Notes

- This scaffold intentionally omits full business logic.
- Integration details for Kafka/Spark/MinIO/Iceberg are marked with TODOs.
- Keep config externalized through environment variables and files under `configs/`.
