# Architecture Overview

## Components

- **Historical ingestion**: `apps/batch/backfill_bronze.py` ingests monthly CSV or CSV.GZ files into append-only Bronze JSONL.
- **Streaming ingress**: `apps/producer/replay_csv_to_kafka.py` replays events to Kafka when available, or to a local replay bus for local simulation.
- **Processing core**: shared pipeline modules under `pipelines/` apply the same normalization, validation, dedupe, and aggregation logic for batch and streaming-friendly entrypoints.
- **Storage**: logical stage URIs remain MinIO/S3-friendly while local runs materialize them under `data/lakehouse/`.
- **Serving**: Gold analytics tables are emitted as stable contracts with matching Trino view definitions in `apps/sql/trino_gold_views.sql`.

## Visible Layer

- Batch and streaming-friendly CLIs.
- Bronze, Silver, and Gold datasets with predictable partition layouts.
- Gold tables: `user_activity_summary`, `product_popularity`, `conversion_funnel`, `revenue_by_category`, and `session_summary`.
- Trino-facing SQL views aligned to future Iceberg tables.

## Hidden Layer

- Deterministic run IDs and JSON manifests for every stage.
- Timestamp parsing across both sample ISO timestamps and historical `UTC` strings.
- Required-column enforcement, null normalization, type coercion, and record hashing.
- Dedupe keys, Silver quarantine outputs, and future-ready checkpoint locations.

## Data Flow

1. Historical files or replayed events are materialized into append-only Bronze rows with source metadata.
2. Bronze payloads are canonicalized into Silver events partitioned by `event_date`.
3. Invalid rows are written to Silver quarantine for later review.
4. Silver events are aggregated into Gold CSV contracts for analytics and BI.
5. Future Trino and Superset layers consume the same Gold contracts after migration to Iceberg.
