# Architecture Overview

## Components

- **Historical ingestion**: `infra/jobs/batch_backfill_to_iceberg.py` ingests monthly CSV or CSV.GZ files into physical Bronze Iceberg tables.
- **Streaming ingress**: `apps/producer/replay_csv_to_kafka.py` replays events to Kafka when available, or to a local replay bus for local simulation.
- **Processing core**: Spark performs Bronze to Silver normalization, deduplication, sessionization, and Gold aggregations.
- **Storage**: MinIO stores Iceberg table data under the shared `warehouse` bucket, with Iceberg catalog metadata stored in Postgres.
- **Serving**: Trino queries physical Iceberg Bronze, Silver, and Gold tables, and Superset connects to Trino for dashboards.

## Visible Layer

- Batch-first historical backfill CLI for `2019-10` to `2020-02`.
- Physical Bronze, Silver, and Gold Iceberg tables with stable table names.
- Gold tables: `daily_revenue`, `top_products`, `conversion_funnel_daily`, `category_performance_daily`, `session_funnel`, and `user_conversion_path`.
- Trino SQL access and a Superset sample dashboard backed by materialized Gold tables.

## Hidden Layer

- Deterministic `record_hash` and `dedupe_key` identities for rerun-safe Bronze and incremental Silver inserts.
- Timestamp parsing across both sample ISO timestamps and historical `UTC` strings.
- Null normalization, type coercion, and event-type canonicalization before analytics.
- Session resolution that uses `user_session` first and derives fallback sessions with a 30-minute inactivity rule only when needed.

## Data Flow

1. Historical CSV files are read by Spark and inserted into `lakehouse.demo.bronze_events` with source metadata.
2. The same source batch is normalized, deduplicated, and incrementally inserted into `lakehouse.demo.silver_events`.
3. Only `event_date` partitions touched by newly inserted Silver rows are recomputed for Gold.
4. Spark materializes Gold analytics into Iceberg tables in the same catalog.
5. Trino and Superset consume those physical Iceberg tables directly in Phase 1.
