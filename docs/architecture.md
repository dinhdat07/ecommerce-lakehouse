# Architecture Overview

## Components

- **Ingestion**: Kafka receives event stream; CSV replay script simulates events for local tests.
- **Storage**: MinIO (S3-compatible) stores Bronze/Silver/Gold datasets.
- **Processing**: Spark batch and structured streaming jobs.
- **Table Format**: Iceberg for managed Silver/Gold tables and schema evolution.
- **Serving**: Trino queries curated tables; Superset consumes Trino datasets.

## Data Flow

1. Raw files and event stream are ingested into Bronze.
2. Bronze records are cleaned and typed into Silver canonical events.
3. Silver is aggregated into Gold analytics tables.
4. BI tools query Gold through Trino.

## Design Principles

- Local-first development with tiny sample data.
- Config externalization through environment variables and config files.
- Modular code layout by concern (`common/` vs job entrypoints).
