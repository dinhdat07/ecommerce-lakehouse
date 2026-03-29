# System Map

This document records the role of the main files added during the lakehouse implementation.

## Shared Runtime

- `pyproject.toml`: package metadata, optional dependency groups, and default test/lint settings.
- `common/config.py`: central `AppConfig` loader and logical-storage to local-path mapping.
- `common/runtime.py`: run IDs, UTC timestamps, and directory bootstrap helpers.
- `common/manifests.py`: JSON run manifests used for hidden-layer observability and replay tracing.
- `common/storage.py`: JSONL/CSV readers and writers for local Bronze/Silver/Gold persistence.
- `common/quality.py`: timestamp parsing, numeric coercion, null normalization, and dedupe hashing.

## Pipeline Core

- `pipelines/bronze/backfill.py`: historical CSV and CSV.GZ ingestion into append-only Bronze JSONL.
- `pipelines/silver/transform.py`: canonicalization, validation, partitioning, deduplication, and quarantine handling.
- `pipelines/gold/aggregate.py`: Gold table builders for user, product, funnel, category, and session analytics.

## Entrypoints

- `apps/batch/backfill_bronze.py`: operator CLI for historical Bronze backfills.
- `apps/batch/bronze_to_silver.py`: operator CLI for batch Silver canonicalization.
- `apps/batch/silver_to_gold.py`: operator CLI for batch Gold aggregation.
- `apps/producer/replay_csv_to_kafka.py`: Kafka replay producer with a local fallback bus when Kafka is unavailable.
- `apps/streaming/kafka_to_bronze.py`: local-bus or future Kafka ingestion entrypoint for streaming-friendly Bronze ingestion.
- `apps/streaming/bronze_to_silver_stream.py`: micro-batch compatible Bronze-to-Silver entrypoint.
- `apps/streaming/silver_to_gold_stream.py`: micro-batch compatible Silver-to-Gold entrypoint.

## Operator Assets

- `scripts/run_local_batch.sh`: local batch smoke path from sample input to Gold outputs.
- `scripts/run_local_streaming.sh`: local replay-bus simulation for the streaming path.
- `apps/sql/trino_gold_views.sql`: serving-layer SQL views aligned with the Gold contracts.
