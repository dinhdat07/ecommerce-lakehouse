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

- `infra/jobs/batch_backfill_to_iceberg.py`: Phase 1 Spark batch job that materializes Bronze, Silver, and Gold as physical Iceberg tables.
- `pipelines/bronze/backfill.py`: legacy local JSONL Bronze path retained for lightweight non-Docker smoke tests.
- `pipelines/silver/transform.py`: canonicalization, validation, partitioning, deduplication, and quarantine handling for the local file-based path.
- `pipelines/gold/aggregate.py`: Gold table builders for the local file-based path and shared business-rule reference logic.

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
- `apps/sql/trino_gold_views.sql`: legacy serving-layer SQL views from the pre-materialized demo path.
- `infra/trino/sql/prepare_demo_views.sql`: legacy demo-view bootstrap kept for reference; not used by the Phase 1 Spark-only transformation flow.
