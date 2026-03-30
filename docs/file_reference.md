# File Reference

This document summarizes the important added or modified files, their roles, and the main classes or functions they expose.

## Shared Core

- `common/constants.py`
  - Role: central environment-backed constants.
  - Main items: storage roots, Kafka settings, Spark settings, Iceberg settings.
- `common/config.py`
  - Role: build the runtime configuration and map logical URIs to local paths.
  - Main items:
    - `AppConfig`: immutable configuration object used by every stage.
    - `materialize_logical_uri()`: maps `s3a://...` style URIs onto local paths.
    - `load_config()`: builds the configuration from environment values and overrides.
- `common/runtime.py`
  - Role: shared operational helpers.
  - Main items: `utc_now()`, `utc_now_iso()`, `build_run_id()`, `ensure_directories()`.
- `common/manifests.py`
  - Role: persist stage manifests for traceability.
  - Main items:
    - `RunManifest`: structured metadata for one run.
    - `write_manifest()`: writes the manifest JSON to disk.
- `common/storage.py`
  - Role: simple JSONL and CSV persistence layer used by the local backend.
  - Main items: `write_jsonl()`, `append_jsonl()`, `read_jsonl()`, `iter_jsonl_files()`, `write_csv()`.
- `common/quality.py`
  - Role: hidden-layer data quality and normalization helpers.
  - Main items: `parse_event_timestamp()`, `normalize_nullable_text()`, `coerce_int()`, `coerce_float()`, `build_event_identity()`, `derive_event_date()`.

## Pipeline Modules

- `pipelines/bronze/backfill.py`
  - Role: ingest CSV and CSV.GZ inputs into append-only Bronze records.
  - Main items:
    - `BronzeBackfillResult`: result summary returned to callers.
    - `discover_input_files()`: finds supported input files.
    - `iter_bronze_rows()`: yields Bronze records with source metadata.
    - `run_bronze_backfill()`: executes the full Bronze backfill stage.
- `pipelines/silver/transform.py`
  - Role: canonicalize Bronze records into Silver events and quarantine invalid rows.
  - Main items:
    - `SilverTransformResult`: result summary for the stage.
    - `canonicalize_record()`: validates and transforms one Bronze record.
    - `run_bronze_to_silver()`: executes the full Silver transformation.
- `pipelines/gold/aggregate.py`
  - Role: compute BI-facing Gold tables from Silver events.
  - Main items:
    - `GoldAggregationResult`: result summary for the aggregation stage.
    - `build_gold_tables()`: computes all Gold tables in memory from canonical events.
    - `run_silver_to_gold()`: writes Gold outputs and manifest metadata.

## Entrypoints and Scripts

- `apps/batch/backfill_bronze.py`
  - Role: operator CLI for batch Bronze backfills.
  - Main function: `main()` loads config, executes the Bronze stage, and logs outputs.
- `apps/batch/bronze_to_silver.py`
  - Role: operator CLI for batch Silver transformation.
  - Main function: `main()` runs the shared Silver stage and reports quality counts.
- `apps/batch/silver_to_gold.py`
  - Role: operator CLI for batch Gold aggregation.
  - Main function: `main()` runs the shared Gold stage and logs written tables.
- `apps/producer/replay_csv_to_kafka.py`
  - Role: replay sample CSV rows to Kafka or a local fallback bus.
  - Main items: `default_input_csv()`, `iter_csv_rows()`, `_write_local_bus()`, `main()`.
- `apps/streaming/kafka_to_bronze.py`
  - Role: consume the local fallback bus and feed Bronze ingestion.
  - Main items: `build_spark_session()` for future backend compatibility and `main()` for the local path.
- `apps/streaming/bronze_to_silver_stream.py`
  - Role: streaming-friendly Silver entrypoint reusing the same transformation core.
- `apps/streaming/silver_to_gold_stream.py`
  - Role: streaming-friendly Gold entrypoint reusing the same aggregation core.
- `scripts/run_local_batch.sh`
  - Role: simple sample-data batch run.
- `scripts/run_local_streaming.sh`
  - Role: simple streaming-friendly run using the local replay bus.
- `scripts/clean_local_state.sh`
  - Role: clear local outputs, manifests, and replay checkpoints.
- `scripts/bootstrap_local.sh`
  - Role: create `.venv` and install local development dependencies.
- `scripts/verify_local.py`
  - Role: zero-dependency verification of the local batch round trip.

## Tooling and Deployment Assets

- `pyproject.toml`
  - Role: package metadata, optional dependency groups, pytest settings, and lint settings.
- `requirements-dev.txt`
  - Role: practical install list for test and lint tooling.
- `requirements-platform.txt`
  - Role: optional platform dependencies for Kafka and Spark integration.
- `Makefile`
  - Role: ergonomic commands for bootstrap, testing, cleanup, and local runs.
- `infra/docker-compose.yml`
  - Role: laptop-friendly multi-node simulation stack with MinIO, Kafka, Spark master, and one or two Spark workers.
- `infra/scripts/common.sh`
  - Role: Docker Compose helper wrapper that supports either `docker compose` or `docker-compose`.
- `infra/scripts/up-core.sh`
  - Role: start the lightweight default compose stack.
- `infra/scripts/up-extended.sh`
  - Role: start the stack with a second Spark worker for a more realistic multi-node simulation.
- `infra/scripts/status.sh`
  - Role: show the current compose service state.
- `infra/scripts/verify.sh`
  - Role: smoke-test Kafka topics, MinIO buckets, and Spark worker registration.
- `infra/scripts/down.sh`
  - Role: stop the compose stack without deleting persistent volumes.
- `infra/scripts/purge.sh`
  - Role: remove the compose stack, its volumes, and its service images to reclaim disk space.
- `apps/sql/trino_gold_views.sql`
  - Role: serving-layer SQL view definitions aligned with Gold contracts.

## Documentation

- `README.md`: repository overview and quick start.
- `docs/gap_analysis.md`: current implementation status and remaining gaps.
- `docs/local_setup.md`: local environment, sample data, and test instructions.
- `docs/docker_laptop_stack.md`: laptop-friendly compose architecture, usage, and cleanup guidance.
- `docs/multi_node_readiness.md`: current scaling posture and migration path.
- `docs/deployment_3node_ubuntu.md`: step-by-step 3-server deployment guide.
- `docs/improvements.md`: limitations and recommended next improvements.
