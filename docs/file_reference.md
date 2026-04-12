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
- `common/observability.py`
  - Role: shared wrapper for stage manifests and structured `pipeline_metrics` events.
  - Main items:
    - `stage_timer()`: starts wall-clock timing for a stage.
    - `stage_elapsed_seconds()`: converts a stage timer into seconds.
    - `record_stage_event()`: writes the manifest and emits the matching metrics payload.
- `common/storage.py`
  - Role: simple JSONL and CSV persistence layer used by the local backend.
  - Main items: `write_jsonl()`, `append_jsonl()`, `read_jsonl()`, `iter_jsonl_files()`, `write_csv()`.
- `common/quality.py`
  - Role: hidden-layer data quality and normalization helpers.
  - Main items: `parse_event_timestamp()`, `normalize_nullable_text()`, `coerce_int()`, `coerce_float()`, `build_event_identity()`, `derive_event_date()`.
- `common/dq_checks.py`
  - Role: shared DQ rule definitions for local publication gates.
  - Main items: `DQRuleResult`, `DQReport`, `validate_silver_rows_for_publication()`, `validate_gold_tables_for_publication()`.

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
  - Role: replay bounded sample CSV rows to Kafka with batch pacing suitable for laptop demos.
  - Main items:
    - `default_input_csv()`: selects the preferred sample file for replay.
    - `derive_source_month()`: infers `YYYY-MM` from `event_time`.
    - `build_message()`: wraps each CSV row in the replay envelope consumed by Spark.
    - `send_with_console_producer()`: fallback Kafka sender using the containerized console producer.
    - `main()`: runs the bounded replay.
- `apps/streaming/kafka_to_bronze.py`
  - Role: legacy local fallback entrypoint retained for non-Docker development.
- `apps/streaming/bronze_to_silver_stream.py`
  - Role: legacy local fallback Silver entrypoint retained for non-Docker development.
- `apps/streaming/silver_to_gold_stream.py`
  - Role: legacy local fallback Gold entrypoint retained for non-Docker development.
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
- `infra/jobs/batch_backfill_to_iceberg.py`
  - Role: Phase 1 Spark job that materializes Bronze, Silver, and Gold as physical Iceberg tables from historical CSV inputs.
  - Main items:
    - `build_silver_base_rows()`: normalizes Bronze rows before deduplication and DQ.
    - `deduplicate_silver_candidates()`: derives rerun-safe Silver candidates.
    - `process_bronze_batch()`: runs Bronze, Silver, and Gold with stage manifests and metrics.
- `infra/jobs/kafka_stream_to_iceberg.py`
  - Role: Phase 2 Spark Structured Streaming job that consumes Kafka replay events and incrementally refreshes Bronze, Silver, and Gold.
  - Main items:
    - `parse_replay_messages()`: parses Kafka JSON envelopes into typed rows.
    - `process_microbatch()`: projects the Bronze micro-batch and reuses the shared Phase 1 transformation logic.
    - `main()`: runs the bounded streaming query with configurable timeout and checkpointing.
- `infra/jobs/dq_iceberg.py`
  - Role: Spark-side DQ summaries for Silver and Gold publication.
  - Main items:
    - `summarize_silver_dataframe_quality()`: returns counts, rules, and bad-record samples for Silver.
    - `summarize_gold_dataframe_quality()`: returns counts, rules, and bad-record samples for Gold tables.
- `infra/jobs/benchmark_system_lakehouse_vs_parquet.py`
  - Role: Phase 3 system benchmark for Spark + Iceberg vs Spark + Parquet.
  - Main items:
    - `_resolve_input_dataset()`: selects full or demo benchmark input.
    - `_run_parquet_baseline()`: writes the Parquet benchmark outputs with the same Spark transforms and DQ checks.
    - `_run_iceberg_lakehouse()`: runs the Iceberg benchmark path and captures stage metrics.
- `infra/jobs/benchmark_batch_vs_streaming.py`
  - Role: Phase 3 processing-model benchmark for one-shot batch vs bounded microbatch replay.
  - Main items:
    - `_run_batch_path()`: measures the single-pass batch path.
    - `_run_streaming_path()`: measures bounded microbatch replay with the same incremental pipeline logic.
- `infra/scripts/run-streaming-demo.sh`
  - Role: bounded orchestration for the laptop-safe Phase 2 demo.
  - Main steps: generate March-April sample data, start the required Compose profiles, recreate the Kafka topic, launch the Spark streaming job, replay the sample to Kafka, and wait for the bounded query to finish.
- `infra/scripts/run-phase3-benchmarks.sh`
  - Role: Docker wrapper for the Phase 3 system and processing-model benchmarks.
- `apps/sql/trino_gold_views.sql`
  - Role: legacy serving-layer SQL view definitions retained for reference from the earlier view-based demo path.

## Documentation

- `README.md`: repository overview and quick start.
- `docs/gap_analysis.md`: current implementation status and remaining gaps.
- `docs/local_setup.md`: local environment, sample data, and test instructions.
- `docs/monitoring_runbook.md`: stage metrics, manifests, DQ rules, reset, and recovery guidance.
- `docs/benchmarking.md`: benchmark modes, commands, output files, and interpretation notes.
- `docs/docker_laptop_stack.md`: laptop-friendly compose architecture, usage, and cleanup guidance.
- `docs/multi_node_readiness.md`: current scaling posture and migration path.
- `docs/deployment_3node_ubuntu.md`: step-by-step 3-server deployment guide.
- `docs/improvements.md`: limitations and recommended next improvements.
