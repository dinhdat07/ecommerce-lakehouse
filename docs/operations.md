# Monitoring, pipeline metrics, and runbook

This document describes the lightweight observability used by the Spark/Iceberg pipelines in Phase 3.

## Where to look

| Artifact | Purpose |
|----------|---------|
| Run manifests (`MANIFEST_ROOT`, default `./data/manifests`) | One JSON file per stage run such as `bronze_ingest_iceberg`, `silver_publish_iceberg`, `gold_refresh_iceberg`, `batch_backfill_iceberg`, `streaming_microbatch_iceberg`, and `streaming_run_iceberg`. |
| Application logs | Standard logs from `common/logger.py`; search for `pipeline_metrics` to find structured JSON events. |
| Optional metrics file | Set `PIPELINE_METRICS_LOG_PATH` to append one JSON object per line for later parsing. |

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `DQ_FAIL_ON_ERROR` | `true` | Fail the publish step on critical DQ issues. Warning-only rules are still logged. |
| `LOG_LEVEL` | `INFO` | Log verbosity. |
| `PIPELINE_METRICS_LOG_PATH` | *(empty)* | Optional JSONL sink for `pipeline_metrics` events. |
| `MANIFEST_ROOT` | `./data/manifests` | Root directory for manifest files. |

## Data quality coverage

`common/dq_checks.py` defines the rule model used by the local pipeline. `infra/jobs/dq_iceberg.py` applies the equivalent checks to Spark DataFrames.

Silver checks:
- required-column validation
- critical null checks for `event_time`, `event_date`, and `event_type`
- invalid timestamp detection
- event-type allowlist validation
- duplicate detection summary
- purchase price anomaly checks

Gold checks:
- required-column validation
- `event_date` null checks
- non-negative aggregate metrics per Gold table

Critical failures stop publication. Warning-only findings such as dropped duplicates and filtered invalid source timestamps are recorded in manifests and metrics but do not abort the run.

## Structured metrics

Every recorded stage emits a `pipeline_metrics` JSON payload with:
- `stage`
- `run_id`
- row counters
- stage duration
- DQ pass/fail fields
- optional `details.manifest` path

Example stages:
- `bronze_ingest_iceberg`
- `silver_publish_iceberg`
- `gold_refresh_iceberg`
- `batch_backfill_iceberg`
- `streaming_microbatch_iceberg`
- `streaming_run_iceberg`

## Runbook

### Batch pipeline run

1. Start the Docker stack with `bash infra/scripts/up-bi.sh` or `bash infra/scripts/up-core.sh`.
2. Run the batch pipeline with `bash infra/scripts/run-e2e-demo.sh` or the manual full backfill script.
3. Inspect `data/manifests/` for stage results and grep logs or the metrics file for `pipeline_metrics`.

### Streaming pipeline run

1. Start the required services with `bash infra/scripts/run-streaming-demo.sh`.
2. Inspect `data/manifests/streaming_microbatch_iceberg/` for per-batch stats.
3. Check `data/manifests/streaming_run_iceberg/` for the run summary.

### Reset and recovery

- Clean demo state: `bash infra/scripts/reset-demo-state.sh`
- Stop containers, keep volumes: `bash infra/scripts/down.sh`
- Remove containers, volumes, and service images: `bash infra/scripts/purge.sh`
- Recover Silver/Gold from Bronze only: rerun `batch_backfill_to_iceberg.py --resume-from silver`

### Common failures

Silver publish fails:
1. Open the latest file in `data/manifests/silver_publish_iceberg/`.
2. Inspect `metrics.dq_rules` and `details.bad_record_samples`.
3. Fix the upstream data or transformation logic, then rerun the stage.

Gold refresh fails:
1. Open the latest file in `data/manifests/gold_refresh_iceberg/`.
2. Check which Gold table reported negative metrics or missing required fields.
3. Rebuild from Silver after the Silver issue is corrected.

Streaming run fails:
1. Inspect the latest `streaming_run_iceberg` and `streaming_microbatch_iceberg` manifests.
2. Verify the checkpoint path is writable and Kafka replay input is bounded.
3. Reset the demo state before rerunning if partial output should be discarded.

## Benchmarks

- System benchmark: `infra/jobs/benchmark_system_lakehouse_vs_parquet.py`
- Processing-model benchmark: `infra/jobs/benchmark_batch_vs_streaming.py`
- Docker wrapper: `bash infra/scripts/run-phase3-benchmarks.sh`

See `docs/benchmarking.md` for dual-mode usage, output files, and interpretation notes.
