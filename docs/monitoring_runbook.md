# Monitoring, pipeline metrics, and runbook

This document describes how to observe the ecommerce lakehouse pipelines, which metrics are emitted automatically, and how to respond to common failure modes.

## Where to look

| Artifact | Purpose |
|----------|---------|
| Run manifests (`MANIFEST_ROOT`, default `./data/manifests`) | Per-stage JSON files with inputs, outputs, row counts, and data-quality results. |
| Application logs | Standard logging from `common/logger.py`; search for `pipeline_metrics` for structured JSON lines. |
| Optional metrics file | Set `PIPELINE_METRICS_LOG_PATH` to append one JSON object per line for dashboards or SIEM. |

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `DQ_FAIL_ON_ERROR` | `true` | If `true`, Silver/Gold publication aborts when DQ checks fail (local JSONL and Spark/Iceberg). |
| `LOG_LEVEL` | `INFO` | Log verbosity. |
| `PIPELINE_METRICS_LOG_PATH` | *(empty)* | If set, append structured `pipeline_metrics` events to this file. |
| `MANIFEST_ROOT` | `./data/manifests` | Root directory for stage manifests. |

## Data quality gates

**Local pipeline (JSONL Bronze → Silver → Gold)**

- **Silver (before writing partitions):** event types must be `view`, `cart`, or `purchase`; `event_date` must match `event_time`; purchases must have a non-null, non-negative price.
- **Gold (before writing CSVs):** key counts and revenue fields must be non-negative in aggregated tables.

Implementation: `common/dq_checks.py`.

**Spark / Iceberg (`batch_backfill_to_iceberg`, streaming job)**

- **Silver:** same rules on the DataFrame about to be appended.
- **Gold:** non-negative checks on numeric columns per table before partition append.

Implementation: `infra/jobs/dq_iceberg.py`.

Failed runs write a manifest with `status: "failed"` and `details.failure` set to `silver_dq_gate` or `gold_dq_gate` where applicable. Metrics include `dq_passed` and per-rule results under `dq_rules`.

## Structured `pipeline_metrics` events

Successful and failed stages (after manifest write) emit a log line:

```text
pipeline_metrics {"details": {...}, "metrics": {...}, "run_id": "...", "stage": "...", "type": "pipeline_metrics"}
```

Downstream tools can grep or parse JSON after the `pipeline_metrics ` prefix.

## Runbook: common scenarios

### Silver publication blocked by DQ

1. Open the latest manifest under `manifests/bronze_to_silver/` for the failing `run_id`.
2. Inspect `metrics.dq_rules` for which rule failed.
3. Fix upstream Bronze data or transformation logic; re-run Bronze → Silver.
4. If investigating only, set `DQ_FAIL_ON_ERROR=false` to avoid raising after a failed manifest is written (publication still **does not** write Silver or Gold outputs when DQ fails).

### Gold publication blocked by DQ

1. Check `manifests/silver_to_gold/` for `status: "failed"` and `details.failure: "gold_dq_gate"`.
2. Validate Silver inputs for the run; confirm aggregates do not contain negative counts.
3. Re-run Silver → Gold after fixing Silver data.

### Iceberg job fails with `DQ gate failed`

1. Note the Spark exception message (invalid event types, date mismatch, bad purchase prices, or negative Gold metrics).
2. Query Bronze/Silver for the offending window or re-run with a smaller month range.
3. Temporarily set `DQ_FAIL_ON_ERROR=false` **only in non-production** to unblock investigation; restore `true` before production.

### Benchmarks

- **System (Iceberg vs Parquet):** `infra/jobs/benchmark_system_lakehouse_vs_parquet.py` — same dataset and transforms, default range 2019-10 .. 2020-02.
- **Processing model (batch vs streaming):** `infra/jobs/benchmark_batch_vs_streaming.py` — batch wall time for 2020-03 .. 2020-04 plus documented streaming latency/freshness/complexity factors.

## Suggested alerts (future)

- Manifest `status != succeeded` for scheduled stages.
- `dq_passed: false` in `pipeline_metrics` for any stage.
- Spark job duration or stage failure from your orchestrator (Airflow, Dagster, etc.).

## Related docs

- `docs/pipeline.md` — layer contracts and refresh model.
- `docs/architecture.md` — system layout.
