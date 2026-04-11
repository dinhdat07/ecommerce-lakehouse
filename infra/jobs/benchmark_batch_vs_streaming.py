"""Secondary benchmark: batch backfill vs bounded Kafka streaming (replay).

**Batch (2020-03 .. 2020-04)** — measure end-to-end wall time for
``batch_backfill_to_iceberg`` over the same months as a replay window.

**Streaming** — ``kafka_stream_to_iceberg`` processes micro-batches with
``processingTime`` triggers and ``maxOffsetsPerTrigger``; latency and freshness
are dominated by trigger interval, consumer lag, and Silver/Gold work per batch.

This script prints a JSON summary for the **batch** path. For streaming, it
documents the knobs to compare against batch (no second Spark cluster assumed).

Example::

    python infra/jobs/benchmark_batch_vs_streaming.py \\
        --input-dir /workspace/data/raw \\
        --start-month 2020-03 --end-month 2020-04
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

JOBS_DIR = Path(__file__).resolve().parent
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

import batch_backfill_to_iceberg as bb  # noqa: E402


def _apply_bench_table_names(prefix: str = "lakehouse.demo.bench_") -> None:
    bb.BRONZE_TABLE = f"{prefix}bronze_events"
    bb.SILVER_TABLE = f"{prefix}silver_events"
    bb.GOLD_DAILY_REVENUE = f"{prefix}daily_revenue"
    bb.GOLD_TOP_PRODUCTS = f"{prefix}top_products"
    bb.GOLD_CONVERSION_FUNNEL = f"{prefix}conversion_funnel_daily"
    bb.GOLD_CATEGORY_PERFORMANCE = f"{prefix}category_performance_daily"
    bb.GOLD_SESSION_FUNNEL = f"{prefix}session_funnel"
    bb.GOLD_USER_CONVERSION_PATH = f"{prefix}user_conversion_path"


def _drop_bench_tables(spark) -> None:
    for table_name in [
        bb.GOLD_USER_CONVERSION_PATH,
        bb.GOLD_SESSION_FUNNEL,
        bb.GOLD_CATEGORY_PERFORMANCE,
        bb.GOLD_CONVERSION_FUNNEL,
        bb.GOLD_TOP_PRODUCTS,
        bb.GOLD_DAILY_REVENUE,
        bb.SILVER_TABLE,
        bb.BRONZE_TABLE,
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare batch timing vs streaming parameters (documented).")
    parser.add_argument("--input-dir", default="/workspace/data/raw", help="Raw monthly CSV directory.")
    parser.add_argument("--start-month", default="2020-03", help="Inclusive YYYY-MM (replay window start).")
    parser.add_argument("--end-month", default="2020-04", help="Inclusive YYYY-MM (replay window end).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = bb.build_spark_session()
    _apply_bench_table_names()
    _drop_bench_tables(spark)
    bb.create_tables_if_needed(spark)

    input_dir = Path(args.input_dir)
    input_files, missing_months = bb.discover_input_files(input_dir, args.start_month, args.end_month)
    if not input_files:
        raise SystemExit(f"No input files under {input_dir} for {args.start_month}..{args.end_month}")

    batch_run_id = f"bench-batch-{args.start_month}-{args.end_month}"
    bronze_batch = bb.read_bronze_batch(spark, input_files, batch_run_id, source_month_override=None)

    t0 = time.perf_counter()
    result = bb.process_bronze_batch(
        spark,
        bronze_batch,
        append_bronze=True,
        bronze_partitions=8,
        silver_partitions=8,
    )
    elapsed = time.perf_counter() - t0

    streaming_notes = {
        "job": "infra/jobs/kafka_stream_to_iceberg.py",
        "latency_drivers": [
            "trigger interval (--trigger-seconds): upper bound on micro-batch latency",
            "maxOffsetsPerTrigger: caps batch size; smaller batches improve freshness, increase overhead",
            "foreachBatch Silver/Gold path matches batch transforms (same complexity per batch)",
        ],
        "freshness": "time from Kafka record availability to Iceberg Gold append ≈ batch trigger + processing time",
        "operational_complexity": "streaming adds checkpoints, consumer groups, and replay discipline vs single batch job",
    }

    out = {
        "mode": "batch_backfill",
        "range": {"start_month": args.start_month, "end_month": args.end_month},
        "input_files": [path.name for path in input_files],
        "missing_months": missing_months,
        "wall_seconds": round(elapsed, 3),
        "affected_gold_dates": len(result["affected_dates"]),
        "streaming_comparison": streaming_notes,
    }
    print(json.dumps(out, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
