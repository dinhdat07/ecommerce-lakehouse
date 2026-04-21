"""Secondary benchmark: one-shot batch backfill vs bounded microbatch replay.

This benchmark compares processing models on the same March-April slice:

- ``batch`` processes the full input in one Bronze -> Silver -> Gold pass.
- ``streaming`` replays the same input as sequential bounded microbatches using
  the existing incremental pipeline logic.

It is intentionally focused on latency, freshness, and operational complexity.
Kafka transport overhead is validated separately by the Phase 2 demo scripts.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Any

from pyspark.sql import Window
from pyspark.sql import functions as F

JOBS_DIR = Path(__file__).resolve().parent
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

import batch_backfill_to_iceberg as bb  # noqa: E402


def _apply_bench_table_names(prefix: str) -> None:
    bb.BRONZE_TABLE = f"{prefix}bronze_events"
    bb.SILVER_TABLE = f"{prefix}silver_events"
    bb.GOLD_DAILY_REVENUE = f"{prefix}daily_revenue"
    bb.GOLD_TOP_PRODUCTS = f"{prefix}top_products"
    bb.GOLD_CONVERSION_FUNNEL = f"{prefix}conversion_funnel_daily"
    bb.GOLD_CATEGORY_PERFORMANCE = f"{prefix}category_performance_daily"
    bb.GOLD_SESSION_FUNNEL = f"{prefix}session_funnel"
    bb.GOLD_USER_CONVERSION_PATH = f"{prefix}user_conversion_path"
    bb.GOLD_COHORT_RETENTION = f"{prefix}cohort_retention"
    bb.GOLD_REPEAT_PURCHASE = f"{prefix}repeat_purchase"
    bb.GOLD_PRODUCT_AFFINITY = f"{prefix}product_affinity"
    bb.GOLD_TIME_TO_CONVERSION_DISTRIBUTION = f"{prefix}time_to_conversion_distribution"
    bb.GOLD_RFM_SEGMENTATION = f"{prefix}rfm_segmentation"


def _drop_current_bench_tables(spark) -> None:
    for table_name in [
        bb.GOLD_RFM_SEGMENTATION,
        bb.GOLD_TIME_TO_CONVERSION_DISTRIBUTION,
        bb.GOLD_PRODUCT_AFFINITY,
        bb.GOLD_REPEAT_PURCHASE,
        bb.GOLD_COHORT_RETENTION,
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


def _resolve_input_dataset(spark, args: argparse.Namespace) -> tuple[str, list[Path], list[str], Any]:
    input_dir = Path(args.input_dir)
    input_files, missing_months = bb.discover_input_files(input_dir, args.start_month, args.end_month)
    if args.mode == "full" and not input_files:
        raise SystemExit(f"No input files under {input_dir} for {args.start_month}..{args.end_month}")

    if args.mode == "demo" or (args.mode == "auto" and not input_files):
        sample_path = Path(args.sample_file)
        if not sample_path.exists():
            raise SystemExit(f"Demo sample file not found: {sample_path}")
        bronze_batch = bb.read_bronze_batch(
            spark,
            [sample_path],
            batch_run_id=f"bench-stream-demo-{args.demo_source_month.replace('-', '')}",
            source_month_override=args.demo_source_month,
        )
        return ("demo", [sample_path], [], bronze_batch)

    bronze_batch = bb.read_bronze_batch(
        spark,
        input_files,
        batch_run_id=f"bench-stream-full-{args.start_month}-{args.end_month}",
        source_month_override=None,
    )
    return ("full", input_files, missing_months, bronze_batch)


def _write_results(results: dict[str, Any], *, output_json: str | None, output_csv: str | None) -> None:
    if output_json:
        path = Path(output_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    if output_csv:
        path = Path(output_csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "benchmark_mode": results["benchmark_mode"],
            "dataset_label": results["dataset"]["label"],
            "batch_wall_seconds": results["batch"]["wall_seconds"],
            "batch_silver_rows_written": results["batch"]["silver_rows_written"],
            "streaming_wall_seconds": results["streaming"]["wall_seconds"],
            "streaming_microbatches": results["streaming"]["microbatches"],
            "streaming_avg_latency_seconds": results["streaming"]["avg_microbatch_latency_seconds"],
            "streaming_avg_freshness_seconds": results["streaming"]["avg_freshness_seconds"],
        }
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            writer.writeheader()
            writer.writerow(row)


def _run_batch_path(spark, bronze_batch) -> dict[str, Any]:
    _apply_bench_table_names("lakehouse.demo.bench_batch_")
    _drop_current_bench_tables(spark)
    bb.create_tables_if_needed(spark)

    started = time.perf_counter()
    result = bb.process_bronze_batch(
        spark,
        bronze_batch,
        append_bronze=True,
        bronze_partitions=8,
        silver_partitions=8,
        run_id="benchmark-batch-model",
    )
    wall_seconds = round(time.perf_counter() - started, 3)
    return {
        "wall_seconds": wall_seconds,
        "bronze_rows_written": result["bronze_metrics"]["rows_written"],
        "silver_rows_written": result["silver_metrics"]["rows_written"],
        "gold_rows_written": result["gold_metrics"]["rows_written"],
        "affected_dates": result["affected_dates"],
        "dq_passed": bool(result["silver_dq_summary"]["dq_passed"])
        and all(summary["dq_passed"] for summary in result["gold_dq_summaries"].values()),
    }


def _run_streaming_path(spark, bronze_batch, *, microbatch_size: int) -> dict[str, Any]:
    _apply_bench_table_names("lakehouse.demo.bench_stream_")
    _drop_current_bench_tables(spark)
    bb.create_tables_if_needed(spark)

    numbered = bronze_batch.withColumn(
        "_bench_row_num",
        F.row_number().over(Window.orderBy(F.monotonically_increasing_id())),
    )
    total_rows = numbered.count()

    batch_latencies: list[float] = []
    batch_freshness: list[float] = []
    affected_dates: set[str] = set()
    silver_rows_written = 0
    gold_rows_written = 0
    microbatches = 0

    started = time.perf_counter()
    for lower in range(1, total_rows + 1, microbatch_size):
        upper = lower + microbatch_size
        microbatch_df = numbered.where(
            (F.col("_bench_row_num") >= F.lit(lower)) & (F.col("_bench_row_num") < F.lit(upper))
        ).drop("_bench_row_num")
        microbatch_started = time.perf_counter()
        result = bb.process_bronze_batch(
            spark,
            microbatch_df,
            append_bronze=True,
            bronze_partitions=4,
            silver_partitions=4,
            run_id=f"benchmark-stream-model-{microbatches:06d}",
        )
        latency = round(time.perf_counter() - microbatch_started, 3)
        batch_latencies.append(latency)
        batch_freshness.append(latency)
        affected_dates.update(result["affected_dates"])
        silver_rows_written += int(result["silver_metrics"]["rows_written"])
        gold_rows_written += int(result["gold_metrics"]["rows_written"])
        microbatches += 1

    wall_seconds = round(time.perf_counter() - started, 3)
    return {
        "wall_seconds": wall_seconds,
        "microbatches": microbatches,
        "avg_microbatch_latency_seconds": round(sum(batch_latencies) / len(batch_latencies), 3) if batch_latencies else 0.0,
        "max_microbatch_latency_seconds": round(max(batch_latencies), 3) if batch_latencies else 0.0,
        "avg_freshness_seconds": round(sum(batch_freshness) / len(batch_freshness), 3) if batch_freshness else 0.0,
        "max_freshness_seconds": round(max(batch_freshness), 3) if batch_freshness else 0.0,
        "silver_rows_written": silver_rows_written,
        "gold_rows_written": gold_rows_written,
        "affected_dates": sorted(affected_dates),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare batch processing vs bounded microbatch replay.")
    parser.add_argument("--input-dir", default="/workspace/data/raw", help="Raw monthly CSV directory.")
    parser.add_argument("--start-month", default="2020-03", help="Inclusive YYYY-MM (window start).")
    parser.add_argument("--end-month", default="2020-04", help="Inclusive YYYY-MM (window end).")
    parser.add_argument("--mode", choices=["auto", "full", "demo"], default="auto", help="Benchmark dataset mode.")
    parser.add_argument(
        "--sample-file",
        default="/workspace/data/sample/events_streaming_demo_1500.csv",
        help="Demo-scale fallback sample used when raw March-April files are unavailable.",
    )
    parser.add_argument(
        "--demo-source-month",
        default="2020-04",
        help="Logical source month used when benchmarking from a demo sample file.",
    )
    parser.add_argument("--microbatch-size", type=int, default=500, help="Rows per bounded replay microbatch.")
    parser.add_argument("--output-json", help="Optional JSON output path.")
    parser.add_argument("--output-csv", help="Optional CSV output path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = bb.build_spark_session()

    mode, input_files, missing_months, bronze_batch = _resolve_input_dataset(spark, args)
    bronze_row_count = bronze_batch.count()

    results = {
        "benchmark_mode": mode,
        "dataset": {
            "label": f"{mode}:{args.start_month}..{args.end_month}" if mode == "full" else f"{mode}:{Path(input_files[0]).name}",
            "start_month": args.start_month,
            "end_month": args.end_month,
            "input_files": [path.name for path in input_files],
            "missing_months": missing_months,
            "bronze_row_count": bronze_row_count,
        },
        "batch": _run_batch_path(spark, bronze_batch),
        "streaming": _run_streaming_path(spark, bronze_batch, microbatch_size=args.microbatch_size),
        "comparison": {
            "latency_tradeoff": "Streaming favors smaller per-batch latency and freshness at the cost of more batch overhead.",
            "freshness_tradeoff": "Batch publishes after the full window completes; microbatch replay publishes incrementally after each chunk.",
            "complexity_tradeoff": "Streaming requires checkpoints and replay discipline, while batch remains simpler to operate.",
            "notes": "This benchmark isolates the Spark processing model. Kafka broker overhead is validated separately through the Phase 2 demo.",
        },
    }
    _write_results(results, output_json=args.output_json, output_csv=args.output_csv)
    print(json.dumps(results, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
