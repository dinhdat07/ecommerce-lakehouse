"""System benchmark: Spark + Iceberg (lakehouse) vs Spark + Parquet (baseline).

Uses the same raw monthly files, the same Silver/Gold transforms as
``batch_backfill_to_iceberg.py``, and the default range **2019-10 .. 2020-02**.

Run inside the project Spark + Iceberg environment (for example the Docker
stack) so the Iceberg catalog is available.

Example::

    python infra/jobs/benchmark_system_lakehouse_vs_parquet.py \\
        --input-dir /workspace/data/raw \\
        --start-month 2019-10 --end-month 2020-02
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


def _run_parquet_baseline(spark, bronze_batch, warehouse: Path) -> tuple[float, dict[str, object]]:
    """Write Silver + Gold as Parquet only (no Iceberg catalog)."""

    t0 = time.perf_counter()
    silver_candidates = bb.build_silver_candidates(bronze_batch)
    silver_path = warehouse / "silver"
    silver_candidates.write.mode("overwrite").partitionBy("event_date").format("parquet").save(str(silver_path))

    silver_slice = spark.read.parquet(str(silver_path))
    affected_dates = sorted(
        str(row["event_date"]) for row in silver_slice.select("event_date").distinct().collect()
    )
    if not affected_dates:
        elapsed = time.perf_counter() - t0
        return elapsed, {"silver_rows": 0, "gold_tables": 0}

    sessionized = bb.build_sessionized_events(silver_slice)
    gold_tables = {
        "daily_revenue": bb.build_daily_revenue(silver_slice),
        "top_products": bb.build_top_products(silver_slice),
        "conversion_funnel": bb.build_conversion_funnel_daily(silver_slice),
        "category_performance": bb.build_category_performance_daily(silver_slice),
        "session_funnel": bb.build_session_funnel(sessionized),
        "user_conversion_path": bb.build_user_conversion_path(sessionized),
    }
    gold_written = 0
    for name, dataframe in gold_tables.items():
        if dataframe.limit(1).count() == 0:
            continue
        out = warehouse / "gold" / name
        dataframe.write.mode("overwrite").partitionBy("event_date").format("parquet").save(str(out))
        gold_written += 1

    elapsed = time.perf_counter() - t0
    silver_rows = silver_slice.count()
    return elapsed, {"silver_rows": silver_rows, "gold_tables": gold_written, "affected_dates": len(affected_dates)}


def _run_iceberg_lakehouse(spark, bronze_batch) -> tuple[float, dict[str, object]]:
    _drop_bench_tables(spark)
    bb.create_tables_if_needed(spark)
    batch_run_id = "benchmark-iceberg"
    t0 = time.perf_counter()
    result = bb.process_bronze_batch(
        spark,
        bronze_batch,
        append_bronze=True,
        bronze_partitions=8,
        silver_partitions=8,
    )
    elapsed = time.perf_counter() - t0
    details = {
        "affected_dates": len(result["affected_dates"]),
        "gold_tables": len(result["gold_row_counts"]),
    }
    return elapsed, details


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Iceberg vs Parquet with identical transforms.")
    parser.add_argument("--input-dir", default="/workspace/data/raw", help="Directory with monthly CSV files.")
    parser.add_argument("--start-month", default="2019-10", help="Inclusive YYYY-MM lower bound.")
    parser.add_argument("--end-month", default="2020-02", help="Inclusive YYYY-MM upper bound.")
    parser.add_argument(
        "--parquet-warehouse",
        default="/tmp/benchmark_parquet_warehouse",
        help="Directory for Parquet baseline outputs.",
    )
    parser.add_argument("--skip-iceberg", action="store_true", help="Only run the Parquet baseline.")
    parser.add_argument("--skip-parquet", action="store_true", help="Only run the Iceberg path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = bb.build_spark_session()
    _apply_bench_table_names()

    input_dir = Path(args.input_dir)
    input_files, missing_months = bb.discover_input_files(input_dir, args.start_month, args.end_month)
    if not input_files:
        raise SystemExit(f"No input files under {input_dir} for {args.start_month}..{args.end_month}")

    batch_run_id = f"bench-{args.start_month}-{args.end_month}"
    bronze_batch = bb.read_bronze_batch(spark, input_files, batch_run_id, source_month_override=None)
    bronze_count = bronze_batch.count()

    results: dict[str, object] = {
        "range": {"start_month": args.start_month, "end_month": args.end_month},
        "input_files": [path.name for path in input_files],
        "missing_months": missing_months,
        "bronze_row_count": bronze_count,
        "parquet_seconds": None,
        "iceberg_seconds": None,
        "parquet_details": None,
        "iceberg_details": None,
    }

    if not args.skip_parquet:
        pw = Path(args.parquet_warehouse)
        pw.mkdir(parents=True, exist_ok=True)
        pq_sec, pq_details = _run_parquet_baseline(spark, bronze_batch, pw)
        results["parquet_seconds"] = round(pq_sec, 3)
        results["parquet_details"] = pq_details

    if not args.skip_iceberg:
        ig_sec, ig_details = _run_iceberg_lakehouse(spark, bronze_batch)
        results["iceberg_seconds"] = round(ig_sec, 3)
        results["iceberg_details"] = ig_details

    if (
        results["parquet_seconds"] is not None
        and results["iceberg_seconds"] is not None
        and results["iceberg_seconds"] > 0
    ):
        results["iceberg_vs_parquet_ratio"] = round(float(results["iceberg_seconds"]) / float(results["parquet_seconds"]), 4)

    print(json.dumps(results, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
