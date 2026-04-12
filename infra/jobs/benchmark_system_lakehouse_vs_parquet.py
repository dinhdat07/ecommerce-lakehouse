"""System benchmark: Spark + Iceberg (lakehouse) vs Spark + Parquet (baseline).

The benchmark runs in two modes:

- ``full`` uses external raw files for the target historical range.
- ``demo`` falls back to a smaller checked-in sample file for local smoke tests.

Both paths reuse the same Spark Silver/Gold transforms and the same DQ checks.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
import time
from pathlib import Path
from typing import Any

JOBS_DIR = Path(__file__).resolve().parent
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

import batch_backfill_to_iceberg as bb  # noqa: E402
from common.constants import ICEBERG_WAREHOUSE  # noqa: E402
from dq_iceberg import summarize_gold_dataframe_quality, summarize_silver_dataframe_quality  # noqa: E402


def _apply_bench_table_names(prefix: str = "lakehouse.demo.bench_") -> None:
    bb.BRONZE_TABLE = f"{prefix}bronze_events"
    bb.SILVER_TABLE = f"{prefix}silver_events"
    bb.GOLD_DAILY_REVENUE = f"{prefix}daily_revenue"
    bb.GOLD_TOP_PRODUCTS = f"{prefix}top_products"
    bb.GOLD_CONVERSION_FUNNEL = f"{prefix}conversion_funnel_daily"
    bb.GOLD_CATEGORY_PERFORMANCE = f"{prefix}category_performance_daily"
    bb.GOLD_SESSION_FUNNEL = f"{prefix}session_funnel"
    bb.GOLD_USER_CONVERSION_PATH = f"{prefix}user_conversion_path"


def _bench_table_names() -> list[str]:
    return [
        bb.BRONZE_TABLE,
        bb.SILVER_TABLE,
        bb.GOLD_DAILY_REVENUE,
        bb.GOLD_TOP_PRODUCTS,
        bb.GOLD_CONVERSION_FUNNEL,
        bb.GOLD_CATEGORY_PERFORMANCE,
        bb.GOLD_SESSION_FUNNEL,
        bb.GOLD_USER_CONVERSION_PATH,
    ]


def _drop_bench_tables(spark) -> None:
    for table_name in reversed(_bench_table_names()):
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def _flatten_for_csv(results: dict[str, Any]) -> dict[str, Any]:
    return {
        "benchmark_mode": results["benchmark_mode"],
        "dataset_label": results["dataset"]["label"],
        "bronze_row_count": results["bronze_row_count"],
        "parquet_ingestion_seconds": results["parquet"]["ingestion_seconds"],
        "parquet_transformation_seconds": results["parquet"]["transformation_seconds"],
        "parquet_query_latency_seconds": results["parquet"]["query_latency_seconds_avg"],
        "parquet_storage_bytes": results["parquet"]["storage_bytes"],
        "iceberg_ingestion_seconds": results["iceberg"]["ingestion_seconds"],
        "iceberg_transformation_seconds": results["iceberg"]["transformation_seconds"],
        "iceberg_query_latency_seconds": results["iceberg"]["query_latency_seconds_avg"],
        "iceberg_storage_bytes": results["iceberg"]["storage_bytes"],
    }


def _write_results(results: dict[str, Any], *, output_json: str | None, output_csv: str | None) -> None:
    if output_json:
        path = Path(output_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    if output_csv:
        path = Path(output_csv)
        path.parent.mkdir(parents=True, exist_ok=True)
        row = _flatten_for_csv(results)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            writer.writeheader()
            writer.writerow(row)


def _local_dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(candidate.stat().st_size for candidate in path.rglob("*") if candidate.is_file())


def _warehouse_table_path(table_name: str, warehouse_uri: str) -> str:
    _, schema, short_name = table_name.split(".", 2)
    warehouse_root = warehouse_uri.rstrip("/")
    return f"{warehouse_root}/{schema}.db/{short_name}"


def _hadoop_path_size_bytes(spark, uri: str) -> int:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hadoop_conf)
    if not fs.exists(path):
        return 0
    return int(fs.getContentSummary(path).getLength())


def _run_queries(spark, queries: dict[str, str]) -> dict[str, float]:
    latencies: dict[str, float] = {}
    for name, query in queries.items():
        started = time.perf_counter()
        spark.sql(query).collect()
        latencies[name] = round(time.perf_counter() - started, 3)
    return latencies


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
            batch_run_id=f"bench-demo-{args.demo_source_month.replace('-', '')}",
            source_month_override=args.demo_source_month,
        )
        return (
            "demo",
            [sample_path],
            [],
            bronze_batch,
        )

    bronze_batch = bb.read_bronze_batch(
        spark,
        input_files,
        batch_run_id=f"bench-full-{args.start_month}-{args.end_month}",
        source_month_override=None,
    )
    return ("full", input_files, missing_months, bronze_batch)


def _run_parquet_baseline(spark, bronze_batch, warehouse: Path) -> dict[str, Any]:
    if warehouse.exists():
        shutil.rmtree(warehouse)
    warehouse.mkdir(parents=True, exist_ok=True)

    bronze_path = warehouse / "bronze"
    silver_path = warehouse / "silver"
    gold_root = warehouse / "gold"

    ingest_started = time.perf_counter()
    bronze_rows = bronze_batch.count()
    bronze_batch.write.mode("overwrite").partitionBy("source_month").format("parquet").save(str(bronze_path))
    ingestion_seconds = round(time.perf_counter() - ingest_started, 3)

    transform_started = time.perf_counter()
    silver_base_rows = bb.build_silver_base_rows(bronze_batch)
    silver_candidates = bb.deduplicate_silver_candidates(silver_base_rows)
    silver_dq = summarize_silver_dataframe_quality(silver_base_rows, silver_candidates, stage="silver_parquet")
    silver_candidates.write.mode("overwrite").partitionBy("event_date").format("parquet").save(str(silver_path))

    silver_slice = spark.read.parquet(str(silver_path))
    sessionized = bb.build_sessionized_events(silver_slice)
    gold_tables = {
        "daily_revenue": bb.build_daily_revenue(silver_slice),
        "top_products": bb.build_top_products(silver_slice),
        "conversion_funnel_daily": bb.build_conversion_funnel_daily(silver_slice),
        "category_performance_daily": bb.build_category_performance_daily(silver_slice),
        "session_funnel": bb.build_session_funnel(sessionized),
        "user_conversion_path": bb.build_user_conversion_path(sessionized),
    }
    gold_specs = {
        "daily_revenue": ("daily_revenue", ("purchase_count", "purchase_revenue", "unique_buyers")),
        "top_products": ("top_products", ("views", "carts", "purchases", "purchase_revenue", "unique_users")),
        "conversion_funnel_daily": ("conversion_funnel_daily", ("views", "carts", "purchases")),
        "category_performance_daily": (
            "category_performance_daily",
            ("views", "carts", "purchases", "purchase_revenue", "unique_buyers"),
        ),
        "session_funnel": (
            "session_funnel",
            ("total_events", "distinct_products", "views", "carts", "purchases", "purchase_revenue"),
        ),
        "user_conversion_path": (
            "user_conversion_path",
            ("session_count", "views", "carts", "purchases", "purchase_revenue"),
        ),
    }
    gold_dq: dict[str, dict[str, Any]] = {}
    gold_row_counts: dict[str, int] = {}
    for name, dataframe in gold_tables.items():
        row_count = dataframe.count()
        short_name, cols = gold_specs[name]
        gold_dq[short_name] = summarize_gold_dataframe_quality(
            dataframe,
            name=short_name,
            columns=cols,
            stage="gold_parquet",
        )
        out = gold_root / name
        if row_count > 0:
            dataframe.write.mode("overwrite").partitionBy("event_date").format("parquet").save(str(out))
        gold_row_counts[name] = row_count
    transformation_seconds = round(time.perf_counter() - transform_started, 3)

    gold_tables["daily_revenue"].createOrReplaceTempView("bench_parquet_daily_revenue")
    gold_tables["top_products"].createOrReplaceTempView("bench_parquet_top_products")
    gold_tables["conversion_funnel_daily"].createOrReplaceTempView("bench_parquet_conversion_funnel_daily")
    query_latencies = _run_queries(
        spark,
        {
            "daily_revenue_sum": "SELECT round(sum(purchase_revenue), 2) AS revenue FROM bench_parquet_daily_revenue",
            "top_products_avg": "SELECT round(avg(purchase_revenue), 2) AS avg_revenue FROM bench_parquet_top_products",
            "funnel_totals": "SELECT sum(views) AS views, sum(purchases) AS purchases FROM bench_parquet_conversion_funnel_daily",
        },
    )

    return {
        "ingestion_seconds": ingestion_seconds,
        "transformation_seconds": transformation_seconds,
        "query_latency_seconds": query_latencies,
        "query_latency_seconds_avg": round(sum(query_latencies.values()) / len(query_latencies), 3),
        "storage_bytes": _local_dir_size_bytes(warehouse),
        "bronze_rows": bronze_rows,
        "silver_rows": silver_candidates.count(),
        "gold_row_counts": gold_row_counts,
        "silver_dq_passed": silver_dq["dq_passed"],
        "gold_dq_passed": all(summary["dq_passed"] for summary in gold_dq.values()),
    }


def _run_iceberg_lakehouse(spark, bronze_batch, *, warehouse_uri: str) -> dict[str, Any]:
    _drop_bench_tables(spark)
    bb.create_tables_if_needed(spark)
    result = bb.process_bronze_batch(
        spark,
        bronze_batch,
        append_bronze=True,
        bronze_partitions=8,
        silver_partitions=8,
        run_id="benchmark-iceberg",
    )
    query_latencies = _run_queries(
        spark,
        {
            "daily_revenue_sum": f"SELECT round(sum(purchase_revenue), 2) AS revenue FROM {bb.GOLD_DAILY_REVENUE}",
            "top_products_avg": f"SELECT round(avg(purchase_revenue), 2) AS avg_revenue FROM {bb.GOLD_TOP_PRODUCTS}",
            "funnel_totals": f"SELECT sum(views) AS views, sum(purchases) AS purchases FROM {bb.GOLD_CONVERSION_FUNNEL}",
        },
    )
    storage_bytes = sum(
        _hadoop_path_size_bytes(spark, _warehouse_table_path(table_name, warehouse_uri))
        for table_name in _bench_table_names()
    )
    return {
        "ingestion_seconds": result["bronze_metrics"]["duration_seconds"],
        "transformation_seconds": round(
            float(result["silver_metrics"]["duration_seconds"]) + float(result["gold_metrics"]["duration_seconds"]),
            3,
        ),
        "query_latency_seconds": query_latencies,
        "query_latency_seconds_avg": round(sum(query_latencies.values()) / len(query_latencies), 3),
        "storage_bytes": storage_bytes,
        "bronze_rows": result["bronze_metrics"]["rows_written"],
        "silver_rows": result["silver_metrics"]["rows_written"],
        "gold_row_counts": result["gold_row_counts"],
        "silver_dq_passed": result["silver_dq_summary"]["dq_passed"],
        "gold_dq_passed": all(summary["dq_passed"] for summary in result["gold_dq_summaries"].values()),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark Iceberg vs Parquet with identical Spark transforms.")
    parser.add_argument("--input-dir", default="/workspace/data/raw", help="Directory with monthly CSV files.")
    parser.add_argument("--start-month", default="2019-10", help="Inclusive YYYY-MM lower bound.")
    parser.add_argument("--end-month", default="2020-02", help="Inclusive YYYY-MM upper bound.")
    parser.add_argument("--mode", choices=["auto", "full", "demo"], default="auto", help="Benchmark dataset mode.")
    parser.add_argument(
        "--sample-file",
        default="/workspace/data/sample/events_sample_100k.csv",
        help="Demo-scale fallback sample used when historical raw files are unavailable.",
    )
    parser.add_argument(
        "--demo-source-month",
        default="2020-04",
        help="Logical source month used when benchmarking from a demo sample file.",
    )
    parser.add_argument(
        "--parquet-warehouse",
        default="/tmp/benchmark_parquet_warehouse",
        help="Directory for Parquet baseline outputs.",
    )
    parser.add_argument(
        "--warehouse-uri",
        default=ICEBERG_WAREHOUSE.replace("s3a://", "s3://"),
        help="Warehouse URI used to estimate Iceberg storage size.",
    )
    parser.add_argument("--output-json", help="Optional JSON output path.")
    parser.add_argument("--output-csv", help="Optional CSV output path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = bb.build_spark_session()
    _apply_bench_table_names()

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
        },
        "bronze_row_count": bronze_row_count,
        "parquet": _run_parquet_baseline(spark, bronze_batch, Path(args.parquet_warehouse)),
        "iceberg": _run_iceberg_lakehouse(spark, bronze_batch, warehouse_uri=args.warehouse_uri),
    }
    _write_results(results, output_json=args.output_json, output_csv=args.output_csv)
    print(json.dumps(results, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
