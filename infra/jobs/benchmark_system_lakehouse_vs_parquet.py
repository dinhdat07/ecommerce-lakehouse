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
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from pyspark import StorageLevel

JOBS_DIR = Path(__file__).resolve().parent
ROOT = JOBS_DIR.parents[1]
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

import batch_backfill_to_iceberg as bb  # noqa: E402
import benchmark_input_samples as bis  # noqa: E402
from common.constants import ICEBERG_WAREHOUSE  # noqa: E402
from dq_iceberg import summarize_gold_dataframe_quality, summarize_silver_dataframe_quality  # noqa: E402


BENCHMARK_WRITE_PARTITIONS = int(os.getenv("BENCHMARK_WRITE_PARTITIONS", "4"))
SAFE_BENCHMARK_DELETE_PREFIXES = (
    "/tmp/benchmark",
    "/srv/ecommerce/benchmarks",
    "s3a://warehouse/benchmarks",
    "s3://warehouse/benchmarks",
)


def _apply_bench_table_names(prefix: str = "lakehouse.demo.bench_") -> None:
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
    bb.INCREMENTAL_GOLD_TABLES = [
        bb.GOLD_DAILY_REVENUE,
        bb.GOLD_TOP_PRODUCTS,
        bb.GOLD_CONVERSION_FUNNEL,
        bb.GOLD_CATEGORY_PERFORMANCE,
        bb.GOLD_SESSION_FUNNEL,
        bb.GOLD_USER_CONVERSION_PATH,
    ]
    bb.ADVANCED_GOLD_TABLES = [
        bb.GOLD_COHORT_RETENTION,
        bb.GOLD_REPEAT_PURCHASE,
        bb.GOLD_PRODUCT_AFFINITY,
        bb.GOLD_TIME_TO_CONVERSION_DISTRIBUTION,
        bb.GOLD_RFM_SEGMENTATION,
    ]
    bb.ALL_GOLD_TABLES = bb.INCREMENTAL_GOLD_TABLES + bb.ADVANCED_GOLD_TABLES


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
        bb.GOLD_COHORT_RETENTION,
        bb.GOLD_REPEAT_PURCHASE,
        bb.GOLD_PRODUCT_AFFINITY,
        bb.GOLD_TIME_TO_CONVERSION_DISTRIBUTION,
        bb.GOLD_RFM_SEGMENTATION,
    ]


def _drop_bench_tables(spark) -> None:
    for table_name in reversed(_bench_table_names()):
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def _flatten_for_csv(results: dict[str, Any]) -> dict[str, Any]:
    parquet = results.get("parquet") or {}
    return {
        "benchmark_mode": results["benchmark_mode"],
        "dataset_label": results["dataset"]["label"],
        "bronze_row_count": results["bronze_row_count"],
        "parquet_ingestion_seconds": parquet.get("ingestion_seconds", 0),
        "parquet_transformation_seconds": parquet.get("transformation_seconds", 0),
        "parquet_query_latency_seconds": parquet.get("query_latency_seconds_avg", 0),
        "parquet_storage_bytes": parquet.get("storage_bytes", 0),
        "parquet_throughput_rows_per_second": parquet.get("throughput_rows_per_second", 0),
        "iceberg_ingestion_seconds": results["iceberg"]["ingestion_seconds"],
        "iceberg_transformation_seconds": results["iceberg"]["transformation_seconds"],
        "iceberg_query_latency_seconds": results["iceberg"]["query_latency_seconds_avg"],
        "iceberg_storage_bytes": results["iceberg"]["storage_bytes"],
        "iceberg_throughput_rows_per_second": results["iceberg"]["throughput_rows_per_second"],
        "subset_enabled": results["subset"]["subset_enabled"],
        "subset_mode": results["subset"]["subset_mode"],
        "input_row_count": results["subset"]["input_row_count"],
        "benchmark_row_count": results["subset"]["benchmark_row_count"],
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


def _path_join(base: str, child: str) -> str:
    return f"{base.rstrip('/')}/{child}"


def _assert_safe_benchmark_path(path: str) -> None:
    normalized = path.rstrip("/")
    if not normalized or normalized in {"/", "/tmp", "/srv", "/srv/ecommerce", "s3a://warehouse", "s3://warehouse"}:
        raise ValueError(f"Refusing to delete unsafe benchmark path: {path}")
    if not any(normalized.startswith(prefix) for prefix in SAFE_BENCHMARK_DELETE_PREFIXES):
        raise ValueError(f"Refusing to delete non-benchmark path: {path}")


def _delete_output_path(spark, path: str) -> None:
    _assert_safe_benchmark_path(path)
    if "://" not in path:
        local_path = Path(path)
        if local_path.exists():
            import shutil

            shutil.rmtree(local_path)
        return

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(hadoop_conf)
    if fs.exists(target):
        fs.delete(target, True)


def _path_size_bytes(spark, path: str) -> int:
    if "://" not in path:
        return _local_dir_size_bytes(Path(path))
    return _hadoop_path_size_bytes(spark, path)


def _warehouse_table_path(table_name: str, warehouse_uri: str) -> str:
    _, schema, short_name = table_name.split(".", 2)
    warehouse_root = warehouse_uri.replace("s3://", "s3a://", 1).rstrip("/")
    return f"{warehouse_root}/{schema}/{short_name}"


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


def _git_commit() -> str:
    if os.getenv("GIT_COMMIT"):
        return os.environ["GIT_COMMIT"]
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    except Exception:
        return "unknown"


def _disk_usage_snapshot() -> dict[str, str]:
    paths = ["/", "/srv/ecommerce/spark-tmp", "/tmp"]
    snapshot: dict[str, str] = {}
    for path in paths:
        try:
            snapshot[path] = subprocess.check_output(["df", "-h", path], text=True).strip().splitlines()[-1]
        except Exception as exc:
            snapshot[path] = f"unavailable: {exc}"
    return snapshot


def _validate_parquet_warehouse(spark, warehouse: str) -> None:
    master = spark.sparkContext.master
    if not master.startswith("local") and not warehouse.startswith(("s3a://", "s3://")):
        raise SystemExit(
            f"Refusing distributed Parquet baseline on local/file path {warehouse!r}; use s3a://warehouse/benchmarks/..."
        )
    _assert_safe_benchmark_path(warehouse)


def _materialize_dataframe(dataframe):
    """Optionally pin benchmark dataframes; disabled by default to avoid local disk pressure; when enabled, prefer memory before spilling."""

    if os.getenv("BENCHMARK_CACHE_DATAFRAMES", "false").lower() in {"1", "true", "yes"}:
        return dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    return dataframe


def _stage_benchmark_input(spark, dataframe, args: argparse.Namespace) -> tuple[Any, dict[str, Any]]:
    """Optionally stage the benchmark subset once so later phases do not rescan gzip CSV."""

    staging_enabled = _parse_bool(args.staging_enabled)
    metadata: dict[str, Any] = {
        "staging_enabled": staging_enabled,
        "staging_path": args.staging_path if staging_enabled else None,
        "staging_seconds": 0.0,
        "staging_storage_bytes": 0,
    }
    if not staging_enabled:
        return dataframe, metadata

    if not args.staging_path:
        raise SystemExit("--staging-path is required when --staging-enabled=true")

    started = time.perf_counter()
    _delete_output_path(spark, args.staging_path)
    dataframe.write.mode("overwrite").format("parquet").save(args.staging_path)
    staged = spark.read.parquet(args.staging_path)
    metadata["staging_seconds"] = round(time.perf_counter() - started, 3)
    metadata["staging_storage_bytes"] = _path_size_bytes(spark, args.staging_path)
    return staged, metadata


def _partitioned_output(dataframe, partition_column: str):
    """Use narrow coalesce to avoid benchmark-only shuffle spill before partitioned writes."""

    return dataframe.coalesce(BENCHMARK_WRITE_PARTITIONS)


def _parse_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "false").lower() in {"1", "true", "yes", "y"}


def _rows_per_second(rows: int | float, seconds: int | float) -> float:
    seconds = float(seconds or 0)
    return round(float(rows or 0) / seconds, 3) if seconds > 0 else 0.0


def _cluster_specs(spark) -> dict[str, Any]:
    return {
        "cluster_topology": "3-node Spark standalone cluster",
        "node_count": 3,
        "node_memory_gb_each": "approximately 8",
        "node_root_disk_gb_each": "approximately 115",
        "spark_master": spark.sparkContext.master,
        "storage": "MinIO S3A warehouse",
        "catalog": "Iceberg JDBC catalog on Postgres",
    }


def _spark_config_snapshot(spark) -> dict[str, str | None]:
    conf = spark.sparkContext.getConf()
    keys = [
        "spark.master",
        "spark.driver.memory",
        "spark.executor.memory",
        "spark.executor.cores",
        "spark.default.parallelism",
        "spark.sql.shuffle.partitions",
        "spark.local.dir",
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        "spark.sql.files.maxPartitionBytes",
        "spark.sql.files.openCostInBytes",
        "spark.hadoop.fs.s3a.endpoint",
    ]
    return {key: conf.get(key, None) for key in keys}


def _benchmark_limitations(subset_enabled: bool, parquet_enabled: bool) -> list[str]:
    limitations = [
        "Benchmark uses the same Bronze/Silver/Gold transforms and dedupe semantics as the pipeline.",
        "Benchmark phases read from a reusable Parquet input sample when input-sample caching is enabled.",
        "Benchmark write fanout is intentionally small for the 3-node cluster.",
    ]
    if parquet_enabled:
        limitations.append("Parquet baseline output is written under s3a://warehouse/benchmarks/parquet.")
    else:
        limitations.append("Parquet baseline is disabled for this profile to avoid duplicate distributed output pressure.")
    if subset_enabled:
        limitations.append("Benchmark is a representative subset run, not an absolute full historical dataset benchmark.")
    return limitations


def _subset_metadata(args: argparse.Namespace, *, input_row_count: int, benchmark_row_count: int) -> dict[str, Any]:
    return {
        "subset_enabled": _parse_bool(args.subset_enabled),
        "subset_mode": args.subset_mode,
        "start_month": args.start_month,
        "end_month": args.end_month,
        "sample_fraction": float(args.sample_fraction),
        "sample_seed": int(args.sample_seed),
        "input_row_count": input_row_count,
        "benchmark_row_count": benchmark_row_count,
    }


def _apply_optional_fraction_subset(dataframe, args: argparse.Namespace):
    if _parse_bool(args.subset_enabled) and args.subset_mode == "fraction":
        return dataframe.sample(False, float(args.sample_fraction), int(args.sample_seed))
    return dataframe


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


def _run_parquet_baseline(spark, bronze_batch, warehouse: str) -> dict[str, Any]:
    _validate_parquet_warehouse(spark, warehouse)
    _delete_output_path(spark, warehouse)

    bronze_path = _path_join(warehouse, "bronze")
    silver_path = _path_join(warehouse, "silver")
    gold_root = _path_join(warehouse, "gold")

    ingest_started = time.perf_counter()
    bronze_rows = bronze_batch.count()
    _partitioned_output(bronze_batch, "source_month").write.mode("overwrite").partitionBy("source_month").format(
        "parquet"
    ).save(bronze_path)
    ingestion_seconds = round(time.perf_counter() - ingest_started, 3)

    transform_started = time.perf_counter()
    silver_base_rows = bb.build_silver_base_rows(bronze_batch)
    silver_candidates = _materialize_dataframe(bb.deduplicate_silver_candidates(silver_base_rows))
    silver_dq = summarize_silver_dataframe_quality(silver_base_rows, silver_candidates, stage="silver_parquet")
    _partitioned_output(silver_candidates, "event_date").write.mode("overwrite").partitionBy("event_date").format(
        "parquet"
    ).save(silver_path)

    silver_slice = _materialize_dataframe(spark.read.parquet(silver_path))
    sessionized = _materialize_dataframe(bb.build_sessionized_events(silver_slice))
    gold_tables = {
        "daily_revenue": bb.build_daily_revenue(silver_slice),
        "top_products": bb.build_top_products(silver_slice),
        "conversion_funnel_daily": bb.build_conversion_funnel_daily(silver_slice),
        "category_performance_daily": bb.build_category_performance_daily(silver_slice),
        "session_funnel": bb.build_session_funnel(sessionized),
        "user_conversion_path": bb.build_user_conversion_path(sessionized),
        "cohort_retention": bb.build_cohort_retention(silver_slice),
        "repeat_purchase": bb.build_repeat_purchase(silver_slice),
        "product_affinity": bb.build_product_affinity(sessionized),
        "time_to_conversion_distribution": bb.build_time_to_conversion_distribution(sessionized),
        "rfm_segmentation": bb.build_rfm_segmentation(silver_slice),
    }
    gold_specs = {
        "daily_revenue": ("daily_revenue", ("purchase_count", "purchase_revenue", "unique_buyers"), ("event_date",)),
        "top_products": ("top_products", ("views", "carts", "purchases", "purchase_revenue", "unique_users"), ("event_date",)),
        "conversion_funnel_daily": ("conversion_funnel_daily", ("views", "carts", "purchases"), ("event_date",)),
        "category_performance_daily": (
            "category_performance_daily",
            ("views", "carts", "purchases", "purchase_revenue", "unique_buyers"),
            ("event_date",),
        ),
        "session_funnel": (
            "session_funnel",
            ("total_events", "distinct_products", "views", "carts", "purchases", "purchase_revenue"),
            ("event_date",),
        ),
        "user_conversion_path": (
            "user_conversion_path",
            ("session_count", "views", "carts", "purchases", "purchase_revenue"),
            ("event_date",),
        ),
        "cohort_retention": ("cohort_retention", ("cohort_users", "active_users"), ("cohort_month", "period_offset")),
        "repeat_purchase": ("repeat_purchase", ("purchasers", "repeat_purchasers"), ("activity_month",)),
        "product_affinity": ("product_affinity", ("co_purchase_sessions",), ("product_a", "product_b")),
        "time_to_conversion_distribution": (
            "time_to_conversion_distribution",
            ("conversions",),
            ("event_date", "time_bucket"),
        ),
        "rfm_segmentation": (
            "rfm_segmentation",
            ("recency_days", "frequency_90d", "monetary_90d", "r_score", "f_score", "m_score"),
            ("as_of_date", "user_id"),
        ),
    }
    gold_dq: dict[str, dict[str, Any]] = {}
    gold_row_counts: dict[str, int] = {}
    for name, dataframe in gold_tables.items():
        row_count = dataframe.count()
        short_name, cols, required_cols = gold_specs[name]
        gold_dq[short_name] = summarize_gold_dataframe_quality(
            dataframe,
            name=short_name,
            columns=cols,
            required_columns=required_cols,
            stage="gold_parquet",
        )
        out = _path_join(gold_root, name)
        if row_count > 0:
            if "event_date" in dataframe.columns:
                dataframe = _partitioned_output(dataframe, "event_date")
                dataframe.write.mode("overwrite").partitionBy("event_date").format("parquet").save(out)
            else:
                dataframe.coalesce(2).write.mode("overwrite").format("parquet").save(out)
        gold_row_counts[name] = row_count
    transformation_seconds = round(time.perf_counter() - transform_started, 3)

    gold_tables["daily_revenue"].createOrReplaceTempView("bench_parquet_daily_revenue")
    gold_tables["top_products"].createOrReplaceTempView("bench_parquet_top_products")
    gold_tables["conversion_funnel_daily"].createOrReplaceTempView("bench_parquet_conversion_funnel_daily")
    gold_tables["repeat_purchase"].createOrReplaceTempView("bench_parquet_repeat_purchase")
    gold_tables["rfm_segmentation"].createOrReplaceTempView("bench_parquet_rfm_segmentation")
    query_latencies = _run_queries(
        spark,
        {
            "daily_revenue_sum": "SELECT round(sum(purchase_revenue), 2) AS revenue FROM bench_parquet_daily_revenue",
            "top_products_avg": "SELECT round(avg(purchase_revenue), 2) AS avg_revenue FROM bench_parquet_top_products",
            "funnel_totals": "SELECT sum(views) AS views, sum(purchases) AS purchases FROM bench_parquet_conversion_funnel_daily",
            "repeat_purchase_avg": "SELECT round(avg(repeat_purchase_rate), 4) AS rate FROM bench_parquet_repeat_purchase",
            "rfm_users": "SELECT count(*) AS users FROM bench_parquet_rfm_segmentation",
        },
    )

    total_runtime_seconds = round(ingestion_seconds + transformation_seconds + sum(query_latencies.values()), 3)
    return {
        "ingestion_seconds": ingestion_seconds,
        "transformation_seconds": transformation_seconds,
        "total_runtime_seconds": total_runtime_seconds,
        "throughput_rows_per_second": _rows_per_second(bronze_rows, total_runtime_seconds),
        "query_latency_seconds": query_latencies,
        "query_latency_seconds_avg": round(sum(query_latencies.values()) / len(query_latencies), 3),
        "storage_bytes": _path_size_bytes(spark, warehouse),
        "bronze_rows": bronze_rows,
        "silver_rows": silver_candidates.count(),
        "gold_row_counts": gold_row_counts,
        "silver_dq_passed": silver_dq["dq_passed"],
        "gold_dq_passed": all(summary["dq_passed"] for summary in gold_dq.values()),
        "storage_footprint": {
            "root_path": warehouse,
            "bytes": _path_size_bytes(spark, warehouse),
        },
    }


def _run_iceberg_lakehouse(
    spark,
    bronze_batch,
    *,
    warehouse_uri: str,
    gold_refresh_mode: str,
    drop_existing_tables: bool = True,
) -> dict[str, Any]:
    if drop_existing_tables:
        _drop_bench_tables(spark)
    bb.create_tables_if_needed(spark)
    result = bb.process_bronze_batch(
        spark,
        bronze_batch,
        append_bronze=True,
        bronze_partitions=BENCHMARK_WRITE_PARTITIONS,
        silver_partitions=BENCHMARK_WRITE_PARTITIONS,
        gold_refresh_mode=gold_refresh_mode,
        run_id="benchmark-iceberg",
    )
    query_latencies = _run_queries(
        spark,
        {
            "daily_revenue_sum": f"SELECT round(sum(purchase_revenue), 2) AS revenue FROM {bb.GOLD_DAILY_REVENUE}",
            "top_products_avg": f"SELECT round(avg(purchase_revenue), 2) AS avg_revenue FROM {bb.GOLD_TOP_PRODUCTS}",
            "funnel_totals": f"SELECT sum(views) AS views, sum(purchases) AS purchases FROM {bb.GOLD_CONVERSION_FUNNEL}",
            "repeat_purchase_avg": f"SELECT round(avg(repeat_purchase_rate), 4) AS rate FROM {bb.GOLD_REPEAT_PURCHASE}",
            "rfm_users": f"SELECT count(*) AS users FROM {bb.GOLD_RFM_SEGMENTATION}",
        },
    )
    storage_bytes = sum(
        _hadoop_path_size_bytes(spark, _warehouse_table_path(table_name, warehouse_uri))
        for table_name in _bench_table_names()
    )
    ingestion_seconds = float(result["bronze_metrics"]["duration_seconds"])
    transformation_seconds = round(
        float(result["silver_metrics"]["duration_seconds"]) + float(result["gold_metrics"]["duration_seconds"]),
        3,
    )
    total_runtime_seconds = round(ingestion_seconds + transformation_seconds + sum(query_latencies.values()), 3)
    bronze_rows = int(result["bronze_metrics"]["rows_written"])
    return {
        "ingestion_seconds": ingestion_seconds,
        "transformation_seconds": transformation_seconds,
        "total_runtime_seconds": total_runtime_seconds,
        "throughput_rows_per_second": _rows_per_second(bronze_rows, total_runtime_seconds),
        "query_latency_seconds": query_latencies,
        "query_latency_seconds_avg": round(sum(query_latencies.values()) / len(query_latencies), 3),
        "storage_bytes": storage_bytes,
        "bronze_rows": bronze_rows,
        "silver_rows": result["silver_metrics"]["rows_written"],
        "gold_row_counts": result["gold_row_counts"],
        "gold_refresh_mode": gold_refresh_mode,
        "gold_tables_skipped": result["gold_metrics"].get("gold_tables_skipped", 0),
        "gold_skip_reason": result["gold_metrics"].get("gold_skip_reason", ""),
        "silver_dq_passed": result["silver_dq_summary"]["dq_passed"],
        "gold_dq_passed": all(summary["dq_passed"] for summary in result["gold_dq_summaries"].values()),
    }


def _skipped_parquet_baseline(reason: str) -> dict[str, Any]:
    return {
        "enabled": False,
        "skipped": True,
        "skip_reason": reason,
        "ingestion_seconds": 0.0,
        "transformation_seconds": 0.0,
        "total_runtime_seconds": 0.0,
        "throughput_rows_per_second": 0.0,
        "query_latency_seconds": {},
        "query_latency_seconds_avg": 0.0,
        "storage_bytes": 0,
        "bronze_rows": 0,
        "silver_rows": 0,
        "gold_row_counts": {},
        "silver_dq_passed": None,
        "gold_dq_passed": None,
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
        "--parquet-baseline-enabled",
        default=os.getenv("BENCHMARK_PARQUET_BASELINE_ENABLED", "true"),
        help="Run the Parquet baseline. Disable for production-like Iceberg-only profiles to reduce disk pressure.",
    )
    parser.add_argument(
        "--warehouse-uri",
        default=ICEBERG_WAREHOUSE.replace("s3a://", "s3://"),
        help="Warehouse URI used to estimate Iceberg storage size.",
    )
    parser.add_argument("--subset-enabled", default="false", help="Whether this benchmark run uses a subset scope.")
    parser.add_argument("--subset-mode", choices=["month_range", "fraction"], default="month_range", help="Benchmark-only subset mode.")
    parser.add_argument("--sample-fraction", type=float, default=0.1, help="Fraction used when --subset-mode=fraction.")
    parser.add_argument("--sample-seed", type=int, default=42, help="Random seed used when --subset-mode=fraction.")
    parser.add_argument("--staging-enabled", default="false", help="Stage the selected benchmark input once before timed phases.")
    parser.add_argument("--staging-path", default="", help="S3A/local path for benchmark-only staged input.")
    parser.add_argument("--gold-refresh-mode", choices=sorted(bb.GOLD_REFRESH_MODES), default=os.getenv("SYSTEM_GOLD_REFRESH_MODE", "full"))
    parser.add_argument(
        "--iceberg-drop-existing-tables",
        default="true",
        help="Drop benchmark Iceberg tables before this run. Set false for month-chunked continuation runs.",
    )
    parser.add_argument("--profile", default=os.getenv("BENCHMARK_PROFILE", "ad-hoc"), help="Benchmark profile name.")
    parser.add_argument("--run-id", default=os.getenv("BENCHMARK_RUN_ID", ""), help="Benchmark run id.")
    parser.add_argument("--output-json", help="Optional JSON output path.")
    parser.add_argument("--output-csv", help="Optional CSV output path.")
    bis.add_input_sample_args(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = bb.build_spark_session()
    bb.log_spark_runtime_config(spark, output_path=args.output_json or args.parquet_warehouse)
    _apply_bench_table_names()
    disk_before = _disk_usage_snapshot()

    input_result = bis.resolve_benchmark_input(spark, args)
    mode = input_result["mode"]
    input_files = input_result["input_files"]
    missing_months = input_result["missing_months"]
    input_row_count = input_result["input_row_count"]
    raw_read_seconds = input_result["raw_read_seconds"]
    bronze_batch = input_result["bronze_batch"]
    staging_metadata = input_result["staging"]
    input_sample_metadata = input_result["input_sample"]
    bronze_batch = _materialize_dataframe(bronze_batch)
    bronze_row_count = int(input_result["benchmark_row_count"])
    subset_enabled = _parse_bool(args.subset_enabled)

    parquet_enabled = _parse_bool(args.parquet_baseline_enabled)
    if parquet_enabled:
        parquet_results = _run_parquet_baseline(spark, bronze_batch, args.parquet_warehouse)
    else:
        parquet_results = _skipped_parquet_baseline("disabled_by_profile_or_env")
    iceberg_results = _run_iceberg_lakehouse(
        spark,
        bronze_batch,
        warehouse_uri=args.warehouse_uri,
        gold_refresh_mode=args.gold_refresh_mode,
        drop_existing_tables=_parse_bool(args.iceberg_drop_existing_tables),
    )

    results = {
        "run_id": args.run_id or Path(args.output_json or "").parent.name or "ad-hoc",
        "git_commit": _git_commit(),
        "profile": args.profile,
        "benchmark_mode": mode,
        "dataset": {
            "label": f"{mode}:{args.start_month}..{args.end_month}" if mode == "full" else f"{mode}:{Path(input_files[0]).name}",
            "start_month": args.start_month,
            "end_month": args.end_month,
            "input_files": [path.name for path in input_files],
            "missing_months": missing_months,
            "input_row_count": input_row_count,
            "benchmark_row_count": bronze_row_count,
            "raw_read_seconds": raw_read_seconds,
            **staging_metadata,
        },
        "input_sample": input_sample_metadata,
        "subset": _subset_metadata(args, input_row_count=input_row_count, benchmark_row_count=bronze_row_count),
        "cluster_specs": _cluster_specs(spark),
        "spark_config": _spark_config_snapshot(spark),
        "disk_usage": {
            "before": disk_before,
            "after": _disk_usage_snapshot(),
        },
        "limitations": _benchmark_limitations(subset_enabled, parquet_enabled),
        "bronze_row_count": bronze_row_count,
        "parquet_baseline_enabled": parquet_enabled,
        "gold_refresh_mode": args.gold_refresh_mode,
        "parquet": parquet_results,
        "iceberg": iceberg_results,
    }
    _write_results(results, output_json=args.output_json, output_csv=args.output_csv)
    print(json.dumps(results, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
