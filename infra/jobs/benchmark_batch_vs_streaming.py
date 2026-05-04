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
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StructField, StructType

JOBS_DIR = Path(__file__).resolve().parent
ROOT = JOBS_DIR.parents[1]
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

import batch_backfill_to_iceberg as bb  # noqa: E402
import benchmark_input_samples as bis  # noqa: E402


BENCHMARK_WRITE_PARTITIONS = int(os.getenv("BENCHMARK_WRITE_PARTITIONS", "4"))
SAFE_BENCHMARK_DELETE_PREFIXES = (
    "/tmp/benchmark",
    "/srv/ecommerce/benchmarks",
    "s3a://warehouse/benchmarks",
    "s3://warehouse/benchmarks",
)


def _materialize_dataframe(dataframe):
    """Optionally pin benchmark dataframes; disabled by default to avoid local disk pressure; when enabled, prefer memory before spilling."""

    if os.getenv("BENCHMARK_CACHE_DATAFRAMES", "false").lower() in {"1", "true", "yes"}:
        return dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    return dataframe


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
        local_path = Path(path)
        if not local_path.exists():
            return 0
        return sum(candidate.stat().st_size for candidate in local_path.rglob("*") if candidate.is_file())

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(hadoop_conf)
    if not fs.exists(target):
        return 0
    return int(fs.getContentSummary(target).getLength())


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


def _with_bench_row_numbers(bronze_batch):
    """Build exact microbatch row ids without the global window sort cost."""

    numbered_schema = StructType(list(bronze_batch.schema.fields) + [StructField("_bench_row_num", LongType(), False)])
    numbered_rdd = bronze_batch.rdd.zipWithIndex().map(lambda pair: tuple(pair[0]) + (pair[1] + 1,))
    return bronze_batch.sparkSession.createDataFrame(numbered_rdd, schema=numbered_schema)


def _parse_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "false").lower() in {"1", "true", "yes", "y"}


def _rows_per_second(rows: int | float, seconds: int | float) -> float:
    seconds = float(seconds or 0)
    return round(float(rows or 0) / seconds, 3) if seconds > 0 else 0.0


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


def _benchmark_limitations(subset_enabled: bool) -> list[str]:
    limitations = [
        "Benchmark uses the same Bronze/Silver/Gold transforms and dedupe semantics as the pipeline.",
        "Benchmark phases read from a reusable Parquet input sample when input-sample caching is enabled.",
        "Benchmark write fanout is intentionally small for the 3-node cluster.",
        "Streaming replay is bounded and writes only benchmark-prefixed Iceberg tables.",
    ]
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
            "batch_throughput_rows_per_second": results["batch"]["throughput_rows_per_second"],
            "streaming_throughput_rows_per_second": results["streaming"]["throughput_rows_per_second"],
            "subset_enabled": results["subset"]["subset_enabled"],
            "subset_mode": results["subset"]["subset_mode"],
            "input_row_count": results["subset"]["input_row_count"],
            "benchmark_row_count": results["subset"]["benchmark_row_count"],
        }
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            writer.writeheader()
            writer.writerow(row)


def _skipped_batch_path(reason: str) -> dict[str, Any]:
    return {
        "enabled": False,
        "skipped": True,
        "skip_reason": reason,
        "wall_seconds": 0.0,
        "bronze_rows_written": 0,
        "silver_rows_written": 0,
        "gold_rows_written": 0,
        "affected_dates": [],
        "dq_passed": None,
    }


def _run_batch_path(spark, bronze_batch, *, gold_refresh_mode: str) -> dict[str, Any]:
    _apply_bench_table_names("lakehouse.demo.bench_batch_")
    _drop_current_bench_tables(spark)
    bb.create_tables_if_needed(spark)

    started = time.perf_counter()
    result = bb.process_bronze_batch(
        spark,
        bronze_batch,
        append_bronze=True,
        bronze_partitions=BENCHMARK_WRITE_PARTITIONS,
        silver_partitions=BENCHMARK_WRITE_PARTITIONS,
        gold_refresh_mode=gold_refresh_mode,
        run_id="benchmark-batch-model",
    )
    wall_seconds = round(time.perf_counter() - started, 3)
    return {
        "enabled": True,
        "skipped": False,
        "wall_seconds": wall_seconds,
        "bronze_rows_written": result["bronze_metrics"]["rows_written"],
        "silver_rows_written": result["silver_metrics"]["rows_written"],
        "gold_rows_written": result["gold_metrics"]["rows_written"],
        "affected_dates": result["affected_dates"],
        "dq_passed": bool(result["silver_dq_summary"]["dq_passed"])
        and all(summary["dq_passed"] for summary in result["gold_dq_summaries"].values()),
    }


def _run_streaming_path(spark, bronze_batch, *, microbatch_size: int, gold_refresh_mode: str) -> dict[str, Any]:
    _apply_bench_table_names("lakehouse.demo.bench_stream_")
    _drop_current_bench_tables(spark)
    bb.create_tables_if_needed(spark)

    numbered = _materialize_dataframe(_with_bench_row_numbers(bronze_batch))
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
            bronze_partitions=BENCHMARK_WRITE_PARTITIONS,
            silver_partitions=BENCHMARK_WRITE_PARTITIONS,
            gold_refresh_mode=gold_refresh_mode,
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
        "enabled": True,
        "skipped": False,
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
    parser.add_argument(
        "--batch-enabled",
        default=os.getenv("PROCESSING_BATCH_ENABLED", "true"),
        help="Run the batch side of this processing benchmark. Disable for historical-batch-plus-streaming profiles.",
    )
    parser.add_argument(
        "--streaming-checkpoint-location",
        default=os.getenv("STREAM_BENCHMARK_CHECKPOINT_LOCATION", ""),
        help="Run-scoped checkpoint location recorded for bounded replay operations.",
    )
    parser.add_argument("--subset-enabled", default="false", help="Whether this benchmark run uses a subset scope.")
    parser.add_argument("--subset-mode", choices=["month_range", "fraction"], default="month_range", help="Benchmark-only subset mode.")
    parser.add_argument("--sample-fraction", type=float, default=0.1, help="Fraction used when --subset-mode=fraction.")
    parser.add_argument("--sample-seed", type=int, default=42, help="Random seed used when --subset-mode=fraction.")
    parser.add_argument("--staging-enabled", default="false", help="Stage the selected benchmark input once before timed phases.")
    parser.add_argument("--staging-path", default="", help="S3A/local path for benchmark-only staged input.")
    parser.add_argument("--gold-refresh-mode", choices=sorted(bb.GOLD_REFRESH_MODES), default="full")
    parser.add_argument("--profile", default=os.getenv("BENCHMARK_PROFILE", "ad-hoc"), help="Benchmark profile name.")
    parser.add_argument("--run-id", default=os.getenv("BENCHMARK_RUN_ID", ""), help="Benchmark run id.")
    parser.add_argument("--output-json", help="Optional JSON output path.")
    parser.add_argument("--output-csv", help="Optional CSV output path.")
    bis.add_input_sample_args(parser)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = bb.build_spark_session()
    bb.log_spark_runtime_config(spark, output_path=args.output_json or "")
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

    batch_enabled = _parse_bool(args.batch_enabled)
    if batch_enabled:
        batch_results = _run_batch_path(spark, bronze_batch, gold_refresh_mode=args.gold_refresh_mode)
    else:
        batch_results = _skipped_batch_path("disabled_by_profile; historical batch is measured in the system phase")
    streaming_results = _run_streaming_path(
        spark,
        bronze_batch,
        microbatch_size=args.microbatch_size,
        gold_refresh_mode=args.gold_refresh_mode,
    )
    batch_results["throughput_rows_per_second"] = _rows_per_second(bronze_row_count, batch_results["wall_seconds"])
    streaming_results["throughput_rows_per_second"] = _rows_per_second(bronze_row_count, streaming_results["wall_seconds"])

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
            "bronze_row_count": bronze_row_count,
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
        "limitations": _benchmark_limitations(subset_enabled),
        "gold_refresh_mode": args.gold_refresh_mode,
        "batch_enabled": batch_enabled,
        "streaming_checkpoint_location": args.streaming_checkpoint_location,
        "batch": batch_results,
        "streaming": streaming_results,
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
