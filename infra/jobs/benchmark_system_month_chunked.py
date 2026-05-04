"""Disk-aware monthly system benchmark split into Bronze and date chunks.

This job is intentionally narrow: it reuses the production Bronze/Silver/Gold
transforms but lets the shell runner submit Bronze, each Silver/Gold date chunk,
and final reporting as separate Spark applications. That keeps Spark local spill
bounded and allows temp cleanup between chunks on the small 3-node cluster.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from pyspark.sql import functions as F

JOBS_DIR = Path(__file__).resolve().parent
ROOT = JOBS_DIR.parents[1]
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

import batch_backfill_to_iceberg as bb  # noqa: E402
import benchmark_system_lakehouse_vs_parquet as sysbench  # noqa: E402
from common.constants import ICEBERG_WAREHOUSE  # noqa: E402


BENCHMARK_WRITE_PARTITIONS = int(os.getenv("BENCHMARK_WRITE_PARTITIONS", "16"))


def _parse_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "false").strip().lower() in {"1", "true", "yes", "y"}


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _git_commit() -> str:
    if os.getenv("GIT_COMMIT"):
        return os.environ["GIT_COMMIT"]
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    except Exception:
        return "unknown"


def _month_date_bounds(month: str) -> tuple[date, date]:
    start = datetime.strptime(month, "%Y-%m").date()
    if start.month == 12:
        next_month = date(start.year + 1, 1, 1)
    else:
        next_month = date(start.year, start.month + 1, 1)
    return start, next_month - timedelta(days=1)


def _date_chunks(month: str, chunk_days: int) -> list[tuple[str, str]]:
    start, end = _month_date_bounds(month)
    chunks: list[tuple[str, str]] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        chunks.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return chunks


def _write_json(path: str | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: str | None, results: dict[str, Any]) -> None:
    if not path:
        return
    sysbench._write_results(results, output_json=None, output_csv=path)


def _discover_input_files(input_dir: str, month: str) -> tuple[list[Path], list[str]]:
    files, missing = bb.discover_input_files(Path(input_dir), month, month)
    if missing or not files:
        raise SystemExit(f"No historical file found in {input_dir} for {month}; missing={missing}")
    return files, missing


def _apply_benchmark_tables() -> None:
    sysbench._apply_bench_table_names("lakehouse.demo.bench_")


def _bench_storage_bytes(spark, warehouse_uri: str) -> int:
    return sum(
        sysbench._hadoop_path_size_bytes(spark, sysbench._warehouse_table_path(table_name, warehouse_uri))
        for table_name in sysbench._bench_table_names()
    )


def _run_bronze(args: argparse.Namespace) -> dict[str, Any]:
    spark = bb.build_spark_session()
    bb.log_spark_runtime_config(spark, output_path=args.output_json or "")
    _apply_benchmark_tables()
    disk_before = sysbench._disk_usage_snapshot()
    try:
        if _parse_bool(args.drop_existing_tables):
            sysbench._drop_bench_tables(spark)
        bb.create_tables_if_needed(spark)

        input_files, missing_months = _discover_input_files(args.input_dir, args.month)
        batch_run_id = args.run_id or f"chunked-{args.month}"
        bronze_batch = bb.read_bronze_batch(
            spark,
            input_files,
            batch_run_id=batch_run_id,
            source_month_override=args.month,
        )

        started = time.perf_counter()
        bb.append_to_iceberg(bb.BRONZE_TABLE, bronze_batch, partitions=BENCHMARK_WRITE_PARTITIONS)
        bronze_rows = int(spark.table(bb.BRONZE_TABLE).where(F.col("source_month") == args.month).count())
        duration = round(time.perf_counter() - started, 3)

        start_date, end_date = _month_date_bounds(args.month)
        parsed_date = F.to_date(bb.parse_event_timestamp("event_time"))
        valid_month_rows = int(
            spark.table(bb.BRONZE_TABLE)
            .where(F.col("source_month") == args.month)
            .where((parsed_date >= F.lit(start_date.isoformat())) & (parsed_date <= F.lit(end_date.isoformat())))
            .count()
        )
        invalid_or_out_of_range_rows = max(bronze_rows - valid_month_rows, 0)

        result = {
            "phase": "bronze",
            "run_id": batch_run_id,
            "month": args.month,
            "profile": args.profile,
            "git_commit": _git_commit(),
            "input_files": [path.name for path in input_files],
            "missing_months": missing_months,
            "bronze_rows_written": bronze_rows,
            "valid_month_rows": valid_month_rows,
            "invalid_or_out_of_range_rows": invalid_or_out_of_range_rows,
            "duration_seconds": duration,
            "write_partitions": BENCHMARK_WRITE_PARTITIONS,
            "disk_usage": {"before": disk_before, "after": sysbench._disk_usage_snapshot()},
            "completed_at_utc": _utc_now_iso(),
        }
        _write_json(args.output_json, result)
        print(json.dumps(result, indent=2, sort_keys=True))
        return result
    finally:
        spark.stop()


def _run_chunk(args: argparse.Namespace) -> dict[str, Any]:
    if not args.chunk_start_date or not args.chunk_end_date:
        raise SystemExit("--chunk-start-date and --chunk-end-date are required for phase=silver-gold-chunk")

    spark = bb.build_spark_session()
    bb.log_spark_runtime_config(spark, output_path=args.output_json or "")
    _apply_benchmark_tables()
    disk_before = sysbench._disk_usage_snapshot()
    try:
        bb.create_tables_if_needed(spark)
        parsed_date = F.to_date(bb.parse_event_timestamp("event_time"))
        bronze_chunk = (
            spark.table(bb.BRONZE_TABLE)
            .where(F.col("source_month") == args.month)
            .where((parsed_date >= F.lit(args.chunk_start_date)) & (parsed_date <= F.lit(args.chunk_end_date)))
        )

        started = time.perf_counter()
        result = bb.process_bronze_batch(
            spark,
            bronze_chunk,
            append_bronze=False,
            bronze_partitions=BENCHMARK_WRITE_PARTITIONS,
            silver_partitions=BENCHMARK_WRITE_PARTITIONS,
            gold_refresh_mode=args.gold_refresh_mode,
            run_id=f"{args.run_id}-{args.chunk_start_date}-{args.chunk_end_date}" if args.run_id else None,
            input_descriptions=[f"{bb.BRONZE_TABLE}:{args.month}:{args.chunk_start_date}..{args.chunk_end_date}"],
        )
        duration = round(time.perf_counter() - started, 3)

        payload = {
            "phase": "silver-gold-chunk",
            "run_id": args.run_id,
            "month": args.month,
            "chunk_start_date": args.chunk_start_date,
            "chunk_end_date": args.chunk_end_date,
            "profile": args.profile,
            "gold_refresh_mode": args.gold_refresh_mode,
            "duration_seconds": duration,
            "write_partitions": BENCHMARK_WRITE_PARTITIONS,
            "result": result,
            "disk_usage": {"before": disk_before, "after": sysbench._disk_usage_snapshot()},
            "completed_at_utc": _utc_now_iso(),
        }
        _write_json(args.output_json, payload)
        print(json.dumps(payload, indent=2, sort_keys=True))
        return payload
    finally:
        spark.stop()


def _load_phase_jsons(paths: list[str]) -> list[dict[str, Any]]:
    loaded: list[dict[str, Any]] = []
    for item in paths:
        path = Path(item)
        if path.exists():
            loaded.append(json.loads(path.read_text(encoding="utf-8")))
    return loaded


def _run_finalize(args: argparse.Namespace) -> dict[str, Any]:
    spark = bb.build_spark_session()
    bb.log_spark_runtime_config(spark, output_path=args.output_json or "")
    _apply_benchmark_tables()
    disk_before = sysbench._disk_usage_snapshot()
    try:
        bronze_metrics = json.loads(Path(args.bronze_json).read_text(encoding="utf-8")) if args.bronze_json else {}
        chunk_metrics = _load_phase_jsons(args.chunk_json)

        query_latencies = sysbench._run_queries(
            spark,
            {
                "daily_revenue_sum": f"SELECT round(sum(purchase_revenue), 2) AS revenue FROM {bb.GOLD_DAILY_REVENUE}",
                "top_products_avg": f"SELECT round(avg(purchase_revenue), 2) AS avg_revenue FROM {bb.GOLD_TOP_PRODUCTS}",
                "funnel_totals": f"SELECT sum(views) AS views, sum(purchases) AS purchases FROM {bb.GOLD_CONVERSION_FUNNEL}",
                "repeat_purchase_avg": f"SELECT round(avg(repeat_purchase_rate), 4) AS rate FROM {bb.GOLD_REPEAT_PURCHASE}",
                "rfm_users": f"SELECT count(*) AS users FROM {bb.GOLD_RFM_SEGMENTATION}",
            },
        )

        gold_row_counts = {table_name: int(spark.table(table_name).count()) for table_name in bb.ALL_GOLD_TABLES}
        bronze_rows = int(spark.table(bb.BRONZE_TABLE).where(F.col("source_month") == args.month).count())
        silver_rows = int(spark.table(bb.SILVER_TABLE).where(F.col("source_month") == args.month).count())
        ingestion_seconds = float(bronze_metrics.get("duration_seconds") or 0.0)
        transformation_seconds = round(sum(float(c.get("duration_seconds") or 0.0) for c in chunk_metrics), 3)
        total_runtime_seconds = round(ingestion_seconds + transformation_seconds + sum(query_latencies.values()), 3)

        all_silver_dq = all(
            bool(c.get("result", {}).get("silver_dq_summary", {}).get("dq_passed", True)) for c in chunk_metrics
        )
        all_gold_dq = all(
            all(bool(summary.get("dq_passed", True)) for summary in c.get("result", {}).get("gold_dq_summaries", {}).values())
            for c in chunk_metrics
        )

        results = {
            "run_id": args.run_id or Path(args.output_json or "").parent.name or f"chunked-{args.month}",
            "git_commit": _git_commit(),
            "profile": args.profile,
            "benchmark_mode": "full",
            "dataset": {
                "label": f"full:{args.month}..{args.month}:date_chunked",
                "start_month": args.month,
                "end_month": args.month,
                "input_files": bronze_metrics.get("input_files", []),
                "missing_months": bronze_metrics.get("missing_months", []),
                "input_row_count": bronze_rows,
                "benchmark_row_count": bronze_rows,
                "raw_read_seconds": 0.0,
                "valid_month_rows": bronze_metrics.get("valid_month_rows"),
                "invalid_or_out_of_range_rows": bronze_metrics.get("invalid_or_out_of_range_rows"),
            },
            "input_sample": {"enabled": False},
            "subset": {
                "subset_enabled": False,
                "subset_mode": "month_range",
                "start_month": args.month,
                "end_month": args.month,
                "sample_fraction": 1.0,
                "sample_seed": 42,
                "input_row_count": bronze_rows,
                "benchmark_row_count": bronze_rows,
            },
            "cluster_specs": sysbench._cluster_specs(spark),
            "spark_config": sysbench._spark_config_snapshot(spark),
            "disk_usage": {"before": disk_before, "after": sysbench._disk_usage_snapshot()},
            "limitations": sysbench._benchmark_limitations(False, False)
            + [
                "This monthly run split Bronze ingestion and Silver/Gold date chunks into separate Spark applications to bound local spill.",
                "Advanced history-wide Gold tables are skipped in affected_dates mode, matching the disk-safe profile.",
            ],
            "bronze_row_count": bronze_rows,
            "parquet_baseline_enabled": False,
            "gold_refresh_mode": args.gold_refresh_mode,
            "chunking": {
                "month": args.month,
                "chunk_count": len(chunk_metrics),
                "chunks": [
                    {
                        "start_date": c.get("chunk_start_date"),
                        "end_date": c.get("chunk_end_date"),
                        "rows_seen": c.get("result", {}).get("bronze_metrics", {}).get("rows_seen"),
                        "silver_rows_written": c.get("result", {}).get("silver_metrics", {}).get("rows_written"),
                        "duration_seconds": c.get("duration_seconds"),
                    }
                    for c in chunk_metrics
                ],
            },
            "parquet": sysbench._skipped_parquet_baseline("disabled_by_profile_or_env"),
            "iceberg": {
                "ingestion_seconds": ingestion_seconds,
                "transformation_seconds": transformation_seconds,
                "total_runtime_seconds": total_runtime_seconds,
                "throughput_rows_per_second": sysbench._rows_per_second(bronze_rows, total_runtime_seconds),
                "query_latency_seconds": query_latencies,
                "query_latency_seconds_avg": round(sum(query_latencies.values()) / len(query_latencies), 3),
                "storage_bytes": _bench_storage_bytes(spark, args.warehouse_uri),
                "bronze_rows": bronze_rows,
                "silver_rows": silver_rows,
                "gold_row_counts": gold_row_counts,
                "gold_refresh_mode": args.gold_refresh_mode,
                "gold_tables_skipped": len(bb.ADVANCED_GOLD_TABLES) if args.gold_refresh_mode != bb.GOLD_REFRESH_MODE_FULL else 0,
                "gold_skip_reason": "skipped_history_wide_gold_tables_in_affected_dates_mode"
                if args.gold_refresh_mode != bb.GOLD_REFRESH_MODE_FULL
                else "",
                "silver_dq_passed": all_silver_dq,
                "gold_dq_passed": all_gold_dq,
            },
            "completed_at_utc": _utc_now_iso(),
        }
        _write_json(args.output_json, results)
        _write_csv(args.output_csv, results)
        print(json.dumps(results, indent=2, sort_keys=True))
        return results
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run disk-aware month-chunked benchmark phases.")
    parser.add_argument("--phase", choices=["bronze", "silver-gold-chunk", "finalize"], required=True)
    parser.add_argument("--input-dir", default="/srv/ecommerce/raw")
    parser.add_argument("--month", required=True, help="Month to process, e.g. 2019-11.")
    parser.add_argument("--chunk-days", type=int, default=3)
    parser.add_argument("--chunk-start-date")
    parser.add_argument("--chunk-end-date")
    parser.add_argument("--gold-refresh-mode", choices=sorted(bb.GOLD_REFRESH_MODES), default=os.getenv("SYSTEM_GOLD_REFRESH_MODE", "affected_dates"))
    parser.add_argument("--drop-existing-tables", default="false")
    parser.add_argument("--warehouse-uri", default=ICEBERG_WAREHOUSE)
    parser.add_argument("--profile", default=os.getenv("BENCHMARK_PROFILE", "profile_2month_batch_streaming_sample"))
    parser.add_argument("--run-id", default=os.getenv("BENCHMARK_RUN_ID", ""))
    parser.add_argument("--bronze-json", default="")
    parser.add_argument("--chunk-json", action="append", default=[])
    parser.add_argument("--output-json")
    parser.add_argument("--output-csv")
    parser.add_argument("--print-chunks", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.print_chunks:
        print(json.dumps({"month": args.month, "chunk_days": args.chunk_days, "chunks": _date_chunks(args.month, args.chunk_days)}, indent=2))
        return
    if args.phase == "bronze":
        _run_bronze(args)
    elif args.phase == "silver-gold-chunk":
        _run_chunk(args)
    elif args.phase == "finalize":
        _run_finalize(args)


if __name__ == "__main__":
    main()
