"""Dry-run Iceberg/MinIO maintenance planner for benchmark and demo tables."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

JOBS_DIR = Path(__file__).resolve().parent
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

import batch_backfill_to_iceberg as bb  # noqa: E402


def _collect_scalar(spark, query: str, default: int = 0) -> int:
    try:
        row = spark.sql(query).collect()[0]
        value = row[0]
        return int(value or default)
    except Exception:
        return default


def _table_exists(spark, table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception:
        return False


def _file_report(spark, table_name: str, small_file_bytes: int) -> dict[str, Any]:
    files_table = f"{table_name}.files"
    try:
        files = spark.table(files_table)
        summary = files.selectExpr(
            "count(*) as data_file_count",
            "coalesce(sum(file_size_in_bytes), 0) as total_file_bytes",
            "coalesce(avg(file_size_in_bytes), 0) as avg_file_bytes",
            f"sum(case when file_size_in_bytes < {small_file_bytes} then 1 else 0 end) as small_file_count",
        ).collect()[0].asDict()
    except Exception as exc:
        summary = {
            "data_file_count": 0,
            "total_file_bytes": 0,
            "avg_file_bytes": 0,
            "small_file_count": 0,
            "error": str(exc),
        }
    return summary


def _snapshot_report(spark, table_name: str) -> dict[str, Any]:
    snapshots = _collect_scalar(spark, f"SELECT count(*) FROM {table_name}.snapshots")
    manifests = _collect_scalar(spark, f"SELECT count(*) FROM {table_name}.manifests")
    return {"snapshot_count": snapshots, "manifest_count": manifests}


BENCHMARK_TABLE_PREFIXES = (
    "lakehouse.demo.bench_",
    "lakehouse.demo.bench_batch_",
    "lakehouse.demo.bench_stream_",
)


def _is_benchmark_table(table_name: str) -> bool:
    return table_name.startswith(BENCHMARK_TABLE_PREFIXES)


def _maintenance_plan(table_name: str, *, execute: bool = False) -> dict[str, str]:
    orphan_dry_run = "false" if execute else "true"
    return {
        "rewrite_data_files_sql": f"CALL lakehouse.system.rewrite_data_files(table => '{table_name}')",
        "rewrite_manifests_sql": f"CALL lakehouse.system.rewrite_manifests('{table_name}')",
        "expire_snapshots_sql": f"CALL lakehouse.system.expire_snapshots(table => '{table_name}', retain_last => 5)",
        "remove_orphan_files_sql": f"CALL lakehouse.system.remove_orphan_files(table => '{table_name}', dry_run => {orphan_dry_run})",
        "recommended_table_properties": (
            "write.target-file-size-bytes=134217728, "
            "write.metadata.delete-after-commit.enabled=true, "
            "write.metadata.previous-versions-max=5, "
            "commit.manifest.min-count-to-merge=5, "
            "commit.manifest.target-size-bytes=8388608"
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plan Iceberg small-file and metadata maintenance without changing tables.")
    parser.add_argument("--tables", nargs="*", default=[], help="Fully qualified Iceberg tables to inspect.")
    parser.add_argument("--include-benchmark-tables", action="store_true", help="Include known benchmark table names.")
    parser.add_argument("--small-file-bytes", type=int, default=32 * 1024 * 1024)
    parser.add_argument(
        "--execute-benchmark-maintenance",
        action="store_true",
        help="Execute rewrite/expire/remove procedures for benchmark tables only.",
    )
    parser.add_argument("--output-json", default="", help="Optional report path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = bb.build_spark_session()
    bb.log_spark_runtime_config(spark, output_path=args.output_json)
    tables = list(args.tables)
    if args.include_benchmark_tables:
        for prefix in ("lakehouse.demo.bench_", "lakehouse.demo.bench_batch_", "lakehouse.demo.bench_stream_"):
            for suffix in (
                "bronze_events",
                "silver_events",
                "daily_revenue",
                "top_products",
                "conversion_funnel_daily",
                "category_performance_daily",
                "session_funnel",
                "user_conversion_path",
                "cohort_retention",
                "repeat_purchase",
                "product_affinity",
                "time_to_conversion_distribution",
                "rfm_segmentation",
            ):
                tables.append(f"{prefix}{suffix}")
    if not tables:
        tables = [bb.BRONZE_TABLE, bb.SILVER_TABLE] + bb.ALL_GOLD_TABLES

    report: dict[str, Any] = {
        "dry_run": not args.execute_benchmark_maintenance,
        "execute_benchmark_maintenance": args.execute_benchmark_maintenance,
        "small_file_bytes": args.small_file_bytes,
        "tables": {},
    }
    for table_name in sorted(set(tables)):
        if not _table_exists(spark, table_name):
            report["tables"][table_name] = {"exists": False}
            continue
        if args.execute_benchmark_maintenance and not _is_benchmark_table(table_name):
            raise SystemExit(f"Refusing to execute maintenance for non-benchmark table: {table_name}")
        file_report = _file_report(spark, table_name, args.small_file_bytes)
        snapshot_report = _snapshot_report(spark, table_name)
        recommended_actions = _maintenance_plan(table_name, execute=args.execute_benchmark_maintenance)
        executed_actions: dict[str, str] = {}
        if args.execute_benchmark_maintenance:
            for action_name in (
                "rewrite_data_files_sql",
                "rewrite_manifests_sql",
                "expire_snapshots_sql",
                "remove_orphan_files_sql",
            ):
                sql = recommended_actions[action_name]
                try:
                    spark.sql(sql).collect()
                    executed_actions[action_name] = "succeeded"
                except Exception as exc:
                    executed_actions[action_name] = f"failed: {exc}"
        report["tables"][table_name] = {
            "exists": True,
            "benchmark_table": _is_benchmark_table(table_name),
            **file_report,
            **snapshot_report,
            "recommended_actions": recommended_actions,
            "executed_actions": executed_actions,
        }

    if args.output_json:
        path = Path(args.output_json)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
