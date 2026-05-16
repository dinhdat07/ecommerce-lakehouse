"""Materialize persistent demo Iceberg tables from historical raw data plus a cached sample.

This job is intended for the server stack. It keeps the same Bronze/Silver/Gold
transforms as the benchmark pipeline, but writes to the normal
``lakehouse.demo.*`` tables that Trino and Superset expect.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import batch_backfill_to_iceberg as bb  # noqa: E402


class RemoteInputFile:
    """Path-like wrapper that preserves an object-storage URI for Spark reads."""

    def __init__(self, uri: str) -> None:
        self.uri = uri

    @property
    def name(self) -> str:
        return self.uri.rstrip("/").rsplit("/", 1)[-1]

    def __str__(self) -> str:
        return self.uri


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize demo Iceberg tables from Oct-Nov raw data plus a Dec sample parquet cache."
    )
    parser.add_argument("--batch-input-dir", required=True, help="Raw historical input directory, local or s3a://...")
    parser.add_argument("--batch-start-month", default="2019-10")
    parser.add_argument("--batch-end-month", default="2019-11")
    parser.add_argument(
        "--append-parquet",
        action="append",
        default=[],
        help="Bronze-shaped parquet path to append after the initial raw batch. Pass multiple times.",
    )
    parser.add_argument(
        "--batch-gold-refresh-mode",
        choices=sorted(bb.GOLD_REFRESH_MODES),
        default=bb.GOLD_REFRESH_MODE_FULL,
        help="Gold refresh mode for the initial Oct-Nov load.",
    )
    parser.add_argument(
        "--stream-gold-refresh-mode",
        choices=sorted(bb.GOLD_REFRESH_MODES),
        default=bb.GOLD_REFRESH_MODE_AFFECTED_DATES,
        help="Gold refresh mode for each appended parquet dataset.",
    )
    parser.add_argument("--reset-tables", action="store_true", help="Drop the current demo tables before loading.")
    parser.add_argument("--bronze-partitions", type=int, default=8)
    parser.add_argument("--silver-partitions", type=int, default=8)
    parser.add_argument("--output-json", help="Optional JSON summary output path.")
    return parser.parse_args()


def _discover_remote_input_files(spark, input_dir: str, start_month: str, end_month: str) -> tuple[list[RemoteInputFile], list[str]]:
    expected_months = bb.iter_months(start_month, end_month)
    selected_files: list[RemoteInputFile] = []
    available_months: set[str] = set()

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    root = jvm.org.apache.hadoop.fs.Path(input_dir.rstrip("/"))
    fs = root.getFileSystem(hadoop_conf)
    if not fs.exists(root):
        return [], expected_months

    iterator = fs.listFiles(root, True)
    while iterator.hasNext():
        status = iterator.next()
        uri = status.getPath().toString()
        name = uri.rsplit("/", 1)[-1]
        if not (name.endswith(".csv") or name.endswith(".csv.gz")):
            continue
        source_month = bb.extract_source_month(Path(name))
        if source_month is None:
            continue
        if start_month <= source_month <= end_month:
            selected_files.append(RemoteInputFile(uri))
            available_months.add(source_month)

    selected_files.sort(key=lambda item: item.uri)
    missing_months = [month for month in expected_months if month not in available_months]
    return selected_files, missing_months


def discover_input_files(spark, input_dir: str, start_month: str, end_month: str) -> tuple[list[Path | RemoteInputFile], list[str]]:
    if "://" in input_dir:
        return _discover_remote_input_files(spark, input_dir, start_month, end_month)
    return bb.discover_input_files(Path(input_dir), start_month, end_month)


def _write_summary(path: str, payload: dict[str, Any]) -> None:
    summary_path = Path(path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    spark = bb.build_spark_session()
    bb.log_spark_runtime_config(spark, output_path=args.output_json or "")

    if args.reset_tables:
        bb.drop_tables_if_requested(spark, True)
    bb.create_tables_if_needed(spark)

    batch_files, missing_months = discover_input_files(
        spark, args.batch_input_dir, args.batch_start_month, args.batch_end_month
    )
    if not batch_files:
        raise SystemExit(
            f"No historical files found in {args.batch_input_dir} for {args.batch_start_month}..{args.batch_end_month}"
        )

    batch_run_id = f"demo-batch-{args.batch_start_month.replace('-', '')}-{args.batch_end_month.replace('-', '')}"
    batch_bronze = bb.read_bronze_batch(spark, batch_files, batch_run_id, source_month_override=None)
    # Historical gzip inputs can arrive as a single partition. Repartition the
    # Bronze-shaped frame here so demo Silver/Gold refreshes use the cluster.
    batch_bronze = batch_bronze.repartition(args.bronze_partitions)
    batch_result = bb.process_bronze_batch(
        spark,
        batch_bronze,
        append_bronze=True,
        bronze_partitions=args.bronze_partitions,
        silver_partitions=args.silver_partitions,
        gold_refresh_mode=args.batch_gold_refresh_mode,
        run_id=batch_run_id,
        input_descriptions=[str(path) for path in batch_files],
    )

    summary = {
        "batch": {
            "input_files": [str(path) for path in batch_files],
            "missing_months": missing_months,
            "bronze_rows_written": int(batch_result["bronze_metrics"]["rows_written"]),
            "silver_rows_written": int(batch_result["silver_metrics"]["rows_written"]),
            "gold_rows_written": int(batch_result["gold_metrics"]["rows_written"]),
            "affected_dates": batch_result["affected_dates"],
            "gold_refresh_mode": args.batch_gold_refresh_mode,
        },
        "append_parquet": [],
        "tables": {
            "bronze": bb.BRONZE_TABLE,
            "silver": bb.SILVER_TABLE,
            "gold": bb.ALL_GOLD_TABLES,
        },
    }
    for index, parquet_path in enumerate(args.append_parquet):
        bronze_batch = spark.read.parquet(parquet_path).repartition(args.bronze_partitions)
        run_id = f"demo-append-{index:02d}"
        append_result = bb.process_bronze_batch(
            spark,
            bronze_batch,
            append_bronze=True,
            bronze_partitions=args.bronze_partitions,
            silver_partitions=args.silver_partitions,
            gold_refresh_mode=args.stream_gold_refresh_mode,
            run_id=run_id,
            input_descriptions=[parquet_path],
        )
        summary["append_parquet"].append(
            {
                "input_parquet": parquet_path,
                "run_id": run_id,
                "bronze_rows_written": int(append_result["bronze_metrics"]["rows_written"]),
                "silver_rows_written": int(append_result["silver_metrics"]["rows_written"]),
                "gold_rows_written": int(append_result["gold_metrics"]["rows_written"]),
                "affected_dates": append_result["affected_dates"],
                "gold_refresh_mode": args.stream_gold_refresh_mode,
            }
        )
    print(json.dumps(summary, indent=2))
    if args.output_json:
        _write_summary(args.output_json, summary)
    spark.stop()


if __name__ == "__main__":
    main()
