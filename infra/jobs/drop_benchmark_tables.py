"""Drop benchmark-scoped Iceberg tables only."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession

JOBS_DIR = Path(__file__).resolve().parent
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

import batch_backfill_to_iceberg as bb  # noqa: E402


SAFE_PREFIXES = (
    "lakehouse.demo.bench_",
    "lakehouse.demo.bench_batch_",
    "lakehouse.demo.bench_stream_",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drop benchmark Iceberg tables and purge their files.")
    parser.add_argument("--namespace", default="lakehouse.demo")
    parser.add_argument("--like", default="bench*")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = bb.build_spark_session()
    bb.log_spark_runtime_config(spark)
    rows = spark.sql(f"SHOW TABLES IN {args.namespace} LIKE '{args.like}'").collect()
    tables = sorted(f"{args.namespace}.{row.tableName}" for row in rows if not row.isTemporary)
    print(f"benchmark_tables_found={len(tables)}")
    for table_name in tables:
        if not table_name.startswith(SAFE_PREFIXES):
            raise SystemExit(f"Refusing to drop non-benchmark table: {table_name}")
        print(f"DROP TABLE IF EXISTS {table_name} PURGE")
        spark.sql(f"DROP TABLE IF EXISTS {table_name} PURGE")
    print("benchmark_table_drop_complete")
    spark.stop()


if __name__ == "__main__":
    main()
