"""Streaming-friendly Silver-to-Gold entrypoint.

The current implementation computes deterministic micro-batch Gold tables from
the locally materialized Silver layer, while preserving the CLI shape needed
for a later Spark structured streaming upgrade.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.config import load_config
from common.constants import GOLD_PATH, SILVER_PATH, SPARK_APP_NAME_PREFIX, SPARK_MASTER
from common.logger import get_logger
from pipelines.gold.aggregate import run_silver_to_gold

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream aggregate Silver -> Gold.")
    parser.add_argument("--silver-path", default=SILVER_PATH, help="Silver source path.")
    parser.add_argument("--gold-path", default=GOLD_PATH, help="Gold destination path.")
    return parser.parse_args()


def build_spark_session():
    """Create a Spark session when PySpark is available."""

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(f"{SPARK_APP_NAME_PREFIX}-silver-to-gold-stream")
        .getOrCreate()
    )


def main() -> None:
    """Aggregate Silver partitions into Gold outputs using the shared core."""

    args = parse_args()
    logger.info("Starting silver_to_gold_stream silver=%s gold=%s", args.silver_path, args.gold_path)
    config = load_config(silver_uri=args.silver_path, gold_uri=args.gold_path)
    result = run_silver_to_gold(
        config,
        silver_root=Path(args.silver_path) if "://" not in args.silver_path else None,
        gold_root=Path(args.gold_path) if "://" not in args.gold_path else None,
    )
    logger.info(
        "Streaming-friendly Gold aggregation finished run_id=%s rows_read=%s tables_written=%s manifest=%s",
        result.run_id,
        result.rows_read,
        len(result.output_paths),
        result.manifest_path,
    )


if __name__ == "__main__":
    main()
