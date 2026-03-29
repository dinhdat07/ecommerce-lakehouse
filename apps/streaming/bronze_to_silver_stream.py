"""Streaming-friendly Bronze-to-Silver entrypoint.

This command currently reuses the deterministic Silver transformation logic on
newly materialized Bronze files. The shared interface preserves a clean upgrade
path to real Spark structured streaming with checkpoints and watermarks.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.config import load_config
from common.constants import BRONZE_PATH, SILVER_PATH, SPARK_APP_NAME_PREFIX, SPARK_MASTER
from common.logger import get_logger
from pipelines.silver.transform import run_bronze_to_silver

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream transform Bronze -> Silver.")
    parser.add_argument("--bronze-path", default=BRONZE_PATH, help="Bronze source path.")
    parser.add_argument("--silver-path", default=SILVER_PATH, help="Silver destination path.")
    return parser.parse_args()


def build_spark_session():
    """Create a Spark session when PySpark is available."""

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(f"{SPARK_APP_NAME_PREFIX}-bronze-to-silver-stream")
        .getOrCreate()
    )


def main() -> None:
    """Transform Bronze records into Silver partitions using the shared core."""

    args = parse_args()
    logger.info("Starting bronze_to_silver_stream bronze=%s silver=%s", args.bronze_path, args.silver_path)
    config = load_config(bronze_uri=args.bronze_path, silver_uri=args.silver_path)
    result = run_bronze_to_silver(
        config,
        bronze_root=Path(args.bronze_path) if "://" not in args.bronze_path else None,
        silver_root=Path(args.silver_path) if "://" not in args.silver_path else None,
    )
    logger.info(
        "Streaming-friendly Silver transform finished run_id=%s valid_rows=%s invalid_rows=%s manifest=%s",
        result.run_id,
        result.valid_rows,
        result.invalid_rows,
        result.manifest_path,
    )


if __name__ == "__main__":
    main()
