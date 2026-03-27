"""Structured streaming job: Bronze raw events -> Silver canonical events."""

from __future__ import annotations

import argparse

from common.constants import BRONZE_PATH, SILVER_PATH, SPARK_APP_NAME_PREFIX, SPARK_MASTER
from common.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream transform Bronze -> Silver.")
    parser.add_argument("--bronze-path", default=BRONZE_PATH, help="Bronze source path.")
    parser.add_argument("--silver-path", default=SILVER_PATH, help="Silver destination path.")
    return parser.parse_args()


def build_spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(f"{SPARK_APP_NAME_PREFIX}-bronze-to-silver-stream")
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    logger.info("Starting bronze_to_silver_stream bronze=%s silver=%s", args.bronze_path, args.silver_path)
    logger.info("TODO: Parse raw payload, cast schema, and enforce required columns.")
    logger.info("TODO: Write streaming output to Silver (Iceberg) with checkpoints.")


if __name__ == "__main__":
    main()
