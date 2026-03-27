"""Structured streaming job: Silver canonical events -> Gold aggregates."""

from __future__ import annotations

import argparse

from common.constants import GOLD_PATH, SILVER_PATH, SPARK_APP_NAME_PREFIX, SPARK_MASTER
from common.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream aggregate Silver -> Gold.")
    parser.add_argument("--silver-path", default=SILVER_PATH, help="Silver source path.")
    parser.add_argument("--gold-path", default=GOLD_PATH, help="Gold destination path.")
    return parser.parse_args()


def build_spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(f"{SPARK_APP_NAME_PREFIX}-silver-to-gold-stream")
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    logger.info("Starting silver_to_gold_stream silver=%s gold=%s", args.silver_path, args.gold_path)
    logger.info("TODO: Define windowed aggregations for BI-ready metrics.")
    logger.info("TODO: Upsert/append Gold tables with watermark strategy.")


if __name__ == "__main__":
    main()
