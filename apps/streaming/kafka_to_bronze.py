"""Structured streaming job: Kafka topic -> Bronze raw storage."""

from __future__ import annotations

import argparse

from common.constants import BRONZE_PATH, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_EVENTS, SPARK_APP_NAME_PREFIX, SPARK_MASTER
from common.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Kafka events into Bronze.")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers.")
    parser.add_argument("--topic", default=KAFKA_TOPIC_EVENTS, help="Kafka topic.")
    parser.add_argument("--bronze-path", default=BRONZE_PATH, help="Bronze destination path.")
    return parser.parse_args()


def build_spark_session():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(f"{SPARK_APP_NAME_PREFIX}-kafka-to-bronze")
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    logger.info("Starting kafka_to_bronze topic=%s bronze=%s", args.topic, args.bronze_path)
    logger.info("TODO: Add Spark Kafka source options and checkpointing.")
    logger.info("TODO: Persist raw payload and metadata to Bronze.")


if __name__ == "__main__":
    main()
