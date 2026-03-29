"""Streaming-friendly ingestion entrypoint for Kafka or local replay bus.

The repository does not depend on Kafka or PySpark at commit time, so this
entrypoint supports a local JSONL bus fallback while keeping the CLI and
configuration compatible with future Spark/Kafka execution.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.config import load_config
from common.constants import BRONZE_PATH, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_EVENTS, SPARK_APP_NAME_PREFIX, SPARK_MASTER
from common.logger import get_logger
from pipelines.bronze.backfill import run_bronze_backfill

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream Kafka events into Bronze.")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers.")
    parser.add_argument("--topic", default=KAFKA_TOPIC_EVENTS, help="Kafka topic.")
    parser.add_argument("--bronze-path", default=BRONZE_PATH, help="Bronze destination path.")
    return parser.parse_args()


def build_spark_session():
    """Create a Spark session when PySpark is available."""

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(f"{SPARK_APP_NAME_PREFIX}-kafka-to-bronze")
        .getOrCreate()
    )


def main() -> None:
    """Consume the local replay bus or document the future Kafka/Spark path."""

    args = parse_args()
    logger.info("Starting kafka_to_bronze topic=%s bronze=%s", args.topic, args.bronze_path)
    config = load_config(bronze_uri=args.bronze_path)
    bus_dir = config.checkpoint_root / "local_bus" / args.topic
    if not bus_dir.exists():
        logger.info("Local replay bus not found at %s; nothing to ingest.", bus_dir)
        return

    staging_dir = config.checkpoint_root / "streaming_ingest_csv"
    staging_dir.mkdir(parents=True, exist_ok=True)
    csv_path = staging_dir / "local_bus_snapshot.csv"

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer: csv.DictWriter | None = None
        row_count = 0
        for jsonl_file in sorted(bus_dir.glob("*.jsonl")):
            with jsonl_file.open("r", encoding="utf-8") as source_handle:
                for line in source_handle:
                    payload = json.loads(line)["payload"]
                    if writer is None:
                        writer = csv.DictWriter(handle, fieldnames=list(payload.keys()))
                        writer.writeheader()
                    writer.writerow(payload)
                    row_count += 1

    if row_count == 0:
        logger.info("No events available on the local replay bus.")
        return

    result = run_bronze_backfill(config, input_dir=staging_dir, bronze_uri=args.bronze_path, source_type="local_bus")
    logger.info(
        "Local streaming ingest finished run_id=%s rows_written=%s output=%s manifest=%s",
        result.run_id,
        result.rows_written,
        result.output_path,
        result.manifest_path,
    )


if __name__ == "__main__":
    main()
