"""Replay sample CSV events to Kafka topic for streaming simulation."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from common.constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_EVENTS, SAMPLE_DATA_DIR
from common.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay CSV to Kafka topic.")
    parser.add_argument("--input-csv", default=f"{SAMPLE_DATA_DIR}/events_sample.csv", help="CSV file path.")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers.")
    parser.add_argument("--topic", default=KAFKA_TOPIC_EVENTS, help="Kafka topic.")
    return parser.parse_args()


def iter_csv_rows(path: Path):
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_csv)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    logger.info("Starting replay_csv_to_kafka input=%s topic=%s", input_path, args.topic)
    logger.info("TODO: Plug in Kafka producer library and send records.")

    # Placeholder behavior: preview first 3 records.
    for i, row in enumerate(iter_csv_rows(input_path), start=1):
        if i <= 3:
            logger.info("Preview record %d: %s", i, json.dumps(row))
        else:
            break


if __name__ == "__main__":
    main()
