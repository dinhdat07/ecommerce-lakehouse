"""Replay sample CSV events to Kafka or a local fallback bus.

This entrypoint prefers Kafka when a compatible client is installed. When the
client is unavailable, it writes newline-delimited JSON events to a local
staging file so the rest of the repository can still exercise a streaming-like
flow without external infrastructure.
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
from common.constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_EVENTS, SAMPLE_DATA_DIR
from common.logger import get_logger
from common.runtime import build_run_id, ensure_directories, utc_now_iso

logger = get_logger(__name__)


def default_input_csv() -> str:
    """Return the preferred default sample CSV path for replay.

    Inputs:
        None.

    Outputs:
        Path string for a sample CSV file that already exists in the repository
        when possible, otherwise the generator-friendly default target.

    Internal logic:
        - Prefer the checked-in `events_sample_100k.csv` file for out-of-the-box runs.
        - Fall back to `events_sample.csv` so generated local samples still work.
    """

    preferred = Path(SAMPLE_DATA_DIR) / "events_sample_100k.csv"
    if preferred.exists():
        return str(preferred)
    return f"{SAMPLE_DATA_DIR}/events_sample.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay CSV to Kafka topic.")
    parser.add_argument("--input-csv", default=default_input_csv(), help="CSV file path.")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers.")
    parser.add_argument("--topic", default=KAFKA_TOPIC_EVENTS, help="Kafka topic.")
    return parser.parse_args()


def iter_csv_rows(path: Path):
    """Yield event rows from a CSV file."""

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def _write_local_bus(input_path: Path, topic: str) -> Path:
    """Persist replayed events to the local fallback bus."""

    config = load_config()
    bus_dir = config.checkpoint_root / "local_bus" / topic
    ensure_directories([bus_dir])
    output_path = bus_dir / f"{build_run_id('replay')}.jsonl"
    with output_path.open("w", encoding="utf-8") as handle:
        for row in iter_csv_rows(input_path):
            handle.write(
                json.dumps({"replayed_at": utc_now_iso(), "topic": topic, "payload": row}, sort_keys=True)
            )
            handle.write("\n")
    return output_path


def main() -> None:
    """Replay a CSV file to Kafka or the repository's local fallback bus."""

    args = parse_args()
    input_path = Path(args.input_csv)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    logger.info("Starting replay_csv_to_kafka input=%s topic=%s", input_path, args.topic)
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError:
        output_path = _write_local_bus(input_path, args.topic)
        logger.info("Kafka client unavailable; wrote replay events to local bus at %s", output_path)
        return

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    sent = 0
    for row in iter_csv_rows(input_path):
        producer.send(args.topic, row)
        sent += 1
    producer.flush()
    producer.close()
    logger.info("Replayed %s events to Kafka topic %s", sent, args.topic)


if __name__ == "__main__":
    main()
