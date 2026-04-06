"""Replay sample CSV events to Kafka in bounded demo-sized batches.

This entrypoint prefers `kafka-python` when available. When that dependency is
not installed, it falls back to Kafka's console producer inside the repository
Docker stack so the replay path still works on a laptop without extra Python
packages.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_EVENTS, SAMPLE_DATA_DIR
from common.logger import get_logger
from common.runtime import utc_now_iso

logger = get_logger(__name__)


def default_input_csv() -> str:
    """Return the preferred default sample CSV path for replay."""

    preferred = Path(SAMPLE_DATA_DIR) / "events_streaming_demo_1500.csv"
    if preferred.exists():
        return str(preferred)

    fallback = Path(SAMPLE_DATA_DIR) / "events_sample_100k.csv"
    if fallback.exists():
        return str(fallback)
    return f"{SAMPLE_DATA_DIR}/events_sample.csv"


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for bounded replay into Kafka."""

    parser = argparse.ArgumentParser(description="Replay CSV to Kafka topic.")
    parser.add_argument("--input-csv", default=default_input_csv(), help="CSV file path.")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers.")
    parser.add_argument("--topic", default=KAFKA_TOPIC_EVENTS, help="Kafka topic.")
    parser.add_argument("--batch-size", type=int, default=500, help="Rows to send before sleeping.")
    parser.add_argument("--sleep-seconds", type=float, default=1.0, help="Pause between replay batches.")
    parser.add_argument("--max-rows", type=int, help="Optional cap on replayed rows.")
    parser.add_argument(
        "--docker-kafka-container",
        default="ecommerce-lakehouse-laptop-kafka-1",
        help="Docker Kafka container for console-producer fallback.",
    )
    parser.add_argument(
        "--internal-bootstrap-servers",
        default="kafka:29092",
        help="Kafka bootstrap address used inside the Docker container fallback.",
    )
    return parser.parse_args()


def iter_csv_rows(path: Path):
    """Yield event rows from a CSV file."""

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def derive_source_month(row: dict[str, str]) -> str | None:
    """Derive the logical source month in YYYY-MM format from event_time."""

    event_time = row.get("event_time", "")
    if len(event_time) >= 7:
        return event_time[:7]
    return None


def build_message(input_path: Path, row: dict[str, str]) -> dict[str, object]:
    """Build the replay envelope consumed by the streaming Spark job."""

    return {
        "replayed_at": utc_now_iso(),
        "source_file": input_path.name,
        "source_month": derive_source_month(row),
        "payload": row,
    }


def iter_messages(input_path: Path, max_rows: int | None):
    """Yield bounded replay messages from the input CSV file."""

    for index, row in enumerate(iter_csv_rows(input_path)):
        if max_rows is not None and index >= max_rows:
            break
        yield build_message(input_path, row)


def send_with_console_producer(
    input_path: Path,
    *,
    topic: str,
    internal_bootstrap_servers: str,
    docker_kafka_container: str,
    batch_size: int,
    sleep_seconds: float,
    max_rows: int | None,
) -> int:
    """Replay messages through Kafka's console producer inside the Docker container."""

    command = [
        "docker",
        "exec",
        "-i",
        docker_kafka_container,
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server",
        internal_bootstrap_servers,
        "--topic",
        topic,
    ]
    process = subprocess.Popen(command, stdin=subprocess.PIPE, text=True)
    sent = 0
    try:
        assert process.stdin is not None
        for message in iter_messages(input_path, max_rows):
            process.stdin.write(json.dumps(message, sort_keys=True))
            process.stdin.write("\n")
            sent += 1
            if batch_size > 0 and sent % batch_size == 0 and sleep_seconds > 0:
                process.stdin.flush()
                time.sleep(sleep_seconds)
        process.stdin.close()
        return_code = process.wait()
    finally:
        if process.stdin is not None and not process.stdin.closed:
            process.stdin.close()

    if return_code != 0:
        raise RuntimeError(f"Kafka console producer failed with exit code {return_code}")
    return sent


def main() -> None:
    """Replay a CSV file to Kafka in bounded demo-sized batches."""

    args = parse_args()
    input_path = Path(args.input_csv)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    logger.info(
        "Starting replay_csv_to_kafka input=%s topic=%s batch_size=%s max_rows=%s",
        input_path,
        args.topic,
        args.batch_size,
        args.max_rows,
    )
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError:
        sent = send_with_console_producer(
            input_path,
            topic=args.topic,
            internal_bootstrap_servers=args.internal_bootstrap_servers,
            docker_kafka_container=args.docker_kafka_container,
            batch_size=args.batch_size,
            sleep_seconds=args.sleep_seconds,
            max_rows=args.max_rows,
        )
        logger.info("Replayed %s events to Kafka topic %s via console producer fallback", sent, args.topic)
        return

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    sent = 0
    for message in iter_messages(input_path, args.max_rows):
        producer.send(args.topic, message)
        sent += 1
        if args.batch_size > 0 and sent % args.batch_size == 0 and args.sleep_seconds > 0:
            producer.flush()
            time.sleep(args.sleep_seconds)

    producer.flush()
    producer.close()
    logger.info("Replayed %s events to Kafka topic %s", sent, args.topic)


if __name__ == "__main__":
    main()
