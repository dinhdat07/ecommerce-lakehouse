"""Replay CSV events to Kafka in bounded demo mode or server-style replay mode.

This entrypoint prefers ``kafka-python`` when available. When that dependency
is not installed, it falls back to Kafka's console producer inside the local
Docker stack so the replay path still works on a laptop without extra packages.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterator

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.constants import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_EVENTS,
    REPLAY_BATCH_SIZE,
    REPLAY_COMPRESSION_TYPE,
    REPLAY_KAFKA_ACKS,
    REPLAY_REPEAT_INPUT,
    REPLAY_REPORT_EVERY,
    REPLAY_RUNTIME_SECONDS,
    REPLAY_SLEEP_SECONDS,
    SAMPLE_DATA_DIR,
    STREAM_MODE,
)
from common.logger import get_logger  # noqa: E402
from common.runtime import utc_now_iso  # noqa: E402

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
    """Parse CLI arguments for replay into Kafka."""

    parser = argparse.ArgumentParser(description="Replay CSV to Kafka topic.")
    parser.add_argument("--mode", choices=["demo", "server"], default=STREAM_MODE, help="Replay operating mode.")
    parser.add_argument("--input-csv", default=default_input_csv(), help="CSV file path.")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers.")
    parser.add_argument("--topic", default=KAFKA_TOPIC_EVENTS, help="Kafka topic.")
    parser.add_argument("--batch-size", type=int, default=REPLAY_BATCH_SIZE, help="Rows to send before sleeping.")
    parser.add_argument("--sleep-seconds", type=float, default=REPLAY_SLEEP_SECONDS, help="Pause between replay batches.")
    parser.add_argument("--max-rows", type=int, help="Optional cap on replayed rows.")
    parser.add_argument(
        "--runtime-seconds",
        type=int,
        default=REPLAY_RUNTIME_SECONDS,
        help="Optional replay runtime limit. Use 0 to disable.",
    )
    parser.add_argument(
        "--repeat-input",
        action="store_true",
        default=REPLAY_REPEAT_INPUT,
        help="Repeat the input CSV from the beginning until max-rows or runtime-seconds is reached.",
    )
    parser.add_argument(
        "--no-repeat-input",
        action="store_false",
        dest="repeat_input",
        help="Disable repeated replay even if the environment default enables it.",
    )
    parser.add_argument(
        "--report-every",
        type=int,
        default=REPLAY_REPORT_EVERY,
        help="Log replay progress every N events.",
    )
    parser.add_argument("--acks", default=REPLAY_KAFKA_ACKS, help="Kafka producer acks setting.")
    parser.add_argument(
        "--compression-type",
        default=REPLAY_COMPRESSION_TYPE,
        help="Kafka producer compression type, for example none, gzip, snappy, lz4, or zstd.",
    )
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


def iter_csv_rows(path: Path) -> Iterator[dict[str, str]]:
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


def iter_messages(
    input_path: Path,
    *,
    max_rows: int | None,
    repeat_input: bool = False,
    runtime_seconds: int = 0,
) -> Iterator[dict[str, object]]:
    """Yield replay messages with optional repeat and runtime controls."""

    sent = 0
    started = time.monotonic()
    while True:
        for row in iter_csv_rows(input_path):
            if max_rows is not None and sent >= max_rows:
                return
            if runtime_seconds > 0 and (time.monotonic() - started) >= runtime_seconds:
                return
            yield build_message(input_path, row)
            sent += 1
        if not repeat_input:
            return


def send_with_console_producer(
    input_path: Path,
    *,
    topic: str,
    internal_bootstrap_servers: str,
    docker_kafka_container: str,
    batch_size: int,
    sleep_seconds: float,
    max_rows: int | None,
    repeat_input: bool,
    runtime_seconds: int,
    report_every: int,
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
        for message in iter_messages(
            input_path,
            max_rows=max_rows,
            repeat_input=repeat_input,
            runtime_seconds=runtime_seconds,
        ):
            process.stdin.write(json.dumps(message, sort_keys=True))
            process.stdin.write("\n")
            sent += 1
            if report_every > 0 and sent % report_every == 0:
                logger.info("Replay progress via console producer: %s events sent", sent)
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
    """Replay a CSV file to Kafka in bounded or server-style replay mode."""

    args = parse_args()
    input_path = Path(args.input_csv)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    logger.info(
        "Starting replay_csv_to_kafka mode=%s input=%s topic=%s batch_size=%s max_rows=%s runtime_seconds=%s repeat_input=%s",
        args.mode,
        input_path,
        args.topic,
        args.batch_size,
        args.max_rows,
        args.runtime_seconds,
        args.repeat_input,
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
            repeat_input=args.repeat_input,
            runtime_seconds=args.runtime_seconds,
            report_every=args.report_every,
        )
        logger.info("Replayed %s events to Kafka topic %s via console producer fallback", sent, args.topic)
        return

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks=args.acks,
        compression_type=None if args.compression_type == "none" else args.compression_type,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    sent = 0
    for message in iter_messages(
        input_path,
        max_rows=args.max_rows,
        repeat_input=args.repeat_input,
        runtime_seconds=args.runtime_seconds,
    ):
        producer.send(args.topic, message)
        sent += 1
        if args.report_every > 0 and sent % args.report_every == 0:
            logger.info("Replay progress: %s events sent to %s", sent, args.topic)
        if args.batch_size > 0 and sent % args.batch_size == 0 and args.sleep_seconds > 0:
            producer.flush()
            time.sleep(args.sleep_seconds)

    producer.flush()
    producer.close()
    logger.info("Replayed %s events to Kafka topic %s", sent, args.topic)


if __name__ == "__main__":
    main()
