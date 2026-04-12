"""Project-wide constants and environment-backed settings."""

from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "true" if default else "false").strip().lower()
    return raw in ("1", "true", "yes", "y", "on")


PROJECT_NAME = _env("PROJECT_NAME", "ecommerce-lakehouse")
ENV = _env("ENV", "local")
DEPLOYMENT_PROFILE = _env("DEPLOYMENT_PROFILE", "local")

RAW_DATA_DIR = _env("RAW_DATA_DIR", "./data/raw")
SAMPLE_DATA_DIR = _env("SAMPLE_DATA_DIR", "./data/sample")
LOCAL_DATA_ROOT = _env("LOCAL_DATA_ROOT", "./data/lakehouse")
MANIFEST_ROOT = _env("MANIFEST_ROOT", "./data/manifests")
CHECKPOINT_ROOT = _env("CHECKPOINT_ROOT", "./checkpoints")
BENCHMARK_OUTPUT_ROOT = _env("BENCHMARK_OUTPUT_ROOT", "./data/benchmarks")

KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_EVENTS = _env("KAFKA_TOPIC_EVENTS", "ecom.events")
KAFKA_TOPIC_DLQ = _env("KAFKA_TOPIC_DLQ", "ecom.events.dlq")
KAFKA_TOPIC_PARTITIONS = _env_int("KAFKA_TOPIC_PARTITIONS", 3)
KAFKA_TOPIC_REPLICATION_FACTOR = _env_int("KAFKA_TOPIC_REPLICATION_FACTOR", 1)
KAFKA_TOPIC_MIN_INSYNC_REPLICAS = _env_int("KAFKA_TOPIC_MIN_INSYNC_REPLICAS", 1)
KAFKA_RETENTION_HOURS = _env_int("KAFKA_RETENTION_HOURS", 12)
KAFKA_RETENTION_BYTES = _env_int("KAFKA_RETENTION_BYTES", 536870912)
KAFKA_MESSAGE_MAX_BYTES = _env_int("KAFKA_MESSAGE_MAX_BYTES", 2097152)
KAFKA_CONTROLLER_QUORUM_VOTERS = _env("KAFKA_CONTROLLER_QUORUM_VOTERS", "")

SPARK_MASTER = _env("SPARK_MASTER", "local[*]")
SPARK_APP_NAME_PREFIX = _env("SPARK_APP_NAME_PREFIX", "ecommerce-lakehouse")
SPARK_DEPLOY_MODE = _env("SPARK_DEPLOY_MODE", "client")
SPARK_DRIVER_MEMORY = _env("SPARK_DRIVER_MEMORY", "1400m")
SPARK_EXECUTOR_MEMORY = _env("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_SQL_SHUFFLE_PARTITIONS = _env_int("SPARK_SQL_SHUFFLE_PARTITIONS", 8)
SPARK_LOCAL_DIR = _env("SPARK_LOCAL_DIR", "/tmp")
SPARK_CHECKPOINT_ROOT = _env("SPARK_CHECKPOINT_ROOT", CHECKPOINT_ROOT)

STREAM_MODE = _env("STREAM_MODE", "demo")
STREAM_QUERY_NAME = _env("STREAM_QUERY_NAME", "ecommerce-streaming")
STREAM_STARTING_OFFSETS = _env("STREAM_STARTING_OFFSETS", "earliest")
STREAM_TRIGGER_SECONDS = _env_int("STREAM_TRIGGER_SECONDS", 5)
STREAM_MAX_OFFSETS_PER_TRIGGER = _env_int("STREAM_MAX_OFFSETS_PER_TRIGGER", 2000)
STREAM_TIMEOUT_SECONDS = _env_int("STREAM_TIMEOUT_SECONDS", 120)
STREAM_STOP_AFTER_SECONDS = _env_int("STREAM_STOP_AFTER_SECONDS", 0)
STREAM_PROGRESS_POLL_SECONDS = _env_int("STREAM_PROGRESS_POLL_SECONDS", 10)
STREAM_CHECKPOINT_LOCATION = _env(
    "STREAM_CHECKPOINT_LOCATION",
    f"{SPARK_CHECKPOINT_ROOT.rstrip('/')}/phase2/kafka_to_iceberg",
)
STREAM_PROGRESS_LOG_PATH = _env("STREAM_PROGRESS_LOG_PATH", "")
STREAM_FAIL_ON_DATA_LOSS = _env_bool("STREAM_FAIL_ON_DATA_LOSS", False)

REPLAY_BATCH_SIZE = _env_int("REPLAY_BATCH_SIZE", 500)
REPLAY_SLEEP_SECONDS = float(_env("REPLAY_SLEEP_SECONDS", "1.0"))
REPLAY_MAX_ROWS = _env("REPLAY_MAX_ROWS", "")
REPLAY_RUNTIME_SECONDS = _env_int("REPLAY_RUNTIME_SECONDS", 0)
REPLAY_REPORT_EVERY = _env_int("REPLAY_REPORT_EVERY", 1000)
REPLAY_REPEAT_INPUT = _env_bool("REPLAY_REPEAT_INPUT", False)
REPLAY_KAFKA_ACKS = _env("REPLAY_KAFKA_ACKS", "all")
REPLAY_COMPRESSION_TYPE = _env("REPLAY_COMPRESSION_TYPE", "none")

S3_ENDPOINT = _env("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = _env("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = _env("S3_SECRET_KEY", "minioadmin")
S3_REGION = _env("S3_REGION", "us-east-1")
S3_PATH_STYLE_ACCESS = _env_bool("S3_PATH_STYLE_ACCESS", True)
S3_BUCKET_BRONZE = _env("S3_BUCKET_BRONZE", "bronze")
S3_BUCKET_SILVER = _env("S3_BUCKET_SILVER", "silver")
S3_BUCKET_GOLD = _env("S3_BUCKET_GOLD", "gold")
S3_BUCKET_WAREHOUSE = _env("S3_BUCKET_WAREHOUSE", "warehouse")

BRONZE_PATH = _env("BRONZE_PATH", "s3a://bronze/events")
SILVER_PATH = _env("SILVER_PATH", "s3a://silver/events")
GOLD_PATH = _env("GOLD_PATH", "s3a://gold/analytics")

ICEBERG_CATALOG = _env("ICEBERG_CATALOG", "local_catalog")
ICEBERG_WAREHOUSE = _env("ICEBERG_WAREHOUSE", "s3a://warehouse")
ICEBERG_CATALOG_TYPE = _env("ICEBERG_CATALOG_TYPE", "jdbc")
ICEBERG_CATALOG_URI = _env("ICEBERG_CATALOG_URI", "jdbc:postgresql://postgres:5432/iceberg")
ICEBERG_CATALOG_USER = _env("ICEBERG_CATALOG_USER", "iceberg")
ICEBERG_CATALOG_PASSWORD = _env("ICEBERG_CATALOG_PASSWORD", "iceberg")
ICEBERG_CATALOG_DRIVER = _env("ICEBERG_CATALOG_DRIVER", "org.postgresql.Driver")

# Data quality: abort publication when checks fail (Silver/Gold JSONL and Iceberg paths).
DQ_FAIL_ON_ERROR = _env_bool("DQ_FAIL_ON_ERROR", True)
