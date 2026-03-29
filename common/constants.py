"""Project-wide constants and environment-backed settings."""

from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


PROJECT_NAME = _env("PROJECT_NAME", "ecommerce-lakehouse")
ENV = _env("ENV", "local")

RAW_DATA_DIR = _env("RAW_DATA_DIR", "./data/raw")
SAMPLE_DATA_DIR = _env("SAMPLE_DATA_DIR", "./data/sample")
LOCAL_DATA_ROOT = _env("LOCAL_DATA_ROOT", "./data/lakehouse")
MANIFEST_ROOT = _env("MANIFEST_ROOT", "./data/manifests")
CHECKPOINT_ROOT = _env("CHECKPOINT_ROOT", "./checkpoints")

KAFKA_BOOTSTRAP_SERVERS = _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_EVENTS = _env("KAFKA_TOPIC_EVENTS", "ecom.events")
KAFKA_TOPIC_DLQ = _env("KAFKA_TOPIC_DLQ", "ecom.events.dlq")

SPARK_MASTER = _env("SPARK_MASTER", "local[*]")
SPARK_APP_NAME_PREFIX = _env("SPARK_APP_NAME_PREFIX", "ecommerce-lakehouse")

S3_ENDPOINT = _env("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = _env("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = _env("S3_SECRET_KEY", "minioadmin")
S3_REGION = _env("S3_REGION", "us-east-1")

BRONZE_PATH = _env("BRONZE_PATH", "s3a://bronze/events")
SILVER_PATH = _env("SILVER_PATH", "s3a://silver/events")
GOLD_PATH = _env("GOLD_PATH", "s3a://gold/analytics")

ICEBERG_CATALOG = _env("ICEBERG_CATALOG", "local_catalog")
ICEBERG_WAREHOUSE = _env("ICEBERG_WAREHOUSE", "s3a://warehouse")
