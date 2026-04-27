"""Runtime configuration for the local-first lakehouse implementation.

This module converts environment variables and CLI overrides into a single
`AppConfig` object. The configuration intentionally separates logical storage
URIs such as `s3a://bronze/events` from local materialized paths so the same
pipeline code can support a future MinIO/S3-backed deployment.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from common.constants import (
    BENCHMARK_OUTPUT_ROOT,
    BRONZE_PATH,
    CHECKPOINT_ROOT,
    DEPLOYMENT_PROFILE,
    ENV,
    GOLD_PATH,
    ICEBERG_CATALOG,
    ICEBERG_CATALOG_DRIVER,
    ICEBERG_CATALOG_PASSWORD,
    ICEBERG_CATALOG_TYPE,
    ICEBERG_CATALOG_URI,
    ICEBERG_CATALOG_USER,
    ICEBERG_WAREHOUSE,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_DLQ,
    KAFKA_TOPIC_EVENTS,
    KAFKA_TOPIC_MIN_INSYNC_REPLICAS,
    KAFKA_TOPIC_PARTITIONS,
    KAFKA_TOPIC_REPLICATION_FACTOR,
    LOCAL_DATA_ROOT,
    MANIFEST_ROOT,
    PROJECT_NAME,
    RAW_DATA_DIR,
    REPLAY_BATCH_SIZE,
    REPLAY_COMPRESSION_TYPE,
    REPLAY_KAFKA_ACKS,
    REPLAY_REPEAT_INPUT,
    REPLAY_REPORT_EVERY,
    REPLAY_RUNTIME_SECONDS,
    REPLAY_SLEEP_SECONDS,
    S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS,
    S3_REGION,
    S3_BUCKET_BRONZE,
    S3_BUCKET_GOLD,
    S3_BUCKET_SILVER,
    S3_BUCKET_WAREHOUSE,
    SAMPLE_DATA_DIR,
    SILVER_PATH,
    SPARK_APP_NAME_PREFIX,
    SPARK_MASTER,
    SPARK_SQL_SHUFFLE_PARTITIONS,
    STREAM_CHECKPOINT_LOCATION,
    STREAM_FAIL_ON_DATA_LOSS,
    STREAM_GOLD_REFRESH_MODE,
    STREAM_MAX_OFFSETS_PER_TRIGGER,
    STREAM_MODE,
    STREAM_PROGRESS_LOG_PATH,
    STREAM_QUERY_NAME,
    STREAM_STARTING_OFFSETS,
    STREAM_STOP_AFTER_SECONDS,
    STREAM_TIMEOUT_SECONDS,
    STREAM_TRIGGER_SECONDS,
)


@dataclass(frozen=True)
class AppConfig:
    """Container for pipeline settings shared across applications.

    Inputs:
        environment: Named deployment environment such as `local`.
        project_name: Logical project identifier used in run metadata.
        raw_data_dir: Root directory containing historical `.csv.gz` files.
        sample_data_dir: Root directory containing local sample CSV files.
        bronze_uri/silver_uri/gold_uri: Logical storage locations.
        local_data_root: Root directory used to mirror logical URIs locally.
        manifest_root: Directory used to store run manifests and audit metadata.
        checkpoint_root: Directory used for streaming checkpoints.
        kafka_bootstrap_servers: Kafka broker list for future streaming mode.
        kafka_topic_events/kafka_topic_dlq: Kafka topics for valid and rejected events.
        spark_master/spark_app_name_prefix: Spark runtime settings for future PySpark jobs.
        s3_endpoint/iceberg_catalog/iceberg_warehouse: Future-serving settings.

    Outputs:
        Immutable configuration object with helpers for common derived paths.

    Interactions:
        Consumed by app entrypoints, stage implementations, and documentation
        helpers to keep path resolution and operational metadata consistent.
    """

    environment: str
    deployment_profile: str
    project_name: str
    raw_data_dir: Path
    sample_data_dir: Path
    bronze_uri: str
    silver_uri: str
    gold_uri: str
    local_data_root: Path
    manifest_root: Path
    checkpoint_root: Path
    benchmark_output_root: Path
    kafka_bootstrap_servers: str
    kafka_topic_events: str
    kafka_topic_dlq: str
    kafka_topic_partitions: int
    kafka_topic_replication_factor: int
    kafka_topic_min_insync_replicas: int
    spark_master: str
    spark_app_name_prefix: str
    spark_sql_shuffle_partitions: int
    stream_mode: str
    stream_query_name: str
    stream_checkpoint_location: str
    stream_progress_log_path: str
    stream_starting_offsets: str
    stream_trigger_seconds: int
    stream_max_offsets_per_trigger: int
    stream_timeout_seconds: int
    stream_stop_after_seconds: int
    stream_fail_on_data_loss: bool
    stream_gold_refresh_mode: str
    replay_batch_size: int
    replay_sleep_seconds: float
    replay_runtime_seconds: int
    replay_report_every: int
    replay_repeat_input: bool
    replay_kafka_acks: str
    replay_compression_type: str
    s3_endpoint: str
    s3_region: str
    s3_path_style_access: bool
    s3_bucket_bronze: str
    s3_bucket_silver: str
    s3_bucket_gold: str
    s3_bucket_warehouse: str
    iceberg_catalog: str
    iceberg_catalog_type: str
    iceberg_catalog_uri: str
    iceberg_catalog_user: str
    iceberg_catalog_password: str
    iceberg_catalog_driver: str
    iceberg_warehouse: str

    @property
    def bronze_local_path(self) -> Path:
        """Return the local directory used for Bronze stage outputs."""

        return materialize_logical_uri(self.bronze_uri, self.local_data_root)

    @property
    def silver_local_path(self) -> Path:
        """Return the local directory used for Silver stage outputs."""

        return materialize_logical_uri(self.silver_uri, self.local_data_root)

    @property
    def gold_local_path(self) -> Path:
        """Return the local directory used for Gold stage outputs."""

        return materialize_logical_uri(self.gold_uri, self.local_data_root)


def materialize_logical_uri(uri: str, local_root: Path) -> Path:
    """Convert a logical URI or filesystem path into a writable local path.

    Inputs:
        uri: Logical storage location such as `s3a://bronze/events` or a local path.
        local_root: Root directory used to mirror object-storage style paths.

    Outputs:
        Local `Path` where the repository should write data during local runs.

    Internal logic:
        - Preserve plain filesystem paths.
        - Map `scheme://bucket/path` URIs to `<local_root>/<bucket>/<path>`.

    Interactions:
        Used by all stages to keep local batch runs compatible with future
        object-storage deployment targets.
    """

    if "://" not in uri:
        return Path(uri)

    _, remainder = uri.split("://", 1)
    return local_root / remainder


def load_config(
    *,
    bronze_uri: Optional[str] = None,
    silver_uri: Optional[str] = None,
    gold_uri: Optional[str] = None,
    local_data_root: Optional[str | Path] = None,
    manifest_root: Optional[str | Path] = None,
    checkpoint_root: Optional[str | Path] = None,
) -> AppConfig:
    """Build the application configuration from repository defaults.

    Inputs:
        bronze_uri/silver_uri/gold_uri: Optional CLI overrides for logical stage URIs.
        local_data_root/manifest_root/checkpoint_root: Optional overrides for
            local execution roots used by tests, temporary runs, or alternate environments.

    Outputs:
        Fully populated `AppConfig`.

    Interactions:
        Called from CLI entrypoints so stage jobs inherit the same runtime contract.
    """

    resolved_local_data_root = Path(local_data_root) if local_data_root is not None else Path(LOCAL_DATA_ROOT)
    resolved_manifest_root = Path(manifest_root) if manifest_root is not None else Path(MANIFEST_ROOT)
    resolved_checkpoint_root = Path(checkpoint_root) if checkpoint_root is not None else Path(CHECKPOINT_ROOT)

    return AppConfig(
        environment=ENV,
        deployment_profile=DEPLOYMENT_PROFILE,
        project_name=PROJECT_NAME,
        raw_data_dir=Path(RAW_DATA_DIR),
        sample_data_dir=Path(SAMPLE_DATA_DIR),
        bronze_uri=bronze_uri or BRONZE_PATH,
        silver_uri=silver_uri or SILVER_PATH,
        gold_uri=gold_uri or GOLD_PATH,
        local_data_root=resolved_local_data_root,
        manifest_root=resolved_manifest_root,
        checkpoint_root=resolved_checkpoint_root,
        benchmark_output_root=Path(BENCHMARK_OUTPUT_ROOT),
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic_events=KAFKA_TOPIC_EVENTS,
        kafka_topic_dlq=KAFKA_TOPIC_DLQ,
        kafka_topic_partitions=KAFKA_TOPIC_PARTITIONS,
        kafka_topic_replication_factor=KAFKA_TOPIC_REPLICATION_FACTOR,
        kafka_topic_min_insync_replicas=KAFKA_TOPIC_MIN_INSYNC_REPLICAS,
        spark_master=SPARK_MASTER,
        spark_app_name_prefix=SPARK_APP_NAME_PREFIX,
        spark_sql_shuffle_partitions=SPARK_SQL_SHUFFLE_PARTITIONS,
        stream_mode=STREAM_MODE,
        stream_query_name=STREAM_QUERY_NAME,
        stream_checkpoint_location=STREAM_CHECKPOINT_LOCATION,
        stream_progress_log_path=STREAM_PROGRESS_LOG_PATH,
        stream_starting_offsets=STREAM_STARTING_OFFSETS,
        stream_trigger_seconds=STREAM_TRIGGER_SECONDS,
        stream_max_offsets_per_trigger=STREAM_MAX_OFFSETS_PER_TRIGGER,
        stream_timeout_seconds=STREAM_TIMEOUT_SECONDS,
        stream_stop_after_seconds=STREAM_STOP_AFTER_SECONDS,
        stream_fail_on_data_loss=STREAM_FAIL_ON_DATA_LOSS,
        stream_gold_refresh_mode=STREAM_GOLD_REFRESH_MODE,
        replay_batch_size=REPLAY_BATCH_SIZE,
        replay_sleep_seconds=REPLAY_SLEEP_SECONDS,
        replay_runtime_seconds=REPLAY_RUNTIME_SECONDS,
        replay_report_every=REPLAY_REPORT_EVERY,
        replay_repeat_input=REPLAY_REPEAT_INPUT,
        replay_kafka_acks=REPLAY_KAFKA_ACKS,
        replay_compression_type=REPLAY_COMPRESSION_TYPE,
        s3_endpoint=S3_ENDPOINT,
        s3_region=S3_REGION,
        s3_path_style_access=S3_PATH_STYLE_ACCESS,
        s3_bucket_bronze=S3_BUCKET_BRONZE,
        s3_bucket_silver=S3_BUCKET_SILVER,
        s3_bucket_gold=S3_BUCKET_GOLD,
        s3_bucket_warehouse=S3_BUCKET_WAREHOUSE,
        iceberg_catalog=ICEBERG_CATALOG,
        iceberg_catalog_type=ICEBERG_CATALOG_TYPE,
        iceberg_catalog_uri=ICEBERG_CATALOG_URI,
        iceberg_catalog_user=ICEBERG_CATALOG_USER,
        iceberg_catalog_password=ICEBERG_CATALOG_PASSWORD,
        iceberg_catalog_driver=ICEBERG_CATALOG_DRIVER,
        iceberg_warehouse=ICEBERG_WAREHOUSE,
    )
