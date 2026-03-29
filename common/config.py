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
    BRONZE_PATH,
    CHECKPOINT_ROOT,
    ENV,
    GOLD_PATH,
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_DLQ,
    KAFKA_TOPIC_EVENTS,
    LOCAL_DATA_ROOT,
    MANIFEST_ROOT,
    PROJECT_NAME,
    RAW_DATA_DIR,
    S3_ENDPOINT,
    SAMPLE_DATA_DIR,
    SILVER_PATH,
    SPARK_APP_NAME_PREFIX,
    SPARK_MASTER,
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
    project_name: str
    raw_data_dir: Path
    sample_data_dir: Path
    bronze_uri: str
    silver_uri: str
    gold_uri: str
    local_data_root: Path
    manifest_root: Path
    checkpoint_root: Path
    kafka_bootstrap_servers: str
    kafka_topic_events: str
    kafka_topic_dlq: str
    spark_master: str
    spark_app_name_prefix: str
    s3_endpoint: str
    iceberg_catalog: str
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
        project_name=PROJECT_NAME,
        raw_data_dir=Path(RAW_DATA_DIR),
        sample_data_dir=Path(SAMPLE_DATA_DIR),
        bronze_uri=bronze_uri or BRONZE_PATH,
        silver_uri=silver_uri or SILVER_PATH,
        gold_uri=gold_uri or GOLD_PATH,
        local_data_root=resolved_local_data_root,
        manifest_root=resolved_manifest_root,
        checkpoint_root=resolved_checkpoint_root,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        kafka_topic_events=KAFKA_TOPIC_EVENTS,
        kafka_topic_dlq=KAFKA_TOPIC_DLQ,
        spark_master=SPARK_MASTER,
        spark_app_name_prefix=SPARK_APP_NAME_PREFIX,
        s3_endpoint=S3_ENDPOINT,
        iceberg_catalog=ICEBERG_CATALOG,
        iceberg_warehouse=ICEBERG_WAREHOUSE,
    )
