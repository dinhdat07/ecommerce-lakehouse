"""Historical batch backfill from raw CSV files into Iceberg Bronze, Silver, and Gold tables.

This Spark job is the Phase 1 batch-first implementation for the Docker stack.
It reads monthly historical CSV or CSV.GZ files, appends raw rows to Bronze,
incrementally inserts cleaned canonical rows into Silver, and recomputes only
the affected Gold date partitions as physical Iceberg tables.
"""

from __future__ import annotations

import argparse
import calendar
import os
import re
import sys
from datetime import date
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark import StorageLevel

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dq_iceberg import summarize_gold_dataframe_quality, summarize_silver_dataframe_quality
except ImportError:  # pragma: no cover - alternate import when running as a package
    from infra.jobs.dq_iceberg import summarize_gold_dataframe_quality, summarize_silver_dataframe_quality

from common.constants import SPARK_APP_NAME_PREFIX, SPARK_SQL_SHUFFLE_PARTITIONS  # noqa: E402
from common.observability import record_stage_event, stage_elapsed_seconds, stage_timer  # noqa: E402
from common.runtime import utc_now_iso  # noqa: E402

CATALOG = "lakehouse"
SCHEMA = "demo"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_events"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_events"
GOLD_DAILY_REVENUE = f"{CATALOG}.{SCHEMA}.daily_revenue"
GOLD_TOP_PRODUCTS = f"{CATALOG}.{SCHEMA}.top_products"
GOLD_CONVERSION_FUNNEL = f"{CATALOG}.{SCHEMA}.conversion_funnel_daily"
GOLD_CATEGORY_PERFORMANCE = f"{CATALOG}.{SCHEMA}.category_performance_daily"
GOLD_SESSION_FUNNEL = f"{CATALOG}.{SCHEMA}.session_funnel"
GOLD_USER_CONVERSION_PATH = f"{CATALOG}.{SCHEMA}.user_conversion_path"
GOLD_COHORT_RETENTION = f"{CATALOG}.{SCHEMA}.cohort_retention"
GOLD_REPEAT_PURCHASE = f"{CATALOG}.{SCHEMA}.repeat_purchase"
GOLD_PRODUCT_AFFINITY = f"{CATALOG}.{SCHEMA}.product_affinity"
GOLD_TIME_TO_CONVERSION_DISTRIBUTION = f"{CATALOG}.{SCHEMA}.time_to_conversion_distribution"
GOLD_RFM_SEGMENTATION = f"{CATALOG}.{SCHEMA}.rfm_segmentation"
GOLD_REFRESH_MODE_NONE = "none"
GOLD_REFRESH_MODE_AFFECTED_DATES = "affected_dates"
GOLD_REFRESH_MODE_FULL = "full"
GOLD_REFRESH_MODES = {
    GOLD_REFRESH_MODE_NONE,
    GOLD_REFRESH_MODE_AFFECTED_DATES,
    GOLD_REFRESH_MODE_FULL,
}
INCREMENTAL_GOLD_TABLES = [
    GOLD_DAILY_REVENUE,
    GOLD_TOP_PRODUCTS,
    GOLD_CONVERSION_FUNNEL,
    GOLD_CATEGORY_PERFORMANCE,
    GOLD_SESSION_FUNNEL,
    GOLD_USER_CONVERSION_PATH,
]
ADVANCED_GOLD_TABLES = [
    GOLD_COHORT_RETENTION,
    GOLD_REPEAT_PURCHASE,
    GOLD_PRODUCT_AFFINITY,
    GOLD_TIME_TO_CONVERSION_DISTRIBUTION,
    GOLD_RFM_SEGMENTATION,
]
ALL_GOLD_TABLES = INCREMENTAL_GOLD_TABLES + ADVANCED_GOLD_TABLES
HISTORICAL_FILE_RE = re.compile(r"^(?P<year>\d{4})-(?P<month>[A-Za-z]{3})\.csv(?:\.gz)?$")
MONTH_NAME_TO_NUMBER = {month[:3]: f"{index:02d}" for index, month in enumerate(calendar.month_name) if month}
TABLE_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "uncompressed",
    "write.target-file-size-bytes": "134217728",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "5",
    "commit.manifest.min-count-to-merge": "5",
    "commit.manifest.target-size-bytes": "8388608",
}


def env_bool(name: str, default: bool = False) -> bool:
    """Parse a permissive boolean environment flag."""

    return os.getenv(name, "true" if default else "false").strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int = 0) -> int:
    """Parse an integer environment flag, falling back on invalid values."""

    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the historical batch backfill job."""

    parser = argparse.ArgumentParser(description="Materialize Bronze, Silver, and Gold Iceberg tables from raw data.")
    parser.add_argument("--input-dir", default="/workspace/data/raw", help="Directory containing raw monthly CSV files.")
    parser.add_argument("--input-file", help="Single CSV or CSV.GZ file to materialize, typically a demo sample.")
    parser.add_argument("--start-month", default="2019-10", help="Inclusive start month in YYYY-MM format.")
    parser.add_argument("--end-month", default="2020-02", help="Inclusive end month in YYYY-MM format.")
    parser.add_argument(
        "--source-month",
        help="Logical source month in YYYY-MM format when --input-file does not follow the historical naming convention.",
    )
    parser.add_argument(
        "--resume-from",
        choices=["full", "silver"],
        default="full",
        help="Start from raw input (`full`) or reuse existing Bronze rows for Silver/Gold recovery (`silver`).",
    )
    parser.add_argument(
        "--reset-tables",
        action="store_true",
        help="Drop the current Bronze, Silver, and Gold demo tables before materializing new output.",
    )
    return parser.parse_args()


def resolve_spark_local_dir() -> str:
    """Pick a writable Spark spill directory without defaulting server runs to /tmp."""

    configured = os.getenv("SPARK_LOCAL_DIR") or os.getenv("SPARK_LOCAL_DIRS")
    if configured:
        return configured.split(",", 1)[0]

    server_spill_dir = Path("/srv/ecommerce/spark-tmp")
    try:
        server_spill_dir.mkdir(parents=True, exist_ok=True)
        if os.access(server_spill_dir, os.W_OK):
            return str(server_spill_dir)
    except OSError:
        pass
    return "/tmp"


def build_spark_session() -> SparkSession:
    """Create the Spark session used for the batch backfill."""

    builder = (
        SparkSession.builder.appName(f"{SPARK_APP_NAME_PREFIX}-batch-backfill")
        .config("spark.sql.adaptive.enabled", os.getenv("SPARK_SQL_ADAPTIVE_ENABLED", "true"))
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            os.getenv("SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS_ENABLED", "true"),
        )
        .config(
            "spark.sql.adaptive.advisoryPartitionSizeInBytes",
            os.getenv("SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE", "32m"),
        )
        .config("spark.sql.files.maxPartitionBytes", os.getenv("SPARK_SQL_FILES_MAX_PARTITION_BYTES", "64m"))
        .config("spark.local.dir", resolve_spark_local_dir())
    )
    spark = builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", str(SPARK_SQL_SHUFFLE_PARTITIONS))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def log_spark_runtime_config(spark: SparkSession, *, output_path: str = "") -> None:
    """Log the execution settings that determine shuffle, spill, and storage behavior."""

    conf = spark.sparkContext.getConf()
    keys = [
        "spark.master",
        "spark.local.dir",
        "spark.sql.shuffle.partitions",
        "spark.default.parallelism",
        "spark.driver.memory",
        "spark.executor.memory",
        "spark.executor.cores",
        "spark.sql.adaptive.enabled",
        "spark.sql.files.maxPartitionBytes",
        "spark.hadoop.fs.s3a.endpoint",
        "spark.sql.catalog.lakehouse.s3.endpoint",
    ]
    print("Spark execution safety config:")
    for key in keys:
        print(f" - {key}={conf.get(key, '<unset>')}")
    print(f" - S3_ENDPOINT={os.getenv('S3_ENDPOINT', '<unset>')}")
    print(f" - benchmark_output_path={output_path or os.getenv('BENCHMARK_OUTPUT_ROOT', '<unset>')}")


def month_to_date(month_value: str) -> date:
    """Convert a YYYY-MM month string into a date anchored on the first day."""

    year_text, month_text = month_value.split("-", 1)
    return date(int(year_text), int(month_text), 1)


def iter_months(start_month: str, end_month: str) -> list[str]:
    """Return the inclusive month range between two YYYY-MM values."""

    current = month_to_date(start_month)
    end = month_to_date(end_month)
    months: list[str] = []
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return months


def extract_source_month(path: Path) -> str | None:
    """Extract the logical source month from a historical file name."""

    match = HISTORICAL_FILE_RE.match(path.name)
    if match is None:
        return None
    month_number = MONTH_NAME_TO_NUMBER.get(match.group("month").title())
    if month_number is None:
        return None
    return f"{match.group('year')}-{month_number}"


def discover_input_files(input_dir: Path, start_month: str, end_month: str) -> tuple[list[Path], list[str]]:
    """Return historical files inside the requested month range and any missing months."""

    expected_months = iter_months(start_month, end_month)
    selected_files: list[Path] = []
    available_months: set[str] = set()

    for candidate in sorted(input_dir.glob("*.csv")) + sorted(input_dir.glob("*.csv.gz")):
        source_month = extract_source_month(candidate)
        if source_month is None:
            continue
        if start_month <= source_month <= end_month:
            selected_files.append(candidate)
            available_months.add(source_month)

    missing_months = [month for month in expected_months if month not in available_months]
    return selected_files, missing_months


def read_bronze_slice(spark: SparkSession, start_month: str, end_month: str) -> DataFrame:
    """Read the requested Bronze month range from Iceberg for recovery-mode runs."""

    requested_months = iter_months(start_month, end_month)
    return spark.table(BRONZE_TABLE).where(F.col("source_month").isin(requested_months))


def quoted_table_properties() -> str:
    """Return Iceberg table properties as a SQL fragment."""

    return ", ".join(f"'{key}'='{value}'" for key, value in TABLE_PROPERTIES.items())


def append_to_iceberg(table_name: str, dataframe: DataFrame, partitions: int | None = None) -> None:
    """Append rows to Iceberg with fanout enabled for historical backfills.

    Historical monthly gzip files are not splittable at read time, so Spark may
    start with very few partitions. Repartitioning before the append and
    enabling Iceberg fanout prevents a whole month from collapsing into one
    oversized writer task when many rows share the same partition value.
    """

    prepared = dataframe.repartition(partitions) if partitions is not None else dataframe
    prepared.writeTo(table_name).option("fanout-enabled", "true").append()


def infer_source_month_from_event_time(column_name: str) -> Column:
    """Infer a logical source month in YYYY-MM format from an event timestamp."""

    parsed_event_time = parse_event_timestamp(column_name)
    return F.date_format(parsed_event_time, "yyyy-MM")


def drop_tables_if_requested(spark: SparkSession, enabled: bool) -> None:
    """Drop existing demo tables when a clean demo reset is requested."""

    if not enabled:
        return

    for table_name in [
        GOLD_RFM_SEGMENTATION,
        GOLD_TIME_TO_CONVERSION_DISTRIBUTION,
        GOLD_PRODUCT_AFFINITY,
        GOLD_REPEAT_PURCHASE,
        GOLD_COHORT_RETENTION,
        GOLD_USER_CONVERSION_PATH,
        GOLD_SESSION_FUNNEL,
        GOLD_CATEGORY_PERFORMANCE,
        GOLD_CONVERSION_FUNNEL,
        GOLD_TOP_PRODUCTS,
        GOLD_DAILY_REVENUE,
        SILVER_TABLE,
        BRONZE_TABLE,
    ]:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def create_tables_if_needed(spark: SparkSession) -> None:
    """Create the Bronze, Silver, and Gold Iceberg tables if they do not already exist."""

    table_properties = quoted_table_properties()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SCHEMA}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            batch_run_id STRING,
            ingested_at TIMESTAMP,
            source_type STRING,
            source_file STRING,
            source_month STRING,
            record_hash STRING,
            event_time STRING,
            event_type STRING,
            product_id STRING,
            category_id STRING,
            category_code STRING,
            brand STRING,
            price STRING,
            user_id STRING,
            user_session STRING
        )
        USING iceberg
        PARTITIONED BY (source_month)
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            event_id STRING,
            dedupe_key STRING,
            batch_run_id STRING,
            ingested_at TIMESTAMP,
            source_file STRING,
            source_month STRING,
            record_hash STRING,
            event_time TIMESTAMP,
            event_date DATE,
            event_type STRING,
            product_id BIGINT,
            category_id BIGINT,
            category_code STRING,
            brand STRING,
            price DOUBLE,
            user_id BIGINT,
            user_session STRING
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DAILY_REVENUE} (
            event_date DATE,
            purchase_count BIGINT,
            purchase_revenue DOUBLE,
            unique_buyers BIGINT,
            average_order_value DOUBLE
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TOP_PRODUCTS} (
            event_date DATE,
            product_rank INT,
            product_id BIGINT,
            category_id BIGINT,
            category_code STRING,
            brand STRING,
            views BIGINT,
            carts BIGINT,
            purchases BIGINT,
            purchase_revenue DOUBLE,
            unique_users BIGINT,
            view_to_purchase_rate DOUBLE
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_CONVERSION_FUNNEL} (
            event_date DATE,
            views BIGINT,
            carts BIGINT,
            purchases BIGINT,
            view_to_cart_rate DOUBLE,
            cart_to_purchase_rate DOUBLE,
            view_to_purchase_rate DOUBLE
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_CATEGORY_PERFORMANCE} (
            event_date DATE,
            category_id BIGINT,
            category_code STRING,
            views BIGINT,
            carts BIGINT,
            purchases BIGINT,
            purchase_revenue DOUBLE,
            unique_buyers BIGINT,
            average_order_value DOUBLE
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_SESSION_FUNNEL} (
            event_date DATE,
            user_id BIGINT,
            session_id STRING,
            session_start_time TIMESTAMP,
            session_end_time TIMESTAMP,
            session_duration_seconds BIGINT,
            total_events BIGINT,
            distinct_products BIGINT,
            views BIGINT,
            carts BIGINT,
            purchases BIGINT,
            purchase_revenue DOUBLE,
            path_label STRING,
            converted BOOLEAN
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_USER_CONVERSION_PATH} (
            event_date DATE,
            user_id BIGINT,
            session_count BIGINT,
            first_event_time TIMESTAMP,
            last_event_time TIMESTAMP,
            views BIGINT,
            carts BIGINT,
            purchases BIGINT,
            purchase_revenue DOUBLE,
            path_label STRING,
            converted BOOLEAN
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_COHORT_RETENTION} (
            cohort_month STRING,
            period_offset INT,
            cohort_users BIGINT,
            active_users BIGINT,
            retention_rate DOUBLE
        )
        USING iceberg
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_REPEAT_PURCHASE} (
            activity_month STRING,
            purchasers BIGINT,
            repeat_purchasers BIGINT,
            repeat_purchase_rate DOUBLE
        )
        USING iceberg
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_PRODUCT_AFFINITY} (
            product_a BIGINT,
            product_b BIGINT,
            co_purchase_sessions BIGINT,
            affinity_lift DOUBLE
        )
        USING iceberg
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TIME_TO_CONVERSION_DISTRIBUTION} (
            event_date DATE,
            time_bucket STRING,
            conversions BIGINT
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES ({table_properties})
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_RFM_SEGMENTATION} (
            as_of_date DATE,
            user_id BIGINT,
            recency_days INT,
            frequency_90d BIGINT,
            monetary_90d DOUBLE,
            r_score INT,
            f_score INT,
            m_score INT,
            rfm_segment STRING
        )
        USING iceberg
        TBLPROPERTIES ({table_properties})
        """
    )


def project_bronze_columns(
    raw: DataFrame,
    *,
    batch_run_id: str,
    source_type: str,
    source_file: Column,
    source_month: Column,
) -> DataFrame:
    """Project raw event columns into the shared Bronze Iceberg schema."""

    return (
        raw.select(
            F.lit(batch_run_id).alias("batch_run_id"),
            F.current_timestamp().alias("ingested_at"),
            F.lit(source_type).alias("source_type"),
            source_file.alias("source_file"),
            source_month.alias("source_month"),
            F.sha2(F.concat_ws("||", source_file, *[F.coalesce(F.col(column), F.lit("")) for column in raw.columns]), 256)
            .alias("record_hash"),
            F.col("event_time").cast("string").alias("event_time"),
            F.col("event_type").cast("string").alias("event_type"),
            F.col("product_id").cast("string").alias("product_id"),
            F.col("category_id").cast("string").alias("category_id"),
            F.col("category_code").cast("string").alias("category_code"),
            F.col("brand").cast("string").alias("brand"),
            F.col("price").cast("string").alias("price"),
            F.col("user_id").cast("string").alias("user_id"),
            F.col("user_session").cast("string").alias("user_session"),
        )
        .where(F.col("source_month").isNotNull())
    )


def read_bronze_batch(
    spark: SparkSession,
    input_files: list[Path],
    batch_run_id: str,
    source_month_override: str | None = None,
) -> DataFrame:
    """Read historical raw files and project them into the Bronze table schema."""

    month_map = F.create_map(
        *[
            value
            for pair in MONTH_NAME_TO_NUMBER.items()
            for value in (F.lit(pair[0]), F.lit(pair[1]))
        ]
    )
    raw = spark.read.option("header", True).csv([str(path) for path in input_files])
    source_file = F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)
    source_year = F.regexp_extract(source_file, r"^(\d{4})-[A-Za-z]{3}\.csv(?:\.gz)?$", 1)
    source_month_name = F.initcap(F.regexp_extract(source_file, r"^\d{4}-([A-Za-z]{3})\.csv(?:\.gz)?$", 1))
    source_month = (
        F.lit(source_month_override)
        if source_month_override is not None
        else F.concat_ws("-", source_year, month_map[source_month_name])
    )

    return project_bronze_columns(
        raw,
        batch_run_id=batch_run_id,
        source_type="historical_csv",
        source_file=source_file,
        source_month=source_month,
    )


def build_streaming_bronze_batch(batch_df: DataFrame, batch_run_id: str) -> DataFrame:
    """Project a parsed Kafka replay micro-batch into the shared Bronze schema."""

    source_file = F.coalesce(F.col("source_file"), F.lit("kafka_replay"))
    source_month = F.coalesce(F.col("source_month"), infer_source_month_from_event_time("event_time"))
    return project_bronze_columns(
        batch_df,
        batch_run_id=batch_run_id,
        source_type="kafka_replay",
        source_file=source_file,
        source_month=source_month,
    )


def normalize_nullable_text(column_name: str) -> F.Column:
    """Trim a string column and convert blank values to null."""

    return F.when(F.length(F.trim(F.col(column_name))) == 0, F.lit(None)).otherwise(F.trim(F.col(column_name)))


def parse_event_timestamp(column_name: str) -> F.Column:
    """Parse raw timestamp strings into UTC Spark timestamps."""

    return F.coalesce(
        F.to_timestamp(F.col(column_name), "yyyy-MM-dd'T'HH:mm:ssX"),
        F.to_timestamp(F.col(column_name), "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(F.col(column_name), "yyyy-MM-dd HH:mm:ss 'UTC'"),
        F.to_timestamp(F.col(column_name), "yyyy-MM-dd HH:mm:ssX"),
        F.to_timestamp(F.col(column_name)),
    )


def build_silver_base_rows(bronze_batch: DataFrame) -> DataFrame:
    """Normalize Bronze rows into Silver-shaped records before deduplication."""

    parsed_event_time = parse_event_timestamp("event_time")
    normalized_event_type = normalize_nullable_text("event_type")

    return bronze_batch.select(
        F.col("record_hash").alias("event_id"),
        F.col("batch_run_id"),
        F.col("ingested_at"),
        F.col("source_file"),
        F.col("source_month"),
        F.col("record_hash"),
        normalize_nullable_text("event_time").alias("_raw_event_time"),
        normalize_nullable_text("event_type").alias("_raw_event_type"),
        parsed_event_time.alias("event_time"),
        F.to_date(parsed_event_time).alias("event_date"),
        F.lower(normalized_event_type).alias("event_type"),
        normalize_nullable_text("product_id").cast("bigint").alias("product_id"),
        normalize_nullable_text("category_id").cast("bigint").alias("category_id"),
        F.lower(normalize_nullable_text("category_code")).alias("category_code"),
        normalize_nullable_text("brand").alias("brand"),
        normalize_nullable_text("price").cast("double").alias("price"),
        normalize_nullable_text("user_id").cast("bigint").alias("user_id"),
        normalize_nullable_text("user_session").alias("user_session"),
    )


def deduplicate_silver_candidates(silver_base_rows: DataFrame) -> DataFrame:
    """Return canonical Silver candidates with deterministic deduplication applied."""

    canonical_event_time = F.date_format(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    candidates = (
        silver_base_rows.where(F.col("event_time").isNotNull() & F.col("event_type").isNotNull())
        .withColumn(
            "dedupe_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(canonical_event_time, F.lit("")),
                    F.coalesce(F.col("event_type"), F.lit("")),
                    F.coalesce(F.col("product_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("category_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("category_code"), F.lit("")),
                    F.coalesce(F.col("brand"), F.lit("")),
                    F.coalesce(F.col("price").cast("string"), F.lit("")),
                    F.coalesce(F.col("user_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("user_session"), F.lit("")),
                ),
                256,
            ),
        )
    )
    dedupe_window = Window.partitionBy("dedupe_key").orderBy(F.col("event_time").asc(), F.col("record_hash").asc())
    return (
        candidates.withColumn("row_num", F.row_number().over(dedupe_window))
        .where(F.col("row_num") == 1)
        .drop("row_num")
        .drop("_raw_event_time", "_raw_event_type")
    )


def build_silver_candidates(bronze_batch: DataFrame) -> DataFrame:
    """Normalize Bronze rows into dedupable Silver candidates."""

    return deduplicate_silver_candidates(build_silver_base_rows(bronze_batch))


def maybe_broadcast_key_projection(keys: DataFrame, *, threshold_env: str, label: str) -> DataFrame:
    """Broadcast small dedupe key projections only when explicitly configured."""

    threshold = env_int(threshold_env, 0)
    if threshold <= 0:
        return keys
    key_count = keys.count()
    if key_count <= threshold:
        print(f"{label} dedupe key projection broadcast enabled: {key_count} <= {threshold}")
        return F.broadcast(keys)
    print(f"{label} dedupe key projection broadcast skipped: {key_count} > {threshold}")
    return keys


def insert_new_silver_rows(spark: SparkSession, silver_candidates: DataFrame) -> DataFrame:
    """Return only previously unseen Silver rows."""

    affected_dates = [
        str(row["event_date"])
        for row in silver_candidates.select("event_date").where(F.col("event_date").isNotNull()).distinct().collect()
    ]
    existing_keys = spark.table(SILVER_TABLE).select("event_date", "dedupe_key")
    if affected_dates:
        # The dedupe key includes event_time, so matching historical keys must be on the same event_date partition.
        existing_keys = existing_keys.where(F.col("event_date").cast("string").isin(affected_dates))
    existing_keys = existing_keys.select("dedupe_key").where(F.col("dedupe_key").isNotNull()).distinct()
    if env_bool("SILVER_DEDUPE_CACHE_KEYS", False):
        existing_keys = existing_keys.persist(StorageLevel.MEMORY_ONLY)
        existing_keys.count()
    existing_keys = maybe_broadcast_key_projection(
        existing_keys,
        threshold_env="SILVER_DEDUPE_BROADCAST_MAX_KEYS",
        label="Silver",
    )

    joined = silver_candidates.join(existing_keys, on="dedupe_key", how="left_anti")
    if env_bool("SILVER_DEDUPE_EXPLAIN", False):
        print(f"Silver dedupe affected_date_partitions={affected_dates}")
        joined.explain(mode="formatted")
    return joined


def insert_new_bronze_rows(spark: SparkSession, bronze_batch: DataFrame) -> DataFrame:
    """Return only previously unseen Bronze rows for rerun-safe raw ingestion.

    Bronze deduplication keys off `record_hash`, which includes the source file
    name and the raw column payload. Silver still derives from the full source
    batch so reruns can recover if a prior attempt committed Bronze but failed
    before Silver or Gold completed.
    """

    affected_source_files = [
        str(row["source_file"])
        for row in bronze_batch.select("source_file").where(F.col("source_file").isNotNull()).distinct().collect()
    ]
    existing_hashes = spark.table(BRONZE_TABLE).select("source_file", "record_hash")
    if affected_source_files:
        # `source_file` is part of the record_hash input, so unrelated files cannot contain matching hashes.
        existing_hashes = existing_hashes.where(F.col("source_file").isin(affected_source_files))
    existing_hashes = existing_hashes.select("record_hash").where(F.col("record_hash").isNotNull()).distinct()
    existing_hashes = maybe_broadcast_key_projection(
        existing_hashes,
        threshold_env="BRONZE_DEDUPE_BROADCAST_MAX_KEYS",
        label="Bronze",
    )
    joined = bronze_batch.join(existing_hashes, on="record_hash", how="left_anti")
    if env_bool("BRONZE_DEDUPE_EXPLAIN", False):
        print(f"Bronze dedupe affected_source_files={affected_source_files}")
        joined.explain(mode="formatted")
    return joined


def stage_path_label(first_view_at: str, first_cart_at: str, first_purchase_at: str) -> F.Column:
    """Build a compact conversion path label from earliest event timestamps."""

    return F.expr(
        "concat_ws('>', transform(array_sort(filter(array("
        f"named_struct('ts', {first_view_at}, 'stage', 'view'), "
        f"named_struct('ts', {first_cart_at}, 'stage', 'cart'), "
        f"named_struct('ts', {first_purchase_at}, 'stage', 'purchase')"
        "), x -> x.ts is not null)), x -> x.stage))"
    )


def build_sessionized_events(silver_slice: DataFrame) -> DataFrame:
    """Resolve sessions for Gold session analytics.

    The primary session identifier is the dataset's `user_session` column.
    When it is missing, fallback sessions are derived per `(event_date, user_id)`
    using a 30-minute inactivity rule.
    """

    user_window = Window.partitionBy("event_date", "user_id").orderBy(F.col("event_time").asc(), F.col("event_id").asc())
    previous_event_time = F.lag("event_time").over(user_window)
    previous_user_session = F.lag("user_session").over(user_window)
    gap_minutes = (F.unix_timestamp("event_time") - F.unix_timestamp(previous_event_time)) / F.lit(60.0)
    derived_break = (
        F.when(F.col("user_session").isNotNull(), F.lit(0))
        .when(previous_event_time.isNull(), F.lit(1))
        .when(previous_user_session.isNotNull(), F.lit(1))
        .when(gap_minutes > F.lit(30.0), F.lit(1))
        .otherwise(F.lit(0))
    )
    derived_index = F.sum(derived_break).over(user_window.rowsBetween(Window.unboundedPreceding, Window.currentRow))
    fallback_session = F.concat_ws(
        "-",
        F.lit("derived"),
        F.coalesce(F.col("user_id").cast("string"), F.lit("anonymous")),
        F.date_format(F.col("event_date"), "yyyyMMdd"),
        F.lpad(derived_index.cast("string"), 4, "0"),
    )

    return silver_slice.withColumn("session_id", F.coalesce(F.col("user_session"), fallback_session))


def safe_ratio(numerator: F.Column, denominator: F.Column, scale: int = 4) -> F.Column:
    """Return a rounded ratio or zero when the denominator is zero."""

    return F.when(denominator == 0, F.lit(0.0)).otherwise(F.round(numerator / denominator.cast("double"), scale))


def build_daily_revenue(silver_slice: DataFrame) -> DataFrame:
    """Aggregate daily purchase metrics."""

    purchases = silver_slice.where(F.col("event_type") == "purchase")
    daily = (
        purchases.groupBy("event_date")
        .agg(
            F.count("*").alias("purchase_count"),
            F.round(F.sum(F.coalesce(F.col("price"), F.lit(0.0))), 2).alias("purchase_revenue"),
            F.countDistinct("user_id").alias("unique_buyers"),
        )
        .withColumn("average_order_value", safe_ratio(F.col("purchase_revenue"), F.col("purchase_count"), scale=2))
    )
    return daily.select("event_date", "purchase_count", "purchase_revenue", "unique_buyers", "average_order_value")


def build_top_products(silver_slice: DataFrame) -> DataFrame:
    """Aggregate daily top products ranked by purchase revenue."""

    product_agg = (
        silver_slice.groupBy("event_date", "product_id", "category_id", "category_code", "brand")
        .agg(
            F.sum(F.when(F.col("event_type") == "view", F.lit(1)).otherwise(F.lit(0))).alias("views"),
            F.sum(F.when(F.col("event_type") == "cart", F.lit(1)).otherwise(F.lit(0))).alias("carts"),
            F.sum(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("purchases"),
            F.round(
                F.sum(F.when(F.col("event_type") == "purchase", F.coalesce(F.col("price"), F.lit(0.0))).otherwise(F.lit(0.0))),
                2,
            ).alias("purchase_revenue"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .withColumn("view_to_purchase_rate", safe_ratio(F.col("purchases"), F.col("views")))
    )

    product_rank = Window.partitionBy("event_date").orderBy(
        F.col("purchase_revenue").desc(),
        F.col("purchases").desc(),
        F.col("views").desc(),
        F.col("product_id").asc_nulls_last(),
    )
    return (
        product_agg.withColumn("product_rank", F.row_number().over(product_rank))
        .where(F.col("product_rank") <= 10)
        .select(
            "event_date",
            "product_rank",
            "product_id",
            "category_id",
            "category_code",
            "brand",
            "views",
            "carts",
            "purchases",
            "purchase_revenue",
            "unique_users",
            "view_to_purchase_rate",
        )
    )


def build_conversion_funnel_daily(silver_slice: DataFrame) -> DataFrame:
    """Aggregate daily funnel counts and rates."""

    return (
        silver_slice.groupBy("event_date")
        .agg(
            F.sum(F.when(F.col("event_type") == "view", F.lit(1)).otherwise(F.lit(0))).alias("views"),
            F.sum(F.when(F.col("event_type") == "cart", F.lit(1)).otherwise(F.lit(0))).alias("carts"),
            F.sum(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("purchases"),
        )
        .withColumn("view_to_cart_rate", safe_ratio(F.col("carts"), F.col("views")))
        .withColumn("cart_to_purchase_rate", safe_ratio(F.col("purchases"), F.col("carts")))
        .withColumn("view_to_purchase_rate", safe_ratio(F.col("purchases"), F.col("views")))
        .select(
            "event_date",
            "views",
            "carts",
            "purchases",
            "view_to_cart_rate",
            "cart_to_purchase_rate",
            "view_to_purchase_rate",
        )
    )


def build_category_performance_daily(silver_slice: DataFrame) -> DataFrame:
    """Aggregate daily category performance metrics."""

    return (
        silver_slice.groupBy("event_date", "category_id", "category_code")
        .agg(
            F.sum(F.when(F.col("event_type") == "view", F.lit(1)).otherwise(F.lit(0))).alias("views"),
            F.sum(F.when(F.col("event_type") == "cart", F.lit(1)).otherwise(F.lit(0))).alias("carts"),
            F.sum(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("purchases"),
            F.round(
                F.sum(F.when(F.col("event_type") == "purchase", F.coalesce(F.col("price"), F.lit(0.0))).otherwise(F.lit(0.0))),
                2,
            ).alias("purchase_revenue"),
            F.countDistinct(F.when(F.col("event_type") == "purchase", F.col("user_id"))).alias("unique_buyers"),
        )
        .withColumn("average_order_value", safe_ratio(F.col("purchase_revenue"), F.col("purchases"), scale=2))
        .select(
            "event_date",
            "category_id",
            "category_code",
            "views",
            "carts",
            "purchases",
            "purchase_revenue",
            "unique_buyers",
            "average_order_value",
        )
    )


def build_session_funnel(sessionized: DataFrame) -> DataFrame:
    """Aggregate one row per resolved session."""

    return (
        sessionized.groupBy("event_date", "user_id", "session_id")
        .agg(
            F.min("event_time").alias("session_start_time"),
            F.max("event_time").alias("session_end_time"),
            F.count("*").alias("total_events"),
            F.countDistinct("product_id").alias("distinct_products"),
            F.sum(F.when(F.col("event_type") == "view", F.lit(1)).otherwise(F.lit(0))).alias("views"),
            F.sum(F.when(F.col("event_type") == "cart", F.lit(1)).otherwise(F.lit(0))).alias("carts"),
            F.sum(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("purchases"),
            F.round(
                F.sum(F.when(F.col("event_type") == "purchase", F.coalesce(F.col("price"), F.lit(0.0))).otherwise(F.lit(0.0))),
                2,
            ).alias("purchase_revenue"),
            F.min(F.when(F.col("event_type") == "view", F.col("event_time"))).alias("first_view_at"),
            F.min(F.when(F.col("event_type") == "cart", F.col("event_time"))).alias("first_cart_at"),
            F.min(F.when(F.col("event_type") == "purchase", F.col("event_time"))).alias("first_purchase_at"),
        )
        .withColumn(
            "session_duration_seconds",
            F.greatest(F.unix_timestamp("session_end_time") - F.unix_timestamp("session_start_time"), F.lit(0)),
        )
        .withColumn("path_label", stage_path_label("first_view_at", "first_cart_at", "first_purchase_at"))
        .withColumn("converted", F.col("purchases") > 0)
        .drop("first_view_at", "first_cart_at", "first_purchase_at")
        .select(
            "event_date",
            "user_id",
            "session_id",
            "session_start_time",
            "session_end_time",
            "session_duration_seconds",
            "total_events",
            "distinct_products",
            "views",
            "carts",
            "purchases",
            "purchase_revenue",
            "path_label",
            "converted",
        )
    )


def build_user_conversion_path(sessionized: DataFrame) -> DataFrame:
    """Aggregate one row per user per day using ordered first-touch funnel stages."""

    return (
        sessionized.groupBy("event_date", "user_id")
        .agg(
            F.countDistinct("session_id").alias("session_count"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
            F.sum(F.when(F.col("event_type") == "view", F.lit(1)).otherwise(F.lit(0))).alias("views"),
            F.sum(F.when(F.col("event_type") == "cart", F.lit(1)).otherwise(F.lit(0))).alias("carts"),
            F.sum(F.when(F.col("event_type") == "purchase", F.lit(1)).otherwise(F.lit(0))).alias("purchases"),
            F.round(
                F.sum(F.when(F.col("event_type") == "purchase", F.coalesce(F.col("price"), F.lit(0.0))).otherwise(F.lit(0.0))),
                2,
            ).alias("purchase_revenue"),
            F.min(F.when(F.col("event_type") == "view", F.col("event_time"))).alias("first_view_at"),
            F.min(F.when(F.col("event_type") == "cart", F.col("event_time"))).alias("first_cart_at"),
            F.min(F.when(F.col("event_type") == "purchase", F.col("event_time"))).alias("first_purchase_at"),
        )
        .withColumn("path_label", stage_path_label("first_view_at", "first_cart_at", "first_purchase_at"))
        .withColumn("converted", F.col("purchases") > 0)
        .drop("first_view_at", "first_cart_at", "first_purchase_at")
        .select(
            "event_date",
            "user_id",
            "session_count",
            "first_event_time",
            "last_event_time",
            "views",
            "carts",
            "purchases",
            "purchase_revenue",
            "path_label",
            "converted",
        )
    )


def delete_all_rows(spark: SparkSession, table_name: str) -> None:
    """Delete all rows from a table before a full-table refresh."""

    spark.sql(f"DELETE FROM {table_name} WHERE true")


def build_cohort_retention(full_silver: DataFrame) -> DataFrame:
    """Build first-purchase monthly cohorts with monthly purchase retention."""

    spark = full_silver.sparkSession
    empty = spark.createDataFrame(
        [],
        "cohort_month STRING, period_offset INT, cohort_users BIGINT, active_users BIGINT, retention_rate DOUBLE",
    )
    purchases = full_silver.where(
        (F.col("event_type") == F.lit("purchase")) & F.col("user_id").isNotNull()
    ).select("user_id", "event_date")
    if purchases.limit(1).count() == 0:
        return empty

    first_purchase = purchases.groupBy("user_id").agg(F.min("event_date").alias("first_purchase_date"))
    cohorts = first_purchase.withColumn("cohort_month", F.date_format("first_purchase_date", "yyyy-MM"))
    active = (
        purchases.join(cohorts.select("user_id", "cohort_month", "first_purchase_date"), "user_id", "inner")
        .withColumn(
            "period_offset",
            F.months_between(F.trunc(F.col("event_date"), "month"), F.trunc(F.col("first_purchase_date"), "month")).cast(
                "int"
            ),
        )
        .where(F.col("period_offset") >= 0)
        .groupBy("cohort_month", "period_offset")
        .agg(F.countDistinct("user_id").alias("active_users"))
    )
    cohort_sizes = cohorts.groupBy("cohort_month").agg(F.countDistinct("user_id").alias("cohort_users"))
    return (
        active.join(cohort_sizes, "cohort_month", "inner")
        .withColumn("retention_rate", safe_ratio(F.col("active_users"), F.col("cohort_users")))
        .select("cohort_month", "period_offset", "cohort_users", "active_users", "retention_rate")
        .orderBy("cohort_month", "period_offset")
    )


def build_repeat_purchase(full_silver: DataFrame) -> DataFrame:
    """Build monthly repeat-purchase rates based on each user's first purchase month."""

    spark = full_silver.sparkSession
    empty = spark.createDataFrame(
        [],
        "activity_month STRING, purchasers BIGINT, repeat_purchasers BIGINT, repeat_purchase_rate DOUBLE",
    )
    purchases = full_silver.where((F.col("event_type") == F.lit("purchase")) & F.col("user_id").isNotNull())
    if purchases.limit(1).count() == 0:
        return empty

    first_purchase = purchases.groupBy("user_id").agg(F.min("event_date").alias("first_purchase_date"))
    purchase_months = purchases.select(
        "user_id",
        F.date_format(F.date_trunc("month", F.col("event_date")), "yyyy-MM").alias("activity_month"),
    ).distinct()
    month_start = F.to_date(F.concat_ws("-", F.col("activity_month"), F.lit("01")))
    repeat_users = (
        purchase_months.join(first_purchase, "user_id", "inner")
        .where(F.col("first_purchase_date") < month_start)
        .select("activity_month", "user_id")
        .distinct()
    )
    purchasers = purchase_months.groupBy("activity_month").agg(F.countDistinct("user_id").alias("purchasers"))
    repeats = repeat_users.groupBy("activity_month").agg(F.countDistinct("user_id").alias("repeat_purchasers"))
    return (
        purchasers.join(repeats, "activity_month", "left")
        .fillna(0, subset=["repeat_purchasers"])
        .withColumn("repeat_purchase_rate", safe_ratio(F.col("repeat_purchasers"), F.col("purchasers")))
        .select("activity_month", "purchasers", "repeat_purchasers", "repeat_purchase_rate")
        .orderBy("activity_month")
    )


def build_product_affinity(sessionized: DataFrame, *, top_pairs: int = 200) -> DataFrame:
    """Build product-pair affinity based on products purchased within the same resolved session."""

    spark = sessionized.sparkSession
    empty = spark.createDataFrame([], "product_a BIGINT, product_b BIGINT, co_purchase_sessions BIGINT, affinity_lift DOUBLE")
    purchases = sessionized.where(
        (F.col("event_type") == F.lit("purchase")) & F.col("product_id").isNotNull() & F.col("session_id").isNotNull()
    ).select("session_id", "product_id")
    if purchases.limit(1).count() == 0:
        return empty

    products_by_session = purchases.groupBy("session_id").agg(F.collect_set("product_id").alias("products")).where(
        F.size(F.col("products")) >= 2
    )
    if products_by_session.limit(1).count() == 0:
        return empty

    exploded_a = products_by_session.select("session_id", F.explode(F.col("products")).alias("product_a"))
    exploded_b = products_by_session.select(F.col("session_id").alias("session_id_b"), F.explode(F.col("products")).alias("product_b"))
    pairs = (
        exploded_a.join(exploded_b, exploded_a.session_id == exploded_b.session_id_b, "inner")
        .where(F.col("product_a") < F.col("product_b"))
        .groupBy("product_a", "product_b")
        .agg(F.count(F.lit(1)).alias("co_purchase_sessions"))
    )
    product_sessions = purchases.groupBy("product_id").agg(F.countDistinct("session_id").alias("session_count"))
    total_sessions = int(purchases.agg(F.countDistinct("session_id").alias("session_count")).collect()[0]["session_count"])
    if total_sessions <= 0:
        return empty

    return (
        pairs.join(product_sessions.alias("left_counts"), F.col("product_a") == F.col("left_counts.product_id"), "inner")
        .join(product_sessions.alias("right_counts"), F.col("product_b") == F.col("right_counts.product_id"), "inner")
        .select(
            F.col("product_a"),
            F.col("product_b"),
            F.col("co_purchase_sessions"),
            F.col("left_counts.session_count").alias("session_count_a"),
            F.col("right_counts.session_count").alias("session_count_b"),
        )
        .withColumn(
            "affinity_lift",
            safe_ratio(
                F.col("co_purchase_sessions") * F.lit(float(total_sessions)),
                F.col("session_count_a") * F.col("session_count_b"),
            ),
        )
        .select("product_a", "product_b", "co_purchase_sessions", "affinity_lift")
        .orderBy(F.col("co_purchase_sessions").desc(), F.col("product_a").asc(), F.col("product_b").asc())
        .limit(top_pairs)
    )


def build_time_to_conversion_distribution(sessionized: DataFrame) -> DataFrame:
    """Build a histogram of seconds from first view to first purchase within each session."""

    spark = sessionized.sparkSession
    empty = spark.createDataFrame([], "event_date DATE, time_bucket STRING, conversions BIGINT")
    session_times = sessionized.groupBy("event_date", "session_id").agg(
        F.min(F.when(F.col("event_type") == F.lit("view"), F.col("event_time"))).alias("first_view_at"),
        F.min(F.when(F.col("event_type") == F.lit("purchase"), F.col("event_time"))).alias("first_purchase_at"),
    )
    session_times = session_times.where(
        F.col("first_view_at").isNotNull()
        & F.col("first_purchase_at").isNotNull()
        & (F.col("first_purchase_at") >= F.col("first_view_at"))
    )
    if session_times.limit(1).count() == 0:
        return empty

    bucket = (
        F.when(F.col("seconds_to_conversion") <= 60, F.lit("0-60s"))
        .when(F.col("seconds_to_conversion") <= 300, F.lit("61-300s"))
        .when(F.col("seconds_to_conversion") <= 1800, F.lit("301-1800s"))
        .when(F.col("seconds_to_conversion") <= 86400, F.lit("1801-86400s"))
        .otherwise(F.lit(">86400s"))
    )
    return (
        session_times.withColumn(
            "seconds_to_conversion",
            F.unix_timestamp("first_purchase_at") - F.unix_timestamp("first_view_at"),
        )
        .withColumn("time_bucket", bucket)
        .groupBy("event_date", "time_bucket")
        .agg(F.count(F.lit(1)).alias("conversions"))
        .orderBy("event_date", "time_bucket")
    )


def build_rfm_segmentation(full_silver: DataFrame, *, lookback_days: int = 90) -> DataFrame:
    """Build trailing-window RFM scores and coarse user segments from purchase activity."""

    spark = full_silver.sparkSession
    empty = spark.createDataFrame(
        [],
        (
            "as_of_date DATE, user_id BIGINT, recency_days INT, frequency_90d BIGINT, monetary_90d DOUBLE, "
            "r_score INT, f_score INT, m_score INT, rfm_segment STRING"
        ),
    )
    purchases_all = full_silver.where((F.col("event_type") == F.lit("purchase")) & F.col("user_id").isNotNull())
    if purchases_all.limit(1).count() == 0:
        return empty

    as_of_date = purchases_all.agg(F.max("event_date").alias("as_of_date")).collect()[0]["as_of_date"]
    if as_of_date is None:
        return empty

    window_start = F.date_sub(F.lit(as_of_date), lookback_days - 1)
    purchases = purchases_all.where((F.col("event_date") >= window_start) & (F.col("event_date") <= F.lit(as_of_date)))
    if purchases.limit(1).count() == 0:
        return empty

    rfm = (
        purchases.groupBy("user_id")
        .agg(
            F.max("event_date").alias("last_purchase_date"),
            F.count(F.lit(1)).alias("frequency_90d"),
            F.round(F.sum(F.coalesce(F.col("price"), F.lit(0.0))), 2).alias("monetary_90d"),
        )
        .withColumn("as_of_date", F.lit(as_of_date))
        .withColumn("recency_days", F.datediff(F.col("as_of_date"), F.col("last_purchase_date")))
    )
    recency_window = Window.orderBy(F.col("recency_days").asc_nulls_last(), F.col("user_id").asc())
    frequency_window = Window.orderBy(F.col("frequency_90d").desc_nulls_last(), F.col("user_id").asc())
    monetary_window = Window.orderBy(F.col("monetary_90d").desc_nulls_last(), F.col("user_id").asc())
    return (
        rfm.withColumn("r_tile", F.ntile(5).over(recency_window))
        .withColumn("f_tile", F.ntile(5).over(frequency_window))
        .withColumn("m_tile", F.ntile(5).over(monetary_window))
        .withColumn("r_score", (F.lit(6) - F.col("r_tile")).cast("int"))
        .withColumn("f_score", (F.lit(6) - F.col("f_tile")).cast("int"))
        .withColumn("m_score", (F.lit(6) - F.col("m_tile")).cast("int"))
        .withColumn(
            "rfm_segment",
            F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), F.lit("champions"))
            .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), F.lit("new_customers"))
            .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), F.lit("at_risk"))
            .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), F.lit("hibernating"))
            .when(F.col("f_score") >= 4, F.lit("loyal_customers"))
            .otherwise(F.lit("potential"))
        )
        .select(
            "as_of_date",
            "user_id",
            "recency_days",
            "frequency_90d",
            "monetary_90d",
            "r_score",
            "f_score",
            "m_score",
            "rfm_segment",
        )
        .orderBy("user_id")
    )


def delete_affected_partitions(spark: SparkSession, table_name: str, affected_dates: list[str]) -> None:
    """Delete Gold partitions for the affected dates before appending recomputed rows."""

    if not affected_dates:
        return
    date_literals = ", ".join(f"DATE '{value}'" for value in affected_dates)
    spark.sql(f"DELETE FROM {table_name} WHERE event_date IN ({date_literals})")


def refresh_gold_tables(
    spark: SparkSession,
    affected_dates: list[str],
    *,
    mode: str = GOLD_REFRESH_MODE_FULL,
) -> dict[str, object]:
    """Refresh Gold tables using the requested scope."""

    if mode not in GOLD_REFRESH_MODES:
        raise ValueError(f"Unsupported gold refresh mode: {mode}")

    if mode == GOLD_REFRESH_MODE_NONE:
        return {
            "row_counts": {table_name: 0 for table_name in ALL_GOLD_TABLES},
            "dq_summaries": {},
            "tables_written": 0,
            "refresh_mode": mode,
            "skipped_reason": "configured_to_skip_gold_refresh",
            "skipped_tables": list(ALL_GOLD_TABLES),
        }

    if not affected_dates:
        return {
            "row_counts": {table_name: 0 for table_name in ALL_GOLD_TABLES},
            "dq_summaries": {},
            "tables_written": 0,
            "refresh_mode": mode,
            "skipped_reason": "no_affected_dates",
            "skipped_tables": [],
        }

    silver_slice = spark.table(SILVER_TABLE).where(F.col("event_date").cast("string").isin(affected_dates))
    sessionized = build_sessionized_events(silver_slice)

    incremental_gold_tables = {
        GOLD_DAILY_REVENUE: build_daily_revenue(silver_slice),
        GOLD_TOP_PRODUCTS: build_top_products(silver_slice),
        GOLD_CONVERSION_FUNNEL: build_conversion_funnel_daily(silver_slice),
        GOLD_CATEGORY_PERFORMANCE: build_category_performance_daily(silver_slice),
        GOLD_SESSION_FUNNEL: build_session_funnel(sessionized),
        GOLD_USER_CONVERSION_PATH: build_user_conversion_path(sessionized),
    }
    advanced_gold_tables: dict[str, DataFrame] = {}
    skipped_tables: list[str] = []
    skipped_reason = ""

    if mode == GOLD_REFRESH_MODE_FULL:
        full_silver = spark.table(SILVER_TABLE)
        sessionized_full = build_sessionized_events(full_silver)
        advanced_gold_tables = {
            GOLD_COHORT_RETENTION: build_cohort_retention(full_silver),
            GOLD_REPEAT_PURCHASE: build_repeat_purchase(full_silver),
            GOLD_PRODUCT_AFFINITY: build_product_affinity(sessionized_full),
            GOLD_TIME_TO_CONVERSION_DISTRIBUTION: build_time_to_conversion_distribution(sessionized_full),
            GOLD_RFM_SEGMENTATION: build_rfm_segmentation(full_silver),
        }
    else:
        skipped_tables = list(ADVANCED_GOLD_TABLES)
        skipped_reason = "skipped_history_wide_gold_tables_in_affected_dates_mode"

    row_counts: dict[str, int] = {}
    dq_summaries: dict[str, dict[str, object]] = {}
    incremental_gold_dq_specs: dict[str, tuple[str, tuple[str, ...]]] = {
        GOLD_DAILY_REVENUE: ("daily_revenue", ("purchase_count", "purchase_revenue", "unique_buyers")),
        GOLD_TOP_PRODUCTS: ("top_products", ("views", "carts", "purchases", "purchase_revenue", "unique_users")),
        GOLD_CONVERSION_FUNNEL: ("conversion_funnel", ("views", "carts", "purchases")),
        GOLD_CATEGORY_PERFORMANCE: (
            "category_performance",
            ("views", "carts", "purchases", "purchase_revenue", "unique_buyers"),
        ),
        GOLD_SESSION_FUNNEL: (
            "session_funnel",
            ("total_events", "distinct_products", "views", "carts", "purchases", "purchase_revenue"),
        ),
        GOLD_USER_CONVERSION_PATH: (
            "user_conversion_path",
            ("session_count", "views", "carts", "purchases", "purchase_revenue"),
        ),
    }
    advanced_gold_dq_specs: dict[str, tuple[str, tuple[str, ...], tuple[str, ...]]] = {
        GOLD_COHORT_RETENTION: ("cohort_retention", ("cohort_users", "active_users"), ("cohort_month", "period_offset")),
        GOLD_REPEAT_PURCHASE: (
            "repeat_purchase",
            ("purchasers", "repeat_purchasers"),
            ("activity_month",),
        ),
        GOLD_PRODUCT_AFFINITY: (
            "product_affinity",
            ("co_purchase_sessions",),
            ("product_a", "product_b"),
        ),
        GOLD_TIME_TO_CONVERSION_DISTRIBUTION: (
            "time_to_conversion_distribution",
            ("conversions",),
            ("event_date", "time_bucket"),
        ),
        GOLD_RFM_SEGMENTATION: (
            "rfm_segmentation",
            ("recency_days", "frequency_90d", "monetary_90d", "r_score", "f_score", "m_score"),
            ("as_of_date", "user_id"),
        ),
    }
    for table_name, dataframe in incremental_gold_tables.items():
        row_count = dataframe.count()
        spec = incremental_gold_dq_specs.get(table_name)
        if spec:
            short_name, cols = spec[0], spec[1]
            dq_summaries[short_name] = summarize_gold_dataframe_quality(
                dataframe,
                name=short_name,
                columns=cols,
            )
        delete_affected_partitions(spark, table_name, affected_dates)
        if row_count > 0:
            append_to_iceberg(table_name, dataframe, partitions=max(len(affected_dates), 1))
        row_counts[table_name] = row_count

    # These tables depend on global purchase/session history, so only refresh them in full mode.
    for table_name, dataframe in advanced_gold_tables.items():
        row_count = dataframe.count()
        spec = advanced_gold_dq_specs.get(table_name)
        if spec:
            short_name, cols, required_cols = spec
            dq_summaries[short_name] = summarize_gold_dataframe_quality(
                dataframe,
                name=short_name,
                columns=cols,
                required_columns=required_cols,
            )
        delete_all_rows(spark, table_name)
        if row_count > 0:
            if table_name == GOLD_TIME_TO_CONVERSION_DISTRIBUTION:
                partition_count = max(dataframe.select("event_date").distinct().count(), 1)
                append_to_iceberg(table_name, dataframe, partitions=partition_count)
            else:
                append_to_iceberg(table_name, dataframe, partitions=8)
        row_counts[table_name] = row_count

    for table_name in skipped_tables:
        row_counts[table_name] = 0

    return {
        "row_counts": row_counts,
        "dq_summaries": dq_summaries,
        "tables_written": sum(1 for count in row_counts.values() if count > 0),
        "refresh_mode": mode,
        "skipped_reason": skipped_reason,
        "skipped_tables": skipped_tables,
    }


def process_bronze_batch(
    spark: SparkSession,
    bronze_batch: DataFrame,
    *,
    append_bronze: bool,
    gold_refresh_mode: str = GOLD_REFRESH_MODE_FULL,
    bronze_partitions: int = 8,
    silver_partitions: int = 8,
    run_id: str | None = None,
    input_descriptions: list[str] | None = None,
) -> dict[str, object]:
    """Process a Bronze DataFrame through Silver and Gold using the shared Phase 1 logic."""

    bronze_inputs = list(input_descriptions or [])
    bronze_rows_seen = bronze_batch.count()

    bronze_started_at = utc_now_iso()
    bronze_timer = stage_timer()
    try:
        if append_bronze:
            new_bronze_rows = insert_new_bronze_rows(spark, bronze_batch)
            bronze_rows_written = new_bronze_rows.count()
            if bronze_rows_written > 0:
                append_to_iceberg(BRONZE_TABLE, new_bronze_rows, partitions=bronze_partitions)
        else:
            bronze_rows_written = 0

        bronze_metrics = {
            "rows_seen": bronze_rows_seen,
            "rows_written": bronze_rows_written,
            "rows_skipped_existing": max(bronze_rows_seen - bronze_rows_written, 0),
            "duration_seconds": stage_elapsed_seconds(bronze_timer),
        }
        if run_id is not None:
            record_stage_event(
                "bronze_ingest_iceberg",
                run_id,
                status="succeeded",
                metrics=bronze_metrics,
                details={"append_enabled": append_bronze},
                inputs=bronze_inputs,
                outputs=[BRONZE_TABLE],
                started_at=bronze_started_at,
            )
    except Exception as exc:
        bronze_metrics = {
            "rows_seen": bronze_rows_seen,
            "duration_seconds": stage_elapsed_seconds(bronze_timer),
        }
        if run_id is not None:
            record_stage_event(
                "bronze_ingest_iceberg",
                run_id,
                status="failed",
                metrics=bronze_metrics,
                details={"append_enabled": append_bronze, "failure": str(exc)},
                inputs=bronze_inputs,
                outputs=[BRONZE_TABLE],
                started_at=bronze_started_at,
            )
        raise

    silver_started_at = utc_now_iso()
    silver_timer = stage_timer()
    try:
        silver_base_rows = build_silver_base_rows(bronze_batch)
        silver_candidates = deduplicate_silver_candidates(silver_base_rows)
        inserted_silver_rows = insert_new_silver_rows(spark, silver_candidates)
        silver_candidates_count = silver_candidates.count()
        silver_rows_written = inserted_silver_rows.count()
        silver_dq_summary = summarize_silver_dataframe_quality(
            silver_base_rows,
            inserted_silver_rows,
        )
        affected_dates = sorted(
            str(row["event_date"]) for row in inserted_silver_rows.select("event_date").distinct().collect()
        )
        if affected_dates:
            append_to_iceberg(SILVER_TABLE, inserted_silver_rows, partitions=silver_partitions)
        silver_metrics = {
            "rows_seen": bronze_rows_seen,
            "rows_valid_after_cleaning": silver_candidates_count,
            "rows_written": silver_rows_written,
            "rows_skipped_existing": max(silver_candidates_count - silver_rows_written, 0),
            "affected_dates": len(affected_dates),
            "duration_seconds": stage_elapsed_seconds(silver_timer),
            **silver_dq_summary,
        }
        if run_id is not None:
            record_stage_event(
                "silver_publish_iceberg",
                run_id,
                status="succeeded",
                metrics=silver_metrics,
                details={
                    "affected_dates": affected_dates,
                    "bad_record_samples": silver_dq_summary.get("bad_record_samples", {}),
                },
                inputs=[BRONZE_TABLE],
                outputs=[SILVER_TABLE],
                started_at=silver_started_at,
            )
    except Exception as exc:
        if run_id is not None:
            record_stage_event(
                "silver_publish_iceberg",
                run_id,
                status="failed",
                metrics={"rows_seen": bronze_rows_seen, "duration_seconds": stage_elapsed_seconds(silver_timer)},
                details={"failure": str(exc)},
                inputs=[BRONZE_TABLE],
                outputs=[SILVER_TABLE],
                started_at=silver_started_at,
            )
        raise

    gold_started_at = utc_now_iso()
    gold_timer = stage_timer()
    try:
        gold_result = refresh_gold_tables(spark, affected_dates, mode=gold_refresh_mode)
        gold_row_counts = gold_result["row_counts"]
        gold_dq_summaries = gold_result["dq_summaries"]
        gold_metrics: dict[str, object] = {
            "gold_refresh_mode": gold_refresh_mode,
            "affected_dates": len(affected_dates),
            "tables_written": gold_result["tables_written"],
            "rows_written": sum(gold_row_counts.values()),
            "duration_seconds": stage_elapsed_seconds(gold_timer),
            "gold_refresh_skipped": bool(gold_result.get("skipped_reason")),
        }
        if gold_result.get("skipped_reason"):
            gold_metrics["gold_skip_reason"] = gold_result["skipped_reason"]
            gold_metrics["gold_tables_skipped"] = len(gold_result.get("skipped_tables", []))
        gold_metrics["dq_passed"] = all(
            bool(summary.get("dq_passed", True)) for summary in gold_dq_summaries.values()
        )
        for table_name, summary in gold_dq_summaries.items():
            gold_metrics[f"{table_name}_rows_to_publish"] = summary.get(
                f"gold_{table_name}_rows_to_publish",
                0,
            )
            gold_metrics[f"{table_name}_negative_metric_rows"] = summary.get(
                f"gold_{table_name}_negative_metric_rows",
                0,
            )
        if run_id is not None:
            record_stage_event(
                "gold_refresh_iceberg",
                run_id,
                status="succeeded",
                metrics=gold_metrics,
                details={
                    "affected_dates": affected_dates,
                    "dq_tables": gold_dq_summaries,
                    "gold_refresh_mode": gold_refresh_mode,
                    "skipped_reason": gold_result.get("skipped_reason", ""),
                    "skipped_tables": gold_result.get("skipped_tables", []),
                },
                inputs=[SILVER_TABLE],
                outputs=list(gold_row_counts.keys()),
                started_at=gold_started_at,
            )
    except Exception as exc:
        if run_id is not None:
            record_stage_event(
                "gold_refresh_iceberg",
                run_id,
                status="failed",
                metrics={"affected_dates": len(affected_dates), "duration_seconds": stage_elapsed_seconds(gold_timer)},
                details={"failure": str(exc), "affected_dates": affected_dates},
                inputs=[SILVER_TABLE],
                outputs=[
                    GOLD_DAILY_REVENUE,
                    GOLD_TOP_PRODUCTS,
                    GOLD_CONVERSION_FUNNEL,
                    GOLD_CATEGORY_PERFORMANCE,
                    GOLD_SESSION_FUNNEL,
                    GOLD_USER_CONVERSION_PATH,
                    GOLD_COHORT_RETENTION,
                    GOLD_REPEAT_PURCHASE,
                    GOLD_PRODUCT_AFFINITY,
                    GOLD_TIME_TO_CONVERSION_DISTRIBUTION,
                    GOLD_RFM_SEGMENTATION,
                ],
                started_at=gold_started_at,
            )
        raise
    return {
        "affected_dates": affected_dates,
        "gold_row_counts": gold_row_counts,
        "gold_dq_summaries": gold_dq_summaries,
        "silver_dq_summary": silver_dq_summary,
        "bronze_metrics": bronze_metrics,
        "silver_metrics": silver_metrics,
        "gold_metrics": gold_metrics,
    }


def main() -> None:
    """Run the full historical Bronze, Silver, and Gold backfill pipeline."""

    args = parse_args()
    spark = build_spark_session()
    batch_run_id = f"batch-{args.start_month.replace('-', '')}-{args.end_month.replace('-', '')}"
    overall_started_at = utc_now_iso()
    overall_timer = stage_timer()

    try:
        drop_tables_if_requested(spark, args.reset_tables)
        create_tables_if_needed(spark)

        input_dir = Path(args.input_dir)
        if args.input_file:
            input_files = [Path(args.input_file)]
            missing_months: list[str] = []
        else:
            input_files, missing_months = discover_input_files(input_dir, args.start_month, args.end_month)

        if args.resume_from == "full" and not input_files:
            raise SystemExit(f"No historical files found in {input_dir} for range {args.start_month}..{args.end_month}")

        if args.resume_from == "silver":
            bronze_batch = read_bronze_slice(spark, args.start_month, args.end_month)
        else:
            bronze_batch = read_bronze_batch(spark, input_files, batch_run_id, source_month_override=args.source_month)

        process_result = process_bronze_batch(
            spark,
            bronze_batch,
            append_bronze=args.resume_from != "silver",
            bronze_partitions=8,
            silver_partitions=8,
            run_id=batch_run_id,
            input_descriptions=[str(path) for path in input_files],
        )
        affected_dates = process_result["affected_dates"]
        gold_row_counts = process_result["gold_row_counts"]

        record_stage_event(
            "batch_backfill_iceberg",
            batch_run_id,
            status="succeeded",
            metrics={
                "duration_seconds": stage_elapsed_seconds(overall_timer),
                "affected_dates": len(affected_dates),
                "gold_tables_refreshed": len(gold_row_counts),
                "bronze_rows_written": process_result["bronze_metrics"]["rows_written"],
                "silver_rows_written": process_result["silver_metrics"]["rows_written"],
                "gold_rows_written": process_result["gold_metrics"]["rows_written"],
                "gold_refresh_mode": process_result["gold_metrics"]["gold_refresh_mode"],
            },
            details={
                "resume_from": args.resume_from,
                "missing_months": missing_months,
                "affected_dates": affected_dates,
            },
            inputs=[str(path) for path in input_files],
            outputs=[
                BRONZE_TABLE,
                SILVER_TABLE,
                GOLD_DAILY_REVENUE,
                GOLD_TOP_PRODUCTS,
                GOLD_CONVERSION_FUNNEL,
                GOLD_CATEGORY_PERFORMANCE,
                GOLD_SESSION_FUNNEL,
                GOLD_USER_CONVERSION_PATH,
                GOLD_COHORT_RETENTION,
                GOLD_REPEAT_PURCHASE,
                GOLD_PRODUCT_AFFINITY,
                GOLD_TIME_TO_CONVERSION_DISTRIBUTION,
                GOLD_RFM_SEGMENTATION,
            ],
            started_at=overall_started_at,
        )

        print("Historical batch backfill complete.")
        print(f"Resume mode: {args.resume_from}")
        print(f"Input files: {[path.name for path in input_files]}")
        if missing_months:
            print(f"Missing months in requested range: {missing_months}")
        print(f"Affected Gold dates: {affected_dates}")
        print("Gold tables refreshed:")
        for table_name, row_count in sorted(gold_row_counts.items()):
            print(f" - {table_name}: {row_count} rows")
    except Exception as exc:
        record_stage_event(
            "batch_backfill_iceberg",
            batch_run_id,
            status="failed",
            metrics={"duration_seconds": stage_elapsed_seconds(overall_timer)},
            details={"resume_from": args.resume_from, "failure": str(exc)},
            started_at=overall_started_at,
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
