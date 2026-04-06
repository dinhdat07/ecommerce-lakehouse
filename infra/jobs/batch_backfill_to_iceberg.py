"""Historical batch backfill from raw CSV files into Iceberg Bronze, Silver, and Gold tables.

This Spark job is the Phase 1 batch-first implementation for the Docker stack.
It reads monthly historical CSV or CSV.GZ files, appends raw rows to Bronze,
incrementally inserts cleaned canonical rows into Silver, and recomputes only
the affected Gold date partitions as physical Iceberg tables.
"""

from __future__ import annotations

import argparse
import calendar
import re
from datetime import date
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

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
HISTORICAL_FILE_RE = re.compile(r"^(?P<year>\d{4})-(?P<month>[A-Za-z]{3})\.csv(?:\.gz)?$")
MONTH_NAME_TO_NUMBER = {month[:3]: f"{index:02d}" for index, month in enumerate(calendar.month_name) if month}
TABLE_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "uncompressed",
}


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


def build_spark_session() -> SparkSession:
    """Create the Spark session used for the batch backfill."""

    spark = SparkSession.builder.appName("batch-backfill-to-iceberg").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


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


def build_silver_candidates(bronze_batch: DataFrame) -> DataFrame:
    """Normalize Bronze rows into dedupable Silver candidates."""

    parsed_event_time = parse_event_timestamp("event_time")
    normalized_event_type = normalize_nullable_text("event_type")
    canonical_event_time = F.date_format(parsed_event_time, "yyyy-MM-dd'T'HH:mm:ss'Z'")

    candidates = (
        bronze_batch.select(
            F.col("record_hash").alias("event_id"),
            F.col("batch_run_id"),
            F.col("ingested_at"),
            F.col("source_file"),
            F.col("source_month"),
            F.col("record_hash"),
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
        .where(F.col("event_time").isNotNull() & F.col("event_type").isNotNull())
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
    )


def insert_new_silver_rows(spark: SparkSession, silver_candidates: DataFrame) -> DataFrame:
    """Return only previously unseen Silver rows."""

    existing_keys = spark.table(SILVER_TABLE).select("dedupe_key")
    return silver_candidates.join(existing_keys, on="dedupe_key", how="left_anti")


def insert_new_bronze_rows(spark: SparkSession, bronze_batch: DataFrame) -> DataFrame:
    """Return only previously unseen Bronze rows for rerun-safe raw ingestion.

    Bronze deduplication keys off `record_hash`, which includes the source file
    name and the raw column payload. Silver still derives from the full source
    batch so reruns can recover if a prior attempt committed Bronze but failed
    before Silver or Gold completed.
    """

    existing_hashes = spark.table(BRONZE_TABLE).select("record_hash")
    return bronze_batch.join(existing_hashes, on="record_hash", how="left_anti")


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


def delete_affected_partitions(spark: SparkSession, table_name: str, affected_dates: list[str]) -> None:
    """Delete Gold partitions for the affected dates before appending recomputed rows."""

    if not affected_dates:
        return
    date_literals = ", ".join(f"DATE '{value}'" for value in affected_dates)
    spark.sql(f"DELETE FROM {table_name} WHERE event_date IN ({date_literals})")


def refresh_gold_tables(spark: SparkSession, affected_dates: list[str]) -> dict[str, int]:
    """Recompute Gold tables only for dates touched by newly inserted Silver rows."""

    if not affected_dates:
        return {}

    silver_slice = spark.table(SILVER_TABLE).where(F.col("event_date").cast("string").isin(affected_dates))
    sessionized = build_sessionized_events(silver_slice)

    gold_tables = {
        GOLD_DAILY_REVENUE: build_daily_revenue(silver_slice),
        GOLD_TOP_PRODUCTS: build_top_products(silver_slice),
        GOLD_CONVERSION_FUNNEL: build_conversion_funnel_daily(silver_slice),
        GOLD_CATEGORY_PERFORMANCE: build_category_performance_daily(silver_slice),
        GOLD_SESSION_FUNNEL: build_session_funnel(sessionized),
        GOLD_USER_CONVERSION_PATH: build_user_conversion_path(sessionized),
    }

    row_counts: dict[str, int] = {}
    for table_name, dataframe in gold_tables.items():
        delete_affected_partitions(spark, table_name, affected_dates)
        append_to_iceberg(table_name, dataframe, partitions=max(len(affected_dates), 1))
        row_counts[table_name] = -1
    return row_counts


def process_bronze_batch(
    spark: SparkSession,
    bronze_batch: DataFrame,
    *,
    append_bronze: bool,
    bronze_partitions: int = 8,
    silver_partitions: int = 8,
) -> dict[str, object]:
    """Process a Bronze DataFrame through Silver and Gold using the shared Phase 1 logic."""

    if append_bronze:
        new_bronze_rows = insert_new_bronze_rows(spark, bronze_batch)
        append_to_iceberg(BRONZE_TABLE, new_bronze_rows, partitions=bronze_partitions)

    silver_candidates = build_silver_candidates(bronze_batch)
    inserted_silver_rows = insert_new_silver_rows(spark, silver_candidates)
    affected_dates = sorted(str(row["event_date"]) for row in inserted_silver_rows.select("event_date").distinct().collect())
    if affected_dates:
        append_to_iceberg(SILVER_TABLE, inserted_silver_rows, partitions=silver_partitions)
    gold_row_counts = refresh_gold_tables(spark, affected_dates)
    return {
        "affected_dates": affected_dates,
        "gold_row_counts": gold_row_counts,
    }


def main() -> None:
    """Run the full historical Bronze, Silver, and Gold backfill pipeline."""

    args = parse_args()
    spark = build_spark_session()
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

    batch_run_id = f"batch-{args.start_month.replace('-', '')}-{args.end_month.replace('-', '')}"
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
    )
    affected_dates = process_result["affected_dates"]
    gold_row_counts = process_result["gold_row_counts"]

    print("Historical batch backfill complete.")
    print(f"Resume mode: {args.resume_from}")
    print(f"Input files: {[path.name for path in input_files]}")
    if missing_months:
        print(f"Missing months in requested range: {missing_months}")
    print(f"Affected Gold dates: {affected_dates}")
    print("Gold tables refreshed:")
    for table_name, row_count in sorted(gold_row_counts.items()):
        if row_count >= 0:
            print(f" - {table_name}: {row_count} rows")
        else:
            print(f" - {table_name}")
    spark.stop()


if __name__ == "__main__":
    main()
