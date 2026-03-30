"""Load the checked-in sample CSV into the Bronze Iceberg table.

This job is intended for the laptop Docker Compose stack. It runs on the Spark
cluster, stores table metadata in the Iceberg JDBC catalog backed by Postgres,
and writes table data to MinIO. The downstream Silver and Gold demo layers are
prepared by Trino views in ``infra/trino/sql/prepare_demo_views.sql``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


def build_spark_session() -> SparkSession:
    """Create a Spark session that targets the shared Iceberg catalog."""

    return SparkSession.builder.appName("load-sample-to-iceberg").getOrCreate()


def write_iceberg_table(df: DataFrame, table_name: str) -> None:
    """Write a DataFrame to Iceberg using a laptop-safe Parquet codec."""

    (
        df.writeTo(table_name)
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "uncompressed")
        .createOrReplace()
    )


def main() -> None:
    """Read the sample CSV and materialize the Bronze Iceberg table."""

    spark = build_spark_session()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.demo")

    raw = spark.read.option("header", True).csv("/workspace/data/sample/events_sample_100k.csv")

    bronze = (
        raw.withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.lit("events_sample_100k.csv"))
        .withColumn("record_hash", F.sha2(F.concat_ws("|", *raw.columns), 256))
    )
    write_iceberg_table(bronze, "lakehouse.demo.bronze_events")

    print("Loaded Iceberg table:")
    print(" - lakehouse.demo.bronze_events")
    print("Next step: apply infra/trino/sql/prepare_demo_views.sql in Trino to expose Silver and Gold demo views.")

    spark.stop()


if __name__ == "__main__":
    main()
