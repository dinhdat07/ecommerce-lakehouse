"""Kafka to Iceberg Structured Streaming job for the Phase 2 demo path.

This job reads bounded demo replay events from Kafka, appends physical Bronze
rows to Iceberg, and incrementally refreshes Silver and Gold inside
`foreachBatch` using the same Spark transformations as the Phase 1 batch path.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

JOBS_DIR = Path(__file__).resolve().parent
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))

from batch_backfill_to_iceberg import build_streaming_bronze_batch, create_tables_if_needed, process_bronze_batch

REPLAY_MESSAGE_SCHEMA = StructType(
    [
        StructField("replayed_at", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("source_month", StringType(), True),
        StructField(
            "payload",
            StructType(
                [
                    StructField("event_time", StringType(), True),
                    StructField("event_type", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("category_id", StringType(), True),
                    StructField("category_code", StringType(), True),
                    StructField("brand", StringType(), True),
                    StructField("price", StringType(), True),
                    StructField("user_id", StringType(), True),
                    StructField("user_session", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the bounded streaming demo job."""

    parser = argparse.ArgumentParser(description="Read Kafka replay events and refresh Bronze, Silver, and Gold.")
    parser.add_argument("--bootstrap-servers", default="kafka:29092", help="Kafka bootstrap servers reachable from Spark.")
    parser.add_argument("--topic", default="ecom.events", help="Kafka topic to consume.")
    parser.add_argument(
        "--checkpoint-location",
        default="/opt/spark/work-dir/checkpoints/phase2/kafka_to_iceberg",
        help="Streaming checkpoint directory.",
    )
    parser.add_argument("--starting-offsets", default="earliest", help="Kafka startingOffsets setting.")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=2000, help="Max Kafka records per micro-batch.")
    parser.add_argument("--trigger-seconds", type=int, default=5, help="Processing time trigger in seconds.")
    parser.add_argument("--timeout-seconds", type=int, default=120, help="Maximum runtime before the query stops.")
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    """Create the Spark session used for the streaming demo."""

    spark = SparkSession.builder.appName("kafka-stream-to-iceberg").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def parse_replay_messages(stream_df: DataFrame) -> DataFrame:
    """Parse Kafka JSON messages into typed replay payload rows."""

    parsed = stream_df.select(
        F.from_json(F.col("value").cast("string"), REPLAY_MESSAGE_SCHEMA).alias("message"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    return parsed.select(
        F.col("message.source_file").alias("source_file"),
        F.col("message.source_month").alias("source_month"),
        F.col("message.replayed_at").alias("replayed_at"),
        F.col("kafka_timestamp"),
        F.col("message.payload.event_time").alias("event_time"),
        F.col("message.payload.event_type").alias("event_type"),
        F.col("message.payload.product_id").alias("product_id"),
        F.col("message.payload.category_id").alias("category_id"),
        F.col("message.payload.category_code").alias("category_code"),
        F.col("message.payload.brand").alias("brand"),
        F.col("message.payload.price").alias("price"),
        F.col("message.payload.user_id").alias("user_id"),
        F.col("message.payload.user_session").alias("user_session"),
    )


def process_microbatch(batch_df: DataFrame, batch_id: int) -> None:
    """Append the current Kafka micro-batch to Bronze and refresh Silver and Gold."""

    if batch_df.limit(1).count() == 0:
        print(f"Skipping empty streaming batch {batch_id}")
        return

    spark = batch_df.sparkSession
    bronze_batch = build_streaming_bronze_batch(batch_df, batch_run_id=f"stream-batch-{batch_id:06d}")
    result = process_bronze_batch(
        spark,
        bronze_batch,
        append_bronze=True,
        bronze_partitions=4,
        silver_partitions=4,
    )
    print(
        f"Processed streaming batch {batch_id}: affected_dates={result['affected_dates']} "
        f"gold_tables={sorted(result['gold_row_counts'])}"
    )


def main() -> None:
    """Run the bounded Structured Streaming demo job."""

    args = parse_args()
    spark = build_spark_session()
    create_tables_if_needed(spark)

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = parse_replay_messages(kafka_stream)
    query = (
        parsed_stream.writeStream.foreachBatch(process_microbatch)
        .option("checkpointLocation", args.checkpoint_location)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )
    query.awaitTermination(args.timeout_seconds)
    if query.isActive:
        query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
