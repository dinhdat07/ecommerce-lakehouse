"""Kafka to Iceberg Structured Streaming job for demo and server modes.

This job reads Kafka replay events, appends physical Bronze rows to Iceberg,
and incrementally refreshes Silver and Gold inside ``foreachBatch`` using the
same Spark transformations as the Phase 1 batch path.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

JOBS_DIR = Path(__file__).resolve().parent
if str(JOBS_DIR) not in sys.path:
    sys.path.insert(0, str(JOBS_DIR))
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from batch_backfill_to_iceberg import (  # noqa: E402
    ALL_GOLD_TABLES,
    GOLD_REFRESH_MODE_AFFECTED_DATES,
    GOLD_REFRESH_MODE_FULL,
    GOLD_REFRESH_MODE_NONE,
    INCREMENTAL_GOLD_TABLES,
    build_streaming_bronze_batch,
    create_tables_if_needed,
    process_bronze_batch,
)
from common.constants import (  # noqa: E402
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_EVENTS,
    SPARK_APP_NAME_PREFIX,
    SPARK_SQL_SHUFFLE_PARTITIONS,
    STREAM_CHECKPOINT_LOCATION,
    STREAM_FAIL_ON_DATA_LOSS,
    STREAM_GOLD_REFRESH_MODE,
    STREAM_MAX_OFFSETS_PER_TRIGGER,
    STREAM_MODE,
    STREAM_PROGRESS_LOG_PATH,
    STREAM_PROGRESS_POLL_SECONDS,
    STREAM_QUERY_NAME,
    STREAM_STARTING_OFFSETS,
    STREAM_STOP_AFTER_SECONDS,
    STREAM_TIMEOUT_SECONDS,
    STREAM_TRIGGER_SECONDS,
)
from common.observability import record_stage_event, stage_elapsed_seconds, stage_timer  # noqa: E402
from common.runtime import utc_now_iso  # noqa: E402

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

STREAM_BATCH_RESULTS: list[dict[str, object]] = []
STREAM_JOB_ARGS: argparse.Namespace | None = None


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for demo and server streaming execution."""

    parser = argparse.ArgumentParser(description="Read Kafka events and refresh Bronze, Silver, and Gold.")
    parser.add_argument("--mode", choices=["demo", "server"], default=STREAM_MODE, help="Streaming operating mode.")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers reachable from Spark.")
    parser.add_argument("--topic", default=KAFKA_TOPIC_EVENTS, help="Kafka topic to consume.")
    parser.add_argument("--query-name", default=STREAM_QUERY_NAME, help="Stable structured-streaming query name.")
    parser.add_argument(
        "--checkpoint-location",
        default=STREAM_CHECKPOINT_LOCATION,
        help="Streaming checkpoint directory.",
    )
    parser.add_argument(
        "--progress-log-path",
        default=STREAM_PROGRESS_LOG_PATH,
        help="Optional JSONL file for periodic progress snapshots.",
    )
    parser.add_argument("--starting-offsets", default=STREAM_STARTING_OFFSETS, help="Kafka startingOffsets setting.")
    parser.add_argument(
        "--max-offsets-per-trigger",
        type=int,
        default=STREAM_MAX_OFFSETS_PER_TRIGGER,
        help="Max Kafka records per micro-batch. Set 0 to disable the option.",
    )
    parser.add_argument("--trigger-seconds", type=int, default=STREAM_TRIGGER_SECONDS, help="Processing time trigger in seconds.")
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=STREAM_TIMEOUT_SECONDS,
        help="Max runtime before the query stops. Use 0 for indefinite execution.",
    )
    parser.add_argument(
        "--stop-after-seconds",
        type=int,
        default=STREAM_STOP_AFTER_SECONDS,
        help="Optional graceful stop deadline independent of timeout. Use 0 to disable.",
    )
    parser.add_argument(
        "--progress-poll-seconds",
        type=int,
        default=STREAM_PROGRESS_POLL_SECONDS,
        help="How often to inspect and emit query progress.",
    )
    parser.add_argument(
        "--fail-on-data-loss",
        default="true" if STREAM_FAIL_ON_DATA_LOSS else "false",
        help="Kafka failOnDataLoss setting: true or false.",
    )
    parser.add_argument(
        "--gold-refresh-mode",
        choices=[GOLD_REFRESH_MODE_NONE, GOLD_REFRESH_MODE_AFFECTED_DATES, GOLD_REFRESH_MODE_FULL],
        default=STREAM_GOLD_REFRESH_MODE,
        help="Gold refresh scope for each streaming microbatch.",
    )
    parser.add_argument("--bronze-partitions", type=int, default=4, help="Bronze append repartition count.")
    parser.add_argument("--silver-partitions", type=int, default=4, help="Silver append repartition count.")
    return parser.parse_args()


def build_spark_session(app_name: str | None = None) -> SparkSession:
    """Create the Spark session used for the streaming job."""

    spark = SparkSession.builder.appName(app_name or f"{SPARK_APP_NAME_PREFIX}-streaming").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", str(SPARK_SQL_SHUFFLE_PARTITIONS))
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


def _write_json_line(path_value: str, payload: dict[str, Any]) -> None:
    if not path_value:
        return
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _batch_outputs(gold_refresh_mode: str) -> list[str]:
    base_outputs = [
        "lakehouse.demo.bronze_events",
        "lakehouse.demo.silver_events",
    ]
    if gold_refresh_mode == GOLD_REFRESH_MODE_NONE:
        return base_outputs
    if gold_refresh_mode == GOLD_REFRESH_MODE_AFFECTED_DATES:
        return base_outputs + INCREMENTAL_GOLD_TABLES
    return base_outputs + ALL_GOLD_TABLES


def process_microbatch(batch_df: DataFrame, batch_id: int) -> None:
    """Append the current Kafka micro-batch to Bronze and refresh Silver and Gold."""

    assert STREAM_JOB_ARGS is not None

    run_id = f"{STREAM_JOB_ARGS.query_name}-batch-{batch_id:06d}"
    started_at = utc_now_iso()
    timer = stage_timer()
    batch_count = batch_df.count()
    if batch_count == 0:
        skipped_metrics = {
            "batch_id": batch_id,
            "run_id": run_id,
            "status": "skipped",
            "input_rows": 0,
            "silver_rows_written": 0,
            "gold_refresh_mode": STREAM_JOB_ARGS.gold_refresh_mode,
            "gold_rows_written": 0,
            "affected_dates": [],
            "gold_refresh_skipped": True,
            "gold_skip_reason": "empty_microbatch",
            "duration_seconds": stage_elapsed_seconds(timer),
        }
        STREAM_BATCH_RESULTS.append(skipped_metrics)
        _write_json_line(
            STREAM_JOB_ARGS.progress_log_path,
            {
                "type": "streaming_microbatch_skipped",
                "run_id": run_id,
                "batch_id": batch_id,
                "mode": STREAM_JOB_ARGS.mode,
                "input_rows": 0,
                "silver_rows_written": 0,
                "gold_refresh_mode": STREAM_JOB_ARGS.gold_refresh_mode,
                "gold_rows_written": 0,
                "duration_seconds": skipped_metrics["duration_seconds"],
                "affected_dates": [],
                "gold_refresh_skipped": True,
                "skip_reason": "empty_microbatch",
            },
        )
        return

    spark = batch_df.sparkSession
    bronze_batch = build_streaming_bronze_batch(batch_df, batch_run_id=run_id)
    source_files = [row["source_file"] for row in batch_df.select("source_file").distinct().limit(10).collect()]
    lag_stats = batch_df.select(
        F.avg(
            F.unix_timestamp("kafka_timestamp") - F.unix_timestamp(F.to_timestamp("replayed_at"))
        ).alias("avg_freshness_lag_seconds"),
        F.max(
            F.unix_timestamp("kafka_timestamp") - F.unix_timestamp(F.to_timestamp("replayed_at"))
        ).alias("max_freshness_lag_seconds"),
    ).collect()[0]

    try:
        result = process_bronze_batch(
            spark,
            bronze_batch,
            append_bronze=True,
            gold_refresh_mode=STREAM_JOB_ARGS.gold_refresh_mode,
            bronze_partitions=STREAM_JOB_ARGS.bronze_partitions,
            silver_partitions=STREAM_JOB_ARGS.silver_partitions,
            run_id=run_id,
            input_descriptions=source_files,
        )
        microbatch_metrics = {
            "batch_id": batch_id,
            "input_rows": batch_count,
            "silver_rows_written": result["silver_metrics"]["rows_written"],
            "gold_refresh_mode": STREAM_JOB_ARGS.gold_refresh_mode,
            "gold_rows_written": result["gold_metrics"]["rows_written"],
            "affected_dates": len(result["affected_dates"]),
            "gold_refresh_skipped": bool(result["gold_metrics"].get("gold_refresh_skipped", False)),
            "avg_freshness_lag_seconds": float(lag_stats["avg_freshness_lag_seconds"] or 0.0),
            "max_freshness_lag_seconds": float(lag_stats["max_freshness_lag_seconds"] or 0.0),
            "duration_seconds": stage_elapsed_seconds(timer),
        }
        if result["gold_metrics"].get("gold_skip_reason"):
            microbatch_metrics["gold_skip_reason"] = str(result["gold_metrics"]["gold_skip_reason"])
        record_stage_event(
            "streaming_microbatch_iceberg",
            run_id,
            status="succeeded",
            metrics=microbatch_metrics,
            details={
                "affected_dates": result["affected_dates"],
                "mode": STREAM_JOB_ARGS.mode,
                "gold_refresh_mode": STREAM_JOB_ARGS.gold_refresh_mode,
                "gold_skip_reason": result["gold_metrics"].get("gold_skip_reason", ""),
            },
            inputs=source_files,
            outputs=_batch_outputs(STREAM_JOB_ARGS.gold_refresh_mode),
            started_at=started_at,
        )
        STREAM_BATCH_RESULTS.append(
            {
                "batch_id": batch_id,
                "run_id": run_id,
                "status": "succeeded",
                **microbatch_metrics,
                "affected_dates": result["affected_dates"],
            }
        )
        _write_json_line(
            STREAM_JOB_ARGS.progress_log_path,
            {
                "type": "streaming_microbatch",
                "run_id": run_id,
                "batch_id": batch_id,
                "mode": STREAM_JOB_ARGS.mode,
                "input_rows": batch_count,
                "silver_rows_written": result["silver_metrics"]["rows_written"],
                "gold_refresh_mode": STREAM_JOB_ARGS.gold_refresh_mode,
                "gold_rows_written": result["gold_metrics"]["rows_written"],
                "duration_seconds": microbatch_metrics["duration_seconds"],
                "metrics": microbatch_metrics,
                "affected_dates": result["affected_dates"],
                "gold_refresh_skipped": bool(result["gold_metrics"].get("gold_refresh_skipped", False)),
                "skip_reason": result["gold_metrics"].get("gold_skip_reason", ""),
            },
        )
        print(
            f"Processed streaming batch {batch_id}: input_rows={batch_count} "
            f"silver_rows_written={result['silver_metrics']['rows_written']} "
            f"gold_refresh_mode={STREAM_JOB_ARGS.gold_refresh_mode} "
            f"gold_rows_written={result['gold_metrics']['rows_written']} "
            f"affected_dates={result['affected_dates']}"
        )
    except Exception as exc:
        microbatch_metrics = {
            "batch_id": batch_id,
            "input_rows": batch_count,
            "silver_rows_written": 0,
            "gold_refresh_mode": STREAM_JOB_ARGS.gold_refresh_mode,
            "gold_rows_written": 0,
            "affected_dates": 0,
            "duration_seconds": stage_elapsed_seconds(timer),
        }
        record_stage_event(
            "streaming_microbatch_iceberg",
            run_id,
            status="failed",
            metrics=microbatch_metrics,
            details={
                "failure": str(exc),
                "mode": STREAM_JOB_ARGS.mode,
                "gold_refresh_mode": STREAM_JOB_ARGS.gold_refresh_mode,
            },
            inputs=source_files,
            outputs=[],
            started_at=started_at,
        )
        STREAM_BATCH_RESULTS.append(
            {
                "batch_id": batch_id,
                "run_id": run_id,
                "status": "failed",
                **microbatch_metrics,
            }
        )
        _write_json_line(
            STREAM_JOB_ARGS.progress_log_path,
            {
                "type": "streaming_microbatch_failed",
                "run_id": run_id,
                "batch_id": batch_id,
                "mode": STREAM_JOB_ARGS.mode,
                "gold_refresh_mode": STREAM_JOB_ARGS.gold_refresh_mode,
                "error": str(exc),
                "metrics": microbatch_metrics,
            },
        )
        raise


def _emit_query_progress(query, *, run_id: str, progress_log_path: str, last_progress_id: int | None) -> int | None:
    recent = query.recentProgress
    if not recent:
        return last_progress_id

    newest = recent[-1]
    progress_id = int(newest.get("batchId", -1))
    if last_progress_id is not None and progress_id <= last_progress_id:
        return last_progress_id

    _write_json_line(
        progress_log_path,
        {
            "type": "streaming_query_progress",
            "run_id": run_id,
            "progress": newest,
        },
    )
    return progress_id


def _final_run_metrics() -> dict[str, Any]:
    return {
        "microbatches_processed": len([row for row in STREAM_BATCH_RESULTS if row["status"] == "succeeded"]),
        "microbatches_failed": len([row for row in STREAM_BATCH_RESULTS if row["status"] == "failed"]),
        "microbatches_skipped": len([row for row in STREAM_BATCH_RESULTS if row["status"] == "skipped"]),
        "rows_read": sum(int(row.get("input_rows", 0)) for row in STREAM_BATCH_RESULTS),
        "silver_rows_written": sum(int(row.get("silver_rows_written", 0)) for row in STREAM_BATCH_RESULTS),
        "gold_rows_written": sum(int(row.get("gold_rows_written", 0)) for row in STREAM_BATCH_RESULTS),
    }


def _wait_for_query(query, args: argparse.Namespace, *, run_id: str) -> str:
    poll_seconds = max(args.progress_poll_seconds, 1)
    timeout_deadline = time.time() + args.timeout_seconds if args.timeout_seconds > 0 else None
    stop_deadline = time.time() + args.stop_after_seconds if args.stop_after_seconds > 0 else None
    last_progress_id: int | None = None

    while query.isActive:
        now = time.time()
        wait_seconds = poll_seconds
        if timeout_deadline is not None:
            wait_seconds = min(wait_seconds, max(timeout_deadline - now, 0.0))
        if stop_deadline is not None:
            wait_seconds = min(wait_seconds, max(stop_deadline - now, 0.0))
        if wait_seconds <= 0:
            break

        query.awaitTermination(wait_seconds)
        last_progress_id = _emit_query_progress(
            query,
            run_id=run_id,
            progress_log_path=args.progress_log_path,
            last_progress_id=last_progress_id,
        )

        if timeout_deadline is not None and time.time() >= timeout_deadline and query.isActive:
            query.stop()
            return "timeout"
        if stop_deadline is not None and time.time() >= stop_deadline and query.isActive:
            query.stop()
            return "stop_after"

    return "completed"


def main() -> None:
    """Run the Structured Streaming job in demo or server mode."""

    global STREAM_JOB_ARGS

    args = parse_args()
    STREAM_JOB_ARGS = args
    if args.mode == "server" and args.gold_refresh_mode == GOLD_REFRESH_MODE_FULL:
        print(
            "WARNING: STREAM_GOLD_REFRESH_MODE=full preserves legacy streaming behavior "
            "and may recompute history-wide Gold tables on every microbatch."
        )
    spark = build_spark_session(app_name=f"{SPARK_APP_NAME_PREFIX}-{args.mode}-streaming")
    create_tables_if_needed(spark)
    STREAM_BATCH_RESULTS.clear()
    run_id = f"{args.query_name}-{utc_now_iso().replace(':', '').replace('-', '')}"
    started_at = utc_now_iso()
    timer = stage_timer()

    try:
        kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("failOnDataLoss", "true" if _parse_bool(args.fail_on_data_loss) else "false")
        )
        if args.max_offsets_per_trigger > 0:
            kafka_stream = kafka_stream.option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)

        parsed_stream = parse_replay_messages(kafka_stream.load())
        query = (
            parsed_stream.writeStream.foreachBatch(process_microbatch)
            .queryName(args.query_name)
            .option("checkpointLocation", args.checkpoint_location)
            .trigger(processingTime=f"{args.trigger_seconds} seconds")
            .start()
        )
        stop_reason = _wait_for_query(query, args, run_id=run_id)
        if query.isActive:
            query.stop()

        run_metrics = {
            **_final_run_metrics(),
            "gold_refresh_mode": args.gold_refresh_mode,
            "duration_seconds": stage_elapsed_seconds(timer),
        }
        record_stage_event(
            "streaming_run_iceberg",
            run_id,
            status="succeeded",
            metrics=run_metrics,
            details={
                "topic": args.topic,
                "mode": args.mode,
                "gold_refresh_mode": args.gold_refresh_mode,
                "query_name": args.query_name,
                "checkpoint_location": args.checkpoint_location,
                "stop_reason": stop_reason,
                "microbatches": STREAM_BATCH_RESULTS,
            },
            outputs=_batch_outputs(args.gold_refresh_mode),
            started_at=started_at,
        )
        _write_json_line(
            args.progress_log_path,
            {
                "type": "streaming_run_complete",
                "run_id": run_id,
                "mode": args.mode,
                "gold_refresh_mode": args.gold_refresh_mode,
                "stop_reason": stop_reason,
                "metrics": run_metrics,
            },
        )
    except Exception as exc:
        record_stage_event(
            "streaming_run_iceberg",
            run_id,
            status="failed",
            metrics={
                **_final_run_metrics(),
                "duration_seconds": stage_elapsed_seconds(timer),
            },
            details={
                "topic": args.topic,
                "mode": args.mode,
                "gold_refresh_mode": args.gold_refresh_mode,
                "query_name": args.query_name,
                "checkpoint_location": args.checkpoint_location,
                "failure": str(exc),
                "microbatches": STREAM_BATCH_RESULTS,
            },
            started_at=started_at,
        )
        _write_json_line(
            args.progress_log_path,
            {
                "type": "streaming_run_failed",
                "run_id": run_id,
                "mode": args.mode,
                "gold_refresh_mode": args.gold_refresh_mode,
                "error": str(exc),
            },
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
