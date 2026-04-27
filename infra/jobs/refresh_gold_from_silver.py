"""Refresh Gold tables from the existing Silver table without replaying Bronze."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from infra.jobs.batch_backfill_to_iceberg import (  # noqa: E402
    GOLD_REFRESH_MODE_AFFECTED_DATES,
    GOLD_REFRESH_MODE_FULL,
    SILVER_TABLE,
    build_spark_session,
    create_tables_if_needed,
    iter_months,
    refresh_gold_tables,
)
from common.observability import record_stage_event, stage_elapsed_seconds, stage_timer  # noqa: E402
from common.runtime import utc_now_iso  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh Gold tables from the current Silver state.")
    parser.add_argument("--start-month", required=True, help="Inclusive YYYY-MM lower bound.")
    parser.add_argument("--end-month", required=True, help="Inclusive YYYY-MM upper bound.")
    parser.add_argument(
        "--mode",
        choices=[GOLD_REFRESH_MODE_AFFECTED_DATES, GOLD_REFRESH_MODE_FULL],
        default=GOLD_REFRESH_MODE_FULL,
        help="Refresh scope. Use full to rebuild history-wide Gold tables from Silver.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark_session()
    run_id = f"gold-refresh-{args.start_month.replace('-', '')}-{args.end_month.replace('-', '')}"
    started_at = utc_now_iso()
    timer = stage_timer()

    try:
        create_tables_if_needed(spark)
        requested_months = iter_months(args.start_month, args.end_month)
        affected_dates = sorted(
            str(row["event_date"])
            for row in spark.table(SILVER_TABLE)
            .where(F.col("source_month").isin(requested_months))
            .select("event_date")
            .where(F.col("event_date").isNotNull())
            .distinct()
            .collect()
        )
        gold_result = refresh_gold_tables(spark, affected_dates, mode=args.mode)
        gold_rows_written = sum(int(value) for value in gold_result["row_counts"].values())
        record_stage_event(
            "gold_refresh_iceberg",
            run_id,
            status="succeeded",
            metrics={
                "gold_refresh_mode": args.mode,
                "affected_dates": len(affected_dates),
                "rows_written": gold_rows_written,
                "tables_written": gold_result["tables_written"],
                "duration_seconds": stage_elapsed_seconds(timer),
            },
            details={
                "start_month": args.start_month,
                "end_month": args.end_month,
                "affected_dates": affected_dates,
                "skipped_reason": gold_result.get("skipped_reason", ""),
                "skipped_tables": gold_result.get("skipped_tables", []),
            },
            inputs=[SILVER_TABLE],
            outputs=list(gold_result["row_counts"].keys()),
            started_at=started_at,
        )
        print(
            f"Refreshed Gold from Silver: mode={args.mode} affected_dates={len(affected_dates)} "
            f"rows_written={gold_rows_written}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
