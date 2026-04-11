"""Spark-side data quality checks aligned with ``common/dq_checks.py``.

Run on DataFrames immediately before Iceberg append for Silver and Gold.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.dq_checks import ALLOWED_EVENT_TYPES

DQ_FAIL_ON_ERROR = os.getenv("DQ_FAIL_ON_ERROR", "true").strip().lower() in ("1", "true", "yes", "y")


def _fail(message: str) -> None:
    if DQ_FAIL_ON_ERROR:
        raise RuntimeError(f"DQ gate failed: {message}")


def assert_silver_dataframe_quality(silver_df: DataFrame, *, stage: str = "silver_iceberg") -> dict[str, int]:
    """Return violation counts; optionally raise when ``DQ_FAIL_ON_ERROR`` is set."""

    allowed = F.array(*[F.lit(x) for x in sorted(ALLOWED_EVENT_TYPES)])
    invalid_type = silver_df.where(~F.array_contains(allowed, F.col("event_type"))).count()
    date_ok = F.to_date(F.col("event_time"))
    mismatch = silver_df.where(F.col("event_date") != date_ok).count()
    bad_purchase_price = silver_df.where(
        (F.col("event_type") == "purchase") & ((F.col("price").isNull()) | (F.col("price") < F.lit(0.0)))
    ).count()

    metrics = {
        f"{stage}_invalid_event_type_rows": invalid_type,
        f"{stage}_event_date_mismatch_rows": mismatch,
        f"{stage}_bad_purchase_price_rows": bad_purchase_price,
    }

    if invalid_type:
        _fail(f"invalid event_type rows={invalid_type}")
    if mismatch:
        _fail(f"event_date != date(event_time) rows={mismatch}")
    if bad_purchase_price:
        _fail(f"purchase rows with null/negative price={bad_purchase_price}")

    return metrics


def assert_gold_dataframe_non_negative(
    df: DataFrame,
    *,
    name: str,
    columns: tuple[str, ...],
) -> dict[str, int]:
    """Ensure listed numeric columns have no negative values."""

    if not columns:
        return {}

    condition = None
    for col in columns:
        c = F.col(col)
        part = c.isNotNull() & (c < F.lit(0))
        condition = part if condition is None else (condition | part)

    bad = df.where(condition).count() if condition is not None else 0
    key = f"gold_{name}_negative_metric_rows"
    if bad:
        _fail(f"{name}: negative metric rows={bad} cols={columns}")
    return {key: bad}
