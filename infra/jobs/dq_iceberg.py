"""Spark-side data quality checks aligned with ``common/dq_checks.py``.

Run on DataFrames immediately before Iceberg append for Silver and Gold.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.dq_checks import (  # noqa: E402
    ALLOWED_EVENT_TYPES,
    DQ_SEVERITY_CRITICAL,
    DQ_SEVERITY_WARNING,
    SILVER_REQUIRED_COLUMNS,
)

DQ_FAIL_ON_ERROR = os.getenv("DQ_FAIL_ON_ERROR", "true").strip().lower() in ("1", "true", "yes", "y")
DEFAULT_SAMPLE_LIMIT = 5


def _fail(message: str) -> None:
    if DQ_FAIL_ON_ERROR:
        raise RuntimeError(f"DQ gate failed: {message}")


def _rule(name: str, passed: bool, detail: str, *, severity: str = DQ_SEVERITY_CRITICAL) -> dict[str, str | bool]:
    return {
        "name": name,
        "passed": passed,
        "detail": detail,
        "severity": severity,
    }


def _evaluate_rules(rules: list[dict[str, str | bool]]) -> bool:
    return all(bool(rule["passed"]) for rule in rules if rule["severity"] == DQ_SEVERITY_CRITICAL)


def _sample_rows(df: DataFrame, columns: tuple[str, ...], *, limit: int = DEFAULT_SAMPLE_LIMIT) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    sample = df.select(*columns).limit(limit).collect()
    return [row.asDict(recursive=True) for row in sample]


def _missing_columns(df: DataFrame, required_columns: tuple[str, ...]) -> list[str]:
    present = set(df.columns)
    return [column for column in required_columns if column not in present]


def summarize_silver_dataframe_quality(
    silver_base_rows: DataFrame,
    silver_publish_rows: DataFrame,
    *,
    stage: str = "silver_iceberg",
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> dict[str, Any]:
    """Return structured Silver DQ metrics and sampled bad records."""

    required_columns = SILVER_REQUIRED_COLUMNS + ("event_id", "dedupe_key")
    missing_columns = _missing_columns(silver_publish_rows, required_columns)

    base_count = silver_base_rows.count()
    valid_pre_dedupe = silver_base_rows.where(
        F.col("event_time").isNotNull() & F.col("event_type").isNotNull()
    ).count()
    publish_count = silver_publish_rows.count()
    duplicate_rows = max(valid_pre_dedupe - publish_count, 0)

    invalid_timestamp_df = silver_base_rows.where(
        F.col("_raw_event_time").isNotNull() & F.col("event_time").isNull()
    )
    invalid_event_type_df = silver_publish_rows.where(~F.col("event_type").isin(*sorted(ALLOWED_EVENT_TYPES)))
    null_event_time_df = silver_publish_rows.where(F.col("event_time").isNull())
    null_event_date_df = silver_publish_rows.where(F.col("event_date").isNull())
    null_event_type_df = silver_publish_rows.where(F.col("event_type").isNull())
    bad_purchase_price_df = silver_publish_rows.where(
        (F.col("event_type") == "purchase")
        & ((F.col("price").isNull()) | (F.col("price") < F.lit(0.0)))
    )

    invalid_timestamp = invalid_timestamp_df.count()
    invalid_event_type = invalid_event_type_df.count()
    null_event_time = null_event_time_df.count()
    null_event_date = null_event_date_df.count()
    null_event_type = null_event_type_df.count()
    bad_purchase_price = bad_purchase_price_df.count()

    metrics = {
        f"{stage}_rows_seen": base_count,
        f"{stage}_rows_valid_pre_dedupe": valid_pre_dedupe,
        f"{stage}_rows_to_publish": publish_count,
        f"{stage}_duplicate_rows_dropped": duplicate_rows,
        f"{stage}_invalid_timestamp_rows": invalid_timestamp,
        f"{stage}_invalid_event_type_rows": invalid_event_type,
        f"{stage}_null_event_time_rows": null_event_time,
        f"{stage}_null_event_date_rows": null_event_date,
        f"{stage}_null_event_type_rows": null_event_type,
        f"{stage}_bad_purchase_price_rows": bad_purchase_price,
        f"{stage}_missing_required_column_count": len(missing_columns),
    }

    rules = [
        _rule(
            "silver_required_columns_present",
            len(missing_columns) == 0,
            f"missing_columns={missing_columns}",
        ),
        _rule(
            "silver_invalid_source_timestamps_dropped",
            invalid_timestamp == 0,
            f"invalid_timestamp_rows={invalid_timestamp}",
            severity=DQ_SEVERITY_WARNING,
        ),
        _rule(
            "silver_duplicate_rows_dropped",
            duplicate_rows == 0,
            f"duplicate_rows_dropped={duplicate_rows}",
            severity=DQ_SEVERITY_WARNING,
        ),
        _rule(
            "silver_event_type_in_allowlist",
            invalid_event_type == 0,
            f"invalid_event_type_rows={invalid_event_type}",
        ),
        _rule(
            "silver_event_time_not_null",
            null_event_time == 0,
            f"null_event_time_rows={null_event_time}",
        ),
        _rule(
            "silver_event_date_not_null",
            null_event_date == 0,
            f"null_event_date_rows={null_event_date}",
        ),
        _rule(
            "silver_event_type_not_null",
            null_event_type == 0,
            f"null_event_type_rows={null_event_type}",
        ),
        _rule(
            "silver_purchase_price_non_negative",
            bad_purchase_price == 0,
            f"bad_purchase_price_rows={bad_purchase_price}",
        ),
    ]

    bad_record_samples: dict[str, list[dict[str, Any]]] = {}
    if invalid_timestamp:
        bad_record_samples["invalid_timestamp"] = _sample_rows(
            invalid_timestamp_df,
            ("source_file", "source_month", "_raw_event_time", "event_type", "user_id"),
            limit=sample_limit,
        )
    if invalid_event_type:
        bad_record_samples["invalid_event_type"] = _sample_rows(
            invalid_event_type_df,
            ("source_file", "source_month", "event_time", "event_type", "user_id"),
            limit=sample_limit,
        )
    if bad_purchase_price:
        bad_record_samples["bad_purchase_price"] = _sample_rows(
            bad_purchase_price_df,
            ("source_file", "source_month", "event_time", "event_type", "price", "user_id"),
            limit=sample_limit,
        )

    dq_passed = _evaluate_rules(rules)
    summary = {
        "dq_passed": dq_passed,
        "dq_rules": rules,
        "bad_record_samples": bad_record_samples,
        **metrics,
    }
    if not dq_passed:
        critical_failures = [rule["detail"] for rule in rules if not rule["passed"] and rule["severity"] == DQ_SEVERITY_CRITICAL]
        _fail("; ".join(str(value) for value in critical_failures))
    return summary


def summarize_gold_dataframe_quality(
    df: DataFrame,
    *,
    name: str,
    columns: tuple[str, ...],
    required_columns: tuple[str, ...] = ("event_date",),
    stage: str = "gold",
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
) -> dict[str, Any]:
    """Return structured Gold DQ metrics and sampled bad rows."""

    missing_columns = _missing_columns(df, required_columns)
    null_event_date_df = df.where(F.col("event_date").isNull()) if "event_date" in df.columns else df.limit(0)

    condition = None
    for col in columns:
        if col not in df.columns:
            continue
        part = F.col(col).isNotNull() & (F.col(col) < F.lit(0))
        condition = part if condition is None else (condition | part)
    negative_df = df.where(condition) if condition is not None else df.limit(0)

    row_count = df.count()
    null_event_date = null_event_date_df.count() if "event_date" in df.columns else 0
    negative_rows = negative_df.count()

    metrics = {
        f"{stage}_{name}_rows_to_publish": row_count,
        f"{stage}_{name}_missing_required_column_count": len(missing_columns),
        f"{stage}_{name}_null_event_date_rows": null_event_date,
        f"{stage}_{name}_negative_metric_rows": negative_rows,
    }
    rules = [
        _rule(
            f"{name}_required_columns_present",
            len(missing_columns) == 0,
            f"missing_columns={missing_columns}",
        ),
        _rule(
            f"{name}_event_date_not_null",
            null_event_date == 0,
            f"null_event_date_rows={null_event_date}",
        ),
        _rule(
            f"{name}_metrics_non_negative",
            negative_rows == 0,
            f"negative_metric_rows={negative_rows} cols={columns}",
        ),
    ]

    bad_record_samples: dict[str, list[dict[str, Any]]] = {}
    if negative_rows:
        sample_cols = tuple(column for column in ("event_date", *columns) if column in df.columns)
        bad_record_samples["negative_metrics"] = _sample_rows(negative_df, sample_cols, limit=sample_limit)

    dq_passed = _evaluate_rules(rules)
    summary = {
        "dq_passed": dq_passed,
        "dq_rules": rules,
        "bad_record_samples": bad_record_samples,
        **metrics,
    }
    if not dq_passed:
        critical_failures = [rule["detail"] for rule in rules if not rule["passed"] and rule["severity"] == DQ_SEVERITY_CRITICAL]
        _fail("; ".join(str(value) for value in critical_failures))
    return summary


def assert_silver_dataframe_quality(silver_df: DataFrame, *, stage: str = "silver_iceberg") -> dict[str, Any]:
    """Backward-compatible Silver publication check for pre-filtered rows only."""

    return summarize_silver_dataframe_quality(silver_df, silver_df, stage=stage)


def assert_gold_dataframe_non_negative(
    df: DataFrame,
    *,
    name: str,
    columns: tuple[str, ...],
) -> dict[str, Any]:
    """Backward-compatible Gold publication check with structured metrics."""

    return summarize_gold_dataframe_quality(df, name=name, columns=columns)
