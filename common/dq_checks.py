"""Publication gate data-quality checks for Silver and Gold outputs.

Rules are applied immediately before writing Silver JSONL partitions or Gold
CSVs (local pipeline) and are mirrored for Spark/Iceberg in
``infra/jobs/dq_iceberg.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from common.quality import derive_event_date

ALLOWED_EVENT_TYPES = frozenset({"view", "cart", "purchase"})
SILVER_REQUIRED_COLUMNS = (
    "event_time",
    "event_date",
    "event_type",
    "product_id",
    "user_id",
)
SILVER_CRITICAL_FIELDS = ("event_time", "event_date", "event_type")
GOLD_REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "conversion_funnel": ("event_date", "views", "carts", "purchases"),
    "user_activity_summary": ("event_date", "total_events", "views", "carts", "purchases"),
    "product_popularity": ("event_date", "views", "carts", "purchases"),
    "session_summary": ("event_date", "total_events", "views", "carts", "purchases"),
    "revenue_by_category": ("event_date", "purchase_count", "purchase_revenue", "unique_buyers"),
    "cohort_retention": ("cohort_month", "period_offset", "cohort_users", "active_users", "retention_rate"),
    "repeat_purchase": ("activity_month", "purchasers", "repeat_purchasers", "repeat_purchase_rate"),
    "product_affinity": ("product_a", "product_b", "co_purchase_sessions", "affinity_lift"),
    "time_to_conversion_distribution": ("event_date", "time_bucket", "conversions"),
    "rfm_segmentation": (
        "as_of_date",
        "user_id",
        "recency_days",
        "frequency_90d",
        "monetary_90d",
        "r_score",
        "f_score",
        "m_score",
        "rfm_segment",
    ),
}
DQ_SEVERITY_CRITICAL = "critical"
DQ_SEVERITY_WARNING = "warning"


@dataclass(frozen=True)
class DQRuleResult:
    """Outcome of a single named rule."""

    name: str
    passed: bool
    detail: str = ""
    severity: str = DQ_SEVERITY_CRITICAL


@dataclass
class DQReport:
    """Aggregated DQ outcome for a stage."""

    stage: str
    rules: list[DQRuleResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(
            rule.passed for rule in self.rules if rule.severity == DQ_SEVERITY_CRITICAL
        )

    def to_metrics(self) -> dict[str, object]:
        """Serialize for run manifests and logs."""

        return {
            "dq_passed": self.passed,
            "dq_rules": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "detail": r.detail,
                    "severity": r.severity,
                }
                for r in self.rules
            ],
        }


def _is_missing(value: object) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _missing_columns(row: dict, required: tuple[str, ...]) -> list[str]:
    return [column for column in required if column not in row]


def validate_silver_rows_for_publication(rows: list[dict]) -> DQReport:
    """Block Silver publication if canonical rows violate production rules."""

    report = DQReport(stage="silver_publication")
    n = len(rows)
    if n == 0:
        report.rules.append(DQRuleResult("silver_batch_empty", True, "0 valid rows after canonicalization"))
        return report

    invalid_event_type = 0
    missing_required = 0
    missing_critical = 0
    invalid_timestamp = 0
    date_mismatch = 0
    bad_purchase_price = 0
    duplicate_rows = 0
    seen_rows: set[str] = set()

    for row in rows:
        if _missing_columns(row, SILVER_REQUIRED_COLUMNS):
            missing_required += 1

        if any(_is_missing(row.get(field)) for field in SILVER_CRITICAL_FIELDS):
            missing_critical += 1

        et = row.get("event_type")
        if et not in ALLOWED_EVENT_TYPES:
            invalid_event_type += 1

        event_time = row.get("event_time")
        event_date = row.get("event_date")
        if isinstance(event_time, str) and isinstance(event_date, str):
            try:
                if derive_event_date(event_time) != event_date:
                    date_mismatch += 1
            except Exception:
                invalid_timestamp += 1
        elif not _is_missing(event_time):
            invalid_timestamp += 1

        if et == "purchase":
            price = row.get("price")
            if price is None or (isinstance(price, (int, float)) and price < 0):
                bad_purchase_price += 1

        duplicate_key = str(
            (
                row.get("event_id"),
                row.get("dedupe_key"),
                row.get("event_time"),
                row.get("event_type"),
                row.get("product_id"),
                row.get("category_id"),
                row.get("category_code"),
                row.get("brand"),
                row.get("price"),
                row.get("user_id"),
                row.get("user_session"),
            )
        )
        if duplicate_key in seen_rows:
            duplicate_rows += 1
        else:
            seen_rows.add(duplicate_key)

    report.rules.append(
        DQRuleResult(
            "silver_required_columns_present",
            missing_required == 0,
            f"rows_with_missing_columns={missing_required} total={n}",
        )
    )
    report.rules.append(
        DQRuleResult(
            "silver_critical_fields_not_null",
            missing_critical == 0,
            f"rows_with_null_critical_fields={missing_critical} total={n}",
        )
    )
    report.rules.append(
        DQRuleResult(
            "event_type_in_allowlist",
            invalid_event_type == 0,
            f"invalid_event_type_rows={invalid_event_type} total={n}",
        )
    )
    report.rules.append(
        DQRuleResult(
            "event_time_is_valid_timestamp",
            invalid_timestamp == 0,
            f"invalid_timestamp_rows={invalid_timestamp} total={n}",
        )
    )
    report.rules.append(
        DQRuleResult(
            "event_date_matches_event_time",
            date_mismatch == 0,
            f"date_mismatch_rows={date_mismatch} total={n}",
        )
    )
    report.rules.append(
        DQRuleResult(
            "purchase_has_non_negative_price",
            bad_purchase_price == 0,
            f"bad_purchase_price_rows={bad_purchase_price} total={n}",
        )
    )
    report.rules.append(
        DQRuleResult(
            "silver_duplicate_rows_detected",
            duplicate_rows == 0,
            f"duplicate_rows={duplicate_rows} total={n}",
            severity=DQ_SEVERITY_WARNING,
        )
    )
    return report


def validate_gold_tables_for_publication(tables: dict[str, list[dict]]) -> DQReport:
    """Block Gold publication if aggregated outputs contain impossible metrics."""

    report = DQReport(stage="gold_publication")

    def check_non_negative_counts(table_name: str, rows: list[dict], keys: tuple[str, ...]) -> None:
        bad = 0
        for row in rows:
            for key in keys:
                v = row.get(key)
                if isinstance(v, (int, float)) and v < 0:
                    bad += 1
                    break
        report.rules.append(
            DQRuleResult(
                f"{table_name}_non_negative_metrics",
                bad == 0,
                f"negative_metric_rows={bad} in {table_name}",
            )
        )

    def check_schema(table_name: str, rows: list[dict]) -> None:
        required = GOLD_REQUIRED_COLUMNS.get(table_name, ())
        missing_columns = 0
        null_event_date = 0
        for row in rows:
            if _missing_columns(row, required):
                missing_columns += 1
            if "event_date" in required and _is_missing(row.get("event_date")):
                null_event_date += 1
        report.rules.append(
            DQRuleResult(
                f"{table_name}_required_columns_present",
                missing_columns == 0,
                f"rows_with_missing_columns={missing_columns} in {table_name}",
            )
        )
        if "event_date" in required:
            report.rules.append(
                DQRuleResult(
                    f"{table_name}_event_date_not_null",
                    null_event_date == 0,
                    f"rows_with_null_event_date={null_event_date} in {table_name}",
                )
            )

    funnel = tables.get("conversion_funnel") or []
    check_schema("conversion_funnel", funnel)
    check_non_negative_counts("conversion_funnel", funnel, ("views", "carts", "purchases"))

    user_act = tables.get("user_activity_summary") or []
    check_schema("user_activity_summary", user_act)
    check_non_negative_counts(
        "user_activity_summary",
        user_act,
        ("total_events", "views", "carts", "purchases"),
    )

    product_pop = tables.get("product_popularity") or []
    check_schema("product_popularity", product_pop)
    check_non_negative_counts("product_popularity", product_pop, ("views", "carts", "purchases"))

    sessions = tables.get("session_summary") or []
    check_schema("session_summary", sessions)
    check_non_negative_counts(
        "session_summary",
        sessions,
        ("total_events", "views", "carts", "purchases"),
    )

    revenue_cat = tables.get("revenue_by_category") or []
    check_schema("revenue_by_category", revenue_cat)
    check_non_negative_counts(
        "revenue_by_category",
        revenue_cat,
        ("purchase_count", "purchase_revenue", "unique_buyers"),
    )

    cohort_retention = tables.get("cohort_retention") or []
    check_schema("cohort_retention", cohort_retention)
    check_non_negative_counts("cohort_retention", cohort_retention, ("cohort_users", "active_users"))

    repeat_purchase = tables.get("repeat_purchase") or []
    check_schema("repeat_purchase", repeat_purchase)
    check_non_negative_counts("repeat_purchase", repeat_purchase, ("purchasers", "repeat_purchasers"))

    product_affinity = tables.get("product_affinity") or []
    check_schema("product_affinity", product_affinity)
    check_non_negative_counts("product_affinity", product_affinity, ("co_purchase_sessions",))

    time_to_conversion_distribution = tables.get("time_to_conversion_distribution") or []
    check_schema("time_to_conversion_distribution", time_to_conversion_distribution)
    check_non_negative_counts("time_to_conversion_distribution", time_to_conversion_distribution, ("conversions",))

    rfm_segmentation = tables.get("rfm_segmentation") or []
    check_schema("rfm_segmentation", rfm_segmentation)
    check_non_negative_counts(
        "rfm_segmentation",
        rfm_segmentation,
        ("recency_days", "frequency_90d", "monetary_90d", "r_score", "f_score", "m_score"),
    )

    return report
