"""Publication gate data-quality checks for Silver and Gold outputs.

Rules are applied immediately before writing Silver JSONL partitions or Gold
CSVs (local pipeline) and are mirrored for Spark/Iceberg in ``infra/jobs/dq_iceberg.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from common.quality import derive_event_date

ALLOWED_EVENT_TYPES = frozenset({"view", "cart", "purchase"})


@dataclass(frozen=True)
class DQRuleResult:
    """Outcome of a single named rule."""

    name: str
    passed: bool
    detail: str = ""


@dataclass
class DQReport:
    """Aggregated DQ outcome for a stage."""

    stage: str
    rules: list[DQRuleResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(rule.passed for rule in self.rules)

    def to_metrics(self) -> dict[str, object]:
        """Serialize for run manifests and logs."""

        return {
            "dq_passed": self.passed,
            "dq_rules": [
                {"name": r.name, "passed": r.passed, "detail": r.detail} for r in self.rules
            ],
        }


def validate_silver_rows_for_publication(rows: list[dict]) -> DQReport:
    """Block Silver publication if canonical rows violate production rules."""

    report = DQReport(stage="silver_publication")
    n = len(rows)
    if n == 0:
        report.rules.append(DQRuleResult("silver_batch_empty", True, "0 valid rows after canonicalization"))
        return report

    invalid_event_type = 0
    date_mismatch = 0
    bad_purchase_price = 0

    for row in rows:
        et = row.get("event_type")
        if et not in ALLOWED_EVENT_TYPES:
            invalid_event_type += 1

        event_time = row.get("event_time")
        event_date = row.get("event_date")
        if isinstance(event_time, str) and isinstance(event_date, str):
            if derive_event_date(event_time) != event_date:
                date_mismatch += 1

        if et == "purchase":
            price = row.get("price")
            if price is None or (isinstance(price, (int, float)) and price < 0):
                bad_purchase_price += 1

    report.rules.append(
        DQRuleResult(
            "event_type_in_allowlist",
            invalid_event_type == 0,
            f"invalid_event_type_rows={invalid_event_type} total={n}",
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

    funnel = tables.get("conversion_funnel") or []
    check_non_negative_counts("conversion_funnel", funnel, ("views", "carts", "purchases"))

    user_act = tables.get("user_activity_summary") or []
    check_non_negative_counts(
        "user_activity_summary",
        user_act,
        ("total_events", "views", "carts", "purchases"),
    )

    product_pop = tables.get("product_popularity") or []
    check_non_negative_counts("product_popularity", product_pop, ("views", "carts", "purchases"))

    sessions = tables.get("session_summary") or []
    check_non_negative_counts(
        "session_summary",
        sessions,
        ("total_events", "views", "carts", "purchases"),
    )

    revenue_cat = tables.get("revenue_by_category") or []
    check_non_negative_counts(
        "revenue_by_category",
        revenue_cat,
        ("purchase_count", "purchase_revenue", "unique_buyers"),
    )

    return report
