from common.dq_checks import (
    DQ_SEVERITY_WARNING,
    validate_gold_tables_for_publication,
    validate_silver_rows_for_publication,
)


def test_validate_silver_passes_empty_batch():
    report = validate_silver_rows_for_publication([])
    assert report.passed


def test_validate_silver_passes_clean_rows():
    rows = [
        {
            "event_time": "2020-04-01T10:00:00Z",
            "event_date": "2020-04-01",
            "event_type": "purchase",
            "product_id": 1,
            "user_id": 10,
            "price": 10.0,
        }
    ]
    report = validate_silver_rows_for_publication(rows)
    assert report.passed


def test_validate_silver_fails_bad_event_type():
    rows = [
        {
            "event_time": "2020-04-01T10:00:00Z",
            "event_date": "2020-04-01",
            "event_type": "unknown",
            "product_id": 1,
            "user_id": 10,
            "price": 1.0,
        }
    ]
    report = validate_silver_rows_for_publication(rows)
    assert not report.passed


def test_validate_silver_fails_missing_required_columns():
    rows = [
        {
            "event_time": "2020-04-01T10:00:00Z",
            "event_date": "2020-04-01",
            "event_type": "view",
        }
    ]
    report = validate_silver_rows_for_publication(rows)
    assert not report.passed
    assert any(rule.name == "silver_required_columns_present" and not rule.passed for rule in report.rules)


def test_validate_silver_duplicate_warning_does_not_fail_publication():
    rows = [
        {
            "event_time": "2020-04-01T10:00:00Z",
            "event_date": "2020-04-01",
            "event_type": "view",
            "product_id": 1,
            "user_id": 10,
            "price": 1.0,
        },
        {
            "event_time": "2020-04-01T10:00:00Z",
            "event_date": "2020-04-01",
            "event_type": "view",
            "product_id": 1,
            "user_id": 10,
            "price": 1.0,
        },
    ]
    report = validate_silver_rows_for_publication(rows)
    duplicate_rule = next(rule for rule in report.rules if rule.name == "silver_duplicate_rows_detected")
    assert report.passed
    assert duplicate_rule.severity == DQ_SEVERITY_WARNING
    assert not duplicate_rule.passed


def test_validate_gold_passes_non_negative_metrics():
    tables = {
        "conversion_funnel": [{"event_date": "2020-04-01", "views": 1, "carts": 0, "purchases": 0}],
        "revenue_by_category": [
            {
                "event_date": "2020-04-01",
                "category_id": 1,
                "category_code": "a",
                "purchase_count": 1,
                "purchase_revenue": 10.0,
                "unique_buyers": 1,
            }
        ],
        "user_activity_summary": [
            {"event_date": "2020-04-01", "total_events": 1, "views": 1, "carts": 0, "purchases": 0},
        ],
        "product_popularity": [{"event_date": "2020-04-01", "views": 1, "carts": 0, "purchases": 0}],
        "session_summary": [{"event_date": "2020-04-01", "total_events": 1, "views": 1, "carts": 0, "purchases": 0}],
        "cohort_retention": [
            {"cohort_month": "2020-04", "period_offset": 0, "cohort_users": 1, "active_users": 1, "retention_rate": 1.0}
        ],
        "repeat_purchase": [
            {"activity_month": "2020-04", "purchasers": 1, "repeat_purchasers": 0, "repeat_purchase_rate": 0.0}
        ],
        "product_affinity": [
            {"product_a": 1, "product_b": 2, "co_purchase_sessions": 1, "affinity_lift": 1.0}
        ],
        "time_to_conversion_distribution": [
            {"event_date": "2020-04-01", "time_bucket": "0-60s", "conversions": 1}
        ],
        "rfm_segmentation": [
            {
                "as_of_date": "2020-04-01",
                "user_id": 10,
                "recency_days": 0,
                "frequency_90d": 1,
                "monetary_90d": 10.0,
                "r_score": 5,
                "f_score": 5,
                "m_score": 5,
                "rfm_segment": "champions",
            }
        ],
    }
    report = validate_gold_tables_for_publication(tables)
    assert report.passed


def test_validate_gold_fails_missing_event_date():
    tables = {
        "conversion_funnel": [{"views": 1, "carts": 0, "purchases": 0}],
        "revenue_by_category": [],
        "user_activity_summary": [],
        "product_popularity": [],
        "session_summary": [],
        "time_to_conversion_distribution": [],
    }
    report = validate_gold_tables_for_publication(tables)
    assert not report.passed


def test_validate_gold_fails_missing_phase4_required_columns():
    tables = {
        "conversion_funnel": [],
        "revenue_by_category": [],
        "user_activity_summary": [],
        "product_popularity": [],
        "session_summary": [],
        "cohort_retention": [{"cohort_month": "2020-04", "active_users": 1}],
    }
    report = validate_gold_tables_for_publication(tables)
    assert not report.passed
    assert any(rule.name == "cohort_retention_required_columns_present" and not rule.passed for rule in report.rules)
