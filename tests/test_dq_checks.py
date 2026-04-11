from common.dq_checks import validate_gold_tables_for_publication, validate_silver_rows_for_publication


def test_validate_silver_passes_empty_batch():
    report = validate_silver_rows_for_publication([])
    assert report.passed


def test_validate_silver_passes_clean_rows():
    rows = [
        {
            "event_time": "2020-04-01T10:00:00Z",
            "event_date": "2020-04-01",
            "event_type": "purchase",
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
            "price": 1.0,
        }
    ]
    report = validate_silver_rows_for_publication(rows)
    assert not report.passed


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
            {"total_events": 1, "views": 1, "carts": 0, "purchases": 0},
        ],
        "product_popularity": [{"views": 1, "carts": 0, "purchases": 0}],
        "session_summary": [{"total_events": 1, "views": 1, "carts": 0, "purchases": 0}],
    }
    report = validate_gold_tables_for_publication(tables)
    assert report.passed
