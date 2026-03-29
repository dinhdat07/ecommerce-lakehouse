from common.quality import (
    build_event_identity,
    coerce_float,
    coerce_int,
    normalize_nullable_text,
    parse_event_timestamp,
)


def test_parse_event_timestamp_supports_historical_utc_format():
    assert parse_event_timestamp("2019-10-01 00:00:00 UTC") == "2019-10-01T00:00:00Z"


def test_parse_event_timestamp_supports_sample_iso_format():
    assert parse_event_timestamp("2020-04-01T10:00:00Z") == "2020-04-01T10:00:00Z"


def test_nullable_normalization_and_numeric_coercion():
    assert normalize_nullable_text("  ") is None
    assert coerce_int("123") == 123
    assert coerce_float("12.50") == 12.5
    assert coerce_float("bad") is None


def test_build_event_identity_is_stable():
    row = {
        "event_time": "2020-04-01T10:00:00Z",
        "event_type": "view",
        "product_id": 1,
        "category_id": 2,
        "category_code": "electronics.smartphone",
        "brand": "Acme",
        "price": 12.5,
        "user_id": 3,
        "user_session": "sess-1",
    }
    assert build_event_identity(row) == build_event_identity(dict(row))
