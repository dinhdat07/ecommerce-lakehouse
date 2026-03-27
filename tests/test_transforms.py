import pytest

from common.transforms import event_date_from_iso, normalize_event, validate_required_columns


def test_normalize_event_basic_rules():
    record = {
        "event_type": " View ",
        "brand": " ACME ",
        "price": "12.50",
    }
    out = normalize_event(record)
    assert out["event_type"] == "view"
    assert out["brand"] == "ACME"
    assert out["price"] == 12.5


def test_event_date_from_iso():
    assert event_date_from_iso("2020-04-01T10:00:00Z") == "2020-04-01"


def test_validate_required_columns_raises_on_missing():
    with pytest.raises(ValueError):
        validate_required_columns(["event_time", "event_type"])
