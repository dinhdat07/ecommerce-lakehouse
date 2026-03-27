from common.schemas import EVENT_COLUMNS, EVENT_SCHEMA_SIMPLE, get_required_columns


def test_required_columns_match_base_schema():
    required = get_required_columns()
    assert required == EVENT_COLUMNS
    assert set(required) == set(EVENT_SCHEMA_SIMPLE.keys())


def test_event_schema_has_expected_fields():
    assert EVENT_SCHEMA_SIMPLE["event_time"] == "timestamp"
    assert EVENT_SCHEMA_SIMPLE["price"] == "double"
    assert EVENT_SCHEMA_SIMPLE["user_session"] == "string"
