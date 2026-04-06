from pathlib import Path

from apps.producer.replay_csv_to_kafka import build_message, derive_source_month, iter_messages


def test_derive_source_month_uses_event_time_prefix():
    assert derive_source_month({"event_time": "2020-03-15T12:30:00Z"}) == "2020-03"
    assert derive_source_month({"event_time": "bad"}) is None


def test_build_message_wraps_payload_with_replay_metadata(tmp_path):
    input_path = tmp_path / "events_streaming_demo.csv"
    row = {"event_time": "2020-04-01T10:00:00Z", "event_type": "view"}

    message = build_message(input_path, row)

    assert message["source_file"] == "events_streaming_demo.csv"
    assert message["source_month"] == "2020-04"
    assert message["payload"] == row
    assert str(message["replayed_at"]).endswith("Z")


def test_iter_messages_respects_max_rows(tmp_path):
    input_path = tmp_path / "events.csv"
    input_path.write_text(
        "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session\n"
        "2020-03-01T00:00:00Z,view,1,2,a.b,Acme,10.00,11,sess-1\n"
        "2020-03-01T00:00:01Z,cart,1,2,a.b,Acme,10.00,11,sess-1\n",
        encoding="utf-8",
    )

    messages = list(iter_messages(Path(input_path), max_rows=1))

    assert len(messages) == 1
    assert messages[0]["payload"]["event_type"] == "view"
