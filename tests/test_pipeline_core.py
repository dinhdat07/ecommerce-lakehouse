from pathlib import Path

from common.config import load_config
from pipelines.bronze.backfill import run_bronze_backfill
from pipelines.gold.aggregate import build_gold_tables
from pipelines.silver.transform import canonicalize_record, run_bronze_to_silver


def test_canonicalize_record_rejects_missing_columns():
    valid, invalid = canonicalize_record({"record_hash": "abc", "payload": {"event_type": "view"}})
    assert valid is None
    assert invalid is not None
    assert invalid["reason"] == "missing_required_columns"


def test_build_gold_tables_computes_expected_funnel_metrics():
    silver_rows = [
        {
            "event_id": "1",
            "event_time": "2020-04-01T10:00:00Z",
            "event_date": "2020-04-01",
            "event_type": "view",
            "product_id": 10,
            "category_id": 100,
            "category_code": "electronics.audio",
            "brand": "Acme",
            "price": 10.0,
            "user_id": 1,
            "user_session": "sess-1",
        },
        {
            "event_id": "2",
            "event_time": "2020-04-01T10:01:00Z",
            "event_date": "2020-04-01",
            "event_type": "cart",
            "product_id": 10,
            "category_id": 100,
            "category_code": "electronics.audio",
            "brand": "Acme",
            "price": 10.0,
            "user_id": 1,
            "user_session": "sess-1",
        },
        {
            "event_id": "3",
            "event_time": "2020-04-01T10:02:00Z",
            "event_date": "2020-04-01",
            "event_type": "purchase",
            "product_id": 10,
            "category_id": 100,
            "category_code": "electronics.audio",
            "brand": "Acme",
            "price": 10.0,
            "user_id": 1,
            "user_session": "sess-1",
        },
    ]
    tables = build_gold_tables(silver_rows)
    funnel = tables["conversion_funnel"][0]
    assert funnel["views"] == 1
    assert funnel["carts"] == 1
    assert funnel["purchases"] == 1
    assert funnel["view_to_purchase_rate"] == 1.0


def test_batch_pipeline_round_trip_on_small_sample(tmp_path):
    sample_dir = tmp_path / "sample"
    sample_dir.mkdir()
    sample_file = sample_dir / "events.csv"
    sample_file.write_text(
        "\n".join(
            [
                "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session",
                "2020-04-01T10:00:00Z,view,101,200,electronics.audio,Acme,10.00,1,sess-1",
                "2020-04-01T10:01:00Z,purchase,101,200,electronics.audio,Acme,10.00,1,sess-1",
            ]
        ),
        encoding="utf-8",
    )

    config = load_config(
        bronze_uri=str(tmp_path / "bronze"),
        silver_uri=str(tmp_path / "silver"),
        gold_uri=str(tmp_path / "gold"),
    )
    bronze_result = run_bronze_backfill(config, input_dir=sample_dir)
    assert bronze_result.rows_written == 2

    silver_result = run_bronze_to_silver(config)
    assert silver_result.valid_rows == 2
    assert Path(silver_result.output_paths[0]).exists()
