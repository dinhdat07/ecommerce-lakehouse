from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from infra.jobs.batch_backfill_to_iceberg import discover_input_files, extract_source_month, iter_months


def test_iter_months_is_inclusive():
    assert iter_months("2019-10", "2020-02") == ["2019-10", "2019-11", "2019-12", "2020-01", "2020-02"]


def test_extract_source_month_supports_csv_and_gzip_names():
    assert extract_source_month(Path("2019-Oct.csv")) == "2019-10"
    assert extract_source_month(Path("2020-Feb.csv.gz")) == "2020-02"
    assert extract_source_month(Path("notes.txt")) is None


def test_discover_input_files_filters_range_and_reports_missing_months(tmp_path):
    (tmp_path / "2019-Oct.csv.gz").write_text("placeholder", encoding="utf-8")
    (tmp_path / "2019-Dec.csv").write_text("placeholder", encoding="utf-8")
    (tmp_path / "2020-Mar.csv").write_text("placeholder", encoding="utf-8")

    files, missing = discover_input_files(tmp_path, "2019-10", "2020-01")

    assert [path.name for path in files] == ["2019-Oct.csv.gz", "2019-Dec.csv"]
    assert missing == ["2019-11", "2020-01"]
