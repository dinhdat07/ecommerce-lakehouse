"""Zero-dependency local verification for the batch pipeline.

This script gives the repository an automated validation path even before
external test dependencies are installed. It runs a small Bronze -> Silver ->
Gold flow in a temporary directory and asserts that the main Gold outputs are
produced.
"""

from __future__ import annotations

import tempfile
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.config import load_config  # noqa: E402
from pipelines.bronze.backfill import run_bronze_backfill  # noqa: E402
from pipelines.gold.aggregate import run_silver_to_gold  # noqa: E402
from pipelines.silver.transform import run_bronze_to_silver  # noqa: E402


def build_sample_csv(path: Path) -> None:
    """Write a tiny deterministic sample CSV used by local verification."""

    path.write_text(
        "\n".join(
            [
                "event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session",
                "2020-04-01T10:00:00Z,view,101,200,electronics.audio,Acme,10.00,1,sess-1",
                "2020-04-01T10:00:30Z,view,102,200,electronics.audio,Acme,12.00,1,sess-1",
                "2020-04-01T10:01:00Z,cart,101,200,electronics.audio,Acme,10.00,1,sess-1",
                "2020-04-01T10:01:30Z,cart,102,200,electronics.audio,Acme,12.00,1,sess-1",
                "2020-04-01T10:02:00Z,purchase,101,200,electronics.audio,Acme,10.00,1,sess-1",
                "2020-04-01T10:02:30Z,purchase,102,200,electronics.audio,Acme,12.00,1,sess-1",
            ]
        ),
        encoding="utf-8",
    )


def main() -> None:
    """Run a local round-trip verification and print a concise summary."""

    with tempfile.TemporaryDirectory(prefix="ecommerce_lakehouse_verify_") as tmp:
        root = Path(tmp)
        sample_dir = root / "sample"
        sample_dir.mkdir(parents=True, exist_ok=True)
        build_sample_csv(sample_dir / "events.csv")

        config = load_config(
            bronze_uri=str(root / "bronze"),
            silver_uri=str(root / "silver"),
            gold_uri=str(root / "gold"),
            local_data_root=root / "local_data_root",
            manifest_root=root / "manifests",
            checkpoint_root=root / "checkpoints",
        )

        bronze = run_bronze_backfill(config, input_dir=sample_dir)
        silver = run_bronze_to_silver(config)
        gold = run_silver_to_gold(config)

        assert bronze.rows_written == 6
        assert silver.valid_rows == 6
        assert gold.rows_read == 6

        expected_tables = {
            "cohort_retention",
            "conversion_funnel",
            "product_affinity",
            "product_popularity",
            "repeat_purchase",
            "revenue_by_category",
            "rfm_segmentation",
            "session_summary",
            "time_to_conversion_distribution",
            "user_activity_summary",
        }
        actual_tables = {path.parents[1].name for path in gold.output_paths}
        missing = sorted(expected_tables - actual_tables)
        assert not missing, f"Missing expected Gold tables: {missing}"

        print(
            "verify-local ok",
            {
                "bronze_rows": bronze.rows_written,
                "silver_rows": silver.valid_rows,
                "gold_tables": sorted(actual_tables),
            },
        )


if __name__ == "__main__":
    main()
