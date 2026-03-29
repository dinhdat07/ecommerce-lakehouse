"""CLI entrypoint for historical Bronze backfills.

This file keeps CLI parsing and operator-facing logging separate from the stage
implementation in `pipelines.bronze.backfill`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.config import load_config
from common.constants import BRONZE_PATH, RAW_DATA_DIR
from common.logger import get_logger
from pipelines.bronze.backfill import run_bronze_backfill

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill historical raw data to Bronze.")
    parser.add_argument("--input-dir", default=RAW_DATA_DIR, help="Directory containing raw .csv.gz files.")
    parser.add_argument("--bronze-path", default=BRONZE_PATH, help="Bronze destination path.")
    return parser.parse_args()


def main() -> None:
    """Run the Bronze historical backfill job and log the resulting manifest."""

    args = parse_args()
    logger.info("Starting backfill_bronze with input_dir=%s bronze_path=%s", args.input_dir, args.bronze_path)
    config = load_config(bronze_uri=args.bronze_path)
    result = run_bronze_backfill(config, input_dir=Path(args.input_dir), bronze_uri=args.bronze_path)
    logger.info(
        "Bronze backfill finished run_id=%s rows_written=%s files_processed=%s output=%s manifest=%s",
        result.run_id,
        result.rows_written,
        result.files_processed,
        result.output_path,
        result.manifest_path,
    )


if __name__ == "__main__":
    main()
