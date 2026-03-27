"""Backfill raw historical files into Bronze storage."""

from __future__ import annotations

import argparse

from common.constants import BRONZE_PATH, RAW_DATA_DIR
from common.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill historical raw data to Bronze.")
    parser.add_argument("--input-dir", default=RAW_DATA_DIR, help="Directory containing raw .csv.gz files.")
    parser.add_argument("--bronze-path", default=BRONZE_PATH, help="Bronze destination path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.info("Starting backfill_bronze with input_dir=%s bronze_path=%s", args.input_dir, args.bronze_path)
    logger.info("TODO: Initialize Spark session and ingest monthly CSV.GZ files.")
    logger.info("TODO: Write append-only raw data to Bronze storage.")


if __name__ == "__main__":
    main()
