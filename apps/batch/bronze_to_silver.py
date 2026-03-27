"""Transform Bronze raw events into typed Silver canonical events."""

from __future__ import annotations

import argparse

from common.constants import BRONZE_PATH, SILVER_PATH
from common.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch transform Bronze -> Silver.")
    parser.add_argument("--bronze-path", default=BRONZE_PATH, help="Bronze source path.")
    parser.add_argument("--silver-path", default=SILVER_PATH, help="Silver destination path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.info("Starting bronze_to_silver with bronze=%s silver=%s", args.bronze_path, args.silver_path)
    logger.info("TODO: Read Bronze dataset with Spark.")
    logger.info("TODO: Apply canonical typing/cleaning and write Silver Iceberg table.")


if __name__ == "__main__":
    main()
