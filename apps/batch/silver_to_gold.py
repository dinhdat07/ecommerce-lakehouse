"""Aggregate Silver canonical events into Gold analytics tables."""

from __future__ import annotations

import argparse

from common.constants import GOLD_PATH, SILVER_PATH
from common.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch transform Silver -> Gold.")
    parser.add_argument("--silver-path", default=SILVER_PATH, help="Silver source path.")
    parser.add_argument("--gold-path", default=GOLD_PATH, help="Gold destination path.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger.info("Starting silver_to_gold with silver=%s gold=%s", args.silver_path, args.gold_path)
    logger.info("TODO: Build aggregated metrics (user/product/category/day).")
    logger.info("TODO: Write Gold Iceberg tables.")


if __name__ == "__main__":
    main()
