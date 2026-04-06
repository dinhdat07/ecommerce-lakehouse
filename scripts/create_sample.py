"""Create local sample CSV data for development/testing."""

from __future__ import annotations

import argparse
import csv
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.constants import SAMPLE_DATA_DIR
from common.logger import get_logger

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic ecommerce sample events.")
    parser.add_argument("--rows", type=int, default=100, help="Number of rows to generate.")
    parser.add_argument(
        "--output",
        default=f"{SAMPLE_DATA_DIR}/events_sample.csv",
        help="Output CSV path.",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible output.")
    parser.add_argument(
        "--start-date",
        default="2020-04-01T10:00:00Z",
        help="Inclusive UTC start timestamp for generated events.",
    )
    parser.add_argument(
        "--days-span",
        type=int,
        default=0,
        help="If greater than zero, spread events randomly across this many days and sort by event_time.",
    )
    return parser.parse_args()


def generate_row(i: int, start_dt: datetime, days_span: int) -> dict[str, str]:
    event_types = ["view", "cart", "purchase"]
    event_type_weights = [0.8, 0.15, 0.05]
    categories = [
        ("electronics.smartphone", 200),
        ("electronics.audio", 210),
        ("apparel.shoes", 300),
        ("home.kitchen", 400),
    ]
    brands = ["Acme", "Globex", "Umbra", "Nova"]

    category_code, category_id = random.choice(categories)
    product_id = random.randint(1000, 9999)
    user_id = random.randint(1, 5000)
    session_id = f"sess-{random.randint(1, 20000):05d}"
    event_type = random.choices(event_types, weights=event_type_weights, k=1)[0]
    price = round(random.uniform(5.0, 1500.0), 2)
    if days_span > 0:
        offset_seconds = random.randint(0, max(days_span * 24 * 60 * 60 - 1, 0))
        event_time = start_dt + timedelta(seconds=offset_seconds)
    else:
        event_time = start_dt + timedelta(seconds=i * random.randint(1, 3))

    return {
        "event_time": event_time.isoformat().replace("+00:00", "Z"),
        "event_type": event_type,
        "product_id": str(product_id),
        "category_id": str(category_id),
        "category_code": category_code,
        "brand": random.choice(brands),
        "price": f"{price:.2f}",
        "user_id": str(user_id),
        "user_session": session_id,
    }


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    out_file = Path(args.output)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    start_dt = datetime.fromisoformat(args.start_date.replace("Z", "+00:00")).astimezone(timezone.utc)
    rows = [generate_row(i, start_dt, args.days_span) for i in range(args.rows)]
    if args.days_span > 0:
        rows.sort(key=lambda row: row["event_time"])

    fieldnames = list(rows[0].keys())
    with out_file.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Sample file created: %s (%d rows)", out_file, args.rows)


if __name__ == "__main__":
    main()
