"""Repeat purchase Gold helper for the filesystem-backed local pipeline."""

from __future__ import annotations

from collections import defaultdict
from datetime import date


def _month_key(value: str) -> str:
    return value[:7]


def _month_start(month_value: str) -> date:
    year, month = map(int, month_value.split("-"))
    return date(year, month, 1)


def build_repeat_purchase_table(silver_rows: list[dict]) -> list[dict]:
    """Build monthly repeat-purchase rates."""

    purchases = [
        (row["user_id"], row["event_date"])
        for row in silver_rows
        if row.get("event_type") == "purchase" and row.get("user_id") is not None
    ]
    if not purchases:
        return []

    first_purchase_date: dict[int, str] = {}
    activity_month_users: dict[str, set[int]] = defaultdict(set)
    for user_id, event_date in sorted(purchases, key=lambda item: item[1]):
        first_purchase_date.setdefault(user_id, event_date)
        activity_month_users[_month_key(event_date)].add(user_id)

    results: list[dict] = []
    for activity_month in sorted(activity_month_users.keys()):
        purchasers = activity_month_users[activity_month]
        month_start = _month_start(activity_month)
        repeat_purchasers = {
            user_id
            for user_id in purchasers
            if date.fromisoformat(first_purchase_date[user_id]) < month_start
        }
        purchaser_count = len(purchasers)
        results.append(
            {
                "activity_month": activity_month,
                "purchasers": purchaser_count,
                "repeat_purchasers": len(repeat_purchasers),
                "repeat_purchase_rate": round(len(repeat_purchasers) / purchaser_count, 4) if purchaser_count else 0.0,
            }
        )
    return results
