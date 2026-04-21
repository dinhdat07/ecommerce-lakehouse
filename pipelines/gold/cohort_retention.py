"""Cohort retention Gold helper for the filesystem-backed local pipeline."""

from __future__ import annotations

from collections import defaultdict


def _month_key(value: str) -> str:
    return value[:7]


def _month_diff(start_month: str, end_month: str) -> int:
    start_year, start_month_num = map(int, start_month.split("-"))
    end_year, end_month_num = map(int, end_month.split("-"))
    return (end_year - start_year) * 12 + (end_month_num - start_month_num)


def build_cohort_retention_table(silver_rows: list[dict]) -> list[dict]:
    """Build monthly retention based on each user's first purchase month."""

    purchases = [
        row for row in silver_rows if row.get("event_type") == "purchase" and row.get("user_id") is not None
    ]
    if not purchases:
        return []

    first_purchase_month: dict[int, str] = {}
    activity_by_user: dict[int, set[str]] = defaultdict(set)
    for row in sorted(purchases, key=lambda item: (item["event_time"], item["event_id"])):
        user_id = row["user_id"]
        activity_month = _month_key(row["event_date"])
        activity_by_user[user_id].add(activity_month)
        first_purchase_month.setdefault(user_id, activity_month)

    cohort_users: dict[str, set[int]] = defaultdict(set)
    retention_users: dict[tuple[str, int], set[int]] = defaultdict(set)
    for user_id, cohort_month in first_purchase_month.items():
        cohort_users[cohort_month].add(user_id)
        for activity_month in activity_by_user[user_id]:
            period_offset = _month_diff(cohort_month, activity_month)
            if period_offset >= 0:
                retention_users[(cohort_month, period_offset)].add(user_id)

    results: list[dict] = []
    for (cohort_month, period_offset), users in sorted(retention_users.items()):
        cohort_size = len(cohort_users[cohort_month])
        active_users = len(users)
        results.append(
            {
                "cohort_month": cohort_month,
                "period_offset": period_offset,
                "cohort_users": cohort_size,
                "active_users": active_users,
                "retention_rate": round(active_users / cohort_size, 4) if cohort_size else 0.0,
            }
        )
    return results
