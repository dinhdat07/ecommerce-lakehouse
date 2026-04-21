"""Time-to-conversion Gold helper for the filesystem-backed local pipeline."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone


def _parse_event_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _time_bucket(seconds: float) -> str:
    if seconds <= 60:
        return "0-60s"
    if seconds <= 300:
        return "61-300s"
    if seconds <= 1800:
        return "301-1800s"
    if seconds <= 86400:
        return "1801-86400s"
    return ">86400s"


def build_time_to_conversion_distribution_table(silver_rows: list[dict]) -> list[dict]:
    """Build a histogram of first-view to first-purchase time within each session."""

    session_events: dict[tuple[int | None, str | None, str], list[dict]] = defaultdict(list)
    for row in sorted(silver_rows, key=lambda item: (item["event_time"], item["event_id"])):
        session_key = (row.get("user_id"), row.get("user_session"), row["event_date"])
        session_events[session_key].append(row)

    bucket_counts: dict[tuple[str, str], int] = defaultdict(int)
    for (_user_id, _session, event_date), events in session_events.items():
        first_view = None
        first_purchase = None
        for event in events:
            event_time = _parse_event_time(str(event["event_time"]))
            if event.get("event_type") == "view" and first_view is None:
                first_view = event_time
            if event.get("event_type") == "purchase" and first_purchase is None:
                first_purchase = event_time
        if first_view is None or first_purchase is None or first_purchase < first_view:
            continue
        bucket_counts[(event_date, _time_bucket((first_purchase - first_view).total_seconds()))] += 1

    return [
        {"event_date": event_date, "time_bucket": bucket, "conversions": conversions}
        for (event_date, bucket), conversions in sorted(bucket_counts.items())
    ]
