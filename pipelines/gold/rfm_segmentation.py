"""RFM segmentation Gold helper for the filesystem-backed local pipeline."""

from __future__ import annotations

from datetime import date, timedelta


def _parse_event_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def _quintile_score(order_index: int, total_rows: int) -> int:
    if total_rows <= 0:
        return 3
    return 5 - min(4, (order_index * 5) // total_rows)


def _segment_name(r_score: int, f_score: int, m_score: int) -> str:
    if r_score >= 4 and f_score >= 4 and m_score >= 4:
        return "champions"
    if r_score >= 4 and f_score <= 2:
        return "new_customers"
    if r_score <= 2 and f_score >= 3:
        return "at_risk"
    if r_score <= 2 and f_score <= 2:
        return "hibernating"
    if f_score >= 4:
        return "loyal_customers"
    return "potential"


def build_rfm_segmentation_table(silver_rows: list[dict], *, lookback_days: int = 90) -> list[dict]:
    """Build trailing-window RFM segmentation from purchase history."""

    all_dates = [_parse_event_date(row["event_date"]) for row in silver_rows]
    if not all_dates:
        return []

    as_of_date = max(all_dates)
    window_start = as_of_date - timedelta(days=lookback_days - 1)
    purchases = [
        (row["user_id"], _parse_event_date(row["event_date"]), float(row.get("price") or 0.0))
        for row in silver_rows
        if row.get("event_type") == "purchase"
        and row.get("user_id") is not None
        and window_start <= _parse_event_date(row["event_date"]) <= as_of_date
    ]
    if not purchases:
        return []

    by_user: dict[int, dict[str, object]] = {}
    for user_id, event_date, price in purchases:
        current = by_user.setdefault(
            user_id,
            {"last_purchase_date": event_date, "frequency_90d": 0, "monetary_90d": 0.0},
        )
        current["frequency_90d"] = int(current["frequency_90d"]) + 1
        current["monetary_90d"] = float(current["monetary_90d"]) + price
        if event_date > current["last_purchase_date"]:
            current["last_purchase_date"] = event_date

    user_ids = sorted(by_user.keys())
    recencies = [(user_id, (as_of_date - by_user[user_id]["last_purchase_date"]).days) for user_id in user_ids]
    frequencies = [(user_id, int(by_user[user_id]["frequency_90d"])) for user_id in user_ids]
    monetaries = [(user_id, round(float(by_user[user_id]["monetary_90d"]), 2)) for user_id in user_ids]
    total_rows = len(user_ids)

    r_score_by_user = {
        user_id: _quintile_score(index, total_rows)
        for index, (user_id, _value) in enumerate(sorted(recencies, key=lambda item: item[1]))
    }
    f_score_by_user = {
        user_id: _quintile_score(index, total_rows)
        for index, (user_id, _value) in enumerate(sorted(frequencies, key=lambda item: item[1], reverse=True))
    }
    m_score_by_user = {
        user_id: _quintile_score(index, total_rows)
        for index, (user_id, _value) in enumerate(sorted(monetaries, key=lambda item: item[1], reverse=True))
    }

    results: list[dict] = []
    for user_id in user_ids:
        recency_days = next(value for key, value in recencies if key == user_id)
        frequency_90d = next(value for key, value in frequencies if key == user_id)
        monetary_90d = next(value for key, value in monetaries if key == user_id)
        r_score = r_score_by_user[user_id]
        f_score = f_score_by_user[user_id]
        m_score = m_score_by_user[user_id]
        results.append(
            {
                "as_of_date": as_of_date.isoformat(),
                "user_id": user_id,
                "recency_days": recency_days,
                "frequency_90d": frequency_90d,
                "monetary_90d": monetary_90d,
                "r_score": r_score,
                "f_score": f_score,
                "m_score": m_score,
                "rfm_segment": _segment_name(r_score, f_score, m_score),
            }
        )
    return results
