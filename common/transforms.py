"""Reusable transformation helpers for batch and streaming jobs."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable

from common.schemas import get_required_columns


def validate_required_columns(columns: Iterable[str]) -> None:
    """Raise ValueError if required canonical columns are missing."""
    missing = sorted(set(get_required_columns()) - set(columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def normalize_event(record: Dict[str, object]) -> Dict[str, object]:
    """Return a minimally normalized event record.

    This function is intentionally small for scaffold phase and can be extended
    by Spark UDF/native transforms later.
    """
    out = dict(record)
    if out.get("event_type") is not None:
        out["event_type"] = str(out["event_type"]).strip().lower()

    if out.get("brand") is not None:
        out["brand"] = str(out["brand"]).strip()

    if isinstance(out.get("price"), str):
        try:
            out["price"] = float(out["price"])
        except ValueError:
            out["price"] = None

    return out


def event_date_from_iso(event_time: str) -> str:
    """Extract YYYY-MM-DD from an ISO-like datetime string."""
    dt = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    return dt.date().isoformat()
