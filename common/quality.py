"""Data-quality rules shared by batch and streaming-friendly stages.

The functions in this module define the hidden contract for canonical events:
timestamp parsing, null normalization, type coercion, required-column checks,
and deterministic deduplication.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Iterable

from common.schemas import get_required_columns
from common.transforms import event_date_from_iso


def parse_event_timestamp(value: str) -> str:
    """Normalize an incoming event timestamp into canonical ISO UTC format.

    Inputs:
        value: Raw timestamp string from sample or historical datasets.

    Outputs:
        Canonical timestamp string formatted as `YYYY-MM-DDTHH:MM:SSZ`.

    Raises:
        ValueError: If the timestamp cannot be parsed.
    """

    candidates = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S UTC",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S%z",
    ]
    for fmt in candidates:
        try:
            parsed = datetime.strptime(value, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_nullable_text(value: object) -> str | None:
    """Trim text values and convert empty strings to `None`."""

    if value is None:
        return None
    text = str(value).strip()
    return text or None


def coerce_int(value: object) -> int | None:
    """Convert a raw value to `int`, returning `None` on blanks or invalid values."""

    text = normalize_nullable_text(value)
    if text is None:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def coerce_float(value: object) -> float | None:
    """Convert a raw value to `float`, returning `None` on blanks or invalid values."""

    text = normalize_nullable_text(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def missing_required_columns(row: dict[str, object]) -> list[str]:
    """Return missing required canonical columns for a raw event row."""

    missing: list[str] = []
    for column in get_required_columns():
        if column not in row:
            missing.append(column)
    return missing


def build_record_hash(values: Iterable[object]) -> str:
    """Create a deterministic SHA-256 hash from the provided values."""

    payload = "|".join("" if value is None else str(value) for value in values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_event_identity(row: dict[str, object]) -> str:
    """Build a stable identity hash for a canonical event row."""

    return build_record_hash(
        [
            row.get("event_time"),
            row.get("event_type"),
            row.get("product_id"),
            row.get("category_id"),
            row.get("category_code"),
            row.get("brand"),
            row.get("price"),
            row.get("user_id"),
            row.get("user_session"),
        ]
    )


def derive_event_date(canonical_timestamp: str) -> str:
    """Extract the event date from a canonical timestamp string."""

    return event_date_from_iso(canonical_timestamp)
