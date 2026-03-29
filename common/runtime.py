"""Shared runtime helpers for timestamps, run identifiers, and filesystem setup.

The functions in this module are intentionally lightweight so every pipeline
stage can record consistent operational metadata without pulling in external
dependencies.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from uuid import uuid4


def utc_now() -> datetime:
    """Return the current UTC timestamp."""

    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    """Return the current UTC timestamp formatted as an ISO 8601 string."""

    return utc_now().isoformat().replace("+00:00", "Z")


def build_run_id(prefix: str) -> str:
    """Create a short unique run identifier with a stage-specific prefix."""

    return f"{prefix}-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"


def ensure_directories(paths: Iterable[Path]) -> None:
    """Create the provided directories if they do not already exist."""

    for path in paths:
        path.mkdir(parents=True, exist_ok=True)
