from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any


def suggest_chart(columns: list[str], rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not columns or not rows:
        return None
    if len(columns) >= 2 and _is_time_key(rows[0].get(columns[0])) and _is_numeric(rows[0].get(columns[1])):
        return {"type": "line", "xKey": columns[0], "yKeys": [col for col in columns[1:] if _is_numeric(rows[0].get(col))]}
    if len(columns) >= 2 and _is_textual(rows[0].get(columns[0])) and _is_numeric(rows[0].get(columns[1])):
        return {"type": "bar", "xKey": columns[0], "yKeys": [columns[1]]}
    if len(columns) == 3 and _is_textual(rows[0].get(columns[0])) and _is_textual(rows[0].get(columns[1])) and _is_numeric(rows[0].get(columns[2])):
        return {"type": "heatmap", "xKey": columns[0], "seriesKey": columns[1], "valueKey": columns[2]}
    return None


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _is_time_key(value: Any) -> bool:
    return isinstance(value, (date, datetime)) or (isinstance(value, str) and "-" in value)


def _is_textual(value: Any) -> bool:
    return isinstance(value, str)
