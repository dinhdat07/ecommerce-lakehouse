"""Structured pipeline metrics for logs and downstream monitoring hooks.

Emit one JSON object per line with a stable prefix so log aggregators can index
``pipeline_metrics`` events without parsing free-form messages.
"""

from __future__ import annotations

import json
import os
from typing import Any

from common.logger import get_logger

logger = get_logger(__name__)


def emit_pipeline_metrics(
    stage: str,
    run_id: str,
    metrics: dict[str, Any],
    *,
    details: dict[str, Any] | None = None,
) -> None:
    """Log a single JSON metrics record for the given stage."""

    payload: dict[str, Any] = {
        "type": "pipeline_metrics",
        "stage": stage,
        "run_id": run_id,
        "metrics": metrics,
    }
    if details:
        payload["details"] = details
    logger.info("pipeline_metrics %s", json.dumps(payload, sort_keys=True))

    path = os.getenv("PIPELINE_METRICS_LOG_PATH", "").strip()
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
