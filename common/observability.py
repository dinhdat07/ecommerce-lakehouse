"""Shared manifest and metrics helpers for pipeline observability.

The Spark/Iceberg jobs and the lightweight local pipeline both use these
helpers to record consistent stage-level metadata without adding a separate
monitoring stack.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from common.constants import MANIFEST_ROOT
from common.manifests import RunManifest, write_manifest
from common.pipeline_metrics import emit_pipeline_metrics
from common.runtime import utc_now_iso


def stage_timer() -> float:
    """Return a high-resolution start timestamp for a pipeline stage."""

    return time.perf_counter()


def stage_elapsed_seconds(started_at: float) -> float:
    """Return elapsed wall-clock seconds from ``stage_timer``."""

    return round(time.perf_counter() - started_at, 3)


def record_stage_event(
    stage: str,
    run_id: str,
    *,
    status: str,
    metrics: dict[str, Any] | None = None,
    details: dict[str, Any] | None = None,
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
    started_at: str | None = None,
) -> Path:
    """Write a stage manifest and emit the corresponding structured metrics."""

    manifest = RunManifest(
        stage=stage,
        run_id=run_id,
        status=status,
        started_at=started_at or utc_now_iso(),
        inputs=list(inputs or []),
        outputs=list(outputs or []),
        metrics=dict(metrics or {}),
        details=dict(details or {}),
    )
    manifest_path = write_manifest(Path(os.getenv("MANIFEST_ROOT", MANIFEST_ROOT)), manifest)
    emit_pipeline_metrics(
        stage,
        run_id,
        dict(metrics or {}),
        details={
            "status": status,
            "manifest": str(manifest_path),
            **dict(details or {}),
        },
    )
    return manifest_path
