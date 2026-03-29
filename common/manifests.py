"""Run manifest utilities for pipeline observability and replay.

Each stage writes a manifest that records inputs, outputs, counts, and run
metadata. These manifests form the hidden operational layer used for debugging,
idempotency analysis, and future orchestration integration.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from common.runtime import ensure_directories, utc_now_iso


@dataclass
class RunManifest:
    """Structured metadata describing one pipeline stage execution.

    Inputs:
        stage: Pipeline stage name such as `bronze_backfill`.
        run_id: Unique execution identifier.
        status: Final run status.
        inputs: Source locations or files consumed by the run.
        outputs: Materialized stage outputs produced by the run.
        metrics: Count-based operational metrics.
        details: Additional implementation-specific metadata.

    Outputs:
        JSON-serializable representation persisted to the manifests directory.
    """

    stage: str
    run_id: str
    status: str = "started"
    started_at: str = field(default_factory=utc_now_iso)
    finished_at: str | None = None
    inputs: list[str] = field(default_factory=list)
    outputs: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    details: dict[str, Any] = field(default_factory=dict)


def write_manifest(manifest_root: Path, manifest: RunManifest) -> Path:
    """Persist a run manifest to disk and return the created file path.

    Inputs:
        manifest_root: Root directory for manifest storage.
        manifest: Manifest object describing the run.

    Outputs:
        Absolute manifest file path.

    Interactions:
        Called by all Bronze/Silver/Gold stage implementations after they finish.
    """

    stage_root = manifest_root / manifest.stage
    ensure_directories([stage_root])

    manifest.finished_at = manifest.finished_at or utc_now_iso()
    path = stage_root / f"{manifest.run_id}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(asdict(manifest), handle, indent=2, sort_keys=True)

    return path
