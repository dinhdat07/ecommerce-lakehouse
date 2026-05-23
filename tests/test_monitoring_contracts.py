from __future__ import annotations

import json
from pathlib import Path

from common.observability import record_stage_event


def test_record_stage_event_writes_manifest_and_metrics_log(tmp_path: Path, monkeypatch):
    metrics_log = tmp_path / "pipeline_metrics.jsonl"
    manifest_root = tmp_path / "manifests"
    monkeypatch.setenv("MANIFEST_ROOT", str(manifest_root))
    monkeypatch.setenv("PIPELINE_METRICS_LOG_PATH", str(metrics_log))

    manifest_path = record_stage_event(
        "silver_publish_iceberg",
        "run-123",
        status="succeeded",
        metrics={"rows_written": 10, "dq_passed": True},
        details={"affected_dates": ["2020-04-01"]},
        inputs=["bronze_events"],
        outputs=["silver_events"],
        started_at="2026-01-01T00:00:00Z",
    )

    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["stage"] == "silver_publish_iceberg"
    assert manifest["status"] == "succeeded"
    assert manifest["metrics"]["rows_written"] == 10
    assert manifest["details"]["affected_dates"] == ["2020-04-01"]

    metrics_lines = metrics_log.read_text(encoding="utf-8").strip().splitlines()
    assert len(metrics_lines) == 1
    payload = json.loads(metrics_lines[0])
    assert payload["type"] == "pipeline_metrics"
    assert payload["stage"] == "silver_publish_iceberg"
    assert payload["metrics"]["rows_written"] == 10
    assert payload["details"]["status"] == "succeeded"


def test_record_stage_event_supports_failure_status(tmp_path: Path, monkeypatch):
    metrics_log = tmp_path / "pipeline_metrics.jsonl"
    manifest_root = tmp_path / "manifests"
    monkeypatch.setenv("MANIFEST_ROOT", str(manifest_root))
    monkeypatch.setenv("PIPELINE_METRICS_LOG_PATH", str(metrics_log))

    manifest_path = record_stage_event(
        "gold_refresh_iceberg",
        "run-456",
        status="failed",
        metrics={"duration_seconds": 1.25},
        details={"failure": "DQ gate failed"},
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["details"]["failure"] == "DQ gate failed"
