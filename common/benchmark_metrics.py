"""Lightweight action-level instrumentation for Spark benchmark jobs."""

from __future__ import annotations

import contextlib
import csv
import json
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

from common.runtime import utc_now_iso

_METRICS: list[dict[str, Any]] = []
_JSONL_PATH: Path | None = None
_CSV_PATH: Path | None = None
_RUN_ID = ""
_PROFILE = ""


def configure(*, run_id: str = "", profile: str = "", jsonl_path: str = "", csv_path: str = "") -> None:
    """Set output destinations for action metrics collected in this process."""

    global _JSONL_PATH, _CSV_PATH, _RUN_ID, _PROFILE
    _RUN_ID = run_id
    _PROFILE = profile
    _JSONL_PATH = Path(jsonl_path) if jsonl_path else None
    _CSV_PATH = Path(csv_path) if csv_path else None
    _METRICS.clear()
    for path in (_JSONL_PATH, _CSV_PATH):
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)


def metrics() -> list[dict[str, Any]]:
    """Return a copy of action metrics captured so far."""

    return list(_METRICS)


def _stage_summary(spark: Any, group_id: str) -> dict[str, Any]:
    if spark is None:
        return {"job_ids": [], "stage_ids": [], "task_count": 0}
    try:
        sc = spark.sparkContext
        tracker = spark.sparkContext.statusTracker()
        job_ids = sorted(int(job_id) for job_id in tracker.getJobIdsForGroup(group_id))
        stage_ids: set[int] = set()
        task_count = 0
        for job_id in job_ids:
            job_info = tracker.getJobInfo(job_id)
            if job_info is None:
                continue
            for stage_id in list(job_info.stageIds):
                stage_ids.add(int(stage_id))
        for stage_id in sorted(stage_ids):
            stage_info = tracker.getStageInfo(stage_id)
            if stage_info is not None:
                task_count += int(stage_info.numTasks)
        executor_ids: list[str] = []
        try:
            executor_status = sc._jsc.sc().getExecutorMemoryStatus()
            executor_ids = [str(item) for item in list(executor_status.keySet())]
        except Exception:
            executor_ids = []
        return {
            "job_ids": job_ids,
            "stage_ids": sorted(stage_ids),
            "task_count": task_count,
            "application_id": getattr(sc, "applicationId", ""),
            "application_name": getattr(sc, "appName", ""),
            "executor_ids": executor_ids,
        }
    except Exception as exc:  # pragma: no cover - Spark tracker is best-effort.
        return {"job_ids": [], "stage_ids": [], "task_count": 0, "tracker_error": str(exc)}


def record_action(
    *,
    action: str,
    phase: str,
    duration_seconds: float,
    status: str,
    details: dict[str, Any] | None = None,
    spark_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Append one structured action metric and mirror it to JSONL."""

    payload = {
        "run_id": _RUN_ID,
        "profile": _PROFILE,
        "action": action,
        "phase": phase,
        "status": status,
        "duration_seconds": round(duration_seconds, 3),
        "recorded_at_utc": utc_now_iso(),
        **dict(spark_summary or {}),
        "details": dict(details or {}),
    }
    _METRICS.append(payload)
    if _JSONL_PATH is not None:
        with _JSONL_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")
    return payload


@contextlib.contextmanager
def timed_action(
    spark: Any,
    action: str,
    *,
    phase: str,
    details: dict[str, Any] | None = None,
) -> Iterator[None]:
    """Time a Spark action and capture job/stage/task ids via job groups."""

    group_id = f"bench-{action}-{uuid.uuid4().hex[:10]}"
    if spark is not None:
        spark.sparkContext.setJobGroup(group_id, f"{phase}:{action}", interruptOnCancel=True)
    started = time.perf_counter()
    status = "succeeded"
    try:
        yield
    except Exception as exc:
        status = "failed"
        merged_details = {**dict(details or {}), "error": str(exc)}
        record_action(
            action=action,
            phase=phase,
            duration_seconds=time.perf_counter() - started,
            status=status,
            details=merged_details,
            spark_summary=_stage_summary(spark, group_id),
        )
        raise
    else:
        record_action(
            action=action,
            phase=phase,
            duration_seconds=time.perf_counter() - started,
            status=status,
            details=details,
            spark_summary=_stage_summary(spark, group_id),
        )
    finally:
        if spark is not None:
            spark.sparkContext.setJobGroup("", "")


def timed_count(dataframe: Any, action: str, *, phase: str, details: dict[str, Any] | None = None) -> int:
    """Count a DataFrame with action-level instrumentation."""

    spark = dataframe.sparkSession
    with timed_action(spark, action, phase=phase, details=details):
        return int(dataframe.count())


def flush_csv(path: str | None = None) -> None:
    """Write all captured action metrics to CSV."""

    target = Path(path) if path else _CSV_PATH
    if target is None:
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run_id",
        "profile",
        "phase",
        "action",
        "status",
        "duration_seconds",
        "task_count",
        "stage_ids",
        "job_ids",
        "details",
        "recorded_at_utc",
    ]
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in _METRICS:
            row = dict(item)
            row["stage_ids"] = json.dumps(row.get("stage_ids", []), sort_keys=True)
            row["job_ids"] = json.dumps(row.get("job_ids", []), sort_keys=True)
            row["details"] = json.dumps(row.get("details", {}), sort_keys=True)
            writer.writerow({key: row.get(key, "") for key in fieldnames})
