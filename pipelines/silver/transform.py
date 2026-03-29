"""Bronze-to-Silver transformation logic.

The Silver layer standardizes event types, parses timestamps, coerces numeric
fields, enforces required columns, and separates invalid records into a
quarantine path for later inspection.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from common.config import AppConfig
from common.manifests import RunManifest, write_manifest
from common.quality import (
    build_event_identity,
    coerce_float,
    coerce_int,
    derive_event_date,
    missing_required_columns,
    normalize_nullable_text,
    parse_event_timestamp,
)
from common.runtime import build_run_id, utc_now_iso
from common.storage import iter_jsonl_files, read_jsonl, write_jsonl
from common.transforms import normalize_event


@dataclass(frozen=True)
class SilverTransformResult:
    """Summary of one Bronze-to-Silver run."""

    run_id: str
    valid_rows: int
    invalid_rows: int
    duplicate_rows: int
    output_paths: list[Path]
    quarantine_path: Path
    manifest_path: Path


def canonicalize_record(bronze_record: dict) -> tuple[dict | None, dict | None]:
    """Convert one Bronze record into a canonical Silver row or quarantine row.

    Outputs:
        `(valid_row, invalid_row)` where only one element is populated.
    """

    payload = dict(bronze_record.get("payload", {}))
    missing = missing_required_columns(payload)
    if missing:
        return None, {
            "record_hash": bronze_record.get("record_hash"),
            "rejected_at": utc_now_iso(),
            "reason": "missing_required_columns",
            "missing_columns": missing,
            "payload": payload,
        }

    normalized = normalize_event(payload)
    try:
        event_time = parse_event_timestamp(str(payload["event_time"]))
    except ValueError as exc:
        return None, {
            "record_hash": bronze_record.get("record_hash"),
            "rejected_at": utc_now_iso(),
            "reason": "invalid_timestamp",
            "error": str(exc),
            "payload": payload,
        }

    canonical = {
        "event_id": bronze_record.get("record_hash"),
        "record_hash": bronze_record.get("record_hash"),
        "run_id": bronze_record.get("run_id"),
        "source_file": bronze_record.get("source_file"),
        "source_row_number": bronze_record.get("source_row_number"),
        "ingested_at": bronze_record.get("ingested_at"),
        "event_time": event_time,
        "event_date": derive_event_date(event_time),
        "event_type": normalize_nullable_text(normalized.get("event_type")),
        "product_id": coerce_int(payload.get("product_id")),
        "category_id": coerce_int(payload.get("category_id")),
        "category_code": normalize_nullable_text(payload.get("category_code")),
        "brand": normalize_nullable_text(normalized.get("brand")),
        "price": coerce_float(normalized.get("price")),
        "user_id": coerce_int(payload.get("user_id")),
        "user_session": normalize_nullable_text(payload.get("user_session")),
    }
    canonical["dedupe_key"] = build_event_identity(canonical)
    return canonical, None


def run_bronze_to_silver(
    config: AppConfig,
    *,
    bronze_root: Path | None = None,
    silver_root: Path | None = None,
) -> SilverTransformResult:
    """Transform Bronze JSONL files into canonical Silver partitions."""

    stage_name = "bronze_to_silver"
    run_id = build_run_id("silver")
    bronze_root = bronze_root or config.bronze_local_path
    silver_root = silver_root or config.silver_local_path

    valid_by_date: dict[str, list[dict]] = {}
    quarantine_rows: list[dict] = []
    seen_keys: set[str] = set()
    duplicate_rows = 0

    for jsonl_file in iter_jsonl_files(bronze_root):
        for bronze_record in read_jsonl(jsonl_file):
            canonical, invalid = canonicalize_record(bronze_record)
            if invalid is not None:
                quarantine_rows.append(invalid)
                continue
            assert canonical is not None
            dedupe_key = canonical["dedupe_key"]
            if dedupe_key in seen_keys:
                duplicate_rows += 1
                continue
            seen_keys.add(dedupe_key)
            valid_by_date.setdefault(canonical["event_date"], []).append(canonical)

    output_paths: list[Path] = []
    valid_rows = 0
    for event_date, rows in sorted(valid_by_date.items()):
        output_path = silver_root / f"event_date={event_date}" / f"run_id={run_id}" / "records.jsonl"
        valid_rows += write_jsonl(output_path, rows)
        output_paths.append(output_path)

    quarantine_path = silver_root / "_quarantine" / f"run_id={run_id}" / "records.jsonl"
    invalid_rows = write_jsonl(quarantine_path, quarantine_rows)

    manifest = RunManifest(
        stage=stage_name,
        run_id=run_id,
        status="succeeded",
        inputs=[str(bronze_root)],
        outputs=[str(path) for path in output_paths] + [str(quarantine_path)],
        metrics={
            "valid_rows": valid_rows,
            "invalid_rows": invalid_rows,
            "duplicate_rows": duplicate_rows,
        },
    )
    manifest_path = write_manifest(config.manifest_root, manifest)

    return SilverTransformResult(
        run_id=run_id,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        duplicate_rows=duplicate_rows,
        output_paths=output_paths,
        quarantine_path=quarantine_path,
        manifest_path=manifest_path,
    )
