"""Historical ingestion into the Bronze layer.

This module reads raw CSV or CSV.GZ files and writes append-only Bronze records
with source metadata, deterministic record hashes, and run manifests.
"""

from __future__ import annotations

import csv
import gzip
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from common.config import AppConfig, materialize_logical_uri
from common.manifests import RunManifest, write_manifest
from common.quality import build_record_hash
from common.runtime import build_run_id, utc_now_iso
from common.storage import write_jsonl


@dataclass(frozen=True)
class BronzeBackfillResult:
    """Summary of one Bronze backfill run."""

    run_id: str
    rows_written: int
    files_processed: int
    output_path: Path
    manifest_path: Path


def discover_input_files(input_dir: Path) -> list[Path]:
    """Return supported historical input files from a directory."""

    patterns = ("*.csv", "*.csv.gz")
    files: list[Path] = []
    for pattern in patterns:
        files.extend(sorted(input_dir.glob(pattern)))
    return sorted(set(files))


def _open_csv(path: Path):
    """Open a CSV or gzip-compressed CSV file for text reading."""

    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", newline="")
    return path.open("r", encoding="utf-8", newline="")


def iter_bronze_rows(input_files: list[Path], run_id: str, source_type: str) -> Iterator[dict]:
    """Yield Bronze records from the provided raw input files.

    Inputs:
        input_files: Historical files to ingest.
        run_id: Unique stage execution identifier.
        source_type: Logical source label such as `historical_csv`.

    Outputs:
        Iterator of append-only Bronze rows containing raw payload and source metadata.
    """

    ingested_at = utc_now_iso()
    for input_file in input_files:
        with _open_csv(input_file) as handle:
            reader = csv.DictReader(handle)
            for row_number, row in enumerate(reader, start=2):
                yield {
                    "run_id": run_id,
                    "ingested_at": ingested_at,
                    "source_type": source_type,
                    "source_file": input_file.name,
                    "source_row_number": row_number,
                    "record_hash": build_record_hash(
                        [input_file.name, row_number] + [row.get(key) for key in reader.fieldnames or []]
                    ),
                    "payload": row,
                }


def run_bronze_backfill(
    config: AppConfig,
    *,
    input_dir: Path,
    bronze_uri: str | None = None,
    source_type: str = "historical_csv",
) -> BronzeBackfillResult:
    """Ingest historical raw files into the Bronze layer.

    Inputs:
        config: Shared application settings.
        input_dir: Directory containing CSV or CSV.GZ files.
        bronze_uri: Optional logical Bronze destination override.
        source_type: Source label used in Bronze metadata.

    Outputs:
        `BronzeBackfillResult` describing the completed run.

    Interactions:
        Reads raw files from disk, writes append-only JSONL Bronze partitions,
        and persists a stage manifest for auditing and replay.
    """

    stage_name = "bronze_backfill"
    run_id = build_run_id("bronze")
    bronze_root = (
        config.bronze_local_path
        if bronze_uri is None
        else materialize_logical_uri(bronze_uri, config.local_data_root)
    )

    input_files = discover_input_files(input_dir)
    rows = list(iter_bronze_rows(input_files, run_id, source_type))

    output_path = bronze_root / f"run_id={run_id}" / "records.jsonl"
    rows_written = write_jsonl(output_path, rows)

    manifest = RunManifest(
        stage=stage_name,
        run_id=run_id,
        status="succeeded",
        inputs=[str(path) for path in input_files],
        outputs=[str(output_path)],
        metrics={"rows_written": rows_written, "files_processed": len(input_files)},
        details={"source_type": source_type},
    )
    manifest_path = write_manifest(config.manifest_root, manifest)

    return BronzeBackfillResult(
        run_id=run_id,
        rows_written=rows_written,
        files_processed=len(input_files),
        output_path=output_path,
        manifest_path=manifest_path,
    )
