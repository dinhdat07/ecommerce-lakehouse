"""File-backed storage helpers for Bronze, Silver, and Gold outputs.

The repository currently materializes lakehouse stages as partitioned JSONL/CSV
files. The layout is intentionally close to object-storage conventions so the
pipeline can later move to MinIO and Iceberg with minimal contract changes.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable, Iterator, Sequence

from common.runtime import ensure_directories


def write_jsonl(path: Path, rows: Iterable[dict]) -> int:
    """Write dictionaries to a JSONL file and return the row count."""

    ensure_directories([path.parent])
    count = 0
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
            count += 1
    return count


def append_jsonl(path: Path, rows: Iterable[dict]) -> int:
    """Append dictionaries to a JSONL file and return the number of added rows."""

    ensure_directories([path.parent])
    count = 0
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
            count += 1
    return count


def read_jsonl(path: Path) -> Iterator[dict]:
    """Yield dictionaries from a JSONL file."""

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def iter_jsonl_files(root: Path) -> Iterator[Path]:
    """Yield JSONL files under the provided root in deterministic order."""

    if not root.exists():
        return iter(())

    files = sorted(path for path in root.rglob("*.jsonl") if path.is_file())
    return iter(files)


def write_csv(path: Path, rows: Sequence[dict], fieldnames: Sequence[str]) -> int:
    """Write rows to a CSV file and return the row count."""

    ensure_directories([path.parent])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)
