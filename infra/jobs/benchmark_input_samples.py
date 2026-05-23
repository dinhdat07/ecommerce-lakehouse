"""Reusable benchmark input samples.

Production ingestion still reads raw inputs directly.  This module is used only by
benchmark jobs to avoid scanning large gzip CSV files after a deterministic sample
has already been materialized under the benchmark S3A prefix.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

import batch_backfill_to_iceberg as bb
from common import benchmark_metrics as bm

SAFE_BENCHMARK_DELETE_PREFIXES = (
    "/tmp/benchmark",
    "/srv/ecommerce/benchmarks",
    "s3a://warehouse/benchmarks",
    "s3://warehouse/benchmarks",
)
DEFAULT_INPUT_SAMPLE_ROOT = "s3a://warehouse/benchmarks/input_samples"


class RemoteInputFile:
    """Path-like wrapper that preserves an S3A URI while exposing a file name."""

    def __init__(self, uri: str) -> None:
        self.uri = uri

    @property
    def name(self) -> str:
        return self.uri.rstrip("/").rsplit("/", 1)[-1]

    def stat(self):
        raise OSError("remote input metadata is not available through pathlib")

    def __str__(self) -> str:
        return self.uri


def parse_bool(value: str | bool | None) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "false").lower() in {"1", "true", "yes", "y", "on"}


def _path_join(base: str, child: str) -> str:
    return f"{base.rstrip('/')}/{child}"


def _hadoop_path(spark, uri: str):
    return spark._jvm.org.apache.hadoop.fs.Path(uri)


def path_exists(spark, uri: str) -> bool:
    if "://" not in uri:
        return Path(uri).exists()
    path = _hadoop_path(spark, uri)
    return bool(path.getFileSystem(spark._jsc.hadoopConfiguration()).exists(path))


def path_size_bytes(spark, uri: str) -> int:
    if "://" not in uri:
        local = Path(uri)
        if not local.exists():
            return 0
        return sum(candidate.stat().st_size for candidate in local.rglob("*") if candidate.is_file())
    path = _hadoop_path(spark, uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return 0
    return int(fs.getContentSummary(path).getLength())


def assert_safe_benchmark_path(path: str) -> None:
    normalized = path.rstrip("/")
    if not normalized or normalized in {"/", "/tmp", "/srv", "/srv/ecommerce", "s3a://warehouse", "s3://warehouse"}:
        raise ValueError(f"Refusing to delete unsafe benchmark path: {path}")
    if not any(normalized.startswith(prefix) for prefix in SAFE_BENCHMARK_DELETE_PREFIXES):
        raise ValueError(f"Refusing to delete non-benchmark path: {path}")


def delete_output_path(spark, path: str) -> None:
    assert_safe_benchmark_path(path)
    if "://" not in path:
        local_path = Path(path)
        if local_path.exists():
            import shutil

            shutil.rmtree(local_path)
        return

    target = _hadoop_path(spark, path)
    fs = target.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(target):
        fs.delete(target, True)


def _write_text(spark, path: str, payload: str) -> None:
    delete_output_path(spark, path)
    spark.sparkContext.parallelize([payload], 1).saveAsTextFile(path)


def _read_text(spark, path: str) -> str:
    return "\n".join(row["value"] for row in spark.read.text(path).collect())


def _file_descriptor(path: Path) -> dict[str, Any]:
    descriptor = {"name": path.name, "path": str(path)}
    try:
        stat = path.stat()
        descriptor.update({"size_bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    except OSError:
        descriptor.update({"size_bytes": None, "mtime_ns": None})
    return descriptor


def _is_remote_uri(path: str) -> bool:
    return "://" in path


def _discover_remote_input_files(spark, input_dir: str, start_month: str, end_month: str) -> tuple[list[RemoteInputFile], list[str]]:
    """Recursively discover historical CSV files under an S3A-compatible input prefix."""

    expected_months = bb.iter_months(start_month, end_month)
    selected_files: list[RemoteInputFile] = []
    available_months: set[str] = set()
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    root = jvm.org.apache.hadoop.fs.Path(input_dir.rstrip("/"))
    fs = root.getFileSystem(hadoop_conf)
    if not fs.exists(root):
        return [], expected_months

    iterator = fs.listFiles(root, True)
    while iterator.hasNext():
        status = iterator.next()
        uri = status.getPath().toString()
        name = uri.rsplit("/", 1)[-1]
        if not (name.endswith(".csv") or name.endswith(".csv.gz")):
            continue
        source_month = bb.extract_source_month(Path(name))
        if source_month is None:
            continue
        if start_month <= source_month <= end_month:
            selected_files.append(RemoteInputFile(uri))
            available_months.add(source_month)

    selected_files.sort(key=lambda item: item.uri)
    missing_months = [month for month in expected_months if month not in available_months]
    return selected_files, missing_months


def _git_or_env_version() -> str:
    if os.getenv("BENCHMARK_INPUT_SAMPLE_CODE_VERSION"):
        return os.environ["BENCHMARK_INPUT_SAMPLE_CODE_VERSION"]
    if os.getenv("GIT_COMMIT"):
        return os.environ["GIT_COMMIT"]
    return "unknown"


def _descriptor(args: argparse.Namespace, *, mode: str, input_files: list[Path]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "code_version": args.input_sample_code_version or _git_or_env_version(),
        "profile": args.profile,
        "mode": mode,
        "start_month": args.start_month,
        "end_month": args.end_month,
        "subset_enabled": parse_bool(args.subset_enabled),
        "subset_mode": args.subset_mode,
        "sample_fraction": float(args.sample_fraction),
        "sample_seed": int(args.sample_seed),
        "source_files": [_file_descriptor(path) for path in input_files],
    }


def _descriptor_hash(descriptor: dict[str, Any]) -> str:
    payload = json.dumps(descriptor, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _sample_paths(args: argparse.Namespace, descriptor_hash: str) -> dict[str, str]:
    root = (args.input_sample_root or DEFAULT_INPUT_SAMPLE_ROOT).rstrip("/")
    base = _path_join(_path_join(root, args.profile), descriptor_hash[:24])
    return {
        "root": root,
        "base_path": base,
        "data_path": _path_join(base, "data"),
        "metadata_path": _path_join(base, "metadata"),
    }


def _read_cached_sample(spark, paths: dict[str, str], descriptor_hash: str) -> tuple[Any, dict[str, Any]] | None:
    if not path_exists(spark, paths["data_path"]) or not path_exists(spark, paths["metadata_path"]):
        return None
    try:
        metadata = json.loads(_read_text(spark, paths["metadata_path"]))
    except Exception:
        return None
    if metadata.get("descriptor_hash") != descriptor_hash:
        return None
    dataframe = spark.read.parquet(paths["data_path"])
    metadata.update(
        {
            "enabled": True,
            "action": "reused",
            "metadata_matched": True,
            "data_path": paths["data_path"],
            "metadata_path": paths["metadata_path"],
            "storage_bytes": path_size_bytes(spark, paths["data_path"]),
        }
    )
    return dataframe, metadata


def _write_sample_cache(
    spark,
    dataframe,
    paths: dict[str, str],
    descriptor: dict[str, Any],
    descriptor_hash: str,
    *,
    action: str,
    input_row_count: int | None,
    bootstrap_path: str | None = None,
) -> tuple[Any, dict[str, Any]]:
    started = time.perf_counter()
    delete_output_path(spark, paths["base_path"])
    with bm.timed_action(spark, "input_sample_write_parquet", phase="input_sample", details={"path": paths["data_path"]}):
        dataframe.coalesce(int(os.getenv("BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS", "4"))).write.mode("overwrite").parquet(
            paths["data_path"]
        )
    staged = spark.read.parquet(paths["data_path"])
    benchmark_row_count = bm.timed_count(staged, "input_sample_benchmark_count", phase="input_sample")
    metadata = {
        "enabled": True,
        "action": action,
        "metadata_matched": True,
        "descriptor": descriptor,
        "descriptor_hash": descriptor_hash,
        "root": paths["root"],
        "base_path": paths["base_path"],
        "data_path": paths["data_path"],
        "metadata_path": paths["metadata_path"],
        "input_row_count": input_row_count,
        "benchmark_row_count": benchmark_row_count,
        "storage_bytes": path_size_bytes(spark, paths["data_path"]),
        "duration_seconds": round(time.perf_counter() - started, 3),
        "bootstrap_path": bootstrap_path,
    }
    _write_text(spark, paths["metadata_path"], json.dumps(metadata, sort_keys=True))
    return staged, metadata


def _apply_optional_fraction_subset(dataframe, args: argparse.Namespace):
    if parse_bool(args.subset_enabled) and args.subset_mode == "fraction":
        return dataframe.sample(False, float(args.sample_fraction), int(args.sample_seed))
    return dataframe


def _stage_run_input(spark, dataframe, args: argparse.Namespace) -> tuple[Any, dict[str, Any]]:
    staging_enabled = parse_bool(args.staging_enabled)
    metadata: dict[str, Any] = {
        "staging_enabled": staging_enabled,
        "staging_path": args.staging_path if staging_enabled else None,
        "staging_action": "disabled",
        "staging_seconds": 0.0,
        "staging_storage_bytes": 0,
    }
    if not staging_enabled:
        return dataframe, metadata
    if not args.staging_path:
        raise SystemExit("--staging-path is required when --staging-enabled=true")
    started = time.perf_counter()
    delete_output_path(spark, args.staging_path)
    staging_partitions = int(
        os.getenv(
            "BENCHMARK_STAGING_WRITE_PARTITIONS",
            os.getenv("BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS", os.getenv("BENCHMARK_WRITE_PARTITIONS", "0")),
        )
    )
    staged_input = dataframe.repartition(staging_partitions) if staging_partitions > 0 else dataframe
    with bm.timed_action(
        spark,
        "raw_gzip_stage_to_parquet_write",
        phase="raw_ingest_cost",
        details={"path": args.staging_path, "write_partitions": staging_partitions or None},
    ):
        staged_input.write.mode("overwrite").format("parquet").save(args.staging_path)
    staged = spark.read.parquet(args.staging_path)
    metadata["staging_seconds"] = round(time.perf_counter() - started, 3)
    metadata["staging_storage_bytes"] = path_size_bytes(spark, args.staging_path)
    metadata["staging_action"] = "created"
    return staged, metadata


def _read_reusable_staging(spark, args: argparse.Namespace) -> tuple[Any, dict[str, Any]] | None:
    if not parse_bool(args.staging_enabled) or not parse_bool(args.staging_reuse_existing):
        return None
    if not args.staging_path:
        raise SystemExit("--staging-path is required when --staging-reuse-existing=true")
    if not path_exists(spark, args.staging_path):
        return None
    started = time.perf_counter()
    staged = spark.read.parquet(args.staging_path)
    benchmark_count = bm.timed_count(staged, "staged_parquet_reuse_count", phase="input_resolution", details={"path": args.staging_path})
    metadata = {
        "staging_enabled": True,
        "staging_path": args.staging_path,
        "staging_action": "reused",
        "staging_seconds": round(time.perf_counter() - started, 3),
        "staging_storage_bytes": path_size_bytes(spark, args.staging_path),
        "benchmark_count": benchmark_count,
    }
    return staged, metadata


def _discover_dataset(spark, args: argparse.Namespace) -> tuple[str, list[Any], list[str], str | None, str]:
    if _is_remote_uri(args.input_dir):
        input_files, missing_months = _discover_remote_input_files(
            spark,
            args.input_dir,
            args.start_month,
            args.end_month,
        )
        input_dir_label = args.input_dir
    else:
        input_dir = Path(args.input_dir)
        input_files, missing_months = bb.discover_input_files(input_dir, args.start_month, args.end_month)
        input_dir_label = str(input_dir)
    if args.mode == "full" and not input_files:
        raise SystemExit(f"No input files under {input_dir_label} for {args.start_month}..{args.end_month}")

    if args.mode == "demo" or (args.mode == "auto" and not input_files):
        sample_path = Path(args.sample_file)
        if not sample_path.exists():
            raise SystemExit(f"Demo sample file not found: {sample_path}")
        return "demo", [sample_path], [], args.demo_source_month, f"bench-demo-{args.demo_source_month.replace('-', '')}"

    return "full", input_files, missing_months, None, f"bench-full-{args.start_month}-{args.end_month}"


def resolve_benchmark_input(spark, args: argparse.Namespace) -> dict[str, Any]:
    """Resolve benchmark input, preferring a reusable sampled Parquet cache."""

    started = time.perf_counter()
    mode, input_files, missing_months, source_month_override, batch_run_id = _discover_dataset(spark, args)
    input_sample_enabled = parse_bool(args.input_sample_enabled) and parse_bool(args.subset_enabled) and args.subset_mode == "fraction"
    input_sample_metadata: dict[str, Any] = {"enabled": input_sample_enabled, "action": "disabled"}

    reusable_staging = _read_reusable_staging(spark, args)
    if reusable_staging is not None:
        bronze_batch, staging = reusable_staging
        benchmark_count = int(staging["benchmark_count"])
        return {
            "mode": mode,
            "input_files": input_files,
            "missing_months": missing_months,
            "bronze_batch": bronze_batch,
            "input_row_count": int(args.input_row_count_override) if args.input_row_count_override else benchmark_count,
            "benchmark_row_count": benchmark_count,
            "raw_read_seconds": 0.0,
            "staging": staging,
            "input_sample": input_sample_metadata,
        }

    if input_sample_enabled:
        descriptor = _descriptor(args, mode=mode, input_files=input_files)
        descriptor_hash = _descriptor_hash(descriptor)
        paths = _sample_paths(args, descriptor_hash)
        cached = _read_cached_sample(spark, paths, descriptor_hash)
        if cached is not None:
            bronze_batch, input_sample_metadata = cached
            return {
                "mode": mode,
                "input_files": input_files,
                "missing_months": missing_months,
                "bronze_batch": bronze_batch,
                "input_row_count": input_sample_metadata.get("input_row_count"),
                "benchmark_row_count": int(input_sample_metadata.get("benchmark_row_count") or bronze_batch.count()),
                "raw_read_seconds": 0.0,
                "staging": {"staging_enabled": False, "staging_path": None, "staging_seconds": 0.0, "staging_storage_bytes": 0},
                "input_sample": input_sample_metadata,
            }

        if args.input_sample_bootstrap_path:
            if not path_exists(spark, args.input_sample_bootstrap_path):
                raise SystemExit(f"Input sample bootstrap path does not exist: {args.input_sample_bootstrap_path}")
            bootstrap_df = spark.read.parquet(args.input_sample_bootstrap_path)
            input_count = int(args.input_row_count_override) if args.input_row_count_override else None
            bronze_batch, input_sample_metadata = _write_sample_cache(
                spark,
                bootstrap_df,
                paths,
                descriptor,
                descriptor_hash,
                action="created_from_bootstrap",
                input_row_count=input_count,
                bootstrap_path=args.input_sample_bootstrap_path,
            )
            return {
                "mode": mode,
                "input_files": input_files,
                "missing_months": missing_months,
                "bronze_batch": bronze_batch,
                "input_row_count": input_count,
                "benchmark_row_count": int(input_sample_metadata["benchmark_row_count"]),
                "raw_read_seconds": 0.0,
                "staging": {"staging_enabled": False, "staging_path": None, "staging_seconds": 0.0, "staging_storage_bytes": 0},
                "input_sample": input_sample_metadata,
            }

        input_bronze_batch = bb.read_bronze_batch(
            spark,
            input_files,
            batch_run_id=batch_run_id,
            source_month_override=source_month_override,
        )
        input_count = (
            int(args.input_row_count_override)
            if args.input_row_count_override
            else bm.timed_count(input_bronze_batch, "raw_input_count", phase="raw_ingest_cost")
        )
        sampled = _apply_optional_fraction_subset(input_bronze_batch, args)
        bronze_batch, input_sample_metadata = _write_sample_cache(
            spark,
            sampled,
            paths,
            descriptor,
            descriptor_hash,
            action="created",
            input_row_count=input_count,
        )
        return {
            "mode": mode,
            "input_files": input_files,
            "missing_months": missing_months,
            "bronze_batch": bronze_batch,
            "input_row_count": input_count,
            "benchmark_row_count": int(input_sample_metadata["benchmark_row_count"]),
            "raw_read_seconds": round(time.perf_counter() - started, 3),
            "staging": {"staging_enabled": False, "staging_path": None, "staging_seconds": 0.0, "staging_storage_bytes": 0},
            "input_sample": input_sample_metadata,
        }

    input_bronze_batch = bb.read_bronze_batch(
        spark,
        input_files,
        batch_run_id=batch_run_id,
        source_month_override=source_month_override,
    )
    input_count = (
        int(args.input_row_count_override)
        if args.input_row_count_override
        else bm.timed_count(input_bronze_batch, "raw_input_count", phase="raw_ingest_cost")
    )
    selected = _apply_optional_fraction_subset(input_bronze_batch, args)
    bronze_batch, staging = _stage_run_input(spark, selected, args)
    benchmark_count = bm.timed_count(bronze_batch, "benchmark_input_count", phase="input_resolution")
    return {
        "mode": mode,
        "input_files": input_files,
        "missing_months": missing_months,
        "bronze_batch": bronze_batch,
        "input_row_count": input_count,
        "benchmark_row_count": benchmark_count,
        "raw_read_seconds": round(time.perf_counter() - started, 3),
        "staging": staging,
        "input_sample": input_sample_metadata,
    }


def add_input_sample_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--input-sample-enabled", default=os.getenv("BENCHMARK_INPUT_SAMPLE_ENABLED", "true"))
    parser.add_argument("--input-sample-root", default=os.getenv("BENCHMARK_INPUT_SAMPLE_ROOT", DEFAULT_INPUT_SAMPLE_ROOT))
    parser.add_argument("--input-sample-code-version", default=os.getenv("BENCHMARK_INPUT_SAMPLE_CODE_VERSION", ""))
    parser.add_argument("--input-sample-bootstrap-path", default=os.getenv("BENCHMARK_INPUT_SAMPLE_BOOTSTRAP_PATH", ""))
    parser.add_argument("--input-row-count-override", default=os.getenv("BENCHMARK_INPUT_ROW_COUNT_OVERRIDE", ""))
    parser.add_argument("--staging-reuse-existing", default=os.getenv("BENCHMARK_STAGING_REUSE_EXISTING", "false"))
