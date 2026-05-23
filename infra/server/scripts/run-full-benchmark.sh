#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=infra/server/scripts/common.sh
source "${SCRIPT_DIR}/common.sh"
# shellcheck source=infra/server/spark/submit-common.sh
source "${SERVER_ROOT_DIR}/spark/submit-common.sh"

STORAGE_ENV_FILE="${STORAGE_ENV_FILE:-${SERVER_ENV_DIR}/storage-minio.env}"
BENCHMARK_ENV_FILE="${BENCHMARK_ENV_FILE:-${SERVER_ENV_DIR}/benchmark.env}"
load_server_env "${STORAGE_ENV_FILE}" "${BENCHMARK_ENV_FILE}"

BENCHMARK_PROFILE="${BENCHMARK_PROFILE:-smoke}"
RUN_LABEL="${BENCHMARK_RUN_LABEL:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_ROOT="${BENCHMARK_OUTPUT_ROOT}/${RUN_LABEL}"
BENCHMARK_STAGING_ROOT="${BENCHMARK_STAGING_ROOT:-s3a://warehouse/benchmarks/staging/${RUN_LABEL}}"
BENCHMARK_INPUT_SAMPLE_ROOT="${BENCHMARK_INPUT_SAMPLE_ROOT:-s3a://warehouse/benchmarks/input_samples}"
if [[ -n "${BENCHMARK_PARQUET_WAREHOUSE:-}" ]]; then
  BENCHMARK_PARQUET_ROOT="${BENCHMARK_PARQUET_WAREHOUSE%/}/${RUN_LABEL}"
else
  BENCHMARK_PARQUET_ROOT="s3a://warehouse/benchmarks/parquet/${RUN_LABEL}"
fi
GIT_COMMIT="${GIT_COMMIT:-$(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || echo unknown)}"
export BENCHMARK_PROFILE BENCHMARK_RUN_ID="${RUN_LABEL}" BENCHMARK_CACHE_DATAFRAMES="${BENCHMARK_CACHE_DATAFRAMES:-false}" GIT_COMMIT
mkdir -p "${RUN_ROOT}"
chmod 0777 "${RUN_ROOT}" 2>/dev/null || true

apply_benchmark_profile() {
  case "${BENCHMARK_PROFILE}" in
    smoke)
      BENCHMARK_MODE="${BENCHMARK_MODE:-full}"
      BENCHMARK_SUBSET_ENABLED="${BENCHMARK_SUBSET_ENABLED:-true}"
      BENCHMARK_SUBSET_MODE="${BENCHMARK_SUBSET_MODE:-fraction}"
      BENCHMARK_SAMPLE_FRACTION="${BENCHMARK_SAMPLE_FRACTION:-0.001}"
      STREAM_BENCHMARK_MICROBATCH_SIZE="${STREAM_BENCHMARK_MICROBATCH_SIZE:-50000}"
      PROCESSING_GOLD_REFRESH_MODE="${PROCESSING_GOLD_REFRESH_MODE:-affected_dates}"
      ;;
    simple-small)
      BENCHMARK_MODE="${BENCHMARK_MODE:-full}"
      BENCHMARK_SUBSET_ENABLED="${BENCHMARK_SUBSET_ENABLED:-true}"
      BENCHMARK_SUBSET_MODE="${BENCHMARK_SUBSET_MODE:-fraction}"
      BENCHMARK_SAMPLE_FRACTION="${BENCHMARK_SAMPLE_FRACTION:-0.025}"
      STREAM_BENCHMARK_MICROBATCH_SIZE="${STREAM_BENCHMARK_MICROBATCH_SIZE:-250000}"
      PROCESSING_GOLD_REFRESH_MODE="${PROCESSING_GOLD_REFRESH_MODE:-affected_dates}"
      ;;
    simple-medium)
      BENCHMARK_MODE="${BENCHMARK_MODE:-full}"
      BENCHMARK_SUBSET_ENABLED="${BENCHMARK_SUBSET_ENABLED:-true}"
      BENCHMARK_SUBSET_MODE="${BENCHMARK_SUBSET_MODE:-fraction}"
      BENCHMARK_SAMPLE_FRACTION="${BENCHMARK_SAMPLE_FRACTION:-0.05}"
      STREAM_BENCHMARK_MICROBATCH_SIZE="${STREAM_BENCHMARK_MICROBATCH_SIZE:-500000}"
      PROCESSING_GOLD_REFRESH_MODE="${PROCESSING_GOLD_REFRESH_MODE:-affected_dates}"
      ;;
    stress-lite)
      BENCHMARK_MODE="${BENCHMARK_MODE:-full}"
      BENCHMARK_SUBSET_ENABLED="${BENCHMARK_SUBSET_ENABLED:-true}"
      BENCHMARK_SUBSET_MODE="${BENCHMARK_SUBSET_MODE:-fraction}"
      BENCHMARK_SAMPLE_FRACTION="${BENCHMARK_SAMPLE_FRACTION:-0.25}"
      STREAM_BENCHMARK_MICROBATCH_SIZE="${STREAM_BENCHMARK_MICROBATCH_SIZE:-1000000}"
      PROCESSING_GOLD_REFRESH_MODE="${PROCESSING_GOLD_REFRESH_MODE:-affected_dates}"
      ;;
    profile_2month_batch_streaming_sample)
      BENCHMARK_MODE="${BENCHMARK_MODE:-full}"
      SYSTEM_START_MONTH="${SYSTEM_START_MONTH:-2019-10}"
      SYSTEM_END_MONTH="${SYSTEM_END_MONTH:-2019-11}"
      STREAM_START_MONTH="${STREAM_START_MONTH:-2019-12}"
      STREAM_END_MONTH="${STREAM_END_MONTH:-2019-12}"
      SYSTEM_BENCHMARK_SUBSET_ENABLED="${SYSTEM_BENCHMARK_SUBSET_ENABLED:-false}"
      SYSTEM_BENCHMARK_SUBSET_MODE="${SYSTEM_BENCHMARK_SUBSET_MODE:-month_range}"
      SYSTEM_BENCHMARK_MONTH_CHUNKED="${SYSTEM_BENCHMARK_MONTH_CHUNKED:-true}"
      STREAM_BENCHMARK_SUBSET_ENABLED="${STREAM_BENCHMARK_SUBSET_ENABLED:-true}"
      STREAM_BENCHMARK_SUBSET_MODE="${STREAM_BENCHMARK_SUBSET_MODE:-fraction}"
      STREAM_BENCHMARK_SAMPLE_FRACTION="${STREAM_BENCHMARK_SAMPLE_FRACTION:-${PROFILE_2MONTH_STREAM_SAMPLE_FRACTION:-0.10}}"
      BENCHMARK_PARQUET_BASELINE_ENABLED="${BENCHMARK_PARQUET_BASELINE_ENABLED:-false}"
      SYSTEM_GOLD_REFRESH_MODE="${SYSTEM_GOLD_REFRESH_MODE:-affected_dates}"
      PROCESSING_GOLD_REFRESH_MODE="${PROCESSING_GOLD_REFRESH_MODE:-affected_dates}"
      PROCESSING_BATCH_ENABLED="${PROCESSING_BATCH_ENABLED:-false}"
      STREAM_BENCHMARK_MICROBATCH_SIZE="${STREAM_BENCHMARK_MICROBATCH_SIZE:-250000}"
      BENCHMARK_DISK_FREE_THRESHOLD_GB="${BENCHMARK_DISK_FREE_THRESHOLD_GB:-20}"
      BENCHMARK_REQUIRE_SPARK_LOCAL_DIRS="${BENCHMARK_REQUIRE_SPARK_LOCAL_DIRS:-true}"
      SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-16}"
      SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-16}"
      BENCHMARK_WRITE_PARTITIONS="${BENCHMARK_WRITE_PARTITIONS:-16}"
      BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS="${BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS:-16}"
      ;;
    full)
      if [[ "${BENCHMARK_ALLOW_FULL:-false}" != "true" ]]; then
        fail "full profile is disabled by default. Re-run with BENCHMARK_ALLOW_FULL=true BENCHMARK_PROFILE=full after capacity review."
      fi
      BENCHMARK_MODE="${BENCHMARK_MODE:-full}"
      BENCHMARK_SUBSET_ENABLED="${BENCHMARK_SUBSET_ENABLED:-false}"
      BENCHMARK_SUBSET_MODE="${BENCHMARK_SUBSET_MODE:-month_range}"
      STREAM_BENCHMARK_MICROBATCH_SIZE="${STREAM_BENCHMARK_MICROBATCH_SIZE:-1000000}"
      PROCESSING_GOLD_REFRESH_MODE="${PROCESSING_GOLD_REFRESH_MODE:-full}"
      ;;
    *)
      fail "unknown BENCHMARK_PROFILE=${BENCHMARK_PROFILE}; expected smoke|simple-small|simple-medium|stress-lite|profile_2month_batch_streaming_sample|full"
      ;;
  esac
}

log_disk_usage() {
  local label="$1"
  log "Disk usage ${label}:"
  df -h / /srv/ecommerce /srv/ecommerce/spark-tmp /tmp /var/lib/docker 2>/dev/null || true
  du -xsh /srv/ecommerce /srv/ecommerce/spark-tmp /tmp /var/lib/docker 2>/dev/null || true
}

log_remote_disk_usage() {
  local label="$1"
  local remote_hosts=( ${BENCHMARK_SPARK_CLEANUP_HOSTS:-} )
  local host
  for host in "${remote_hosts[@]}"; do
    log "Remote disk usage ${label} on ${host}:"
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "df -h / /srv/ecommerce /srv/ecommerce/spark-tmp /tmp /var/lib/docker 2>/dev/null || true; du -xsh /srv/ecommerce /srv/ecommerce/spark-tmp /tmp /var/lib/docker 2>/dev/null || true" || true
  done
}

free_gb_for_root() {
  df -Pk / | awk 'NR == 2 { printf "%.0f\n", $4 / 1024 / 1024 }'
}

check_benchmark_free_space() {
  local threshold_gb="${BENCHMARK_DISK_FREE_THRESHOLD_GB:-20}"
  local free_gb
  free_gb="$(free_gb_for_root)"
  if [[ "${free_gb}" =~ ^[0-9]+$ && "${free_gb}" -lt "${threshold_gb}" ]]; then
    log "Disk free-space threshold exceeded on local host: ${free_gb}GB < ${threshold_gb}GB"
    return 1
  fi

  local hosts=( ${BENCHMARK_SPARK_CLEANUP_HOSTS:-} )
  local host
  for host in "${hosts[@]}"; do
    free_gb="$(ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "df -Pk / | awk 'NR == 2 { printf \"%.0f\\n\", \$4 / 1024 / 1024 }'" 2>/dev/null || echo 0)"
    if [[ "${free_gb}" =~ ^[0-9]+$ && "${free_gb}" -lt "${threshold_gb}" ]]; then
      log "Disk free-space threshold exceeded on ${host}: ${free_gb}GB < ${threshold_gb}GB"
      return 1
    fi
  done
}

spark_local_dirs_match() {
  local actual="$1"
  local expected="$2"
  [[ ",${actual}," == *",${expected},"* ]]
}

validate_spark_local_dirs_on_host() {
  local host="$1"
  local expected="${SPARK_LOCAL_DIR:-/srv/ecommerce/spark-tmp}"
  local container="${2:-spark-worker}"
  local actual=""
  local write_status=""
  if [[ "${host}" == "local" ]]; then
    actual="$(docker exec "${container}" sh -lc 'printf "%s" "${SPARK_LOCAL_DIRS:-}"' 2>/dev/null || true)"
    write_status="$(docker exec "${container}" sh -lc 'worker_dir="${SPARK_WORKER_DIR:-/srv/ecommerce/spark-tmp/worker}"; mkdir -p "${worker_dir}" 2>/dev/null || true; touch "${worker_dir}/.write-test" && rm -f "${worker_dir}/.write-test" && printf write-ok || printf write-fail' 2>/dev/null || true)"
  else
    actual="$(ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "docker exec ${container} sh -lc 'printf \"%s\" \"\${SPARK_LOCAL_DIRS:-}\"'" 2>/dev/null || true)"
    write_status="$(ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "docker exec ${container} sh -lc 'worker_dir=\"\${SPARK_WORKER_DIR:-/srv/ecommerce/spark-tmp/worker}\"; mkdir -p \"\${worker_dir}\" 2>/dev/null || true; touch \"\${worker_dir}/.write-test\" && rm -f \"\${worker_dir}/.write-test\" && printf write-ok || printf write-fail'" 2>/dev/null || true)"
  fi
  if spark_local_dirs_match "${actual}" "${expected}" && [[ "${write_status}" == "write-ok" ]]; then
    log "Spark ${container} on ${host} has SPARK_LOCAL_DIRS=${actual} and writable worker dir"
    return 0
  fi
  if ! spark_local_dirs_match "${actual}" "${expected}"; then
    log "Spark ${container} on ${host} has SPARK_LOCAL_DIRS=${actual:-<unset>}; expected ${expected}. Standalone executors may otherwise spill to /tmp."
  fi
  if [[ "${write_status}" != "write-ok" ]]; then
    log "Spark ${container} on ${host} cannot write SPARK_WORKER_DIR=/srv/ecommerce/spark-tmp/worker; fix permissions with chmod 1777 /srv/ecommerce/spark-tmp /srv/ecommerce/spark-tmp/worker on that node."
  fi
  return 1
}

validate_spark_local_dirs() {
  local required="${BENCHMARK_REQUIRE_SPARK_LOCAL_DIRS:-false}"
  local ok=true
  mkdir -p "${SPARK_LOCAL_DIR:-/srv/ecommerce/spark-tmp}" 2>/dev/null || true
  validate_spark_local_dirs_on_host local spark-worker || ok=false
  local remote_hosts=( ${BENCHMARK_SPARK_CLEANUP_HOSTS:-} )
  local host
  for host in "${remote_hosts[@]}"; do
    validate_spark_local_dirs_on_host "${host}" spark-worker || ok=false
  done
  if [[ "${ok}" != "true" && "${required}" == "true" ]]; then
    fail "Spark worker local dirs are not pinned to ${SPARK_LOCAL_DIR:-/srv/ecommerce/spark-tmp}. Restart Spark workers with SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIR:-/srv/ecommerce/spark-tmp}, or set BENCHMARK_REQUIRE_SPARK_LOCAL_DIRS=false only after accepting /tmp spill risk."
  fi
}

check_spark_idle() {
  local master_host="${SPARK_MASTER#spark://}"
  master_host="${master_host%%:*}"
  local master_status_url="${SPARK_MASTER_STATUS_URL:-http://${master_host}:8080/json/}"
  python3 - <<'PY' "${master_status_url}"
import json
import sys
import urllib.request

url = sys.argv[1]
try:
    with urllib.request.urlopen(url, timeout=5) as response:
        status = json.load(response)
except Exception as exc:
    raise SystemExit(f"cannot query Spark master status at {url}: {exc}")

active_apps = status.get("activeapps") or []
active_drivers = status.get("activedrivers") or []
cores_used = int(status.get("coresused") or 0)
memory_used = int(status.get("memoryused") or 0)
if active_apps or active_drivers or cores_used or memory_used:
    raise SystemExit(
        "Spark cluster is not idle: "
        f"activeapps={len(active_apps)} activedrivers={len(active_drivers)} "
        f"coresused={cores_used} memoryused={memory_used}"
    )
print(
    "Spark idle preflight passed: "
    f"aliveworkers={status.get('aliveworkers')} coresused={cores_used} memoryused={memory_used}"
)
PY
}

cleanup_spark_temp_dirs() {
  log "Preparing to clean benchmark Spark temp directories only"
  log_disk_usage "before spark-temp cleanup"
  log "Targets: Spark temp/spill and completed worker app dirs only; raw data, MinIO, and Iceberg data are excluded"
  mkdir -p /srv/ecommerce/spark-tmp /srv/ecommerce/spark-tmp/worker 2>/dev/null || true
  chmod 1777 /srv/ecommerce/spark-tmp /srv/ecommerce/spark-tmp/worker 2>/dev/null || true
  docker exec spark-master mkdir -p /srv/ecommerce/spark-tmp 2>/dev/null || true
  find /srv/ecommerce/spark-tmp -mindepth 1 -maxdepth 1 ! -name worker -exec rm -rf -- {} + 2>/dev/null || true
  find /srv/ecommerce/spark-tmp/worker -mindepth 1 -maxdepth 1 \( -name 'app-*' -o -name 'spark-*' -o -name 'blockmgr-*' \) -exec rm -rf -- {} + 2>/dev/null || true
  chmod 1777 /srv/ecommerce/spark-tmp /srv/ecommerce/spark-tmp/worker 2>/dev/null || true
  find /tmp -maxdepth 1 -name 'spark-*' -exec rm -rf -- {} + 2>/dev/null || true
  find /tmp -maxdepth 1 -name 'blockmgr-*' -exec rm -rf -- {} + 2>/dev/null || true
  find /tmp -maxdepth 1 -name 'hive-*' -exec rm -rf -- {} + 2>/dev/null || true

  local remote_hosts=( ${BENCHMARK_SPARK_CLEANUP_HOSTS:-} )
  local host
  for host in "${remote_hosts[@]}"; do
    log "Cleaning benchmark Spark temp directories on ${host}: temp/spill and completed worker app dirs only"
    ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "mkdir -p /srv/ecommerce/spark-tmp /srv/ecommerce/spark-tmp/worker 2>/dev/null || true; chmod 1777 /srv/ecommerce/spark-tmp /srv/ecommerce/spark-tmp/worker 2>/dev/null || true; docker exec spark-worker mkdir -p /srv/ecommerce/spark-tmp 2>/dev/null || true; df -h / /srv/ecommerce/spark-tmp /tmp 2>/dev/null || true; find /srv/ecommerce/spark-tmp -mindepth 1 -maxdepth 1 ! -name worker -exec rm -rf -- {} + 2>/dev/null || true; find /srv/ecommerce/spark-tmp/worker -mindepth 1 -maxdepth 1 \( -name 'app-*' -o -name 'spark-*' -o -name 'blockmgr-*' \) -exec rm -rf -- {} + 2>/dev/null || true; chmod 1777 /srv/ecommerce/spark-tmp /srv/ecommerce/spark-tmp/worker 2>/dev/null || true; find /tmp -maxdepth 1 \( -name 'spark-*' -o -name 'blockmgr-*' -o -name 'hive-*' \) -exec rm -rf -- {} + 2>/dev/null || true" || true
  done
  log_disk_usage "after spark-temp cleanup"
}

check_benchmark_disk_usage() {
  local threshold="${BENCHMARK_DISK_WATCHDOG_THRESHOLD_PCT:-90}"
  local usage
  usage="$(df -P / | awk 'NR == 2 { gsub("%", "", $5); print $5 }')"
  if [[ "${usage}" =~ ^[0-9]+$ && "${usage}" -ge "${threshold}" ]]; then
    log "Disk watchdog threshold exceeded on local host: ${usage}% >= ${threshold}%"
    return 1
  fi

  local hosts=( ${BENCHMARK_SPARK_CLEANUP_HOSTS:-} )
  local host
  for host in "${hosts[@]}"; do
    usage="$(ssh -o BatchMode=yes -o StrictHostKeyChecking=no "${host}" "df -P / | awk 'NR == 2 { gsub(\"%\", \"\", \$5); print \$5 }'" 2>/dev/null || echo 0)"
    if [[ "${usage}" =~ ^[0-9]+$ && "${usage}" -ge "${threshold}" ]]; then
      log "Disk watchdog threshold exceeded on ${host}: ${usage}% >= ${threshold}%"
      return 1
    fi
  done
  check_benchmark_free_space
}

run_with_disk_watchdog() {
  local label="$1"
  shift
  local interval="${BENCHMARK_DISK_WATCHDOG_INTERVAL_SECONDS:-30}"
  log "Starting ${label} with disk watchdog threshold ${BENCHMARK_DISK_WATCHDOG_THRESHOLD_PCT:-90}%"
  "$@" &
  local job_pid=$!
  while kill -0 "${job_pid}" 2>/dev/null; do
    if ! check_benchmark_disk_usage; then
      log "Stopping ${label} because disk usage is too high"
      kill "${job_pid}" 2>/dev/null || true
      wait "${job_pid}" 2>/dev/null || true
      return 99
    fi
    sleep "${interval}"
  done
  wait "${job_pid}"
}

write_metadata() {
  local status="$1"
  python3 - <<'PY' "${RUN_ROOT}/metadata.json" "${RUN_LABEL}" "${BENCHMARK_PROFILE}" "${BENCHMARK_MODE}" "${BENCHMARK_INPUT_DIR}" "${ICEBERG_WAREHOUSE}" "${BENCHMARK_SUBSET_ENABLED}" "${BENCHMARK_SUBSET_MODE}" "${SYSTEM_START_MONTH}" "${SYSTEM_END_MONTH}" "${STREAM_START_MONTH}" "${STREAM_END_MONTH}" "${BENCHMARK_SAMPLE_FRACTION:-}" "${BENCHMARK_SAMPLE_SEED:-42}" "${BENCHMARK_STAGING_ENABLED:-true}" "${BENCHMARK_STAGING_ROOT}" "${BENCHMARK_PARQUET_ROOT}" "${status}" "${BENCHMARK_INPUT_SAMPLE_ENABLED:-true}" "${BENCHMARK_INPUT_SAMPLE_ROOT}" "${PROCESSING_GOLD_REFRESH_MODE:-full}"
import json
import os
import subprocess
import sys
from datetime import datetime, timezone

(
    path,
    run_label,
    profile,
    mode,
    input_dir,
    warehouse,
    subset_enabled,
    subset_mode,
    system_start,
    system_end,
    stream_start,
    stream_end,
    sample_fraction,
    sample_seed,
    staging_enabled,
    staging_root,
    parquet_root,
    status,
    input_sample_enabled,
    input_sample_root,
    processing_gold_refresh_mode,
) = sys.argv[1:]
try:
    git_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
except Exception:
    git_commit = "unknown"
try:
    disk_usage = subprocess.check_output(["df", "-h", "/", "/srv/ecommerce/spark-tmp", "/tmp"], text=True)
except Exception as exc:
    disk_usage = f"unavailable: {exc}"
payload = {
    "run_id": run_label,
    "run_label": run_label,
    "git_commit": git_commit,
    "profile": profile,
    "benchmark_mode": mode,
    "input_dir": input_dir,
    "iceberg_warehouse": warehouse,
    "parquet_warehouse": parquet_root,
    "subset_enabled": subset_enabled.lower() == "true",
    "subset_mode": subset_mode,
    "sample_fraction": float(sample_fraction) if sample_fraction else None,
    "sample_seed": int(sample_seed),
    "system_benchmark_start_month": system_start,
    "system_benchmark_end_month": system_end,
    "stream_benchmark_start_month": stream_start,
    "stream_benchmark_end_month": stream_end,
    "staging_enabled": staging_enabled.lower() == "true",
    "staging_root": staging_root,
    "input_sample_enabled": input_sample_enabled.lower() == "true",
    "input_sample_root": input_sample_root,
    "processing_gold_refresh_mode": processing_gold_refresh_mode,
    "parquet_baseline_enabled": os.getenv("BENCHMARK_PARQUET_BASELINE_ENABLED", "true").lower() in {"1", "true", "yes", "y"},
    "processing_batch_enabled": os.getenv("PROCESSING_BATCH_ENABLED", "true").lower() in {"1", "true", "yes", "y"},
    "system_benchmark_month_chunked": os.getenv("SYSTEM_BENCHMARK_MONTH_CHUNKED", "false").lower() in {"1", "true", "yes", "y"},
    "profile_scope": {
        "historical_batch_months": [system_start, system_end],
        "streaming_replay_months": [stream_start, stream_end],
        "streaming_sample_fraction": float(os.getenv("STREAM_BENCHMARK_SAMPLE_FRACTION", sample_fraction or "0")),
        "description": "profile_2month_batch_streaming_sample runs full historical batch for 2019-10..2019-11 and sampled streaming replay for 2019-12.",
    },
    "status": status,
    "disk_usage_snapshot": disk_usage,
    "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
PY
}

month_list() {
  python3 - <<'PY' "$1" "$2"
import sys
from datetime import date
start, end = sys.argv[1:3]
y, m = map(int, start.split("-"))
ey, em = map(int, end.split("-"))
months = []
while (y, m) <= (ey, em):
    months.append(f"{y:04d}-{m:02d}")
    m += 1
    if m == 13:
        y += 1
        m = 1
print(" ".join(months))
PY
}

run_system_benchmark_phase() {
  local label="$1"
  local start_month="$2"
  local end_month="$3"
  local drop_existing="$4"
  local output_json="$5"
  local output_csv="$6"
  local staging_suffix="$7"
  run_with_disk_watchdog "${label}" spark_submit_iceberg_job "${ROOT_DIR}/infra/jobs/benchmark_system_lakehouse_vs_parquet.py" \
    --mode "${BENCHMARK_MODE:-full}" \
    --input-dir "${BENCHMARK_INPUT_DIR}" \
    --sample-file "${BENCHMARK_SAMPLE_FILE:-/opt/ecommerce-lakehouse/data/sample/events_sample_100k.csv}" \
    --start-month "${start_month}" \
    --end-month "${end_month}" \
    --subset-enabled "${SYSTEM_BENCHMARK_SUBSET_ENABLED}" \
    --subset-mode "${SYSTEM_BENCHMARK_SUBSET_MODE}" \
    --sample-fraction "${SYSTEM_BENCHMARK_SAMPLE_FRACTION}" \
    --sample-seed "${BENCHMARK_SAMPLE_SEED}" \
    --staging-enabled "${BENCHMARK_STAGING_ENABLED}" \
    --staging-path "${BENCHMARK_STAGING_ROOT}/${staging_suffix}" \
    --staging-reuse-existing "${BENCHMARK_STAGING_REUSE_EXISTING:-false}" \
    --input-sample-enabled "${BENCHMARK_INPUT_SAMPLE_ENABLED}" \
    --input-sample-root "${BENCHMARK_INPUT_SAMPLE_ROOT}" \
    --input-sample-bootstrap-path "${BENCHMARK_INPUT_SAMPLE_BOOTSTRAP_PATH:-}" \
    --input-row-count-override "${BENCHMARK_INPUT_ROW_COUNT_OVERRIDE:-}" \
    --warehouse-uri "${ICEBERG_WAREHOUSE}" \
    --parquet-warehouse "${BENCHMARK_PARQUET_ROOT}" \
    --parquet-baseline-enabled "${BENCHMARK_PARQUET_BASELINE_ENABLED}" \
    --gold-refresh-mode "${SYSTEM_GOLD_REFRESH_MODE}" \
    --iceberg-drop-existing-tables "${drop_existing}" \
    --profile "${BENCHMARK_PROFILE}" \
    --run-id "${RUN_LABEL}" \
    --output-json "${output_json}" \
    --output-csv "${output_csv}"
}

aggregate_system_chunk_results() {
  python3 - <<'PY' "${RUN_ROOT}" "$@"
import csv
import json
import pathlib
import sys
run_root = pathlib.Path(sys.argv[1])
paths = [pathlib.Path(p) for p in sys.argv[2:]]
chunks = [json.loads(p.read_text()) for p in paths]
if not chunks:
    raise SystemExit("no chunk results to aggregate")
base = chunks[-1].copy()
iceberg_chunks = [c.get("iceberg", {}) for c in chunks]
input_rows = sum(int(c.get("dataset", {}).get("input_row_count") or 0) for c in chunks)
benchmark_rows = sum(int(c.get("dataset", {}).get("benchmark_row_count") or 0) for c in chunks)
bronze_rows = sum(int(c.get("bronze_row_count") or 0) for c in chunks)
iceberg_total = round(sum(float(i.get("total_runtime_seconds") or 0) for i in iceberg_chunks), 3)
iceberg_ingest = round(sum(float(i.get("ingestion_seconds") or 0) for i in iceberg_chunks), 3)
iceberg_transform = round(sum(float(i.get("transformation_seconds") or 0) for i in iceberg_chunks), 3)
base["dataset"] = dict(base.get("dataset", {}), label="month_chunked:" + ",".join(c.get("dataset", {}).get("start_month", "?") for c in chunks), start_month=chunks[0].get("dataset", {}).get("start_month"), end_month=chunks[-1].get("dataset", {}).get("end_month"), input_row_count=input_rows, benchmark_row_count=benchmark_rows, chunks=[c.get("dataset", {}) for c in chunks])
base["subset"] = dict(base.get("subset", {}), input_row_count=input_rows, benchmark_row_count=benchmark_rows)
base["bronze_row_count"] = bronze_rows
base["system_benchmark_month_chunked"] = True
base["chunk_result_files"] = [str(p) for p in paths]
base["iceberg"] = dict(base.get("iceberg", {}), ingestion_seconds=iceberg_ingest, transformation_seconds=iceberg_transform, total_runtime_seconds=iceberg_total, throughput_rows_per_second=round(bronze_rows / iceberg_total, 3) if iceberg_total > 0 else 0, chunk_results=[c.get("iceberg", {}) for c in chunks])
base["parquet"] = base.get("parquet", {"skipped_reason": "disabled_by_profile_or_env"})
out_json = run_root / "system_benchmark.json"
out_json.write_text(json.dumps(base, indent=2, sort_keys=True) + "\n")
out_csv = run_root / "system_benchmark.csv"
flat = {
    "benchmark_mode": base.get("benchmark_mode"),
    "dataset_label": base.get("dataset", {}).get("label"),
    "bronze_row_count": bronze_rows,
    "input_row_count": input_rows,
    "benchmark_row_count": benchmark_rows,
    "iceberg_ingestion_seconds": iceberg_ingest,
    "iceberg_transformation_seconds": iceberg_transform,
    "iceberg_storage_bytes": base.get("iceberg", {}).get("storage_bytes"),
    "iceberg_throughput_rows_per_second": base.get("iceberg", {}).get("throughput_rows_per_second"),
    "system_benchmark_month_chunked": True,
}
with out_csv.open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=list(flat))
    writer.writeheader()
    writer.writerow(flat)
print(out_json)
PY
}

write_run_summary() {
  python3 - <<'PY' "${RUN_ROOT}"
import json
import pathlib
import sys

run_root = pathlib.Path(sys.argv[1])
metadata = json.loads((run_root / "metadata.json").read_text()) if (run_root / "metadata.json").exists() else {}
system = json.loads((run_root / "system_benchmark.json").read_text()) if (run_root / "system_benchmark.json").exists() else {}
processing = json.loads((run_root / "processing_model_benchmark.json").read_text()) if (run_root / "processing_model_benchmark.json").exists() else {}
lines = [
    f"# Benchmark Run {metadata.get('run_id', run_root.name)}",
    "",
    f"- Profile: {metadata.get('profile')}",
    f"- Git commit: {metadata.get('git_commit')}",
    f"- Source/month: {metadata.get('system_benchmark_start_month')}..{metadata.get('system_benchmark_end_month')}",
    f"- Sample fraction: {metadata.get('sample_fraction')}",
]
if system:
    subset = system.get("subset", {})
    input_sample = system.get("input_sample", {})
    lines += [
        f"- System input rows/sampled rows: {subset.get('input_row_count')} / {subset.get('benchmark_row_count')}",
        f"- Input sample action: {input_sample.get('action')}",
        f"- Input sample path: {input_sample.get('data_path')}",
        f"- Iceberg throughput rows/sec: {system.get('iceberg', {}).get('throughput_rows_per_second')}",
        f"- Parquet throughput rows/sec: {system.get('parquet', {}).get('throughput_rows_per_second')}",
        f"- Iceberg storage bytes: {system.get('iceberg', {}).get('storage_bytes')}",
        f"- Parquet storage bytes: {system.get('parquet', {}).get('storage_bytes')}",
    ]
if processing:
    lines += [
        f"- Processing Gold refresh mode: {processing.get('gold_refresh_mode')}",
        f"- Batch wall seconds: {processing.get('batch', {}).get('wall_seconds')}",
        f"- Streaming wall seconds: {processing.get('streaming', {}).get('wall_seconds')}",
        f"- Streaming microbatches: {processing.get('streaming', {}).get('microbatches')}",
    ]
lines += ["", "## Limitations", "- Subset benchmarks are not full historical capacity proof.", "- Disk watchdog protects root disk but does not replace Spark UI/storage monitoring."]
(run_root / "run_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
}

apply_benchmark_profile

BENCHMARK_SUBSET_ENABLED="${BENCHMARK_SUBSET_ENABLED:-true}"
BENCHMARK_SUBSET_MODE="${BENCHMARK_SUBSET_MODE:-fraction}"
SYSTEM_START_MONTH="${SYSTEM_START_MONTH:-${BENCHMARK_START_MONTH:-2019-10}}"
SYSTEM_END_MONTH="${SYSTEM_END_MONTH:-${BENCHMARK_END_MONTH:-2019-10}}"
STREAM_START_MONTH="${STREAM_START_MONTH:-${STREAM_BENCHMARK_START_MONTH:-${SYSTEM_START_MONTH}}}"
STREAM_END_MONTH="${STREAM_END_MONTH:-${STREAM_BENCHMARK_END_MONTH:-${SYSTEM_END_MONTH}}}"
SYSTEM_BENCHMARK_SUBSET_ENABLED="${SYSTEM_BENCHMARK_SUBSET_ENABLED:-${BENCHMARK_SUBSET_ENABLED}}"
SYSTEM_BENCHMARK_SUBSET_MODE="${SYSTEM_BENCHMARK_SUBSET_MODE:-${BENCHMARK_SUBSET_MODE}}"
SYSTEM_BENCHMARK_SAMPLE_FRACTION="${SYSTEM_BENCHMARK_SAMPLE_FRACTION:-${BENCHMARK_SAMPLE_FRACTION:-0.001}}"
STREAM_BENCHMARK_SUBSET_ENABLED="${STREAM_BENCHMARK_SUBSET_ENABLED:-${BENCHMARK_SUBSET_ENABLED}}"
STREAM_BENCHMARK_SUBSET_MODE="${STREAM_BENCHMARK_SUBSET_MODE:-${BENCHMARK_SUBSET_MODE}}"
STREAM_BENCHMARK_SAMPLE_FRACTION="${STREAM_BENCHMARK_SAMPLE_FRACTION:-${BENCHMARK_SAMPLE_FRACTION:-0.001}}"
BENCHMARK_STAGING_ENABLED="${BENCHMARK_STAGING_ENABLED:-true}"
BENCHMARK_INPUT_SAMPLE_ENABLED="${BENCHMARK_INPUT_SAMPLE_ENABLED:-true}"
BENCHMARK_SAMPLE_SEED="${BENCHMARK_SAMPLE_SEED:-42}"
BENCHMARK_PARQUET_BASELINE_ENABLED="${BENCHMARK_PARQUET_BASELINE_ENABLED:-true}"
SYSTEM_GOLD_REFRESH_MODE="${SYSTEM_GOLD_REFRESH_MODE:-full}"
PROCESSING_GOLD_REFRESH_MODE="${PROCESSING_GOLD_REFRESH_MODE:-full}"
PROCESSING_BATCH_ENABLED="${PROCESSING_BATCH_ENABLED:-true}"
STREAM_BENCHMARK_CHECKPOINT_LOCATION="${STREAM_BENCHMARK_CHECKPOINT_LOCATION:-s3a://warehouse/benchmarks/checkpoints/${BENCHMARK_PROFILE}/${RUN_LABEL}/streaming_replay}"
export BENCHMARK_WRITE_PARTITIONS="${BENCHMARK_WRITE_PARTITIONS:-4}"
export BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS="${BENCHMARK_INPUT_SAMPLE_WRITE_PARTITIONS:-4}"

log "Benchmark profile=${BENCHMARK_PROFILE} run_id=${RUN_LABEL}"
log "Profile scope: historical_batch=${SYSTEM_START_MONTH}..${SYSTEM_END_MONTH} subset=${SYSTEM_BENCHMARK_SUBSET_ENABLED}/${SYSTEM_BENCHMARK_SUBSET_MODE}; streaming_replay=${STREAM_START_MONTH}..${STREAM_END_MONTH} subset=${STREAM_BENCHMARK_SUBSET_ENABLED}/${STREAM_BENCHMARK_SUBSET_MODE} fraction=${STREAM_BENCHMARK_SAMPLE_FRACTION}; parquet_baseline=${BENCHMARK_PARQUET_BASELINE_ENABLED}; processing_batch=${PROCESSING_BATCH_ENABLED}"
log "Spark master=${SPARK_MASTER} local_dir=${SPARK_LOCAL_DIR:-/srv/ecommerce/spark-tmp} shuffle_partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-8} default_parallelism=${SPARK_DEFAULT_PARALLELISM:-8}"
log "Spark memory driver=${SPARK_DRIVER_MEMORY:-2g} executor=${SPARK_EXECUTOR_MEMORY:-unset} S3 endpoint=${S3_ENDPOINT} output=${RUN_ROOT} parquet=${BENCHMARK_PARQUET_ROOT} staging=${BENCHMARK_STAGING_ROOT} input_samples=${BENCHMARK_INPUT_SAMPLE_ROOT}"
log_disk_usage "before benchmark"
log_remote_disk_usage "before benchmark"
check_benchmark_free_space || fail "insufficient free disk for benchmark; set BENCHMARK_DISK_FREE_THRESHOLD_GB only after capacity review"
validate_spark_local_dirs
check_spark_idle
write_metadata "started"

if [[ "${BENCHMARK_CLEANUP_SPARK_TEMP_BEFORE:-true}" == "true" ]]; then
  cleanup_spark_temp_dirs
  log_remote_disk_usage "after spark-temp cleanup"
fi

if [[ "${SYSTEM_BENCHMARK_MONTH_CHUNKED:-false}" == "true" ]]; then
  mkdir -p "${RUN_ROOT}/system_chunks"
  chmod 0777 "${RUN_ROOT}" "${RUN_ROOT}/system_chunks" 2>/dev/null || true
  chunk_jsons=()
  drop_existing="${SYSTEM_BENCHMARK_DROP_EXISTING_FIRST_CHUNK:-true}"
  for month in $(month_list "${SYSTEM_START_MONTH}" "${SYSTEM_END_MONTH}"); do
    chunk_json="${RUN_ROOT}/system_chunks/system_benchmark_${month}.json"
    chunk_csv="${RUN_ROOT}/system_chunks/system_benchmark_${month}.csv"
    log "Running month-chunked historical batch month=${month} drop_existing=${drop_existing}"
    run_system_benchmark_phase "historical batch system benchmark ${month}" "${month}" "${month}" "${drop_existing}" "${chunk_json}" "${chunk_csv}" "system_input_${month}"
    chunk_jsons+=("${chunk_json}")
    drop_existing="false"
    log_disk_usage "after historical batch chunk ${month}"
    log_remote_disk_usage "after historical batch chunk ${month}"
    if [[ "${BENCHMARK_CLEANUP_SPARK_TEMP_BETWEEN_CHUNKS:-true}" == "true" ]]; then
      cleanup_spark_temp_dirs
    fi
  done
  aggregate_system_chunk_results "${chunk_jsons[@]}"
else
  run_system_benchmark_phase "historical batch system benchmark" "${SYSTEM_START_MONTH}" "${SYSTEM_END_MONTH}" "true" "${RUN_ROOT}/system_benchmark.json" "${RUN_ROOT}/system_benchmark.csv" "system_input"
fi
log_disk_usage "after historical batch phase"
log_remote_disk_usage "after historical batch phase"

run_with_disk_watchdog "streaming replay benchmark" spark_submit_iceberg_job "${ROOT_DIR}/infra/jobs/benchmark_batch_vs_streaming.py" \
  --mode "${BENCHMARK_MODE:-full}" \
  --input-dir "${STREAM_BENCHMARK_INPUT_DIR:-${BENCHMARK_INPUT_DIR}}" \
  --sample-file "${STREAM_BENCHMARK_SAMPLE_FILE:-/opt/ecommerce-lakehouse/data/sample/events_streaming_demo_1500.csv}" \
  --start-month "${STREAM_START_MONTH}" \
  --end-month "${STREAM_END_MONTH}" \
  --subset-enabled "${STREAM_BENCHMARK_SUBSET_ENABLED}" \
  --subset-mode "${STREAM_BENCHMARK_SUBSET_MODE}" \
  --sample-fraction "${STREAM_BENCHMARK_SAMPLE_FRACTION}" \
  --sample-seed "${BENCHMARK_SAMPLE_SEED}" \
  --staging-enabled "${BENCHMARK_STAGING_ENABLED}" \
  --staging-path "${BENCHMARK_STAGING_ROOT}/processing_input" \
  --staging-reuse-existing "${BENCHMARK_STAGING_REUSE_EXISTING:-false}" \
  --input-sample-enabled "${BENCHMARK_INPUT_SAMPLE_ENABLED}" \
  --input-sample-root "${BENCHMARK_INPUT_SAMPLE_ROOT}" \
  --input-sample-bootstrap-path "" \
  --input-row-count-override "${BENCHMARK_INPUT_ROW_COUNT_OVERRIDE:-}" \
  --microbatch-size "${STREAM_BENCHMARK_MICROBATCH_SIZE:-50000}" \
  --gold-refresh-mode "${PROCESSING_GOLD_REFRESH_MODE}" \
  --batch-enabled "${PROCESSING_BATCH_ENABLED}" \
  --streaming-checkpoint-location "${STREAM_BENCHMARK_CHECKPOINT_LOCATION}" \
  --profile "${BENCHMARK_PROFILE}" \
  --run-id "${RUN_LABEL}" \
  --output-json "${RUN_ROOT}/processing_model_benchmark.json" \
  --output-csv "${RUN_ROOT}/processing_model_benchmark.csv"
log_disk_usage "after streaming replay phase"
log_remote_disk_usage "after streaming replay phase"

write_metadata "succeeded"
write_run_summary
log_disk_usage "after benchmark"
log_remote_disk_usage "after benchmark"
log "Benchmark artifacts written to ${RUN_ROOT}"
