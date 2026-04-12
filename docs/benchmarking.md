# Benchmarking

Phase 3 provides two benchmark entrypoints plus one Docker wrapper script.

## Modes

- `full`: use external historical raw files. This is the intended benchmark for `2019-10` to `2020-02`.
- `demo`: use a checked-in sample file for local smoke testing.
- `auto`: prefer `full` when the raw files exist under `data/raw/`; otherwise fall back to `demo`.

The benchmark output always labels the selected mode so demo-scale results are not mistaken for full historical numbers.

## System benchmark

Compares `Spark + Iceberg` with `Spark + Parquet` using the same Bronze, Silver, and Gold Spark transforms and the same DQ checks.

Command inside the Docker stack:

```bash
bash infra/scripts/run-phase3-benchmarks.sh
```

Direct Spark job example:

```bash
python infra/jobs/benchmark_system_lakehouse_vs_parquet.py \
  --mode auto \
  --input-dir /workspace/data/raw \
  --sample-file /workspace/data/sample/events_sample_100k.csv \
  --output-json /workspace/data/benchmarks/system_benchmark.json \
  --output-csv /workspace/data/benchmarks/system_benchmark.csv
```

Reported metrics:
- ingestion time
- transformation time
- representative Spark SQL query latency
- storage size

## Processing-model benchmark

Compares one-shot batch processing with bounded microbatch replay on the same March-April slice.

This benchmark focuses on:
- total wall time
- microbatch latency
- publish freshness
- qualitative complexity notes

It intentionally isolates the Spark processing model. Kafka transport overhead is still validated separately through the Phase 2 replay demo.

Direct Spark job example:

```bash
python infra/jobs/benchmark_batch_vs_streaming.py \
  --mode auto \
  --input-dir /workspace/data/raw \
  --sample-file /workspace/data/sample/events_streaming_demo_1500.csv \
  --microbatch-size 500 \
  --output-json /workspace/data/benchmarks/processing_model_benchmark.json \
  --output-csv /workspace/data/benchmarks/processing_model_benchmark.csv
```

## Output files

The Docker wrapper writes:
- `data/benchmarks/system_benchmark.json`
- `data/benchmarks/system_benchmark.csv`
- `data/benchmarks/processing_model_benchmark.json`
- `data/benchmarks/processing_model_benchmark.csv`

## Interpretation notes

- Demo-mode numbers are for validation and regression detection, not capacity planning.
- Iceberg storage size depends on object-store layout and metadata overhead.
- The microbatch benchmark is useful for relative latency and freshness tradeoffs, not for broker-throughput tuning.
