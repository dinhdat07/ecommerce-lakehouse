# profile_2month_batch_streaming_sample

Production-like benchmark profile for the current 3-node server cluster. It keeps multi-node Spark, distributed MinIO, Iceberg JDBC catalog, and Trino-compatible Iceberg table paths. It intentionally does not switch to single-node MinIO and does not attempt a full 5-7 month benchmark on this hardware.

## Scope

- Historical batch phase: full `2019-10..2019-11`
  - `2019-Oct.csv.gz`
  - `2019-Nov.csv.gz`
  - expected raw rows: about `109,950,743`
- Streaming replay phase: sampled `2019-12`
  - `2019-Dec.csv.gz`
  - default sample fraction: `0.10`
  - override with `PROFILE_2MONTH_STREAM_SAMPLE_FRACTION` or `STREAM_BENCHMARK_SAMPLE_FRACTION`
- Parquet baseline: disabled by default for this profile
- Gold refresh: `affected_dates` by default
- Streaming benchmark batch side: disabled by default; historical batch is measured in the system phase

## Cleanup

Dry-run first:

```bash
cd /opt/ecommerce-lakehouse
BENCHMARK_PROFILE=profile_2month_batch_streaming_sample \
BENCHMARK_SPARK_CLEANUP_HOSTS="100.120.253.14 100.99.13.51" \
infra/server/scripts/cleanup-benchmark-artifacts.sh --dry-run --include-s3 --profile profile_2month_batch_streaming_sample
```

Execute only benchmark/temp cleanup:

```bash
cd /opt/ecommerce-lakehouse
BENCHMARK_PROFILE=profile_2month_batch_streaming_sample \
BENCHMARK_SPARK_CLEANUP_HOSTS="100.120.253.14 100.99.13.51" \
infra/server/scripts/cleanup-benchmark-artifacts.sh --execute --include-s3 --profile profile_2month_batch_streaming_sample
```

This deletes Spark temp/spill, old local benchmark run folders, `warehouse/benchmarks/staging`, `warehouse/benchmarks/parquet`, `warehouse/benchmarks/tmp`, and profile-scoped benchmark checkpoints. It preserves raw October/November/December files, production Iceberg tables, Postgres catalog, MinIO bucket roots, and benchmark input samples unless `--expire-input-samples` is explicitly passed.

## Smoke Validation

```bash
cd /opt/ecommerce-lakehouse
BENCHMARK_PROFILE=smoke \
BENCHMARK_RUN_LABEL=smoke-$(date -u +%Y%m%dT%H%M%SZ) \
BENCHMARK_SPARK_CLEANUP_HOSTS="100.120.253.14 100.99.13.51" \
BENCHMARK_DISK_FREE_THRESHOLD_GB=20 \
infra/server/scripts/run-full-benchmark.sh
```

## Target Profile Command

Do not run unless capacity is reviewed and no other Spark jobs are active:

```bash
cd /opt/ecommerce-lakehouse
BENCHMARK_PROFILE=profile_2month_batch_streaming_sample \
PROFILE_2MONTH_STREAM_SAMPLE_FRACTION=0.10 \
BENCHMARK_RUN_LABEL=profile2m-$(date -u +%Y%m%dT%H%M%SZ) \
BENCHMARK_SPARK_CLEANUP_HOSTS="100.120.253.14 100.99.13.51" \
BENCHMARK_DISK_FREE_THRESHOLD_GB=20 \
BENCHMARK_DISK_WATCHDOG_INTERVAL_SECONDS=30 \
infra/server/scripts/run-full-benchmark.sh
```

The target profile fails early by default if Spark workers do not expose
`SPARK_LOCAL_DIRS=/srv/ecommerce/spark-tmp`, because Spark standalone workers
can otherwise ignore `spark.local.dir` and spill under `/tmp`. Check before a
long run:

```bash
for h in 100.73.230.102 100.120.253.14 100.99.13.51; do
  ssh -o BatchMode=yes "$h" 'echo ===$(hostname)===; docker exec spark-worker sh -lc "echo SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIRS:-<unset>}"'
done
```

If the check is unset, restart the Spark worker containers with
`SPARK_LOCAL_DIRS=/srv/ecommerce/spark-tmp` and the `/srv/ecommerce` mount
present. Set `BENCHMARK_REQUIRE_SPARK_LOCAL_DIRS=false` only for smoke/debug
runs where `/tmp` spill risk is explicitly accepted.

## Streaming Replay Only

This runs only the bounded replay job over sampled December data. It writes benchmark-prefixed Iceberg tables and records a run-scoped checkpoint location in metadata.

```bash
cd /opt/ecommerce-lakehouse
BENCHMARK_PROFILE=profile_2month_batch_streaming_sample \
PROFILE_2MONTH_STREAM_SAMPLE_FRACTION=0.10 \
BENCHMARK_RUN_LABEL=stream-dec-sample-$(date -u +%Y%m%dT%H%M%SZ) \
BENCHMARK_SPARK_CLEANUP_HOSTS="100.120.253.14 100.99.13.51" \
BENCHMARK_DISK_FREE_THRESHOLD_GB=20 \
infra/server/scripts/run-profile-streaming-replay.sh
```

This helper performs preflight disk checks, uses the same sample-cache strategy, disables the processing batch side, and records run-scoped checkpoint/staging paths.

## Row Count Validation

```bash
docker exec spark-master /opt/spark/bin/spark-sql --master local[1] \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.defaultCatalog=lakehouse \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=jdbc \
  --conf spark.sql.catalog.lakehouse.uri=jdbc:postgresql://100.73.230.102:5432/iceberg \
  --conf spark.sql.catalog.lakehouse.jdbc.user=iceberg \
  --conf spark.sql.catalog.lakehouse.jdbc.password=iceberg \
  --conf spark.sql.catalog.lakehouse.jdbc.driver=org.postgresql.Driver \
  --conf spark.sql.catalog.lakehouse.warehouse=s3://warehouse \
  --conf spark.sql.catalog.lakehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.lakehouse.s3.endpoint=http://100.73.230.102:9000 \
  --conf spark.sql.catalog.lakehouse.s3.access-key-id=minioadmin \
  --conf spark.sql.catalog.lakehouse.s3.secret-access-key=minioadmin \
  --conf spark.sql.catalog.lakehouse.s3.path-style-access=true \
  --conf spark.sql.catalog.lakehouse.client.region=us-east-1 \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.postgresql:postgresql:42.7.3 \
  -e "SELECT source_month, count(*) FROM lakehouse.demo.bench_bronze_events GROUP BY source_month ORDER BY source_month;"
```

## Gold/DQ Validation

Artifacts contain Silver and Gold DQ summaries. For an additional catalog check:

```bash
python3 infra/jobs/dq_iceberg.py --help
```

Use project DQ jobs against the benchmark-prefixed tables after the run if deeper validation is required.

## Disk Monitoring

```bash
watch -n 30 'for h in 100.73.230.102 100.120.253.14 100.99.13.51; do echo ===$h===; ssh -o BatchMode=yes $h "df -h / /srv/ecommerce /srv/ecommerce/spark-tmp /tmp; du -xsh /srv/ecommerce/spark-tmp /tmp 2>/dev/null"; done'
```

## tmux/nohup Safe Run

```bash
tmux new -s profile2m
cd /opt/ecommerce-lakehouse
BENCHMARK_PROFILE=profile_2month_batch_streaming_sample \
PROFILE_2MONTH_STREAM_SAMPLE_FRACTION=0.10 \
BENCHMARK_RUN_LABEL=profile2m-$(date -u +%Y%m%dT%H%M%SZ) \
BENCHMARK_SPARK_CLEANUP_HOSTS="100.120.253.14 100.99.13.51" \
BENCHMARK_DISK_FREE_THRESHOLD_GB=20 \
infra/server/scripts/run-full-benchmark.sh 2>&1 | tee /srv/ecommerce/benchmarks/profile2m-$(date -u +%Y%m%dT%H%M%SZ).log
```

## Gold Affected Refresh Semantics

`affected_dates` refresh recomputes date-partitioned incremental Gold outputs only for dates written to Silver in the current run. History-wide Gold tables such as cohort, repeat purchase, product affinity, time-to-conversion distribution, and RFM remain full-refresh-only; in affected mode they are recorded as skipped in benchmark metadata instead of silently producing stale partial outputs.

## Limits

This profile is still large for the current hardware. For 5-7 months or a 150M+ full Parquet-vs-Iceberg benchmark, use larger disks and memory, or external object storage. Recommended minimum: 3 nodes with 16-32GB RAM each and 250-500GB fast disk per node, plus dedicated object storage capacity.
