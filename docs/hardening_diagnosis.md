# Benchmark Hardening Diagnosis

| Issue | Severity | Affected layer | Fix risk | Proposed fix |
| --- | --- | --- | --- | --- |
| Spark shuffle/spill can fill root or `/tmp`, and worker spill may miss the intended disk. | High | Spark runtime / server scripts | Low | Force `spark.local.dir` and executor local-dir env to `/srv/ecommerce/spark-tmp`, bound shuffle/default parallelism, enable AQE, log effective configs, and run a disk watchdog. |
| Parquet baseline used local `file:/` output in distributed mode. | High | Benchmark storage | Low | Reject local/file Parquet warehouse when Spark master is non-local, write under `s3a://warehouse/benchmarks/...`, and only delete benchmark-scoped paths. |
| DISK_ONLY benchmark caching can duplicate full input on local disks. | High | Benchmark Spark jobs | Low | Keep dataframe caching disabled by default; optional cache now uses memory-first storage instead of `DISK_ONLY`. |
| Iceberg/MinIO small files and metadata grow quickly from many benchmark tables/microbatches. | Medium | Object storage / Iceberg catalog | Low | Add table properties for target file size and metadata retention, plus dry-run maintenance reporting and rewrite/expire planning. |
| Bronze/Silver dedupe anti-joins scan accumulated table history. | High | Bronze/Silver Spark transforms | Medium | Keep uniqueness definitions, project only key columns, prune Silver existing keys by affected `event_date` partitions, and add optional explain diagnostics. Bronze remains globally checked because its hash is global. |
| Gold full refresh slows streaming replay. | High | Gold aggregation / streaming benchmark | Medium | Preserve correctness by defaulting benchmark Gold to full refresh, while exposing explicit `affected_dates` mode for partition-local Gold and warning that history-wide Gold tables are skipped in that mode. Server streaming configs use `affected_dates` rather than `none`. |
| Benchmark profile/scope was environment-only and easy to run at unsafe scale. | High | Benchmark orchestration | Low | Add first-class profiles (`smoke`, `simple-small`, `simple-medium`, `stress-lite`, `full`) and require `BENCHMARK_ALLOW_FULL=true` for full. |
| Cleanup was ad hoc and could be too broad. | High | Operations scripts | Low | Add dry-run cleanup for benchmark/temp artifacts only, with disk-usage logging and explicit S3 benchmark root opt-in. |
| Artifacts lacked a single summary and some run metadata. | Medium | Benchmark reporting | Low | Emit run id, git commit, profile, Spark configs, disk snapshots, storage footprint, and `run_summary.md`. |
