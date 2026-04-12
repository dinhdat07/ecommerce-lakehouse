# Gap Analysis

## Current Status

The repository now has:

- Phase 1 Spark + Iceberg batch materialization for Bronze, Silver, and all six Gold tables
- Phase 2 bounded Kafka + Spark Structured Streaming replay
- Phase 3 production-like hardening with DQ gates, stage manifests, structured metrics, and benchmark tooling

## Gaps Closed In This Iteration

- Added Spark-side DQ summaries with critical vs warning semantics before Silver and Gold publication.
- Added stage manifests and `pipeline_metrics` emission for the real Spark/Iceberg batch and streaming jobs.
- Completed the system benchmark with dual-mode input selection, query latency, storage-size reporting, and JSON/CSV outputs.
- Replaced the placeholder processing-model benchmark with a runnable bounded comparison and documented its scope.

## Remaining Limitations

- Phase 2 and the Phase 3 processing-model benchmark remain bounded demos, not long-running production services.
- Gold refresh still recomputes affected partitions from Silver rather than maintaining stateful incremental aggregates.
- Demo-mode benchmarks are for local validation; full historical benchmarking still requires external raw files.
- Kafka remains single-broker and Spark runs in a laptop-oriented local-submit mode inside Docker.
- True production multi-node deployment still needs stronger orchestration, secrets management, storage durability, and scheduler integration.

## Recommended Next Priorities

1. Add batch-vs-streaming semantic parity tests on the same controlled slice.
2. Persist audit and DQ summaries into queryable Iceberg audit tables.
3. Add compaction and retention policies for the object-store-backed tables.
4. Promote the current shell orchestration into an explicit workflow runner when the environment becomes longer lived.
