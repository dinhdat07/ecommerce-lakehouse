# Gap Analysis

## Current Status

The repository now has a working Phase 1 Spark + Iceberg batch pipeline and a bounded Phase 2 Kafka + Spark Structured Streaming demo path. Core data processing, validation SQL, tests, and serving integrations are present for laptop-scale runs.

## Gaps Closed In This Iteration

- Materialized Bronze, Silver, and all six Gold outputs as physical Iceberg tables.
- Reused the Phase 1 Spark transformations from both batch and streaming execution paths.
- Added bounded Kafka replay, Spark Structured Streaming ingestion, and incremental Silver/Gold refresh for March-April demo data.
- Added Phase 2 orchestration and validation SQL for repeatable local runs.

## Remaining Limitations

- Phase 2 is a bounded demo job, not a continuously running production stream.
- Gold refresh is incremental by affected date partitions, but still recomputes each affected partition from Silver rather than maintaining stateful incremental aggregates.
- Kafka remains single-broker and Spark runs in a laptop-oriented local-submit mode inside the Docker stack.
- Older local fallback streaming entrypoints remain in the repo for non-Docker development and are not the primary Phase 2 path.
- True multi-node production still requires externalized storage durability, stronger Kafka topology, Spark cluster scheduling, orchestration, and secrets management.

## Recommended Next Priorities

1. Add integration tests that assert batch and streaming paths produce consistent Silver and Gold results on the same controlled slice.
2. Introduce audit tables for replay batches, quality counters, and refresh-watermark tracking.
3. Move the bounded shell orchestration into a small scheduler or workflow runner when longer-lived environments are needed.
4. Promote Kafka, Spark, and Iceberg catalog settings into environment-specific config overlays for real multi-node deployment.
