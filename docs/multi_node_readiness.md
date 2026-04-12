# Multi-Node Readiness

## Implemented Readiness

- Storage, manifest, and checkpoint roots are externalized through environment variables.
- Logical Bronze, Silver, and Gold URIs are separated from local filesystem materialization.
- App entrypoints are thin wrappers around shared pipeline modules, which makes backend replacement straightforward.
- Kafka, Spark, MinIO, Iceberg, Trino, and Superset settings already have explicit configuration placeholders.
- `infra/server/` now provides server-oriented env templates, runbooks, clustered Kafka Compose, Spark submit wrappers, storage validation, Trino catalog rendering, and benchmark wrappers.
- The streaming job supports dual mode:
  - `demo`: bounded replay for laptop runs
  - `server`: long-running execution with configurable checkpointing, progress logs, and restart controls

## Shared-State Assumptions

- All Spark writers and query engines must see the same object store buckets and the same Iceberg catalog metadata.
- Local/demo mode still maps logical URIs to local paths. Server mode is intended for external MinIO or S3 plus the shared JDBC Iceberg catalog.
- Streaming checkpoints, progress logs, manifests, and benchmark outputs should live on shared or durable server paths.

## Server-Ready Components

1. Clustered Kafka template:
   - `infra/server/compose/docker-compose.kafka-cluster.yml`
   - topic bootstrap via `infra/server/scripts/create-kafka-topics.sh`
2. Server streaming controls:
   - `infra/server/scripts/start-streaming.sh`
   - `infra/server/scripts/stop-streaming.sh`
   - `infra/server/scripts/reset-streaming-state.sh`
   - `infra/server/scripts/inspect-streaming-state.sh`
3. External storage support:
   - `infra/server/env/storage-minio.env.example`
   - `infra/server/env/storage-s3.env.example`
   - `infra/server/scripts/validate-storage.sh`
   - `infra/server/scripts/render-trino-catalog.sh`
4. Full benchmark wrappers:
   - `infra/server/scripts/run-full-benchmark.sh`
   - `infra/server/scripts/collect-benchmark-results.sh`
5. Long-running service templates:
   - `infra/server/systemd/*.template`

## What Still Needs Real Server Validation

- Multi-broker Kafka quorum behavior and failover under real network conditions
- Shared-object-storage concurrency with multiple Spark executors or nodes
- Long-running checkpoint recovery after node or broker interruption
- Full historical benchmark runtime on server hardware using external raw data
- Trino connectivity against the rendered server catalog in the target environment

These pieces are implemented in config, scripts, and runbooks, but remain pending actual server execution.
