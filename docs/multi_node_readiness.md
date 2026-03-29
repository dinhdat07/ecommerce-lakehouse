# Multi-Node Readiness

## What Is Already Ready

- Storage, manifest, and checkpoint roots are externalized through environment variables.
- Logical Bronze, Silver, and Gold URIs are separated from local filesystem materialization.
- App entrypoints are thin wrappers around shared pipeline modules, which makes backend replacement straightforward.
- Kafka, Spark, MinIO, Iceberg, Trino, and Superset settings already have explicit configuration placeholders.

## Required Deployment Assumptions

- All compute nodes must see the same persisted stage outputs.
- For the current filesystem backend, that means mounting the same shared path on all nodes, for example `/mnt/lakehouse-shared`.
- For the target platform backend, shared persistence should move to MinIO/S3 plus Iceberg metadata.

## Straightforward Scaling Path

1. Keep the current CLI and pipeline module interfaces unchanged.
2. Replace the local JSONL/CSV writers in `common/storage.py` with a backend adapter for Spark/Iceberg.
3. Point `BRONZE_PATH`, `SILVER_PATH`, and `GOLD_PATH` at shared object storage.
4. Set `LOCAL_DATA_ROOT`, `MANIFEST_ROOT`, and `CHECKPOINT_ROOT` to mounted shared paths during the transition period.
5. Move batch and streaming execution to Spark submit commands or a scheduler while preserving the same stage boundaries.

## Practical Current Limitation

The current implementation is multi-node compatible by configuration shape, but not yet multi-node distributed by execution engine. It is best viewed as a clean local reference implementation with a migration path, not as a finished cluster runtime.
