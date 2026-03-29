# Gap Analysis

## Current Status

The repository now has a working local Bronze -> Silver -> Gold pipeline and a streaming-friendly replay path. Core data processing, manifests, tests, and serving SQL are present.

## Gaps Closed In This Iteration

- Added practical test/tooling files: `requirements-dev.txt`, `requirements-platform.txt`, `Makefile`, and `scripts/bootstrap_local.sh`.
- Added zero-dependency verification through `scripts/verify_local.py`.
- Added local cleanup tooling with `scripts/clean_local_state.sh`.
- Fixed the replay producer default so a checked-in sample file works out of the box.
- Reduced memory pressure in the local streaming ingest path by streaming replay-bus rows into CSV instead of loading them all first.
- Added detailed local setup, multi-node readiness, and 3-node deployment documentation.

## Remaining Limitations

- The default runtime still uses filesystem-backed JSONL/CSV outputs rather than Spark + Iceberg tables.
- Streaming mode is micro-batch style and uses a local replay bus fallback when Kafka is unavailable.
- Gold aggregation currently recomputes from Silver inputs rather than performing incremental stateful upserts.
- Production services in `infra/docker-compose.yml` are illustrative bootstrap assets and are not yet integrated into automated local orchestration.
- True multi-node execution still requires external infrastructure: shared object storage or mounted shared storage, a Spark cluster, Kafka brokers, Trino catalog configuration, and Superset metadata persistence.

## Recommended Next Priorities

1. Introduce a Spark/Iceberg backend behind the existing pipeline interfaces.
2. Add integration tests that compare the filesystem backend and Spark backend on the same sample slice.
3. Promote manifests and quality counters into queryable audit tables.
4. Replace the local replay bus with Kafka in automated CI or dev-environment tests once platform dependencies are available.
