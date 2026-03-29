# 3-Node Ubuntu Deployment Guide

This guide describes a simple, production-minded shape for three Ubuntu servers while the repository remains on the local/filesystem backend.

## Target Topology

- `node1`: control and services node
  - Kafka broker/controller
  - MinIO
  - Trino coordinator
  - Superset
  - shared storage export or shared mount management
- `node2`: compute node
  - Python runtime
  - future Spark worker / batch execution
- `node3`: compute node
  - Python runtime
  - future Spark worker / streaming execution

## Step 1: Prepare All Three Servers

Run on `node1`, `node2`, and `node3`:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip openjdk-17-jre-headless git curl
```

Clone the repository to the same path on each node, for example `/opt/ecommerce-lakehouse`.

## Step 2: Provide Shared Storage

For the current backend, all nodes must see the same output and manifest paths.

Recommended simple approach:

1. Export an NFS share from `node1`, for example `/srv/lakehouse-shared`.
2. Mount it on all three nodes at `/mnt/lakehouse-shared`.
3. Use these paths in `.env` on every node:

```bash
LOCAL_DATA_ROOT=/mnt/lakehouse-shared/lakehouse
MANIFEST_ROOT=/mnt/lakehouse-shared/manifests
CHECKPOINT_ROOT=/mnt/lakehouse-shared/checkpoints
```

## Step 3: Bootstrap Python on All Nodes

Run on each node inside the repository:

```bash
bash scripts/bootstrap_local.sh
source .venv/bin/activate
```

If you are enabling future platform dependencies:

```bash
WITH_PLATFORM=1 bash scripts/bootstrap_local.sh
```

## Step 4: Configure Environment

Copy `.env.example` to `.env` on all nodes and align the shared settings:

```bash
cp .env.example .env
```

Set at minimum:

```bash
ENV=cluster
LOCAL_DATA_ROOT=/mnt/lakehouse-shared/lakehouse
MANIFEST_ROOT=/mnt/lakehouse-shared/manifests
CHECKPOINT_ROOT=/mnt/lakehouse-shared/checkpoints
KAFKA_BOOTSTRAP_SERVERS=node1:9092
S3_ENDPOINT=http://node1:9000
TRINO_HOST=node1
TRINO_PORT=8080
SUPERSET_URL=http://node1:8088
```

## Step 5: Validate the Shared Local Backend

On `node2`, run a batch load:

```bash
bash scripts/clean_local_state.sh
bash scripts/run_local_batch.sh
```

On `node3`, inspect the same shared outputs:

```bash
find /mnt/lakehouse-shared/lakehouse -maxdepth 4 -type f | sort
find /mnt/lakehouse-shared/manifests -maxdepth 3 -type f | sort
```

This confirms all nodes can read the same persisted pipeline state.

## Step 6: Introduce Services on Node1

When Docker Compose or container orchestration is available on `node1`:

1. Start MinIO, Kafka, Trino, and Superset based on `infra/docker-compose.yml`.
2. Create the Kafka topic using `scripts/create_kafka_topics.sh`.
3. Point future batch and streaming runs at these service endpoints via `.env`.

## Step 7: Evolve to Real Multi-Node Execution

Once Spark and shared object storage are ready:

1. Move `BRONZE_PATH`, `SILVER_PATH`, and `GOLD_PATH` to `s3a://...` locations in MinIO.
2. Run Spark master/coordinator on `node1`.
3. Run Spark workers on `node2` and `node3`.
4. Keep the existing CLI boundaries and replace only the backend implementation.

## Limitations of This 3-Node Guide

- The current repository does not yet provision Spark, Kafka, Trino, or Superset automatically across the three nodes.
- The filesystem backend is appropriate for shared validation and operator flow, not for full distributed scale.
- A future production version should replace NFS-backed shared files with object storage plus Iceberg metadata and explicit service HA.
