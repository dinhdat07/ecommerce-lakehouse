# 3-Node Ubuntu Deployment Guide

This guide describes the server-ready deployment path using the additive `infra/server/` assets. It keeps the repo architecture unchanged: Spark performs Bronze, Silver, and Gold transformations; Trino remains query-only; Iceberg tables stay physical; local/demo workflows remain separate under the root `infra/` stack.

## Target Topology

- `node1`: control and services node
  - Kafka broker/controller 1
  - MinIO gateway or external object-storage access
  - Postgres for the Iceberg JDBC catalog
  - Trino coordinator
  - Superset
  - optional Spark master
- `node2`: compute node
  - Kafka broker/controller 2
  - Spark worker / batch execution
- `node3`: compute node
  - Kafka broker/controller 3
  - Spark worker / streaming execution

## Step 1: Prepare All Three Servers

Run on `node1`, `node2`, and `node3`:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip openjdk-17-jre-headless git curl docker.io docker-compose-plugin
```

Clone the repository to the same path on each node, for example `/opt/ecommerce-lakehouse`.

## Step 2: Prepare Shared Services

Recommended shape:

1. Use external distributed MinIO or S3-compatible storage for Bronze, Silver, Gold, and the Iceberg warehouse.
2. Run the Iceberg JDBC catalog database on `node1`.
3. Use durable shared paths on each node for Spark temp data, streaming checkpoints, manifests, and benchmark outputs.

Example server paths:

```bash
sudo mkdir -p /srv/ecommerce/{raw,sample,manifests,checkpoints,benchmarks,logs,runtime,spark-tmp}
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

Create the server env files on `node1`, then distribute matching copies to `node2` and `node3`:

```bash
cp infra/server/env/server.env.example infra/server/env/server.env
cp infra/server/env/kafka-cluster.env.example infra/server/env/kafka-cluster.env
cp infra/server/env/storage-minio.env.example infra/server/env/storage-minio.env
cp infra/server/env/benchmark.env.example infra/server/env/benchmark.env
```

Set at minimum:

```bash
ENV=cluster
DEPLOYMENT_PROFILE=server
KAFKA_BOOTSTRAP_SERVERS=node1:9092,node2:9092,node3:9092
KAFKA_CONTROLLER_QUORUM_VOTERS=1@node1:9093,2@node2:9093,3@node3:9093
S3_ENDPOINT=https://minio-cluster.example.com
S3_PATH_STYLE_ACCESS=true
ICEBERG_CATALOG_URI=jdbc:postgresql://node1:5432/iceberg
ICEBERG_WAREHOUSE=s3://warehouse
CHECKPOINT_ROOT=/srv/ecommerce/checkpoints
MANIFEST_ROOT=/srv/ecommerce/manifests
BENCHMARK_OUTPUT_ROOT=/srv/ecommerce/benchmarks
SPARK_MASTER=spark://node1:7077
```

If you use managed S3 instead of MinIO, start from `infra/server/env/storage-s3.env.example` instead.

## Step 5: Bring Up Kafka on Node1

```bash
bash infra/server/scripts/start-kafka-cluster.sh
bash infra/server/scripts/create-kafka-topics.sh
bash infra/server/scripts/validate-kafka.sh
```

This uses the 3-broker KRaft template in `infra/server/compose/docker-compose.kafka-cluster.yml`. For a non-Docker Kafka deployment, keep the same broker addresses and topic settings from the env files.

## Step 6: Validate Object Storage and Trino Catalog Rendering

On `node1`:

```bash
bash infra/server/scripts/validate-storage.sh
bash infra/server/scripts/render-trino-catalog.sh
```

Copy the rendered `infra/server/trino/catalog/iceberg.properties` into the Trino server catalog directory before starting Trino.

## Step 7: Start Spark and Server Streaming

1. Start the Spark master on `node1`.
2. Start Spark workers on `node2` and `node3`.
3. Start the long-running streaming job from the node that owns the driver process:

```bash
bash infra/server/scripts/start-streaming.sh
```

Check progress and checkpoint state:

```bash
bash infra/server/scripts/inspect-streaming-state.sh
```

Stop or reset safely when needed:

```bash
bash infra/server/scripts/stop-streaming.sh
bash infra/server/scripts/reset-streaming-state.sh
```

## Step 8: Run Full Historical Benchmarks

Point `BENCHMARK_INPUT_DIR` at the external raw-data location for `2019-10` through `2020-02`, then run:

```bash
bash infra/server/scripts/run-full-benchmark.sh
bash infra/server/scripts/collect-benchmark-results.sh
```

Artifacts are written under `BENCHMARK_OUTPUT_ROOT/<run_label>/`.

## Step 9: Query and BI Layer

1. Start Trino on `node1` with the rendered Iceberg catalog.
2. Start Superset on `node1`.
3. Point both services at the same Trino coordinator and object store used by Spark.

## Limitations and Pending Validation

- Kafka cluster templates, streaming controls, storage validation, and benchmark wrappers are implemented, but full behavior still needs runtime validation on real servers.
- The repo does not yet ship a full production orchestrator for Spark, Trino, Superset, and Postgres across all nodes.
- External distributed MinIO or S3 credentials, TLS, firewall rules, and service hardening remain environment-specific operational work.
