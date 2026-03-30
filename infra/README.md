# Infrastructure Notes

This directory now contains a laptop-friendly Docker Compose stack for lightweight multi-node simulation.

## Included Components

- Kafka in single-broker KRaft mode
- MinIO as shared object storage
- Spark master
- Spark worker 1
- Optional Spark worker 2 through the `extended` profile
- Postgres for the Iceberg JDBC catalog and Superset metadata
- Trino for SQL serving over Iceberg
- Superset for the demo dashboard

The stack is intentionally minimal so it can run on a single laptop with constrained disk space and still be easy to tear down completely.

## Main Files

- `docker-compose.yml`: profile-based laptop simulation stack
- `scripts/up-core.sh`: start the lightweight default stack
- `scripts/up-extended.sh`: start the stack with a second Spark worker
- `scripts/up-bi.sh`: start the full local stack including Trino and Superset
- `scripts/run-e2e-demo.sh`: load sample data, build Trino views, and bootstrap Superset assets
- `scripts/verify.sh`: smoke-test the stack
- `scripts/down.sh`: stop containers while keeping volumes
- `scripts/purge.sh`: remove containers, volumes, network, and service images

## Operator Guide

Use [docs/docker_laptop_stack.md](/mnt/e/coding/learn%20data/data%20engineering/ecommerce-lakehouse/docs/docker_laptop_stack.md) for step-by-step instructions, disk expectations, verification steps, and cleanup guidance.
