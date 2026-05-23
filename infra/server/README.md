# Server Infrastructure Profile

This directory contains server-oriented assets for the scale-out deployment target.

Contents:
- `compose/`: server-side infrastructure templates, including clustered Kafka
- `env/`: environment examples for server, storage, Kafka, and benchmark settings
- `nginx/`: Nginx templates for HA ingress in front of a self-hosted MinIO cluster
- `scripts/`: operator scripts for Kafka, streaming, storage validation, and full benchmarks
- `spark/`: shared Spark submit helper and templates
- `systemd/`: service unit templates for long-running jobs
- `trino/`: server-side Trino catalog templates for external object storage
- `runbooks/`: deployment and operational guides

This profile is additive:
- local/demo assets remain under the root `infra/`
- server/full-scale assets live under `infra/server/`
- both profiles reuse the same Spark transformation jobs
- self-hosted server storage can be deployed with the MinIO + Nginx assets here, or replaced by managed S3-compatible storage later

Storage-specific guides:
- `runbooks/minio_cluster_ha.md`: self-hosted 3-node MinIO with Nginx ingress
- `runbooks/gcp_minio_ingress.md`: recommended GCP load-balancer pattern in front of the Nginx layer
