# Infrastructure Notes

This repository now runs end to end locally without external services, but the target production-style stack remains:

- Kafka for durable streaming ingest
- MinIO for S3-compatible object storage
- Spark for distributed batch and streaming execution
- Iceberg for managed Silver and Gold tables
- Trino for SQL serving
- Superset for dashboards

`docker-compose.yml` is provided as a starting point for a future integrated developer stack. It is intentionally treated as a bootstrap asset rather than a fully validated production deployment definition.
