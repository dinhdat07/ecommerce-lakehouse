# Storage Switching Runbook

1. Keep local/demo mode on the existing root `infra/` stack.
2. For server mode, copy either:
   - `infra/server/env/storage-minio.env.example` to `storage-minio.env`
   - `infra/server/env/storage-s3.env.example` to `storage-s3.env`
3. Validate storage connectivity:
   `bash infra/server/scripts/validate-storage.sh`
4. Render the Trino Iceberg catalog file:
   `bash infra/server/scripts/render-trino-catalog.sh`
5. Use the same bucket layout and Iceberg warehouse path across Spark and Trino.

Implemented here, pending server execution:
- multi-node concurrent Spark writer validation
- Trino query validation against the rendered server catalog
