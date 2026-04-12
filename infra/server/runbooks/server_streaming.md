# Server Streaming Runbook

1. Copy `infra/server/env/server.env.example` to `infra/server/env/server.env`.
2. Copy one storage profile example to `infra/server/env/storage-minio.env` or `storage-s3.env`.
3. Start the Kafka cluster or point `KAFKA_BOOTSTRAP_SERVERS` at an existing one.
4. Start the streaming job:
   `bash infra/server/scripts/start-streaming.sh`
5. Inspect progress and checkpoint state:
   `bash infra/server/scripts/inspect-streaming-state.sh`
6. Stop the job:
   `bash infra/server/scripts/stop-streaming.sh`
7. Reset checkpoint/progress state before a clean replay:
   `bash infra/server/scripts/reset-streaming-state.sh`

Implemented here, pending server execution:
- long-running restart validation from the configured checkpoint path
- cluster-scale throughput and recovery testing
