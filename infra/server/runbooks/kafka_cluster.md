# Kafka Cluster Runbook

1. Copy `infra/server/env/server.env.example` to `infra/server/env/server.env`.
2. Copy `infra/server/env/kafka-cluster.env.example` to `infra/server/env/kafka-cluster.env`.
3. Start the cluster:
   `bash infra/server/scripts/start-kafka-cluster.sh`
4. Create the project topics:
   `bash infra/server/scripts/create-kafka-topics.sh`
5. Validate the topic layout:
   `bash infra/server/scripts/validate-kafka.sh`
6. Stop the cluster:
   `bash infra/server/scripts/stop-kafka-cluster.sh`

Implemented here, pending server execution:
- broker failover testing
- external listener validation from non-local hosts
- production retention and ISR tuning under load
