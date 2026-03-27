#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC_EVENTS="${KAFKA_TOPIC_EVENTS:-ecom.events}"
TOPIC_DLQ="${KAFKA_TOPIC_DLQ:-ecom.events.dlq}"

kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --create --if-not-exists --topic "$TOPIC_EVENTS" --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server "$BOOTSTRAP_SERVERS" --create --if-not-exists --topic "$TOPIC_DLQ" --partitions 3 --replication-factor 1
