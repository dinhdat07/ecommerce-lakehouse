#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC_EVENTS="${KAFKA_TOPIC_EVENTS:-ecom.events}"
TOPIC_DLQ="${KAFKA_TOPIC_DLQ:-ecom.events.dlq}"
TOPIC_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
TOPIC_REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
TOPIC_MIN_INSYNC_REPLICAS="${KAFKA_TOPIC_MIN_INSYNC_REPLICAS:-1}"
TOPIC_RETENTION_HOURS="${KAFKA_RETENTION_HOURS:-12}"
TOPIC_RETENTION_BYTES="${KAFKA_RETENTION_BYTES:-536870912}"
TOPIC_MESSAGE_MAX_BYTES="${KAFKA_MESSAGE_MAX_BYTES:-2097152}"

kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --create \
  --if-not-exists \
  --topic "$TOPIC_EVENTS" \
  --partitions "$TOPIC_PARTITIONS" \
  --replication-factor "$TOPIC_REPLICATION_FACTOR" \
  --config "min.insync.replicas=${TOPIC_MIN_INSYNC_REPLICAS}" \
  --config "retention.ms=$(( TOPIC_RETENTION_HOURS * 60 * 60 * 1000 ))" \
  --config "retention.bytes=${TOPIC_RETENTION_BYTES}" \
  --config "max.message.bytes=${TOPIC_MESSAGE_MAX_BYTES}"

kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVERS" \
  --create \
  --if-not-exists \
  --topic "$TOPIC_DLQ" \
  --partitions "$TOPIC_PARTITIONS" \
  --replication-factor "$TOPIC_REPLICATION_FACTOR" \
  --config "min.insync.replicas=${TOPIC_MIN_INSYNC_REPLICAS}" \
  --config "retention.ms=$(( TOPIC_RETENTION_HOURS * 60 * 60 * 1000 ))" \
  --config "retention.bytes=${TOPIC_RETENTION_BYTES}" \
  --config "max.message.bytes=${TOPIC_MESSAGE_MAX_BYTES}"
