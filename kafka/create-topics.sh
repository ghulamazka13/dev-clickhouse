#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"

until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; do
  echo "waiting for kafka..."
  sleep 3
done

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --create \
  --if-not-exists \
  --topic raw.security_events \
  --partitions 3 \
  --replication-factor 1
