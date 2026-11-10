#!/bin/bash
set -e
for t in streaming_events dlq_streaming_events qoe_alerts; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$t" --partitions 3 --replication-factor 1 --config retention.ms=86400000
done
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list