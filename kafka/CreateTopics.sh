#!/bin/sh
set -e

for t in streaming_events dlq_streaming_events qoe_alerts; do
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create --if-not-exists \
    --topic "$t" \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000
done

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP_SERVER" \
  --list
