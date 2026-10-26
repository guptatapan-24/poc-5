#!/usr/bin/env bash
# Create required Kafka topics for Real-Time Stock Event Capture & Replay
set -e

BOOTSTRAP="localhost:9092"
KAFKA_BIN="/opt/kafka/bin"

echo "Creating topic 'stock-prices'..."
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists \
  --topic stock-prices \
  --partitions 2 \
  --replication-factor 1 \
  --config retention.ms=604800000

echo "Creating topic 'stock-events'..."
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --if-not-exists \
  --topic stock-events \
  --partitions 2 \
  --replication-factor 1 \
  --config retention.ms=604800000

echo ""
echo "Listing active Kafka topics:"
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list

echo ""
echo "Topic 'stock-prices' details:"
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic stock-prices

echo ""
echo "Topic 'stock-events' details:"
$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic stock-events
