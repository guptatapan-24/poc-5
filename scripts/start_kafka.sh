#!/usr/bin/env bash
# Start Apache Kafka 4.x in KRaft mode with IPv4 preference
set -e
export KAFKA_OPTS="-Djava.net.preferIPv4Stack=true"
echo "Starting Apache Kafka (KRaft mode) on port 9092..."
exec /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
