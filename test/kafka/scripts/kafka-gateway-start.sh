#!/bin/sh

# Kafka Gateway Startup Script for Integration Testing

set -e

echo "Starting Kafka Gateway..."

# Wait for dependencies
echo "Waiting for SeaweedFS Filer..."
while ! nc -z ${SEAWEEDFS_FILER%:*} ${SEAWEEDFS_FILER#*:}; do
  sleep 1
done
echo "SeaweedFS Filer is ready"

echo "Waiting for SeaweedFS MQ Broker..."
while ! nc -z ${SEAWEEDFS_MQ_BROKER%:*} ${SEAWEEDFS_MQ_BROKER#*:}; do
  sleep 1
done
echo "SeaweedFS MQ Broker is ready"

echo "Waiting for Schema Registry..."
while ! curl -f ${SCHEMA_REGISTRY_URL}/subjects > /dev/null 2>&1; do
  sleep 1
done
echo "Schema Registry is ready"

# Create offset database directory
mkdir -p /data/offsets

# Start Kafka Gateway
echo "Starting Kafka Gateway on port ${KAFKA_PORT:-9093}..."
exec /usr/bin/weed kafka.gateway \
  -filer=${SEAWEEDFS_FILER} \
  -mq.broker=${SEAWEEDFS_MQ_BROKER} \
  -schema.registry=${SCHEMA_REGISTRY_URL} \
  -port=${KAFKA_PORT:-9093} \
  -ip=0.0.0.0 \
  -offset.db=/data/offsets/kafka-offsets.db \
  -log.level=1 \
  -v=2
