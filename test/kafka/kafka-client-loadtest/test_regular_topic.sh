#!/bin/bash
set -e

echo "=== Testing Regular Topic (Not Schema Registry) ==="

# Use kcat (kafkacat) if available in a container
docker run --rm --network kafka-client-loadtest \
  confluentinc/cp-kafkacat:latest \
  sh -c '
echo "1. Creating topic and producing message..."
echo "{\"test\":\"extraction\",\"value\":123}" | kafkacat -P -b kafka-gateway:9092 -t regular-test -K:
sleep 2

echo ""
echo "2. Consuming message..."
kafkacat -C -b kafka-gateway:9092 -t regular-test -c 1 -o beginning
' 2>&1 | grep -v "^%"

echo ""
echo "=== Test Complete ==="
