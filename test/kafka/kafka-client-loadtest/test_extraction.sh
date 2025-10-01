#!/bin/bash
set -e

echo "=== Testing Record Extraction Fix ==="
echo ""

# Wait for services
echo "Waiting for services to be ready..."
sleep 5

# Test 1: Create topic
echo "1. Creating test topic..."
docker exec loadtest-kafka-gateway kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic extraction-test \
  --partitions 1 \
  --replication-factor 1 2>/dev/null || echo "Topic may already exist"

sleep 2

# Test 2: Produce message
echo ""
echo "2. Producing test message..."
echo '{"message":"Record extraction works!","timestamp":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}' | \
  docker exec -i loadtest-kafka-gateway kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic extraction-test

sleep 2

# Test 3: Consume message
echo ""
echo "3. Consuming message (timeout 10s)..."
docker exec loadtest-kafka-gateway timeout 10 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic extraction-test \
  --from-beginning \
  --max-messages 1 2>/dev/null || true

echo ""
echo "=== Test Complete ==="
