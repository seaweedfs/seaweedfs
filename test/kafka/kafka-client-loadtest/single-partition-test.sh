#!/bin/bash
# Single partition test - produce and consume from ONE topic, ONE partition

set -e

echo "================================================================"
echo "    Single Partition Test - Isolate Missing Messages"
echo "  - Topic: single-test-topic (1 partition only)"
echo "  - Duration: 2 minutes"
echo "  - Producer: 1 (50 msgs/sec)"
echo "  - Consumer: 1 (reading from partition 0 only)"
echo "================================================================"

# Clean up
make clean
make start

# Run test with single topic, single partition
TEST_MODE=comprehensive \
TEST_DURATION=2m \
PRODUCER_COUNT=1 \
CONSUMER_COUNT=1 \
MESSAGE_RATE=50 \
MESSAGE_SIZE=512 \
TOPIC_COUNT=1 \
PARTITIONS_PER_TOPIC=1 \
VALUE_TYPE=avro \
docker compose --profile loadtest up --abort-on-container-exit kafka-client-loadtest

echo ""
echo "================================================================"
echo "                Single Partition Test Complete!"  
echo "================================================================"
echo ""
echo "Analyzing results..."
cd test-results && python3 analyze_missing.py
