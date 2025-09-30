#!/bin/bash

GATEWAY_IP="172.18.0.6"
GATEWAY_PORT="9093"
TOPIC="loadtest-topic"
NUM_MESSAGES=1000
CONCURRENT_PRODUCERS=5

echo "=== SeaweedFS Kafka Gateway Load Test ==="
echo "Gateway: ${GATEWAY_IP}:${GATEWAY_PORT}"
echo "Topic: ${TOPIC}"
echo "Messages: ${NUM_MESSAGES}"
echo "Concurrent Producers: ${CONCURRENT_PRODUCERS}"
echo ""

# Test basic connectivity
echo "Testing connectivity..."
if ! docker run --rm --network kafka-client-loadtest edenhill/kcat:1.7.1 -b ${GATEWAY_IP}:${GATEWAY_PORT} -L > /dev/null 2>&1; then
    echo "❌ Failed to connect to Kafka Gateway"
    exit 1
fi
echo "✅ Connected to Kafka Gateway"

# Function to produce messages
produce_messages() {
    local producer_id=$1
    local messages_per_producer=$2
    
    echo "Producer ${producer_id} starting..."
    
    for i in $(seq 1 ${messages_per_producer}); do
        echo "Producer-${producer_id}: Message ${i} at $(date)"
    done | docker run --rm -i --network kafka-client-loadtest edenhill/kcat:1.7.1 \
        -b ${GATEWAY_IP}:${GATEWAY_PORT} \
        -t ${TOPIC} \
        -P \
        -X message.timeout.ms=5000 2>/dev/null
    
    echo "Producer ${producer_id} completed"
}

# Start load test
echo ""
echo "Starting load test..."
start_time=$(date +%s)

messages_per_producer=$((NUM_MESSAGES / CONCURRENT_PRODUCERS))

# Start concurrent producers
for i in $(seq 1 ${CONCURRENT_PRODUCERS}); do
    produce_messages $i $messages_per_producer &
done

# Wait for all producers to finish
wait

end_time=$(date +%s)
duration=$((end_time - start_time))

echo ""
echo "=== Load Test Results ==="
echo "Duration: ${duration} seconds"
echo "Total Messages: ${NUM_MESSAGES}"
echo "Messages/Second: $(echo "scale=2; ${NUM_MESSAGES} / ${duration}" | bc -l 2>/dev/null || echo "N/A")"
echo ""

# Check topic status
echo "Checking topic status..."
docker run --rm --network kafka-client-loadtest edenhill/kcat:1.7.1 -b ${GATEWAY_IP}:${GATEWAY_PORT} -L

echo "✅ Load test completed!"
