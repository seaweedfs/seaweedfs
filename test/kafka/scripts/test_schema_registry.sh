#!/bin/bash

# Test script for schema registry E2E testing
# This script sets up a mock schema registry and runs the E2E tests

set -e

echo "🚀 Starting Schema Registry E2E Test"

# Check if we have a real schema registry URL
if [ -n "$SCHEMA_REGISTRY_URL" ]; then
    echo "📡 Using real Schema Registry: $SCHEMA_REGISTRY_URL"
else
    echo "🔧 No SCHEMA_REGISTRY_URL set, using mock registry"
    # For now, we'll skip the test if no real registry is available
    # In the future, we could start a mock registry here
    export SCHEMA_REGISTRY_URL="http://localhost:8081"
    echo "⚠️  Mock registry not implemented yet, test will be skipped"
fi

# Start SeaweedFS infrastructure
echo "🌱 Starting SeaweedFS infrastructure..."
cd /Users/chrislu/go/src/github.com/seaweedfs/seaweedfs

# Clean up any existing processes
pkill -f "weed server" || true
pkill -f "weed mq.broker" || true
sleep 2

# Start SeaweedFS server
echo "🗄️  Starting SeaweedFS server..."
/tmp/weed server -dir=/tmp/seaweedfs-test -master.port=9333 -volume.port=8080 -filer.port=8888 -ip=localhost > /tmp/seaweed-server.log 2>&1 &
SERVER_PID=$!

# Wait for server to be ready
sleep 5

# Start MQ broker
echo "📨 Starting SeaweedMQ broker..."
/tmp/weed mq.broker -master=localhost:9333 -port=17777 > /tmp/seaweed-broker.log 2>&1 &
BROKER_PID=$!

# Wait for broker to be ready
sleep 3

# Check if services are running
if ! curl -s http://localhost:9333/cluster/status > /dev/null; then
    echo "[FAIL] SeaweedFS server not ready"
    exit 1
fi

echo "[OK] SeaweedFS infrastructure ready"

# Run the schema registry E2E tests
echo "🧪 Running Schema Registry E2E tests..."
cd /Users/chrislu/go/src/github.com/seaweedfs/seaweedfs/test/kafka

export SEAWEEDFS_MASTERS=127.0.0.1:9333

# Run the tests
if go test -v ./integration -run TestSchemaRegistryE2E -timeout 5m; then
    echo "[OK] Schema Registry E2E tests PASSED!"
    TEST_RESULT=0
else
    echo "[FAIL] Schema Registry E2E tests FAILED!"
    TEST_RESULT=1
fi

# Cleanup
echo "🧹 Cleaning up..."
kill $BROKER_PID $SERVER_PID 2>/dev/null || true
sleep 2
pkill -f "weed server" || true
pkill -f "weed mq.broker" || true

echo "🏁 Schema Registry E2E Test completed"
exit $TEST_RESULT
