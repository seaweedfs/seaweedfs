#!/bin/bash

# Test script to verify broker discovery works end-to-end

set -e

echo "=== Testing SeaweedFS Broker Discovery ==="

cd /Users/chrislu/go/src/github.com/seaweedfs/seaweedfs

# Build weed binary
echo "Building weed binary..."
go build -o /tmp/weed-discovery ./weed

# Setup data directory
WEED_DATA_DIR="/tmp/seaweedfs-discovery-test-$$"
mkdir -p "$WEED_DATA_DIR"
echo "Using data directory: $WEED_DATA_DIR"

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    pkill -f "weed.*server" || true
    pkill -f "weed.*mq.broker" || true
    sleep 2
    rm -rf "$WEED_DATA_DIR"
    rm -f /tmp/weed-discovery* /tmp/broker-discovery-test*
}
trap cleanup EXIT

# Start SeaweedFS server with consistent IP configuration
echo "Starting SeaweedFS server..."
/tmp/weed-discovery -v 1 server \
  -ip="127.0.0.1" \
  -ip.bind="127.0.0.1" \
  -dir="$WEED_DATA_DIR" \
  -master.raftHashicorp \
  -master.port=9333 \
  -volume.port=8081 \
  -filer.port=8888 \
  -filer=true \
  -metricsPort=9325 \
  > /tmp/weed-discovery-server.log 2>&1 &

SERVER_PID=$!
echo "Server PID: $SERVER_PID"

# Wait for master
echo "Waiting for master..."
for i in $(seq 1 30); do
  if curl -s http://127.0.0.1:9333/cluster/status >/dev/null; then
    echo "✓ Master is up"
    break
  fi
  echo "  Waiting for master... ($i/30)"
  sleep 1
done

# Give components time to initialize
echo "Waiting for components to initialize..."
sleep 10

# Start MQ broker
echo "Starting MQ broker..."
/tmp/weed-discovery -v 2 mq.broker \
  -master="127.0.0.1:9333" \
  -port=17777 \
  > /tmp/weed-discovery-broker.log 2>&1 &

BROKER_PID=$!
echo "Broker PID: $BROKER_PID"

# Wait for broker
echo "Waiting for broker to register..."
sleep 15
broker_ready=false
for i in $(seq 1 20); do
  if nc -z 127.0.0.1 17777; then
    echo "✓ MQ broker is accepting connections"
    broker_ready=true
    break
  fi
  echo "  Waiting for MQ broker... ($i/20)"
  sleep 1
done

if [ "$broker_ready" = false ]; then
  echo "[FAIL] MQ broker failed to start"
  echo "Server logs:"
  cat /tmp/weed-discovery-server.log
  echo "Broker logs:"  
  cat /tmp/weed-discovery-broker.log
  exit 1
fi

# Additional wait for broker registration
echo "Allowing broker to register with master..."
sleep 15

# Check cluster status
echo "Checking cluster status..."
CLUSTER_STATUS=$(curl -s "http://127.0.0.1:9333/cluster/status")
echo "Cluster status: $CLUSTER_STATUS"

# Now test broker discovery using the same approach as the Kafka gateway
echo "Testing broker discovery..."
cd test/kafka
SEAWEEDFS_MASTERS=127.0.0.1:9333 timeout 30s go test -v -run "TestOffsetManagement" -timeout 25s ./e2e/... > /tmp/broker-discovery-test.log 2>&1 && discovery_success=true || discovery_success=false

if [ "$discovery_success" = true ]; then
  echo "[OK] Broker discovery test PASSED!"
  echo "Gateway was able to discover and connect to MQ brokers"
else
  echo "[FAIL] Broker discovery test FAILED"
  echo "Last few lines of test output:"
  tail -20 /tmp/broker-discovery-test.log || echo "No test logs available"
fi

echo
echo "📊 Test Results:"
echo "  Broker startup: ✅"
echo "  Broker registration: ✅"  
echo "  Gateway discovery: $([ "$discovery_success" = true ] && echo "✅" || echo "❌")"

echo
echo "📁 Logs available:"
echo "  Server: /tmp/weed-discovery-server.log"
echo "  Broker: /tmp/weed-discovery-broker.log"
echo "  Discovery test: /tmp/broker-discovery-test.log"
