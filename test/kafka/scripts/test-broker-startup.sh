#!/bin/bash

# Script to test SeaweedFS MQ broker startup locally
# This helps debug broker startup issues before running CI

set -e

echo "=== Testing SeaweedFS MQ Broker Startup ==="

# Build weed binary
echo "Building weed binary..."
cd "$(dirname "$0")/../../.."
go build -o /tmp/weed ./weed

# Setup data directory
WEED_DATA_DIR="/tmp/seaweedfs-broker-test-$$"
mkdir -p "$WEED_DATA_DIR"
echo "Using data directory: $WEED_DATA_DIR"

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    pkill -f "weed.*server" || true
    pkill -f "weed.*mq.broker" || true
    sleep 2
    rm -rf "$WEED_DATA_DIR"
    rm -f /tmp/weed-*.log
}
trap cleanup EXIT

# Start SeaweedFS server
echo "Starting SeaweedFS server..."
/tmp/weed -v 0 server \
  -ip.bind 127.0.0.1 \
  -dir="$WEED_DATA_DIR" \
  -master.raftHashicorp \
  -master.port=9333 \
  -volume.port=8081 \
  -filer.port=8888 \
  -metricsPort=9325 \
  > /tmp/weed-server-test.log 2>&1 &

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

# Wait for filer
echo "Waiting for filer..."
for i in $(seq 1 30); do
  if curl -s http://127.0.0.1:8888/status >/dev/null 2>&1; then
    echo "✓ Filer is up"
    break
  fi
  echo "  Waiting for filer... ($i/30)"
  sleep 1
done

# Start MQ broker
echo "Starting MQ broker..."
/tmp/weed -v 2 mq.broker \
  -filer="127.0.0.1:8888" \
  -port=17777 \
  > /tmp/weed-mq-broker-test.log 2>&1 &

BROKER_PID=$!
echo "Broker PID: $BROKER_PID"

# Wait for broker
echo "Waiting for broker..."
broker_ready=false
for i in $(seq 1 30); do
  if nc -z 127.0.0.1 17777; then
    echo "✓ MQ broker is up"
    broker_ready=true
    break
  fi
  echo "  Waiting for MQ broker... ($i/30)"
  sleep 1
done

if [ "$broker_ready" = false ]; then
  echo "❌ MQ broker failed to start"
  echo
  echo "=== Server logs ==="
  cat /tmp/weed-server-test.log
  echo
  echo "=== Broker logs ==="
  cat /tmp/weed-mq-broker-test.log
  exit 1
fi

# Test broker discovery  
echo "Testing broker discovery..."
if /tmp/weed -v 1 mq.broker.list -filer="127.0.0.1:8888" >/tmp/broker-list-test.log 2>&1; then
  echo "✓ Broker discovery works"
  cat /tmp/broker-list-test.log
else
  echo "❌ Broker discovery failed"
  cat /tmp/broker-list-test.log
fi

echo
echo "✅ All tests passed!"
echo "Server logs: /tmp/weed-server-test.log"  
echo "Broker logs: /tmp/weed-mq-broker-test.log"
