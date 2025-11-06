#!/bin/bash

# Wait for services to be ready
set -e

OPENBAO_ADDR=${OPENBAO_ADDR:-"http://127.0.0.1:8200"}
SEAWEEDFS_S3_ENDPOINT=${SEAWEEDFS_S3_ENDPOINT:-"http://127.0.0.1:8333"}
MAX_WAIT=120 # 2 minutes

echo "🕐 Waiting for services to be ready..."

# Wait for OpenBao
echo "   Waiting for OpenBao at $OPENBAO_ADDR..."
for i in $(seq 1 $MAX_WAIT); do
    if curl -s "$OPENBAO_ADDR/v1/sys/health" >/dev/null 2>&1; then
        echo "   [OK] OpenBao is ready!"
        break
    fi
    if [ $i -eq $MAX_WAIT ]; then
        echo "   [FAIL] Timeout waiting for OpenBao"
        exit 1
    fi
    sleep 1
done

# Wait for SeaweedFS Master
echo "   Waiting for SeaweedFS Master at http://127.0.0.1:9333..."
for i in $(seq 1 $MAX_WAIT); do
    if curl -s "http://127.0.0.1:9333/cluster/status" >/dev/null 2>&1; then
        echo "   [OK] SeaweedFS Master is ready!"
        break
    fi
    if [ $i -eq $MAX_WAIT ]; then
        echo "   [FAIL] Timeout waiting for SeaweedFS Master"
        exit 1
    fi
    sleep 1
done

# Wait for SeaweedFS Volume Server
echo "   Waiting for SeaweedFS Volume Server at http://127.0.0.1:8080..."
for i in $(seq 1 $MAX_WAIT); do
    if curl -s "http://127.0.0.1:8080/status" >/dev/null 2>&1; then
        echo "   [OK] SeaweedFS Volume Server is ready!"
        break
    fi
    if [ $i -eq $MAX_WAIT ]; then
        echo "   [FAIL] Timeout waiting for SeaweedFS Volume Server"
        exit 1
    fi
    sleep 1
done

# Wait for SeaweedFS S3 API
echo "   Waiting for SeaweedFS S3 API at $SEAWEEDFS_S3_ENDPOINT..."
for i in $(seq 1 $MAX_WAIT); do
    if curl -s "$SEAWEEDFS_S3_ENDPOINT/" >/dev/null 2>&1; then
        echo "   [OK] SeaweedFS S3 API is ready!"
        break
    fi
    if [ $i -eq $MAX_WAIT ]; then
        echo "   [FAIL] Timeout waiting for SeaweedFS S3 API"
        exit 1
    fi
    sleep 1
done

echo "🎉 All services are ready!"

# Show service status
echo ""
echo "📊 Service Status:"
echo "   OpenBao:             $(curl -s $OPENBAO_ADDR/v1/sys/health | jq -r '.initialized // "Unknown"')"
echo "   SeaweedFS Master:    $(curl -s http://127.0.0.1:9333/cluster/status | jq -r '.IsLeader // "Unknown"')"
echo "   SeaweedFS Volume:    $(curl -s http://127.0.0.1:8080/status | jq -r '.Version // "Unknown"')"
echo "   SeaweedFS S3 API:    Ready"
echo ""
