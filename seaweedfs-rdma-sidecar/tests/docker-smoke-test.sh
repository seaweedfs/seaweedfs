#!/bin/bash

# Simple smoke test for Docker setup
set -e

echo "🧪 Docker Smoke Test"
echo "===================="
echo ""

echo "📋 1. Testing Docker Compose configuration..."
docker-compose config --quiet
echo "✅ Docker Compose configuration is valid"
echo ""

echo "📋 2. Testing container builds..."
echo "Building RDMA engine container..."
docker build -f Dockerfile.rdma-engine -t test-rdma-engine . > /dev/null
echo "✅ RDMA engine container builds successfully"
echo ""

echo "📋 3. Testing basic container startup..."
echo "Starting RDMA engine container..."
container_id=$(docker run --rm -d --name test-rdma-engine test-rdma-engine)
sleep 5

if docker ps | grep test-rdma-engine > /dev/null; then
    echo "✅ RDMA engine container starts successfully"
    docker stop test-rdma-engine > /dev/null
else
    echo "❌ RDMA engine container failed to start"
    echo "Checking container logs:"
    docker logs test-rdma-engine 2>&1 || true
    docker stop test-rdma-engine > /dev/null 2>&1 || true
    exit 1
fi
echo ""

echo "🎉 All smoke tests passed!"
echo "Docker setup is working correctly."
