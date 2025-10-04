#!/bin/bash
# Show Docker build cache statistics

echo "🔍 Docker Build Cache Statistics"
echo "================================"

# Show Docker system info
echo "📊 Docker System Info:"
docker system df

echo ""
echo "🗂️ Build Cache Details:"
docker buildx du

echo ""
echo "💡 Tips:"
echo "  - Use 'make build-gateway' for cached builds (fast)"
echo "  - Use 'make build-gateway-clean' for fresh builds (slow)"
echo "  - Cache persists between builds when go.mod/go.sum unchanged"
echo "  - Build context optimized with .dockerignore"

