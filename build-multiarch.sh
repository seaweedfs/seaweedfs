#!/bin/bash
set -e

# Build script for multiarch SeaweedFS container with EC verification
# Usage: ./build-multiarch.sh [IMAGE_NAME] [TAG]

IMAGE_NAME="${1:-seaweedfs-ec-verify}"
TAG="${2:-latest}"

echo "Building multiarch container: ${IMAGE_NAME}:${TAG}"

# Build for multiple architectures
# Build the binary directly
echo "Building weed binary..."
GOOS=linux GOARCH=amd64 go build -o ./weed_binary ./weed

# Build the container
echo "Building Docker image..."
docker buildx build \
  --platform linux/amd64 \
  -f Dockerfile.local \
  -t ${IMAGE_NAME}:${TAG} \
  -t ghcr.io/jrcichra/seaweedfs:ec-verify \
  --load \
  .

# Clean up
rm -f ./weed_binary

echo ""
echo "✅ Build complete!"
echo ""
echo "Image: ${IMAGE_NAME}:${TAG}"
echo ""
echo "Test it with:"
echo "  docker run --rm ${IMAGE_NAME}:${TAG} version"
echo ""
echo "Run a server:"
echo "  docker run -d -p 9333:9333 -p 8080:8080 -v /path/to/data:/data ${IMAGE_NAME}:${TAG} server"
echo ""
echo "Run shell commands:"
echo "  echo 'ec.verify -volumeId=123' | docker run -i --rm ${IMAGE_NAME}:${TAG} shell -master=your-master:9333"
