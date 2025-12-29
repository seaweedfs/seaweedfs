#!/bin/bash
set -e

# Build script for SeaweedFS container with EC verification (ghcr.io only)

echo "Building SeaweedFS container: ghcr.io/jrcichra/seaweedfs:ec-verify"

# Build the binary directly
echo "Building weed binary..."
GOOS=linux GOARCH=amd64 go build -o ./weed_binary ./weed

# Build the container
echo "Building Docker image..."
docker buildx build \
  --platform linux/amd64 \
  -f Dockerfile.local \
  -t ghcr.io/jrcichra/seaweedfs:ec-verify \
  --load \
  .

# Clean up
rm -f ./weed_binary

echo ""
echo "✅ Build complete!"
echo ""
echo "Image: ghcr.io/jrcichra/seaweedfs:ec-verify"
echo ""
echo "Test it with:"
echo "  docker run --rm ghcr.io/jrcichra/seaweedfs:ec-verify version"
echo ""
echo "Run a server:"
echo "  docker run -d -p 9333:9333 -p 8080:8080 -v /path/to/data:/data ghcr.io/jrcichra/seaweedfs:ec-verify server"
echo ""
echo "Run shell commands:"
echo "  echo 'ec.verify -volumeId=123' | docker run -i --rm ghcr.io/jrcichra/seaweedfs:ec-verify shell -master=your-master:9333"
