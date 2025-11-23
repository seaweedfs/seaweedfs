#!/bin/bash

set -e

echo "=== SeaweedFS Spark Integration Tests Runner ==="
echo ""

# Check if SeaweedFS is running
check_seaweedfs() {
    if curl -f http://localhost:8888/ > /dev/null 2>&1; then
        echo "✓ SeaweedFS filer is accessible at http://localhost:8888"
        return 0
    else
        echo "✗ SeaweedFS filer is not accessible"
        return 1
    fi
}

# Main
if ! check_seaweedfs; then
    echo ""
    echo "Please start SeaweedFS first. You can use:"
    echo "  cd test/java/spark && docker-compose up -d"
    echo "Or:"
    echo "  make docker-up"
    exit 1
fi

echo ""
echo "Running Spark integration tests..."
echo ""

export SEAWEEDFS_TEST_ENABLED=true
export SEAWEEDFS_FILER_HOST=localhost
export SEAWEEDFS_FILER_PORT=8888
export SEAWEEDFS_FILER_GRPC_PORT=18888

# Run tests
mvn test "$@"

echo ""
echo "✓ Test run completed"
echo "View detailed reports in: target/surefire-reports/"


