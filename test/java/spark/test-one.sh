#!/bin/bash

# Run a single test method for quick iteration

set -e

if [ $# -eq 0 ]; then
    echo "Usage: ./test-one.sh <TestClass>#<methodName>"
    echo ""
    echo "Examples:"
    echo "  ./test-one.sh SparkReadWriteTest#testWriteAndReadParquet"
    echo "  ./test-one.sh SparkSQLTest#testCreateTableAndQuery"
    echo ""
    exit 1
fi

# Check if SeaweedFS is running
if ! curl -f http://localhost:8888/ > /dev/null 2>&1; then
    echo "✗ SeaweedFS filer is not accessible at http://localhost:8888"
    echo ""
    echo "Please start SeaweedFS first:"
    echo "  docker-compose up -d"
    echo ""
    exit 1
fi

echo "✓ SeaweedFS filer is accessible"
echo ""
echo "Running test: $1"
echo ""

# Set environment variables
export SEAWEEDFS_TEST_ENABLED=true
export SEAWEEDFS_FILER_HOST=localhost
export SEAWEEDFS_FILER_PORT=8888
export SEAWEEDFS_FILER_GRPC_PORT=18888

# Run the specific test
mvn test -Dtest="$1"

