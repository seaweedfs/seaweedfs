#!/bin/bash

set -e

echo "=== SeaweedFS Spark Integration Tests Quick Start ==="
echo ""

# Check if SeaweedFS is running
check_seaweedfs() {
    echo "Checking if SeaweedFS is running..."
    if curl -f http://localhost:8888/ > /dev/null 2>&1; then
        echo "✓ SeaweedFS filer is accessible at http://localhost:8888"
        return 0
    else
        echo "✗ SeaweedFS filer is not accessible"
        return 1
    fi
}

# Start SeaweedFS with Docker if not running
start_seaweedfs() {
    echo ""
    echo "Starting SeaweedFS with Docker..."
    docker-compose up -d seaweedfs-master seaweedfs-volume seaweedfs-filer
    
    echo "Waiting for SeaweedFS to be ready..."
    for i in {1..30}; do
        if curl -f http://localhost:8888/ > /dev/null 2>&1; then
            echo "✓ SeaweedFS is ready!"
            return 0
        fi
        echo -n "."
        sleep 2
    done
    
    echo ""
    echo "✗ SeaweedFS failed to start"
    return 1
}

# Build the project
build_project() {
    echo ""
    echo "Building the project..."
    mvn clean package -DskipTests
    echo "✓ Build completed"
}

# Run tests
run_tests() {
    echo ""
    echo "Running integration tests..."
    export SEAWEEDFS_TEST_ENABLED=true
    mvn test
    echo "✓ Tests completed"
}

# Run example
run_example() {
    echo ""
    echo "Running example application..."
    
    if ! command -v spark-submit > /dev/null; then
        echo "⚠ spark-submit not found. Skipping example application."
        echo "To run the example, install Apache Spark and try: make run-example"
        return 0
    fi
    
    spark-submit \
        --class seaweed.spark.SparkSeaweedFSExample \
        --master local[2] \
        --conf spark.hadoop.fs.seaweedfs.impl=seaweed.hdfs.SeaweedFileSystem \
        --conf spark.hadoop.fs.seaweed.filer.host=localhost \
        --conf spark.hadoop.fs.seaweed.filer.port=8888 \
        --conf spark.hadoop.fs.seaweed.filer.port.grpc=18888 \
        target/seaweedfs-spark-integration-tests-1.0-SNAPSHOT.jar \
        seaweedfs://localhost:8888/spark-quickstart-output
    
    echo "✓ Example completed"
}

# Cleanup
cleanup() {
    echo ""
    echo "Cleaning up..."
    docker-compose down -v
    echo "✓ Cleanup completed"
}

# Main execution
main() {
    # Check if Docker is available
    if ! command -v docker > /dev/null; then
        echo "Error: Docker is not installed or not in PATH"
        exit 1
    fi

    # Check if Maven is available
    if ! command -v mvn > /dev/null; then
        echo "Error: Maven is not installed or not in PATH"
        exit 1
    fi

    # Check if SeaweedFS is running, if not start it
    if ! check_seaweedfs; then
        read -p "Do you want to start SeaweedFS with Docker? (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            start_seaweedfs || exit 1
        else
            echo "Please start SeaweedFS manually and rerun this script."
            exit 1
        fi
    fi

    # Build project
    build_project || exit 1

    # Run tests
    run_tests || exit 1

    # Run example if Spark is available
    run_example

    echo ""
    echo "=== Quick Start Completed Successfully! ==="
    echo ""
    echo "Next steps:"
    echo "  - View test results in target/surefire-reports/"
    echo "  - Check example output at http://localhost:8888/"
    echo "  - Run 'make help' for more options"
    echo "  - Read README.md for detailed documentation"
    echo ""
    
    read -p "Do you want to stop SeaweedFS? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cleanup
    fi
}

# Handle Ctrl+C
trap cleanup INT

# Run main
main


