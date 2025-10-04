#!/bin/bash

set -e

echo "=== Building Java Test Container ==="
cd "$(dirname "$0")"

# Build the Docker image
docker build -f Dockerfile.java -t kafka-java-test .

echo ""
echo "=== Running Schema Registry Reproducer ==="

# Run the test in the Docker network
docker run --rm \
    --network kafka-client-loadtest \
    kafka-java-test \
    java -cp ".:kafka-clients-8.0.0-ccs.jar:slf4j-api-1.7.36.jar:slf4j-simple-1.7.36.jar" \
    SchemaRegistryReproducer

echo ""
echo "=== Test Complete ==="
