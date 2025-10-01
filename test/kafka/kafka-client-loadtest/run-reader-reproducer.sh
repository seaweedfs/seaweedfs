#!/bin/bash

# Script to build and run the Schema Registry reader thread reproducer

set -e

echo "Building Java reproducer..."
cd debug-client

# Compile
docker run --rm \
  -v "$(pwd)":/work \
  -w /work \
  eclipse-temurin:17-jdk \
  javac -cp "kafka-clients-3.9.0.jar:." SchemaReaderThreadReproducer.java

echo "Running reproducer in Docker network..."
docker run --rm \
  --network kafka-client-loadtest \
  -v "$(pwd)":/work \
  -w /work \
  eclipse-temurin:17-jre \
  java -cp "kafka-clients-3.9.0.jar:." SchemaReaderThreadReproducer

echo "Done!"

