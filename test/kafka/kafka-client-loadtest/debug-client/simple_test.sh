#!/bin/bash
echo "=== Schema Registry Simulator - Quick Test ==="
echo "Testing Kafka Gateway from inside Docker network..."

# Use a lightweight container with Java
docker run --rm --network kafka-client-loadtest \
  -v $(pwd):/workspace \
  -w /workspace \
  eclipse-temurin:17-jre-alpine \
  sh -c "
    echo 'Installing curl...'
    apk add --no-cache curl
    
    echo 'Downloading Kafka clients JAR...'
    curl -L -o kafka-clients.jar 'https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar'
    
    echo 'Downloading SLF4J JARs...'
    curl -L -o slf4j-api.jar 'https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar'
    curl -L -o slf4j-simple.jar 'https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar'
    
    echo 'Compiling Java simulator...'
    javac -cp kafka-clients.jar SchemaRegistrySimulator.java
    
    echo 'Running Schema Registry simulator...'
    java -cp .:kafka-clients.jar:slf4j-api.jar:slf4j-simple.jar SchemaRegistrySimulator
  "
