#!/bin/bash

echo "=== Building Schema Registry Simulator ==="

# Download required JARs if not present
KAFKA_JAR="kafka-clients-8.0.0-ccs.jar"
SLF4J_API_JAR="slf4j-api-1.7.36.jar"
SLF4J_SIMPLE_JAR="slf4j-simple-1.7.36.jar"

if [ ! -f "$KAFKA_JAR" ]; then
    echo "Downloading Kafka clients JAR..."
    curl -L -o "$KAFKA_JAR" "https://packages.confluent.io/maven/org/apache/kafka/kafka-clients/8.0.0-ccs/kafka-clients-8.0.0-ccs.jar"
fi

if [ ! -f "$SLF4J_API_JAR" ]; then
    echo "Downloading SLF4J API JAR..."
    curl -L -o "$SLF4J_API_JAR" "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar"
fi

if [ ! -f "$SLF4J_SIMPLE_JAR" ]; then
    echo "Downloading SLF4J Simple JAR..."
    curl -L -o "$SLF4J_SIMPLE_JAR" "https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar"
fi

CLASSPATH=".:$KAFKA_JAR:$SLF4J_API_JAR:$SLF4J_SIMPLE_JAR"

# Compile the Java class
echo "Compiling SchemaRegistrySimulator.java..."
javac -cp "$CLASSPATH" SchemaRegistrySimulator.java

if [ $? -eq 0 ]; then
    echo "✅ Compilation successful!"
    echo ""
    echo "To run the simulator:"
    echo "  java -cp $CLASSPATH SchemaRegistrySimulator"
else
    echo "❌ Compilation failed!"
    exit 1
fi
