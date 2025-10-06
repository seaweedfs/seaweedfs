#!/bin/bash

# Wait for Services Script for Kafka Integration Tests

set -e

echo "Waiting for services to be ready..."

# Configuration
KAFKA_HOST=${KAFKA_HOST:-localhost}
KAFKA_PORT=${KAFKA_PORT:-9092}
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://localhost:8081}
KAFKA_GATEWAY_HOST=${KAFKA_GATEWAY_HOST:-localhost}
KAFKA_GATEWAY_PORT=${KAFKA_GATEWAY_PORT:-9093}
SEAWEEDFS_MASTER_URL=${SEAWEEDFS_MASTER_URL:-http://localhost:9333}
MAX_WAIT=${MAX_WAIT:-300}  # 5 minutes

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper function to wait for a service
wait_for_service() {
    local service_name=$1
    local check_command=$2
    local timeout=${3:-60}
    
    echo -e "${BLUE}Waiting for ${service_name}...${NC}"
    
    local count=0
    while [ $count -lt $timeout ]; do
        if eval "$check_command" > /dev/null 2>&1; then
            echo -e "${GREEN}[OK] ${service_name} is ready${NC}"
            return 0
        fi
        
        if [ $((count % 10)) -eq 0 ]; then
            echo -e "${YELLOW}Still waiting for ${service_name}... (${count}s)${NC}"
        fi
        
        sleep 1
        count=$((count + 1))
    done
    
    echo -e "${RED}[FAIL] ${service_name} failed to start within ${timeout} seconds${NC}"
    return 1
}

# Wait for Zookeeper
echo "=== Checking Zookeeper ==="
wait_for_service "Zookeeper" "nc -z localhost 2181" 30

# Wait for Kafka
echo "=== Checking Kafka ==="
wait_for_service "Kafka" "nc -z ${KAFKA_HOST} ${KAFKA_PORT}" 60

# Test Kafka broker API
echo "=== Testing Kafka API ==="
wait_for_service "Kafka API" "timeout 5 kafka-broker-api-versions --bootstrap-server ${KAFKA_HOST}:${KAFKA_PORT}" 30

# Wait for Schema Registry
echo "=== Checking Schema Registry ==="
wait_for_service "Schema Registry" "curl -f ${SCHEMA_REGISTRY_URL}/subjects" 60

# Wait for SeaweedFS Master
echo "=== Checking SeaweedFS Master ==="
wait_for_service "SeaweedFS Master" "curl -f ${SEAWEEDFS_MASTER_URL}/cluster/status" 30

# Wait for SeaweedFS Volume
echo "=== Checking SeaweedFS Volume ==="
wait_for_service "SeaweedFS Volume" "curl -f http://localhost:8080/status" 30

# Wait for SeaweedFS Filer
echo "=== Checking SeaweedFS Filer ==="
wait_for_service "SeaweedFS Filer" "curl -f http://localhost:8888/" 30

# Wait for SeaweedFS MQ Broker
echo "=== Checking SeaweedFS MQ Broker ==="
wait_for_service "SeaweedFS MQ Broker" "nc -z localhost 17777" 30

# Wait for SeaweedFS MQ Agent
echo "=== Checking SeaweedFS MQ Agent ==="
wait_for_service "SeaweedFS MQ Agent" "nc -z localhost 16777" 30

# Wait for Kafka Gateway
echo "=== Checking Kafka Gateway ==="
wait_for_service "Kafka Gateway" "nc -z ${KAFKA_GATEWAY_HOST} ${KAFKA_GATEWAY_PORT}" 60

# Final verification
echo "=== Final Verification ==="

# Test Kafka topic creation
echo "Testing Kafka topic operations..."
TEST_TOPIC="health-check-$(date +%s)"
if kafka-topics --create --topic "$TEST_TOPIC" --bootstrap-server "${KAFKA_HOST}:${KAFKA_PORT}" --partitions 1 --replication-factor 1 > /dev/null 2>&1; then
    echo -e "${GREEN}[OK] Kafka topic creation works${NC}"
    kafka-topics --delete --topic "$TEST_TOPIC" --bootstrap-server "${KAFKA_HOST}:${KAFKA_PORT}" > /dev/null 2>&1 || true
else
    echo -e "${RED}[FAIL] Kafka topic creation failed${NC}"
    exit 1
fi

# Test Schema Registry
echo "Testing Schema Registry..."
if curl -f "${SCHEMA_REGISTRY_URL}/subjects" > /dev/null 2>&1; then
    echo -e "${GREEN}[OK] Schema Registry is accessible${NC}"
else
    echo -e "${RED}[FAIL] Schema Registry is not accessible${NC}"
    exit 1
fi

# Test Kafka Gateway connectivity
echo "Testing Kafka Gateway..."
if nc -z "${KAFKA_GATEWAY_HOST}" "${KAFKA_GATEWAY_PORT}"; then
    echo -e "${GREEN}[OK] Kafka Gateway is accessible${NC}"
else
    echo -e "${RED}[FAIL] Kafka Gateway is not accessible${NC}"
    exit 1
fi

echo -e "${GREEN}All services are ready!${NC}"
echo ""
echo "Service endpoints:"
echo "  Kafka: ${KAFKA_HOST}:${KAFKA_PORT}"
echo "  Schema Registry: ${SCHEMA_REGISTRY_URL}"
echo "  Kafka Gateway: ${KAFKA_GATEWAY_HOST}:${KAFKA_GATEWAY_PORT}"
echo "  SeaweedFS Master: ${SEAWEEDFS_MASTER_URL}"
echo "  SeaweedFS Filer: http://localhost:8888"
echo "  SeaweedFS MQ Broker: localhost:17777"
echo "  SeaweedFS MQ Agent: localhost:16777"
echo ""
echo "Ready to run integration tests!"
