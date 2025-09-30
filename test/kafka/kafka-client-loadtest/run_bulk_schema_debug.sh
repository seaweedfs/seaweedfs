#!/bin/bash

# Comprehensive Bulk Schema Debug Test Runner
set -e

echo "🔧 Bulk Schema Operations Debug Test Suite"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test configuration
DOCKER_NETWORK="kafka-client-loadtest"
KAFKA_GATEWAY="kafka-gateway:9093"
SCHEMA_REGISTRY="http://schema-registry:8081"

# Function to run a test inside Docker
run_test_in_docker() {
    local test_name=$1
    local test_file=$2
    
    echo -e "${BLUE}[INFO]${NC} Running $test_name..."
    
    # Build the test
    echo -e "${BLUE}[INFO]${NC} Building $test_file..."
    docker run --rm \
        --network $DOCKER_NETWORK \
        -v $(pwd):/workspace \
        -w /workspace \
        golang:1.21-alpine \
        sh -c "
            apk add --no-cache git && \
            go mod tidy && \
            go build -o /tmp/$test_name $test_file
        "
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}[ERROR]${NC} Failed to build $test_file"
        return 1
    fi
    
    # Run the test
    echo -e "${BLUE}[INFO]${NC} Executing $test_name..."
    docker run --rm \
        --network $DOCKER_NETWORK \
        -v $(pwd):/workspace \
        -w /workspace \
        golang:1.21-alpine \
        /tmp/$test_name
    
    local exit_code=$?
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}[SUCCESS]${NC} $test_name completed successfully"
    else
        echo -e "${RED}[ERROR]${NC} $test_name failed with exit code $exit_code"
    fi
    
    return $exit_code
}

# Function to check service health
check_service_health() {
    local service_name=$1
    local health_url=$2
    local max_attempts=30
    local attempt=1
    
    echo -e "${BLUE}[INFO]${NC} Checking $service_name health..."
    
    while [ $attempt -le $max_attempts ]; do
        if docker run --rm --network $DOCKER_NETWORK curlimages/curl:latest \
           curl -s -f $health_url > /dev/null 2>&1; then
            echo -e "${GREEN}[SUCCESS]${NC} $service_name is healthy"
            return 0
        fi
        
        echo -e "${YELLOW}[WAIT]${NC} $service_name not ready, attempt $attempt/$max_attempts..."
        sleep 2
        ((attempt++))
    done
    
    echo -e "${RED}[ERROR]${NC} $service_name failed to become healthy"
    return 1
}

# Function to show Docker logs
show_logs() {
    local service=$1
    local lines=${2:-50}
    
    echo -e "${BLUE}[INFO]${NC} Last $lines lines of $service logs:"
    docker compose logs --tail=$lines $service
}

# Main execution
main() {
    echo -e "${BLUE}[INFO]${NC} Starting bulk schema debug test suite..."
    
    # Check if services are running
    echo -e "${BLUE}[INFO]${NC} Checking if services are running..."
    if ! docker compose ps | grep -q "Up"; then
        echo -e "${YELLOW}[WARN]${NC} Services not running, starting them..."
        docker compose up -d
        sleep 10
    fi
    
    # Wait for services to be healthy
    echo -e "${BLUE}[INFO]${NC} Waiting for services to be healthy..."
    
    if ! check_service_health "Kafka Gateway" "http://$KAFKA_GATEWAY"; then
        echo -e "${RED}[ERROR]${NC} Kafka Gateway is not healthy"
        show_logs "kafka-gateway"
        exit 1
    fi
    
    if ! check_service_health "Schema Registry" "$SCHEMA_REGISTRY/subjects"; then
        echo -e "${RED}[ERROR]${NC} Schema Registry is not healthy"
        show_logs "schema-registry"
        exit 1
    fi
    
    echo -e "${GREEN}[SUCCESS]${NC} All services are healthy!"
    
    # Run the test suite
    echo -e "${BLUE}[INFO]${NC} Starting test execution..."
    
    # Test 1: Layer-by-layer bulk schema debug
    echo -e "\n${BLUE}===========================================${NC}"
    echo -e "${BLUE}Test 1: Layer-by-Layer Bulk Schema Debug${NC}"
    echo -e "${BLUE}===========================================${NC}"
    
    if run_test_in_docker "bulk_schema_debug" "test_bulk_schema_debug.go"; then
        echo -e "${GREEN}[SUCCESS]${NC} Bulk schema debug test passed"
    else
        echo -e "${RED}[ERROR]${NC} Bulk schema debug test failed"
        show_logs "schema-registry" 100
        show_logs "kafka-gateway" 100
    fi
    
    echo -e "\n${BLUE}Waiting 5 seconds before next test...${NC}"
    sleep 5
    
    # Test 2: Schema Registry internals
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}Test 2: Schema Registry Internals${NC}"
    echo -e "${BLUE}========================================${NC}"
    
    if run_test_in_docker "schema_registry_internals" "test_schema_registry_internals.go"; then
        echo -e "${GREEN}[SUCCESS]${NC} Schema Registry internals test passed"
    else
        echo -e "${RED}[ERROR]${NC} Schema Registry internals test failed"
        show_logs "schema-registry" 100
        show_logs "kafka-gateway" 100
    fi
    
    echo -e "\n${BLUE}Waiting 5 seconds before next test...${NC}"
    sleep 5
    
    # Test 3: SeaweedMQ _schemas performance
    echo -e "\n${BLUE}===========================================${NC}"
    echo -e "${BLUE}Test 3: SeaweedMQ _schemas Performance${NC}"
    echo -e "${BLUE}===========================================${NC}"
    
    if run_test_in_docker "seaweedmq_schemas_performance" "test_seaweedmq_schemas_performance.go"; then
        echo -e "${GREEN}[SUCCESS]${NC} SeaweedMQ _schemas performance test passed"
    else
        echo -e "${RED}[ERROR]${NC} SeaweedMQ _schemas performance test failed"
        show_logs "seaweedfs-mq-broker" 100
        show_logs "kafka-gateway" 100
    fi
    
    # Summary
    echo -e "\n${BLUE}===========================================${NC}"
    echo -e "${BLUE}Test Suite Summary${NC}"
    echo -e "${BLUE}===========================================${NC}"
    
    echo -e "${GREEN}[INFO]${NC} All tests completed!"
    echo -e "${BLUE}[INFO]${NC} Check the logs above for detailed analysis"
    
    # Show current service status
    echo -e "\n${BLUE}[INFO]${NC} Current service status:"
    docker compose ps
    
    # Show _schemas topic status
    echo -e "\n${BLUE}[INFO]${NC} Final _schemas topic status:"
    docker run --rm --network $DOCKER_NETWORK \
        -v $(pwd):/workspace \
        -w /workspace \
        golang:1.21-alpine \
        sh -c "
            apk add --no-cache git && \
            go mod tidy && \
            go run -c '
                package main
                import (
                    \"fmt\"
                    \"github.com/IBM/sarama\"
                )
                func main() {
                    config := sarama.NewConfig()
                    config.Version = sarama.V2_6_0_0
                    client, err := sarama.NewClient([]string{\"kafka-gateway:9093\"}, config)
                    if err != nil {
                        fmt.Printf(\"Error: %v\", err)
                        return
                    }
                    defer client.Close()
                    
                    oldest, _ := client.GetOffset(\"_schemas\", 0, sarama.OffsetOldest)
                    newest, _ := client.GetOffset(\"_schemas\", 0, sarama.OffsetNewest)
                    fmt.Printf(\"_schemas topic: oldest=%d, newest=%d, messages=%d\", oldest, newest, newest-oldest)
                }
            '
        " 2>/dev/null || echo "Could not get _schemas topic status"
}

# Handle script arguments
case "${1:-run}" in
    "run")
        main
        ;;
    "clean")
        echo -e "${BLUE}[INFO]${NC} Cleaning up test environment..."
        docker compose down -v
        docker system prune -f
        echo -e "${GREEN}[SUCCESS]${NC} Cleanup completed"
        ;;
    "logs")
        service=${2:-"all"}
        if [ "$service" = "all" ]; then
            docker compose logs --tail=100
        else
            show_logs $service 100
        fi
        ;;
    "status")
        echo -e "${BLUE}[INFO]${NC} Service status:"
        docker compose ps
        ;;
    *)
        echo "Usage: $0 [run|clean|logs [service]|status]"
        echo "  run    - Run the test suite (default)"
        echo "  clean  - Clean up test environment"
        echo "  logs   - Show logs for all services or specific service"
        echo "  status - Show service status"
        exit 1
        ;;
esac

