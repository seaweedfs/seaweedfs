#!/bin/bash

# Kafka Client Load Test Runner Script
# This script helps run various load test scenarios against SeaweedFS Kafka Gateway

set -euo pipefail

# Default configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_COMPOSE_FILE="$PROJECT_DIR/docker-compose.yml"
CONFIG_FILE="$PROJECT_DIR/config/loadtest.yaml"

# Default test parameters
TEST_MODE="comprehensive"
TEST_DURATION="300s"
PRODUCER_COUNT=10
CONSUMER_COUNT=5
MESSAGE_RATE=1000
MESSAGE_SIZE=1024
TOPIC_COUNT=5
PARTITIONS_PER_TOPIC=3

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    cat << EOF
Kafka Client Load Test Runner

Usage: $0 [OPTIONS] [COMMAND]

Commands:
  start               Start the load test infrastructure and run tests
  stop                Stop all services
  restart             Restart all services
  status              Show service status
  logs                Show logs from all services
  clean               Clean up all resources (volumes, networks, etc.)
  monitor             Start monitoring stack (Prometheus + Grafana)
  scenarios           Run predefined test scenarios

Options:
  -m, --mode MODE           Test mode: producer, consumer, comprehensive (default: comprehensive)
  -d, --duration DURATION   Test duration (default: 300s)
  -p, --producers COUNT     Number of producers (default: 10)
  -c, --consumers COUNT     Number of consumers (default: 5)
  -r, --rate RATE          Messages per second per producer (default: 1000)
  -s, --size SIZE          Message size in bytes (default: 1024)
  -t, --topics COUNT       Number of topics (default: 5)
  --partitions COUNT       Partitions per topic (default: 3)
  --config FILE           Configuration file (default: config/loadtest.yaml)
  --monitoring            Enable monitoring stack
  --wait-ready            Wait for services to be ready before starting tests
  -v, --verbose           Verbose output
  -h, --help              Show this help message

Examples:
  # Run comprehensive test for 5 minutes
  $0 start -m comprehensive -d 5m

  # Run producer-only test with high throughput
  $0 start -m producer -p 20 -r 2000 -d 10m

  # Run consumer-only test
  $0 start -m consumer -c 10

  # Run with monitoring
  $0 start --monitoring -d 15m

  # Clean up everything
  $0 clean

Predefined Scenarios:
  quick              Quick smoke test (1 min, low load)
  standard           Standard load test (5 min, medium load) 
  stress             Stress test (10 min, high load)
  endurance          Endurance test (30 min, sustained load)
  burst              Burst test (variable load)

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -m|--mode)
                TEST_MODE="$2"
                shift 2
                ;;
            -d|--duration)
                TEST_DURATION="$2"
                shift 2
                ;;
            -p|--producers)
                PRODUCER_COUNT="$2"
                shift 2
                ;;
            -c|--consumers)
                CONSUMER_COUNT="$2"
                shift 2
                ;;
            -r|--rate)
                MESSAGE_RATE="$2"
                shift 2
                ;;
            -s|--size)
                MESSAGE_SIZE="$2"
                shift 2
                ;;
            -t|--topics)
                TOPIC_COUNT="$2"
                shift 2
                ;;
            --partitions)
                PARTITIONS_PER_TOPIC="$2"
                shift 2
                ;;
            --config)
                CONFIG_FILE="$2"
                shift 2
                ;;
            --monitoring)
                ENABLE_MONITORING=1
                shift
                ;;
            --wait-ready)
                WAIT_READY=1
                shift
                ;;
            -v|--verbose)
                VERBOSE=1
                shift
                ;;
            -h|--help)
                show_usage
                exit 0
                ;;
            -*)
                log_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
            *)
                if [[ -z "${COMMAND:-}" ]]; then
                    COMMAND="$1"
                else
                    log_error "Multiple commands specified"
                    show_usage
                    exit 1
                fi
                shift
                ;;
        esac
    done
}

# Check if Docker and Docker Compose are available
check_dependencies() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not installed or not in PATH"
        exit 1
    fi
    
    # Use docker compose if available, otherwise docker-compose
    if docker compose version &> /dev/null; then
        DOCKER_COMPOSE="docker compose"
    else
        DOCKER_COMPOSE="docker-compose"
    fi
}

# Wait for services to be ready
wait_for_services() {
    log_info "Waiting for services to be ready..."
    
    local timeout=300  # 5 minutes timeout
    local elapsed=0
    local check_interval=5
    
    while [[ $elapsed -lt $timeout ]]; do
        if $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" ps --format table | grep -q "healthy"; then
            if check_service_health; then
                log_success "All services are ready!"
                return 0
            fi
        fi
        
        sleep $check_interval
        elapsed=$((elapsed + check_interval))
        log_info "Waiting... ($elapsed/${timeout}s)"
    done
    
    log_error "Services did not become ready within $timeout seconds"
    return 1
}

# Check health of critical services
check_service_health() {
    # Check Kafka Gateway
    if ! curl -s http://localhost:9093 >/dev/null 2>&1; then
        return 1
    fi
    
    # Check Schema Registry
    if ! curl -s http://localhost:8081/subjects >/dev/null 2>&1; then
        return 1
    fi
    
    return 0
}

# Start the load test infrastructure
start_services() {
    log_info "Starting SeaweedFS Kafka load test infrastructure..."
    
    # Set environment variables
    export TEST_MODE="$TEST_MODE"
    export TEST_DURATION="$TEST_DURATION"
    export PRODUCER_COUNT="$PRODUCER_COUNT"
    export CONSUMER_COUNT="$CONSUMER_COUNT"
    export MESSAGE_RATE="$MESSAGE_RATE"
    export MESSAGE_SIZE="$MESSAGE_SIZE"
    export TOPIC_COUNT="$TOPIC_COUNT"
    export PARTITIONS_PER_TOPIC="$PARTITIONS_PER_TOPIC"
    
    # Start core services
    $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" up -d \
        seaweedfs-master \
        seaweedfs-volume \
        seaweedfs-filer \
        seaweedfs-mq-broker \
        kafka-gateway \
        schema-registry
    
    # Start monitoring if enabled
    if [[ "${ENABLE_MONITORING:-0}" == "1" ]]; then
        log_info "Starting monitoring stack..."
        $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" --profile monitoring up -d
    fi
    
    # Wait for services to be ready if requested
    if [[ "${WAIT_READY:-0}" == "1" ]]; then
        wait_for_services
    fi
    
    log_success "Infrastructure started successfully"
}

# Run the load test
run_loadtest() {
    log_info "Starting Kafka client load test..."
    log_info "Mode: $TEST_MODE, Duration: $TEST_DURATION"
    log_info "Producers: $PRODUCER_COUNT, Consumers: $CONSUMER_COUNT"
    log_info "Message Rate: $MESSAGE_RATE msgs/sec, Size: $MESSAGE_SIZE bytes"
    
    # Run the load test
    $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" --profile loadtest up --abort-on-container-exit kafka-client-loadtest
    
    # Show test results
    show_results
}

# Show test results
show_results() {
    log_info "Load test completed! Gathering results..."
    
    # Get final metrics from the load test container
    if $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" ps kafka-client-loadtest-runner &>/dev/null; then
        log_info "Final test statistics:"
        $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" exec -T kafka-client-loadtest-runner curl -s http://localhost:8080/stats || true
    fi
    
    # Show Prometheus metrics if monitoring is enabled
    if [[ "${ENABLE_MONITORING:-0}" == "1" ]]; then
        log_info "Monitoring dashboards available at:"
        log_info "  Prometheus: http://localhost:9090"
        log_info "  Grafana:    http://localhost:3000 (admin/admin)"
    fi
    
    # Show where results are stored
    if [[ -d "$PROJECT_DIR/test-results" ]]; then
        log_info "Test results saved to: $PROJECT_DIR/test-results/"
    fi
}

# Stop services
stop_services() {
    log_info "Stopping all services..."
    $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" --profile loadtest --profile monitoring down
    log_success "Services stopped"
}

# Show service status
show_status() {
    log_info "Service status:"
    $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" ps
}

# Show logs
show_logs() {
    $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" logs -f "${1:-}"
}

# Clean up all resources
clean_all() {
    log_warning "This will remove all volumes, networks, and containers. Are you sure? (y/N)"
    read -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        log_info "Cleaning up all resources..."
        $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" --profile loadtest --profile monitoring down -v --remove-orphans
        
        # Remove any remaining volumes
        docker volume ls -q | grep -E "(kafka-client-loadtest|seaweedfs)" | xargs -r docker volume rm
        
        # Remove networks
        docker network ls -q | grep -E "kafka-client-loadtest" | xargs -r docker network rm
        
        log_success "Cleanup completed"
    else
        log_info "Cleanup cancelled"
    fi
}

# Run predefined scenarios
run_scenario() {
    local scenario="$1"
    
    case "$scenario" in
        quick)
            TEST_MODE="comprehensive"
            TEST_DURATION="1m"
            PRODUCER_COUNT=2
            CONSUMER_COUNT=2
            MESSAGE_RATE=100
            MESSAGE_SIZE=512
            TOPIC_COUNT=2
            ;;
        standard)
            TEST_MODE="comprehensive"
            TEST_DURATION="5m"
            PRODUCER_COUNT=5
            CONSUMER_COUNT=3
            MESSAGE_RATE=500
            MESSAGE_SIZE=1024
            TOPIC_COUNT=3
            ;;
        stress)
            TEST_MODE="comprehensive"
            TEST_DURATION="10m"
            PRODUCER_COUNT=20
            CONSUMER_COUNT=10
            MESSAGE_RATE=2000
            MESSAGE_SIZE=2048
            TOPIC_COUNT=10
            ;;
        endurance)
            TEST_MODE="comprehensive"
            TEST_DURATION="30m"
            PRODUCER_COUNT=10
            CONSUMER_COUNT=5
            MESSAGE_RATE=1000
            MESSAGE_SIZE=1024
            TOPIC_COUNT=5
            ;;
        burst)
            TEST_MODE="comprehensive"
            TEST_DURATION="10m"
            PRODUCER_COUNT=10
            CONSUMER_COUNT=5
            MESSAGE_RATE=1000
            MESSAGE_SIZE=1024
            TOPIC_COUNT=5
            # Note: Burst behavior would be configured in the load test config
            ;;
        *)
            log_error "Unknown scenario: $scenario"
            log_info "Available scenarios: quick, standard, stress, endurance, burst"
            exit 1
            ;;
    esac
    
    log_info "Running $scenario scenario..."
    start_services
    if [[ "${WAIT_READY:-0}" == "1" ]]; then
        wait_for_services
    fi
    run_loadtest
}

# Main execution
main() {
    if [[ $# -eq 0 ]]; then
        show_usage
        exit 0
    fi
    
    parse_args "$@"
    check_dependencies
    
    case "${COMMAND:-}" in
        start)
            start_services
            run_loadtest
            ;;
        stop)
            stop_services
            ;;
        restart)
            stop_services
            start_services
            ;;
        status)
            show_status
            ;;
        logs)
            show_logs
            ;;
        clean)
            clean_all
            ;;
        monitor)
            ENABLE_MONITORING=1
            $DOCKER_COMPOSE -f "$DOCKER_COMPOSE_FILE" --profile monitoring up -d
            log_success "Monitoring stack started"
            log_info "Prometheus: http://localhost:9090"
            log_info "Grafana:    http://localhost:3000 (admin/admin)"
            ;;
        scenarios)
            if [[ -n "${2:-}" ]]; then
                run_scenario "$2"
            else
                log_error "Please specify a scenario"
                log_info "Available scenarios: quick, standard, stress, endurance, burst"
                exit 1
            fi
            ;;
        *)
            log_error "Unknown command: ${COMMAND:-}"
            show_usage
            exit 1
            ;;
    esac
}

# Set default values
ENABLE_MONITORING=0
WAIT_READY=0
VERBOSE=0

# Run main function
main "$@"
