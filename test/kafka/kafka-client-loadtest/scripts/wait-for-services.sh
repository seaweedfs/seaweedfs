#!/bin/bash

# Wait for SeaweedFS and Kafka Gateway services to be ready
# This script checks service health and waits until all services are operational

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

# Configuration
TIMEOUT=${TIMEOUT:-300}  # 5 minutes default timeout
CHECK_INTERVAL=${CHECK_INTERVAL:-5}  # Check every 5 seconds
SEAWEEDFS_MASTER_URL=${SEAWEEDFS_MASTER_URL:-"http://localhost:9333"}
KAFKA_GATEWAY_URL=${KAFKA_GATEWAY_URL:-"localhost:9093"}
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-"http://localhost:8081"}
SEAWEEDFS_FILER_URL=${SEAWEEDFS_FILER_URL:-"http://localhost:8888"}

# Check if a service is reachable
check_http_service() {
    local url=$1
    local name=$2
    
    if curl -sf "$url" >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Check TCP port
check_tcp_service() {
    local host=$1
    local port=$2
    local name=$3
    
    if timeout 3 bash -c "</dev/tcp/$host/$port" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Check SeaweedFS Master
check_seaweedfs_master() {
    if check_http_service "$SEAWEEDFS_MASTER_URL/cluster/status" "SeaweedFS Master"; then
        # Additional check: ensure cluster has volumes
        local status_json
        status_json=$(curl -s "$SEAWEEDFS_MASTER_URL/cluster/status" 2>/dev/null || echo "{}")
        
        # Check if we have at least one volume server
        if echo "$status_json" | grep -q '"Max":0'; then
            log_warning "SeaweedFS Master is running but no volumes are available"
            return 1
        fi
        
        return 0
    fi
    return 1
}

# Check SeaweedFS Filer
check_seaweedfs_filer() {
    check_http_service "$SEAWEEDFS_FILER_URL/" "SeaweedFS Filer"
}

# Check Kafka Gateway
check_kafka_gateway() {
    local host="localhost"
    local port="9093"
    check_tcp_service "$host" "$port" "Kafka Gateway"
}

# Check Schema Registry
check_schema_registry() {
    # Check if Schema Registry container is running first
    if ! docker compose ps schema-registry | grep -q "Up"; then
        # Schema Registry is not running, which is okay for basic tests
        return 0
    fi
    
    # FIXED: Wait for Docker healthcheck to report "healthy", not just "Up"
    # Schema Registry has a 30s start_period, so we need to wait for the actual healthcheck
    local health_status
    health_status=$(docker inspect loadtest-schema-registry --format='{{.State.Health.Status}}' 2>/dev/null || echo "none")
    
    # If container has no healthcheck or healthcheck is not yet healthy, check HTTP directly
    if [[ "$health_status" == "healthy" ]]; then
        # Container reports healthy, do a final verification
        if check_http_service "$SCHEMA_REGISTRY_URL/subjects" "Schema Registry"; then
            return 0
        fi
    elif [[ "$health_status" == "starting" ]]; then
        # Still in startup period, wait longer
        return 1
    elif [[ "$health_status" == "none" ]]; then
        # No healthcheck defined (shouldn't happen), fall back to HTTP check
        if check_http_service "$SCHEMA_REGISTRY_URL/subjects" "Schema Registry"; then
            local subjects
            subjects=$(curl -s "$SCHEMA_REGISTRY_URL/subjects" 2>/dev/null || echo "[]")
            
            # Schema registry should at least return an empty array
            if [[ "$subjects" == "[]" ]]; then
                return 0
            elif echo "$subjects" | grep -q '\['; then
                return 0
            else
                log_warning "Schema Registry is not properly connected"
                return 1
            fi
        fi
    fi
    return 1
}

# Check MQ Broker
check_mq_broker() {
    check_tcp_service "localhost" "17777" "SeaweedFS MQ Broker"
}

# Main health check function
check_all_services() {
    local all_healthy=true
    
    log_info "Checking service health..."
    
    # Check SeaweedFS Master
    if check_seaweedfs_master; then
        log_success "✓ SeaweedFS Master is healthy"
    else
        log_error "✗ SeaweedFS Master is not ready"
        all_healthy=false
    fi
    
    # Check SeaweedFS Filer
    if check_seaweedfs_filer; then
        log_success "✓ SeaweedFS Filer is healthy"
    else
        log_error "✗ SeaweedFS Filer is not ready"
        all_healthy=false
    fi
    
    # Check MQ Broker
    if check_mq_broker; then
        log_success "✓ SeaweedFS MQ Broker is healthy"
    else
        log_error "✗ SeaweedFS MQ Broker is not ready"
        all_healthy=false
    fi
    
    # Check Kafka Gateway
    if check_kafka_gateway; then
        log_success "✓ Kafka Gateway is healthy"
    else
        log_error "✗ Kafka Gateway is not ready"
        all_healthy=false
    fi
    
    # Check Schema Registry
    if ! docker compose ps schema-registry | grep -q "Up"; then
        log_warning "⚠ Schema Registry is stopped (skipping)"
    elif check_schema_registry; then
        log_success "✓ Schema Registry is healthy"
    else
        # Check if it's still starting up (healthcheck start_period)
        local health_status
        health_status=$(docker inspect loadtest-schema-registry --format='{{.State.Health.Status}}' 2>/dev/null || echo "unknown")
        if [[ "$health_status" == "starting" ]]; then
            log_warning "⏳ Schema Registry is starting (waiting for healthcheck...)"
        else
            log_error "✗ Schema Registry is not ready (status: $health_status)"
        fi
        all_healthy=false
    fi
    
    $all_healthy
}

# Wait for all services to be ready
wait_for_services() {
    log_info "Waiting for all services to be ready (timeout: ${TIMEOUT}s)..."
    
    local elapsed=0
    
    while [[ $elapsed -lt $TIMEOUT ]]; do
        if check_all_services; then
            log_success "All services are ready! (took ${elapsed}s)"
            return 0
        fi
        
        log_info "Some services are not ready yet. Waiting ${CHECK_INTERVAL}s... (${elapsed}/${TIMEOUT}s)"
        sleep $CHECK_INTERVAL
        elapsed=$((elapsed + CHECK_INTERVAL))
    done
    
    log_error "Services did not become ready within ${TIMEOUT} seconds"
    log_error "Final service status:"
    check_all_services
    
    # Always dump Schema Registry diagnostics on timeout since it's the problematic service
    log_error "==========================================="
    log_error "Schema Registry Container Status:"
    log_error "==========================================="
    docker compose ps schema-registry 2>&1 || echo "Failed to get container status"
    docker inspect loadtest-schema-registry --format='Health: {{.State.Health.Status}} ({{len .State.Health.Log}} checks)' 2>&1 || echo "Failed to inspect container"
    log_error "==========================================="
    
    log_error "Network Connectivity Check:"
    log_error "==========================================="
    log_error "Can Schema Registry reach Kafka Gateway?"
    docker compose exec -T schema-registry ping -c 3 kafka-gateway 2>&1 || echo "Ping failed"
    docker compose exec -T schema-registry nc -zv kafka-gateway 9093 2>&1 || echo "Port 9093 unreachable"
    log_error "==========================================="
    
    log_error "Schema Registry Logs (last 100 lines):"
    log_error "==========================================="
    docker compose logs --tail=100 schema-registry 2>&1 || echo "Failed to get Schema Registry logs"
    log_error "==========================================="
    
    log_error "Kafka Gateway Logs (last 50 lines with 'SR' prefix):"
    log_error "==========================================="
    docker compose logs --tail=200 kafka-gateway 2>&1 | grep -i "SR" | tail -50 || echo "No SR-related logs found in Kafka Gateway"
    log_error "==========================================="
    
    log_error "MQ Broker Logs (last 30 lines):"
    log_error "==========================================="
    docker compose logs --tail=30 seaweedfs-mq-broker 2>&1 || echo "Failed to get MQ Broker logs"
    log_error "==========================================="
    
    return 1
}

# Show current service status
show_status() {
    log_info "Current service status:"
    check_all_services
}

# Main function
main() {
    case "${1:-wait}" in
        "wait")
            wait_for_services
            ;;
        "check")
            show_status
            ;;
        "status")
            show_status
            ;;
        *)
            echo "Usage: $0 [wait|check|status]"
            echo ""
            echo "Commands:"
            echo "  wait   - Wait for all services to be ready (default)"
            echo "  check  - Check current service status"
            echo "  status - Same as check"
            echo ""
            echo "Environment variables:"
            echo "  TIMEOUT - Maximum time to wait in seconds (default: 300)"
            echo "  CHECK_INTERVAL - Check interval in seconds (default: 5)"
            echo "  SEAWEEDFS_MASTER_URL - Master URL (default: http://localhost:9333)"
            echo "  KAFKA_GATEWAY_URL - Gateway URL (default: localhost:9093)"
            echo "  SCHEMA_REGISTRY_URL - Schema Registry URL (default: http://localhost:8081)"
            echo "  SEAWEEDFS_FILER_URL - Filer URL (default: http://localhost:8888)"
            exit 1
            ;;
    esac
}

main "$@"
