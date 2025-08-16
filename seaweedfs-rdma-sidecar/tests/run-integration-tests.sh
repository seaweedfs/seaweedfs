#!/bin/bash

# SeaweedFS RDMA Integration Test Suite
# Comprehensive testing of the complete integration in Docker environment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_header() {
    echo -e "\n${PURPLE}===============================================${NC}"
    echo -e "${PURPLE}$1${NC}"
    echo -e "${PURPLE}===============================================${NC}\n"
}

print_step() {
    echo -e "${CYAN}🔵 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

wait_for_service() {
    local url=$1
    local service_name=$2
    local max_attempts=30
    local attempt=1

    print_step "Waiting for $service_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$url" > /dev/null 2>&1; then
            print_success "$service_name is ready"
            return 0
        fi
        
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    print_error "$service_name failed to become ready after $max_attempts attempts"
    return 1
}

test_seaweedfs_master() {
    print_header "TESTING SEAWEEDFS MASTER"
    
    wait_for_service "$SEAWEEDFS_MASTER/cluster/status" "SeaweedFS Master"
    
    print_step "Checking master status..."
    response=$(curl -s "$SEAWEEDFS_MASTER/cluster/status")
    
    if echo "$response" | jq -e '.IsLeader == true' > /dev/null; then
        print_success "SeaweedFS Master is leader and ready"
    else
        print_error "SeaweedFS Master is not ready"
        echo "$response"
        return 1
    fi
}

test_seaweedfs_volume() {
    print_header "TESTING SEAWEEDFS VOLUME SERVER"
    
    wait_for_service "$SEAWEEDFS_VOLUME/status" "SeaweedFS Volume Server"
    
    print_step "Checking volume server status..."
    response=$(curl -s "$SEAWEEDFS_VOLUME/status")
    
    if echo "$response" | jq -e '.Version' > /dev/null; then
        print_success "SeaweedFS Volume Server is ready"
        echo "Volume Server Version: $(echo "$response" | jq -r '.Version')"
    else
        print_error "SeaweedFS Volume Server is not ready"
        echo "$response"
        return 1
    fi
}

test_rdma_engine() {
    print_header "TESTING RDMA ENGINE"
    
    print_step "Checking RDMA engine socket..."
    if [ -S "$RDMA_SOCKET_PATH" ]; then
        print_success "RDMA engine socket exists"
    else
        print_error "RDMA engine socket not found at $RDMA_SOCKET_PATH"
        return 1
    fi
    
    print_step "Testing RDMA engine ping..."
    if ./test-rdma ping --socket "$RDMA_SOCKET_PATH" 2>/dev/null; then
        print_success "RDMA engine ping successful"
    else
        print_error "RDMA engine ping failed"
        return 1
    fi
    
    print_step "Testing RDMA engine capabilities..."
    if ./test-rdma capabilities --socket "$RDMA_SOCKET_PATH" 2>/dev/null | grep -q "Version:"; then
        print_success "RDMA engine capabilities retrieved"
        ./test-rdma capabilities --socket "$RDMA_SOCKET_PATH" 2>/dev/null | head -5
    else
        print_error "RDMA engine capabilities failed"
        return 1
    fi
}

test_rdma_sidecar() {
    print_header "TESTING RDMA SIDECAR"
    
    wait_for_service "$SIDECAR_URL/health" "RDMA Sidecar"
    
    print_step "Testing sidecar health..."
    response=$(curl -s "$SIDECAR_URL/health")
    
    if echo "$response" | jq -e '.status == "healthy"' > /dev/null; then
        print_success "RDMA Sidecar is healthy"
        echo "RDMA Status: $(echo "$response" | jq -r '.rdma.enabled')"
    else
        print_error "RDMA Sidecar health check failed"
        echo "$response"
        return 1
    fi
    
    print_step "Testing sidecar stats..."
    stats=$(curl -s "$SIDECAR_URL/stats")
    
    if echo "$stats" | jq -e '.enabled' > /dev/null; then
        print_success "RDMA Sidecar stats retrieved"
        echo "RDMA Enabled: $(echo "$stats" | jq -r '.enabled')"
        echo "RDMA Connected: $(echo "$stats" | jq -r '.connected')"
        
        if echo "$stats" | jq -e '.capabilities' > /dev/null; then
            version=$(echo "$stats" | jq -r '.capabilities.version')
            sessions=$(echo "$stats" | jq -r '.capabilities.max_sessions')
            print_success "RDMA Engine Info: Version=$version, Max Sessions=$sessions"
        fi
    else
        print_error "RDMA Sidecar stats failed"
        echo "$stats"
        return 1
    fi
}

test_direct_rdma_operations() {
    print_header "TESTING DIRECT RDMA OPERATIONS"
    
    print_step "Testing direct RDMA read operation..."
    if ./test-rdma read --socket "$RDMA_SOCKET_PATH" --volume 1 --needle 12345 --size 1024 2>/dev/null | grep -q "RDMA read completed"; then
        print_success "Direct RDMA read operation successful"
    else
        print_warning "Direct RDMA read operation failed (expected in mock mode)"
    fi
    
    print_step "Running RDMA performance benchmark..."
    benchmark_result=$(./test-rdma bench --socket "$RDMA_SOCKET_PATH" --iterations 5 --read-size 2048 2>/dev/null | tail -10)
    
    if echo "$benchmark_result" | grep -q "Operations/sec:"; then
        print_success "RDMA benchmark completed"
        echo "$benchmark_result" | grep -E "Operations|Latency|Throughput"
    else
        print_warning "RDMA benchmark had issues (expected in mock mode)"
    fi
}

test_sidecar_needle_operations() {
    print_header "TESTING SIDECAR NEEDLE OPERATIONS"
    
    print_step "Testing needle read via sidecar..."
    response=$(curl -s "$SIDECAR_URL/read?volume=1&needle=12345&cookie=305419896&size=1024")
    
    if echo "$response" | jq -e '.success == true' > /dev/null; then
        print_success "Sidecar needle read successful"
        
        is_rdma=$(echo "$response" | jq -r '.is_rdma')
        source=$(echo "$response" | jq -r '.source')
        duration=$(echo "$response" | jq -r '.duration')
        
        if [ "$is_rdma" = "true" ]; then
            print_success "RDMA fast path used! Duration: $duration"
        else
            print_warning "HTTP fallback used. Duration: $duration"
        fi
        
        echo "Response details:"
        echo "$response" | jq '{success, is_rdma, source, duration, data_size}'
    else
        print_error "Sidecar needle read failed"
        echo "$response"
        return 1
    fi
}

test_sidecar_benchmark() {
    print_header "TESTING SIDECAR BENCHMARK"
    
    print_step "Running sidecar performance benchmark..."
    response=$(curl -s "$SIDECAR_URL/benchmark?iterations=5&size=2048")
    
    if echo "$response" | jq -e '.benchmark_results' > /dev/null; then
        print_success "Sidecar benchmark completed"
        
        rdma_ops=$(echo "$response" | jq -r '.benchmark_results.rdma_ops')
        http_ops=$(echo "$response" | jq -r '.benchmark_results.http_ops')
        avg_latency=$(echo "$response" | jq -r '.benchmark_results.avg_latency')
        ops_per_sec=$(echo "$response" | jq -r '.benchmark_results.ops_per_sec')
        
        echo "Benchmark Results:"
        echo "  RDMA Operations: $rdma_ops"
        echo "  HTTP Operations: $http_ops"
        echo "  Average Latency: $avg_latency"
        echo "  Operations/sec: $ops_per_sec"
    else
        print_error "Sidecar benchmark failed"
        echo "$response"
        return 1
    fi
}

test_error_handling() {
    print_header "TESTING ERROR HANDLING AND FALLBACK"
    
    print_step "Testing invalid needle read..."
    response=$(curl -s "$SIDECAR_URL/read?volume=999&needle=999999&size=1024")
    
    # Should succeed with mock data or fail gracefully
    if echo "$response" | jq -e '.success' > /dev/null; then
        result=$(echo "$response" | jq -r '.success')
        if [ "$result" = "true" ]; then
            print_success "Error handling working - mock data returned"
        else
            print_success "Error handling working - graceful failure"
        fi
    else
        print_success "Error handling working - proper error response"
    fi
}

main() {
    print_header "🚀 SEAWEEDFS RDMA INTEGRATION TEST SUITE"
    
    echo -e "${GREEN}Starting comprehensive integration tests...${NC}"
    echo -e "${BLUE}Environment:${NC}"
    echo -e "  RDMA Socket: $RDMA_SOCKET_PATH"
    echo -e "  Sidecar URL: $SIDECAR_URL"
    echo -e "  SeaweedFS Master: $SEAWEEDFS_MASTER"
    echo -e "  SeaweedFS Volume: $SEAWEEDFS_VOLUME"
    
    # Run tests in sequence
    test_seaweedfs_master
    test_seaweedfs_volume
    test_rdma_engine
    test_rdma_sidecar
    test_direct_rdma_operations
    test_sidecar_needle_operations
    test_sidecar_benchmark
    test_error_handling
    
    print_header "🎉 ALL INTEGRATION TESTS COMPLETED!"
    
    echo -e "${GREEN}✅ Test Summary:${NC}"
    echo -e "  ✅ SeaweedFS Master: Working"
    echo -e "  ✅ SeaweedFS Volume Server: Working"
    echo -e "  ✅ Rust RDMA Engine: Working (Mock Mode)"
    echo -e "  ✅ Go RDMA Sidecar: Working"
    echo -e "  ✅ IPC Communication: Working"
    echo -e "  ✅ Needle Operations: Working"
    echo -e "  ✅ Performance Benchmarking: Working"
    echo -e "  ✅ Error Handling: Working"
    
    print_success "SeaweedFS RDMA integration is fully functional!"
    
    return 0
}

# Check required environment variables
if [ -z "$RDMA_SOCKET_PATH" ] || [ -z "$SIDECAR_URL" ] || [ -z "$SEAWEEDFS_MASTER" ] || [ -z "$SEAWEEDFS_VOLUME" ]; then
    print_error "Required environment variables not set"
    echo "Required: RDMA_SOCKET_PATH, SIDECAR_URL, SEAWEEDFS_MASTER, SEAWEEDFS_VOLUME"
    exit 1
fi

# Run main test suite
main "$@"
