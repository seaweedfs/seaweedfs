#!/bin/bash

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
MOUNT_POINT=${MOUNT_POINT:-"/mnt/seaweedfs"}
FILER_ADDR=${FILER_ADDR:-"seaweedfs-filer:8888"}
RDMA_SIDECAR_ADDR=${RDMA_SIDECAR_ADDR:-"rdma-sidecar:8081"}
TEST_RESULTS_DIR=${TEST_RESULTS_DIR:-"/test-results"}

# Test counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Create results directory
mkdir -p "$TEST_RESULTS_DIR"

# Log file
LOG_FILE="$TEST_RESULTS_DIR/integration-test.log"
exec > >(tee -a "$LOG_FILE")
exec 2>&1

echo -e "${BLUE}🧪 SEAWEEDFS RDMA MOUNT INTEGRATION TESTS${NC}"
echo "=========================================="
echo "Mount Point: $MOUNT_POINT"
echo "Filer Address: $FILER_ADDR"
echo "RDMA Sidecar: $RDMA_SIDECAR_ADDR"
echo "Results Directory: $TEST_RESULTS_DIR"
echo "Log File: $LOG_FILE"
echo ""

# Function to run a test
run_test() {
    local test_name=$1
    local test_command=$2
    
    echo -e "${BLUE}🔬 Running test: $test_name${NC}"
    ((TOTAL_TESTS++))
    
    if eval "$test_command"; then
        echo -e "${GREEN}✅ PASSED: $test_name${NC}"
        ((PASSED_TESTS++))
        echo "PASS" > "$TEST_RESULTS_DIR/${test_name}.result"
    else
        echo -e "${RED}❌ FAILED: $test_name${NC}"
        ((FAILED_TESTS++))
        echo "FAIL" > "$TEST_RESULTS_DIR/${test_name}.result"
    fi
    echo ""
}

# Function to wait for mount to be ready
wait_for_mount() {
    local max_attempts=30
    local attempt=1
    
    echo -e "${BLUE}⏳ Waiting for mount to be ready...${NC}"
    
    while [[ $attempt -le $max_attempts ]]; do
        if mountpoint -q "$MOUNT_POINT" 2>/dev/null && ls "$MOUNT_POINT" >/dev/null 2>&1; then
            echo -e "${GREEN}✅ Mount is ready${NC}"
            return 0
        fi
        echo "   Attempt $attempt/$max_attempts..."
        sleep 2
        ((attempt++))
    done
    
    echo -e "${RED}❌ Mount failed to be ready${NC}"
    return 1
}

# Function to check RDMA sidecar
check_rdma_sidecar() {
    echo -e "${BLUE}🔍 Checking RDMA sidecar status...${NC}"
    
    local response
    if response=$(curl -s "http://$RDMA_SIDECAR_ADDR/health" 2>/dev/null); then
        echo "RDMA Sidecar Health: $response"
        return 0
    else
        echo -e "${RED}❌ RDMA sidecar is not responding${NC}"
        return 1
    fi
}

# Test 1: Mount Point Accessibility
test_mount_accessibility() {
    mountpoint -q "$MOUNT_POINT" && ls "$MOUNT_POINT" >/dev/null
}

# Test 2: Basic File Operations
test_basic_file_operations() {
    local test_file="$MOUNT_POINT/test_basic_ops.txt"
    local test_content="Hello, RDMA World! $(date)"
    
    # Write test
    echo "$test_content" > "$test_file" || return 1
    
    # Read test
    local read_content
    read_content=$(cat "$test_file") || return 1
    
    # Verify content
    [[ "$read_content" == "$test_content" ]] || return 1
    
    # Cleanup
    rm -f "$test_file"
    
    return 0
}

# Test 3: Large File Operations
test_large_file_operations() {
    local test_file="$MOUNT_POINT/test_large_file.dat"
    local size_mb=10
    
    # Create large file
    dd if=/dev/zero of="$test_file" bs=1M count=$size_mb 2>/dev/null || return 1
    
    # Verify size
    local actual_size
    actual_size=$(stat -c%s "$test_file" 2>/dev/null) || return 1
    local expected_size=$((size_mb * 1024 * 1024))
    
    [[ "$actual_size" -eq "$expected_size" ]] || return 1
    
    # Read test
    dd if="$test_file" of=/dev/null bs=1M 2>/dev/null || return 1
    
    # Cleanup
    rm -f "$test_file"
    
    return 0
}

# Test 4: Directory Operations
test_directory_operations() {
    local test_dir="$MOUNT_POINT/test_directory"
    local test_file="$test_dir/test_file.txt"
    
    # Create directory
    mkdir -p "$test_dir" || return 1
    
    # Create file in directory
    echo "Directory test" > "$test_file" || return 1
    
    # List directory
    ls "$test_dir" | grep -q "test_file.txt" || return 1
    
    # Read file
    grep -q "Directory test" "$test_file" || return 1
    
    # Cleanup
    rm -rf "$test_dir"
    
    return 0
}

# Test 5: Multiple File Operations
test_multiple_files() {
    local test_dir="$MOUNT_POINT/test_multiple"
    local num_files=20
    
    mkdir -p "$test_dir" || return 1
    
    # Create multiple files
    for i in $(seq 1 $num_files); do
        echo "File $i content" > "$test_dir/file_$i.txt" || return 1
    done
    
    # Verify all files exist and have correct content
    for i in $(seq 1 $num_files); do
        [[ -f "$test_dir/file_$i.txt" ]] || return 1
        grep -q "File $i content" "$test_dir/file_$i.txt" || return 1
    done
    
    # List files
    local file_count
    file_count=$(ls "$test_dir" | wc -l) || return 1
    [[ "$file_count" -eq "$num_files" ]] || return 1
    
    # Cleanup
    rm -rf "$test_dir"
    
    return 0
}

# Test 6: RDMA Statistics
test_rdma_statistics() {
    local stats_response
    stats_response=$(curl -s "http://$RDMA_SIDECAR_ADDR/stats" 2>/dev/null) || return 1
    
    # Check if response contains expected fields
    echo "$stats_response" | jq -e '.rdma_enabled' >/dev/null || return 1
    echo "$stats_response" | jq -e '.total_reads' >/dev/null || return 1
    
    return 0
}

# Test 7: Performance Baseline
test_performance_baseline() {
    local test_file="$MOUNT_POINT/performance_test.dat"
    local size_mb=50
    
    # Write performance test
    local write_start write_end write_time
    write_start=$(date +%s%N)
    dd if=/dev/zero of="$test_file" bs=1M count=$size_mb 2>/dev/null || return 1
    write_end=$(date +%s%N)
    write_time=$(((write_end - write_start) / 1000000)) # Convert to milliseconds
    
    # Read performance test
    local read_start read_end read_time
    read_start=$(date +%s%N)
    dd if="$test_file" of=/dev/null bs=1M 2>/dev/null || return 1
    read_end=$(date +%s%N)
    read_time=$(((read_end - read_start) / 1000000)) # Convert to milliseconds
    
    # Log performance metrics
    echo "Performance Metrics:" > "$TEST_RESULTS_DIR/performance.txt"
    echo "Write Time: ${write_time}ms for ${size_mb}MB" >> "$TEST_RESULTS_DIR/performance.txt"
    echo "Read Time: ${read_time}ms for ${size_mb}MB" >> "$TEST_RESULTS_DIR/performance.txt"
    echo "Write Throughput: $(bc <<< "scale=2; $size_mb * 1000 / $write_time") MB/s" >> "$TEST_RESULTS_DIR/performance.txt"
    echo "Read Throughput: $(bc <<< "scale=2; $size_mb * 1000 / $read_time") MB/s" >> "$TEST_RESULTS_DIR/performance.txt"
    
    # Cleanup
    rm -f "$test_file"
    
    # Performance test always passes (it's just for metrics)
    return 0
}

# Main test execution
main() {
    echo -e "${BLUE}🚀 Starting integration tests...${NC}"
    echo ""
    
    # Wait for mount to be ready
    if ! wait_for_mount; then
        echo -e "${RED}❌ Mount is not ready, aborting tests${NC}"
        exit 1
    fi
    
    # Check RDMA sidecar
    check_rdma_sidecar || echo -e "${YELLOW}⚠️  RDMA sidecar check failed, continuing with tests${NC}"
    
    echo ""
    echo -e "${BLUE}📋 Running test suite...${NC}"
    echo ""
    
    # Run all tests
    run_test "mount_accessibility" "test_mount_accessibility"
    run_test "basic_file_operations" "test_basic_file_operations"
    run_test "large_file_operations" "test_large_file_operations"
    run_test "directory_operations" "test_directory_operations"
    run_test "multiple_files" "test_multiple_files"
    run_test "rdma_statistics" "test_rdma_statistics"
    run_test "performance_baseline" "test_performance_baseline"
    
    # Generate test summary
    echo -e "${BLUE}📊 TEST SUMMARY${NC}"
    echo "==============="
    echo "Total Tests: $TOTAL_TESTS"
    echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
    echo -e "Failed: ${RED}$FAILED_TESTS${NC}"
    
    if [[ $FAILED_TESTS -eq 0 ]]; then
        echo -e "${GREEN}🎉 ALL TESTS PASSED!${NC}"
        echo "SUCCESS" > "$TEST_RESULTS_DIR/overall.result"
        exit 0
    else
        echo -e "${RED}💥 SOME TESTS FAILED!${NC}"
        echo "FAILURE" > "$TEST_RESULTS_DIR/overall.result"
        exit 1
    fi
}

# Run main function
main "$@"
