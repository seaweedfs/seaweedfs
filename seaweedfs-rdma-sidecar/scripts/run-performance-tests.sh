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
RDMA_SIDECAR_ADDR=${RDMA_SIDECAR_ADDR:-"rdma-sidecar:8081"}
PERFORMANCE_RESULTS_DIR=${PERFORMANCE_RESULTS_DIR:-"/performance-results"}

# Create results directory
mkdir -p "$PERFORMANCE_RESULTS_DIR"

# Log file
LOG_FILE="$PERFORMANCE_RESULTS_DIR/performance-test.log"
exec > >(tee -a "$LOG_FILE")
exec 2>&1

echo -e "${BLUE}🏁 SEAWEEDFS RDMA MOUNT PERFORMANCE TESTS${NC}"
echo "==========================================="
echo "Mount Point: $MOUNT_POINT"
echo "RDMA Sidecar: $RDMA_SIDECAR_ADDR"
echo "Results Directory: $PERFORMANCE_RESULTS_DIR"
echo "Log File: $LOG_FILE"
echo ""

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

# Function to get RDMA statistics
get_rdma_stats() {
    curl -s "http://$RDMA_SIDECAR_ADDR/stats" 2>/dev/null || echo "{}"
}

# Function to run dd performance test
run_dd_test() {
    local test_name=$1
    local file_size_mb=$2
    local block_size=$3
    local operation=$4  # "write" or "read"
    
    local test_file="$MOUNT_POINT/perf_test_${test_name}.dat"
    local result_file="$PERFORMANCE_RESULTS_DIR/dd_${test_name}.json"
    
    echo -e "${BLUE}🔬 Running DD test: $test_name${NC}"
    echo "   Size: ${file_size_mb}MB, Block Size: $block_size, Operation: $operation"
    
    local start_time end_time duration_ms throughput_mbps
    
    if [[ "$operation" == "write" ]]; then
        start_time=$(date +%s%N)
        dd if=/dev/zero of="$test_file" bs="$block_size" count=$((file_size_mb * 1024 * 1024 / $(numfmt --from=iec "$block_size"))) 2>/dev/null
        end_time=$(date +%s%N)
    else
        # Create file first if it doesn't exist
        if [[ ! -f "$test_file" ]]; then
            dd if=/dev/zero of="$test_file" bs=1M count="$file_size_mb" 2>/dev/null
        fi
        start_time=$(date +%s%N)
        dd if="$test_file" of=/dev/null bs="$block_size" 2>/dev/null
        end_time=$(date +%s%N)
    fi
    
    duration_ms=$(((end_time - start_time) / 1000000))
    throughput_mbps=$(bc <<< "scale=2; $file_size_mb * 1000 / $duration_ms")
    
    # Save results
    cat > "$result_file" << EOF
{
  "test_name": "$test_name",
  "operation": "$operation",
  "file_size_mb": $file_size_mb,
  "block_size": "$block_size",
  "duration_ms": $duration_ms,
  "throughput_mbps": $throughput_mbps,
  "timestamp": "$(date -Iseconds)"
}
EOF
    
    echo "   Duration: ${duration_ms}ms"
    echo "   Throughput: ${throughput_mbps} MB/s"
    echo ""
    
    # Cleanup write test files
    if [[ "$operation" == "write" ]]; then
        rm -f "$test_file"
    fi
}

# Function to run FIO performance test
run_fio_test() {
    local test_name=$1
    local rw_type=$2      # "read", "write", "randread", "randwrite"
    local block_size=$3
    local file_size=$4
    local iodepth=$5
    
    local test_file="$MOUNT_POINT/fio_test_${test_name}.dat"
    local result_file="$PERFORMANCE_RESULTS_DIR/fio_${test_name}.json"
    
    echo -e "${BLUE}🔬 Running FIO test: $test_name${NC}"
    echo "   Type: $rw_type, Block Size: $block_size, File Size: $file_size, IO Depth: $iodepth"
    
    # Run FIO test
    fio --name="$test_name" \
        --filename="$test_file" \
        --rw="$rw_type" \
        --bs="$block_size" \
        --size="$file_size" \
        --iodepth="$iodepth" \
        --direct=1 \
        --runtime=30 \
        --time_based \
        --group_reporting \
        --output-format=json \
        --output="$result_file" \
        2>/dev/null
    
    # Extract key metrics
    if [[ -f "$result_file" ]]; then
        local iops throughput_kbps latency_us
        iops=$(jq -r '.jobs[0].'"$rw_type"'.iops // 0' "$result_file" 2>/dev/null || echo "0")
        throughput_kbps=$(jq -r '.jobs[0].'"$rw_type"'.bw // 0' "$result_file" 2>/dev/null || echo "0")
        latency_us=$(jq -r '.jobs[0].'"$rw_type"'.lat_ns.mean // 0' "$result_file" 2>/dev/null || echo "0")
        latency_us=$(bc <<< "scale=2; $latency_us / 1000" 2>/dev/null || echo "0")
        
        echo "   IOPS: $iops"
        echo "   Throughput: $(bc <<< "scale=2; $throughput_kbps / 1024") MB/s"
        echo "   Average Latency: ${latency_us} μs"
    else
        echo "   FIO test failed or no results"
    fi
    echo ""
    
    # Cleanup
    rm -f "$test_file"
}

# Function to run concurrent access test
run_concurrent_test() {
    local num_processes=$1
    local file_size_mb=$2
    
    echo -e "${BLUE}🔬 Running concurrent access test${NC}"
    echo "   Processes: $num_processes, File Size per Process: ${file_size_mb}MB"
    
    local start_time end_time duration_ms total_throughput
    local pids=()
    
    start_time=$(date +%s%N)
    
    # Start concurrent processes
    for i in $(seq 1 "$num_processes"); do
        (
            local test_file="$MOUNT_POINT/concurrent_test_$i.dat"
            dd if=/dev/zero of="$test_file" bs=1M count="$file_size_mb" 2>/dev/null
            dd if="$test_file" of=/dev/null bs=1M 2>/dev/null
            rm -f "$test_file"
        ) &
        pids+=($!)
    done
    
    # Wait for all processes to complete
    for pid in "${pids[@]}"; do
        wait "$pid"
    done
    
    end_time=$(date +%s%N)
    duration_ms=$(((end_time - start_time) / 1000000))
    total_throughput=$(bc <<< "scale=2; $num_processes * $file_size_mb * 2 * 1000 / $duration_ms")
    
    # Save results
    cat > "$PERFORMANCE_RESULTS_DIR/concurrent_test.json" << EOF
{
  "test_name": "concurrent_access",
  "num_processes": $num_processes,
  "file_size_mb_per_process": $file_size_mb,
  "total_data_mb": $((num_processes * file_size_mb * 2)),
  "duration_ms": $duration_ms,
  "total_throughput_mbps": $total_throughput,
  "timestamp": "$(date -Iseconds)"
}
EOF
    
    echo "   Duration: ${duration_ms}ms"
    echo "   Total Throughput: ${total_throughput} MB/s"
    echo ""
}

# Function to generate performance report
generate_report() {
    local report_file="$PERFORMANCE_RESULTS_DIR/performance_report.html"
    
    echo -e "${BLUE}📊 Generating performance report...${NC}"
    
    cat > "$report_file" << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>SeaweedFS RDMA Mount Performance Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: #f0f0f0; padding: 20px; border-radius: 5px; }
        .test-section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
        .metric { margin: 5px 0; }
        .good { color: green; font-weight: bold; }
        .warning { color: orange; font-weight: bold; }
        .error { color: red; font-weight: bold; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏁 SeaweedFS RDMA Mount Performance Report</h1>
        <p>Generated: $(date)</p>
        <p>Mount Point: $MOUNT_POINT</p>
        <p>RDMA Sidecar: $RDMA_SIDECAR_ADDR</p>
    </div>
EOF
    
    # Add DD test results
    echo '<div class="test-section"><h2>DD Performance Tests</h2><table><tr><th>Test</th><th>Operation</th><th>Size</th><th>Block Size</th><th>Throughput (MB/s)</th><th>Duration (ms)</th></tr>' >> "$report_file"
    
    for result_file in "$PERFORMANCE_RESULTS_DIR"/dd_*.json; do
        if [[ -f "$result_file" ]]; then
            local test_name operation file_size_mb block_size throughput_mbps duration_ms
            test_name=$(jq -r '.test_name' "$result_file" 2>/dev/null || echo "unknown")
            operation=$(jq -r '.operation' "$result_file" 2>/dev/null || echo "unknown")
            file_size_mb=$(jq -r '.file_size_mb' "$result_file" 2>/dev/null || echo "0")
            block_size=$(jq -r '.block_size' "$result_file" 2>/dev/null || echo "unknown")
            throughput_mbps=$(jq -r '.throughput_mbps' "$result_file" 2>/dev/null || echo "0")
            duration_ms=$(jq -r '.duration_ms' "$result_file" 2>/dev/null || echo "0")
            
            echo "<tr><td>$test_name</td><td>$operation</td><td>${file_size_mb}MB</td><td>$block_size</td><td>$throughput_mbps</td><td>$duration_ms</td></tr>" >> "$report_file"
        fi
    done
    
    echo '</table></div>' >> "$report_file"
    
    # Add FIO test results
    echo '<div class="test-section"><h2>FIO Performance Tests</h2>' >> "$report_file"
    echo '<p>Detailed FIO results are available in individual JSON files.</p></div>' >> "$report_file"
    
    # Add concurrent test results
    if [[ -f "$PERFORMANCE_RESULTS_DIR/concurrent_test.json" ]]; then
        echo '<div class="test-section"><h2>Concurrent Access Test</h2>' >> "$report_file"
        local num_processes total_throughput duration_ms
        num_processes=$(jq -r '.num_processes' "$PERFORMANCE_RESULTS_DIR/concurrent_test.json" 2>/dev/null || echo "0")
        total_throughput=$(jq -r '.total_throughput_mbps' "$PERFORMANCE_RESULTS_DIR/concurrent_test.json" 2>/dev/null || echo "0")
        duration_ms=$(jq -r '.duration_ms' "$PERFORMANCE_RESULTS_DIR/concurrent_test.json" 2>/dev/null || echo "0")
        
        echo "<p>Processes: $num_processes</p>" >> "$report_file"
        echo "<p>Total Throughput: $total_throughput MB/s</p>" >> "$report_file"
        echo "<p>Duration: $duration_ms ms</p>" >> "$report_file"
        echo '</div>' >> "$report_file"
    fi
    
    echo '</body></html>' >> "$report_file"
    
    echo "   Report saved to: $report_file"
}

# Main test execution
main() {
    echo -e "${BLUE}🚀 Starting performance tests...${NC}"
    echo ""
    
    # Wait for mount to be ready
    if ! wait_for_mount; then
        echo -e "${RED}❌ Mount is not ready, aborting tests${NC}"
        exit 1
    fi
    
    # Get initial RDMA stats
    echo -e "${BLUE}📊 Initial RDMA Statistics:${NC}"
    get_rdma_stats | jq . 2>/dev/null || get_rdma_stats
    echo ""
    
    # Run DD performance tests
    echo -e "${BLUE}🏃 Running DD Performance Tests...${NC}"
    run_dd_test "small_write" 10 "4k" "write"
    run_dd_test "small_read" 10 "4k" "read"
    run_dd_test "medium_write" 100 "64k" "write"
    run_dd_test "medium_read" 100 "64k" "read"
    run_dd_test "large_write" 500 "1M" "write"
    run_dd_test "large_read" 500 "1M" "read"
    
    # Run FIO performance tests
    echo -e "${BLUE}🏃 Running FIO Performance Tests...${NC}"
    run_fio_test "seq_read" "read" "64k" "100M" 1
    run_fio_test "seq_write" "write" "64k" "100M" 1
    run_fio_test "rand_read" "randread" "4k" "100M" 16
    run_fio_test "rand_write" "randwrite" "4k" "100M" 16
    
    # Run concurrent access test
    echo -e "${BLUE}🏃 Running Concurrent Access Test...${NC}"
    run_concurrent_test 4 50
    
    # Get final RDMA stats
    echo -e "${BLUE}📊 Final RDMA Statistics:${NC}"
    get_rdma_stats | jq . 2>/dev/null || get_rdma_stats
    echo ""
    
    # Generate performance report
    generate_report
    
    echo -e "${GREEN}🎉 Performance tests completed!${NC}"
    echo "Results saved to: $PERFORMANCE_RESULTS_DIR"
}

# Run main function
main "$@"
