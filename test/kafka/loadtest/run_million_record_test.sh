#!/bin/bash

# Script to run the Kafka Gateway Million Record Integration Test
# This test requires a running SeaweedFS infrastructure (Master, Filer, MQ Broker)

set -e

echo "=== SeaweedFS Kafka Gateway Million Record Integration Test ==="
echo "Test Date: $(date)"
echo "Hostname: $(hostname)"
echo ""

# Configuration
MASTERS=${SEAWEED_MASTERS:-"localhost:9333"}
FILER_GROUP=${SEAWEED_FILER_GROUP:-"default"}
TEST_DIR="."
TEST_NAME="TestDirectBroker_MillionRecordsIntegration"

echo "Configuration:"
echo "  Masters: $MASTERS"
echo "  Filer Group: $FILER_GROUP"
echo "  Test Directory: $TEST_DIR"
echo ""

# Check if SeaweedFS infrastructure is running
echo "=== Checking Infrastructure ==="

# Function to check if a service is running
check_service() {
    local host_port=$1
    local service_name=$2
    
    if timeout 3 bash -c "</dev/tcp/${host_port//://}" 2>/dev/null; then
        echo "✓ $service_name is running on $host_port"
        return 0
    else
        echo "✗ $service_name is NOT running on $host_port"
        return 1
    fi
}

# Check each master
IFS=',' read -ra MASTER_ARRAY <<< "$MASTERS"
MASTERS_OK=true
for master in "${MASTER_ARRAY[@]}"; do
    if ! check_service "$master" "SeaweedFS Master"; then
        MASTERS_OK=false
    fi
done

if [ "$MASTERS_OK" = false ]; then
    echo ""
    echo "ERROR: One or more SeaweedFS Masters are not running."
    echo "Please start your SeaweedFS infrastructure before running this test."
    echo ""
    echo "Example commands to start SeaweedFS:"
    echo "  # Terminal 1: Start Master"
    echo "  weed master -defaultReplication=001 -mdir=/tmp/seaweedfs/master"
    echo ""
    echo "  # Terminal 2: Start Filer"
    echo "  weed filer -master=localhost:9333 -filer.dir=/tmp/seaweedfs/filer"
    echo ""
    echo "  # Terminal 3: Start MQ Broker"
    echo "  weed mq.broker -filer=localhost:8888 -master=localhost:9333"
    echo ""
    exit 1
fi

echo ""
echo "=== Infrastructure Check Passed ==="
echo ""

# Change to the correct directory
cd "$TEST_DIR"

# Set environment variables for the test
export SEAWEED_MASTERS="$MASTERS"
export SEAWEED_FILER_GROUP="$FILER_GROUP"

# Run the test with verbose output
echo "=== Running Million Record Integration Test ==="
echo "This may take several minutes..."
echo ""

# Run the specific test with timeout and verbose output
timeout 1800 go test -v -run "$TEST_NAME" -timeout=30m 2>&1 | tee /tmp/seaweed_million_record_test.log

TEST_EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "=== Test Completed ==="
echo "Exit Code: $TEST_EXIT_CODE"
echo "Full log available at: /tmp/seaweed_million_record_test.log"
echo ""

# Show summary from the log
echo "=== Performance Summary ==="
if grep -q "PERFORMANCE SUMMARY" /tmp/seaweed_million_record_test.log; then
    grep -A 15 "PERFORMANCE SUMMARY" /tmp/seaweed_million_record_test.log
else
    echo "Performance summary not found in log"
fi

echo ""

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "🎉 TEST PASSED: Million record integration test completed successfully!"
else
    echo "❌ TEST FAILED: Million record integration test failed with exit code $TEST_EXIT_CODE"
    echo "Check the log file for details: /tmp/seaweed_million_record_test.log"
fi

echo ""
echo "=== Test Run Complete ==="
exit $TEST_EXIT_CODE
