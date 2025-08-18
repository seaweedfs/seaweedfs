#!/bin/bash

# Test RDMA functionality in simulation environment
# This script validates that RDMA devices and libraries are working

set -e

echo "🧪 Testing RDMA simulation environment..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    local status="$1"
    local message="$2"
    
    case "$status" in
        "success")
            echo -e "${GREEN}✅ $message${NC}"
            ;;
        "warning") 
            echo -e "${YELLOW}⚠️  $message${NC}"
            ;;
        "error")
            echo -e "${RED}❌ $message${NC}"
            ;;
        "info")
            echo -e "${BLUE}📋 $message${NC}"
            ;;
    esac
}

# Function to test RDMA devices
test_rdma_devices() {
    print_status "info" "Testing RDMA devices..."
    
    # Check for InfiniBand/RDMA devices
    if [ -d /sys/class/infiniband ]; then
        local device_count=$(ls /sys/class/infiniband/ 2>/dev/null | wc -l)
        if [ "$device_count" -gt 0 ]; then
            print_status "success" "Found $device_count RDMA device(s)"
            
            # List devices
            for device in /sys/class/infiniband/*; do
                if [ -d "$device" ]; then
                    local dev_name=$(basename "$device")
                    print_status "info" "Device: $dev_name"
                fi
            done
            return 0
        else
            print_status "error" "No RDMA devices found"
            return 1
        fi
    else
        print_status "error" "/sys/class/infiniband directory not found"
        return 1
    fi
}

# Function to test libibverbs
test_libibverbs() {
    print_status "info" "Testing libibverbs..."
    
    if command -v ibv_devinfo >/dev/null 2>&1; then
        # Get device info
        local device_info=$(ibv_devinfo 2>/dev/null)
        if [ -n "$device_info" ]; then
            print_status "success" "libibverbs working - devices detected"
            
            # Show basic info
            echo "$device_info" | head -5
            
            # Test device capabilities
            if echo "$device_info" | grep -q "transport.*InfiniBand\|transport.*Ethernet"; then
                print_status "success" "RDMA transport layer detected"
            else
                print_status "warning" "Transport layer information unclear"
            fi
            
            return 0
        else
            print_status "error" "ibv_devinfo found no devices"
            return 1
        fi
    else
        print_status "error" "ibv_devinfo command not found"
        return 1
    fi
}

# Function to test UCX
test_ucx() {
    print_status "info" "Testing UCX..."
    
    if command -v ucx_info >/dev/null 2>&1; then
        # Test UCX device detection
        local ucx_output=$(ucx_info -d 2>/dev/null)
        if [ -n "$ucx_output" ]; then
            print_status "success" "UCX detecting devices"
            
            # Show UCX device info
            echo "$ucx_output" | head -10
            
            # Check for RDMA transports
            if echo "$ucx_output" | grep -q "rc\|ud\|dc"; then
                print_status "success" "UCX RDMA transports available" 
            else
                print_status "warning" "UCX RDMA transports not detected"
            fi
            
            return 0
        else
            print_status "warning" "UCX not detecting devices"
            return 1
        fi
    else
        print_status "warning" "UCX tools not available"
        return 1
    fi
}

# Function to test RDMA CM (Connection Manager)
test_rdma_cm() {
    print_status "info" "Testing RDMA Connection Manager..."
    
    # Check for RDMA CM device
    if [ -e /dev/infiniband/rdma_cm ]; then
        print_status "success" "RDMA CM device found"
        return 0
    else
        print_status "warning" "RDMA CM device not found"
        return 1
    fi
}

# Function to test basic RDMA operations
test_rdma_operations() {
    print_status "info" "Testing basic RDMA operations..."
    
    # Try to run a simple RDMA test if tools are available
    if command -v ibv_rc_pingpong >/dev/null 2>&1; then
        # This would need a client/server setup, so just check if binary exists
        print_status "success" "RDMA test tools available (ibv_rc_pingpong)"
    else
        print_status "warning" "RDMA test tools not available"
    fi
    
    # Check for other useful RDMA utilities
    local tools_found=0
    for tool in ibv_asyncwatch ibv_read_lat ibv_write_lat; do
        if command -v "$tool" >/dev/null 2>&1; then
            tools_found=$((tools_found + 1))
        fi
    done
    
    if [ "$tools_found" -gt 0 ]; then
        print_status "success" "Found $tools_found additional RDMA test tools"
    else
        print_status "warning" "No additional RDMA test tools found"
    fi
}

# Function to generate test summary
generate_summary() {
    echo ""
    print_status "info" "RDMA Simulation Test Summary"
    echo "======================================"
    
    # Re-run key tests for summary
    local devices_ok=0
    local libibverbs_ok=0
    local ucx_ok=0
    
    if [ -d /sys/class/infiniband ] && [ "$(ls /sys/class/infiniband/ 2>/dev/null | wc -l)" -gt 0 ]; then
        devices_ok=1
    fi
    
    if command -v ibv_devinfo >/dev/null 2>&1 && ibv_devinfo >/dev/null 2>&1; then
        libibverbs_ok=1
    fi
    
    if command -v ucx_info >/dev/null 2>&1 && ucx_info -d >/dev/null 2>&1; then
        ucx_ok=1
    fi
    
    echo "📊 Test Results:"
    [ "$devices_ok" -eq 1 ] && print_status "success" "RDMA Devices: PASS" || print_status "error" "RDMA Devices: FAIL"
    [ "$libibverbs_ok" -eq 1 ] && print_status "success" "libibverbs: PASS" || print_status "error" "libibverbs: FAIL"
    [ "$ucx_ok" -eq 1 ] && print_status "success" "UCX: PASS" || print_status "warning" "UCX: FAIL/WARNING"
    
    echo ""
    if [ "$devices_ok" -eq 1 ] && [ "$libibverbs_ok" -eq 1 ]; then
        print_status "success" "RDMA simulation environment is ready! 🎉"
        echo ""
        print_status "info" "You can now:"
        echo "  - Run RDMA applications"
        echo "  - Test SeaweedFS RDMA engine with real RDMA"
        echo "  - Use UCX for high-performance transfers"
        return 0
    else
        print_status "error" "RDMA simulation setup needs attention"
        echo ""
        print_status "info" "Troubleshooting:"
        echo "  - Run setup script: sudo /opt/rdma-sim/setup-soft-roce.sh"
        echo "  - Check container privileges (--privileged flag)"
        echo "  - Verify kernel RDMA support"
        return 1
    fi
}

# Main test execution
main() {
    echo "🚀 RDMA Simulation Test Suite"
    echo "======================================"
    
    # Run tests
    test_rdma_devices || true
    echo ""
    
    test_libibverbs || true
    echo ""
    
    test_ucx || true
    echo ""
    
    test_rdma_cm || true
    echo ""
    
    test_rdma_operations || true
    echo ""
    
    # Generate summary
    generate_summary
}

# Health check mode (for Docker healthcheck)
if [ "$1" = "healthcheck" ]; then
    # Quick health check - just verify devices exist
    if [ -d /sys/class/infiniband ] && [ "$(ls /sys/class/infiniband/ 2>/dev/null | wc -l)" -gt 0 ]; then
        exit 0
    else
        exit 1
    fi
fi

# Execute main function
main "$@"
