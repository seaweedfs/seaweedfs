#!/bin/bash

# Setup Soft-RoCE (RXE) for RDMA simulation
# This script enables RDMA over Ethernet using the RXE kernel module

set -e

echo "🔧 Setting up Soft-RoCE (RXE) RDMA simulation..."

# Function to check if running with required privileges
check_privileges() {
    if [ "$EUID" -ne 0 ]; then
        echo "❌ This script requires root privileges"
        echo "Run with: sudo $0 or inside a privileged container"
        exit 1
    fi
}

# Function to load RXE kernel module
load_rxe_module() {
    echo "📦 Loading RXE kernel module..."
    
    # Try to load the rdma_rxe module
    if modprobe rdma_rxe 2>/dev/null; then
        echo "✅ rdma_rxe module loaded successfully"
    else
        echo "⚠️  Failed to load rdma_rxe module, trying alternative approach..."
        
        # Alternative: Try loading rxe_net (older kernels)
        if modprobe rxe_net 2>/dev/null; then
            echo "✅ rxe_net module loaded successfully"
        else
            echo "❌ Failed to load RXE modules. Possible causes:"
            echo "  - Kernel doesn't support RXE (needs CONFIG_RDMA_RXE=m)"
            echo "  - Running in unprivileged container"
            echo "  - Missing kernel modules"
            echo ""
            echo "🔧 Workaround: Run container with --privileged flag"
            exit 1
        fi
    fi
    
    # Verify module is loaded
    if lsmod | grep -q "rdma_rxe\|rxe_net"; then
        echo "✅ RXE module verification successful"
    else
        echo "❌ RXE module verification failed"
        exit 1
    fi
}

# Function to setup virtual RDMA device
setup_rxe_device() {
    echo "🌐 Setting up RXE device over Ethernet interface..."
    
    # Find available network interface (prefer eth0, fallback to others)
    local interface=""
    for iface in eth0 enp0s3 enp0s8 lo; do
        if ip link show "$iface" >/dev/null 2>&1; then
            interface="$iface"
            break
        fi
    done
    
    if [ -z "$interface" ]; then
        echo "❌ No suitable network interface found"
        echo "Available interfaces:"
        ip link show | grep "^[0-9]" | cut -d':' -f2 | tr -d ' '
        exit 1
    fi
    
    echo "📡 Using network interface: $interface"
    
    # Create RXE device
    echo "🔨 Creating RXE device on $interface..."
    
    # Try modern rxe_cfg approach first
    if command -v rxe_cfg >/dev/null 2>&1; then
        rxe_cfg add "$interface" || {
            echo "⚠️  rxe_cfg failed, trying manual approach..."
            setup_rxe_manual "$interface"
        }
    else
        echo "⚠️  rxe_cfg not available, using manual setup..."
        setup_rxe_manual "$interface"
    fi
}

# Function to manually setup RXE device
setup_rxe_manual() {
    local interface="$1"
    
    # Use sysfs interface to create RXE device
    if [ -d /sys/module/rdma_rxe ]; then
        echo "$interface" > /sys/module/rdma_rxe/parameters/add 2>/dev/null || {
            echo "❌ Failed to add RXE device via sysfs"
            exit 1
        }
    else
        echo "❌ RXE sysfs interface not found"
        exit 1
    fi
}

# Function to verify RDMA devices
verify_rdma_devices() {
    echo "🔍 Verifying RDMA devices..."
    
    # Check for RDMA devices
    if [ -d /sys/class/infiniband ]; then
        local devices=$(ls /sys/class/infiniband/ 2>/dev/null | wc -l)
        if [ "$devices" -gt 0 ]; then
            echo "✅ Found $devices RDMA device(s):"
            ls /sys/class/infiniband/
            
            # Show device details
            for device in /sys/class/infiniband/*; do
                if [ -d "$device" ]; then
                    local dev_name=$(basename "$device")
                    echo "  📋 Device: $dev_name"
                    
                    # Try to get device info
                    if command -v ibv_devinfo >/dev/null 2>&1; then
                        ibv_devinfo -d "$dev_name" | head -10
                    fi
                fi
            done
        else
            echo "❌ No RDMA devices found in /sys/class/infiniband/"
            exit 1
        fi
    else
        echo "❌ /sys/class/infiniband directory not found"
        exit 1
    fi
}

# Function to test basic RDMA functionality
test_basic_rdma() {
    echo "🧪 Testing basic RDMA functionality..."
    
    # Test libibverbs
    if command -v ibv_devinfo >/dev/null 2>&1; then
        echo "📋 RDMA device information:"
        ibv_devinfo | head -20
    else
        echo "⚠️  ibv_devinfo not available"
    fi
    
    # Test UCX if available
    if command -v ucx_info >/dev/null 2>&1; then
        echo "📋 UCX information:"
        ucx_info -d | head -10
    else
        echo "⚠️  UCX tools not available"
    fi
}

# Main execution
main() {
    echo "🚀 Starting Soft-RoCE RDMA simulation setup..."
    echo "======================================"
    
    check_privileges
    load_rxe_module
    setup_rxe_device  
    verify_rdma_devices
    test_basic_rdma
    
    echo ""
    echo "🎉 Soft-RoCE setup completed successfully!"
    echo "======================================"
    echo "✅ RDMA simulation is ready for testing"
    echo "📡 You can now run RDMA applications"
    echo ""
    echo "Next steps:"
    echo "  - Test with: /opt/rdma-sim/test-rdma.sh"
    echo "  - Check UCX: /opt/rdma-sim/ucx-info.sh"
    echo "  - Run your RDMA applications"
}

# Execute main function
main "$@"
