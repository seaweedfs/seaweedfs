#!/bin/bash

# UCX Information and Testing Script
# Provides detailed information about UCX configuration and capabilities

set -e

echo "📋 UCX (Unified Communication X) Information"
echo "============================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_section() {
    echo -e "\n${BLUE}📌 $1${NC}"
    echo "----------------------------------------"
}

print_info() {
    echo -e "${GREEN}ℹ️  $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Function to check UCX installation
check_ucx_installation() {
    print_section "UCX Installation Status"
    
    if command -v ucx_info >/dev/null 2>&1; then
        print_info "UCX tools are installed"
        
        # Get UCX version
        if ucx_info -v >/dev/null 2>&1; then
            local version=$(ucx_info -v 2>/dev/null | head -1)
            print_info "Version: $version"
        fi
    else
        print_warning "UCX tools not found"
        echo "Install with: apt-get install ucx-tools libucx-dev"
        return 1
    fi
    
    # Check UCX libraries
    local libs_found=0
    for lib in libucp.so libucs.so libuct.so; do
        if ldconfig -p | grep -q "$lib"; then
            libs_found=$((libs_found + 1))
        fi
    done
    
    if [ "$libs_found" -eq 3 ]; then
        print_info "All UCX libraries found (ucp, ucs, uct)"
    else
        print_warning "Some UCX libraries may be missing ($libs_found/3 found)"
    fi
}

# Function to show UCX device information
show_ucx_devices() {
    print_section "UCX Transport Devices"
    
    if command -v ucx_info >/dev/null 2>&1; then
        echo "Available UCX transports and devices:"
        ucx_info -d 2>/dev/null || {
            print_warning "Failed to get UCX device information"
            return 1
        }
    else
        print_warning "ucx_info command not available"
        return 1
    fi
}

# Function to show UCX configuration
show_ucx_config() {
    print_section "UCX Configuration"
    
    if command -v ucx_info >/dev/null 2>&1; then
        echo "UCX configuration parameters:"
        ucx_info -c 2>/dev/null | head -20 || {
            print_warning "Failed to get UCX configuration"
            return 1
        }
        
        echo ""
        print_info "Key UCX environment variables:"
        echo "  UCX_TLS - Transport layers to use"
        echo "  UCX_NET_DEVICES - Network devices to use"
        echo "  UCX_LOG_LEVEL - Logging level (error, warn, info, debug, trace)"
        echo "  UCX_MEMTYPE_CACHE - Memory type caching (y/n)"
    else
        print_warning "ucx_info command not available"
        return 1
    fi
}

# Function to test UCX capabilities
test_ucx_capabilities() {
    print_section "UCX Capability Testing"
    
    if command -v ucx_info >/dev/null 2>&1; then
        print_info "Testing UCX transport capabilities..."
        
        # Check for RDMA transports
        local ucx_transports=$(ucx_info -d 2>/dev/null | grep -i "transport\|tl:" || true)
        
        if echo "$ucx_transports" | grep -q "rc\|dc\|ud"; then
            print_info "✅ RDMA transports detected (RC/DC/UD)"
        else
            print_warning "No RDMA transports detected"
        fi
        
        if echo "$ucx_transports" | grep -q "tcp"; then
            print_info "✅ TCP transport available"
        else
            print_warning "TCP transport not detected"
        fi
        
        if echo "$ucx_transports" | grep -q "shm\|posix"; then
            print_info "✅ Shared memory transport available"
        else
            print_warning "Shared memory transport not detected"
        fi
        
        # Memory types
        print_info "Testing memory type support..."
        local memory_info=$(ucx_info -d 2>/dev/null | grep -i "memory\|md:" || true)
        if [ -n "$memory_info" ]; then
            echo "$memory_info" | head -5
        fi
        
    else
        print_warning "Cannot test UCX capabilities - ucx_info not available"
        return 1
    fi
}

# Function to show recommended UCX settings for RDMA
show_rdma_settings() {
    print_section "Recommended UCX Settings for RDMA"
    
    print_info "For optimal RDMA performance with SeaweedFS:"
    echo ""
    echo "Environment Variables:"
    echo "  export UCX_TLS=rc_verbs,ud_verbs,rc_mlx5_dv,dc_mlx5_dv"
    echo "  export UCX_NET_DEVICES=all"
    echo "  export UCX_LOG_LEVEL=info"
    echo "  export UCX_RNDV_SCHEME=put_zcopy"
    echo "  export UCX_RNDV_THRESH=8192"
    echo ""
    
    print_info "For development/debugging:"
    echo "  export UCX_LOG_LEVEL=debug"
    echo "  export UCX_LOG_FILE=/tmp/ucx.log"
    echo ""
    
    print_info "For Soft-RoCE (RXE) specifically:"
    echo "  export UCX_TLS=rc_verbs,ud_verbs"
    echo "  export UCX_IB_DEVICE_SPECS=rxe0:1"  
    echo ""
}

# Function to test basic UCX functionality
test_ucx_basic() {
    print_section "Basic UCX Functionality Test"
    
    if command -v ucx_hello_world >/dev/null 2>&1; then
        print_info "UCX hello_world test available"
        echo "You can test UCX with:"
        echo "  Server: UCX_TLS=tcp ucx_hello_world -l"
        echo "  Client: UCX_TLS=tcp ucx_hello_world <server_ip>"
    else
        print_warning "UCX hello_world test not available"
    fi
    
    # Check for other UCX test utilities
    local test_tools=0
    for tool in ucx_perftest ucp_hello_world; do
        if command -v "$tool" >/dev/null 2>&1; then
            test_tools=$((test_tools + 1))
            print_info "UCX test tool available: $tool"
        fi
    done
    
    if [ "$test_tools" -eq 0 ]; then
        print_warning "No UCX test tools found"
        echo "Consider installing: ucx-tools package"
    fi
}

# Function to generate UCX summary
generate_summary() {
    print_section "UCX Status Summary"
    
    local ucx_ok=0
    local devices_ok=0
    local rdma_ok=0
    
    # Check UCX availability
    if command -v ucx_info >/dev/null 2>&1; then
        ucx_ok=1
    fi
    
    # Check devices
    if command -v ucx_info >/dev/null 2>&1 && ucx_info -d >/dev/null 2>&1; then
        devices_ok=1
        
        # Check for RDMA
        if ucx_info -d 2>/dev/null | grep -q "rc\|dc\|ud"; then
            rdma_ok=1
        fi
    fi
    
    echo "📊 UCX Status:"
    [ "$ucx_ok" -eq 1 ] && print_info "✅ UCX Installation: OK" || print_warning "❌ UCX Installation: Missing"
    [ "$devices_ok" -eq 1 ] && print_info "✅ UCX Devices: Detected" || print_warning "❌ UCX Devices: Not detected"
    [ "$rdma_ok" -eq 1 ] && print_info "✅ RDMA Support: Available" || print_warning "⚠️  RDMA Support: Limited/Missing"
    
    echo ""
    if [ "$ucx_ok" -eq 1 ] && [ "$devices_ok" -eq 1 ]; then
        print_info "🎉 UCX is ready for SeaweedFS RDMA integration!"
        
        if [ "$rdma_ok" -eq 1 ]; then
            print_info "🚀 Real RDMA acceleration is available"
        else
            print_warning "💡 Only TCP/shared memory transports available"
        fi
    else
        print_warning "🔧 UCX setup needs attention for optimal performance"
    fi
}

# Main execution
main() {
    check_ucx_installation
    echo ""
    
    show_ucx_devices
    echo ""
    
    show_ucx_config
    echo ""
    
    test_ucx_capabilities  
    echo ""
    
    show_rdma_settings
    echo ""
    
    test_ucx_basic
    echo ""
    
    generate_summary
    
    echo ""
    print_info "For SeaweedFS RDMA engine integration:"
    echo "  1. Use UCX with your Rust engine"
    echo "  2. Configure appropriate transport layers"
    echo "  3. Test with SeaweedFS RDMA sidecar"
    echo "  4. Monitor performance and adjust settings"
}

# Execute main function
main "$@"
