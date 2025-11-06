#!/bin/bash

# Complete Test Environment Setup Script
# This script sets up all required services and configurations for S3 IAM integration tests

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BLUE}🚀 Setting up complete test environment for SeaweedFS S3 IAM...${NC}"
echo -e "${BLUE}==========================================================${NC}"

# Check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}🔍 Checking prerequisites...${NC}"
    
    local missing_tools=()
    
    for tool in docker jq curl; do
        if ! command -v "$tool" >/dev/null 2>&1; then
            missing_tools+=("$tool")
        fi
    done
    
    if [ ${#missing_tools[@]} -gt 0 ]; then
        echo -e "${RED}[FAIL] Missing required tools: ${missing_tools[*]}${NC}"
        echo -e "${YELLOW}Please install the missing tools and try again${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}[OK] All prerequisites met${NC}"
}

# Set up Keycloak for OIDC testing
setup_keycloak() {
    echo -e "\n${BLUE}1. Setting up Keycloak for OIDC testing...${NC}"
    
    if ! "${SCRIPT_DIR}/setup_keycloak.sh"; then
        echo -e "${RED}[FAIL] Failed to set up Keycloak${NC}"
        return 1
    fi
    
    echo -e "${GREEN}[OK] Keycloak setup completed${NC}"
}

# Set up SeaweedFS test cluster
setup_seaweedfs_cluster() {
    echo -e "\n${BLUE}2. Setting up SeaweedFS test cluster...${NC}"
    
    # Build SeaweedFS binary if needed
    echo -e "${YELLOW}🔧 Building SeaweedFS binary...${NC}"
    cd "${SCRIPT_DIR}/../../../"  # Go to seaweedfs root
    if ! make > /dev/null 2>&1; then
        echo -e "${RED}[FAIL] Failed to build SeaweedFS binary${NC}"
        return 1
    fi
    
    cd "${SCRIPT_DIR}"  # Return to test directory
    
    # Clean up any existing test data
    echo -e "${YELLOW}🧹 Cleaning up existing test data...${NC}"
    rm -rf test-volume-data/* 2>/dev/null || true
    
    echo -e "${GREEN}[OK] SeaweedFS cluster setup completed${NC}"
}

# Set up test data and configurations
setup_test_configurations() {
    echo -e "\n${BLUE}3. Setting up test configurations...${NC}"
    
    # Ensure IAM configuration is properly set up
    if [ ! -f "${SCRIPT_DIR}/iam_config.json" ]; then
        echo -e "${YELLOW}⚠️  IAM configuration not found, using default config${NC}"
        cp "${SCRIPT_DIR}/iam_config.local.json" "${SCRIPT_DIR}/iam_config.json" 2>/dev/null || {
            echo -e "${RED}[FAIL] No IAM configuration files found${NC}"
            return 1
        }
    fi
    
    # Validate configuration
    if ! jq . "${SCRIPT_DIR}/iam_config.json" >/dev/null; then
        echo -e "${RED}[FAIL] Invalid IAM configuration JSON${NC}"
        return 1
    fi
    
    echo -e "${GREEN}[OK] Test configurations set up${NC}"
}

# Verify services are ready
verify_services() {
    echo -e "\n${BLUE}4. Verifying services are ready...${NC}"
    
    # Check if Keycloak is responding
    echo -e "${YELLOW}🔍 Checking Keycloak availability...${NC}"
    local keycloak_ready=false
    for i in $(seq 1 30); do
        if curl -sf "http://localhost:8080/health/ready" >/dev/null 2>&1; then
            keycloak_ready=true
            break
        fi
        if curl -sf "http://localhost:8080/realms/master" >/dev/null 2>&1; then
            keycloak_ready=true
            break
        fi
        sleep 2
    done
    
    if [ "$keycloak_ready" = true ]; then
        echo -e "${GREEN}[OK] Keycloak is ready${NC}"
    else
        echo -e "${YELLOW}⚠️  Keycloak may not be fully ready yet${NC}"
        echo -e "${YELLOW}This is okay - tests will wait for Keycloak when needed${NC}"
    fi
    
    echo -e "${GREEN}[OK] Service verification completed${NC}"
}

# Set up environment variables
setup_environment() {
    echo -e "\n${BLUE}5. Setting up environment variables...${NC}"
    
    export ENABLE_DISTRIBUTED_TESTS=true
    export ENABLE_PERFORMANCE_TESTS=true
    export ENABLE_STRESS_TESTS=true
    export KEYCLOAK_URL="http://localhost:8080"
    export S3_ENDPOINT="http://localhost:8333"
    export TEST_TIMEOUT=60m
    export CGO_ENABLED=0
    
    # Write environment to a file for other scripts to source
    cat > "${SCRIPT_DIR}/.test_env" << EOF
export ENABLE_DISTRIBUTED_TESTS=true
export ENABLE_PERFORMANCE_TESTS=true
export ENABLE_STRESS_TESTS=true
export KEYCLOAK_URL="http://localhost:8080"
export S3_ENDPOINT="http://localhost:8333"
export TEST_TIMEOUT=60m
export CGO_ENABLED=0
EOF
    
    echo -e "${GREEN}[OK] Environment variables set${NC}"
}

# Display setup summary
display_summary() {
    echo -e "\n${BLUE}📊 Setup Summary${NC}"
    echo -e "${BLUE}=================${NC}"
    echo -e "Keycloak URL: ${KEYCLOAK_URL:-http://localhost:8080}"
    echo -e "S3 Endpoint: ${S3_ENDPOINT:-http://localhost:8333}"
    echo -e "Test Timeout: ${TEST_TIMEOUT:-60m}"
    echo -e "IAM Config: ${SCRIPT_DIR}/iam_config.json"
    echo -e ""
    echo -e "${GREEN}[OK] Complete test environment setup finished!${NC}"
    echo -e "${YELLOW}💡 You can now run tests with: make run-all-tests${NC}"
    echo -e "${YELLOW}💡 Or run specific tests with: go test -v -timeout=60m -run TestName${NC}"
    echo -e "${YELLOW}💡 To stop Keycloak: docker stop keycloak-iam-test${NC}"
}

# Main execution
main() {
    check_prerequisites
    
    # Track what was set up for cleanup on failure
    local setup_steps=()
    
    if setup_keycloak; then
        setup_steps+=("keycloak")
    else
        echo -e "${RED}[FAIL] Failed to set up Keycloak${NC}"
        exit 1
    fi
    
    if setup_seaweedfs_cluster; then
        setup_steps+=("seaweedfs")
    else
        echo -e "${RED}[FAIL] Failed to set up SeaweedFS cluster${NC}"
        exit 1
    fi
    
    if setup_test_configurations; then
        setup_steps+=("config")
    else
        echo -e "${RED}[FAIL] Failed to set up test configurations${NC}"
        exit 1
    fi
    
    setup_environment
    verify_services
    display_summary
    
    echo -e "${GREEN}🎉 All setup completed successfully!${NC}"
}

# Cleanup on script interruption
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up on script interruption...${NC}"
    # Note: We don't automatically stop Keycloak as it might be shared
    echo -e "${YELLOW}💡 If you want to stop Keycloak: docker stop keycloak-iam-test${NC}"
    exit 1
}

trap cleanup INT TERM

# Execute main function
main "$@"
