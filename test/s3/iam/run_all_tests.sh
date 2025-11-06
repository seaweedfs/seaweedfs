#!/bin/bash

# Master Test Runner - Enables and runs all previously skipped tests

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${BLUE}🎯 SeaweedFS S3 IAM Complete Test Suite${NC}"
echo -e "${BLUE}=====================================${NC}"

# Set environment variables to enable all tests
export ENABLE_DISTRIBUTED_TESTS=true
export ENABLE_PERFORMANCE_TESTS=true
export ENABLE_STRESS_TESTS=true
export KEYCLOAK_URL="http://localhost:8080"
export S3_ENDPOINT="http://localhost:8333"
export TEST_TIMEOUT=60m
export CGO_ENABLED=0

# Function to run test category
run_test_category() {
    local category="$1"
    local test_pattern="$2"
    local description="$3"
    
    echo -e "${YELLOW}🧪 Running $description...${NC}"
    
    if go test -v -timeout=$TEST_TIMEOUT -run "$test_pattern" ./...; then
        echo -e "${GREEN}[OK] $description completed successfully${NC}"
        return 0
    else
        echo -e "${RED}[FAIL] $description failed${NC}"
        return 1
    fi
}

# Track results
TOTAL_CATEGORIES=0
PASSED_CATEGORIES=0

# 1. Standard IAM Integration Tests
echo -e "\n${BLUE}1. Standard IAM Integration Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if run_test_category "standard" "TestS3IAM(?!.*Distributed|.*Performance)" "Standard IAM Integration Tests"; then
    PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
fi

# 2. Keycloak Integration Tests (if Keycloak is available)
echo -e "\n${BLUE}2. Keycloak Integration Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if curl -s "http://localhost:8080/health/ready" > /dev/null 2>&1; then
    if run_test_category "keycloak" "TestKeycloak" "Keycloak Integration Tests"; then
        PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
    fi
else
    echo -e "${YELLOW}⚠️ Keycloak not available, skipping Keycloak tests${NC}"
    echo -e "${YELLOW}💡 Run './setup_all_tests.sh' to start Keycloak${NC}"
fi

# 3. Distributed Tests
echo -e "\n${BLUE}3. Distributed IAM Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if run_test_category "distributed" "TestS3IAMDistributedTests" "Distributed IAM Tests"; then
    PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
fi

# 4. Performance Tests
echo -e "\n${BLUE}4. Performance Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if run_test_category "performance" "TestS3IAMPerformanceTests" "Performance Tests"; then
    PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
fi

# 5. Benchmarks
echo -e "\n${BLUE}5. Benchmark Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if go test -bench=. -benchmem -timeout=$TEST_TIMEOUT ./...; then
    echo -e "${GREEN}[OK] Benchmark tests completed successfully${NC}"
    PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
else
    echo -e "${RED}[FAIL] Benchmark tests failed${NC}"
fi

# 6. Versioning Stress Tests
echo -e "\n${BLUE}6. S3 Versioning Stress Tests${NC}"
TOTAL_CATEGORIES=$((TOTAL_CATEGORIES + 1))
if [ -f "../versioning/enable_stress_tests.sh" ]; then
    if (cd ../versioning && ./enable_stress_tests.sh); then
        echo -e "${GREEN}[OK] Versioning stress tests completed successfully${NC}"
        PASSED_CATEGORIES=$((PASSED_CATEGORIES + 1))
    else
        echo -e "${RED}[FAIL] Versioning stress tests failed${NC}"
    fi
else
    echo -e "${YELLOW}⚠️ Versioning stress tests not available${NC}"
fi

# Summary
echo -e "\n${BLUE}📊 Test Summary${NC}"
echo -e "${BLUE}===============${NC}"
echo -e "Total test categories: $TOTAL_CATEGORIES"
echo -e "Passed: ${GREEN}$PASSED_CATEGORIES${NC}"
echo -e "Failed: ${RED}$((TOTAL_CATEGORIES - PASSED_CATEGORIES))${NC}"

if [ $PASSED_CATEGORIES -eq $TOTAL_CATEGORIES ]; then
    echo -e "\n${GREEN}🎉 All test categories passed!${NC}"
    exit 0
else
    echo -e "\n${RED}[FAIL] Some test categories failed${NC}"
    exit 1
fi
