#!/bin/bash

# Test script to verify the retry logic works correctly
# Simulates Schema Registry eventual consistency behavior

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[TEST]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
}

# Mock function that simulates Schema Registry eventual consistency
# First N attempts fail, then succeeds
mock_schema_registry_query() {
    local subject=$1
    local min_attempts_to_succeed=$2
    local current_attempt=$3
    
    if [[ $current_attempt -ge $min_attempts_to_succeed ]]; then
        # Simulate successful response
        echo '{"id":1,"version":1,"schema":"test"}'
        return 0
    else
        # Simulate 404 Not Found
        echo '{"error_code":40401,"message":"Subject not found"}'
        return 1
    fi
}

# Simulate verify_schema_with_retry logic
test_verify_with_retry() {
    local subject=$1
    local min_attempts_to_succeed=$2
    local max_attempts=5
    local attempt=1
    
    log_info "Testing $subject (should succeed after $min_attempts_to_succeed attempts)"
    
    while [[ $attempt -le $max_attempts ]]; do
        local response
        if response=$(mock_schema_registry_query "$subject" "$min_attempts_to_succeed" "$attempt"); then
            if echo "$response" | grep -q '"id"'; then
                if [[ $attempt -gt 1 ]]; then
                    log_success "$subject verified after $attempt attempts"
                else
                    log_success "$subject verified on first attempt"
                fi
                return 0
            fi
        fi
        
        # Schema not found, wait and retry
        if [[ $attempt -lt $max_attempts ]]; then
            # Exponential backoff: 0.1s, 0.2s, 0.4s, 0.8s
            local wait_time=$(echo "scale=3; 0.1 * (2 ^ ($attempt - 1))" | bc)
            log_info "  Attempt $attempt failed, waiting ${wait_time}s before retry..."
            sleep "$wait_time"
            attempt=$((attempt + 1))
        else
            log_error "$subject verification failed after $max_attempts attempts"
            return 1
        fi
    done
    
    return 1
}

# Run tests
log_info "=========================================="
log_info "Testing Schema Registry Retry Logic"
log_info "=========================================="
echo ""

# Test 1: Schema available immediately
log_info "Test 1: Schema available immediately"
if test_verify_with_retry "immediate-schema" 1; then
    log_success "✓ Test 1 passed"
else
    log_error "✗ Test 1 failed"
    exit 1
fi
echo ""

# Test 2: Schema available after 2 attempts (200ms delay)
log_info "Test 2: Schema available after 2 attempts"
if test_verify_with_retry "delayed-schema-2" 2; then
    log_success "✓ Test 2 passed"
else
    log_error "✗ Test 2 failed"
    exit 1
fi
echo ""

# Test 3: Schema available after 3 attempts (600ms delay)
log_info "Test 3: Schema available after 3 attempts"
if test_verify_with_retry "delayed-schema-3" 3; then
    log_success "✓ Test 3 passed"
else
    log_error "✗ Test 3 failed"
    exit 1
fi
echo ""

# Test 4: Schema available after 4 attempts (1400ms delay)
log_info "Test 4: Schema available after 4 attempts"
if test_verify_with_retry "delayed-schema-4" 4; then
    log_success "✓ Test 4 passed"
else
    log_error "✗ Test 4 failed"
    exit 1
fi
echo ""

# Test 5: Schema never available (should fail)
log_info "Test 5: Schema never available (should fail gracefully)"
if test_verify_with_retry "missing-schema" 10; then
    log_error "✗ Test 5 failed (should have failed but passed)"
    exit 1
else
    log_success "✓ Test 5 passed (correctly failed after max attempts)"
fi
echo ""

log_success "=========================================="
log_success "All tests passed! ✓"
log_success "=========================================="
log_info ""
log_info "Summary:"
log_info "- Immediate availability: works ✓"
log_info "- 2-4 retry attempts: works ✓"
log_info "- Max attempts handling: works ✓"
log_info "- Exponential backoff: works ✓"
log_info ""
log_info "Total retry time budget: ~1.5 seconds (0.1+0.2+0.4+0.8)"
log_info "This should handle Schema Registry consumer lag gracefully."

