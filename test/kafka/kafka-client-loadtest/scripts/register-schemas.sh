#!/bin/bash

# Register schemas with Schema Registry for load testing
# This script registers the necessary schemas before running load tests

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-"http://localhost:8081"}
TIMEOUT=${TIMEOUT:-60}
CHECK_INTERVAL=${CHECK_INTERVAL:-2}

# Wait for Schema Registry to be ready
wait_for_schema_registry() {
    log_info "Waiting for Schema Registry to be ready..."
    
    local elapsed=0
    while [[ $elapsed -lt $TIMEOUT ]]; do
        if curl -sf "$SCHEMA_REGISTRY_URL/subjects" >/dev/null 2>&1; then
            log_success "Schema Registry is ready!"
            return 0
        fi
        
        log_info "Schema Registry not ready yet. Waiting ${CHECK_INTERVAL}s... (${elapsed}/${TIMEOUT}s)"
        sleep $CHECK_INTERVAL
        elapsed=$((elapsed + CHECK_INTERVAL))
    done
    
    log_error "Schema Registry did not become ready within ${TIMEOUT} seconds"
    return 1
}

# Register a schema for a subject
register_schema() {
    local subject=$1
    local schema=$2
    local schema_type=${3:-"AVRO"}
    
    log_info "Registering schema for subject: $subject"
    
    # Create the schema registration payload
    local escaped_schema=$(echo "$schema" | jq -Rs .)
    local payload=$(cat <<EOF
{
    "schema": $escaped_schema,
    "schemaType": "$schema_type"
}
EOF
)
    
    # Register the schema
    local response
    response=$(curl -s -X POST \
        -H "Content-Type: application/vnd.schemaregistry.v1+json" \
        -d "$payload" \
        "$SCHEMA_REGISTRY_URL/subjects/$subject/versions" 2>/dev/null)
    
    if echo "$response" | jq -e '.id' >/dev/null 2>&1; then
        local schema_id
        schema_id=$(echo "$response" | jq -r '.id')
        log_success "✓ Schema registered for $subject with ID: $schema_id"
        return 0
    else
        log_error "✗ Failed to register schema for $subject"
        log_error "Response: $response"
        return 1
    fi
}

# Verify a schema exists
verify_schema() {
    local subject=$1
    
    log_info "Verifying schema for subject: $subject"
    
    local response
    response=$(curl -s "$SCHEMA_REGISTRY_URL/subjects/$subject/versions/latest" 2>/dev/null)
    
    if echo "$response" | jq -e '.id' >/dev/null 2>&1; then
        local schema_id
        local version
        schema_id=$(echo "$response" | jq -r '.id')
        version=$(echo "$response" | jq -r '.version')
        log_success "✓ Schema verified for $subject (ID: $schema_id, Version: $version)"
        return 0
    else
        log_error "✗ Schema not found for $subject"
        return 1
    fi
}

# Register load test schemas
register_loadtest_schemas() {
    log_info "Registering load test schemas..."
    
    # Define the Avro schema for load test messages
    local loadtest_value_schema='{
        "type": "record",
        "name": "LoadTestMessage",
        "namespace": "io.seaweedfs.kafka.loadtest",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "producer_id", "type": "int"},
            {"name": "sequence", "type": "long"},
            {"name": "payload", "type": "string"},
            {"name": "metadata", "type": {"type": "map", "values": "string"}, "default": {}}
        ]
    }'
    
    # Define the key schema (simple string)
    local loadtest_key_schema='{
        "type": "string"
    }'
    
    # Register schemas for all load test topics
    local topics=("loadtest-topic-0" "loadtest-topic-1" "loadtest-topic-2" "loadtest-topic-3" "loadtest-topic-4")
    local success_count=0
    local total_schemas=0
    
    for topic in "${topics[@]}"; do
        # Register value schema
        if register_schema "${topic}-value" "$loadtest_value_schema" "AVRO"; then
            ((success_count++))
        fi
        ((total_schemas++))
        
        # Register key schema
        if register_schema "${topic}-key" "$loadtest_key_schema" "AVRO"; then
            ((success_count++))
        fi
        ((total_schemas++))
    done
    
    log_info "Schema registration summary: $success_count/$total_schemas schemas registered successfully"
    
    if [[ $success_count -eq $total_schemas ]]; then
        log_success "All load test schemas registered successfully!"
        return 0
    else
        log_error "Some schemas failed to register"
        return 1
    fi
}

# Verify all schemas are registered
verify_loadtest_schemas() {
    log_info "Verifying load test schemas..."
    
    local topics=("loadtest-topic-0" "loadtest-topic-1" "loadtest-topic-2" "loadtest-topic-3" "loadtest-topic-4")
    local success_count=0
    local total_schemas=0
    
    for topic in "${topics[@]}"; do
        # Verify value schema
        if verify_schema "${topic}-value"; then
            ((success_count++))
        fi
        ((total_schemas++))
        
        # Verify key schema
        if verify_schema "${topic}-key"; then
            ((success_count++))
        fi
        ((total_schemas++))
    done
    
    log_info "Schema verification summary: $success_count/$total_schemas schemas verified"
    
    if [[ $success_count -eq $total_schemas ]]; then
        log_success "All load test schemas verified successfully!"
        return 0
    else
        log_error "Some schemas are missing or invalid"
        return 1
    fi
}

# List all registered subjects
list_subjects() {
    log_info "Listing all registered subjects..."
    
    local subjects
    subjects=$(curl -s "$SCHEMA_REGISTRY_URL/subjects" 2>/dev/null)
    
    if echo "$subjects" | jq -e '.[]' >/dev/null 2>&1; then
        echo "$subjects" | jq -r '.[]' | while read -r subject; do
            log_info "  - $subject"
        done
    else
        log_warning "No subjects found or Schema Registry not accessible"
    fi
}

# Clean up schemas (for testing)
cleanup_schemas() {
    log_warning "Cleaning up load test schemas..."
    
    local topics=("loadtest-topic-0" "loadtest-topic-1" "loadtest-topic-2" "loadtest-topic-3" "loadtest-topic-4")
    
    for topic in "${topics[@]}"; do
        # Delete value schema
        curl -s -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${topic}-value" >/dev/null 2>&1 || true
        curl -s -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${topic}-value?permanent=true" >/dev/null 2>&1 || true
        
        # Delete key schema
        curl -s -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${topic}-key" >/dev/null 2>&1 || true
        curl -s -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${topic}-key?permanent=true" >/dev/null 2>&1 || true
    done
    
    log_success "Schema cleanup completed"
}

# Main function
main() {
    case "${1:-register}" in
        "register")
            wait_for_schema_registry
            register_loadtest_schemas
            ;;
        "verify")
            wait_for_schema_registry
            verify_loadtest_schemas
            ;;
        "list")
            wait_for_schema_registry
            list_subjects
            ;;
        "cleanup")
            wait_for_schema_registry
            cleanup_schemas
            ;;
        "full")
            wait_for_schema_registry
            register_loadtest_schemas
            verify_loadtest_schemas
            list_subjects
            ;;
        *)
            echo "Usage: $0 [register|verify|list|cleanup|full]"
            echo ""
            echo "Commands:"
            echo "  register - Register load test schemas (default)"
            echo "  verify   - Verify schemas are registered"
            echo "  list     - List all registered subjects"
            echo "  cleanup  - Clean up load test schemas"
            echo "  full     - Register, verify, and list schemas"
            echo ""
            echo "Environment variables:"
            echo "  SCHEMA_REGISTRY_URL - Schema Registry URL (default: http://localhost:8081)"
            echo "  TIMEOUT - Maximum time to wait for Schema Registry (default: 60)"
            echo "  CHECK_INTERVAL - Check interval in seconds (default: 2)"
            exit 1
            ;;
    esac
}

main "$@"
