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
    echo -e "${YELLOW}[WARN]${NC} $1"
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
        if curl -sf --max-time 5 "$SCHEMA_REGISTRY_URL/subjects" >/dev/null 2>&1; then
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
    local max_attempts=5
    local attempt=1
    
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
    
    while [[ $attempt -le $max_attempts ]]; do
        # Register the schema (with 30 second timeout)
        local response
        response=$(curl -s --max-time 30 -X POST \
            -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            -d "$payload" \
            "$SCHEMA_REGISTRY_URL/subjects/$subject/versions" 2>/dev/null)
        
        if echo "$response" | jq -e '.id' >/dev/null 2>&1; then
            local schema_id
            schema_id=$(echo "$response" | jq -r '.id')
            if [[ $attempt -gt 1 ]]; then
                log_success "- Schema registered for $subject with ID: $schema_id [attempt $attempt]"
            else
                log_success "- Schema registered for $subject with ID: $schema_id"
            fi
            return 0
        fi
        
        # Check if it's a consumer lag timeout (error_code 50002)
        local error_code
        error_code=$(echo "$response" | jq -r '.error_code // empty' 2>/dev/null)
        
        if [[ "$error_code" == "50002" && $attempt -lt $max_attempts ]]; then
            # Consumer lag timeout - wait longer for consumer to catch up
            # Use exponential backoff: 1s, 2s, 4s, 8s
            local wait_time=$(echo "2 ^ ($attempt - 1)" | bc)
            log_warning "Schema Registry consumer lag detected for $subject, waiting ${wait_time}s before retry (attempt $attempt)..."
            sleep "$wait_time"
            attempt=$((attempt + 1))
        else
            # Other error or max attempts reached
            log_error "x Failed to register schema for $subject"
            log_error "Response: $response"
            return 1
        fi
    done
    
    return 1
}

# Verify a schema exists (single attempt)
verify_schema() {
    local subject=$1
    
    local response
    response=$(curl -s --max-time 10 "$SCHEMA_REGISTRY_URL/subjects/$subject/versions/latest" 2>/dev/null)
    
    if echo "$response" | jq -e '.id' >/dev/null 2>&1; then
        local schema_id
        local version
        schema_id=$(echo "$response" | jq -r '.id')
        version=$(echo "$response" | jq -r '.version')
        log_success "- Schema verified for $subject (ID: $schema_id, Version: $version)"
        return 0
    else
        return 1
    fi
}

# Verify a schema exists with retry logic (handles Schema Registry consumer lag)
verify_schema_with_retry() {
    local subject=$1
    local max_attempts=10
    local attempt=1
    
    log_info "Verifying schema for subject: $subject"
    
    while [[ $attempt -le $max_attempts ]]; do
        local response
        response=$(curl -s --max-time 10 "$SCHEMA_REGISTRY_URL/subjects/$subject/versions/latest" 2>/dev/null)
        
        if echo "$response" | jq -e '.id' >/dev/null 2>&1; then
            local schema_id
            local version
            schema_id=$(echo "$response" | jq -r '.id')
            version=$(echo "$response" | jq -r '.version')
            
            if [[ $attempt -gt 1 ]]; then
                log_success "- Schema verified for $subject (ID: $schema_id, Version: $version) [attempt $attempt]"
            else
                log_success "- Schema verified for $subject (ID: $schema_id, Version: $version)"
            fi
            return 0
        fi
        
        # Schema not found, wait and retry (handles Schema Registry consumer lag)
        if [[ $attempt -lt $max_attempts ]]; then
            # Longer exponential backoff for Schema Registry consumer lag: 0.5s, 1s, 2s, 3s, 4s...
            local wait_time=$(echo "scale=1; 0.5 * $attempt" | bc)
            sleep "$wait_time"
            attempt=$((attempt + 1))
        else
            log_error "x Schema not found for $subject (tried $max_attempts times)"
            return 1
        fi
    done
    
    return 1
}

# Register load test schemas (optimized for batch registration)
register_loadtest_schemas() {
    log_info "Registering load test schemas with multiple formats..."
    
    # Define the Avro schema for load test messages
    local avro_value_schema='{
        "type": "record",
        "name": "LoadTestMessage",
        "namespace": "com.seaweedfs.loadtest",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "producer_id", "type": "int"},
            {"name": "counter", "type": "long"},
            {"name": "user_id", "type": "string"},
            {"name": "event_type", "type": "string"},
            {"name": "properties", "type": {"type": "map", "values": "string"}}
        ]
    }'
    
    # Define the JSON schema for load test messages
    local json_value_schema='{
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "LoadTestMessage",
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "timestamp": {"type": "integer"},
            "producer_id": {"type": "integer"},
            "counter": {"type": "integer"},
            "user_id": {"type": "string"},
            "event_type": {"type": "string"},
            "properties": {
                "type": "object",
                "additionalProperties": {"type": "string"}
            }
        },
        "required": ["id", "timestamp", "producer_id", "counter", "user_id", "event_type"]
    }'
    
    # Define the Protobuf schema for load test messages
    local protobuf_value_schema='syntax = "proto3";

package com.seaweedfs.loadtest;

message LoadTestMessage {
  string id = 1;
  int64 timestamp = 2;
  int32 producer_id = 3;
  int64 counter = 4;
  string user_id = 5;
  string event_type = 6;
  map<string, string> properties = 7;
}'
    
    # Define the key schema (simple string)
    local avro_key_schema='{"type": "string"}'
    local json_key_schema='{"type": "string"}'
    local protobuf_key_schema='syntax = "proto3"; message Key { string key = 1; }'
    
    # Register schemas for all load test topics with different formats
    local topics=("loadtest-topic-0" "loadtest-topic-1" "loadtest-topic-2" "loadtest-topic-3" "loadtest-topic-4")
    local success_count=0
    local total_schemas=0
    
    # Distribute formats: topic-0=AVRO, topic-1=JSON, topic-2=PROTOBUF, topic-3=AVRO, topic-4=JSON
    local idx=0
    for topic in "${topics[@]}"; do
        local format
        local value_schema
        local key_schema
        
        # Determine format based on topic index (same as producer logic)
        case $((idx % 3)) in
            0)
                format="AVRO"
                value_schema="$avro_value_schema"
                key_schema="$avro_key_schema"
                ;;
            1)
                format="JSON"
                value_schema="$json_value_schema"
                key_schema="$json_key_schema"
                ;;
            2)
                format="PROTOBUF"
                value_schema="$protobuf_value_schema"
                key_schema="$protobuf_key_schema"
                ;;
        esac
        
        log_info "Registering $topic with $format schema..."
        
        # Register value schema
        if register_schema "${topic}-value" "$value_schema" "$format"; then
            success_count=$((success_count + 1))
        fi
        total_schemas=$((total_schemas + 1))
        
        # Small delay to let Schema Registry consumer process (prevents consumer lag)
        sleep 0.2
        
        # Register key schema
        if register_schema "${topic}-key" "$key_schema" "$format"; then
            success_count=$((success_count + 1))
        fi
        total_schemas=$((total_schemas + 1))
        
        # Small delay to let Schema Registry consumer process (prevents consumer lag)
        sleep 0.2
        
        idx=$((idx + 1))
    done
    
    log_info "Schema registration summary: $success_count/$total_schemas schemas registered successfully"
    log_info "Format distribution: topic-0=AVRO, topic-1=JSON, topic-2=PROTOBUF, topic-3=AVRO, topic-4=JSON"
    
    if [[ $success_count -eq $total_schemas ]]; then
        log_success "All load test schemas registered successfully with multiple formats!"
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
        # Verify value schema with retry (handles Schema Registry consumer lag)
        if verify_schema_with_retry "${topic}-value"; then
            success_count=$((success_count + 1))
        fi
        total_schemas=$((total_schemas + 1))
        
        # Verify key schema with retry (handles Schema Registry consumer lag)
        if verify_schema_with_retry "${topic}-key"; then
            success_count=$((success_count + 1))
        fi
        total_schemas=$((total_schemas + 1))
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
    subjects=$(curl -s --max-time 10 "$SCHEMA_REGISTRY_URL/subjects" 2>/dev/null)
    
    if echo "$subjects" | jq -e '.[]' >/dev/null 2>&1; then
        # Use process substitution instead of pipeline to avoid subshell exit code issues
        while IFS= read -r subject; do
            log_info "  - $subject"
        done < <(echo "$subjects" | jq -r '.[]')
    else
        log_warning "No subjects found or Schema Registry not accessible"
    fi
    
    return 0
}

# Clean up schemas (for testing)
cleanup_schemas() {
    log_warning "Cleaning up load test schemas..."
    
    local topics=("loadtest-topic-0" "loadtest-topic-1" "loadtest-topic-2" "loadtest-topic-3" "loadtest-topic-4")
    
    for topic in "${topics[@]}"; do
        # Delete value schema (with timeout)
        curl -s --max-time 10 -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${topic}-value" >/dev/null 2>&1 || true
        curl -s --max-time 10 -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${topic}-value?permanent=true" >/dev/null 2>&1 || true
        
        # Delete key schema (with timeout)
        curl -s --max-time 10 -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${topic}-key" >/dev/null 2>&1 || true
        curl -s --max-time 10 -X DELETE "$SCHEMA_REGISTRY_URL/subjects/${topic}-key?permanent=true" >/dev/null 2>&1 || true
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
            # Wait for Schema Registry consumer to catch up before verification
            log_info "Waiting 3 seconds for Schema Registry consumer to process all schemas..."
            sleep 3
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
    
    return 0
}

main "$@"
