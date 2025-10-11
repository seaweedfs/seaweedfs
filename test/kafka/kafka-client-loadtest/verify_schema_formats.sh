#!/bin/bash
# Verify schema format distribution across topics

set -e

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
TOPIC_PREFIX="${TOPIC_PREFIX:-loadtest-topic}"
TOPIC_COUNT="${TOPIC_COUNT:-5}"

echo "================================"
echo "Schema Format Verification"
echo "================================"
echo ""
echo "Schema Registry: $SCHEMA_REGISTRY_URL"
echo "Topic Prefix: $TOPIC_PREFIX"
echo "Topic Count: $TOPIC_COUNT"
echo ""

echo "Registered Schemas:"
echo "-------------------"

for i in $(seq 0 $((TOPIC_COUNT-1))); do
    topic="${TOPIC_PREFIX}-${i}"
    subject="${topic}-value"
    
    echo -n "Topic $i ($topic): "
    
    # Try to get schema
    response=$(curl -s "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions/latest" 2>/dev/null || echo '{"error":"not found"}')
    
    if echo "$response" | grep -q "error"; then
        echo "❌ NOT REGISTERED"
    else
        schema_type=$(echo "$response" | grep -o '"schemaType":"[^"]*"' | cut -d'"' -f4)
        schema_id=$(echo "$response" | grep -o '"id":[0-9]*' | cut -d':' -f2)
        
        if [ -z "$schema_type" ]; then
            schema_type="AVRO"  # Default if not specified
        fi
        
        # Expected format based on index
        if [ $((i % 2)) -eq 0 ]; then
            expected="AVRO"
        else
            expected="JSON"
        fi
        
        if [ "$schema_type" = "$expected" ]; then
            echo "✅ $schema_type (ID: $schema_id) - matches expected"
        else
            echo "⚠️  $schema_type (ID: $schema_id) - expected $expected"
        fi
    fi
done

echo ""
echo "Expected Distribution:"
echo "----------------------"
echo "Even indices (0, 2, 4, ...): AVRO"
echo "Odd indices  (1, 3, 5, ...): JSON"
echo ""


