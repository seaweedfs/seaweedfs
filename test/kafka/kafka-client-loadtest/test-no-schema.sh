#!/bin/bash
# Test without schema registry to isolate missing messages issue

# Clean old data
find test-results -name "*.jsonl" -delete 2>/dev/null || true

# Run test without schemas
TEST_MODE=comprehensive \
TEST_DURATION=1m \
PRODUCER_COUNT=2 \
CONSUMER_COUNT=2 \
MESSAGE_RATE=50 \
MESSAGE_SIZE=512 \
VALUE_TYPE=json \
SCHEMAS_ENABLED=false \
docker compose --profile loadtest up --abort-on-container-exit kafka-client-loadtest

echo ""
echo "═══════════════════════════════════════════════════════"
echo "Analyzing results..."
if [ -f test-results/produced.jsonl ] && [ -f test-results/consumed.jsonl ]; then
    produced=$(wc -l < test-results/produced.jsonl)
    consumed=$(wc -l < test-results/consumed.jsonl)
    echo "Produced: $produced"
    echo "Consumed: $consumed"
    
    # Check for missing messages
    jq -r '"\(.topic)[\(.partition)]@\(.offset)"' test-results/produced.jsonl | sort > /tmp/produced.txt
    jq -r '"\(.topic)[\(.partition)]@\(.offset)"' test-results/consumed.jsonl | sort > /tmp/consumed.txt
    missing=$(comm -23 /tmp/produced.txt /tmp/consumed.txt | wc -l)
    echo "Missing: $missing"
    
    if [ $missing -eq 0 ]; then
        echo "✓ NO MISSING MESSAGES!"
    else
        echo "✗ Still have missing messages"
        echo "Sample missing:"
        comm -23 /tmp/produced.txt /tmp/consumed.txt | head -10
    fi
else
    echo "✗ Result files not found"
fi
echo "═══════════════════════════════════════════════════════"
