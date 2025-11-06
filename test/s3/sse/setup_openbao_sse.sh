#!/bin/bash

# Setup OpenBao for SSE Integration Testing
# This script configures OpenBao with encryption keys for S3 SSE testing

set -e

# Configuration
OPENBAO_ADDR="${OPENBAO_ADDR:-http://127.0.0.1:8200}"
OPENBAO_TOKEN="${OPENBAO_TOKEN:-root-token-for-testing}"
TRANSIT_PATH="${TRANSIT_PATH:-transit}"

echo "🚀 Setting up OpenBao for S3 SSE integration testing..."
echo "OpenBao Address: $OPENBAO_ADDR"
echo "Transit Path: $TRANSIT_PATH"

# Export for API calls
export VAULT_ADDR="$OPENBAO_ADDR"
export VAULT_TOKEN="$OPENBAO_TOKEN"

# Wait for OpenBao to be ready
echo "⏳ Waiting for OpenBao to be ready..."
for i in {1..30}; do
    if curl -s "$OPENBAO_ADDR/v1/sys/health" > /dev/null 2>&1; then
        echo "[OK] OpenBao is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "[FAIL] OpenBao failed to start within 60 seconds"
        exit 1
    fi
    sleep 2
done

# Enable transit secrets engine (ignore error if already enabled)
echo "🔧 Setting up transit secrets engine..."
curl -s -X POST \
    -H "X-Vault-Token: $OPENBAO_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"type\":\"transit\"}" \
    "$OPENBAO_ADDR/v1/sys/mounts/$TRANSIT_PATH" || echo "Transit engine may already be enabled"

# Create encryption keys for S3 SSE testing
echo "🔑 Creating encryption keys for SSE testing..."

# Test keys that match the existing test expectations
declare -a keys=(
    "test-key-123:SSE-KMS basic integration test key"
    "source-test-key-123:SSE-KMS copy source key"
    "dest-test-key-456:SSE-KMS copy destination key"
    "test-multipart-key:SSE-KMS multipart upload test key"
    "invalid-test-key:SSE-KMS error testing key"
    "test-kms-range-key:SSE-KMS range request test key"
    "seaweedfs-test-key:General SeaweedFS SSE test key"
    "bucket-default-key:Default bucket encryption key"
    "high-security-key:High security encryption key"
    "performance-key:Performance testing key"
)

for key_info in "${keys[@]}"; do
    IFS=':' read -r key_name description <<< "$key_info"
    echo "   Creating key: $key_name ($description)"
    
    # Create key
    response=$(curl -s -X POST \
        -H "X-Vault-Token: $OPENBAO_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"aes256-gcm96\",\"description\":\"$description\"}" \
        "$OPENBAO_ADDR/v1/$TRANSIT_PATH/keys/$key_name")
    
    if echo "$response" | grep -q "errors"; then
        echo "     Warning: $response"
    fi
    
    # Verify key was created
    verify_response=$(curl -s \
        -H "X-Vault-Token: $OPENBAO_TOKEN" \
        "$OPENBAO_ADDR/v1/$TRANSIT_PATH/keys/$key_name")
    
    if echo "$verify_response" | grep -q "\"name\":\"$key_name\""; then
        echo "     [OK] Key $key_name created successfully"
    else
        echo "     [FAIL] Failed to verify key $key_name"
        echo "     Response: $verify_response"
    fi
done

# Test basic encryption/decryption functionality
echo "🧪 Testing basic encryption/decryption..."
test_plaintext="Hello, SeaweedFS SSE Integration!"
test_key="test-key-123"

# Encrypt
encrypt_response=$(curl -s -X POST \
    -H "X-Vault-Token: $OPENBAO_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"plaintext\":\"$(echo -n "$test_plaintext" | base64)\"}" \
    "$OPENBAO_ADDR/v1/$TRANSIT_PATH/encrypt/$test_key")

if echo "$encrypt_response" | grep -q "ciphertext"; then
    ciphertext=$(echo "$encrypt_response" | grep -o '"ciphertext":"[^"]*"' | cut -d'"' -f4)
    echo "   [OK] Encryption successful: ${ciphertext:0:50}..."
    
    # Decrypt to verify
    decrypt_response=$(curl -s -X POST \
        -H "X-Vault-Token: $OPENBAO_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"ciphertext\":\"$ciphertext\"}" \
        "$OPENBAO_ADDR/v1/$TRANSIT_PATH/decrypt/$test_key")
    
    if echo "$decrypt_response" | grep -q "plaintext"; then
        decrypted_b64=$(echo "$decrypt_response" | grep -o '"plaintext":"[^"]*"' | cut -d'"' -f4)
        decrypted=$(echo "$decrypted_b64" | base64 -d)
        if [ "$decrypted" = "$test_plaintext" ]; then
            echo "   [OK] Decryption successful: $decrypted"
        else
            echo "   [FAIL] Decryption failed: expected '$test_plaintext', got '$decrypted'"
        fi
    else
        echo "   [FAIL] Decryption failed: $decrypt_response"
    fi
else
    echo "   [FAIL] Encryption failed: $encrypt_response"
fi

echo ""
echo "📊 OpenBao SSE setup summary:"
echo "   Address: $OPENBAO_ADDR"
echo "   Transit Path: $TRANSIT_PATH"
echo "   Keys Created: ${#keys[@]}"
echo "   Status: Ready for S3 SSE integration testing"
echo ""
echo "🎯 Ready to run S3 SSE integration tests!"
echo ""
echo "Usage:"
echo "   # Run with Docker Compose"
echo "   make test-with-kms"
echo ""
echo "   # Run specific test suites"
echo "   make test-ssekms-integration"
echo ""
echo "   # Check status"
echo "   curl $OPENBAO_ADDR/v1/sys/health"
echo ""

echo "[OK] OpenBao SSE setup complete!"
