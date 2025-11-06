#!/bin/bash

# Setup script for OpenBao KMS integration testing
set -e

OPENBAO_ADDR=${OPENBAO_ADDR:-"http://127.0.0.1:8200"}
OPENBAO_TOKEN=${OPENBAO_TOKEN:-"root-token-for-testing"}
TRANSIT_PATH=${TRANSIT_PATH:-"transit"}

echo "🚀 Setting up OpenBao for KMS integration testing..."
echo "OpenBao Address: $OPENBAO_ADDR"
echo "Transit Path: $TRANSIT_PATH"

# Wait for OpenBao to be ready
echo "⏳ Waiting for OpenBao to be ready..."
for i in {1..30}; do
    if curl -s "$OPENBAO_ADDR/v1/sys/health" >/dev/null 2>&1; then
        echo "[OK] OpenBao is ready!"
        break
    fi
    echo "   Attempt $i/30: OpenBao not ready yet, waiting..."
    sleep 2
done

# Check if we can connect
if ! curl -s -H "X-Vault-Token: $OPENBAO_TOKEN" "$OPENBAO_ADDR/v1/sys/health" >/dev/null; then
    echo "[FAIL] Cannot connect to OpenBao at $OPENBAO_ADDR"
    exit 1
fi

echo "🔧 Setting up transit secrets engine..."

# Enable transit secrets engine (ignore if already enabled)
curl -s -X POST \
    -H "X-Vault-Token: $OPENBAO_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"type":"transit","description":"Transit engine for KMS testing"}' \
    "$OPENBAO_ADDR/v1/sys/mounts/$TRANSIT_PATH" || true

echo "🔑 Creating test encryption keys..."

# Define test keys
declare -a TEST_KEYS=(
    "test-key-1:aes256-gcm96:Test key 1 for basic operations"
    "test-key-2:aes256-gcm96:Test key 2 for multi-key scenarios" 
    "seaweedfs-test-key:aes256-gcm96:SeaweedFS integration test key"
    "bucket-default-key:aes256-gcm96:Default key for bucket encryption"
    "high-security-key:aes256-gcm96:High security test key"
    "performance-key:aes256-gcm96:Performance testing key"
    "aws-compat-key:aes256-gcm96:AWS compatibility test key"
    "multipart-key:aes256-gcm96:Multipart upload test key"
)

# Create each test key
for key_spec in "${TEST_KEYS[@]}"; do
    IFS=':' read -r key_name key_type key_desc <<< "$key_spec"
    
    echo "   Creating key: $key_name ($key_type)"
    
    # Create the encryption key
    curl -s -X POST \
        -H "X-Vault-Token: $OPENBAO_TOKEN" \
        -H "Content-Type: application/json" \
        -d "{\"type\":\"$key_type\",\"description\":\"$key_desc\"}" \
        "$OPENBAO_ADDR/v1/$TRANSIT_PATH/keys/$key_name" || {
        echo "   ⚠️  Key $key_name might already exist"
    }
    
    # Verify the key was created
    if curl -s -H "X-Vault-Token: $OPENBAO_TOKEN" "$OPENBAO_ADDR/v1/$TRANSIT_PATH/keys/$key_name" >/dev/null; then
        echo "   [OK] Key $key_name verified"
    else
        echo "   [FAIL] Failed to create/verify key $key_name"
        exit 1
    fi
done

echo "🧪 Testing basic encryption/decryption..."

# Test basic encrypt/decrypt operation
TEST_PLAINTEXT="Hello, SeaweedFS KMS Integration!"
PLAINTEXT_B64=$(echo -n "$TEST_PLAINTEXT" | base64)

echo "   Testing with key: test-key-1"

# Encrypt
ENCRYPT_RESPONSE=$(curl -s -X POST \
    -H "X-Vault-Token: $OPENBAO_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"plaintext\":\"$PLAINTEXT_B64\"}" \
    "$OPENBAO_ADDR/v1/$TRANSIT_PATH/encrypt/test-key-1")

CIPHERTEXT=$(echo "$ENCRYPT_RESPONSE" | jq -r '.data.ciphertext')

if [[ "$CIPHERTEXT" == "null" || -z "$CIPHERTEXT" ]]; then
    echo "   [FAIL] Encryption test failed"
    echo "   Response: $ENCRYPT_RESPONSE"
    exit 1
fi

echo "   [OK] Encryption successful: ${CIPHERTEXT:0:50}..."

# Decrypt
DECRYPT_RESPONSE=$(curl -s -X POST \
    -H "X-Vault-Token: $OPENBAO_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"ciphertext\":\"$CIPHERTEXT\"}" \
    "$OPENBAO_ADDR/v1/$TRANSIT_PATH/decrypt/test-key-1")

DECRYPTED_B64=$(echo "$DECRYPT_RESPONSE" | jq -r '.data.plaintext')
DECRYPTED_TEXT=$(echo "$DECRYPTED_B64" | base64 -d)

if [[ "$DECRYPTED_TEXT" != "$TEST_PLAINTEXT" ]]; then
    echo "   [FAIL] Decryption test failed"
    echo "   Expected: $TEST_PLAINTEXT"
    echo "   Got: $DECRYPTED_TEXT"
    exit 1
fi

echo "   [OK] Decryption successful: $DECRYPTED_TEXT"

echo "📊 OpenBao KMS setup summary:"
echo "   Address: $OPENBAO_ADDR"
echo "   Transit Path: $TRANSIT_PATH"
echo "   Keys Created: ${#TEST_KEYS[@]}"
echo "   Status: Ready for integration testing"

echo ""
echo "🎯 Ready to run KMS integration tests!"
echo ""
echo "Usage:"
echo "   # Run Go integration tests"
echo "   go test -v ./test/kms/..."
echo ""
echo "   # Run with Docker Compose"
echo "   cd test/kms && docker-compose up -d"
echo "   docker-compose exec openbao bao status"
echo ""
echo "   # Test S3 API with encryption"
echo "   aws s3api put-bucket-encryption \\"
echo "     --endpoint-url http://localhost:8333 \\"
echo "     --bucket test-bucket \\"
echo "     --server-side-encryption-configuration file://bucket-encryption.json"
echo ""
echo "[OK] OpenBao KMS setup complete!"
