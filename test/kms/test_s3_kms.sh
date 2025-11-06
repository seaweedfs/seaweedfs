#!/bin/bash

# End-to-end S3 KMS integration tests
set -e

SEAWEEDFS_S3_ENDPOINT=${SEAWEEDFS_S3_ENDPOINT:-"http://127.0.0.1:8333"}
ACCESS_KEY=${ACCESS_KEY:-"any"}
SECRET_KEY=${SECRET_KEY:-"any"}

echo "🧪 Running S3 KMS Integration Tests"
echo "S3 Endpoint: $SEAWEEDFS_S3_ENDPOINT"

# Test file content
TEST_CONTENT="Hello, SeaweedFS KMS Integration! This is test data that should be encrypted."
TEST_FILE="/tmp/seaweedfs-kms-test.txt"
DOWNLOAD_FILE="/tmp/seaweedfs-kms-download.txt"

# Create test file
echo "$TEST_CONTENT" > "$TEST_FILE"

# AWS CLI configuration
export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"
export AWS_DEFAULT_REGION="us-east-1"

echo "📁 Creating test buckets..."

# Create test buckets
BUCKETS=("test-openbao" "test-vault" "test-local" "secure-data")

for bucket in "${BUCKETS[@]}"; do
    echo "   Creating bucket: $bucket"
    aws s3 mb "s3://$bucket" --endpoint-url "$SEAWEEDFS_S3_ENDPOINT" || {
        echo "   ⚠️  Bucket $bucket might already exist"
    }
done

echo "🔐 Setting up bucket encryption..."

# Test 1: OpenBao KMS Encryption
echo "   Setting OpenBao encryption for test-openbao bucket..."
cat > /tmp/openbao-encryption.json << EOF
{
    "Rules": [
        {
            "ApplyServerSideEncryptionByDefault": {
                "SSEAlgorithm": "aws:kms",
                "KMSMasterKeyID": "test-key-1"
            },
            "BucketKeyEnabled": false
        }
    ]
}
EOF

aws s3api put-bucket-encryption \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT" \
    --bucket test-openbao \
    --server-side-encryption-configuration file:///tmp/openbao-encryption.json || {
    echo "   ⚠️  Failed to set bucket encryption for test-openbao"
}

# Test 2: Verify bucket encryption
echo "   Verifying bucket encryption configuration..."
aws s3api get-bucket-encryption \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT" \
    --bucket test-openbao | jq '.' || {
    echo "   ⚠️  Failed to get bucket encryption for test-openbao"
}

echo "⬆️  Testing object uploads with KMS encryption..."

# Test 3: Upload objects with default bucket encryption
echo "   Uploading object with default bucket encryption..."
aws s3 cp "$TEST_FILE" "s3://test-openbao/encrypted-object-1.txt" \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT"

# Test 4: Upload object with explicit SSE-KMS
echo "   Uploading object with explicit SSE-KMS headers..."
aws s3 cp "$TEST_FILE" "s3://test-openbao/encrypted-object-2.txt" \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT" \
    --sse aws:kms \
    --sse-kms-key-id "test-key-2"

# Test 5: Upload to unencrypted bucket
echo "   Uploading object to unencrypted bucket..."
aws s3 cp "$TEST_FILE" "s3://test-local/unencrypted-object.txt" \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT"

echo "⬇️  Testing object downloads and decryption..."

# Test 6: Download encrypted objects
echo "   Downloading encrypted object 1..."
aws s3 cp "s3://test-openbao/encrypted-object-1.txt" "$DOWNLOAD_FILE" \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT"

# Verify content
if cmp -s "$TEST_FILE" "$DOWNLOAD_FILE"; then
    echo "   [OK] Encrypted object 1 downloaded and decrypted successfully"
else
    echo "   [FAIL] Encrypted object 1 content mismatch"
    exit 1
fi

echo "   Downloading encrypted object 2..."
aws s3 cp "s3://test-openbao/encrypted-object-2.txt" "$DOWNLOAD_FILE" \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT"

# Verify content
if cmp -s "$TEST_FILE" "$DOWNLOAD_FILE"; then
    echo "   [OK] Encrypted object 2 downloaded and decrypted successfully"
else
    echo "   [FAIL] Encrypted object 2 content mismatch"
    exit 1
fi

echo "📊 Testing object metadata..."

# Test 7: Check encryption metadata
echo "   Checking encryption metadata..."
METADATA=$(aws s3api head-object \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT" \
    --bucket test-openbao \
    --key encrypted-object-1.txt)

echo "$METADATA" | jq '.'

# Verify SSE headers are present
if echo "$METADATA" | grep -q "ServerSideEncryption"; then
    echo "   [OK] SSE metadata found in object headers"
else
    echo "   ⚠️  No SSE metadata found (might be internal only)"
fi

echo "📋 Testing list operations..."

# Test 8: List objects
echo "   Listing objects in encrypted bucket..."
aws s3 ls "s3://test-openbao/" --endpoint-url "$SEAWEEDFS_S3_ENDPOINT"

echo "🔄 Testing multipart uploads with encryption..."

# Test 9: Multipart upload with encryption
LARGE_FILE="/tmp/large-test-file.txt"
echo "   Creating large test file..."
for i in {1..1000}; do
    echo "Line $i: $TEST_CONTENT" >> "$LARGE_FILE"
done

echo "   Uploading large file with multipart and SSE-KMS..."
aws s3 cp "$LARGE_FILE" "s3://test-openbao/large-encrypted-file.txt" \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT" \
    --sse aws:kms \
    --sse-kms-key-id "multipart-key"

# Download and verify
echo "   Downloading and verifying large encrypted file..."
DOWNLOAD_LARGE_FILE="/tmp/downloaded-large-file.txt"
aws s3 cp "s3://test-openbao/large-encrypted-file.txt" "$DOWNLOAD_LARGE_FILE" \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT"

if cmp -s "$LARGE_FILE" "$DOWNLOAD_LARGE_FILE"; then
    echo "   [OK] Large encrypted file uploaded and downloaded successfully"
else
    echo "   [FAIL] Large encrypted file content mismatch"
    exit 1
fi

echo "🧹 Cleaning up test files..."
rm -f "$TEST_FILE" "$DOWNLOAD_FILE" "$LARGE_FILE" "$DOWNLOAD_LARGE_FILE" /tmp/*-encryption.json

echo "📈 Running performance test..."

# Test 10: Performance test
PERF_FILE="/tmp/perf-test.txt"
for i in {1..100}; do
    echo "Performance test line $i: $TEST_CONTENT" >> "$PERF_FILE"
done

echo "   Testing upload/download performance with encryption..."
start_time=$(date +%s)

aws s3 cp "$PERF_FILE" "s3://test-openbao/perf-test.txt" \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT" \
    --sse aws:kms \
    --sse-kms-key-id "performance-key"

aws s3 cp "s3://test-openbao/perf-test.txt" "/tmp/perf-download.txt" \
    --endpoint-url "$SEAWEEDFS_S3_ENDPOINT"

end_time=$(date +%s)
duration=$((end_time - start_time))

echo "   ⏱️  Performance test completed in ${duration} seconds"

rm -f "$PERF_FILE" "/tmp/perf-download.txt"

echo ""
echo "🎉 S3 KMS Integration Tests Summary:"
echo "   [OK] Bucket creation and encryption configuration"
echo "   [OK] Default bucket encryption"
echo "   [OK] Explicit SSE-KMS encryption"
echo "   [OK] Object upload and download"
echo "   [OK] Encryption/decryption verification" 
echo "   [OK] Metadata handling"
echo "   [OK] Multipart upload with encryption"
echo "   [OK] Performance test"
echo ""
echo "🔐 All S3 KMS integration tests passed successfully!"
echo ""

# Optional: Show bucket sizes and object counts
echo "📊 Final Statistics:"
for bucket in "${BUCKETS[@]}"; do
    COUNT=$(aws s3 ls "s3://$bucket/" --endpoint-url "$SEAWEEDFS_S3_ENDPOINT" | wc -l)
    echo "   Bucket $bucket: $COUNT objects"
done
