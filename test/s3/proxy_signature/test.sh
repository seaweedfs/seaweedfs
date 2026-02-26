#!/usr/bin/env bash
#
# Integration test for S3 signature verification behind a reverse proxy.
#
# Usage:
#   # With aws CLI installed locally:
#   docker compose up -d --build && ./test.sh && docker compose down
#
#   # Without aws CLI (runs test inside a container):
#   docker compose up -d --build
#   docker run --rm --network host --entrypoint "" amazon/aws-cli:latest \
#       bash < test.sh
#   docker compose down
#
# This script tests S3 operations through an nginx reverse proxy to verify
# that signature verification works correctly when SeaweedFS is configured
# with -s3.externalUrl=http://localhost:9000.
#
set -euo pipefail

PROXY_ENDPOINT="http://localhost:9000"
ACCESS_KEY="test_access_key"
SECRET_KEY="test_secret_key"
REGION="us-east-1"
BUCKET="test-proxy-sig-$$"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

pass() { echo -e "${GREEN}PASS${NC}: $1"; }
fail() { echo -e "${RED}FAIL${NC}: $1"; exit 1; }

# Helper: run aws s3api command against a given endpoint
s3() {
    local endpoint="$1"
    shift
    aws s3api \
        --endpoint-url "$endpoint" \
        --region "$REGION" \
        --no-verify-ssl \
        "$@" 2>&1
}

export AWS_ACCESS_KEY_ID="$ACCESS_KEY"
export AWS_SECRET_ACCESS_KEY="$SECRET_KEY"

echo "=== S3 Proxy Signature Verification Test ==="
echo ""
echo "Testing S3 access through nginx reverse proxy at $PROXY_ENDPOINT"
echo "SeaweedFS configured with -s3.externalUrl=http://localhost:9000"
echo "AWS CLI signs requests with Host: localhost:9000"
echo ""

# Wait for proxy to be ready
echo "Waiting for nginx proxy to be ready..."
for i in $(seq 1 30); do
    http_code=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:9000/" 2>/dev/null || echo "000")
    case $http_code in
        200|[3][0-9][0-9]|403)
            break
            ;;
    esac
    if [ "$i" -eq 30 ]; then
        fail "Proxy did not become ready in time"
    fi
    sleep 1
done
echo "Proxy is ready."
echo ""

# --- Test 1: Bucket operations through proxy ---
echo "--- Test 1: Bucket operations through proxy ---"
s3 "$PROXY_ENDPOINT" create-bucket --bucket "$BUCKET" > /dev/null \
    && pass "create-bucket" \
    || fail "create-bucket — signature verification likely failed"

s3 "$PROXY_ENDPOINT" list-buckets > /dev/null \
    && pass "list-buckets" \
    || fail "list-buckets"
echo ""

# --- Test 2: Object CRUD through proxy ---
echo "--- Test 2: Object CRUD through proxy ---"
echo "hello-from-proxy" > /tmp/test-proxy-sig.txt

s3 "$PROXY_ENDPOINT" put-object --bucket "$BUCKET" --key "test.txt" --body /tmp/test-proxy-sig.txt > /dev/null \
    && pass "put-object" \
    || fail "put-object"

s3 "$PROXY_ENDPOINT" head-object --bucket "$BUCKET" --key "test.txt" > /dev/null \
    && pass "head-object" \
    || fail "head-object"

s3 "$PROXY_ENDPOINT" list-objects-v2 --bucket "$BUCKET" > /dev/null \
    && pass "list-objects-v2" \
    || fail "list-objects-v2"

s3 "$PROXY_ENDPOINT" get-object --bucket "$BUCKET" --key "test.txt" /tmp/test-proxy-sig-get.txt > /dev/null \
    && pass "get-object" \
    || fail "get-object"

# Verify content round-trip
CONTENT=$(cat /tmp/test-proxy-sig-get.txt)
if [ "$CONTENT" = "hello-from-proxy" ]; then
    pass "content integrity (round-trip)"
else
    fail "content mismatch: got \"$CONTENT\", expected \"hello-from-proxy\""
fi
echo ""

# --- Test 3: Delete operations through proxy ---
echo "--- Test 3: Delete through proxy ---"
s3 "$PROXY_ENDPOINT" delete-object --bucket "$BUCKET" --key "test.txt" > /dev/null \
    && pass "delete-object" \
    || fail "delete-object"

s3 "$PROXY_ENDPOINT" delete-bucket --bucket "$BUCKET" > /dev/null \
    && pass "delete-bucket" \
    || fail "delete-bucket"
echo ""

# Cleanup temp files
rm -f /tmp/test-proxy-sig.txt /tmp/test-proxy-sig-get.txt

echo "=== All tests passed ==="
