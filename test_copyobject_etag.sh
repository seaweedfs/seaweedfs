#!/bin/bash
# End-to-end test for CopyObject ETag fix (GitHub issue #8155)

set -e

# Configuration
WEED_BINARY="./weed/weed"
DATA_DIR="/tmp/seaweedfs_test_$$"
MASTER_PORT=9333
FILER_PORT=8888
S3_PORT=8333
S3_ENDPOINT="http://localhost:$S3_PORT"

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    # Kill seaweedfs processes
    pkill -f "weed server.*$DATA_DIR" 2>/dev/null || true
    sleep 2
    # Remove data directory
    rm -rf "$DATA_DIR"
    echo "Cleanup complete"
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Create data directory
mkdir -p "$DATA_DIR"

echo "=== Starting SeaweedFS server ==="
$WEED_BINARY server \
    -dir="$DATA_DIR" \
    -master.port=$MASTER_PORT \
    -filer.port=$FILER_PORT \
    -s3 \
    -s3.port=$S3_PORT \
    -volume.max=0 \
    -filer.disableDirListing=false \
    &> "$DATA_DIR/server.log" &

WEED_PID=$!
echo "SeaweedFS started with PID $WEED_PID"

# Wait for master to be ready
echo "Waiting for master to start..."
for i in {1..30}; do
    if curl -s "http://localhost:$MASTER_PORT/cluster/status" > /dev/null 2>&1; then
        echo "Master is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Master failed to start. Log:"
        cat "$DATA_DIR/server.log"
        exit 1
    fi
    sleep 1
done

# Wait for S3 endpoint to be ready
echo "Waiting for S3 endpoint to start..."
for i in {1..30}; do
    if curl -s "http://localhost:$S3_PORT" > /dev/null 2>&1; then
        echo "S3 endpoint is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "S3 endpoint failed to start. Log:"
        tail -100 "$DATA_DIR/server.log"
        exit 1
    fi
    sleep 1
done

# Additional wait for S3 endpoint to be fully ready
sleep 3

echo ""
echo "=== Configuring mc (MinIO client) ==="
mc alias set seaweedtest "$S3_ENDPOINT" "" "" --api "s3v4" 2>/dev/null || true

echo ""
echo "=== Creating test bucket ==="
mc mb seaweedtest/testbucket 2>/dev/null || true

echo ""
echo "=== Uploading test file ==="
echo "Hello, World! This is a test file for CopyObject ETag." > "$DATA_DIR/testfile.txt"
mc cp "$DATA_DIR/testfile.txt" seaweedtest/testbucket/original.txt

echo ""
echo "=== Getting original file ETag (HEAD) ==="
echo "Using mc stat:"
mc stat seaweedtest/testbucket/original.txt 2>&1 | grep -i etag || echo "(no etag shown by mc)"

echo ""
echo "Using curl HEAD:"
curl -s -I "$S3_ENDPOINT/testbucket/original.txt" | grep -i etag || echo "(no etag header)"

echo ""
echo "=== Performing CopyObject ==="
# Use curl to call CopyObject API directly and capture response
COPY_RESPONSE=$(curl -s -X PUT \
    -H "x-amz-copy-source: /testbucket/original.txt" \
    "$S3_ENDPOINT/testbucket/copied.txt")

echo "CopyObject Response:"
echo "$COPY_RESPONSE"

# Check if ETag is in the response
if echo "$COPY_RESPONSE" | grep -q "<ETag>"; then
    echo ""
    echo "SUCCESS: CopyObject response contains <ETag> element!"
    COPY_ETAG=$(echo "$COPY_RESPONSE" | grep -oP '(?<=<ETag>)[^<]+' | head -1)
    echo "Extracted ETag: $COPY_ETAG"
else
    echo ""
    echo "FAILURE: CopyObject response does NOT contain <ETag> element!"
fi

echo ""
echo "=== Verifying copied file ETag (HEAD) ==="
echo "Using curl HEAD:"
COPIED_ETAG_HEADER=$(curl -s -I "$S3_ENDPOINT/testbucket/copied.txt" | grep -i "^etag:" || echo "")
echo "ETag Header: $COPIED_ETAG_HEADER"

if [ -n "$COPIED_ETAG_HEADER" ]; then
    echo ""
    echo "SUCCESS: HeadObject returns ETag header for copied file!"
else
    echo ""
    echo "FAILURE: HeadObject does NOT return ETag header for copied file!"
fi

echo ""
echo "Using mc stat:"
mc stat seaweedtest/testbucket/copied.txt 2>&1 | grep -i etag || echo "(no etag shown by mc)"

echo ""
echo "=== Test Summary ==="
PASS_COUNT=0
FAIL_COUNT=0

if echo "$COPY_RESPONSE" | grep -q "<ETag>"; then
    echo "[PASS] CopyObject response contains ETag"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "[FAIL] CopyObject response missing ETag"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

if [ -n "$COPIED_ETAG_HEADER" ]; then
    echo "[PASS] HeadObject returns ETag header"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "[FAIL] HeadObject missing ETag header"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

echo ""
echo "Results: $PASS_COUNT passed, $FAIL_COUNT failed"

if [ $FAIL_COUNT -gt 0 ]; then
    echo ""
    echo "Server log tail:"
    tail -50 "$DATA_DIR/server.log"
    exit 1
fi

echo ""
echo "=== All tests passed! ==="
