#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Build weed binary
echo "Building weed binary..."
cd "$PROJECT_ROOT/weed" && go install

# Kill existing server
if lsof -Pi :8333 -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    kill $(lsof -t -i:8333) 2>/dev/null || true
fi

# Start server using weed mini for simpler all-in-one deployment
weed mini \
    -s3 \
    -s3.port=8333 \
    -s3.config="$SCRIPT_DIR/empty_s3_config.json" \
    -s3.iam.config="$SCRIPT_DIR/test_iam_config.json" \
    -s3.allowDeleteBucketNotEmpty=true \
    > /tmp/weed_test_server_custom.log 2>&1 &
SERVER_PID=$!

# Wait for server
MAX_WAIT=30
COUNTER=0
while ! curl -s http://localhost:8333/status > /dev/null 2>&1; do
    sleep 1
    COUNTER=$((COUNTER + 1))
    if [ $COUNTER -ge $MAX_WAIT ]; then
        echo "Server failed to start"
        cat /tmp/weed_test_server_custom.log
        kill $SERVER_PID
        exit 1
    fi
done

trap "kill $SERVER_PID" EXIT

cd "$SCRIPT_DIR"
if [ $# -eq 0 ]; then
    go test -v -run TestS3IAMMultipartUploadPolicyEnforcement .
else
    go test -v "$@" .
fi
