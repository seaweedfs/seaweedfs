#!/bin/bash
# Test runner for S3 policy variables integration tests
# This script starts a SeaweedFS server with the required IAM configuration
# and runs the integration tests.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== S3 Policy Variables Integration Test Runner ===${NC}"

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Always build to ensure latest changes are tested
echo -e "${YELLOW}Building weed binary...${NC}"
cd "$PROJECT_ROOT/weed" && go install
if ! command -v weed &> /dev/null; then
    echo -e "${RED}Failed to build weed binary${NC}"
    exit 1
fi

# Kill any existing weed server on port 8333
echo "Checking for existing weed server..."
if lsof -Pi :8333 -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo -e "${YELLOW}Killing existing weed server on port 8333...${NC}"
    kill $(lsof -t -i:8333) 2>/dev/null || true
    sleep 2
fi

# Start weed server with IAM configuration
echo -e "${GREEN}Starting weed server with IAM configuration...${NC}"
weed server \
    -s3 \
    -s3.port=8333 \
    -s3.iam.config="$SCRIPT_DIR/test_iam_config.json" \
    -filer \
    -volume.max=0 \
    -master.volumeSizeLimitMB=100 \
    -s3.allowDeleteBucketNotEmpty=true \
    -s3.iam.readOnly=false \
    > /tmp/weed_test_server.log 2>&1 &

SERVER_PID=$!
echo "Server started with PID: $SERVER_PID"

# Wait for server to be ready
echo "Waiting for server to be ready..."
MAX_WAIT=30
COUNTER=0
while ! curl -s http://localhost:8333/status > /dev/null 2>&1; do
    sleep 1
    COUNTER=$((COUNTER + 1))
    if [ $COUNTER -ge $MAX_WAIT ]; then
        echo -e "${RED}Server failed to start within ${MAX_WAIT} seconds${NC}"
        echo "Server log:"
        cat /tmp/weed_test_server.log
        kill $SERVER_PID 2>/dev/null || true
        exit 1
    fi
done

echo -e "${GREEN}Server is ready!${NC}"

# Run the tests
echo -e "${GREEN}Running integration tests...${NC}"
cd "$SCRIPT_DIR"

# Trap to ensure server is killed on exit
trap "echo -e '${YELLOW}Shutting down server...${NC}'; kill $SERVER_PID 2>/dev/null || true" EXIT

# Run the tests
go test -v -run TestS3PolicyVariables .

TEST_RESULT=$?

if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}=== All tests passed! ===${NC}"
else
    echo -e "${RED}=== Tests failed ===${NC}"
    echo "Server log (last 50 lines):"
    tail -50 /tmp/weed_test_server.log
fi

exit $TEST_RESULT
