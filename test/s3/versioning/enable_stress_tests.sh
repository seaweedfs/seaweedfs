#!/bin/bash

# Enable S3 Versioning Stress Tests

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}📚 Enabling S3 Versioning Stress Tests${NC}"

# Disable short mode to enable stress tests
export ENABLE_STRESS_TESTS=true

# Run versioning stress tests
echo -e "${YELLOW}🧪 Running versioning stress tests...${NC}"
make test-versioning-stress

echo -e "${GREEN}✅ Versioning stress tests completed${NC}"
