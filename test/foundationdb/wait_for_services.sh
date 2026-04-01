#!/bin/bash

# Script to wait for all services to be ready
set -e

# Colors
BLUE='\033[36m'
GREEN='\033[32m'
YELLOW='\033[33m'
RED='\033[31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Waiting for FoundationDB cluster to be ready...${NC}"

# Wait for FoundationDB cluster
MAX_ATTEMPTS=30
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if docker-compose exec -T fdb1 fdbcli --exec 'status' > /dev/null 2>&1; then
        echo -e "${GREEN}✅ FoundationDB cluster is ready${NC}"
        break
    fi
    
    ATTEMPT=$((ATTEMPT + 1))
    echo -e "${YELLOW}Attempt $ATTEMPT/$MAX_ATTEMPTS - waiting for FoundationDB...${NC}"
    sleep 5
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo -e "${RED}❌ FoundationDB cluster failed to start after $MAX_ATTEMPTS attempts${NC}"
    echo -e "${RED}Checking logs...${NC}"
    docker-compose logs fdb1 fdb2 fdb3 fdb-init
    exit 1
fi

echo -e "${BLUE}Waiting for SeaweedFS to be ready...${NC}"

# Wait for SeaweedFS master
MAX_ATTEMPTS=20
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -s http://127.0.0.1:9333/cluster/status > /dev/null 2>&1; then
        echo -e "${GREEN}✅ SeaweedFS master is ready${NC}"
        break
    fi
    
    ATTEMPT=$((ATTEMPT + 1))
    echo -e "${YELLOW}Attempt $ATTEMPT/$MAX_ATTEMPTS - waiting for SeaweedFS master...${NC}"
    sleep 3
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo -e "${RED}❌ SeaweedFS master failed to start${NC}"
    docker-compose logs seaweedfs
    exit 1
fi

# Wait for SeaweedFS filer
MAX_ATTEMPTS=20
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -s http://127.0.0.1:8888/ > /dev/null 2>&1; then
        echo -e "${GREEN}✅ SeaweedFS filer is ready${NC}"
        break
    fi
    
    ATTEMPT=$((ATTEMPT + 1))
    echo -e "${YELLOW}Attempt $ATTEMPT/$MAX_ATTEMPTS - waiting for SeaweedFS filer...${NC}"
    sleep 3
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo -e "${RED}❌ SeaweedFS filer failed to start${NC}"
    docker-compose logs seaweedfs
    exit 1
fi

# Wait for SeaweedFS S3 API
MAX_ATTEMPTS=20
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    if curl -s http://127.0.0.1:8333/ > /dev/null 2>&1; then
        echo -e "${GREEN}✅ SeaweedFS S3 API is ready${NC}"
        break
    fi
    
    ATTEMPT=$((ATTEMPT + 1))
    echo -e "${YELLOW}Attempt $ATTEMPT/$MAX_ATTEMPTS - waiting for SeaweedFS S3 API...${NC}"
    sleep 3
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo -e "${RED}❌ SeaweedFS S3 API failed to start${NC}"
    docker-compose logs seaweedfs
    exit 1
fi

echo -e "${GREEN}🎉 All services are ready!${NC}"

# Display final status
echo -e "${BLUE}Final status check:${NC}"
docker-compose exec -T fdb1 fdbcli --exec 'status'
echo ""
echo -e "${BLUE}SeaweedFS cluster info:${NC}"
curl -s http://127.0.0.1:9333/cluster/status | head -20
