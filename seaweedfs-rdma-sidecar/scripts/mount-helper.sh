#!/bin/bash

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration from environment variables
FILER_ADDR=${FILER_ADDR:-"seaweedfs-filer:8888"}
RDMA_SIDECAR_ADDR=${RDMA_SIDECAR_ADDR:-"rdma-sidecar:8081"}
MOUNT_POINT=${MOUNT_POINT:-"/mnt/seaweedfs"}
RDMA_ENABLED=${RDMA_ENABLED:-"true"}
RDMA_FALLBACK=${RDMA_FALLBACK:-"true"}
RDMA_MAX_CONCURRENT=${RDMA_MAX_CONCURRENT:-"64"}
RDMA_TIMEOUT_MS=${RDMA_TIMEOUT_MS:-"5000"}
DEBUG=${DEBUG:-"false"}

echo -e "${BLUE}🚀 SeaweedFS RDMA Mount Helper${NC}"
echo "================================"
echo "Filer Address: $FILER_ADDR"
echo "RDMA Sidecar: $RDMA_SIDECAR_ADDR"
echo "Mount Point: $MOUNT_POINT"
echo "RDMA Enabled: $RDMA_ENABLED"
echo "RDMA Fallback: $RDMA_FALLBACK"
echo "Debug Mode: $DEBUG"
echo ""

# Function to wait for service
wait_for_service() {
    local name=$1
    local url=$2
    local max_attempts=30
    local attempt=1
    
    echo -e "${BLUE}⏳ Waiting for $name to be ready...${NC}"
    
    while [[ $attempt -le $max_attempts ]]; do
        if curl -s "$url" >/dev/null 2>&1; then
            echo -e "${GREEN}✅ $name is ready${NC}"
            return 0
        fi
        echo "   Attempt $attempt/$max_attempts..."
        sleep 2
        ((attempt++))
    done
    
    echo -e "${RED}❌ $name failed to be ready within $max_attempts attempts${NC}"
    return 1
}

# Function to check RDMA sidecar capabilities
check_rdma_capabilities() {
    echo -e "${BLUE}🔍 Checking RDMA capabilities...${NC}"
    
    local response
    if response=$(curl -s "http://$RDMA_SIDECAR_ADDR/stats" 2>/dev/null); then
        echo "RDMA Sidecar Stats:"
        echo "$response" | jq . 2>/dev/null || echo "$response"
        echo ""
        
        # Check if RDMA is actually enabled
        if echo "$response" | grep -q '"rdma_enabled":true'; then
            echo -e "${GREEN}✅ RDMA is enabled and ready${NC}"
            return 0
        else
            echo -e "${YELLOW}⚠️  RDMA sidecar is running but RDMA is not enabled${NC}"
            if [[ "$RDMA_FALLBACK" == "true" ]]; then
                echo -e "${YELLOW}   Will use HTTP fallback${NC}"
                return 0
            else
                return 1
            fi
        fi
    else
        echo -e "${RED}❌ Failed to get RDMA sidecar stats${NC}"
        if [[ "$RDMA_FALLBACK" == "true" ]]; then
            echo -e "${YELLOW}   Will use HTTP fallback${NC}"
            return 0
        else
            return 1
        fi
    fi
}

# Function to cleanup on exit
cleanup() {
    echo -e "\n${YELLOW}🧹 Cleaning up...${NC}"
    
    # Unmount if mounted
    if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        echo "📤 Unmounting $MOUNT_POINT..."
        fusermount3 -u "$MOUNT_POINT" 2>/dev/null || umount "$MOUNT_POINT" 2>/dev/null || true
        sleep 2
    fi
    
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

trap cleanup EXIT INT TERM

# Wait for required services
echo -e "${BLUE}🔄 Waiting for required services...${NC}"
wait_for_service "Filer" "http://$FILER_ADDR/"

if [[ "$RDMA_ENABLED" == "true" ]]; then
    wait_for_service "RDMA Sidecar" "http://$RDMA_SIDECAR_ADDR/health"
    check_rdma_capabilities
fi

# Create mount point if it doesn't exist
echo -e "${BLUE}📁 Preparing mount point...${NC}"
mkdir -p "$MOUNT_POINT"

# Check if already mounted
if mountpoint -q "$MOUNT_POINT"; then
    echo -e "${YELLOW}⚠️  $MOUNT_POINT is already mounted, unmounting first...${NC}"
    fusermount3 -u "$MOUNT_POINT" 2>/dev/null || umount "$MOUNT_POINT" 2>/dev/null || true
    sleep 2
fi

# Build mount command
MOUNT_CMD="/usr/local/bin/weed mount"
MOUNT_CMD="$MOUNT_CMD -filer=$FILER_ADDR"
MOUNT_CMD="$MOUNT_CMD -dir=$MOUNT_POINT"
MOUNT_CMD="$MOUNT_CMD -allowOthers=true"

# Add RDMA options if enabled
if [[ "$RDMA_ENABLED" == "true" ]]; then
    MOUNT_CMD="$MOUNT_CMD -rdma.enabled=true"
    MOUNT_CMD="$MOUNT_CMD -rdma.sidecar=$RDMA_SIDECAR_ADDR"
    MOUNT_CMD="$MOUNT_CMD -rdma.fallback=$RDMA_FALLBACK"
    MOUNT_CMD="$MOUNT_CMD -rdma.maxConcurrent=$RDMA_MAX_CONCURRENT"
    MOUNT_CMD="$MOUNT_CMD -rdma.timeoutMs=$RDMA_TIMEOUT_MS"
fi

# Add debug options if enabled
if [[ "$DEBUG" == "true" ]]; then
    MOUNT_CMD="$MOUNT_CMD -debug=true -v=2"
fi

echo -e "${BLUE}🗂️  Starting SeaweedFS mount...${NC}"
echo "Command: $MOUNT_CMD"
echo ""

# Execute mount command
exec $MOUNT_CMD
