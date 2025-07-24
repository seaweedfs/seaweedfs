#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🧪 SeaweedFS EC Worker Testing Environment${NC}"
echo -e "${BLUE}===========================================${NC}"

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ docker-compose is required but not installed${NC}"
    exit 1
fi

# Create necessary directories
echo -e "${YELLOW}📁 Creating required directories...${NC}"
mkdir -p monitor-data admin-config

# Make scripts executable
echo -e "${YELLOW}🔧 Making scripts executable...${NC}"
chmod +x *.sh

# Stop any existing containers
echo -e "${YELLOW}🛑 Stopping any existing containers...${NC}"
docker-compose -f docker-compose-ec-test.yml down -v 2>/dev/null || true

# Build and start the environment
echo -e "${GREEN}🚀 Starting SeaweedFS EC testing environment...${NC}"
echo -e "${BLUE}This will start:${NC}"
echo -e "  • 1 Master server (port 9333)"
echo -e "  • 6 Volume servers (ports 8080-8085) with 50MB volume limit"
echo -e "  • 1 Filer (port 8888)"
echo -e "  • 1 Admin server (port 9900)" 
echo -e "  • 3 EC Workers"
echo -e "  • 1 Load generator (continuous read/write)"
echo -e "  • 1 Monitor (port 9999)"
echo ""

docker-compose -f docker-compose-ec-test.yml up --build -d

echo -e "${GREEN}✅ Environment started successfully!${NC}"
echo ""
echo -e "${BLUE}📊 Monitoring URLs:${NC}"
echo -e "  • Master UI:     http://localhost:9333"
echo -e "  • Filer:         http://localhost:8888"
echo -e "  • Admin Server:  http://localhost:9900/status"
echo -e "  • Monitor:       http://localhost:9999/status"
echo ""
echo -e "${BLUE}📈 Volume Servers:${NC}"
echo -e "  • Volume1:       http://localhost:8080/status"
echo -e "  • Volume2:       http://localhost:8081/status"
echo -e "  • Volume3:       http://localhost:8082/status"
echo -e "  • Volume4:       http://localhost:8083/status"
echo -e "  • Volume5:       http://localhost:8084/status"
echo -e "  • Volume6:       http://localhost:8085/status"
echo ""

echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
sleep 10

# Check service health
echo -e "${BLUE}🔍 Checking service health...${NC}"

check_service() {
    local name=$1
    local url=$2
    
    if curl -s "$url" > /dev/null 2>&1; then
        echo -e "  ✅ $name: ${GREEN}Healthy${NC}"
        return 0
    else
        echo -e "  ❌ $name: ${RED}Not responding${NC}"
        return 1
    fi
}

check_service "Master" "http://localhost:9333/cluster/status"
check_service "Filer" "http://localhost:8888/"
check_service "Admin" "http://localhost:9900/health"
check_service "Monitor" "http://localhost:9999/health"

echo ""
echo -e "${GREEN}🎯 Test Environment is Ready!${NC}"
echo ""
echo -e "${BLUE}What's happening:${NC}"
echo -e "  1. 📝 Load generator continuously writes 1-5MB files at 10 files/sec"
echo -e "  2. 🗑️  Load generator deletes files at 2 files/sec"
echo -e "  3. 📊 Volumes fill up to 50MB limit and trigger EC conversion"
echo -e "  4. 🏭 Admin server detects volumes needing EC and assigns to workers"
echo -e "  5. ⚡ Workers perform comprehensive EC (copy→encode→distribute)"
echo -e "  6. 📈 Monitor tracks all activity and volume states"
echo ""
echo -e "${YELLOW}📋 Useful Commands:${NC}"
echo -e "  • View logs:           docker-compose -f docker-compose-ec-test.yml logs -f [service]"
echo -e "  • Check worker status: docker-compose -f docker-compose-ec-test.yml logs worker1"
echo -e "  • Stop environment:    docker-compose -f docker-compose-ec-test.yml down -v"
echo -e "  • Monitor logs:        docker-compose -f docker-compose-ec-test.yml logs -f monitor"
echo ""
echo -e "${GREEN}🔥 The test will run for 1 hour by default${NC}"
echo -e "${BLUE}Monitor progress at: http://localhost:9999/status${NC}" 