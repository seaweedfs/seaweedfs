#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== SeaweedFS PostgreSQL Setup Validation ===${NC}"

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker not found. Please install Docker.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker found${NC}"

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}✗ Docker Compose not found. Please install Docker Compose.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker Compose found${NC}"

# Check if running from correct directory
if [[ ! -f "docker-compose.yml" ]]; then
    echo -e "${RED}✗ Must run from test/postgres directory${NC}"
    echo "  cd test/postgres && ./validate-setup.sh"
    exit 1
fi
echo -e "${GREEN}✓ Running from correct directory${NC}"

# Check required files
required_files=("docker-compose.yml" "producer.go" "client.go" "Dockerfile.producer" "Dockerfile.client" "run-tests.sh")
for file in "${required_files[@]}"; do
    if [[ ! -f "$file" ]]; then
        echo -e "${RED}✗ Missing required file: $file${NC}"
        exit 1
    fi
done
echo -e "${GREEN}✓ All required files present${NC}"

# Test Docker Compose syntax
echo -e "${YELLOW}Validating Docker Compose configuration...${NC}"
if docker-compose config > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Docker Compose configuration valid${NC}"
else
    echo -e "${RED}✗ Docker Compose configuration invalid${NC}"
    docker-compose config
    exit 1
fi

# Quick smoke test
echo -e "${YELLOW}Running smoke test...${NC}"

# Start services
echo "Starting services..."
docker-compose up -d seaweedfs postgres-server 2>/dev/null

# Wait a bit for services to start
sleep 15

# Check if services are running
seaweedfs_running=$(docker-compose ps seaweedfs | grep -c "Up")
postgres_running=$(docker-compose ps postgres-server | grep -c "Up")

if [[ $seaweedfs_running -eq 1 ]]; then
    echo -e "${GREEN}✓ SeaweedFS service is running${NC}"
else
    echo -e "${RED}✗ SeaweedFS service failed to start${NC}"
    docker-compose logs seaweedfs | tail -10
fi

if [[ $postgres_running -eq 1 ]]; then
    echo -e "${GREEN}✓ PostgreSQL server is running${NC}"
else
    echo -e "${RED}✗ PostgreSQL server failed to start${NC}"
    docker-compose logs postgres-server | tail -10
fi

# Test PostgreSQL connectivity
echo "Testing PostgreSQL connectivity..."
if timeout 10 docker run --rm --network "$(basename $(pwd))_seaweedfs-net" postgres:15-alpine \
    psql -h postgres-server -p 5432 -U seaweedfs -d default -c "SELECT version();" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ PostgreSQL connectivity test passed${NC}"
else
    echo -e "${RED}✗ PostgreSQL connectivity test failed${NC}"
fi

# Test SeaweedFS API
echo "Testing SeaweedFS API..."
if curl -s http://localhost:9333/cluster/status > /dev/null 2>&1; then
    echo -e "${GREEN}✓ SeaweedFS API accessible${NC}"
else
    echo -e "${RED}✗ SeaweedFS API not accessible${NC}"
fi

# Cleanup
echo -e "${YELLOW}Cleaning up...${NC}"
docker-compose down > /dev/null 2>&1

echo -e "${BLUE}=== Validation Summary ===${NC}"

if [[ $seaweedfs_running -eq 1 ]] && [[ $postgres_running -eq 1 ]]; then
    echo -e "${GREEN}✓ Setup validation PASSED${NC}"
    echo
    echo "Your setup is ready! You can now run:"
    echo "  ./run-tests.sh all          # Complete automated test"
    echo "  make all                    # Using Makefile"
    echo "  ./run-tests.sh start        # Manual step-by-step"
    echo
    echo "For interactive testing:"
    echo "  ./run-tests.sh psql         # Connect with psql"
    echo
    echo "Documentation:"
    echo "  cat README.md               # Full documentation"
    exit 0
else
    echo -e "${RED}✗ Setup validation FAILED${NC}"
    echo
    echo "Please check the logs above and ensure:"
    echo "  • Docker and Docker Compose are properly installed"
    echo "  • All required files are present"
    echo "  • No other services are using ports 5432, 9333, 8888"
    echo "  • Docker daemon is running"
    exit 1
fi
