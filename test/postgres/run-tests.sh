#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== SeaweedFS PostgreSQL Test Setup ===${NC}"

# Function to get the correct docker compose command
get_docker_compose_cmd() {
    if command -v docker &> /dev/null && docker compose version &> /dev/null 2>&1; then
        echo "docker compose"
    elif command -v docker-compose &> /dev/null; then
        echo "docker-compose"
    else
        echo -e "${RED}x Neither 'docker compose' nor 'docker-compose' is available${NC}"
        exit 1
    fi
}

# Get the docker compose command to use
DOCKER_COMPOSE_CMD=$(get_docker_compose_cmd)
echo -e "${BLUE}Using: ${DOCKER_COMPOSE_CMD}${NC}"

# Function to wait for service
wait_for_service() {
    local service=$1
    local max_wait=$2
    local count=0
    
    echo -e "${YELLOW}Waiting for $service to be ready...${NC}"
    while [ $count -lt $max_wait ]; do
        if $DOCKER_COMPOSE_CMD ps $service | grep -q "healthy\|Up"; then
            echo -e "${GREEN}- $service is ready${NC}"
            return 0
        fi
        sleep 2
        count=$((count + 1))
        echo -n "."
    done
    
    echo -e "${RED}x Timeout waiting for $service${NC}"
    return 1
}

# Function to show logs
show_logs() {
    local service=$1
    echo -e "${BLUE}=== $service logs ===${NC}"
    $DOCKER_COMPOSE_CMD logs --tail=20 $service
    echo
}

# Parse command line arguments
case "$1" in
    "start")
        echo -e "${YELLOW}Starting SeaweedFS cluster and PostgreSQL server...${NC}"
        $DOCKER_COMPOSE_CMD up -d seaweedfs postgres-server
        
        wait_for_service "seaweedfs" 30
        wait_for_service "postgres-server" 15
        
        echo -e "${GREEN}- SeaweedFS and PostgreSQL server are running${NC}"
        echo
        echo "You can now:"
        echo "  • Run data producer: $0 produce"
        echo "  • Run test client: $0 test"
        echo "  • Connect with psql: $0 psql"
        echo "  • View logs: $0 logs [service]"
        echo "  • Stop services: $0 stop"
        ;;
        
    "produce")
        echo -e "${YELLOW}Creating MQ test data...${NC}"
        $DOCKER_COMPOSE_CMD up --build mq-producer
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}- Test data created successfully${NC}"
            echo
            echo "You can now run: $0 test"
        else
            echo -e "${RED}x Data production failed${NC}"
            show_logs "mq-producer"
        fi
        ;;
        
    "test")
        echo -e "${YELLOW}Running PostgreSQL client tests...${NC}"
        $DOCKER_COMPOSE_CMD up --build postgres-client
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}- Client tests completed${NC}"
        else
            echo -e "${RED}x Client tests failed${NC}"
            show_logs "postgres-client"
        fi
        ;;
        
    "psql")
        echo -e "${YELLOW}Connecting to PostgreSQL with psql...${NC}"
        $DOCKER_COMPOSE_CMD run --rm psql-cli psql -h postgres-server -p 5432 -U seaweedfs -d default
        ;;
        
    "logs")
        service=${2:-"seaweedfs"}
        show_logs "$service"
        ;;
        
    "status")
        echo -e "${BLUE}=== Service Status ===${NC}"
        $DOCKER_COMPOSE_CMD ps
        ;;
        
    "stop")
        echo -e "${YELLOW}Stopping all services...${NC}"
        $DOCKER_COMPOSE_CMD down
        echo -e "${GREEN}- All services stopped${NC}"
        ;;
        
    "clean")
        echo -e "${YELLOW}Cleaning up everything (including data)...${NC}"
        $DOCKER_COMPOSE_CMD down -v
        docker system prune -f
        echo -e "${GREEN}- Cleanup completed${NC}"
        ;;
        
    "all")
        echo -e "${YELLOW}Running complete test suite...${NC}"
        
        # Start services (wait_for_service ensures they're ready)
        $0 start
        
        # Create data ($DOCKER_COMPOSE_CMD up is synchronous)
        $0 produce
        
        # Run tests
        $0 test
        
        echo -e "${GREEN}- Complete test suite finished${NC}"
        ;;
        
    *)
        echo "Usage: $0 {start|produce|test|psql|logs|status|stop|clean|all}"
        echo
        echo "Commands:"
        echo "  start     - Start SeaweedFS and PostgreSQL server"
        echo "  produce   - Create MQ test data (run after start)"
        echo "  test      - Run PostgreSQL client tests (run after produce)"
        echo "  psql      - Connect with psql CLI"
        echo "  logs      - Show service logs (optionally specify service name)"
        echo "  status    - Show service status"
        echo "  stop      - Stop all services"
        echo "  clean     - Stop and remove all data"
        echo "  all       - Run complete test suite (start -> produce -> test)"
        echo
        echo "Example workflow:"
        echo "  $0 all                # Complete automated test"
        echo "  $0 start              # Manual step-by-step"
        echo "  $0 produce"
        echo "  $0 test"
        echo "  $0 psql               # Interactive testing"
        exit 1
        ;;
esac
