#!/bin/bash

# Docker Test Helper - Simplified commands for running integration tests

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_usage() {
    echo -e "${BLUE}SeaweedFS RDMA Docker Integration Test Helper${NC}"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  start     - Start all services"
    echo "  test      - Run integration tests"
    echo "  stop      - Stop all services"
    echo "  clean     - Stop services and clean up volumes"
    echo "  logs      - Show logs from all services"
    echo "  status    - Show status of all services"
    echo "  shell     - Open shell in test client container"
    echo ""
    echo "Examples:"
    echo "  $0 start          # Start all services"
    echo "  $0 test           # Run full integration test suite"
    echo "  $0 logs rdma-engine  # Show logs from RDMA engine"
    echo "  $0 shell          # Interactive testing shell"
}

start_services() {
    echo -e "${GREEN}🚀 Starting SeaweedFS RDMA integration services...${NC}"
    docker-compose up -d seaweedfs-master seaweedfs-volume rdma-engine rdma-sidecar
    
    echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
    sleep 10
    
    echo -e "${GREEN}✅ Services started. Checking health...${NC}"
    docker-compose ps
}

run_tests() {
    echo -e "${GREEN}🧪 Running integration tests...${NC}"
    
    # Make sure services are running
    docker-compose up -d seaweedfs-master seaweedfs-volume rdma-engine rdma-sidecar
    
    # Wait for services to be ready
    echo -e "${YELLOW}⏳ Waiting for services to be ready...${NC}"
    sleep 15
    
    # Run the integration tests
    docker-compose run --rm integration-tests
}

stop_services() {
    echo -e "${YELLOW}🛑 Stopping services...${NC}"
    docker-compose down
    echo -e "${GREEN}✅ Services stopped${NC}"
}

clean_all() {
    echo -e "${YELLOW}🧹 Cleaning up services and volumes...${NC}"
    docker-compose down -v --remove-orphans
    echo -e "${GREEN}✅ Cleanup complete${NC}"
}

show_logs() {
    local service=${1:-}
    if [ -n "$service" ]; then
        echo -e "${BLUE}📋 Showing logs for $service...${NC}"
        docker-compose logs -f "$service"
    else
        echo -e "${BLUE}📋 Showing logs for all services...${NC}"
        docker-compose logs -f
    fi
}

show_status() {
    echo -e "${BLUE}📊 Service Status:${NC}"
    docker-compose ps
    
    echo -e "\n${BLUE}📡 Health Checks:${NC}"
    
    # Check SeaweedFS Master
    if curl -s http://localhost:9333/cluster/status >/dev/null 2>&1; then
        echo -e "  ${GREEN}✅ SeaweedFS Master: Healthy${NC}"
    else
        echo -e "  ${RED}❌ SeaweedFS Master: Unhealthy${NC}"
    fi
    
    # Check SeaweedFS Volume
    if curl -s http://localhost:8080/status >/dev/null 2>&1; then
        echo -e "  ${GREEN}✅ SeaweedFS Volume: Healthy${NC}"
    else
        echo -e "  ${RED}❌ SeaweedFS Volume: Unhealthy${NC}"
    fi
    
    # Check RDMA Sidecar
    if curl -s http://localhost:8081/health >/dev/null 2>&1; then
        echo -e "  ${GREEN}✅ RDMA Sidecar: Healthy${NC}"
    else
        echo -e "  ${RED}❌ RDMA Sidecar: Unhealthy${NC}"
    fi
}

open_shell() {
    echo -e "${GREEN}🐚 Opening interactive shell in test client...${NC}"
    echo -e "${YELLOW}Use './test-rdma --help' for RDMA testing commands${NC}"
    echo -e "${YELLOW}Use 'curl http://rdma-sidecar:8081/health' to test sidecar${NC}"
    
    docker-compose run --rm test-client /bin/bash
}

# Main command handling
case "${1:-}" in
    start)
        start_services
        ;;
    test)
        run_tests
        ;;
    stop)
        stop_services
        ;;
    clean)
        clean_all
        ;;
    logs)
        show_logs "${2:-}"
        ;;
    status)
        show_status
        ;;
    shell)
        open_shell
        ;;
    -h|--help|help)
        print_usage
        ;;
    "")
        print_usage
        exit 1
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $1${NC}"
        print_usage
        exit 1
        ;;
esac
