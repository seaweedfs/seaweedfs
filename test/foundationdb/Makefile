# SeaweedFS FoundationDB Integration Testing Makefile

# Configuration
FDB_CLUSTER_FILE ?= /tmp/fdb.cluster
SEAWEEDFS_S3_ENDPOINT ?= http://127.0.0.1:8333
TEST_TIMEOUT ?= 5m
DOCKER_COMPOSE ?= docker-compose
DOCKER_COMPOSE_ARM64 ?= docker-compose -f docker-compose.arm64.yml

# Colors for output
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
NC := \033[0m # No Color

.PHONY: help setup test test-unit test-integration test-e2e clean logs status \
         setup-arm64 test-arm64 setup-emulated test-emulated clean-arm64

help: ## Show this help message
	@echo "$(BLUE)SeaweedFS FoundationDB Integration Testing$(NC)"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_][a-zA-Z0-9_-]*:.*?## / {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Set up test environment (FoundationDB + SeaweedFS)
	@echo "$(YELLOW)Setting up FoundationDB cluster and SeaweedFS...$(NC)"
	@$(DOCKER_COMPOSE) up -d fdb1 fdb2 fdb3
	@echo "$(BLUE)Waiting for FoundationDB cluster to initialize...$(NC)"
	@sleep 15
	@$(DOCKER_COMPOSE) up -d fdb-init
	@sleep 10
	@echo "$(BLUE)Starting SeaweedFS with FoundationDB filer...$(NC)"
	@$(DOCKER_COMPOSE) up -d seaweedfs
	@echo "$(GREEN)✅ Test environment ready!$(NC)"
	@echo "$(BLUE)Checking cluster status...$(NC)"
	@make status

test: setup test-unit test-integration ## Run all tests

test-unit: ## Run unit tests for FoundationDB filer store
	@echo "$(YELLOW)Running FoundationDB filer store unit tests...$(NC)"
	@cd ../../ && go test -v -timeout=$(TEST_TIMEOUT) -tags foundationdb ./weed/filer/foundationdb/...

test-integration: ## Run integration tests with FoundationDB
	@echo "$(YELLOW)Running FoundationDB integration tests...$(NC)"
	@cd ../../ && go test -v -timeout=$(TEST_TIMEOUT) -tags foundationdb ./test/foundationdb/...

test-benchmark: ## Run performance benchmarks
	@echo "$(YELLOW)Running FoundationDB performance benchmarks...$(NC)"
	@cd ../../ && go test -v -timeout=$(TEST_TIMEOUT) -tags foundationdb -bench=. ./test/foundationdb/...

# ARM64 specific targets (Apple Silicon / M1/M2/M3 Macs)
setup-arm64: ## Set up ARM64-native FoundationDB cluster (builds from source)
	@echo "$(YELLOW)Setting up ARM64-native FoundationDB cluster...$(NC)"
	@echo "$(BLUE)Note: This will build FoundationDB from source - may take 10-15 minutes$(NC)"
	@$(DOCKER_COMPOSE_ARM64) build
	@$(DOCKER_COMPOSE_ARM64) up -d fdb1 fdb2 fdb3
	@echo "$(BLUE)Waiting for FoundationDB cluster to initialize...$(NC)"
	@sleep 20
	@$(DOCKER_COMPOSE_ARM64) up -d fdb-init
	@sleep 15
	@echo "$(BLUE)Starting SeaweedFS with FoundationDB filer...$(NC)"
	@$(DOCKER_COMPOSE_ARM64) up -d seaweedfs
	@echo "$(GREEN)✅ ARM64 test environment ready!$(NC)"

test-arm64: setup-arm64 test-unit test-integration ## Run all tests with ARM64-native FoundationDB

setup-emulated: ## Set up FoundationDB cluster with x86 emulation on ARM64
	@echo "$(YELLOW)Setting up FoundationDB cluster with x86 emulation...$(NC)"
	@echo "$(BLUE)Note: Using Docker platform emulation - may be slower$(NC)"
	@DOCKER_DEFAULT_PLATFORM=linux/amd64 $(DOCKER_COMPOSE) up -d fdb1 fdb2 fdb3
	@echo "$(BLUE)Waiting for FoundationDB cluster to initialize...$(NC)"
	@sleep 15
	@DOCKER_DEFAULT_PLATFORM=linux/amd64 $(DOCKER_COMPOSE) up -d fdb-init
	@sleep 10
	@echo "$(BLUE)Starting SeaweedFS with FoundationDB filer...$(NC)"
	@$(DOCKER_COMPOSE) up -d seaweedfs
	@echo "$(GREEN)✅ Emulated test environment ready!$(NC)"

test-emulated: setup-emulated test-unit test-integration ## Run all tests with x86 emulation

clean-arm64: ## Clean up ARM64-specific containers and volumes
	@echo "$(YELLOW)Cleaning up ARM64 test environment...$(NC)"
	@$(DOCKER_COMPOSE_ARM64) down -v --remove-orphans 2>/dev/null || true
	@echo "$(GREEN)✅ ARM64 environment cleaned up!$(NC)"

test-e2e: setup-complete ## Run end-to-end tests with SeaweedFS + FoundationDB
	@echo "$(YELLOW)Running end-to-end FoundationDB tests...$(NC)"
	@sleep 10  # Wait for SeaweedFS to be ready
	@./test_fdb_s3.sh

setup-complete: ## Start complete environment and wait for readiness
	@echo "$(YELLOW)Starting complete environment...$(NC)"
	@$(DOCKER_COMPOSE) up -d
	@echo "$(BLUE)Waiting for all services to be ready...$(NC)"
	@./wait_for_services.sh

test-crud: ## Test basic CRUD operations
	@echo "$(YELLOW)Testing CRUD operations...$(NC)"
	@cd ../../ && go test -v -timeout=$(TEST_TIMEOUT) -tags foundationdb -run TestFoundationDBCRUD ./test/foundationdb/

test-concurrent: ## Test concurrent operations
	@echo "$(YELLOW)Testing concurrent operations...$(NC)"
	@cd ../../ && go test -v -timeout=$(TEST_TIMEOUT) -tags foundationdb -run TestFoundationDBConcurrent ./test/foundationdb/

clean: ## Clean up test environment (standard + ARM64)
	@echo "$(YELLOW)Cleaning up test environment...$(NC)"
	@$(DOCKER_COMPOSE) down -v --remove-orphans 2>/dev/null || true
	@$(DOCKER_COMPOSE_ARM64) down -v --remove-orphans 2>/dev/null || true
	@echo "$(GREEN)✅ Environment cleaned up!$(NC)"

logs: ## Show logs from all services
	@$(DOCKER_COMPOSE) logs --tail=50 -f

logs-fdb: ## Show FoundationDB logs
	@$(DOCKER_COMPOSE) logs --tail=100 -f fdb1 fdb2 fdb3 fdb-init

logs-seaweedfs: ## Show SeaweedFS logs
	@$(DOCKER_COMPOSE) logs --tail=100 -f seaweedfs

status: ## Show status of all services
	@echo "$(BLUE)Service Status:$(NC)"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "$(BLUE)FoundationDB Cluster Status:$(NC)"
	@$(DOCKER_COMPOSE) exec fdb-init fdbcli --exec 'status' || echo "FoundationDB not accessible"
	@echo ""
	@echo "$(BLUE)SeaweedFS S3 Status:$(NC)"
	@curl -s $(SEAWEEDFS_S3_ENDPOINT) || echo "SeaweedFS S3 not accessible"

debug: ## Debug test environment
	@echo "$(BLUE)Debug Information:$(NC)"
	@echo "FoundationDB Cluster File: $(FDB_CLUSTER_FILE)"
	@echo "SeaweedFS S3 Endpoint: $(SEAWEEDFS_S3_ENDPOINT)"
	@echo "Docker Compose Status:"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "Network connectivity:"
	@docker network ls | grep foundationdb || echo "No FoundationDB network found"
	@echo ""
	@echo "FoundationDB cluster file:"
	@$(DOCKER_COMPOSE) exec fdb1 cat /var/fdb/config/fdb.cluster || echo "Cannot read cluster file"

# Development targets
dev-fdb: ## Start only FoundationDB cluster for development
	@$(DOCKER_COMPOSE) up -d fdb1 fdb2 fdb3 fdb-init
	@sleep 15

dev-test: dev-fdb ## Quick test with just FoundationDB
	@cd ../../ && go test -v -timeout=30s -tags foundationdb -run TestFoundationDBStore_Initialize ./weed/filer/foundationdb/

# Utility targets
install-deps: ## Install required dependencies
	@echo "$(YELLOW)Installing test dependencies...$(NC)"
	@which docker > /dev/null || (echo "$(RED)Docker not found$(NC)" && exit 1)
	@which docker-compose > /dev/null || (echo "$(RED)Docker Compose not found$(NC)" && exit 1)
	@which curl > /dev/null || (echo "$(RED)curl not found$(NC)" && exit 1)
	@echo "$(GREEN)✅ All dependencies available$(NC)"

check-env: ## Check test environment setup
	@echo "$(BLUE)Environment Check:$(NC)"
	@echo "FDB_CLUSTER_FILE: $(FDB_CLUSTER_FILE)"
	@echo "SEAWEEDFS_S3_ENDPOINT: $(SEAWEEDFS_S3_ENDPOINT)"
	@echo "TEST_TIMEOUT: $(TEST_TIMEOUT)"
	@make install-deps

# CI targets
ci-test: ## Run tests in CI environment
	@echo "$(YELLOW)Running CI tests...$(NC)"
	@make setup
	@make test-unit
	@make test-integration
	@make clean

ci-e2e: ## Run end-to-end tests in CI
	@echo "$(YELLOW)Running CI end-to-end tests...$(NC)"
	@make setup-complete
	@make test-e2e
	@make clean

# Container build targets
build-container: ## Build SeaweedFS with FoundationDB in container
	@echo "$(YELLOW)Building SeaweedFS with FoundationDB in container...$(NC)"
	@docker-compose -f docker-compose.build.yml build seaweedfs-fdb-builder
	@echo "$(GREEN)✅ Container build complete!$(NC)"

test-container: build-container ## Run containerized FoundationDB integration test
	@echo "$(YELLOW)Running containerized FoundationDB integration test...$(NC)"
	@docker-compose -f docker-compose.build.yml up --build --abort-on-container-exit
	@echo "$(GREEN)🎉 Containerized integration test complete!$(NC)"

extract-binary: build-container ## Extract built SeaweedFS binary from container
	@echo "$(YELLOW)Extracting SeaweedFS binary from container...$(NC)"
	@docker run --rm -v $(PWD)/bin:/output seaweedfs:foundationdb sh -c "cp /usr/local/bin/weed /output/weed-foundationdb && echo '✅ Binary extracted to ./bin/weed-foundationdb'"
	@mkdir -p bin
	@echo "$(GREEN)✅ Binary available at ./bin/weed-foundationdb$(NC)"

clean-container: ## Clean up container builds
	@echo "$(YELLOW)Cleaning up container builds...$(NC)"
	@docker-compose -f docker-compose.build.yml down -v --remove-orphans || true
	@docker rmi seaweedfs:foundationdb 2>/dev/null || true
	@echo "$(GREEN)✅ Container cleanup complete!$(NC)"

# Simple test environment targets
test-simple: ## Run tests with simplified Docker environment  
	@echo "$(YELLOW)Running simplified FoundationDB integration tests...$(NC)"
	@docker-compose -f docker-compose.simple.yml up --build --abort-on-container-exit
	@echo "$(GREEN)🎉 Simple integration tests complete!$(NC)"

test-mock: ## Run mock tests (no FoundationDB required)
	@echo "$(YELLOW)Running mock integration tests...$(NC)"
	@go test -v ./validation_test.go ./mock_integration_test.go
	@echo "$(GREEN)✅ Mock tests completed!$(NC)"

clean-simple: ## Clean up simple test environment
	@echo "$(YELLOW)Cleaning up simple test environment...$(NC)"
	@docker-compose -f docker-compose.simple.yml down -v --remove-orphans || true
	@echo "$(GREEN)✅ Simple environment cleaned up!$(NC)"

# Combined test target - guaranteed to work
test-reliable: test-mock ## Run all tests that are guaranteed to work
	@echo "$(GREEN)🎉 All reliable tests completed successfully!$(NC)"
