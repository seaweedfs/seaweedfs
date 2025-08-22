# SeaweedFS KMS Integration Testing Makefile

# Configuration
OPENBAO_ADDR ?= http://127.0.0.1:8200
OPENBAO_TOKEN ?= root-token-for-testing
SEAWEEDFS_S3_ENDPOINT ?= http://127.0.0.1:8333
TEST_TIMEOUT ?= 5m
DOCKER_COMPOSE ?= docker-compose

# Colors for output
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
NC := \033[0m # No Color

.PHONY: help setup test test-unit test-integration test-e2e clean logs status

help: ## Show this help message
	@echo "$(BLUE)SeaweedFS KMS Integration Testing$(NC)"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Set up test environment (OpenBao + SeaweedFS)
	@echo "$(YELLOW)Setting up test environment...$(NC)"
	@chmod +x setup_openbao.sh
	@$(DOCKER_COMPOSE) up -d openbao
	@sleep 5
	@echo "$(BLUE)Configuring OpenBao...$(NC)"
	@OPENBAO_ADDR=$(OPENBAO_ADDR) OPENBAO_TOKEN=$(OPENBAO_TOKEN) ./setup_openbao.sh
	@echo "$(GREEN)✅ Test environment ready!$(NC)"

test: setup test-unit test-integration ## Run all tests

test-unit: ## Run unit tests for KMS providers
	@echo "$(YELLOW)Running KMS provider unit tests...$(NC)"
	@cd ../../ && go test -v -timeout=$(TEST_TIMEOUT) ./weed/kms/...

test-integration: ## Run integration tests with OpenBao
	@echo "$(YELLOW)Running KMS integration tests...$(NC)"
	@cd ../../ && go test -v -timeout=$(TEST_TIMEOUT) ./test/kms/...

test-benchmark: ## Run performance benchmarks
	@echo "$(YELLOW)Running KMS performance benchmarks...$(NC)"
	@cd ../../ && go test -v -timeout=$(TEST_TIMEOUT) -bench=. ./test/kms/...

test-e2e: setup-seaweedfs ## Run end-to-end tests with SeaweedFS + KMS
	@echo "$(YELLOW)Running end-to-end KMS tests...$(NC)"
	@sleep 10  # Wait for SeaweedFS to be ready
	@./test_s3_kms.sh

setup-seaweedfs: ## Start complete SeaweedFS cluster with KMS
	@echo "$(YELLOW)Starting SeaweedFS cluster...$(NC)"
	@$(DOCKER_COMPOSE) up -d
	@echo "$(BLUE)Waiting for services to be ready...$(NC)"
	@./wait_for_services.sh

test-aws-compat: ## Test AWS KMS API compatibility
	@echo "$(YELLOW)Testing AWS KMS compatibility...$(NC)"
	@cd ../../ && go test -v -timeout=$(TEST_TIMEOUT) -run TestAWSKMSCompat ./test/kms/...

clean: ## Clean up test environment
	@echo "$(YELLOW)Cleaning up test environment...$(NC)"
	@$(DOCKER_COMPOSE) down -v --remove-orphans
	@docker system prune -f
	@echo "$(GREEN)✅ Environment cleaned up!$(NC)"

logs: ## Show logs from all services
	@$(DOCKER_COMPOSE) logs --tail=50 -f

logs-openbao: ## Show OpenBao logs
	@$(DOCKER_COMPOSE) logs --tail=100 -f openbao

logs-seaweedfs: ## Show SeaweedFS logs
	@$(DOCKER_COMPOSE) logs --tail=100 -f seaweedfs-filer seaweedfs-master seaweedfs-volume

status: ## Show status of all services
	@echo "$(BLUE)Service Status:$(NC)"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "$(BLUE)OpenBao Status:$(NC)"
	@curl -s $(OPENBAO_ADDR)/v1/sys/health | jq '.' || echo "OpenBao not accessible"
	@echo ""
	@echo "$(BLUE)SeaweedFS S3 Status:$(NC)"
	@curl -s $(SEAWEEDFS_S3_ENDPOINT) || echo "SeaweedFS S3 not accessible"

debug: ## Debug test environment
	@echo "$(BLUE)Debug Information:$(NC)"
	@echo "OpenBao Address: $(OPENBAO_ADDR)"
	@echo "SeaweedFS S3 Endpoint: $(SEAWEEDFS_S3_ENDPOINT)"
	@echo "Docker Compose Status:"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "Network connectivity:"
	@docker network ls | grep seaweedfs || echo "No SeaweedFS network found"
	@echo ""
	@echo "OpenBao health:"
	@curl -v $(OPENBAO_ADDR)/v1/sys/health 2>&1 || true

# Development targets
dev-openbao: ## Start only OpenBao for development
	@$(DOCKER_COMPOSE) up -d openbao
	@sleep 5
	@OPENBAO_ADDR=$(OPENBAO_ADDR) OPENBAO_TOKEN=$(OPENBAO_TOKEN) ./setup_openbao.sh

dev-test: dev-openbao ## Quick test with just OpenBao
	@cd ../../ && go test -v -timeout=30s -run TestOpenBaoKMSProvider_Integration ./test/kms/

# Utility targets
install-deps: ## Install required dependencies
	@echo "$(YELLOW)Installing test dependencies...$(NC)"
	@which docker > /dev/null || (echo "$(RED)Docker not found$(NC)" && exit 1)
	@which docker-compose > /dev/null || (echo "$(RED)Docker Compose not found$(NC)" && exit 1)
	@which jq > /dev/null || (echo "$(RED)jq not found - please install jq$(NC)" && exit 1)
	@which curl > /dev/null || (echo "$(RED)curl not found$(NC)" && exit 1)
	@echo "$(GREEN)✅ All dependencies available$(NC)"

check-env: ## Check test environment setup
	@echo "$(BLUE)Environment Check:$(NC)"
	@echo "OPENBAO_ADDR: $(OPENBAO_ADDR)"
	@echo "OPENBAO_TOKEN: $(OPENBAO_TOKEN)" 
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
	@make setup-seaweedfs
	@make test-e2e
	@make clean
