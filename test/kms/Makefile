# SeaweedFS KMS Integration Testing Makefile

# Configuration
OPENBAO_ADDR ?= http://127.0.0.1:8200
OPENBAO_TOKEN ?= root-token-for-testing
SEAWEEDFS_S3_ENDPOINT ?= http://127.0.0.1:8333
TEST_TIMEOUT ?= 5m
DOCKER_COMPOSE ?= docker-compose

# CI configuration (build weed from source, run natively)
SEAWEEDFS_BINARY ?= weed
S3_PORT ?= 8333
ACCESS_KEY ?= some_access_key1
SECRET_KEY ?= some_secret_key1
TEST_DIR := $(shell pwd)
SEAWEEDFS_ROOT := $(shell cd ../.. && pwd)

# Colors for output
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
NC := \033[0m # No Color

.PHONY: help setup test test-unit test-integration test-e2e clean logs status \
       build-weed check-binary start-openbao-ci stop-openbao-ci \
       start-seaweedfs-ci stop-seaweedfs-ci clean-ci \
       test-provider-ci test-s3-kms-ci

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

# CI targets (docker-compose based)
ci-test: ## Run tests in CI environment (docker-compose)
	@echo "$(YELLOW)Running CI tests...$(NC)"
	@make setup
	@make test-unit
	@make test-integration
	@make clean

ci-e2e: ## Run end-to-end tests in CI (docker-compose)
	@echo "$(YELLOW)Running CI end-to-end tests...$(NC)"
	@make setup-seaweedfs
	@make test-e2e
	@make clean

####################################################
# GitHub Actions CI targets (build weed from source)
####################################################

build-weed: ## Build SeaweedFS binary from source
	@echo "Building SeaweedFS binary..."
	@cd $(SEAWEEDFS_ROOT)/weed && go install -buildvcs=false
	@echo "$(GREEN)SeaweedFS binary built successfully$(NC)"

check-binary:
	@if ! command -v $(SEAWEEDFS_BINARY) > /dev/null 2>&1; then \
		echo "$(RED)Error: SeaweedFS binary '$(SEAWEEDFS_BINARY)' not found in PATH$(NC)"; \
		exit 1; \
	fi

start-openbao-ci: ## Start OpenBao via Docker for CI
	@echo "Starting OpenBao for CI..."
	@docker rm -f openbao-ci 2>/dev/null || true
	@docker run -d --name openbao-ci \
		-p 8200:8200 \
		-e BAO_DEV_ROOT_TOKEN_ID=$(OPENBAO_TOKEN) \
		-e BAO_DEV_LISTEN_ADDRESS=0.0.0.0:8200 \
		ghcr.io/openbao/openbao:latest \
		bao server -dev -dev-root-token-id=$(OPENBAO_TOKEN) -dev-listen-address=0.0.0.0:8200
	@echo "Waiting for OpenBao to be ready..."
	@for i in $$(seq 1 30); do \
		if curl -s $(OPENBAO_ADDR)/v1/sys/health > /dev/null 2>&1; then \
			echo "$(GREEN)OpenBao is ready$(NC)"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "$(RED)Timeout waiting for OpenBao$(NC)"; \
			docker logs openbao-ci 2>&1 | tail -20; \
			exit 1; \
		fi; \
		sleep 2; \
	done
	@chmod +x setup_openbao.sh
	@OPENBAO_ADDR=$(OPENBAO_ADDR) OPENBAO_TOKEN=$(OPENBAO_TOKEN) ./setup_openbao.sh

stop-openbao-ci: ## Stop OpenBao CI container
	@echo "Stopping OpenBao..."
	@docker stop openbao-ci 2>/dev/null || true
	@docker rm openbao-ci 2>/dev/null || true

start-seaweedfs-ci: check-binary ## Start weed mini with OpenBao KMS config
	@echo "Starting SeaweedFS with OpenBao KMS..."
	@mkdir -p /tmp/seaweedfs-test-kms
	@rm -f /tmp/seaweedfs-kms-*.log || true
	@sed -e 's/ACCESS_KEY_PLACEHOLDER/$(ACCESS_KEY)/g' \
	     -e 's/SECRET_KEY_PLACEHOLDER/$(SECRET_KEY)/g' \
	     -e 's|OPENBAO_ADDR_PLACEHOLDER|$(OPENBAO_ADDR)|g' \
	     -e 's/OPENBAO_TOKEN_PLACEHOLDER/$(OPENBAO_TOKEN)/g' \
	     s3-config-openbao-template.json > /tmp/seaweedfs-kms-s3.json
	@# Start weed mini from the data dir to avoid picking up test/kms/filer.toml
	@# (filer.toml is read from "." first, and the one here has Docker-only paths)
	@cd /tmp/seaweedfs-test-kms && \
		AWS_ACCESS_KEY_ID=$(ACCESS_KEY) AWS_SECRET_ACCESS_KEY=$(SECRET_KEY) GLOG_v=4 $(SEAWEEDFS_BINARY) mini \
		-dir=/tmp/seaweedfs-test-kms \
		-s3.port=$(S3_PORT) \
		-s3.config=/tmp/seaweedfs-kms-s3.json \
		-ip=127.0.0.1 \
		> /tmp/seaweedfs-kms-mini.log 2>&1 & echo $$! > /tmp/weed-kms-mini.pid
	@echo "Checking S3 service is ready..."
	@for i in $$(seq 1 30); do \
		if curl -s http://127.0.0.1:$(S3_PORT) > /dev/null 2>&1; then \
			echo "$(GREEN)S3 service is ready$(NC)"; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "$(RED)Timeout waiting for S3 service$(NC)"; \
			echo "=== Last 50 lines of server log ==="; \
			tail -50 /tmp/seaweedfs-kms-mini.log 2>/dev/null || echo "No server log found"; \
			exit 1; \
		fi; \
		sleep 1; \
	done

stop-seaweedfs-ci: ## Stop weed mini (CI-safe)
	@echo "Stopping SeaweedFS..."
	@if [ -f /tmp/weed-kms-mini.pid ]; then \
		kill $$(cat /tmp/weed-kms-mini.pid) 2>/dev/null || true; \
		rm -f /tmp/weed-kms-mini.pid; \
	fi
	@if command -v lsof >/dev/null 2>&1; then \
		lsof -ti :$(S3_PORT) 2>/dev/null | head -5 | while read pid; do kill -TERM $$pid 2>/dev/null || true; done; \
	fi
	@sleep 2

clean-ci: ## Clean up CI test artifacts
	@rm -rf /tmp/seaweedfs-test-kms
	@rm -f /tmp/seaweedfs-kms-*.log /tmp/seaweedfs-kms-s3.json /tmp/weed-kms-mini.pid

# Run KMS provider Go integration tests (GitHub Actions compatible)
test-provider-ci: build-weed ## Run KMS provider tests with OpenBao in CI
	@echo "$(YELLOW)Starting KMS provider integration tests...$(NC)"
	@if $(MAKE) start-openbao-ci > /tmp/openbao-ci-setup.log 2>&1; then \
		echo "$(GREEN)OpenBao started and configured$(NC)"; \
		trap '$(MAKE) -C $(TEST_DIR) stop-openbao-ci || true' EXIT; \
		cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) ./test/kms/... || exit 1; \
		echo "$(GREEN)KMS provider tests completed successfully$(NC)"; \
	else \
		echo "$(RED)Failed to start OpenBao$(NC)"; \
		cat /tmp/openbao-ci-setup.log 2>/dev/null || true; \
		exit 1; \
	fi

# Run S3 KMS end-to-end tests (GitHub Actions compatible)
test-s3-kms-ci: build-weed ## Run S3 KMS e2e tests with weed mini + OpenBao in CI
	@echo "$(YELLOW)Starting S3 KMS end-to-end tests...$(NC)"
	@if $(MAKE) start-openbao-ci > /tmp/openbao-ci-setup.log 2>&1; then \
		echo "$(GREEN)OpenBao started and configured$(NC)"; \
		trap 'docker logs openbao-ci > /tmp/openbao-ci-container.log 2>&1 || true; $(MAKE) -C $(TEST_DIR) stop-seaweedfs-ci || true; $(MAKE) -C $(TEST_DIR) stop-openbao-ci || true; $(MAKE) -C $(TEST_DIR) clean-ci || true' EXIT; \
		if $(MAKE) start-seaweedfs-ci > /tmp/weed-kms-ci-setup.log 2>&1; then \
			echo "$(GREEN)SeaweedFS started with OpenBao KMS$(NC)"; \
			sleep 3; \
			chmod +x test_s3_kms.sh; \
			SEAWEEDFS_S3_ENDPOINT=http://127.0.0.1:$(S3_PORT) ACCESS_KEY=$(ACCESS_KEY) SECRET_KEY=$(SECRET_KEY) ./test_s3_kms.sh || exit 1; \
			echo "$(GREEN)S3 KMS e2e tests completed successfully$(NC)"; \
		else \
			echo "$(RED)Failed to start SeaweedFS$(NC)"; \
			cat /tmp/weed-kms-ci-setup.log 2>/dev/null || true; \
			exit 1; \
		fi; \
	else \
		echo "$(RED)Failed to start OpenBao$(NC)"; \
		cat /tmp/openbao-ci-setup.log 2>/dev/null || true; \
		exit 1; \
	fi
