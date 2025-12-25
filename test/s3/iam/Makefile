# SeaweedFS S3 IAM Integration Tests Makefile

.PHONY: all test clean setup start-services stop-services wait-for-services help test-with-server

# Default target
all: test

# Test configuration
WEED_BINARY ?= $(shell go env GOPATH)/bin/weed
LOG_LEVEL ?= 2
S3_PORT ?= 8333
FILER_PORT ?= 8888
MASTER_PORT ?= 9333
VOLUME_PORT ?= 8081
TEST_TIMEOUT ?= 30m

# Service PIDs
MASTER_PID_FILE = /tmp/weed-master.pid
VOLUME_PID_FILE = /tmp/weed-volume.pid  
FILER_PID_FILE = /tmp/weed-filer.pid
S3_PID_FILE = /tmp/weed-s3.pid

help: ## Show this help message
	@echo "SeaweedFS S3 IAM Integration Tests"
	@echo ""
	@echo "Usage:"
	@echo "  make [target]"
	@echo ""
	@echo "Standard Targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-25s %s\n", $$1, $$2}' $(MAKEFILE_LIST) | head -20
	@echo ""
	@echo "New Test Targets (Previously Skipped):"
	@echo "  test-distributed         Run distributed IAM tests"
	@echo "  test-performance         Run performance tests"
	@echo "  test-stress              Run stress tests"
	@echo "  test-versioning-stress   Run S3 versioning stress tests"
	@echo "  test-keycloak-full       Run complete Keycloak integration tests"
	@echo "  test-all-previously-skipped  Run all previously skipped tests"
	@echo "  setup-all-tests          Setup environment for all tests"
	@echo ""
	@echo "Docker Compose Targets:"
	@echo "  docker-test              Run tests with Docker Compose including Keycloak"
	@echo "  docker-up                Start all services with Docker Compose"
	@echo "  docker-down              Stop all Docker Compose services"
	@echo "  docker-logs              Show logs from all services"

test: clean setup start-services run-tests stop-services ## Run complete IAM integration test suite

test-quick: run-tests ## Run tests assuming services are already running

test-with-server: run-tests ## Run tests with auto-managed server (TestMain)

run-tests: ## Execute the Go tests
	@echo "🧪 Running S3 IAM Integration Tests..."
	go test -v -timeout $(TEST_TIMEOUT) ./...

setup: ## Setup test environment
	@echo "🔧 Setting up test environment..."
	@mkdir -p test-volume-data/filerldb2
	@mkdir -p test-volume-data/m9333

start-services: ## Start SeaweedFS services for testing
	@echo "🚀 Starting SeaweedFS services..."
	@echo "Starting master server..."
	@$(WEED_BINARY) master -port=$(MASTER_PORT) \
		-mdir=test-volume-data/m9333 \
		-peers=none > weed-master.log 2>&1 & \
		echo $$! > $(MASTER_PID_FILE)
	
	@echo "Waiting for master server to be ready..."
	@timeout 60 bash -c 'until curl -s http://localhost:$(MASTER_PORT)/cluster/status > /dev/null 2>&1; do echo "Waiting for master server..."; sleep 2; done' || (echo "❌ Master failed to start, checking logs..." && tail -20 weed-master.log && exit 1)
	@echo "✅ Master server is ready"
	
	@echo "Starting volume server..."
	@$(WEED_BINARY) volume -port=$(VOLUME_PORT) \
		-ip=localhost \
		-dataCenter=dc1 -rack=rack1 \
		-dir=test-volume-data \
		-max=100 \
		-master=localhost:$(MASTER_PORT) > weed-volume.log 2>&1 & \
		echo $$! > $(VOLUME_PID_FILE)
	
	@echo "Waiting for volume server to be ready..."
	@timeout 60 bash -c 'until curl -s http://localhost:$(VOLUME_PORT)/status > /dev/null 2>&1; do echo "Waiting for volume server..."; sleep 2; done' || (echo "❌ Volume server failed to start, checking logs..." && tail -20 weed-volume.log && exit 1)
	@echo "✅ Volume server is ready"
	
	@echo "Starting filer server..."
	@$(WEED_BINARY) filer -port=$(FILER_PORT) \
		-defaultStoreDir=test-volume-data/filerldb2 \
		-master=localhost:$(MASTER_PORT) > weed-filer.log 2>&1 & \
		echo $$! > $(FILER_PID_FILE)
	
	@echo "Waiting for filer server to be ready..."
	@timeout 60 bash -c 'until curl -s http://localhost:$(FILER_PORT)/status > /dev/null 2>&1; do echo "Waiting for filer server..."; sleep 2; done' || (echo "❌ Filer failed to start, checking logs..." && tail -20 weed-filer.log && exit 1)
	@echo "✅ Filer server is ready"
	
	@echo "Starting S3 API server with IAM..."
	@$(WEED_BINARY) -v=3 s3 -port=$(S3_PORT) \
		-filer=localhost:$(FILER_PORT) \
		-config=test_config.json \
		-iam.config=$(CURDIR)/iam_config.json > weed-s3.log 2>&1 & \
		echo $$! > $(S3_PID_FILE)
	
	@echo "Waiting for S3 API server to be ready..."
	@timeout 60 bash -c 'until curl -s http://localhost:$(S3_PORT) > /dev/null 2>&1; do echo "Waiting for S3 API server..."; sleep 2; done' || (echo "❌ S3 API failed to start, checking logs..." && tail -20 weed-s3.log && exit 1)
	@echo "✅ S3 API server is ready"
	
	@echo "✅ All services started and ready"

wait-for-services: ## Wait for all services to be ready
	@echo "⏳ Waiting for services to be ready..."
	@echo "Checking master server..."
	@timeout 30 bash -c 'until curl -s http://localhost:$(MASTER_PORT)/cluster/status > /dev/null; do sleep 1; done' || (echo "❌ Master failed to start" && exit 1)
	
	@echo "Checking filer server..."
	@timeout 30 bash -c 'until curl -s http://localhost:$(FILER_PORT)/status > /dev/null; do sleep 1; done' || (echo "❌ Filer failed to start" && exit 1)
	
	@echo "Checking S3 API server..."
	@timeout 30 bash -c 'until curl -s http://localhost:$(S3_PORT) > /dev/null 2>&1; do sleep 1; done' || (echo "❌ S3 API failed to start" && exit 1)
	
	@echo "Pre-allocating volumes for concurrent operations..."
	@curl -s "http://localhost:$(MASTER_PORT)/vol/grow?collection=default&count=10&replication=000" > /dev/null || echo "⚠️  Volume pre-allocation failed, but continuing..."
	@sleep 3
	@echo "✅ All services are ready"

stop-services: ## Stop all SeaweedFS services
	@echo "🛑 Stopping SeaweedFS services..."
	@if [ -f $(S3_PID_FILE) ]; then \
		echo "Stopping S3 API server..."; \
		kill $$(cat $(S3_PID_FILE)) 2>/dev/null || true; \
		rm -f $(S3_PID_FILE); \
	fi
	@if [ -f $(FILER_PID_FILE) ]; then \
		echo "Stopping filer server..."; \
		kill $$(cat $(FILER_PID_FILE)) 2>/dev/null || true; \
		rm -f $(FILER_PID_FILE); \
	fi
	@if [ -f $(VOLUME_PID_FILE) ]; then \
		echo "Stopping volume server..."; \
		kill $$(cat $(VOLUME_PID_FILE)) 2>/dev/null || true; \
		rm -f $(VOLUME_PID_FILE); \
	fi
	@if [ -f $(MASTER_PID_FILE) ]; then \
		echo "Stopping master server..."; \
		kill $$(cat $(MASTER_PID_FILE)) 2>/dev/null || true; \
		rm -f $(MASTER_PID_FILE); \
	fi
	@echo "✅ All services stopped"

clean: stop-services ## Clean up test environment
	@echo "🧹 Cleaning up test environment..."
	@rm -rf test-volume-data
	@rm -f weed-*.log
	@rm -f *.test
	@echo "✅ Cleanup complete"

logs: ## Show service logs
	@echo "📋 Service Logs:"
	@echo "=== Master Log ==="
	@tail -20 weed-master.log 2>/dev/null || echo "No master log"
	@echo ""
	@echo "=== Volume Log ==="
	@tail -20 weed-volume.log 2>/dev/null || echo "No volume log"
	@echo ""
	@echo "=== Filer Log ==="
	@tail -20 weed-filer.log 2>/dev/null || echo "No filer log"
	@echo ""
	@echo "=== S3 API Log ==="
	@tail -20 weed-s3.log 2>/dev/null || echo "No S3 log"

status: ## Check service status
	@echo "📊 Service Status:"
	@echo -n "Master: "; curl -s http://localhost:$(MASTER_PORT)/cluster/status > /dev/null 2>&1 && echo "✅ Running" || echo "❌ Not running"
	@echo -n "Filer: "; curl -s http://localhost:$(FILER_PORT)/status > /dev/null 2>&1 && echo "✅ Running" || echo "❌ Not running"
	@echo -n "S3 API: "; curl -s http://localhost:$(S3_PORT) > /dev/null 2>&1 && echo "✅ Running" || echo "❌ Not running"

debug: start-services wait-for-services ## Start services and keep them running for debugging
	@echo "🐛 Services started in debug mode. Press Ctrl+C to stop..."
	@trap 'make stop-services' INT; \
	while true; do \
		sleep 1; \
	done

# Test specific scenarios
test-auth: ## Test only authentication scenarios
	go test -v -run TestS3IAMAuthentication ./...

test-policy: ## Test only policy enforcement
	go test -v -run TestS3IAMPolicyEnforcement ./...

test-expiration: ## Test only session expiration
	go test -v -run TestS3IAMSessionExpiration ./...

test-multipart: ## Test only multipart upload IAM integration
	go test -v -run TestS3IAMMultipartUploadPolicyEnforcement ./...

test-bucket-policy: ## Test only bucket policy integration
	go test -v -run TestS3IAMBucketPolicyIntegration ./...

test-context: ## Test only contextual policy enforcement  
	go test -v -run TestS3IAMContextualPolicyEnforcement ./...

test-presigned: ## Test only presigned URL integration
	go test -v -run TestS3IAMPresignedURLIntegration ./...

# Performance testing
benchmark: setup start-services wait-for-services ## Run performance benchmarks
	@echo "🏁 Running IAM performance benchmarks..."
	go test -bench=. -benchmem -timeout $(TEST_TIMEOUT) ./...
	@make stop-services

# Continuous integration
ci: ## Run tests suitable for CI environment
	@echo "🔄 Running CI tests..."
	@export CGO_ENABLED=0; make test

# Development helpers
watch: ## Watch for file changes and re-run tests
	@echo "👀 Watching for changes..."
	@command -v entr >/dev/null 2>&1 || (echo "entr is required for watch mode. Install with: brew install entr" && exit 1)
	@find . -name "*.go" | entr -r make test-quick

install-deps: ## Install test dependencies
	@echo "📦 Installing test dependencies..."
	go mod tidy
	go get -u github.com/stretchr/testify
	go get -u github.com/aws/aws-sdk-go
	go get -u github.com/golang-jwt/jwt/v5

# Docker support
docker-test-legacy: ## Run tests in Docker container (legacy)
	@echo "🐳 Running tests in Docker..."
	docker build -f Dockerfile.test -t seaweedfs-s3-iam-test .
	docker run --rm -v $(PWD)/../../../:/app seaweedfs-s3-iam-test

# Docker Compose support with Keycloak
docker-up: ## Start all services with Docker Compose (including Keycloak)
	@echo "🐳 Starting services with Docker Compose including Keycloak..."
	@docker compose up -d
	@echo "⏳ Waiting for services to be healthy..."
	@timeout 120 bash -c 'until curl -s http://localhost:8080/health/ready > /dev/null 2>&1; do sleep 2; done' || (echo "❌ Keycloak failed to become ready" && exit 1)
	@timeout 60 bash -c 'until curl -s http://localhost:8333 > /dev/null 2>&1; do sleep 2; done' || (echo "❌ S3 API failed to become ready" && exit 1)
	@timeout 60 bash -c 'until curl -s http://localhost:8888 > /dev/null 2>&1; do sleep 2; done' || (echo "❌ Filer failed to become ready" && exit 1)
	@timeout 60 bash -c 'until curl -s http://localhost:9333 > /dev/null 2>&1; do sleep 2; done' || (echo "❌ Master failed to become ready" && exit 1)
	@echo "✅ All services are healthy and ready"

docker-down: ## Stop all Docker Compose services
	@echo "🐳 Stopping Docker Compose services..."
	@docker compose down -v
	@echo "✅ All services stopped"

docker-logs: ## Show logs from all services
	@docker compose logs -f

docker-test: docker-up ## Run tests with Docker Compose including Keycloak
	@echo "🧪 Running Keycloak integration tests..."
	@export KEYCLOAK_URL="http://localhost:8080" && \
	 export S3_ENDPOINT="http://localhost:8333" && \
	 go test -v -timeout $(TEST_TIMEOUT) -run "TestKeycloak" ./...
	@echo "🐳 Stopping services after tests..."
	@make docker-down

docker-build: ## Build custom SeaweedFS image for Docker tests
	@echo "🏗️ Building custom SeaweedFS image..."
	@docker build -f Dockerfile.s3 -t seaweedfs-iam:latest ../../..
	@echo "✅ Image built successfully"

# All PHONY targets
.PHONY: test test-quick run-tests setup start-services stop-services wait-for-services clean logs status debug
.PHONY: test-auth test-policy test-expiration test-multipart test-bucket-policy test-context test-presigned  
.PHONY: benchmark ci watch install-deps docker-test docker-up docker-down docker-logs docker-build
.PHONY: test-distributed test-performance test-stress test-versioning-stress test-keycloak-full test-all-previously-skipped setup-all-tests help-advanced



# New test targets for previously skipped tests

test-distributed: ## Run distributed IAM tests
	@echo "🌐 Running distributed IAM tests..."
	@export ENABLE_DISTRIBUTED_TESTS=true && go test -v -timeout $(TEST_TIMEOUT) -run "TestS3IAMDistributedTests" ./...

test-performance: ## Run performance tests
	@echo "🏁 Running performance tests..."
	@export ENABLE_PERFORMANCE_TESTS=true && go test -v -timeout $(TEST_TIMEOUT) -run "TestS3IAMPerformanceTests" ./...

test-stress: ## Run stress tests
	@echo "💪 Running stress tests..."
	@export ENABLE_STRESS_TESTS=true && ./run_stress_tests.sh

test-versioning-stress: ## Run S3 versioning stress tests
	@echo "📚 Running versioning stress tests..."
	@cd ../versioning && ./enable_stress_tests.sh

test-keycloak-full: docker-up ## Run complete Keycloak integration tests
	@echo "🔐 Running complete Keycloak integration tests..."
	@export KEYCLOAK_URL="http://localhost:8080" && \
	 export S3_ENDPOINT="http://localhost:8333" && \
	 go test -v -timeout $(TEST_TIMEOUT) -run "TestKeycloak" ./...
	@make docker-down

test-all-previously-skipped: ## Run all previously skipped tests
	@echo "🎯 Running all previously skipped tests..."
	@./run_all_tests.sh

setup-all-tests: ## Setup environment for all tests (including Keycloak)
	@echo "🚀 Setting up complete test environment..."
	@./setup_all_tests.sh


