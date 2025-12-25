# Makefile for S3 SSE Integration Tests
# This Makefile provides targets for running comprehensive S3 Server-Side Encryption tests

# Default values
SEAWEEDFS_BINARY ?= weed
S3_PORT ?= 8333
FILER_PORT ?= 8888
VOLUME_PORT ?= 8080
MASTER_PORT ?= 9333
TEST_TIMEOUT ?= 15m
BUCKET_PREFIX ?= test-sse-
ACCESS_KEY ?= some_access_key1
SECRET_KEY ?= some_secret_key1
VOLUME_MAX_SIZE_MB ?= 50
VOLUME_MAX_COUNT ?= 100

# SSE-KMS configuration
KMS_KEY_ID ?= test-key-123
KMS_TYPE ?= local
OPENBAO_ADDR ?= http://127.0.0.1:8200
OPENBAO_TOKEN ?= root-token-for-testing
DOCKER_COMPOSE ?= docker-compose

# Test directory
TEST_DIR := $(shell pwd)
SEAWEEDFS_ROOT := $(shell cd ../../../ && pwd)

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m # No Color

.PHONY: all test clean start-seaweedfs stop-seaweedfs stop-seaweedfs-safe start-seaweedfs-ci check-binary build-weed help help-extended test-with-server test-quick-with-server test-metadata-persistence setup-openbao test-with-kms test-ssekms-integration clean-kms start-full-stack stop-full-stack

all: test-basic

# Build SeaweedFS binary (GitHub Actions compatible)
build-weed:
	@echo "Building SeaweedFS binary..."
	@cd $(SEAWEEDFS_ROOT)/weed && go install -buildvcs=false
	@echo "✅ SeaweedFS binary built successfully"

help:
	@echo "SeaweedFS S3 SSE Integration Tests"
	@echo ""
	@echo "Available targets:"
	@echo "  test-basic    - Run basic S3 put/get tests first"
	@echo "  test          - Run all S3 SSE integration tests"
	@echo "  test-ssec     - Run SSE-C tests only"
	@echo "  test-ssekms   - Run SSE-KMS tests only"
	@echo "  test-copy     - Run SSE copy operation tests"
	@echo "  test-multipart - Run SSE multipart upload tests"
	@echo "  test-errors   - Run SSE error condition tests"
	@echo "  benchmark     - Run SSE performance benchmarks"
	@echo "  KMS Integration:"
	@echo "  setup-openbao - Set up OpenBao KMS for testing"
	@echo "  test-with-kms - Run full SSE integration with real KMS"
	@echo "  test-ssekms-integration - Run SSE-KMS with OpenBao only"
	@echo "  start-full-stack - Start SeaweedFS + OpenBao with Docker"
	@echo "  stop-full-stack - Stop Docker services"
	@echo "  clean-kms     - Clean up KMS test environment"
	@echo "  start-seaweedfs - Start SeaweedFS server for testing"
	@echo "  stop-seaweedfs - Stop SeaweedFS server"
	@echo "  clean         - Clean up test artifacts"
	@echo "  check-binary  - Check if SeaweedFS binary exists"
	@echo ""
	@echo "Configuration:"
	@echo "  SEAWEEDFS_BINARY=$(SEAWEEDFS_BINARY)"
	@echo "  S3_PORT=$(S3_PORT)"
	@echo "  FILER_PORT=$(FILER_PORT)"
	@echo "  VOLUME_PORT=$(VOLUME_PORT)"
	@echo "  MASTER_PORT=$(MASTER_PORT)"
	@echo "  TEST_TIMEOUT=$(TEST_TIMEOUT)"
	@echo "  VOLUME_MAX_SIZE_MB=$(VOLUME_MAX_SIZE_MB)"

check-binary:
	@if ! command -v $(SEAWEEDFS_BINARY) > /dev/null 2>&1; then \
		echo "$(RED)Error: SeaweedFS binary '$(SEAWEEDFS_BINARY)' not found in PATH$(NC)"; \
		echo "Please build SeaweedFS first by running 'make' in the root directory"; \
		exit 1; \
	fi
	@echo "$(GREEN)SeaweedFS binary found: $$(which $(SEAWEEDFS_BINARY))$(NC)"

start-seaweedfs: check-binary
	@echo "$(YELLOW)Starting SeaweedFS server for SSE testing...$(NC)"
	@# Use port-based cleanup for consistency and safety
	@echo "Cleaning up any existing processes..."
	@lsof -ti :$(MASTER_PORT) | xargs -r kill -TERM || true
	@lsof -ti :$(VOLUME_PORT) | xargs -r kill -TERM || true
	@lsof -ti :$(FILER_PORT) | xargs -r kill -TERM || true
	@lsof -ti :$(S3_PORT) | xargs -r kill -TERM || true
	@sleep 2
	
	# Create necessary directories
	@mkdir -p /tmp/seaweedfs-test-sse
	
	# Create S3 configuration with SSE-KMS support
	@printf '{"identities":[{"name":"%s","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write"]}],"kms":{"type":"%s","configs":{"keyId":"%s","encryptionContext":{},"bucketKey":false}}}' "$(ACCESS_KEY)" "$(ACCESS_KEY)" "$(SECRET_KEY)" "$(KMS_TYPE)" "$(KMS_KEY_ID)" > /tmp/seaweedfs-sse-s3.json
	
	# Start weed mini
	@$(SEAWEEDFS_BINARY) mini \
		-dir=/tmp/seaweedfs-test-sse \
		-master.port=$(MASTER_PORT) \
		-port.grpc=$$(( $(MASTER_PORT) + 10000 )) \
		-master.volumeSizeLimitMB=$(VOLUME_MAX_SIZE_MB) \
		-volume.port=$(VOLUME_PORT) \
		-volume.max=$(VOLUME_MAX_COUNT) \
		-filer.port=$(FILER_PORT) \
		-filer.port.grpc=$$(( $(FILER_PORT) + 10000 )) \
		-indexer.port.grpc=$$(( $(FILER_PORT) + 10001 )) \
		-filer.dataCenter=defaultDataCenter \
		-s3.port=$(S3_PORT) \
		-s3.config=/tmp/seaweedfs-sse-s3.json \
		-ip=127.0.0.1 \
		-master.peers=none \
		> /tmp/seaweedfs-sse-mini.log 2>&1 & echo $$! > /tmp/weed-mini.pid

	# Wait for S3 service to be ready
	@echo "$(YELLOW)Waiting for S3 service to be ready...$(NC)"
	@for i in $$(seq 1 30); do \
		if curl -s -f http://127.0.0.1:$(S3_PORT) > /dev/null 2>&1; then \
			echo "$(GREEN)S3 service is ready$(NC)"; \
			break; \
		fi; \
		echo "Waiting for S3 service... ($$i/30)"; \
		sleep 1; \
	done
	
	# Additional wait for filer gRPC to be ready
	@echo "$(YELLOW)Waiting for filer gRPC to be ready...$(NC)"
	@sleep 2
	@echo "$(GREEN)SeaweedFS server started successfully for SSE testing$(NC)"
	@echo "Mini Log: /tmp/seaweedfs-sse-mini.log"
	@echo "Master: http://localhost:$(MASTER_PORT)"
	@echo "Volume: http://localhost:$(VOLUME_PORT)"
	@echo "Filer: http://localhost:$(FILER_PORT)"
	@echo "S3: http://localhost:$(S3_PORT)"
	@echo "Volume Max Size: $(VOLUME_MAX_SIZE_MB)MB"
	@echo "SSE-KMS Support: Enabled"

stop-seaweedfs:
	@echo "$(YELLOW)Stopping SeaweedFS server...$(NC)"
	@# Use port-based cleanup for consistency and safety
	@if [ -f /tmp/weed-mini.pid ]; then \
		echo "Stopping weed mini..."; \
		kill $$(cat /tmp/weed-mini.pid) || true; \
		rm -f /tmp/weed-mini.pid; \
	fi
	@lsof -ti :$(MASTER_PORT) | xargs -r kill -TERM || true
	@lsof -ti :$(VOLUME_PORT) | xargs -r kill -TERM || true
	@lsof -ti :$(FILER_PORT) | xargs -r kill -TERM || true
	@lsof -ti :$(S3_PORT) | xargs -r kill -TERM || true
	@sleep 2
	@echo "$(GREEN)SeaweedFS server stopped$(NC)"

# CI-safe server stop that's more conservative
stop-seaweedfs-safe:
	@echo "$(YELLOW)Safely stopping SeaweedFS server...$(NC)"
	@# Use port-based cleanup which is safer in CI
	@if command -v lsof >/dev/null 2>&1; then \
		echo "Using lsof for port-based cleanup..."; \
		lsof -ti :$(MASTER_PORT) 2>/dev/null | head -5 | while read pid; do kill -TERM $$pid 2>/dev/null || true; done; \
		lsof -ti :$(VOLUME_PORT) 2>/dev/null | head -5 | while read pid; do kill -TERM $$pid 2>/dev/null || true; done; \
		lsof -ti :$(FILER_PORT) 2>/dev/null | head -5 | while read pid; do kill -TERM $$pid 2>/dev/null || true; done; \
		lsof -ti :$(S3_PORT) 2>/dev/null | head -5 | while read pid; do kill -TERM $$pid 2>/dev/null || true; done; \
	else \
		echo "lsof not available, using netstat approach..."; \
		netstat -tlnp 2>/dev/null | grep :$(MASTER_PORT) | awk '{print $$7}' | cut -d/ -f1 | head -5 | while read pid; do [ "$$pid" != "-" ] && kill -TERM $$pid 2>/dev/null || true; done; \
		netstat -tlnp 2>/dev/null | grep :$(VOLUME_PORT) | awk '{print $$7}' | cut -d/ -f1 | head -5 | while read pid; do [ "$$pid" != "-" ] && kill -TERM $$pid 2>/dev/null || true; done; \
		netstat -tlnp 2>/dev/null | grep :$(FILER_PORT) | awk '{print $$7}' | cut -d/ -f1 | head -5 | while read pid; do [ "$$pid" != "-" ] && kill -TERM $$pid 2>/dev/null || true; done; \
		netstat -tlnp 2>/dev/null | grep :$(S3_PORT) | awk '{print $$7}' | cut -d/ -f1 | head -5 | while read pid; do [ "$$pid" != "-" ] && kill -TERM $$pid 2>/dev/null || true; done; \
	fi
	@sleep 2
	@echo "$(GREEN)SeaweedFS server safely stopped$(NC)"

clean:
	@echo "$(YELLOW)Cleaning up SSE test artifacts...$(NC)"
	@rm -rf /tmp/seaweedfs-test-sse-*
	@rm -f /tmp/seaweedfs-sse-*.log
	@rm -f /tmp/seaweedfs-sse-s3.json
	@echo "$(GREEN)SSE test cleanup completed$(NC)"

test-basic: check-binary
	@echo "$(YELLOW)Running basic S3 SSE integration tests...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@echo "$(GREEN)Starting basic SSE tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSECIntegrationBasic|TestSSEKMSIntegrationBasic" ./test/s3/sse || (echo "$(RED)Basic SSE tests failed$(NC)" && $(MAKE) stop-seaweedfs-safe && exit 1)
	@$(MAKE) stop-seaweedfs-safe
	@echo "$(GREEN)Basic SSE tests completed successfully!$(NC)"

test: test-basic
	@echo "$(YELLOW)Running all S3 SSE integration tests...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@echo "$(GREEN)Starting comprehensive SSE tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSE.*Integration" ./test/s3/sse || (echo "$(RED)SSE tests failed$(NC)" && $(MAKE) stop-seaweedfs-safe && exit 1)
	@$(MAKE) stop-seaweedfs-safe
	@echo "$(GREEN)All SSE integration tests completed successfully!$(NC)"

test-ssec: check-binary
	@echo "$(YELLOW)Running SSE-C integration tests...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@echo "$(GREEN)Starting SSE-C tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSEC.*Integration" ./test/s3/sse || (echo "$(RED)SSE-C tests failed$(NC)" && $(MAKE) stop-seaweedfs-safe && exit 1)
	@$(MAKE) stop-seaweedfs-safe
	@echo "$(GREEN)SSE-C tests completed successfully!$(NC)"

test-ssekms: check-binary
	@echo "$(YELLOW)Running SSE-KMS integration tests...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@echo "$(GREEN)Starting SSE-KMS tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSEKMS.*Integration" ./test/s3/sse || (echo "$(RED)SSE-KMS tests failed$(NC)" && $(MAKE) stop-seaweedfs-safe && exit 1)
	@$(MAKE) stop-seaweedfs-safe
	@echo "$(GREEN)SSE-KMS tests completed successfully!$(NC)"

test-copy: check-binary
	@echo "$(YELLOW)Running SSE copy operation tests...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@echo "$(GREEN)Starting SSE copy tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run ".*CopyIntegration" ./test/s3/sse || (echo "$(RED)SSE copy tests failed$(NC)" && $(MAKE) stop-seaweedfs-safe && exit 1)
	@$(MAKE) stop-seaweedfs-safe
	@echo "$(GREEN)SSE copy tests completed successfully!$(NC)"

test-multipart: check-binary
	@echo "$(YELLOW)Running SSE multipart upload tests...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@echo "$(GREEN)Starting SSE multipart tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSEMultipartUploadIntegration" ./test/s3/sse || (echo "$(RED)SSE multipart tests failed$(NC)" && $(MAKE) stop-seaweedfs-safe && exit 1)
	@$(MAKE) stop-seaweedfs-safe
	@echo "$(GREEN)SSE multipart tests completed successfully!$(NC)"

test-errors: check-binary
	@echo "$(YELLOW)Running SSE error condition tests...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@echo "$(GREEN)Starting SSE error tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSEErrorConditions" ./test/s3/sse || (echo "$(RED)SSE error tests failed$(NC)" && $(MAKE) stop-seaweedfs-safe && exit 1)
	@$(MAKE) stop-seaweedfs-safe
	@echo "$(GREEN)SSE error tests completed successfully!$(NC)"

test-quick: check-binary
	@echo "$(YELLOW)Running quick SSE tests...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@echo "$(GREEN)Starting quick SSE tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=5m -run "TestSSECIntegrationBasic|TestSSEKMSIntegrationBasic" ./test/s3/sse || (echo "$(RED)Quick SSE tests failed$(NC)" && $(MAKE) stop-seaweedfs-safe && exit 1)
	@$(MAKE) stop-seaweedfs-safe
	@echo "$(GREEN)Quick SSE tests completed successfully!$(NC)"

benchmark: check-binary
	@echo "$(YELLOW)Running SSE performance benchmarks...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@echo "$(GREEN)Starting SSE benchmarks...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=30m -bench=. -run=Benchmark ./test/s3/sse || (echo "$(RED)SSE benchmarks failed$(NC)" && $(MAKE) stop-seaweedfs-safe && exit 1)
	@$(MAKE) stop-seaweedfs-safe
	@echo "$(GREEN)SSE benchmarks completed!$(NC)"

# Debug targets
debug-logs:
	@echo "$(YELLOW)=== Master Log ===$(NC)"
	@tail -n 50 /tmp/seaweedfs-sse-master.log || echo "No master log found"
	@echo "$(YELLOW)=== Volume Log ===$(NC)"
	@tail -n 50 /tmp/seaweedfs-sse-volume.log || echo "No volume log found"
	@echo "$(YELLOW)=== Filer Log ===$(NC)"
	@tail -n 50 /tmp/seaweedfs-sse-filer.log || echo "No filer log found"
	@echo "$(YELLOW)=== S3 Log ===$(NC)"
	@tail -n 50 /tmp/seaweedfs-sse-s3.log || echo "No S3 log found"

debug-status:
	@echo "$(YELLOW)=== Process Status ===$(NC)"
	@ps aux | grep -E "(weed|seaweedfs)" | grep -v grep || echo "No SeaweedFS processes found"
	@echo "$(YELLOW)=== Port Status ===$(NC)"
	@netstat -an | grep -E "($(MASTER_PORT)|$(VOLUME_PORT)|$(FILER_PORT)|$(S3_PORT))" || echo "No ports in use"

# Manual test targets for development
manual-start: start-seaweedfs
	@echo "$(GREEN)SeaweedFS with SSE support is now running for manual testing$(NC)"
	@echo "You can now run SSE tests manually or use S3 clients to test SSE functionality"
	@echo "Run 'make manual-stop' when finished"

manual-stop: stop-seaweedfs clean

# CI/CD targets
ci-test: test-quick

# Stress test
stress: check-binary
	@echo "$(YELLOW)Running SSE stress tests...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=60m -run="TestSSE.*Integration" -count=5 ./test/s3/sse || (echo "$(RED)SSE stress tests failed$(NC)" && $(MAKE) stop-seaweedfs-safe && exit 1)
	@$(MAKE) stop-seaweedfs-safe
	@echo "$(GREEN)SSE stress tests completed!$(NC)"

# Performance test with various data sizes
perf: check-binary
	@echo "$(YELLOW)Running SSE performance tests with various data sizes...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=60m -run=".*VariousDataSizes" ./test/s3/sse || (echo "$(RED)SSE performance tests failed$(NC)" && $(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe && exit 1)
	@$(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe
	@echo "$(GREEN)SSE performance tests completed!$(NC)"

# Test specific scenarios that would catch the metadata bug
test-metadata-persistence: check-binary
	@echo "$(YELLOW)Running SSE metadata persistence tests (would catch filer metadata bugs)...$(NC)"
	@$(MAKE) start-seaweedfs-ci
	@sleep 5
	@echo "$(GREEN)Testing that SSE metadata survives full PUT/GET cycle...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSECIntegrationBasic" ./test/s3/sse || (echo "$(RED)SSE metadata persistence tests failed$(NC)" && $(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe && exit 1)
	@$(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe
	@echo "$(GREEN)SSE metadata persistence tests completed successfully!$(NC)"
	@echo "$(GREEN)✅ These tests would have caught the filer metadata storage bug!$(NC)"

# GitHub Actions compatible test-with-server target that handles server lifecycle
test-with-server: build-weed
	@echo "🚀 Starting SSE integration tests with automated server management..."
	@echo "Starting SeaweedFS cluster..."
	@# Use the CI-safe startup directly without aggressive cleanup
	@if $(MAKE) start-seaweedfs-ci > weed-test.log 2>&1; then \
		echo "✅ SeaweedFS cluster started successfully"; \
		echo "Running SSE integration tests..."; \
		trap '$(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe || true' EXIT; \
		if [ -n "$(TEST_PATTERN)" ]; then \
			echo "🔍 Running tests matching pattern: $(TEST_PATTERN)"; \
			cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" ./test/s3/sse || exit 1; \
		else \
			echo "🔍 Running all SSE integration tests"; \
			cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSE.*Integration" ./test/s3/sse || exit 1; \
		fi; \
		echo "✅ All tests completed successfully"; \
		$(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe || true; \
	else \
		echo "❌ Failed to start SeaweedFS cluster"; \
		echo "=== Server startup logs ==="; \
		tail -100 weed-test.log 2>/dev/null || echo "No startup log available"; \
		echo "=== System information ==="; \
		ps aux | grep -E "weed|make" | grep -v grep || echo "No relevant processes found"; \
		exit 1; \
	fi

# CI-safe server startup that avoids process conflicts
start-seaweedfs-ci: check-binary
	@echo "$(YELLOW)Starting SeaweedFS server for CI testing...$(NC)"
	
	# Create necessary directories
	@mkdir -p /tmp/seaweedfs-test-sse
	
	# Clean up any old server logs
	@rm -f /tmp/seaweedfs-sse-*.log || true
	
	# Create S3 JSON configuration with KMS (Local provider) and basic identity for embedded S3
	@sed -e 's/ACCESS_KEY_PLACEHOLDER/$(ACCESS_KEY)/g' \
	     -e 's/SECRET_KEY_PLACEHOLDER/$(SECRET_KEY)/g' \
	     s3-config-template.json > /tmp/seaweedfs-s3.json
	
	# Start weed mini with embedded S3 using the JSON config (with verbose logging)
	@echo "Starting weed mini with embedded S3..."
	@AWS_ACCESS_KEY_ID=$(ACCESS_KEY) AWS_SECRET_ACCESS_KEY=$(SECRET_KEY) GLOG_v=4 $(SEAWEEDFS_BINARY) mini \
		-dir=/tmp/seaweedfs-test-sse \
		-master.port=$(MASTER_PORT) \
		-port.grpc=$$(( $(MASTER_PORT) + 10000 )) \
		-master.volumeSizeLimitMB=$(VOLUME_MAX_SIZE_MB) \
		-volume.port=$(VOLUME_PORT) \
		-volume.max=$(VOLUME_MAX_COUNT) \
		-filer.port=$(FILER_PORT) \
		-filer.port.grpc=$$(( $(FILER_PORT) + 10000 )) \
		-indexer.port.grpc=$$(( $(FILER_PORT) + 10001 )) \
		-filer.dataCenter=defaultDataCenter \
		-s3.port=$(S3_PORT) \
		-s3.config=/tmp/seaweedfs-s3.json \
		-ip=127.0.0.1 \
		-master.peers=none \
		> /tmp/seaweedfs-sse-mini.log 2>&1 & echo $$! > /tmp/weed-mini.pid
	
	# Wait for S3 service to be ready - use port-based checking for reliability
	@echo "$(YELLOW)Waiting for S3 service to be ready...$(NC)"
	@for i in $$(seq 1 20); do \
		if netstat -an 2>/dev/null | grep -q ":$(S3_PORT).*LISTEN" || \
		   ss -an 2>/dev/null | grep -q ":$(S3_PORT).*LISTEN" || \
		   lsof -i :$(S3_PORT) >/dev/null 2>&1; then \
			echo "$(GREEN)S3 service is listening on port $(S3_PORT)$(NC)"; \
			sleep 1; \
			break; \
		fi; \
		if [ $$i -eq 20 ]; then \
			echo "$(RED)S3 service failed to start within 20 seconds$(NC)"; \
			echo "=== Detailed Logs ==="; \
			echo "Mini log:"; tail -30 /tmp/seaweedfs-sse-mini.log || true; \
			echo "=== Port Status ==="; \
			netstat -an 2>/dev/null | grep ":$(S3_PORT)" || \
			ss -an 2>/dev/null | grep ":$(S3_PORT)" || \
			echo "No port listening on $(S3_PORT)"; \
			echo "=== Process Status ==="; \
			ps aux | grep -E "weed.*(filer|s3).*$(S3_PORT)" | grep -v grep || echo "No S3 process found"; \
			exit 1; \
		fi; \
		echo "Waiting for S3 service... ($$i/20)"; \
		sleep 1; \
	done
	
	# Additional wait for filer gRPC to be ready
	@echo "$(YELLOW)Waiting for filer gRPC to be ready...$(NC)"
	@sleep 2
	@echo "$(GREEN)SeaweedFS server started successfully for SSE testing$(NC)"
	@echo "Master: http://localhost:$(MASTER_PORT)"
	@echo "Volume: http://localhost:$(VOLUME_PORT)"
	@echo "Filer: http://localhost:$(FILER_PORT)"
	@echo "S3: http://localhost:$(S3_PORT)"
	@echo "Volume Max Size: $(VOLUME_MAX_SIZE_MB)MB"
	@echo "SSE-KMS Support: Enabled"

# GitHub Actions compatible quick test subset
test-quick-with-server: build-weed
	@echo "🚀 Starting quick SSE tests with automated server management..."
	@trap 'make stop-seaweedfs-safe || true' EXIT; \
	echo "Starting SeaweedFS cluster..."; \
	if make start-seaweedfs-ci > weed-test.log 2>&1; then \
		echo "✅ SeaweedFS cluster started successfully"; \
		echo "Running quick SSE integration tests..."; \
		cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSECIntegrationBasic|TestSSEKMSIntegrationBasic|TestSimpleSSECIntegration" ./test/s3/sse || exit 1; \
		echo "✅ Quick tests completed successfully"; \
		make stop-seaweedfs-safe || true; \
	else \
		echo "❌ Failed to start SeaweedFS cluster"; \
		echo "=== Server startup logs ==="; \
		tail -50 weed-test.log; \
		exit 1; \
	fi

# Help target - extended version
help-extended:
	@echo "Available targets:"
	@echo "  test                    - Run all SSE integration tests (requires running server)"
	@echo "  test-with-server        - Run all tests with automatic server management (GitHub Actions compatible)"
	@echo "  test-quick-with-server  - Run quick tests with automatic server management"
	@echo "  test-ssec               - Run only SSE-C tests"  
	@echo "  test-ssekms             - Run only SSE-KMS tests"
	@echo "  test-copy               - Run only copy operation tests"
	@echo "  test-multipart          - Run only multipart upload tests"
	@echo "  benchmark               - Run performance benchmarks"
	@echo "  perf                    - Run performance tests with various data sizes"
	@echo "  test-metadata-persistence - Test metadata persistence (catches filer bugs)"
	@echo "  build-weed              - Build SeaweedFS binary"
	@echo "  check-binary            - Check if SeaweedFS binary exists"
	@echo "  start-seaweedfs         - Start SeaweedFS cluster"
	@echo "  start-seaweedfs-ci      - Start SeaweedFS cluster (CI-safe version)"
	@echo "  stop-seaweedfs          - Stop SeaweedFS cluster"
	@echo "  stop-seaweedfs-safe     - Stop SeaweedFS cluster (CI-safe version)"
	@echo "  clean                   - Clean up test artifacts"
	@echo "  debug-logs              - Show recent logs from all services"
	@echo ""
	@echo "Environment Variables:"
	@echo "  ACCESS_KEY              - S3 access key (default: some_access_key1)"
	@echo "  SECRET_KEY              - S3 secret key (default: some_secret_key1)"
	@echo "  KMS_KEY_ID              - KMS key ID for SSE-KMS (default: test-key-123)"
	@echo "  KMS_TYPE                - KMS type (default: local)"
	@echo "  VOLUME_MAX_SIZE_MB      - Volume maximum size in MB (default: 50)"
	@echo "  TEST_TIMEOUT            - Test timeout (default: 15m)"

####################################################
# KMS Integration Testing with OpenBao
####################################################

setup-openbao:
	@echo "$(YELLOW)Setting up OpenBao for SSE-KMS testing...$(NC)"
	@$(DOCKER_COMPOSE) up -d openbao
	@sleep 10
	@echo "$(YELLOW)Configuring OpenBao...$(NC)"
	@OPENBAO_ADDR=$(OPENBAO_ADDR) OPENBAO_TOKEN=$(OPENBAO_TOKEN) ./setup_openbao_sse.sh
	@echo "$(GREEN)✅ OpenBao setup complete!$(NC)"

start-full-stack: setup-openbao
	@echo "$(YELLOW)Starting full SeaweedFS + KMS stack...$(NC)"
	@$(DOCKER_COMPOSE) up -d
	@echo "$(YELLOW)Waiting for services to be ready...$(NC)"
	@sleep 15
	@echo "$(GREEN)✅ Full stack running!$(NC)"
	@echo "OpenBao: $(OPENBAO_ADDR)"
	@echo "S3 API: http://localhost:$(S3_PORT)"

stop-full-stack:
	@echo "$(YELLOW)Stopping full stack...$(NC)"
	@$(DOCKER_COMPOSE) down
	@echo "$(GREEN)✅ Full stack stopped$(NC)"

test-with-kms: start-full-stack
	@echo "$(YELLOW)Running SSE integration tests with real KMS...$(NC)"
	@sleep 5  # Extra time for KMS initialization
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) ./test/s3/sse -run "SSE.*Integration" || (echo "$(RED)Tests failed$(NC)" && make stop-full-stack && exit 1)
	@echo "$(GREEN)✅ All KMS integration tests passed!$(NC)"
	@make stop-full-stack

test-ssekms-integration: start-full-stack  
	@echo "$(YELLOW)Running SSE-KMS integration tests with OpenBao...$(NC)"
	@sleep 5  # Extra time for KMS initialization
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) ./test/s3/sse -run "TestSSEKMS.*Integration" || (echo "$(RED)SSE-KMS tests failed$(NC)" && make stop-full-stack && exit 1)
	@echo "$(GREEN)✅ SSE-KMS integration tests passed!$(NC)"
	@make stop-full-stack

clean-kms:
	@echo "$(YELLOW)Cleaning up KMS test environment...$(NC)"
	@$(DOCKER_COMPOSE) down -v --remove-orphans || true
	@docker system prune -f || true
	@echo "$(GREEN)✅ KMS environment cleaned up!$(NC)"

status-kms:
	@echo "$(YELLOW)KMS Environment Status:$(NC)"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "$(YELLOW)OpenBao Health:$(NC)"
	@curl -s $(OPENBAO_ADDR)/v1/sys/health | jq '.' || echo "OpenBao not accessible"
	@echo ""
	@echo "$(YELLOW)S3 API Status:$(NC)"
	@curl -s http://localhost:$(S3_PORT) || echo "S3 API not accessible"

# Quick test with just basic KMS functionality
test-kms-quick: setup-openbao
	@echo "$(YELLOW)Running quick KMS functionality test...$(NC)"
	@cd ../../../test/kms && make dev-test
	@echo "$(GREEN)✅ Quick KMS test passed!$(NC)"

# Development targets
dev-kms: setup-openbao
	@echo "$(GREEN)Development environment ready$(NC)"
	@echo "OpenBao: $(OPENBAO_ADDR)"
	@echo "Token: $(OPENBAO_TOKEN)"
	@echo "Use 'make test-ssekms-integration' to run tests"
