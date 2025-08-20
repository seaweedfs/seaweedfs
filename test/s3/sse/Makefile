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

# Test directory
TEST_DIR := $(shell pwd)
SEAWEEDFS_ROOT := $(shell cd ../../../ && pwd)

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m # No Color

.PHONY: all test clean start-seaweedfs stop-seaweedfs check-binary help

all: test-basic

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
	@pkill -f "weed master" || true
	@pkill -f "weed volume" || true
	@pkill -f "weed filer" || true
	@pkill -f "weed s3" || true
	@sleep 2
	
	# Create necessary directories
	@mkdir -p /tmp/seaweedfs-test-sse-master
	@mkdir -p /tmp/seaweedfs-test-sse-volume
	@mkdir -p /tmp/seaweedfs-test-sse-filer
	
	# Start master server with volume size limit and explicit gRPC port
	@nohup $(SEAWEEDFS_BINARY) master -port=$(MASTER_PORT) -port.grpc=$$(( $(MASTER_PORT) + 10000 )) -mdir=/tmp/seaweedfs-test-sse-master -volumeSizeLimitMB=$(VOLUME_MAX_SIZE_MB) -ip=127.0.0.1 > /tmp/seaweedfs-sse-master.log 2>&1 &
	@sleep 3
	
	# Start volume server with master HTTP port and increased capacity
	@nohup $(SEAWEEDFS_BINARY) volume -port=$(VOLUME_PORT) -mserver=127.0.0.1:$(MASTER_PORT) -dir=/tmp/seaweedfs-test-sse-volume -max=$(VOLUME_MAX_COUNT) -ip=127.0.0.1 > /tmp/seaweedfs-sse-volume.log 2>&1 &
	@sleep 5
	
	# Start filer server (using standard SeaweedFS gRPC port convention: HTTP port + 10000)
	@nohup $(SEAWEEDFS_BINARY) filer -port=$(FILER_PORT) -port.grpc=$$(( $(FILER_PORT) + 10000 )) -master=127.0.0.1:$(MASTER_PORT) -dataCenter=defaultDataCenter -ip=127.0.0.1 > /tmp/seaweedfs-sse-filer.log 2>&1 &
	@sleep 3
	
	# Create S3 configuration with SSE-KMS support
	@echo '{"identities":[{"name":"$(ACCESS_KEY)","credentials":[{"accessKey":"$(ACCESS_KEY)","secretKey":"$(SECRET_KEY)"}],"actions":["Admin","Read","Write"]}],"kms":{"type":"local","configs":{"keyId":"test-key-123","encryptionContext":{},"bucketKey":false}}}' > /tmp/seaweedfs-sse-s3.json
	
	# Start S3 server with KMS configuration
	@nohup $(SEAWEEDFS_BINARY) s3 -port=$(S3_PORT) -filer=127.0.0.1:$(FILER_PORT) -config=/tmp/seaweedfs-sse-s3.json -ip.bind=127.0.0.1 > /tmp/seaweedfs-sse-s3.log 2>&1 &
	@sleep 5
	
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
	@echo "Master: http://localhost:$(MASTER_PORT)"
	@echo "Volume: http://localhost:$(VOLUME_PORT)"
	@echo "Filer: http://localhost:$(FILER_PORT)"
	@echo "S3: http://localhost:$(S3_PORT)"
	@echo "Volume Max Size: $(VOLUME_MAX_SIZE_MB)MB"
	@echo "SSE-KMS Support: Enabled"

stop-seaweedfs:
	@echo "$(YELLOW)Stopping SeaweedFS server...$(NC)"
	@pkill -f "weed master" || true
	@pkill -f "weed volume" || true
	@pkill -f "weed filer" || true
	@pkill -f "weed s3" || true
	@sleep 2
	@echo "$(GREEN)SeaweedFS server stopped$(NC)"

clean:
	@echo "$(YELLOW)Cleaning up SSE test artifacts...$(NC)"
	@rm -rf /tmp/seaweedfs-test-sse-*
	@rm -f /tmp/seaweedfs-sse-*.log
	@rm -f /tmp/seaweedfs-sse-s3.json
	@echo "$(GREEN)SSE test cleanup completed$(NC)"

test-basic: check-binary
	@echo "$(YELLOW)Running basic S3 SSE integration tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting basic SSE tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSECIntegrationBasic|TestSSEKMSIntegrationBasic" ./test/s3/sse || (echo "$(RED)Basic SSE tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)Basic SSE tests completed successfully!$(NC)"

test: test-basic
	@echo "$(YELLOW)Running all S3 SSE integration tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting comprehensive SSE tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSE.*Integration" ./test/s3/sse || (echo "$(RED)SSE tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)All SSE integration tests completed successfully!$(NC)"

test-ssec: check-binary
	@echo "$(YELLOW)Running SSE-C integration tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting SSE-C tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSEC.*Integration" ./test/s3/sse || (echo "$(RED)SSE-C tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)SSE-C tests completed successfully!$(NC)"

test-ssekms: check-binary
	@echo "$(YELLOW)Running SSE-KMS integration tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting SSE-KMS tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSEKMS.*Integration" ./test/s3/sse || (echo "$(RED)SSE-KMS tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)SSE-KMS tests completed successfully!$(NC)"

test-copy: check-binary
	@echo "$(YELLOW)Running SSE copy operation tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting SSE copy tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run ".*CopyIntegration" ./test/s3/sse || (echo "$(RED)SSE copy tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)SSE copy tests completed successfully!$(NC)"

test-multipart: check-binary
	@echo "$(YELLOW)Running SSE multipart upload tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting SSE multipart tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSEMultipartUploadIntegration" ./test/s3/sse || (echo "$(RED)SSE multipart tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)SSE multipart tests completed successfully!$(NC)"

test-errors: check-binary
	@echo "$(YELLOW)Running SSE error condition tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting SSE error tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSEErrorConditions" ./test/s3/sse || (echo "$(RED)SSE error tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)SSE error tests completed successfully!$(NC)"

test-quick: check-binary
	@echo "$(YELLOW)Running quick SSE tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting quick SSE tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=5m -run "TestSSECIntegrationBasic|TestSSEKMSIntegrationBasic" ./test/s3/sse || (echo "$(RED)Quick SSE tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)Quick SSE tests completed successfully!$(NC)"

benchmark: check-binary
	@echo "$(YELLOW)Running SSE performance benchmarks...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting SSE benchmarks...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=30m -bench=. -run=Benchmark ./test/s3/sse || (echo "$(RED)SSE benchmarks failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
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
	@$(MAKE) start-seaweedfs
	@sleep 5
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=60m -run="TestSSE.*Integration" -count=5 ./test/s3/sse || (echo "$(RED)SSE stress tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)SSE stress tests completed!$(NC)"

# Performance test with various data sizes
perf: check-binary
	@echo "$(YELLOW)Running SSE performance tests with various data sizes...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=60m -run=".*VariousDataSizes" ./test/s3/sse || (echo "$(RED)SSE performance tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)SSE performance tests completed!$(NC)"

# Test specific scenarios that would catch the metadata bug
test-metadata-persistence: check-binary
	@echo "$(YELLOW)Running SSE metadata persistence tests (would catch filer metadata bugs)...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Testing that SSE metadata survives full PUT/GET cycle...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSECIntegrationBasic|TestSSEKMSIntegrationBasic.*CopyIntegration" ./test/s3/sse || (echo "$(RED)SSE metadata persistence tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)SSE metadata persistence tests completed successfully!$(NC)"
	@echo "$(GREEN)✅ These tests would have caught the filer metadata storage bug!$(NC)"
