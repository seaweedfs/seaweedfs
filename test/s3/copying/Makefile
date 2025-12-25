# Makefile for S3 Copying Tests
# This Makefile provides targets for running comprehensive S3 copying tests

# Default values
SEAWEEDFS_BINARY ?= weed
S3_PORT ?= 8333
FILER_PORT ?= 8888
VOLUME_PORT ?= 8080
MASTER_PORT ?= 9333
TEST_TIMEOUT ?= 10m
BUCKET_PREFIX ?= test-copying-
ACCESS_KEY ?= some_access_key1
SECRET_KEY ?= some_secret_key1
VOLUME_MAX_SIZE_MB ?= 50

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
	@echo "SeaweedFS S3 Copying Tests"
	@echo ""
	@echo "Available targets:"
	@echo "  test-basic    - Run basic S3 put/get tests first"
	@echo "  test          - Run all S3 copying tests"
	@echo "  test-quick    - Run quick tests only"
	@echo "  test-full     - Run full test suite including large files"
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
	@echo "$(YELLOW)Starting SeaweedFS server...$(NC)"
	@pkill -f "weed master" || true
	@pkill -f "weed volume" || true
	@pkill -f "weed filer" || true
	@pkill -f "weed s3" || true
	@pkill -f "weed mini" || true
	@sleep 2
	
	# Create necessary directories
	@mkdir -p /tmp/seaweedfs-test-copying
	
	# Start weed mini
	@echo "Starting weed mini with dir=/tmp/seaweedfs-test-copying"
	@export AWS_ACCESS_KEY_ID=$(ACCESS_KEY) && \
	export AWS_SECRET_ACCESS_KEY=$(SECRET_KEY) && \
	nohup $(SEAWEEDFS_BINARY) mini \
		-dir=/tmp/seaweedfs-test-copying \
		-master.volumeSizeLimitMB=$(VOLUME_MAX_SIZE_MB) \
		-s3.port=$(S3_PORT) \
		-filer.port=$(FILER_PORT) \
		-master.port=$(MASTER_PORT) \
		-volume.port=$(VOLUME_PORT) \
		-ip=127.0.0.1 \
		> /tmp/seaweedfs-mini.log 2>&1 &
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
	
	# Additional wait for components
	@echo "$(YELLOW)Waiting for components to be ready...$(NC)"
	@sleep 2
	@echo "$(GREEN)SeaweedFS server started successfully$(NC)"
	@echo "Mini Log: /tmp/seaweedfs-mini.log"
	@echo "Master: http://localhost:$(MASTER_PORT)"
	@echo "Volume: http://localhost:$(VOLUME_PORT)"
	@echo "Filer: http://localhost:$(FILER_PORT)"
	@echo "S3: http://localhost:$(S3_PORT)"
	@echo "Volume Max Size: $(VOLUME_MAX_SIZE_MB)MB"

stop-seaweedfs:
	@echo "$(YELLOW)Stopping SeaweedFS server...$(NC)"
	@pkill -f "weed master" || true
	@pkill -f "weed volume" || true
	@pkill -f "weed filer" || true
	@pkill -f "weed s3" || true
	@sleep 2
	@echo "$(GREEN)SeaweedFS server stopped$(NC)"

clean:
	@echo "$(YELLOW)Cleaning up test artifacts...$(NC)"
	@rm -rf /tmp/seaweedfs-test-copying-*
	@rm -f /tmp/seaweedfs-*.log
	@rm -f /tmp/seaweedfs-s3.json
	@echo "$(GREEN)Cleanup completed$(NC)"

test-basic: check-binary
	@echo "$(YELLOW)Running basic S3 put/get tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting basic tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestBasic" ./test/s3/copying || (echo "$(RED)Basic tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)Basic tests completed successfully!$(NC)"

test: test-basic
	@echo "$(YELLOW)Running S3 copying tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "Test.*" ./test/s3/copying || (echo "$(RED)Tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)All tests completed successfully!$(NC)"

test-quick: check-binary
	@echo "$(YELLOW)Running quick S3 copying tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting quick tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestObjectCopy|TestCopyObjectIf" ./test/s3/copying || (echo "$(RED)Tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)Quick tests completed successfully!$(NC)"

test-full: check-binary
	@echo "$(YELLOW)Running full S3 copying test suite...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting full test suite...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=30m -run "Test.*" ./test/s3/copying || (echo "$(RED)Tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)Full test suite completed successfully!$(NC)"

test-multipart: check-binary
	@echo "$(YELLOW)Running multipart copying tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting multipart tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestMultipart" ./test/s3/copying || (echo "$(RED)Tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)Multipart tests completed successfully!$(NC)"

test-conditional: check-binary
	@echo "$(YELLOW)Running conditional copying tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@echo "$(GREEN)Starting conditional tests...$(NC)"
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=$(TEST_TIMEOUT) -run "TestCopyObjectIf" ./test/s3/copying || (echo "$(RED)Tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)Conditional tests completed successfully!$(NC)"

# Debug targets
debug-logs:
	@echo "$(YELLOW)=== Master Log ===$(NC)"
	@tail -n 50 /tmp/seaweedfs-master.log || echo "No master log found"
	@echo "$(YELLOW)=== Volume Log ===$(NC)"
	@tail -n 50 /tmp/seaweedfs-volume.log || echo "No volume log found"
	@echo "$(YELLOW)=== Filer Log ===$(NC)"
	@tail -n 50 /tmp/seaweedfs-filer.log || echo "No filer log found"
	@echo "$(YELLOW)=== S3 Log ===$(NC)"
	@tail -n 50 /tmp/seaweedfs-s3.log || echo "No S3 log found"

debug-status:
	@echo "$(YELLOW)=== Process Status ===$(NC)"
	@ps aux | grep -E "(weed|seaweedfs)" | grep -v grep || echo "No SeaweedFS processes found"
	@echo "$(YELLOW)=== Port Status ===$(NC)"
	@netstat -an | grep -E "($(MASTER_PORT)|$(VOLUME_PORT)|$(FILER_PORT)|$(S3_PORT))" || echo "No ports in use"

# Manual test targets for development
manual-start: start-seaweedfs
	@echo "$(GREEN)SeaweedFS is now running for manual testing$(NC)"
	@echo "Run 'make manual-stop' when finished"

manual-stop: stop-seaweedfs clean

# CI/CD targets
ci-test: test-quick

# Benchmark targets
benchmark: check-binary
	@echo "$(YELLOW)Running S3 copying benchmarks...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=30m -bench=. -run=Benchmark ./test/s3/copying || (echo "$(RED)Benchmarks failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)Benchmarks completed!$(NC)"

# Stress test
stress: check-binary
	@echo "$(YELLOW)Running S3 copying stress tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=60m -run="TestMultipartCopyMultipleSizes" -count=10 ./test/s3/copying || (echo "$(RED)Stress tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)Stress tests completed!$(NC)"

# Performance test with larger files
perf: check-binary
	@echo "$(YELLOW)Running S3 copying performance tests...$(NC)"
	@$(MAKE) start-seaweedfs
	@sleep 5
	@cd $(SEAWEEDFS_ROOT) && go test -v -timeout=60m -run="TestMultipartCopyMultipleSizes" ./test/s3/copying || (echo "$(RED)Performance tests failed$(NC)" && $(MAKE) stop-seaweedfs && exit 1)
	@$(MAKE) stop-seaweedfs
	@echo "$(GREEN)Performance tests completed!$(NC)" 