# Makefile for S3 Parquet Integration Tests
# This Makefile provides targets for running comprehensive S3 Parquet tests with PyArrow

# Default values
SEAWEEDFS_BINARY ?= weed
S3_PORT ?= 8333
TEST_TIMEOUT ?= 15m
ACCESS_KEY ?= some_access_key1
SECRET_KEY ?= some_secret_key1
BUCKET_NAME ?= test-parquet-bucket
ENABLE_SSE_S3 ?= false

# Python configuration
PYTHON ?= python3
VENV_DIR ?= .venv
PYTHON_TEST_SCRIPT ?= s3_parquet_test.py

# Test directory
TEST_DIR := $(shell pwd)
SEAWEEDFS_ROOT := $(shell cd ../../../ && pwd)

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m # No Color

.PHONY: all build-weed check-binary check-python ci-test clean debug-logs debug-status help manual-start manual-stop setup-python start-seaweedfs start-seaweedfs-ci stop-seaweedfs stop-seaweedfs-safe test test-cross-fs test-cross-fs-with-server test-implicit-dir test-implicit-dir-with-server test-native-s3 test-native-s3-with-server test-native-s3-with-sse test-quick test-sse-s3-compat test-with-server

all: test

# Build SeaweedFS binary (GitHub Actions compatible)
build-weed:
	@echo "Building SeaweedFS binary..."
	@cd $(SEAWEEDFS_ROOT)/weed && go install -buildvcs=false
	@echo "✅ SeaweedFS binary built successfully"

help:
	@echo "SeaweedFS S3 Parquet Integration Tests"
	@echo ""
	@echo "Available targets:"
	@echo "  test              - Run full S3 Parquet integration tests (small and large files)"
	@echo "  test-with-server  - Run full tests with automatic server management (CI compatible)"
	@echo "  test-quick        - Run quick tests with small files only (sets TEST_QUICK=1)"
	@echo "  test-implicit-dir - Test implicit directory fix for s3fs compatibility"
	@echo "  test-implicit-dir-with-server - Test implicit directory fix with server management"
	@echo "  test-native-s3    - Test PyArrow's native S3 filesystem (assumes server running)"
	@echo "  test-native-s3-with-server - Test PyArrow's native S3 filesystem with server management"
	@echo "  test-native-s3-with-sse - Test PyArrow's native S3 with SSE-S3 encryption enabled"
	@echo "  test-cross-fs     - Test cross-filesystem compatibility (s3fs ↔ PyArrow native)"
	@echo "  test-cross-fs-with-server - Test cross-filesystem compatibility with server management"
	@echo "  test-sse-s3-compat - Comprehensive SSE-S3 compatibility test (multipart uploads)"
	@echo "  setup-python      - Setup Python virtual environment and install dependencies"
	@echo "  check-python      - Check if Python and required packages are available"
	@echo "  start-seaweedfs   - Start SeaweedFS server for testing"
	@echo "  start-seaweedfs-ci - Start SeaweedFS server (CI-safe version)"
	@echo "  stop-seaweedfs    - Stop SeaweedFS server"
	@echo "  stop-seaweedfs-safe - Stop SeaweedFS server (CI-safe version)"
	@echo "  clean             - Clean up test artifacts"
	@echo "  check-binary      - Check if SeaweedFS binary exists"
	@echo "  build-weed        - Build SeaweedFS binary"
	@echo ""
	@echo "Configuration:"
	@echo "  SEAWEEDFS_BINARY=$(SEAWEEDFS_BINARY)"
	@echo "  S3_PORT=$(S3_PORT)"
	@echo "  BUCKET_NAME=$(BUCKET_NAME)"
	@echo "  ENABLE_SSE_S3=$(ENABLE_SSE_S3)"
	@echo "  PYTHON=$(PYTHON)"

check-binary:
	@if ! command -v $(SEAWEEDFS_BINARY) > /dev/null 2>&1; then \
		echo "$(RED)Error: SeaweedFS binary '$(SEAWEEDFS_BINARY)' not found in PATH$(NC)"; \
		echo "Please build SeaweedFS first by running 'make' in the root directory"; \
		exit 1; \
	fi
	@echo "$(GREEN)SeaweedFS binary found: $$(which $(SEAWEEDFS_BINARY))$(NC)"

check-python:
	@if ! command -v $(PYTHON) > /dev/null 2>&1; then \
		echo "$(RED)Error: Python '$(PYTHON)' not found$(NC)"; \
		echo "Please install Python 3.8 or later"; \
		exit 1; \
	fi
	@echo "$(GREEN)Python found: $$(which $(PYTHON)) ($$($(PYTHON) --version))$(NC)"

setup-python: check-python
	@echo "$(YELLOW)Setting up Python virtual environment...$(NC)"
	@if [ ! -d "$(VENV_DIR)" ]; then \
		$(PYTHON) -m venv $(VENV_DIR); \
		echo "$(GREEN)Virtual environment created$(NC)"; \
	fi
	@echo "$(YELLOW)Installing Python dependencies...$(NC)"
	@$(VENV_DIR)/bin/pip install --upgrade pip > /dev/null
	@$(VENV_DIR)/bin/pip install -r requirements.txt
	@echo "$(GREEN)Python dependencies installed successfully$(NC)"

start-seaweedfs-ci: check-binary
	@echo "$(YELLOW)Starting SeaweedFS server for Parquet testing...$(NC)"
	
	# Clean up any existing processes first (CI-safe)
	@echo "Cleaning up any existing processes..."
	@if command -v lsof >/dev/null 2>&1; then \
		lsof -ti :$(S3_PORT) 2>/dev/null | head -5 | while read pid; do kill -TERM $$pid 2>/dev/null || true; done; \
	fi
	@sleep 2
	
	# Create necessary directories
	@mkdir -p /tmp/seaweedfs-test-parquet
	
	# Clean up any old server logs
	@rm -f /tmp/seaweedfs-parquet-*.log || true
	
	# Start weed mini with embedded S3
	@echo "Starting weed mini with embedded S3..."
	@if [ "$(ENABLE_SSE_S3)" = "true" ]; then \
		echo "  SSE-S3 encryption: ENABLED"; \
		printf '{"identities":[{"name":"%s","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write"]}],"buckets":[{"name":"$(BUCKET_NAME)","encryption":{"sseS3":{"enabled":true}}}]}' "$(ACCESS_KEY)" "$(ACCESS_KEY)" "$(SECRET_KEY)" > /tmp/seaweedfs-parquet-s3.json; \
	else \
		echo "  SSE-S3 encryption: DISABLED"; \
		printf '{"identities":[{"name":"%s","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","Write"]}]}' "$(ACCESS_KEY)" "$(ACCESS_KEY)" "$(SECRET_KEY)" > /tmp/seaweedfs-parquet-s3.json; \
	fi
	@$(SEAWEEDFS_BINARY) mini \
		-dir=/tmp/seaweedfs-test-parquet \
		-ip.bind=0.0.0.0 \
		-s3.port=$(S3_PORT) \
		-s3.config=/tmp/seaweedfs-parquet-s3.json \
		> /tmp/seaweedfs-parquet-mini.log 2>&1 & echo $$! > /tmp/weed-mini.pid
	
	@echo "Checking S3 service is ready..."
	@for i in $$(seq 1 60); do \
		if curl -s http://localhost:$(S3_PORT) > /dev/null 2>&1; then \
			echo "✅ S3 service is ready"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "ERROR S3 service failed to start within 60 seconds"; \
	tail -20 /tmp/seaweedfs-parquet-mini.log 2>/dev/null || echo "No startup log available"; \
	exit 1

start-seaweedfs: check-binary
	@echo "$(YELLOW)Starting SeaweedFS server for Parquet testing...$(NC)"
	@# Use port-based cleanup for consistency and safety
	@echo "Cleaning up any existing processes..."
	@lsof -ti :$(S3_PORT) 2>/dev/null | xargs -r kill -TERM || true
	@sleep 2
	@$(MAKE) start-seaweedfs-ci

stop-seaweedfs:
	@echo "$(YELLOW)Stopping SeaweedFS server...$(NC)"
	@# Use port-based cleanup for consistency and safety
	@if [ -f /tmp/weed-mini.pid ]; then \
		echo "Stopping weed mini..."; \
		kill $$(cat /tmp/weed-mini.pid) || true; \
		rm -f /tmp/weed-mini.pid; \
	fi
	@lsof -ti :$(S3_PORT) 2>/dev/null | xargs -r kill -TERM || true
	@sleep 2
	@echo "$(GREEN)SeaweedFS server stopped$(NC)"

# CI-safe server stop that's more conservative
stop-seaweedfs-safe:
	@echo "$(YELLOW)Safely stopping SeaweedFS server...$(NC)"
	@# Use port-based cleanup which is safer in CI
	@if command -v lsof >/dev/null 2>&1; then \
		echo "Using lsof for port-based cleanup..."; \
		lsof -ti :$(S3_PORT) 2>/dev/null | head -5 | while read pid; do kill -TERM $$pid 2>/dev/null || true; done; \
	else \
		echo "lsof not available, using netstat approach..."; \
		netstat -tlnp 2>/dev/null | grep :$(S3_PORT) | awk '{print $$7}' | cut -d/ -f1 | head -5 | while read pid; do [ "$$pid" != "-" ] && kill -TERM $$pid 2>/dev/null || true; done; \
	fi
	@sleep 2
	@echo "$(GREEN)SeaweedFS server safely stopped$(NC)"

clean:
	@echo "$(YELLOW)Cleaning up Parquet test artifacts...$(NC)"
	@rm -rf /tmp/seaweedfs-test-parquet-*
	@rm -f /tmp/seaweedfs-parquet-*.log
	@rm -f /tmp/seaweedfs-parquet-s3.json
	@rm -f s3_parquet_test_errors_*.log
	@rm -rf $(VENV_DIR)
	@echo "$(GREEN)Parquet test cleanup completed$(NC)"

# Test with automatic server management (GitHub Actions compatible)
test-with-server: build-weed setup-python
	@echo "🚀 Starting Parquet integration tests with automated server management..."
	@echo "Starting SeaweedFS cluster..."
	@if $(MAKE) start-seaweedfs-ci > weed-test.log 2>&1; then \
		echo "✅ SeaweedFS cluster started successfully"; \
		echo "Running Parquet integration tests..."; \
		trap '$(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe || true' EXIT; \
		S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
		S3_ACCESS_KEY=$(ACCESS_KEY) \
		S3_SECRET_KEY=$(SECRET_KEY) \
		BUCKET_NAME=$(BUCKET_NAME) \
		$(VENV_DIR)/bin/$(PYTHON) $(PYTHON_TEST_SCRIPT) || exit 1; \
		echo "✅ All tests completed successfully"; \
	else \
		echo "❌ Failed to start SeaweedFS cluster"; \
		echo "=== Server startup logs ==="; \
		tail -100 weed-test.log 2>/dev/null || echo "No startup log available"; \
		echo "=== System information ==="; \
		ps aux | grep -E "weed|make" | grep -v grep || echo "No relevant processes found"; \
		exit 1; \
	fi

# Run tests assuming SeaweedFS is already running
test: setup-python
	@echo "$(YELLOW)Running Parquet integration tests...$(NC)"
	@echo "$(YELLOW)Assuming SeaweedFS is already running on localhost:$(S3_PORT)$(NC)"
	@S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
	 S3_ACCESS_KEY=$(ACCESS_KEY) \
	 S3_SECRET_KEY=$(SECRET_KEY) \
	 BUCKET_NAME=$(BUCKET_NAME) \
	 $(VENV_DIR)/bin/$(PYTHON) $(PYTHON_TEST_SCRIPT)

# Run quick tests with small files only
test-quick: setup-python
	@echo "$(YELLOW)Running quick Parquet tests (small files only)...$(NC)"
	@echo "$(YELLOW)Assuming SeaweedFS is already running on localhost:$(S3_PORT)$(NC)"
	@S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
	 S3_ACCESS_KEY=$(ACCESS_KEY) \
	 S3_SECRET_KEY=$(SECRET_KEY) \
	 BUCKET_NAME=$(BUCKET_NAME) \
	 TEST_QUICK=1 \
	 $(VENV_DIR)/bin/$(PYTHON) $(PYTHON_TEST_SCRIPT)

# Test implicit directory fix for s3fs compatibility
test-implicit-dir: setup-python
	@echo "$(YELLOW)Running implicit directory fix tests...$(NC)"
	@echo "$(YELLOW)Assuming SeaweedFS is already running on localhost:$(S3_PORT)$(NC)"
	@S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
	 S3_ACCESS_KEY=$(ACCESS_KEY) \
	 S3_SECRET_KEY=$(SECRET_KEY) \
	 BUCKET_NAME=test-implicit-dir \
	 $(VENV_DIR)/bin/$(PYTHON) test_implicit_directory_fix.py

# Test implicit directory fix with automatic server management
test-implicit-dir-with-server: build-weed setup-python
	@echo "🚀 Starting implicit directory fix tests with automated server management..."
	@echo "Starting SeaweedFS cluster..."
	@if $(MAKE) start-seaweedfs-ci > weed-test.log 2>&1; then \
		echo "✅ SeaweedFS cluster started successfully"; \
		echo "Running implicit directory fix tests..."; \
		trap '$(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe || true' EXIT; \
		S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
		S3_ACCESS_KEY=$(ACCESS_KEY) \
		S3_SECRET_KEY=$(SECRET_KEY) \
		BUCKET_NAME=test-implicit-dir \
		$(VENV_DIR)/bin/$(PYTHON) test_implicit_directory_fix.py || exit 1; \
		echo "✅ All tests completed successfully"; \
	else \
		echo "❌ Failed to start SeaweedFS cluster"; \
		echo "=== Server startup logs ==="; \
		tail -100 weed-test.log 2>/dev/null || echo "No startup log available"; \
		exit 1; \
	fi

# Debug targets
debug-logs:
	@echo "$(YELLOW)=== Mini Log ===$(NC)"
	@tail -n 50 /tmp/seaweedfs-parquet-mini.log || echo "No mini log found"

debug-status:
	@echo "$(YELLOW)=== Process Status ===$(NC)"
	@ps aux | grep -E "(weed|seaweedfs)" | grep -v grep || echo "No SeaweedFS processes found"
	@echo "$(YELLOW)=== Port Status ===$(NC)"
	@netstat -an | grep -E "($(S3_PORT))" || echo "No ports in use"

# Manual test targets for development
manual-start: start-seaweedfs
	@echo "$(GREEN)SeaweedFS with S3 is now running for manual testing$(NC)"
	@echo "You can now run Parquet tests manually"
	@echo "Run 'make manual-stop' when finished"

manual-stop: stop-seaweedfs clean

# Test PyArrow's native S3 filesystem
test-native-s3: setup-python
	@echo "$(YELLOW)Running PyArrow native S3 filesystem tests...$(NC)"
	@echo "$(YELLOW)Assuming SeaweedFS is already running on localhost:$(S3_PORT)$(NC)"
	@S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
	 S3_ACCESS_KEY=$(ACCESS_KEY) \
	 S3_SECRET_KEY=$(SECRET_KEY) \
	 BUCKET_NAME=$(BUCKET_NAME) \
	 $(VENV_DIR)/bin/$(PYTHON) test_pyarrow_native_s3.py

# Test PyArrow's native S3 filesystem with automatic server management
test-native-s3-with-server: build-weed setup-python
	@echo "🚀 Starting PyArrow native S3 filesystem tests with automated server management..."
	@echo "Starting SeaweedFS cluster..."
	@if $(MAKE) start-seaweedfs-ci > weed-test.log 2>&1; then \
		echo "✅ SeaweedFS cluster started successfully"; \
		echo "Running PyArrow native S3 filesystem tests..."; \
		trap '$(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe || true' EXIT; \
		S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
		S3_ACCESS_KEY=$(ACCESS_KEY) \
		S3_SECRET_KEY=$(SECRET_KEY) \
		BUCKET_NAME=$(BUCKET_NAME) \
		$(VENV_DIR)/bin/$(PYTHON) test_pyarrow_native_s3.py || exit 1; \
		echo "✅ All tests completed successfully"; \
	else \
		echo "❌ Failed to start SeaweedFS cluster"; \
		echo "=== Server startup logs ==="; \
		tail -100 weed-test.log 2>/dev/null || echo "No startup log available"; \
		exit 1; \
	fi

# Test cross-filesystem compatibility (s3fs ↔ PyArrow native S3)
test-cross-fs: setup-python
	@echo "$(YELLOW)Running cross-filesystem compatibility tests...$(NC)"
	@echo "$(YELLOW)Assuming SeaweedFS is already running on localhost:$(S3_PORT)$(NC)"
	@S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
	 S3_ACCESS_KEY=$(ACCESS_KEY) \
	 S3_SECRET_KEY=$(SECRET_KEY) \
	 BUCKET_NAME=$(BUCKET_NAME) \
	 $(VENV_DIR)/bin/$(PYTHON) test_cross_filesystem_compatibility.py

# Test cross-filesystem compatibility with automatic server management
test-cross-fs-with-server: build-weed setup-python
	@echo "🚀 Starting cross-filesystem compatibility tests with automated server management..."
	@echo "Starting SeaweedFS cluster..."
	@if $(MAKE) start-seaweedfs-ci > weed-test.log 2>&1; then \
		echo "✅ SeaweedFS cluster started successfully"; \
		echo "Running cross-filesystem compatibility tests..."; \
		trap '$(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe || true' EXIT; \
		S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
		S3_ACCESS_KEY=$(ACCESS_KEY) \
		S3_SECRET_KEY=$(SECRET_KEY) \
		BUCKET_NAME=$(BUCKET_NAME) \
		$(VENV_DIR)/bin/$(PYTHON) test_cross_filesystem_compatibility.py || exit 1; \
		echo "✅ All tests completed successfully"; \
	else \
		echo "❌ Failed to start SeaweedFS cluster"; \
		echo "=== Server startup logs ==="; \
		tail -100 weed-test.log 2>/dev/null || echo "No startup log available"; \
		exit 1; \
	fi

# Test PyArrow's native S3 filesystem compatibility with SSE-S3 enabled backend
# (For encryption-specific validation, use test-sse-s3-compat)
test-native-s3-with-sse: build-weed setup-python
	@echo "🚀 Testing PyArrow native S3 compatibility with SSE-S3 enabled backend..."
	@echo "Starting SeaweedFS cluster with SSE-S3 enabled..."
	@if $(MAKE) start-seaweedfs-ci ENABLE_SSE_S3=true > weed-test-sse.log 2>&1; then \
		echo "✅ SeaweedFS cluster started successfully with SSE-S3"; \
		echo "Running PyArrow native S3 filesystem tests with SSE-S3..."; \
		trap '$(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe || true' EXIT; \
		S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
		S3_ACCESS_KEY=$(ACCESS_KEY) \
		S3_SECRET_KEY=$(SECRET_KEY) \
		BUCKET_NAME=$(BUCKET_NAME) \
		$(VENV_DIR)/bin/$(PYTHON) test_pyarrow_native_s3.py || exit 1; \
		echo "✅ All SSE-S3 tests completed successfully"; \
	else \
		echo "❌ Failed to start SeaweedFS cluster with SSE-S3"; \
		echo "=== Server startup logs ==="; \
		tail -100 weed-test-sse.log 2>/dev/null || echo "No startup log available"; \
		exit 1; \
	fi

# Comprehensive SSE-S3 compatibility test
test-sse-s3-compat: build-weed setup-python
	@echo "🚀 Starting comprehensive SSE-S3 compatibility tests..."
	@echo "Starting SeaweedFS cluster with SSE-S3 enabled..."
	@if $(MAKE) start-seaweedfs-ci ENABLE_SSE_S3=true > weed-test-sse-compat.log 2>&1; then \
		echo "✅ SeaweedFS cluster started successfully with SSE-S3"; \
		echo "Running comprehensive SSE-S3 compatibility tests..."; \
		trap '$(MAKE) -C $(TEST_DIR) stop-seaweedfs-safe || true' EXIT; \
		S3_ENDPOINT_URL=http://localhost:$(S3_PORT) \
		S3_ACCESS_KEY=$(ACCESS_KEY) \
		S3_SECRET_KEY=$(SECRET_KEY) \
		BUCKET_NAME=$(BUCKET_NAME) \
		$(VENV_DIR)/bin/$(PYTHON) test_sse_s3_compatibility.py || exit 1; \
		echo "✅ All SSE-S3 compatibility tests completed successfully"; \
	else \
		echo "❌ Failed to start SeaweedFS cluster with SSE-S3"; \
		echo "=== Server startup logs ==="; \
		tail -100 weed-test-sse-compat.log 2>/dev/null || echo "No startup log available"; \
		exit 1; \
	fi

# CI/CD targets
ci-test: test-with-server

