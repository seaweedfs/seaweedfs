# S3 API SSE-C Test Makefile
# This Makefile provides comprehensive targets for running S3 SSE-C encryption tests

.PHONY: help build-weed start-server stop-server test-sse-c test-sse-c-quick test-sse-c-comprehensive test-with-server clean logs check-deps

# Configuration
WEED_BINARY := ../../../weed/weed_binary
S3_PORT := 8333
MASTER_PORT := 9333
VOLUME_PORT := 8080
FILER_PORT := 8888
TEST_TIMEOUT := 10m
TEST_PATTERN := TestSSEC

# Default target
help:
	@echo "S3 API SSE-C Test Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  help                     - Show this help message"
	@echo "  build-weed              - Build the SeaweedFS binary"
	@echo "  check-deps              - Check dependencies and build binary if needed"
	@echo "  start-server            - Start SeaweedFS server for testing"
	@echo "  stop-server             - Stop SeaweedFS server"
	@echo "  test-sse-c              - Run all SSE-C tests"
	@echo "  test-sse-c-quick        - Run core SSE-C tests only"
	@echo "  test-sse-c-comprehensive - Run comprehensive SSE-C tests"
	@echo "  test-with-server        - Start server, run tests, stop server"
	@echo "  logs                    - Show server logs"
	@echo "  clean                   - Clean up test artifacts and stop server"
	@echo "  benchmark               - Run performance benchmarks"
	@echo ""
	@echo "Configuration:"
	@echo "  S3_PORT=${S3_PORT}"
	@echo "  TEST_TIMEOUT=${TEST_TIMEOUT}"
	@echo "  TEST_PATTERN=${TEST_PATTERN}"

# Build the SeaweedFS binary
build-weed:
	@echo "Building SeaweedFS binary..."
	@cd ../../../weed && go build -o weed_binary .
	@chmod +x $(WEED_BINARY)
	@echo "✅ SeaweedFS binary built at $(WEED_BINARY)"

# Check dependencies
check-deps: build-weed
	@echo "Checking dependencies..."
	@if [ ! -f $(WEED_BINARY) ]; then \
		echo "❌ SeaweedFS binary not found, building..."; \
		$(MAKE) build-weed; \
	else \
		echo "✅ SeaweedFS binary found at $(WEED_BINARY)"; \
	fi

# Start SeaweedFS server
start-server: check-deps
	@echo "Starting SeaweedFS server for SSE-C testing..."
	@set +e; \
	if pgrep -f "weed.*server" > /dev/null 2>&1 || pgrep -f "weed_binary" > /dev/null 2>&1; then \
		echo "⚠️  SeaweedFS server already running, stopping first..."; \
		$(MAKE) stop-server 2>/dev/null || true; \
		sleep 3; \
	fi; \
	mkdir -p ./test-volume-data; \
	echo "Starting server with S3 API on port $(S3_PORT)..."; \
	$(WEED_BINARY) server -s3 -s3.port=$(S3_PORT) -filer -master.volumeSizeLimitMB=100 -volume.max=100 -dir=./test-volume-data > weed-test.log 2>&1 & \
	echo $$! > weed-server.pid; \
	echo "⏳ Waiting for server to start..."; \
	SERVER_READY=false; \
	for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if curl -s -f http://localhost:$(S3_PORT) >/dev/null 2>&1; then \
			echo "✅ Server is ready on port $(S3_PORT)"; \
			SERVER_READY=true; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "❌ Server failed to start within 30 seconds"; \
			echo "=== Server logs ==="; \
			cat weed-test.log 2>/dev/null || echo "No log file available"; \
			echo "=== End of server logs ==="; \
			$(MAKE) stop-server 2>/dev/null || true; \
			exit 1; \
		fi; \
		echo "Waiting for server to start... ($$i/30)"; \
		sleep 1; \
	done; \
	if [ "$$SERVER_READY" != "true" ]; then \
		echo "❌ Server startup verification failed"; \
		exit 1; \
	fi

# Stop SeaweedFS server
stop-server:
	@echo "Stopping SeaweedFS server..."
	@set +e; \
	if [ -f weed-server.pid ]; then \
		PID=$$(cat weed-server.pid 2>/dev/null || echo ""); \
		if [ -n "$$PID" ] && ps -p $$PID > /dev/null 2>&1; then \
			echo "Terminating process $$PID..."; \
			kill -TERM $$PID 2>/dev/null || true; \
			for i in 1 2 3 4 5; do \
				if ! ps -p $$PID > /dev/null 2>&1; then break; fi; \
				echo "Waiting for process to terminate... ($$i/5)"; \
				sleep 1; \
			done; \
			if ps -p $$PID > /dev/null 2>&1; then \
				echo "Force killing process $$PID..."; \
				kill -KILL $$PID 2>/dev/null || true; \
				sleep 1; \
			fi; \
		fi; \
		rm -f weed-server.pid 2>/dev/null || true; \
	fi; \
	pkill -f "weed.*server" 2>/dev/null || true; \
	pkill -f "weed_binary" 2>/dev/null || true; \
	sleep 1; \
	echo "✅ Server stopped"

# Core SSE-C tests (quick)
test-sse-c-quick: check-deps
	@echo "Running core SSE-C tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestSSECBasicEncryption|TestSSECHeadObject|TestSSECCopyOperations" .
	@echo "✅ Core SSE-C tests completed"

# All SSE-C tests
test-sse-c: check-deps
	@echo "Running all SSE-C tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" .
	@echo "✅ All SSE-C tests completed"

# Comprehensive SSE-C tests (all features)
test-sse-c-comprehensive: check-deps
	@echo "Running comprehensive S3 SSE-C tests..."
	@echo "=== Test Environment Info ==="
	@echo "Working directory: $$(pwd)"
	@echo "Go version: $$(go version)"
	@echo "Available test files:"
	@ls -la *.go 2>/dev/null || echo "No Go files found"
	@echo "=== Starting Tests ==="
	@go test -v -timeout=$(TEST_TIMEOUT) . || (echo "❌ Test execution failed, environment:" && env | grep -E "(GO|PATH|HOME)" && exit 1)
	@echo "✅ Comprehensive SSE-C tests completed"

# Run tests with automatic server management
test-with-server: start-server
	@echo "Running SSE-C tests with managed server..."
	@set +e; \
	trap "$(MAKE) stop-server 2>/dev/null || true; exit 0" EXIT INT TERM; \
	$(MAKE) test-sse-c-comprehensive; \
	TEST_RESULT=$$?; \
	if [ $$TEST_RESULT -ne 0 ]; then \
		echo "❌ Tests failed (exit code: $$TEST_RESULT), showing server logs:"; \
		echo "=== Last 50 lines of server logs ==="; \
		tail -50 weed-test.log 2>/dev/null || echo "No log file available"; \
		echo "=== End of server logs ==="; \
	fi; \
	$(MAKE) stop-server 2>/dev/null || true; \
	echo "✅ Server management completed"; \
	exit $$TEST_RESULT

# Show server logs
logs:
	@echo "=== Server logs ==="
	@if [ -f weed-test.log ]; then \
		cat weed-test.log; \
	else \
		echo "No log file found"; \
	fi

# Clean up
clean:
	@echo "Cleaning up test artifacts..."
	@$(MAKE) stop-server
	@rm -f weed-test.log
	@rm -f weed-server.pid
	@rm -rf ./test-volume-data
	@rm -f *.log
	@echo "✅ Cleanup completed"

# Performance benchmarks
benchmark: check-deps
	@echo "Running SSE-C benchmarks..."
	@go test -v -bench=. -benchmem -timeout=$(TEST_TIMEOUT) .
	@echo "✅ Benchmarks completed"

# Legacy targets for backward compatibility
test: test-with-server
setup: start-server

