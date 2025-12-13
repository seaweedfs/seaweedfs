# S3 Filer Group Test Makefile
# This Makefile provides targets for running S3 filer group integration tests
# These tests verify that S3 bucket operations work correctly when a filer group is configured.

.PHONY: help build-weed check-deps start-server stop-server test test-with-server clean logs health-check

# Configuration
WEED_BINARY := ../../../weed/weed_binary
S3_PORT := 8333
MASTER_PORT := 9333
VOLUME_PORT := 8080
FILER_PORT := 8888
TEST_TIMEOUT := 10m
TEST_PATTERN := Test

# Filer group configuration
FILER_GROUP := testgroup

# Default target
help:
	@echo "S3 Filer Group Test Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  help                    - Show this help message"
	@echo "  build-weed             - Build the SeaweedFS binary"
	@echo "  check-deps             - Check dependencies and build binary if needed"
	@echo "  start-server           - Start SeaweedFS server with filer group"
	@echo "  stop-server            - Stop SeaweedFS server"
	@echo "  test                   - Run filer group tests (server must be running)"
	@echo "  test-with-server       - Start server, run tests, stop server"
	@echo "  logs                   - Show server logs"
	@echo "  clean                  - Clean up test artifacts and stop server"
	@echo "  health-check           - Check if server is accessible"
	@echo ""
	@echo "Configuration:"
	@echo "  S3_PORT=${S3_PORT}"
	@echo "  FILER_GROUP=${FILER_GROUP}"
	@echo "  TEST_TIMEOUT=${TEST_TIMEOUT}"

# Build the SeaweedFS binary
build-weed:
	@echo "Building SeaweedFS binary..."
	@cd ../../../weed && go build -o weed_binary .
	@chmod +x $(WEED_BINARY)
	@echo "✅ SeaweedFS binary built at $(WEED_BINARY)"

check-deps: build-weed
	@echo "Checking dependencies..."
	@command -v go >/dev/null 2>&1 || (echo "Go is required but not installed" && exit 1)
	@echo "Go version: $$(go version)"
	@test -f $(WEED_BINARY) || (echo "SeaweedFS binary not found at $(WEED_BINARY)" && exit 1)
	@go list -m github.com/aws/aws-sdk-go-v2 >/dev/null 2>&1 || (echo "AWS SDK Go v2 not found. Run 'go mod tidy'." && exit 1)
	@go list -m github.com/stretchr/testify >/dev/null 2>&1 || (echo "Testify not found. Run 'go mod tidy'." && exit 1)
	@echo "✅ All dependencies are available"

# Start SeaweedFS server with filer group configured
start-server: check-deps
	@echo "Starting SeaweedFS server with filer group: $(FILER_GROUP)..."
	@rm -f weed-server.pid
	@mkdir -p ./test-volume-data
	@if netstat -tlnp 2>/dev/null | grep $(S3_PORT) >/dev/null; then \
		echo "⚠️  Port $(S3_PORT) is already in use"; \
		exit 1; \
	fi
	@echo "Launching SeaweedFS server with filer group $(FILER_GROUP)..."
	@$(WEED_BINARY) server \
		-debug \
		-s3 \
		-s3.port=$(S3_PORT) \
		-s3.allowDeleteBucketNotEmpty=true \
		-s3.config=../../../docker/compose/s3.json \
		-filer \
		-filer.maxMB=64 \
		-filer.group=$(FILER_GROUP) \
		-master.volumeSizeLimitMB=50 \
		-volume.max=100 \
		-dir=./test-volume-data \
		-volume.preStopSeconds=1 \
		-metricsPort=9325 \
		> weed-test.log 2>&1 & echo $$! > weed-server.pid
	@echo "Server PID: $$(cat weed-server.pid 2>/dev/null || echo 'PID file not found')"
	@echo "Waiting for server to start (up to 90 seconds)..."
	@for i in $$(seq 1 90); do \
		if curl -s http://localhost:$(S3_PORT) >/dev/null 2>&1; then \
			echo "✅ SeaweedFS server started successfully on port $(S3_PORT) with filer group $(FILER_GROUP)"; \
			exit 0; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "⚠️  Server taking longer than expected (30s), checking logs..."; \
			if [ -f weed-test.log ]; then \
				tail -20 weed-test.log; \
			fi; \
		fi; \
		sleep 1; \
	done; \
	echo "❌ Server failed to start within 90 seconds"; \
	if [ -f weed-test.log ]; then \
		cat weed-test.log; \
	fi; \
	exit 1

# Stop SeaweedFS server
stop-server:
	@echo "Stopping SeaweedFS server..."
	@if [ -f weed-server.pid ]; then \
		SERVER_PID=$$(cat weed-server.pid); \
		echo "Killing server PID $$SERVER_PID"; \
		if ps -p $$SERVER_PID >/dev/null 2>&1; then \
			kill -TERM $$SERVER_PID 2>/dev/null || true; \
			sleep 2; \
			if ps -p $$SERVER_PID >/dev/null 2>&1; then \
				echo "Process still running, sending KILL signal..."; \
				kill -KILL $$SERVER_PID 2>/dev/null || true; \
				sleep 1; \
			fi; \
		else \
			echo "Process $$SERVER_PID not found (already stopped)"; \
		fi; \
		rm -f weed-server.pid; \
	else \
		echo "No PID file found"; \
	fi
	@echo "✅ SeaweedFS server stopped"

# Show server logs
logs:
	@if test -f weed-test.log; then \
		echo "=== SeaweedFS Server Logs ==="; \
		tail -f weed-test.log; \
	else \
		echo "No log file found. Server may not be running."; \
	fi

# Run filer group tests (assumes server is already running)
test: check-deps
	@echo "Running filer group S3 tests..."
	@FILER_GROUP=$(FILER_GROUP) S3_ENDPOINT=http://localhost:$(S3_PORT) MASTER_ADDRESS=localhost:$(MASTER_PORT) \
		go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" .
	@echo "✅ Filer group tests completed"

# Run tests with automatic server management
test-with-server: start-server
	@echo "Server started successfully, running filer group tests..."
	@echo "Test pattern: $(TEST_PATTERN)"
	@echo "Test timeout: $(TEST_TIMEOUT)"
	@trap "$(MAKE) stop-server" EXIT; \
	$(MAKE) test || (echo "❌ Tests failed, showing server logs:" && echo "=== Last 50 lines of server logs ===" && tail -50 weed-test.log && echo "=== End of server logs ===" && exit 1)
	@$(MAKE) stop-server
	@echo "✅ Tests completed and server stopped"

# Clean up test artifacts
clean:
	@echo "Cleaning up test artifacts..."
	@$(MAKE) stop-server
	@rm -f weed-test*.log weed-server.pid
	@rm -rf test-volume-data/
	@go clean -testcache
	@echo "✅ Cleanup completed"

# Quick health check
health-check:
	@echo "Running health check..."
	@curl -s http://localhost:$(S3_PORT) >/dev/null 2>&1 && echo "✅ S3 API is accessible" || echo "❌ S3 API is not accessible"
	@curl -s http://localhost:9325/metrics >/dev/null 2>&1 && echo "✅ Metrics endpoint is accessible" || echo "❌ Metrics endpoint is not accessible"
