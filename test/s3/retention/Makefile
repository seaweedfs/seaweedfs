# S3 API Retention Test Makefile
# This Makefile provides comprehensive targets for running S3 retention tests

.PHONY: help build-weed setup-server start-server stop-server test-retention test-retention-quick test-retention-comprehensive test-retention-worm test-all clean logs check-deps

# Configuration
WEED_BINARY := ../../../weed/weed_binary
S3_PORT := 8333
MASTER_PORT := 9333
VOLUME_PORT := 8080
FILER_PORT := 8888
TEST_TIMEOUT := 15m
TEST_PATTERN := TestRetention

# Default target
help:
	@echo "S3 API Retention Test Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  help                       - Show this help message"
	@echo "  build-weed                - Build the SeaweedFS binary"
	@echo "  check-deps                - Check dependencies and build binary if needed"
	@echo "  start-server              - Start SeaweedFS server for testing"
	@echo "  start-server-simple       - Start server without process cleanup (for CI)"
	@echo "  stop-server               - Stop SeaweedFS server"
	@echo "  test-retention            - Run all retention tests"
	@echo "  test-retention-quick      - Run core retention tests only"
	@echo "  test-retention-simple     - Run tests without server management"
	@echo "  test-retention-comprehensive - Run comprehensive retention tests"
	@echo "  test-retention-worm       - Run WORM integration tests"
	@echo "  test-all                  - Run all S3 API retention tests"
	@echo "  test-with-server          - Start server, run tests, stop server"
	@echo "  logs                      - Show server logs"
	@echo "  clean                     - Clean up test artifacts and stop server"
	@echo "  health-check              - Check if server is accessible"
	@echo ""
	@echo "Configuration:"
	@echo "  S3_PORT=${S3_PORT}"
	@echo "  TEST_TIMEOUT=${TEST_TIMEOUT}"

# Build the SeaweedFS binary
build-weed:
	@echo "Building SeaweedFS binary..."
	@cd ../../../weed && go build -o weed_binary .
	@chmod +x $(WEED_BINARY)
	@echo "✅ SeaweedFS binary built at $(WEED_BINARY)"

check-deps: build-weed
	@echo "Checking dependencies..."
	@echo "🔍 DEBUG: Checking Go installation..."
	@command -v go >/dev/null 2>&1 || (echo "Go is required but not installed" && exit 1)
	@echo "🔍 DEBUG: Go version: $$(go version)"
	@echo "🔍 DEBUG: Checking binary at $(WEED_BINARY)..."
	@test -f $(WEED_BINARY) || (echo "SeaweedFS binary not found at $(WEED_BINARY)" && exit 1)
	@echo "🔍 DEBUG: Binary size: $$(ls -lh $(WEED_BINARY) | awk '{print $$5}')"
	@echo "🔍 DEBUG: Binary permissions: $$(ls -la $(WEED_BINARY) | awk '{print $$1}')"
	@echo "🔍 DEBUG: Checking Go module dependencies..."
	@go list -m github.com/aws/aws-sdk-go-v2 >/dev/null 2>&1 || (echo "AWS SDK Go v2 not found. Run 'go mod tidy'." && exit 1)
	@go list -m github.com/stretchr/testify >/dev/null 2>&1 || (echo "Testify not found. Run 'go mod tidy'." && exit 1)
	@echo "✅ All dependencies are available"

# Start SeaweedFS server for testing
start-server: check-deps
	@echo "Starting SeaweedFS server..."
	@echo "🔍 DEBUG: Current working directory: $$(pwd)"
	@echo "🔍 DEBUG: Checking for existing weed processes..."
	@ps aux | grep weed | grep -v grep || echo "No existing weed processes found"
	@echo "🔍 DEBUG: Cleaning up any existing PID file..."
	@rm -f weed-server.pid
	@echo "🔍 DEBUG: Checking for port conflicts..."
	@if netstat -tlnp 2>/dev/null | grep $(S3_PORT) >/dev/null; then \
		echo "⚠️  Port $(S3_PORT) is already in use, trying to find the process..."; \
		netstat -tlnp 2>/dev/null | grep $(S3_PORT) || true; \
	else \
		echo "✅ Port $(S3_PORT) is available"; \
	fi
	@echo "🔍 DEBUG: Checking binary at $(WEED_BINARY)"
	@ls -la $(WEED_BINARY) || (echo "❌ Binary not found!" && exit 1)
	@echo "🔍 DEBUG: Checking config file at ../../../docker/compose/s3.json"
	@ls -la ../../../docker/compose/s3.json || echo "⚠️  Config file not found, continuing without it"
	@echo "🔍 DEBUG: Creating volume directory..."
	@mkdir -p ./test-volume-data
	@echo "🔍 DEBUG: Launching SeaweedFS server in background..."
	@echo "🔍 DEBUG: Command: $(WEED_BINARY) server -debug -s3 -s3.port=$(S3_PORT) -s3.allowDeleteBucketNotEmpty=true -s3.config=../../../docker/compose/s3.json -filer -filer.maxMB=64 -master.volumeSizeLimitMB=50 -volume.max=100 -dir=./test-volume-data -volume.preStopSeconds=1 -metricsPort=9324"
	@$(WEED_BINARY) server \
		-debug \
		-s3 \
		-s3.port=$(S3_PORT) \
		-s3.allowDeleteBucketNotEmpty=true \
		-s3.config=../../../docker/compose/s3.json \
		-filer \
		-filer.maxMB=64 \
		-master.volumeSizeLimitMB=50 \
		-volume.max=100 \
		-dir=./test-volume-data \
		-volume.preStopSeconds=1 \
		-metricsPort=9324 \
		> weed-test.log 2>&1 & echo $$! > weed-server.pid
	@echo "🔍 DEBUG: Server PID: $$(cat weed-server.pid 2>/dev/null || echo 'PID file not found')"
	@echo "🔍 DEBUG: Checking if PID is still running..."
	@sleep 2
	@if [ -f weed-server.pid ]; then \
		SERVER_PID=$$(cat weed-server.pid); \
		ps -p $$SERVER_PID || echo "⚠️  Server PID $$SERVER_PID not found after 2 seconds"; \
	else \
		echo "⚠️  PID file not found"; \
	fi
	@echo "🔍 DEBUG: Waiting for server to start (up to 90 seconds)..."
	@for i in $$(seq 1 90); do \
		echo "🔍 DEBUG: Attempt $$i/90 - checking port $(S3_PORT)"; \
		if curl -s http://localhost:$(S3_PORT) >/dev/null 2>&1; then \
			echo "✅ SeaweedFS server started successfully on port $(S3_PORT) after $$i seconds"; \
			exit 0; \
		fi; \
		if [ $$i -eq 5 ]; then \
			echo "🔍 DEBUG: After 5 seconds, checking process and logs..."; \
			ps aux | grep weed | grep -v grep || echo "No weed processes found"; \
			if [ -f weed-test.log ]; then \
				echo "=== First server logs ==="; \
				head -20 weed-test.log; \
			fi; \
		fi; \
		if [ $$i -eq 15 ]; then \
			echo "🔍 DEBUG: After 15 seconds, checking port bindings..."; \
			netstat -tlnp 2>/dev/null | grep $(S3_PORT) || echo "Port $(S3_PORT) not bound"; \
			netstat -tlnp 2>/dev/null | grep 9333 || echo "Port 9333 not bound"; \
			netstat -tlnp 2>/dev/null | grep 8080 || echo "Port 8080 not bound"; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "⚠️  Server taking longer than expected (30s), checking logs..."; \
			if [ -f weed-test.log ]; then \
				echo "=== Recent server logs ==="; \
				tail -20 weed-test.log; \
			fi; \
		fi; \
		sleep 1; \
	done; \
	echo "❌ Server failed to start within 90 seconds"; \
	echo "🔍 DEBUG: Final process check:"; \
	ps aux | grep weed | grep -v grep || echo "No weed processes found"; \
	echo "🔍 DEBUG: Final port check:"; \
	netstat -tlnp 2>/dev/null | grep -E "(8333|9333|8080)" || echo "No ports bound"; \
	echo "=== Full server logs ==="; \
	if [ -f weed-test.log ]; then \
		cat weed-test.log; \
	else \
		echo "No log file found"; \
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
		echo "No PID file found, checking for running processes..."; \
		echo "⚠️  Skipping automatic process cleanup to avoid CI issues"; \
		echo "Note: Any remaining weed processes should be cleaned up by the CI environment"; \
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

# Core retention tests (basic functionality)
test-retention-quick: check-deps
	@echo "Running core S3 retention tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestBasicRetentionWorkflow|TestRetentionModeCompliance|TestLegalHoldWorkflow" .
	@echo "✅ Core retention tests completed"

# All retention tests (comprehensive)
test-retention: check-deps
	@echo "Running all S3 retention tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" .
	@echo "✅ All retention tests completed"

# WORM integration tests
test-retention-worm: check-deps
	@echo "Running WORM integration tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestWORM|TestRetentionExtendedAttributes|TestRetentionConcurrentOperations" .
	@echo "✅ WORM integration tests completed"

# Comprehensive retention tests (all features)
test-retention-comprehensive: check-deps
	@echo "Running comprehensive S3 retention tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetention|TestObjectLock|TestLegalHold|TestWORM" .
	@echo "✅ Comprehensive retention tests completed"

# All tests without server management
test-retention-simple: check-deps
	@echo "Running retention tests (assuming server is already running)..."
	@go test -v -timeout=$(TEST_TIMEOUT) ./...
	@echo "✅ All retention tests completed"

# Run tests with auto-managed server via TestMain
test-with-server: check-deps
	@echo "Running retention tests with auto-managed server..."
	@go test -v -timeout=$(TEST_TIMEOUT) ./...
	@echo "✅ All tests completed"

# Health check
health-check:
	@echo "Checking server health..."
	@if curl -s http://localhost:$(S3_PORT) >/dev/null 2>&1; then \
		echo "✅ Server is accessible on port $(S3_PORT)"; \
	else \
		echo "❌ Server is not accessible on port $(S3_PORT)"; \
		exit 1; \
	fi

# Clean up
clean:
	@echo "Cleaning up test artifacts..."
	@make stop-server
	@rm -f weed-test.log
	@rm -f weed-server.pid
	@rm -rf ./test-volume-data
	@echo "✅ Cleanup completed"

# Individual test targets for specific functionality
test-basic-retention:
	@echo "Running basic retention tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestBasicRetentionWorkflow" .

test-compliance-retention:
	@echo "Running compliance retention tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetentionModeCompliance" .

test-legal-hold:
	@echo "Running legal hold tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestLegalHoldWorkflow" .

test-object-lock-config:
	@echo "Running object lock configuration tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestObjectLockConfiguration" .

test-retention-versions:
	@echo "Running retention with versions tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetentionWithVersions" .

test-retention-combination:
	@echo "Running retention and legal hold combination tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetentionAndLegalHoldCombination" .

test-expired-retention:
	@echo "Running expired retention tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestExpiredRetention" .

test-retention-errors:
	@echo "Running retention error case tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetentionErrorCases" .

# WORM-specific test targets
test-worm-integration:
	@echo "Running WORM integration tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestWORMRetentionIntegration" .

test-worm-legacy:
	@echo "Running WORM legacy compatibility tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestWORMLegacyCompatibility" .

test-retention-overwrite:
	@echo "Running retention overwrite protection tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetentionOverwriteProtection" .

test-retention-bulk:
	@echo "Running retention bulk operations tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetentionBulkOperations" .

test-retention-multipart:
	@echo "Running retention multipart upload tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetentionWithMultipartUpload" .

test-retention-extended-attrs:
	@echo "Running retention extended attributes tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetentionExtendedAttributes" .

test-retention-defaults:
	@echo "Running retention bucket defaults tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetentionBucketDefaults" .

test-retention-concurrent:
	@echo "Running retention concurrent operations tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestRetentionConcurrentOperations" .

# Development targets
dev-start: start-server
	@echo "Development server started. Access S3 API at http://localhost:$(S3_PORT)"
	@echo "To stop: make stop-server"

dev-test: check-deps
	@echo "Running tests in development mode..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestBasicRetentionWorkflow" .

# CI targets
ci-test: check-deps
	@echo "Running tests in CI mode..."
	@go test -v -timeout=$(TEST_TIMEOUT) -race .

# All targets
test-all: test-retention test-retention-worm
	@echo "✅ All S3 retention tests completed"

# Benchmark targets
benchmark-retention:
	@echo "Running retention performance benchmarks..."
	@go test -v -timeout=$(TEST_TIMEOUT) -bench=. -benchmem .

# Coverage targets
coverage:
	@echo "Running tests with coverage..."
	@go test -v -timeout=$(TEST_TIMEOUT) -coverprofile=coverage.out .
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Format and lint
fmt:
	@echo "Formatting Go code..."
	@go fmt .

lint:
	@echo "Running linter..."
	@golint . || echo "golint not available, skipping..."

# Install dependencies for development
install-deps:
	@echo "Installing Go dependencies..."
	@go mod tidy
	@go mod download

# Show current configuration
show-config:
	@echo "Current configuration:"
	@echo "  WEED_BINARY: $(WEED_BINARY)"
	@echo "  S3_PORT: $(S3_PORT)"
	@echo "  TEST_TIMEOUT: $(TEST_TIMEOUT)"
	@echo "  TEST_PATTERN: $(TEST_PATTERN)" 