# CORS Integration Tests Makefile
# This Makefile provides comprehensive targets for running CORS integration tests

.PHONY: help build-weed setup-server start-server stop-server test-cors test-cors-quick test-cors-comprehensive test-all clean logs check-deps

# Configuration
WEED_BINARY := ../../../weed/weed_binary
S3_PORT := 8333
MASTER_PORT := 9333
VOLUME_PORT := 8080
FILER_PORT := 8888
TEST_TIMEOUT := 10m
TEST_PATTERN := TestCORS

# Default target
help:
	@echo "CORS Integration Tests Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  help                       - Show this help message"
	@echo "  build-weed                - Build the SeaweedFS binary"
	@echo "  check-deps                - Check dependencies and build binary if needed"
	@echo "  start-server              - Start SeaweedFS server for testing"
	@echo "  start-server-simple       - Start server without process cleanup (for CI)"
	@echo "  stop-server               - Stop SeaweedFS server"
	@echo "  test-cors                 - Run all CORS tests"
	@echo "  test-cors-quick           - Run core CORS tests only"
	@echo "  test-cors-simple          - Run tests without server management"
	@echo "  test-cors-comprehensive   - Run comprehensive CORS tests"
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
	@mkdir -p $(SERVER_DIR)
	@echo "🔍 DEBUG: Launching SeaweedFS S3 server in background..."
	@echo "🔍 DEBUG: Command: $(WEED_BINARY) mini -dir=$(SERVER_DIR) -s3 -s3.port=$(S3_PORT) -s3.config=$(S3_CONFIG)"
	@$(WEED_BINARY) mini \
		-dir=$(SERVER_DIR) \
		-s3 \
		-s3.port=$(S3_PORT) \
		-s3.config=$(S3_CONFIG) \
		> weed-server.log 2>&1 & \
		echo $$! > weed-server.pid
	
	@echo "Waiting for S3 server to be ready..."
	@for i in $$(seq 1 30); do \
		if echo | nc -z localhost $(S3_PORT); then \
			echo "S3 server is ready!"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "S3 server failed to start"; \
	exit 1 > weed-server.pid
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

# Core CORS tests (basic functionality)
test-cors-quick: check-deps
	@echo "Running core CORS tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSConfigurationManagement|TestCORSPreflightRequest|TestCORSActualRequest" .
	@echo "✅ Core CORS tests completed"

# All CORS tests (comprehensive)
test-cors: check-deps
	@echo "Running all CORS tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" .
	@echo "✅ All CORS tests completed"

# Comprehensive CORS tests (all features)
test-cors-comprehensive: check-deps
	@echo "Running comprehensive CORS tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORS" .
	@echo "✅ Comprehensive CORS tests completed"

# All tests without server management
test-cors-simple: check-deps
	@echo "Running CORS tests (assuming server is already running)..."
	@go test -v -timeout=$(TEST_TIMEOUT) .
	@echo "✅ All CORS tests completed"

# Start server, run tests, stop server
test-with-server: start-server
	@echo "Running CORS tests with managed server..."
	@sleep 5  # Give server time to fully start
	@make test-cors-comprehensive || (echo "Tests failed, stopping server..." && make stop-server && exit 1)
	@make stop-server
	@echo "✅ All tests completed with managed server"

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
	@rm -f cors.test
	@go clean -testcache
	@echo "✅ Cleanup completed"

# Individual test targets for specific functionality
test-basic-cors:
	@echo "Running basic CORS tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSConfigurationManagement" .

test-preflight-cors:
	@echo "Running preflight CORS tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSPreflightRequest" .

test-actual-cors:
	@echo "Running actual CORS request tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSActualRequest" .

test-origin-matching:
	@echo "Running origin matching tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSOriginMatching" .

test-header-matching:
	@echo "Running header matching tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSHeaderMatching" .

test-method-matching:
	@echo "Running method matching tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSMethodMatching" .

test-multiple-rules:
	@echo "Running multiple rules tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSMultipleRulesMatching" .

test-validation:
	@echo "Running validation tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSValidation" .

test-caching:
	@echo "Running caching tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSCaching" .

test-error-handling:
	@echo "Running error handling tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSErrorHandling" .

# Development targets
dev-start: start-server
	@echo "Development server started. Access S3 API at http://localhost:$(S3_PORT)"
	@echo "To stop: make stop-server"

dev-test: check-deps
	@echo "Running tests in development mode..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestCORSConfigurationManagement" .

# CI targets
ci-test: check-deps
	@echo "Running tests in CI mode..."
	@go test -v -timeout=$(TEST_TIMEOUT) -race .

# All targets
test-all: test-cors test-cors-comprehensive
	@echo "✅ All CORS tests completed"

# Benchmark targets
benchmark-cors:
	@echo "Running CORS performance benchmarks..."
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

# Legacy targets for backward compatibility
test: test-with-server
test-verbose: test-cors-comprehensive
test-single: test-basic-cors
test-clean: clean
build: check-deps
setup: check-deps 