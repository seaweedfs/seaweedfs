# S3 Object Tagging Tests
# Tests for GitHub issue #7589: S3 object Tags query comes back empty

.PHONY: help build-weed setup-server start-server stop-server test-tagging test-tagging-quick test-tagging-comprehensive test-all clean logs check-deps test-with-server

# Configuration
WEED_BINARY := ../../../weed/weed_binary
S3_PORT := 8006
MASTER_PORT := 9338
VOLUME_PORT := 8085
FILER_PORT := 8893
TEST_TIMEOUT := 10m
TEST_PATTERN := TestObjectTaggingOnUpload|TestPutObjectTaggingAPI|TestDeleteObjectTagging|TestTag

# Default target
help:
	@echo "S3 Object Tagging Tests Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  help                       - Show this help message"
	@echo "  build-weed                - Build the SeaweedFS binary"
	@echo "  check-deps                - Check dependencies and build binary if needed"
	@echo "  start-server              - Start SeaweedFS server for testing"
	@echo "  stop-server               - Stop SeaweedFS server"
	@echo "  test-tagging              - Run all tagging tests"
	@echo "  test-tagging-quick        - Run core tagging tests only"
	@echo "  test-tagging-comprehensive - Run comprehensive tagging tests"
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
	@echo "🔍 DEBUG: Command: $(WEED_BINARY) server -filer -filer.maxMB=64 -s3 -ip.bind 0.0.0.0 -dir=./test-volume-data -master.raftHashicorp -master.electionTimeout 1s -master.volumeSizeLimitMB=100 -volume.max=100 -volume.preStopSeconds=1 -master.port=$(MASTER_PORT) -volume.port=$(VOLUME_PORT) -filer.port=$(FILER_PORT) -s3.port=$(S3_PORT) -metricsPort=9329 -s3.allowEmptyFolder=false -s3.allowDeleteBucketNotEmpty=true -s3.config=../../../docker/compose/s3.json -master.peers=none"
	@$(WEED_BINARY) server \
		-filer \
		-filer.maxMB=64 \
		-s3 \
		-ip.bind 0.0.0.0 \
		-dir=./test-volume-data \
		-master.raftHashicorp \
		-master.electionTimeout 1s \
		-master.volumeSizeLimitMB=100 \
		-volume.max=100 \
		-volume.preStopSeconds=1 \
		-master.port=$(MASTER_PORT) \
		-volume.port=$(VOLUME_PORT) \
		-filer.port=$(FILER_PORT) \
		-s3.port=$(S3_PORT) \
		-metricsPort=9329 \
		-s3.allowEmptyFolder=false \
		-s3.allowDeleteBucketNotEmpty=true \
		-s3.config=../../../docker/compose/s3.json \
		-master.peers=none \
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
			netstat -tlnp 2>/dev/null | grep $(MASTER_PORT) || echo "Port $(MASTER_PORT) not bound"; \
			netstat -tlnp 2>/dev/null | grep $(VOLUME_PORT) || echo "Port $(VOLUME_PORT) not bound"; \
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
	netstat -tlnp 2>/dev/null | grep -E "($(S3_PORT)|$(MASTER_PORT)|$(VOLUME_PORT))" || echo "No ports bound"; \
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

# Core tagging tests (basic functionality)
test-tagging-quick: check-deps
	@echo "Running core tagging tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestObjectTaggingOnUpload|TestPutObjectTaggingAPI" .
	@echo "✅ Core tagging tests completed"

# All tagging tests (comprehensive)
test-tagging: check-deps
	@echo "Running all tagging tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" .
	@echo "✅ All tagging tests completed"

# Comprehensive tagging tests (all features)
test-tagging-comprehensive: check-deps
	@echo "Running comprehensive tagging tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) .
	@echo "✅ Comprehensive tagging tests completed"

# All tests without server management
test-tagging-simple: check-deps
	@echo "Running tagging tests (assuming server is already running)..."
	@go test -v -timeout=$(TEST_TIMEOUT) .
	@echo "✅ All tagging tests completed"

# Start server, run tests, stop server
test-with-server: start-server
	@echo "Running tagging tests with managed server..."
	@sleep 5  # Give server time to fully start
	@make test-tagging-comprehensive || (echo "Tests failed, stopping server..." && make stop-server && exit 1)
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
	@rm -f tagging.test
	@go clean -testcache
	@echo "✅ Cleanup completed"

# Individual test targets for specific functionality
test-upload:
	@echo "Running upload tagging tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestObjectTaggingOnUpload" .

test-special-chars:
	@echo "Running special characters tagging tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestObjectTaggingOnUploadWithSpecialCharacters" .

test-api:
	@echo "Running API tagging tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestPutObjectTaggingAPI" .

test-get:
	@echo "Running get tagging tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestGetObjectTaggingAPI" .

test-delete:
	@echo "Running delete tagging tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestDeleteObjectTaggingAPI" .

# Development targets
dev-start: start-server
	@echo "Development server started. Access S3 API at http://localhost:$(S3_PORT)"
	@echo "To stop: make stop-server"

dev-test: check-deps
	@echo "Running tests in development mode..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestObjectTaggingOnUpload" .

# CI targets
ci-test: check-deps
	@echo "Running tests in CI mode..."
	@go test -v -timeout=$(TEST_TIMEOUT) -race .

# All targets
test-all: test-tagging test-tagging-comprehensive
	@echo "✅ All tagging tests completed"

# Benchmark targets
benchmark-tagging:
	@echo "Running tagging performance benchmarks..."
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
test-verbose: test-tagging-comprehensive
test-single: test-upload
test-clean: clean
build: check-deps
setup: check-deps
