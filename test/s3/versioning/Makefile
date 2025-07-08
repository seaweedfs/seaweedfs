# S3 API Test Makefile
# This Makefile provides comprehensive targets for running S3 versioning tests

.PHONY: help build-weed setup-server start-server stop-server test-versioning test-versioning-quick test-versioning-comprehensive test-all clean logs check-deps

# Configuration
WEED_BINARY := ../../../weed/weed_binary
S3_PORT := 8333
MASTER_PORT := 9333
VOLUME_PORT := 8080
FILER_PORT := 8888
TEST_TIMEOUT := 10m
TEST_PATTERN := TestVersioning

# Default target
help:
	@echo "S3 API Test Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  help                    - Show this help message"
	@echo "  build-weed             - Build the SeaweedFS binary"
	@echo "  check-deps             - Check dependencies and build binary if needed"
	@echo "  start-server           - Start SeaweedFS server for testing"
	@echo "  stop-server            - Stop SeaweedFS server"
	@echo "  test-versioning        - Run all versioning tests"
	@echo "  test-versioning-quick  - Run core versioning tests only"
	@echo "  test-versioning-comprehensive - Run comprehensive versioning tests"
	@echo "  test-all               - Run all S3 API tests"
	@echo "  test-with-server       - Start server, run tests, stop server"
	@echo "  logs                   - Show server logs"
	@echo "  clean                  - Clean up test artifacts and stop server"
	@echo ""
	@echo "Configuration:"
	@echo "  S3_PORT=${S3_PORT}"
	@echo "  TEST_TIMEOUT=${TEST_TIMEOUT}"

# Check dependencies
# Build the SeaweedFS binary
build-weed:
	@echo "Building SeaweedFS binary..."
	@cd ../../../weed && go build -o weed_binary .
	@chmod +x $(WEED_BINARY)
	@echo "✅ SeaweedFS binary built at $(WEED_BINARY)"

check-deps: build-weed
	@echo "Checking dependencies..."
	@command -v go >/dev/null 2>&1 || (echo "Go is required but not installed" && exit 1)
	@test -f $(WEED_BINARY) || (echo "SeaweedFS binary not found at $(WEED_BINARY)" && exit 1)
	@go list -m github.com/aws/aws-sdk-go-v2 >/dev/null 2>&1 || (echo "AWS SDK Go v2 not found. Run 'go mod tidy'." && exit 1)
	@go list -m github.com/stretchr/testify >/dev/null 2>&1 || (echo "Testify not found. Run 'go mod tidy'." && exit 1)
	@echo "✅ All dependencies are available"

# Start SeaweedFS server for testing
start-server: check-deps
	@echo "Starting SeaweedFS server..."
	@pkill -f "weed.*server" || true
	@sleep 2
	@echo "Launching SeaweedFS server in background..."
	@$(WEED_BINARY) server \
		-debug \
		-s3 \
		-s3.port=$(S3_PORT) \
		-s3.allowEmptyFolder=false \
		-s3.allowDeleteBucketNotEmpty=true \
		-s3.config=../../../docker/compose/s3.json \
		-filer \
		-filer.maxMB=64 \
		-master.volumeSizeLimitMB=1024 \
		-volume.max=100 \
		-volume.preStopSeconds=1 \
		-metricsPort=9324 \
		> weed-test.log 2>&1 &
	@echo "Server PID: $$!"
	@echo "Waiting for server to start (up to 90 seconds)..."
	@for i in $$(seq 1 90); do \
		if curl -s http://localhost:$(S3_PORT) >/dev/null 2>&1; then \
			echo "✅ SeaweedFS server started successfully on port $(S3_PORT) after $$i seconds"; \
			exit 0; \
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
	@pkill -f "weed.*server" || true
	@sleep 2
	@echo "✅ SeaweedFS server stopped"

# Show server logs
logs:
	@if test -f weed-test.log; then \
		echo "=== SeaweedFS Server Logs ==="; \
		tail -f weed-test.log; \
	else \
		echo "No log file found. Server may not be running."; \
	fi

# Core versioning tests (equivalent to Python s3tests)
test-versioning-quick: check-deps
	@echo "Running core S3 versioning tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "TestBucketListReturnDataVersioning|TestVersioningBasicWorkflow|TestVersioningDeleteMarkers" .
	@echo "✅ Core versioning tests completed"

# All versioning tests
test-versioning: check-deps
	@echo "Running all S3 versioning tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" .
	@echo "✅ All versioning tests completed"

# Comprehensive versioning tests (including edge cases)
test-versioning-comprehensive: check-deps
	@echo "Running comprehensive S3 versioning tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" . -count=1
	@echo "✅ Comprehensive versioning tests completed"

# All S3 API tests
test-all: check-deps
	@echo "Running all S3 API tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) ./...
	@echo "✅ All S3 API tests completed"

# Run tests with automatic server management
test-with-server: start-server
	@echo "Running versioning tests with managed server..."
	@$(MAKE) test-versioning || (echo "❌ Tests failed, showing server logs:" && tail -50 weed-test.log && $(MAKE) stop-server && exit 1)
	@$(MAKE) stop-server
	@echo "✅ Tests completed and server stopped"

# Test with different configurations
test-versioning-with-configs: check-deps
	@echo "Testing with different S3 configurations..."
	@echo "Testing with empty folder allowed..."
	@$(WEED_BINARY) server -s3 -s3.port=$(S3_PORT) -s3.allowEmptyFolder=true -filer -master.volumeSizeLimitMB=1024 -volume.max=100 > weed-test-config1.log 2>&1 &
	@sleep 5
	@go test -v -timeout=5m -run "TestVersioningBasicWorkflow" . || true
	@pkill -f "weed.*server" || true
	@sleep 2
	@echo "Testing with delete bucket not empty disabled..."
	@$(WEED_BINARY) server -s3 -s3.port=$(S3_PORT) -s3.allowDeleteBucketNotEmpty=false -filer -master.volumeSizeLimitMB=1024 -volume.max=100 > weed-test-config2.log 2>&1 &
	@sleep 5
	@go test -v -timeout=5m -run "TestVersioningBasicWorkflow" . || true
	@pkill -f "weed.*server" || true
	@echo "✅ Configuration tests completed"

# Performance/stress testing
test-versioning-stress: check-deps
	@echo "Running stress tests for versioning..."
	@go test -v -timeout=20m -run "TestVersioningConcurrentOperations" . -count=5
	@echo "✅ Stress tests completed"

# Generate test reports
test-report: check-deps
	@echo "Generating test reports..."
	@mkdir -p reports
	@go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" . -json > reports/test-results.json 2>&1 || true
	@go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" . -coverprofile=reports/coverage.out 2>&1 || true
	@go tool cover -html=reports/coverage.out -o reports/coverage.html 2>/dev/null || true
	@echo "✅ Test reports generated in reports/ directory"

# Clean up test artifacts
clean:
	@echo "Cleaning up test artifacts..."
	@$(MAKE) stop-server
	@rm -f weed-test*.log
	@rm -rf reports/
	@go clean -testcache
	@echo "✅ Cleanup completed"

# Debug mode - start server with verbose logging
debug-server:
	@echo "Starting SeaweedFS server in debug mode..."
	@$(MAKE) stop-server
	@$(WEED_BINARY) server \
		-debug \
		-s3 \
		-s3.port=$(S3_PORT) \
		-s3.allowEmptyFolder=false \
		-s3.allowDeleteBucketNotEmpty=true \
		-s3.config=../../../docker/compose/s3.json \
		-filer \
		-filer.maxMB=16 \
		-master.volumeSizeLimitMB=50 \
		-volume.max=100 \
		-volume.preStopSeconds=1 \
		-metricsPort=9324

# Run a single test for debugging
debug-test: check-deps
	@echo "Running single test for debugging..."
	@go test -v -timeout=5m -run "TestBucketListReturnDataVersioning" . -count=1

# Continuous testing (re-run tests on file changes)
watch-tests:
	@echo "Starting continuous testing (requires 'entr' command)..."
	@command -v entr >/dev/null 2>&1 || (echo "Install 'entr' for file watching: brew install entr (macOS) or apt-get install entr (Linux)" && exit 1)
	@find . -name "*.go" | entr -c $(MAKE) test-versioning-quick

# Install missing Go dependencies
install-deps:
	@echo "Installing Go dependencies..."
	@go mod download
	@go mod tidy
	@echo "✅ Dependencies installed"

# Validate test configuration
validate-config:
	@echo "Validating test configuration..."
	@test -f test_config.json || (echo "❌ test_config.json not found" && exit 1)
	@python3 -m json.tool test_config.json > /dev/null 2>&1 || (echo "❌ test_config.json is not valid JSON" && exit 1)
	@echo "✅ Configuration is valid"

# Quick health check
health-check:
	@echo "Running health check..."
	@curl -s http://localhost:$(S3_PORT) >/dev/null 2>&1 && echo "✅ S3 API is accessible" || echo "❌ S3 API is not accessible"
	@curl -s http://localhost:9324/metrics >/dev/null 2>&1 && echo "✅ Metrics endpoint is accessible" || echo "❌ Metrics endpoint is not accessible"

# Compare with Python s3tests (if available)
compare-python-tests:
	@echo "Comparing Go tests with Python s3tests..."
	@echo "Go test: TestBucketListReturnDataVersioning"
	@echo "Python equivalent: test_bucket_list_return_data_versioning"
	@echo ""
	@echo "Running Go version..."
	@time go test -v -run "TestBucketListReturnDataVersioning" . 2>&1 | grep -E "(PASS|FAIL|took)" 