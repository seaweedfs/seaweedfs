# Makefile for SeaweedFS S3 Basic tests
# Tests auto-manage server via TestMain using weed mini

.PHONY: test test-quick test-external test-with-server clean check-deps build

# Build SeaweedFS binary if not exists
build:
	@echo "Building SeaweedFS binary..."
	@cd ../../../ && make build
	@test -f ../../../weed || (echo "SeaweedFS binary not found" && exit 1)

# Check dependencies
check-deps: build
	@test -f ../../../weed || (echo "SeaweedFS binary not found" && exit 1)
	@echo "✅ Dependencies available"

# Run all tests (auto-manages weed mini server via TestMain)
test: check-deps
	@echo "Running all S3 basic tests..."
	@go test -v -timeout=10m ./...

# Run quick tests (uses TEST_PATTERN if provided)
test-quick: check-deps
	@echo "Running quick S3 basic tests..."
	@if [ -z "$(TEST_PATTERN)" ]; then \
		go test -v -timeout=10m ./...; \
	else \
		go test -v -timeout=10m -run "$(TEST_PATTERN)" ./...; \
	fi

# Run tests against external server (CI/CD mode)
test-external: check-deps
	@echo "Running tests against external server (USE_EXTERNAL_SERVER=true)..."
	@USE_EXTERNAL_SERVER=true go test -v -timeout=10m ./...

# Run tests with auto-managed server (CI/CD target)
test-with-server: test

# Clean up test artifacts
clean:
	@echo "Cleaning up test artifacts..."
	@rm -f *.log *.pid
	@go clean -testcache
	@echo "✅ Cleanup completed"
