# Makefile for SeaweedFS S3 API versioning tests
# Tests auto-manage server via TestMain using weed mini

.PHONY: test test-quick test-stress test-external test-with-server clean check-deps build

# Build SeaweedFS binary if not exists
build:
	@echo "Building SeaweedFS binary..."
	@cd ../../../ && make build
	@test -f ../../../weed || (echo "SeaweedFS binary not found" && exit 1)

# Check dependencies
check-deps: build
	@test -f ../../../weed || (echo "SeaweedFS binary not found" && exit 1)
	@echo "✅ Dependencies available"

# Run all tests (auto-manages weed mini server)
test: check-deps
	@echo "Running all S3 versioning tests..."
	@go test -v -timeout=15m ./...

# Run quick tests (excludes stress tests)
test-quick: check-deps
	@echo "Running quick S3 versioning tests..."
	@go test -v -timeout=10m -run "!(Stress)" ./...

# Run stress tests only
test-stress: check-deps
	@echo "Running stress tests (may take several minutes)..."
	@ENABLE_STRESS_TESTS=true go test -v -timeout=30m -run "Stress" ./...

# Run tests against external server
test-external: check-deps
	@echo "Running tests against external server (USE_EXTERNAL_SERVER=true)..."
	@USE_EXTERNAL_SERVER=true go test -v -timeout=15m ./...

# Run tests with auto-managed server (alias to test for CI/CD)
test-with-server: test

# Clean up test artifacts
clean:
	@echo "Cleaning up test artifacts..."
	@rm -f *.log *.pid
	@rm -rf reports/
	@rm -rf test-volume-data/
	@go clean -testcache
	@echo "✅ Cleanup completed"
