# SeaweedFS FUSE Integration Testing Makefile

# Configuration
WEED_BINARY := weed
GO_VERSION := 1.24
TEST_TIMEOUT := 30m
COVERAGE_FILE := coverage.out

# Default target
.DEFAULT_GOAL := help

# Check if weed binary exists
check-binary:
	@if [ ! -f "$(WEED_BINARY)" ]; then \
		echo "ERROR SeaweedFS binary not found at $(WEED_BINARY)"; \
		echo "   Please run 'make' in the root directory first"; \
		exit 1; \
	fi
	@echo "OK SeaweedFS binary found"

# Check FUSE installation
check-fuse:
	@if command -v fusermount >/dev/null 2>&1; then \
		echo "OK FUSE is installed (Linux)"; \
	elif command -v umount >/dev/null 2>&1 && [ "$$(uname)" = "Darwin" ]; then \
		echo "OK FUSE is available (macOS)"; \
	else \
			echo "ERROR FUSE not found. Please install:"; \
		echo "   Ubuntu/Debian: sudo apt-get install fuse"; \
		echo "   CentOS/RHEL:   sudo yum install fuse"; \
		echo "   macOS:         brew install macfuse"; \
		exit 1; \
	fi

# Check Go version
check-go:
	@go version | grep -q "go1\.[2-9][0-9]" || \
	go version | grep -q "go1\.2[1-9]" || \
	(echo "ERROR Go $(GO_VERSION)+ required. Current: $$(go version)" && exit 1)
	@echo "OK Go version check passed"

# Verify all prerequisites
check-prereqs: check-go check-fuse
	@echo "✅ All prerequisites satisfied"

# Build the SeaweedFS binary (if needed)
build:
	@echo "Building SeaweedFS..."
	cd ../.. && make
	@echo "OK Build complete"

# Initialize go module (if needed)
init-module:
	@if [ ! -f go.mod ]; then \
		echo "📦 Initializing Go module..."; \
		go mod init seaweedfs-fuse-tests; \
		go mod tidy; \
	fi

# Run all tests
test: check-prereqs init-module
	@echo "🧪 Running all FUSE integration tests..."
	go test -v -timeout $(TEST_TIMEOUT) ./...

# Run tests with coverage
test-coverage: check-prereqs init-module
	@echo "🧪 Running tests with coverage..."
	go test -v -timeout $(TEST_TIMEOUT) -coverprofile=$(COVERAGE_FILE) ./...
	go tool cover -html=$(COVERAGE_FILE) -o coverage.html
	@echo "📊 Coverage report generated: coverage.html"

# Run specific test categories
test-basic: check-prereqs init-module
	@echo "🧪 Running basic file operations tests..."
	go test -v -timeout $(TEST_TIMEOUT) -run TestBasicFileOperations

test-directory: check-prereqs init-module
	@echo "🧪 Running directory operations tests..."
	go test -v -timeout $(TEST_TIMEOUT) -run TestDirectoryOperations

test-concurrent: check-prereqs init-module
	@echo "🧪 Running concurrent operations tests..."
	go test -v -timeout $(TEST_TIMEOUT) -run TestConcurrentFileOperations

test-stress: check-prereqs init-module
	@echo "🧪 Running stress tests..."
	go test -v -timeout $(TEST_TIMEOUT) -run TestStressOperations

test-large-files: check-prereqs init-module
	@echo "🧪 Running large file tests..."
	go test -v -timeout $(TEST_TIMEOUT) -run TestLargeFileOperations

# Run tests with debugging enabled
test-debug: check-prereqs init-module
	@echo "🔍 Running tests with debug output..."
	go test -v -timeout $(TEST_TIMEOUT) -args -debug

# Run tests and keep temp files for inspection
test-no-cleanup: check-prereqs init-module
	@echo "🧪 Running tests without cleanup (for debugging)..."
	go test -v -timeout $(TEST_TIMEOUT) -args -no-cleanup

# Quick smoke test
test-smoke: check-prereqs init-module
	@echo "💨 Running smoke tests..."
	go test -v -timeout 5m -run TestBasicFileOperations/CreateAndReadFile

# Run benchmarks
benchmark: check-prereqs init-module
	@echo "📈 Running benchmarks..."
	go test -v -timeout $(TEST_TIMEOUT) -bench=. -benchmem

# Validate test files compile
validate: init-module
	@echo "✅ Validating test files..."
	go build -o /dev/null ./...
	@echo "✅ All test files compile successfully"

# Clean up generated files
clean:
	@echo "🧹 Cleaning up..."
	rm -f $(COVERAGE_FILE) coverage.html
	rm -rf /tmp/seaweedfs_fuse_test_*
	go clean -testcache
	@echo "✅ Cleanup complete"

# Format Go code
fmt:
	@echo "🎨 Formatting Go code..."
	go fmt ./...

# Run linter
lint:
	@echo "🔍 Running linter..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "⚠️  golangci-lint not found, running go vet instead"; \
		go vet ./...; \
	fi

# Run all quality checks
check: validate lint fmt
	@echo "✅ All quality checks passed"

# Install development dependencies
install-deps:
	@echo "📦 Installing development dependencies..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go mod download
	go mod tidy

# Quick development setup
setup: install-deps build check-prereqs
	@echo "🚀 Development environment ready!"

# Docker-based testing
test-docker:
	@echo "🐳 Running tests in Docker..."
	docker build -t seaweedfs-fuse-tests -f Dockerfile.test ../..
	docker run --rm --privileged seaweedfs-fuse-tests

# Create Docker test image
docker-build:
	@echo "🐳 Building Docker test image..."
	@cat > Dockerfile.test << 'EOF' ;\
FROM golang:$(GO_VERSION) ;\
RUN apt-get update && apt-get install -y fuse ;\
WORKDIR /seaweedfs ;\
COPY . . ;\
RUN make ;\
WORKDIR /seaweedfs/test/fuse ;\
RUN go mod init seaweedfs-fuse-tests && go mod tidy ;\
CMD ["make", "test"] ;\
EOF

# GitHub Actions workflow
generate-workflow:
	@echo "📝 Generating GitHub Actions workflow..."
	@mkdir -p ../../.github/workflows
	@cat > ../../.github/workflows/fuse-integration.yml << 'EOF' ;\
name: FUSE Integration Tests ;\
 ;\
on: ;\
  push: ;\
    branches: [ master, main ] ;\
  pull_request: ;\
    branches: [ master, main ] ;\
 ;\
jobs: ;\
  fuse-integration: ;\
    runs-on: ubuntu-latest ;\
    timeout-minutes: 45 ;\
     ;\
    steps: ;\
    - name: Checkout code ;\
      uses: actions/checkout@v4 ;\
       ;\
    - name: Set up Go ;\
      uses: actions/setup-go@v4 ;\
      with: ;\
        go-version: '$(GO_VERSION)' ;\
         ;\
    - name: Install FUSE ;\
      run: sudo apt-get update && sudo apt-get install -y fuse ;\
       ;\
    - name: Build SeaweedFS ;\
      run: make ;\
       ;\
    - name: Run FUSE Integration Tests ;\
      run: | ;\
        cd test/fuse ;\
        make test ;\
         ;\
    - name: Upload test artifacts ;\
      if: failure() ;\
      uses: actions/upload-artifact@v3 ;\
      with: ;\
        name: test-logs ;\
        path: /tmp/seaweedfs_fuse_test_* ;\
EOF
	@echo "✅ GitHub Actions workflow generated"

# Performance profiling
profile: check-prereqs init-module
	@echo "📊 Running performance profiling..."
	go test -v -timeout $(TEST_TIMEOUT) -cpuprofile cpu.prof -memprofile mem.prof -bench=.
	@echo "📊 Profiles generated: cpu.prof, mem.prof"
	@echo "📊 View with: go tool pprof cpu.prof"

# Memory leak detection
test-memory: check-prereqs init-module
	@echo "🔍 Running memory leak detection..."
	go test -v -timeout $(TEST_TIMEOUT) -race -test.memprofile mem.prof

# List available test functions
list-tests:
	@echo "📋 Available test functions:"
	@grep -r "^func Test" *.go | sed 's/.*func \(Test[^(]*\).*/  \1/' | sort

# Get test status and statistics
test-stats: check-prereqs init-module
	@echo "📊 Test statistics:"
	@go test -v ./... | grep -E "(PASS|FAIL|RUN)" | \
		awk '{ \
			if ($$1 == "RUN") tests++; \
			else if ($$1 == "PASS") passed++; \
			else if ($$1 == "FAIL") failed++; \
		} END { \
			printf "  Total tests: %d\n", tests; \
			printf "  Passed: %d\n", passed; \
			printf "  Failed: %d\n", failed; \
			printf "  Success rate: %.1f%%\n", (passed/tests)*100; \
		}'

# Watch for file changes and run tests
watch:
	@echo "👀 Watching for changes..."
	@if command -v entr >/dev/null 2>&1; then \
		find . -name "*.go" | entr -c make test-smoke; \
	else \
		echo "⚠️  'entr' not found. Install with: apt-get install entr"; \
		echo "   Falling back to manual test run"; \
		make test-smoke; \
	fi

# Show help
help:
	@echo "SeaweedFS FUSE Integration Testing"
	@echo "=================================="
	@echo ""
	@echo "Prerequisites:"
	@echo "  make check-prereqs    - Check all prerequisites"
	@echo "  make setup           - Complete development setup"
	@echo "  make build           - Build SeaweedFS binary"
	@echo ""
	@echo "Testing:"
	@echo "  make test            - Run all tests"
	@echo "  make test-basic      - Run basic file operations tests"
	@echo "  make test-directory  - Run directory operations tests"
	@echo "  make test-concurrent - Run concurrent operations tests"
	@echo "  make test-stress     - Run stress tests"
	@echo "  make test-smoke      - Quick smoke test"
	@echo "  make test-coverage   - Run tests with coverage report"
	@echo ""
	@echo "Debugging:"
	@echo "  make test-debug      - Run tests with debug output"
	@echo "  make test-no-cleanup - Keep temp files for inspection"
	@echo "  make profile         - Performance profiling"
	@echo "  make test-memory     - Memory leak detection"
	@echo ""
	@echo "Quality:"
	@echo "  make validate        - Validate test files compile"
	@echo "  make lint            - Run linter"
	@echo "  make fmt             - Format code"
	@echo "  make check           - Run all quality checks"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean           - Clean up generated files"
	@echo "  make list-tests      - List available test functions"
	@echo "  make test-stats      - Show test statistics"
	@echo "  make watch           - Watch files and run smoke tests"
	@echo ""
	@echo "Docker & CI:"
	@echo "  make test-docker     - Run tests in Docker"
	@echo "  make generate-workflow - Generate GitHub Actions workflow"

.PHONY: help check-prereqs check-binary check-fuse check-go build init-module \
        test test-coverage test-basic test-directory test-concurrent test-stress \
        test-large-files test-debug test-no-cleanup test-smoke benchmark validate \
        clean fmt lint check install-deps setup test-docker docker-build \
        generate-workflow profile test-memory list-tests test-stats watch