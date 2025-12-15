# ETag Format Integration Tests
#
# These tests verify S3 ETag format compatibility, particularly for large files
# that trigger SeaweedFS auto-chunking. This addresses GitHub Issue #7768.
#
# Prerequisites:
#   - SeaweedFS running with S3 API enabled on port 8333
#   - Go 1.21+
#
# Usage:
#   make test          - Run all tests
#   make test-large    - Run only large file tests
#   make test-verbose  - Run with verbose output
#   make clean         - Clean test artifacts

.PHONY: test test-large test-verbose clean help

# Default S3 endpoint
S3_ENDPOINT ?= http://127.0.0.1:8333

test:
	@echo "Running ETag format tests against $(S3_ENDPOINT)..."
	go test -v -timeout 5m ./...

test-large:
	@echo "Running large file ETag tests..."
	go test -v -timeout 5m -run "LargeFile|ExtraLarge" ./...

test-verbose:
	go test -v -timeout 5m -count=1 ./...

test-quick:
	@echo "Running quick ETag tests (small files only)..."
	go test -v -timeout 1m -run "SmallFile|Consistency" ./...

clean:
	go clean -testcache

help:
	@echo "ETag Format Integration Tests"
	@echo ""
	@echo "Targets:"
	@echo "  test          Run all ETag format tests"
	@echo "  test-large    Run only large file tests (>8MB)"
	@echo "  test-quick    Run quick tests (small files only)"
	@echo "  test-verbose  Run with verbose output"
	@echo "  clean         Clean test cache"
	@echo ""
	@echo "Environment Variables:"
	@echo "  S3_ENDPOINT   S3 endpoint URL (default: http://127.0.0.1:8333)"

