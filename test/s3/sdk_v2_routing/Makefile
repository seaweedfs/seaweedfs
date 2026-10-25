# AWS SDK V2 Route Disambiguation Integration Tests
#
# Pins the regression for the route collision between the regular S3 API
# and the S3 Tables REST API on shared top-level paths (/buckets,
# /get-table). The tests use the real AWS SDK V2 for Go so the SDK's own
# XML deserializer is the assertion — a JSON body produces an SDK error
# before any test code runs.
#
# Prerequisites:
#   - SeaweedFS running with S3 API enabled on port 8333
#   - Go 1.21+
#
# Usage:
#   make test         - Run the SDK V2 routing tests
#   make test-verbose - Run with verbose output
#   make clean        - Clean test cache

.PHONY: all test test-verbose clean help

S3_ENDPOINT ?= http://127.0.0.1:8333

all: test

test:
	@echo "Running SDK V2 routing tests against $(S3_ENDPOINT)..."
	S3_ENDPOINT=$(S3_ENDPOINT) go test -v -timeout 5m ./...

test-verbose:
	S3_ENDPOINT=$(S3_ENDPOINT) go test -v -timeout 5m -count=1 ./...

clean:
	go clean -testcache

help:
	@echo "AWS SDK V2 Route Disambiguation Tests"
	@echo "Targets:"
	@echo "  test          Run the routing tests"
	@echo "  test-verbose  Run with verbose output"
	@echo "  clean         Clean test cache"
	@echo "Environment Variables:"
	@echo "  S3_ENDPOINT   S3 endpoint URL (default: http://127.0.0.1:8333)"
