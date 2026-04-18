# S3 Checksum Integration Tests
# Covers flexible-checksum behavior on presigned URL uploads (issue #9075).

.PHONY: help build-weed check-deps start-server stop-server test test-with-server logs clean health-check

WEED_BINARY := ../../../weed/weed_binary
S3_PORT := 8333
ACCESS_KEY ?= some_access_key1
SECRET_KEY ?= some_secret_key1
TEST_TIMEOUT := 10m
TEST_PATTERN ?= TestPresignedPut
SERVER_DIR := ./test-volume-data/server-data

help:
	@echo "S3 Checksum Integration Tests"
	@echo ""
	@echo "Targets:"
	@echo "  build-weed         Build the SeaweedFS binary"
	@echo "  start-server       Start a local SeaweedFS S3 server for testing"
	@echo "  stop-server        Stop the local SeaweedFS server"
	@echo "  test               Run checksum tests (server must already be running)"
	@echo "  test-with-server   Start server, run tests, stop server"
	@echo "  logs               Tail server log"
	@echo "  clean              Remove test artifacts"

build-weed:
	@echo "Building SeaweedFS binary..."
	@cd ../../../weed && go build -o weed_binary .
	@chmod +x $(WEED_BINARY)

check-deps: build-weed
	@command -v go >/dev/null 2>&1 || (echo "Go is required but not installed" && exit 1)
	@test -f $(WEED_BINARY) || (echo "SeaweedFS binary not found at $(WEED_BINARY)" && exit 1)
	@go list -m github.com/aws/aws-sdk-go-v2 >/dev/null 2>&1 || (echo "AWS SDK Go v2 not found. Run 'go mod tidy'." && exit 1)
	@go list -m github.com/stretchr/testify >/dev/null 2>&1 || (echo "Testify not found. Run 'go mod tidy'." && exit 1)

start-server: check-deps
	@echo "Starting SeaweedFS server..."
	@rm -f weed-server.pid
	@mkdir -p $(SERVER_DIR)
	@AWS_ACCESS_KEY_ID=$(ACCESS_KEY) AWS_SECRET_ACCESS_KEY=$(SECRET_KEY) $(WEED_BINARY) mini \
		-dir=$(SERVER_DIR) \
		-s3.port=$(S3_PORT) \
		> weed-test.log 2>&1 & \
		echo $$! > weed-server.pid
	@for i in $$(seq 1 90); do \
		if curl -s http://localhost:$(S3_PORT) >/dev/null 2>&1; then \
			echo "✅ SeaweedFS server started on port $(S3_PORT) after $$i seconds"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "❌ Server failed to start within 90 seconds"; \
	if [ -f weed-server.pid ]; then \
		PID=$$(cat weed-server.pid); \
		if ps -p $$PID >/dev/null 2>&1; then \
			kill -TERM $$PID 2>/dev/null || true; \
			sleep 1; \
			ps -p $$PID >/dev/null 2>&1 && kill -KILL $$PID 2>/dev/null || true; \
		fi; \
		rm -f weed-server.pid; \
	fi; \
	if [ -f weed-test.log ]; then tail -100 weed-test.log; fi; \
	exit 1

stop-server:
	@if [ -f weed-server.pid ]; then \
		PID=$$(cat weed-server.pid); \
		if ps -p $$PID >/dev/null 2>&1; then \
			kill -TERM $$PID 2>/dev/null || true; \
			sleep 2; \
			ps -p $$PID >/dev/null 2>&1 && kill -KILL $$PID 2>/dev/null || true; \
		fi; \
		rm -f weed-server.pid; \
	fi
	@echo "✅ SeaweedFS server stopped"

test: check-deps
	@echo "Running checksum tests (pattern: $(TEST_PATTERN))..."
	@S3_ENDPOINT=http://localhost:$(S3_PORT) \
		S3_ACCESS_KEY=$(ACCESS_KEY) \
		S3_SECRET_KEY=$(SECRET_KEY) \
		go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" .

test-with-server: start-server
	@sleep 3
	@$(MAKE) test || (echo "Tests failed, stopping server..." && $(MAKE) stop-server && exit 1)
	@$(MAKE) stop-server
	@echo "✅ Checksum tests completed"

logs:
	@if [ -f weed-test.log ]; then tail -f weed-test.log; else echo "No log file"; fi

health-check:
	@curl -s http://localhost:$(S3_PORT) >/dev/null 2>&1 && echo "✅ S3 on $(S3_PORT)" || (echo "❌ S3 not up" && exit 1)

clean:
	@$(MAKE) stop-server
	@rm -f weed-test.log weed-server.pid
	@rm -rf ./test-volume-data
