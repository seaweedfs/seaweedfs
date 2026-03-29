# S3 delete regression test harness

.PHONY: all test help build-weed check-deps start-server stop-server test-delete test-with-server clean logs

WEED_BINARY := ../../../weed/weed_binary
S3_PORT := 8333
TEST_TIMEOUT := 10m
SERVER_DIR := ./test-volume-data
S3_CONFIG := ./test_s3.json

.DEFAULT_GOAL := help

all: test

test: test-with-server

help:
	@echo "S3 delete regression test harness"
	@echo ""
	@echo "Available targets:"
	@echo "  build-weed      - Build the SeaweedFS binary"
	@echo "  start-server    - Start SeaweedFS with S3 enabled"
	@echo "  stop-server     - Stop the test server"
	@echo "  test-delete     - Run delete regression tests"
	@echo "  test-with-server - Start server, run tests, stop server"
	@echo "  logs            - Show server logs"
	@echo "  clean           - Remove test artifacts"

build-weed:
	@echo "Building SeaweedFS binary..."
	@cd ../../../weed && go build -o weed_binary .
	@chmod +x $(WEED_BINARY)

check-deps: build-weed
	@command -v go >/dev/null 2>&1 || (echo "Go is required but not installed" && exit 1)
	@test -f $(WEED_BINARY) || (echo "SeaweedFS binary not found at $(WEED_BINARY)" && exit 1)
	@test -f $(S3_CONFIG) || (echo "S3 config not found at $(S3_CONFIG)" && exit 1)

start-server: check-deps
	@echo "Starting SeaweedFS server for delete regression tests..."
	@rm -f weed-server.pid
	@mkdir -p $(SERVER_DIR)
	@$(WEED_BINARY) mini \
		-dir=$(SERVER_DIR) \
		-s3 \
		-s3.port=$(S3_PORT) \
		-s3.config=$(S3_CONFIG) \
		-master.peers=none \
		> weed-test.log 2>&1 & echo $$! > weed-server.pid
	@for i in $$(seq 1 60); do \
		if curl -sS -o /dev/null http://127.0.0.1:$(S3_PORT)/ >/dev/null 2>&1; then \
			echo "SeaweedFS S3 server is ready on port $(S3_PORT)"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "SeaweedFS S3 server failed to start"; \
	echo "=== Server logs ==="; \
	test -f weed-test.log && cat weed-test.log || true; \
	exit 1

stop-server:
	@echo "Stopping SeaweedFS server..."
	@if [ -f weed-server.pid ]; then \
		SERVER_PID=$$(cat weed-server.pid); \
		kill -TERM $$SERVER_PID 2>/dev/null || true; \
		for i in $$(seq 1 20); do \
			if ! kill -0 $$SERVER_PID >/dev/null 2>&1; then \
				break; \
			fi; \
			sleep 0.1; \
		done; \
		if kill -0 $$SERVER_PID >/dev/null 2>&1; then \
			kill -KILL $$SERVER_PID 2>/dev/null || true; \
		fi; \
		rm -f weed-server.pid; \
	fi

test-delete: check-deps
	@echo "Running delete regression tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) .

test-with-server: start-server
	@trap "$(MAKE) stop-server" EXIT; \
	$(MAKE) test-delete || (echo "=== Last 100 lines of server logs ===" && tail -100 weed-test.log && exit 1)
	@$(MAKE) stop-server

logs:
	@if test -f weed-test.log; then \
		tail -f weed-test.log; \
	else \
		echo "No server log file found."; \
	fi

clean:
	@$(MAKE) stop-server
	@rm -f weed-test.log weed-server.pid
	@rm -rf $(SERVER_DIR)
