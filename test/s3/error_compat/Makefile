# S3/IAM error response compatibility test harness

.PHONY: all test help build-weed check-deps start-server stop-server test-errors test-with-server clean logs

WEED_BINARY := ../../../weed/weed_binary
S3_PORT := 8334
TEST_TIMEOUT := 10m
SERVER_DIR := ./test-volume-data
S3_CONFIG := ./test_s3.json

.DEFAULT_GOAL := help

all: test

test: test-with-server

help:
	@echo "S3/IAM error response compatibility test harness"
	@echo ""
	@echo "Available targets:"
	@echo "  build-weed      - Build the SeaweedFS binary"
	@echo "  start-server    - Start SeaweedFS mini with S3 and embedded IAM enabled"
	@echo "  stop-server     - Stop the test server"
	@echo "  test-errors     - Run the error compatibility checks"
	@echo "  test-with-server - Start server, run checks, stop server"
	@echo "  logs            - Show server logs"
	@echo "  clean           - Remove test artifacts"

build-weed:
	@echo "Building SeaweedFS binary..."
	@cd ../../../weed && go build -o weed_binary .
	@chmod +x $(WEED_BINARY)

check-deps: build-weed
	@command -v go >/dev/null 2>&1 || (echo "Go is required but not installed" && exit 1)
	@command -v python3 >/dev/null 2>&1 || (echo "python3 is required but not installed" && exit 1)
	@python3 -c "import botocore" 2>/dev/null || (echo "botocore is required: pip install botocore" && exit 1)
	@test -f $(WEED_BINARY) || (echo "SeaweedFS binary not found at $(WEED_BINARY)" && exit 1)
	@test -f $(S3_CONFIG) || (echo "S3 config not found at $(S3_CONFIG)" && exit 1)

start-server: check-deps
	@echo "Starting SeaweedFS mini for error compatibility tests..."
	@rm -f weed-server.pid
	@mkdir -p $(SERVER_DIR)
	@if curl -sS -o /dev/null http://127.0.0.1:$(S3_PORT)/ >/dev/null 2>&1; then \
		echo "port $(S3_PORT) is already served by another process; refusing to test against it"; \
		exit 1; \
	fi
	@$(WEED_BINARY) mini \
		-dir=$(SERVER_DIR) \
		-s3 \
		-s3.port=$(S3_PORT) \
		-s3.config=$(S3_CONFIG) \
		-s3.iam.readOnly=false \
		-master.peers=none \
		> weed-test.log 2>&1 & echo $$! > weed-server.pid
	@for i in $$(seq 1 60); do \
		kill -0 $$(cat weed-server.pid) 2>/dev/null || { \
			echo "SeaweedFS server exited during startup"; \
			echo "=== Server logs ==="; \
			test -f weed-test.log && cat weed-test.log || true; \
			exit 1; \
		}; \
		if curl -sS -o /dev/null http://127.0.0.1:$(S3_PORT)/ >/dev/null 2>&1; then \
			echo "SeaweedFS S3 server is ready on port $(S3_PORT)"; \
			sleep 3; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "SeaweedFS S3 server failed to start"; \
	echo "=== Server logs ==="; \
	test -f weed-test.log && cat weed-test.log || true; \
	$(MAKE) --no-print-directory stop-server; \
	exit 1

stop-server:
	@if [ -f weed-server.pid ]; then \
		echo "Stopping SeaweedFS server..."; \
		kill $$(cat weed-server.pid) 2>/dev/null || true; \
		sleep 2; \
		rm -f weed-server.pid; \
	fi

test-errors:
	@echo "Running S3/IAM error compatibility checks..."
	python3 s3_error_compat_test.py \
		--endpoint http://127.0.0.1:$(S3_PORT) \
		--access-key admin \
		--secret-key admin

test-with-server:
	@$(MAKE) start-server; START_RESULT=$$?; \
	if [ $$START_RESULT -eq 0 ]; then $(MAKE) test-errors; TEST_RESULT=$$?; else TEST_RESULT=$$START_RESULT; fi; \
	$(MAKE) stop-server; exit $$TEST_RESULT

logs:
	@test -f weed-test.log && tail -100 weed-test.log || echo "No log file found"

clean: stop-server
	@rm -rf $(SERVER_DIR) weed-test.log weed-server.pid
	@echo "Cleaned up test artifacts"
