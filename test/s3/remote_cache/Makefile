# Remote Storage Cache Integration Tests
# Tests the remote object caching functionality with singleflight deduplication
# Uses two SeaweedFS instances: primary (with caching) and secondary (as remote storage)

.PHONY: all help build-weed check-deps start-remote stop-remote start-primary stop-primary \
        setup-remote test test-with-server clean logs logs-primary logs-remote health

all: test-with-server

# Configuration
WEED_BINARY := ../../../weed/weed_binary

ACCESS_KEY ?= some_access_key1
SECRET_KEY ?= some_secret_key1

# Primary SeaweedFS (the one being tested - has remote caching)
PRIMARY_S3_PORT := 8333
PRIMARY_MASTER_PORT := 9333
PRIMARY_METRICS_PORT := 9324
PRIMARY_DIR := ./test-primary-data

# Secondary SeaweedFS (acts as "remote" S3 storage)
REMOTE_S3_PORT := 8334
REMOTE_METRICS_PORT := 9325
REMOTE_DIR := ./test-remote-data

# Test configuration
TEST_TIMEOUT := 10m
TEST_PATTERN := TestRemoteCache

# Buckets
REMOTE_BUCKET := remotesourcebucket

# Default target
help:
	@echo "Remote Storage Cache Integration Tests"
	@echo ""
	@echo "Uses two SeaweedFS instances:"
	@echo "  - Primary (port $(PRIMARY_S3_PORT)): Being tested, has remote caching"
	@echo "  - Remote  (port $(REMOTE_S3_PORT)): Acts as remote S3 storage"
	@echo ""
	@echo "Available targets:"
	@echo "  help              - Show this help message"
	@echo "  build-weed        - Build the SeaweedFS binary"
	@echo "  check-deps        - Check dependencies"
	@echo "  start-remote      - Start remote SeaweedFS (secondary)"
	@echo "  stop-remote       - Stop remote SeaweedFS"
	@echo "  start-primary     - Start primary SeaweedFS"
	@echo "  stop-primary      - Stop primary SeaweedFS"
	@echo "  setup-remote      - Configure remote storage mount"
	@echo "  test              - Run tests (assumes servers are running)"
	@echo "  test-with-server  - Start servers, run tests, stop servers"
	@echo "  clean             - Clean up all resources"
	@echo "  logs              - Show server logs"

# Build the SeaweedFS binary
build-weed:
	@echo "Building SeaweedFS binary..."
	@cd ../../../weed && go build -o weed_binary .
	@chmod +x $(WEED_BINARY)
	@echo "SeaweedFS binary built"

check-deps: build-weed
	@echo "Checking dependencies..."
	@command -v go >/dev/null 2>&1 || (echo "Go is required" && exit 1)
	@test -f $(WEED_BINARY) || (echo "SeaweedFS binary not found" && exit 1)
	@echo "All dependencies available"

# Start remote SeaweedFS (acts as the "remote" S3 storage)
start-remote: check-deps
	@echo "Starting remote SeaweedFS (secondary instance)..."
	@rm -f remote-server.pid
	@mkdir -p $(REMOTE_DIR)
	@AWS_ACCESS_KEY_ID=$(ACCESS_KEY) AWS_SECRET_ACCESS_KEY=$(SECRET_KEY) $(WEED_BINARY) mini \
		-s3.port=$(REMOTE_S3_PORT) \
		-s3.allowDeleteBucketNotEmpty=true \
		-dir=$(REMOTE_DIR) \
		-ip.bind=0.0.0.0 \
		-metricsPort=$(REMOTE_METRICS_PORT) \
		> remote-weed.log 2>&1 & echo $$! > remote-server.pid
	@echo "Waiting for remote SeaweedFS to start..."
	@for i in $$(seq 1 60); do \
		if curl -s http://localhost:$(REMOTE_S3_PORT) >/dev/null 2>&1; then \
			echo "Remote SeaweedFS started on port $(REMOTE_S3_PORT)"; \
			exit 0; \
		fi; \
		sleep 3; \
	done; \
	echo "ERROR: Remote SeaweedFS failed to start"; \
	cat remote-weed.log; \
	exit 1

stop-remote:
	@echo "Stopping remote SeaweedFS..."
	@if [ -f remote-server.pid ]; then \
		kill -TERM $$(cat remote-server.pid) 2>/dev/null || true; \
		sleep 2; \
		kill -KILL $$(cat remote-server.pid) 2>/dev/null || true; \
		rm -f remote-server.pid; \
	fi
	@echo "Remote SeaweedFS stopped"

# Start primary SeaweedFS (the one being tested)
start-primary: check-deps
	@echo "Starting primary SeaweedFS..."
	@rm -f primary-server.pid
	@mkdir -p $(PRIMARY_DIR)
	@AWS_ACCESS_KEY_ID=$(ACCESS_KEY) AWS_SECRET_ACCESS_KEY=$(SECRET_KEY) $(WEED_BINARY) mini \
		-s3.port=$(PRIMARY_S3_PORT) \
		-s3.allowDeleteBucketNotEmpty=true \
		-dir=$(PRIMARY_DIR) \
		-ip.bind=0.0.0.0 \
		-metricsPort=$(PRIMARY_METRICS_PORT) \
		> primary-weed.log 2>&1 & echo $$! > primary-server.pid
	@echo "Waiting for primary SeaweedFS to start..."
	@for i in $$(seq 1 60); do \
		if curl -s http://localhost:$(PRIMARY_S3_PORT) >/dev/null 2>&1; then \
			echo "Primary SeaweedFS started on port $(PRIMARY_S3_PORT)"; \
			exit 0; \
		fi; \
		sleep 3; \
	done; \
	echo "ERROR: Primary SeaweedFS failed to start"; \
	cat primary-weed.log; \
	exit 1

stop-primary:
	@echo "Stopping primary SeaweedFS..."
	@if [ -f primary-server.pid ]; then \
		kill -TERM $$(cat primary-server.pid) 2>/dev/null || true; \
		sleep 2; \
		kill -KILL $$(cat primary-server.pid) 2>/dev/null || true; \
		rm -f primary-server.pid; \
	fi
	@echo "Primary SeaweedFS stopped"

# Create bucket on remote and configure remote storage mount on primary
setup-remote:
	@echo "Creating bucket on remote SeaweedFS..."
	@curl -s -X PUT "http://localhost:$(REMOTE_S3_PORT)/$(REMOTE_BUCKET)" || echo "Bucket may already exist"
	@sleep 1
	@echo "Configuring remote storage on primary..."
	@printf 'remote.configure -name=seaweedremote -type=s3 -s3.access_key=$(ACCESS_KEY) -s3.secret_key=$(SECRET_KEY) -s3.endpoint=http://localhost:$(REMOTE_S3_PORT) -s3.region=us-east-1\nexit\n' | $(WEED_BINARY) shell -master=localhost:$(PRIMARY_MASTER_PORT) 2>&1 || echo "remote.configure done"
	@sleep 2
	@echo "Mounting remote bucket on primary..."
	@printf 'remote.mount -dir=/buckets/remotemounted -remote=seaweedremote/$(REMOTE_BUCKET) -nonempty\nexit\n' | $(WEED_BINARY) shell -master=localhost:$(PRIMARY_MASTER_PORT) 2>&1 || echo "remote.mount done"
	@sleep 1
	@echo "Remote storage configured"

# Run tests
test: check-deps
	@echo "Running remote cache tests..."
	@go test -v -timeout=$(TEST_TIMEOUT) -run "$(TEST_PATTERN)" .
	@echo "Tests completed"

# Full test workflow
test-with-server: start-remote start-primary
	@sleep 3
	@$(MAKE) setup-remote || (echo "Remote setup failed" && $(MAKE) stop-primary stop-remote && exit 1)
	@sleep 2
	@echo "Running remote cache tests..."
	@$(MAKE) test || (echo "Tests failed" && tail -50 primary-weed.log && $(MAKE) stop-primary stop-remote && exit 1)
	@$(MAKE) stop-primary stop-remote
	@echo "All tests passed"

# Show logs
logs:
	@echo "=== Primary SeaweedFS Logs ==="
	@if [ -f primary-weed.log ]; then tail -50 primary-weed.log; else echo "No log file"; fi
	@echo ""
	@echo "=== Remote SeaweedFS Logs ==="
	@if [ -f remote-weed.log ]; then tail -50 remote-weed.log; else echo "No log file"; fi

logs-primary:
	@if [ -f primary-weed.log ]; then tail -f primary-weed.log; else echo "No log file"; fi

logs-remote:
	@if [ -f remote-weed.log ]; then tail -f remote-weed.log; else echo "No log file"; fi

# Clean up
clean:
	@$(MAKE) stop-primary
	@$(MAKE) stop-remote
	@rm -f primary-weed.log remote-weed.log primary-server.pid remote-server.pid
	@rm -rf $(PRIMARY_DIR) $(REMOTE_DIR)
	@rm -f remote_cache.test
	@go clean -testcache
	@echo "Cleanup completed"

# Health check
health:
	@echo "Checking server status..."
	@curl -s http://localhost:$(PRIMARY_S3_PORT) >/dev/null 2>&1 && echo "Primary S3 ($(PRIMARY_S3_PORT)): UP" || echo "Primary S3 ($(PRIMARY_S3_PORT)): DOWN"
	@curl -s http://localhost:$(REMOTE_S3_PORT) >/dev/null 2>&1 && echo "Remote S3 ($(REMOTE_S3_PORT)): UP" || echo "Remote S3 ($(REMOTE_S3_PORT)): DOWN"
