# Makefile for filer group S3 tests
#
# These tests verify that S3 bucket operations work correctly when a filer group is configured.
# They test that collections are named with the filer group prefix (e.g., {filerGroup}_{bucketName}).

.PHONY: test start-servers stop-servers clean

# Default filer group for testing
FILER_GROUP ?= testgroup

# Server configuration
MASTER_PORT ?= 9333
FILER_PORT ?= 8888
S3_PORT ?= 8333
VOLUME_PORT ?= 8080

# Test configuration
S3_ENDPOINT ?= http://localhost:$(S3_PORT)
MASTER_ADDRESS ?= localhost:$(MASTER_PORT)

# Directory for test data and PID files
TEST_DIR ?= /tmp/seaweedfs-filer-group-test

# Start SeaweedFS servers with filer group configured
start-servers:
	@echo "Starting SeaweedFS servers with filer group: $(FILER_GROUP)"
	@mkdir -p $(TEST_DIR)
	@# Start master
	weed master -port=$(MASTER_PORT) -mdir=$(TEST_DIR)/master & echo $$! > $(TEST_DIR)/master.pid
	@sleep 2
	@# Start volume server
	weed volume -port=$(VOLUME_PORT) -mserver=localhost:$(MASTER_PORT) -dir=$(TEST_DIR)/volume & echo $$! > $(TEST_DIR)/volume.pid
	@sleep 2
	@# Start filer with filer group
	weed filer -port=$(FILER_PORT) -master=localhost:$(MASTER_PORT) -filer.group=$(FILER_GROUP) & echo $$! > $(TEST_DIR)/filer.pid
	@sleep 2
	@# Start S3 API with filer group
	weed s3 -port=$(S3_PORT) -filer=localhost:$(FILER_PORT) & echo $$! > $(TEST_DIR)/s3.pid
	@sleep 2
	@echo "Servers started. Filer group: $(FILER_GROUP)"
	@echo "PID files stored in $(TEST_DIR)/"

stop-servers:
	@echo "Stopping SeaweedFS servers..."
	@for pidfile in $(TEST_DIR)/*.pid; do \
		if [ -f "$$pidfile" ]; then \
			pid=$$(cat "$$pidfile"); \
			echo "Stopping process $$pid from $$pidfile"; \
			kill $$pid 2>/dev/null || true; \
			rm -f "$$pidfile"; \
		fi; \
	done
	@echo "Servers stopped"

clean:
	@rm -rf $(TEST_DIR)

# Run tests
test:
	@echo "Running filer group S3 tests..."
	FILER_GROUP=$(FILER_GROUP) S3_ENDPOINT=$(S3_ENDPOINT) MASTER_ADDRESS=$(MASTER_ADDRESS) \
		go test -v -count=1 ./...

# Run tests with coverage
test-coverage:
	@echo "Running filer group S3 tests with coverage..."
	FILER_GROUP=$(FILER_GROUP) S3_ENDPOINT=$(S3_ENDPOINT) MASTER_ADDRESS=$(MASTER_ADDRESS) \
		go test -v -count=1 -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Full test cycle: start servers, run tests, stop servers
full-test: start-servers
	@sleep 3
	@$(MAKE) test || ($(MAKE) stop-servers; exit 1)
	@$(MAKE) stop-servers
