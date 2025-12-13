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

# Start SeaweedFS servers with filer group configured
start-servers:
	@echo "Starting SeaweedFS servers with filer group: $(FILER_GROUP)"
	@mkdir -p /tmp/seaweedfs-filer-group-test
	@# Start master
	weed master -port=$(MASTER_PORT) -mdir=/tmp/seaweedfs-filer-group-test/master &
	@sleep 2
	@# Start volume server
	weed volume -port=$(VOLUME_PORT) -mserver=localhost:$(MASTER_PORT) -dir=/tmp/seaweedfs-filer-group-test/volume &
	@sleep 2
	@# Start filer with filer group
	weed filer -port=$(FILER_PORT) -master=localhost:$(MASTER_PORT) -filer.group=$(FILER_GROUP) &
	@sleep 2
	@# Start S3 API with filer group
	weed s3 -port=$(S3_PORT) -filer=localhost:$(FILER_PORT) &
	@sleep 2
	@echo "Servers started. Filer group: $(FILER_GROUP)"

stop-servers:
	@echo "Stopping SeaweedFS servers..."
	@pkill -f "weed master" || true
	@pkill -f "weed volume" || true
	@pkill -f "weed filer" || true
	@pkill -f "weed s3" || true
	@echo "Servers stopped"

clean:
	@rm -rf /tmp/seaweedfs-filer-group-test

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

