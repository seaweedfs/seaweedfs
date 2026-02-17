# Makefile for Iceberg REST Catalog Integration Tests
#
# This Makefile provides easy-to-use targets for running integration tests
# and starting a local development environment.

.PHONY: build mini test test-pyiceberg clean

# Root directory of the SeaweedFS project
PROJECT_ROOT = ../../..
WEED_BIN = $(PROJECT_ROOT)/weed/weed

# Build the weed binary
build:
	@echo "Building SeaweedFS binary..."
	cd $(PROJECT_ROOT)/weed && go build -v

# Start SeaweedFS mini cluster for manual testing
# Uses default ports: S3=8333, Iceberg=8182
mini: build
	@echo "Starting SeaweedFS mini cluster..."
	$(WEED_BIN) mini -s3.port.iceberg=8182 & echo $$! > weed-mini.pid; wait

# Run all integration tests in this directory
test: build
	@echo "Running all Iceberg catalog integration tests..."
	go test -v .

# Run only the PyIceberg compatibility tests (requires Docker)
test-pyiceberg: build
	@echo "Running PyIceberg compatibility tests..."
	go test -v -run TestPyIcebergRestCatalog .

# Clean up temporary data and stop weed mini if running
clean:
	@echo "Cleaning up..."
	@test -f weed-mini.pid && kill $$(cat weed-mini.pid) && rm weed-mini.pid || true
	@rm -rf /tmp/seaweed-iceberg-test-*
