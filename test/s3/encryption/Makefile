.PHONY: test clean setup

# Configuration
SEAWEED_BIN ?= weed
S3_PORT ?= 8333
FILER_PORT ?= 8888
MASTER_PORT ?= 9333
VOLUME_PORT ?= 8080

# Test data directory
TEST_DATA_DIR = test-volume-data

test: setup
	@echo "Running SSE-C integration tests..."
	go test -v -timeout 60s ./...

setup:
	@echo "Setting up SeaweedFS for SSE-C testing..."
	@mkdir -p $(TEST_DATA_DIR)
	@echo "Starting SeaweedFS servers..."
	@# Kill any existing processes
	@pkill -f "weed master" || true
	@pkill -f "weed volume" || true
	@pkill -f "weed filer" || true
	@pkill -f "weed s3" || true
	@sleep 2
	@# Start master server
	@$(SEAWEED_BIN) master -port=$(MASTER_PORT) -defaultReplication=001 > master.log 2>&1 &
	@sleep 2
	@# Start volume server
	@$(SEAWEED_BIN) volume -mserver=localhost:$(MASTER_PORT) -port=$(VOLUME_PORT) -dir=$(TEST_DATA_DIR) > volume.log 2>&1 &
	@sleep 2
	@# Start filer server
	@$(SEAWEED_BIN) filer -master=localhost:$(MASTER_PORT) -port=$(FILER_PORT) > filer.log 2>&1 &
	@sleep 2
	@# Start S3 server
	@$(SEAWEED_BIN) s3 -filer=localhost:$(FILER_PORT) -port=$(S3_PORT) -config=s3tests.conf > s3.log 2>&1 &
	@sleep 3
	@echo "SeaweedFS servers started. S3 API available at localhost:$(S3_PORT)"

clean:
	@echo "Cleaning up..."
	@pkill -f "weed master" || true
	@pkill -f "weed volume" || true
	@pkill -f "weed filer" || true
	@pkill -f "weed s3" || true
	@rm -rf $(TEST_DATA_DIR)
	@rm -f *.log
	@rm -f *.pid

benchmark:
	@echo "Running SSE-C benchmarks..."
	go test -v -bench=. -benchmem ./...

help:
	@echo "Available targets:"
	@echo "  test      - Run SSE-C integration tests"
	@echo "  setup     - Start SeaweedFS servers for testing"
	@echo "  clean     - Stop servers and clean up test data"
	@echo "  benchmark - Run performance benchmarks"
	@echo "  help      - Show this help message"
