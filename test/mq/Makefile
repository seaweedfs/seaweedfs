# SeaweedFS Message Queue Test Makefile

# Build configuration
GO_BUILD_CMD=go build -o bin/$(1) $(2)
GO_RUN_CMD=go run $(1) $(2)

# Default values
AGENT_ADDR?=localhost:16777
TOPIC_NAMESPACE?=test
TOPIC_NAME?=test-topic
PARTITION_COUNT?=4
MESSAGE_COUNT?=100
CONSUMER_GROUP?=test-consumer-group
CONSUMER_INSTANCE?=test-consumer-1

# Create bin directory
$(shell mkdir -p bin)

.PHONY: all build clean producer consumer test help

all: build

# Build targets
build: build-producer build-consumer

build-producer:
	@echo "Building producer..."
	$(call GO_BUILD_CMD,producer,./producer)

build-consumer:
	@echo "Building consumer..."
	$(call GO_BUILD_CMD,consumer,./consumer)

# Run targets
producer: build-producer
	@echo "Starting producer..."
	./bin/producer \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=$(TOPIC_NAME) \
		-partitions=$(PARTITION_COUNT) \
		-messages=$(MESSAGE_COUNT) \
		-publisher=test-producer \
		-size=1024 \
		-interval=100ms

consumer: build-consumer
	@echo "Starting consumer..."
	./bin/consumer \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=$(TOPIC_NAME) \
		-group=$(CONSUMER_GROUP) \
		-instance=$(CONSUMER_INSTANCE) \
		-max-partitions=10 \
		-window-size=100 \
		-offset=latest \
		-show-messages=true \
		-log-progress=true

# Run producer directly with go run
run-producer:
	@echo "Running producer directly..."
	$(call GO_RUN_CMD,./producer, \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=$(TOPIC_NAME) \
		-partitions=$(PARTITION_COUNT) \
		-messages=$(MESSAGE_COUNT) \
		-publisher=test-producer \
		-size=1024 \
		-interval=100ms)

# Run consumer directly with go run
run-consumer:
	@echo "Running consumer directly..."
	$(call GO_RUN_CMD,./consumer, \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=$(TOPIC_NAME) \
		-group=$(CONSUMER_GROUP) \
		-instance=$(CONSUMER_INSTANCE) \
		-max-partitions=10 \
		-window-size=100 \
		-offset=latest \
		-show-messages=true \
		-log-progress=true)

# Test scenarios
test: test-basic

test-basic: build
	@echo "Running basic producer/consumer test..."
	@echo "1. Starting consumer in background..."
	./bin/consumer \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=$(TOPIC_NAME) \
		-group=$(CONSUMER_GROUP) \
		-instance=$(CONSUMER_INSTANCE) \
		-offset=earliest \
		-show-messages=false \
		-log-progress=true & \
	CONSUMER_PID=$$!; \
	echo "Consumer PID: $$CONSUMER_PID"; \
	sleep 2; \
	echo "2. Starting producer..."; \
	./bin/producer \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=$(TOPIC_NAME) \
		-partitions=$(PARTITION_COUNT) \
		-messages=$(MESSAGE_COUNT) \
		-publisher=test-producer \
		-size=1024 \
		-interval=50ms; \
	echo "3. Waiting for consumer to process messages..."; \
	sleep 5; \
	echo "4. Stopping consumer..."; \
	kill $$CONSUMER_PID || true; \
	echo "Test completed!"

test-performance: build
	@echo "Running performance test..."
	@echo "1. Starting consumer in background..."
	./bin/consumer \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=perf-test \
		-group=perf-consumer-group \
		-instance=perf-consumer-1 \
		-offset=earliest \
		-show-messages=false \
		-log-progress=true & \
	CONSUMER_PID=$$!; \
	echo "Consumer PID: $$CONSUMER_PID"; \
	sleep 2; \
	echo "2. Starting high-throughput producer..."; \
	./bin/producer \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=perf-test \
		-partitions=8 \
		-messages=1000 \
		-publisher=perf-producer \
		-size=512 \
		-interval=10ms; \
	echo "3. Waiting for consumer to process messages..."; \
	sleep 10; \
	echo "4. Stopping consumer..."; \
	kill $$CONSUMER_PID || true; \
	echo "Performance test completed!"

test-multiple-consumers: build
	@echo "Running multiple consumers test..."
	@echo "1. Starting multiple consumers in background..."
	./bin/consumer \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=multi-test \
		-group=multi-consumer-group \
		-instance=consumer-1 \
		-offset=earliest \
		-show-messages=false \
		-log-progress=true & \
	CONSUMER1_PID=$$!; \
	./bin/consumer \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=multi-test \
		-group=multi-consumer-group \
		-instance=consumer-2 \
		-offset=earliest \
		-show-messages=false \
		-log-progress=true & \
	CONSUMER2_PID=$$!; \
	echo "Consumer PIDs: $$CONSUMER1_PID, $$CONSUMER2_PID"; \
	sleep 2; \
	echo "2. Starting producer..."; \
	./bin/producer \
		-agent=$(AGENT_ADDR) \
		-namespace=$(TOPIC_NAMESPACE) \
		-topic=multi-test \
		-partitions=8 \
		-messages=200 \
		-publisher=multi-producer \
		-size=256 \
		-interval=50ms; \
	echo "3. Waiting for consumers to process messages..."; \
	sleep 10; \
	echo "4. Stopping consumers..."; \
	kill $$CONSUMER1_PID $$CONSUMER2_PID || true; \
	echo "Multiple consumers test completed!"

# Clean up
clean:
	@echo "Cleaning up..."
	rm -rf bin/
	go clean -cache

# Help
help:
	@echo "SeaweedFS Message Queue Test Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make build              - Build producer and consumer binaries"
	@echo "  make producer          - Run producer (builds first)"
	@echo "  make consumer          - Run consumer (builds first)"
	@echo "  make run-producer      - Run producer directly with go run"
	@echo "  make run-consumer      - Run consumer directly with go run"
	@echo "  make test              - Run basic producer/consumer test"
	@echo "  make test-performance  - Run performance test"
	@echo "  make test-multiple-consumers - Run multiple consumers test"
	@echo "  make clean             - Clean up build artifacts"
	@echo ""
	@echo "Configuration (set via environment variables):"
	@echo "  AGENT_ADDR=10.21.152.113:16777  - MQ agent address"
	@echo "  TOPIC_NAMESPACE=test            - Topic namespace"
	@echo "  TOPIC_NAME=test-topic           - Topic name"
	@echo "  PARTITION_COUNT=4               - Number of partitions"
	@echo "  MESSAGE_COUNT=100               - Number of messages to produce"
	@echo "  CONSUMER_GROUP=test-consumer-group - Consumer group name"
	@echo "  CONSUMER_INSTANCE=test-consumer-1  - Consumer instance ID"
	@echo ""
	@echo "Examples:"
	@echo "  make producer MESSAGE_COUNT=1000 PARTITION_COUNT=8"
	@echo "  make consumer CONSUMER_GROUP=my-group"
	@echo "  make test AGENT_ADDR=10.21.152.113:16777 MESSAGE_COUNT=500" 