.PHONY: help build test test-basic test-performance test-failover test-agent clean up down logs

# Detect architecture
ARCH := $(shell uname -m)
ifeq ($(ARCH),arm64)
    DOCKER_PLATFORM := linux/arm64
else
    DOCKER_PLATFORM := linux/amd64
endif

# Default target
help:
	@echo "SeaweedMQ Integration Test Suite"
	@echo ""
	@echo "Available targets:"
	@echo "  build          - Build SeaweedFS Docker images"
	@echo "  test           - Run all integration tests"
	@echo "  test-basic     - Run basic pub/sub tests"
	@echo "  test-performance - Run performance tests"
	@echo "  test-failover  - Run failover tests"
	@echo "  test-agent     - Run agent tests"
	@echo "  up             - Start test environment (local build)"
	@echo "  up-prod        - Start test environment (production images)"
	@echo "  up-cluster     - Start cluster only (no test runner)"
	@echo "  down           - Stop test environment"
	@echo "  clean          - Clean up test environment and results"
	@echo "  logs           - Show container logs"

# Build SeaweedFS Docker images
build:
	@echo "Building SeaweedFS Docker image..."
	cd ../.. && docker build --platform linux/arm64 -f docker/Dockerfile.go_build -t chrislusf/seaweedfs:local .
	@echo "Building test runner image..."
	cd ../.. && docker build --platform linux/arm64 -f test/mq/Dockerfile.test -t seaweedfs-test-runner .

# Start the test environment
up: build
	@echo "Starting SeaweedMQ test environment..."
	docker-compose -f docker-compose.test.yml up -d master0 master1 master2
	@echo "Waiting for masters to be ready..."
	sleep 10
	docker-compose -f docker-compose.test.yml up -d volume1 volume2 volume3
	@echo "Waiting for volumes to be ready..."
	sleep 10
	docker-compose -f docker-compose.test.yml up -d filer1 filer2
	@echo "Waiting for filers to be ready..."
	sleep 15
	docker-compose -f docker-compose.test.yml up -d broker1 broker2 broker3
	@echo "Waiting for brokers to be ready..."
	sleep 20
	@echo "Test environment is ready!"

# Start the test environment with production images (no build required)
up-prod: build-test-runner
	@echo "Starting SeaweedMQ test environment with production images..."
	docker-compose -f docker-compose.production.yml up -d master0 master1 master2
	@echo "Waiting for masters to be ready..."
	sleep 10
	docker-compose -f docker-compose.production.yml up -d volume1 volume2 volume3
	@echo "Waiting for volumes to be ready..."
	sleep 10
	docker-compose -f docker-compose.production.yml up -d filer1 filer2
	@echo "Waiting for filers to be ready..."
	sleep 15
	docker-compose -f docker-compose.production.yml up -d broker1 broker2 broker3
	@echo "Waiting for brokers to be ready..."
	sleep 20
	@echo "Test environment is ready!"

# Build only the test runner image (for production setup)
build-test-runner:
	@echo "Building test runner image..."
	cd ../.. && docker build --platform linux/arm64 -f test/mq/Dockerfile.test -t seaweedfs-test-runner .

# Start cluster only (no test runner, no build required)
up-cluster:
	@echo "Starting SeaweedMQ cluster only..."
	docker-compose -f docker-compose.cluster.yml up -d master0 master1 master2
	@echo "Waiting for masters to be ready..."
	sleep 10
	docker-compose -f docker-compose.cluster.yml up -d volume1 volume2 volume3
	@echo "Waiting for volumes to be ready..."
	sleep 10
	docker-compose -f docker-compose.cluster.yml up -d filer1 filer2
	@echo "Waiting for filers to be ready..."
	sleep 15
	docker-compose -f docker-compose.cluster.yml up -d broker1 broker2 broker3
	@echo "Waiting for brokers to be ready..."
	sleep 20
	@echo "SeaweedMQ cluster is ready!"
	@echo "Masters: http://localhost:19333, http://localhost:19334, http://localhost:19335"
	@echo "Filers: http://localhost:18888, http://localhost:18889"
	@echo "Brokers: localhost:17777, localhost:17778, localhost:17779"

# Stop the test environment
down:
	@echo "Stopping SeaweedMQ test environment..."
	docker-compose -f docker-compose.test.yml down
	docker-compose -f docker-compose.production.yml down
	docker-compose -f docker-compose.cluster.yml down

# Clean up everything
clean:
	@echo "Cleaning up test environment..."
	docker-compose -f docker-compose.test.yml down -v
	docker system prune -f
	sudo rm -rf /tmp/test-results/*

# Show container logs
logs:
	docker-compose -f docker-compose.test.yml logs -f

# Run all integration tests
test: up
	@echo "Running all integration tests..."
	docker-compose -f docker-compose.test.yml run --rm test-runner \
		sh -c "go test -v -timeout=30m ./test/mq/integration/... -args -test.parallel=4"

# Run basic pub/sub tests
test-basic: up
	@echo "Running basic pub/sub tests..."
	docker-compose -f docker-compose.test.yml run --rm test-runner \
		sh -c "go test -v -timeout=10m ./test/mq/integration/ -run TestBasic"

# Run performance tests
test-performance: up
	@echo "Running performance tests..."
	docker-compose -f docker-compose.test.yml run --rm test-runner \
		sh -c "go test -v -timeout=20m ./test/mq/integration/ -run TestPerformance"

# Run failover tests
test-failover: up
	@echo "Running failover tests..."
	docker-compose -f docker-compose.test.yml run --rm test-runner \
		sh -c "go test -v -timeout=15m ./test/mq/integration/ -run TestFailover"

# Run agent tests
test-agent: up
	@echo "Running agent tests..."
	docker-compose -f docker-compose.test.yml run --rm test-runner \
		sh -c "go test -v -timeout=10m ./test/mq/integration/ -run TestAgent"

# Development targets
test-dev:
	@echo "Running tests in development mode (using local binaries)..."
	SEAWEED_MASTERS="localhost:19333,localhost:19334,localhost:19335" \
	SEAWEED_BROKERS="localhost:17777,localhost:17778,localhost:17779" \
	SEAWEED_FILERS="localhost:18888,localhost:18889" \
	go test -v -timeout=10m ./test/mq/integration/...

# Quick smoke test
smoke-test: up
	@echo "Running smoke test..."
	docker-compose -f docker-compose.test.yml run --rm test-runner \
		sh -c "go test -v -timeout=5m ./test/mq/integration/ -run TestBasicPublishSubscribe"

# Performance benchmarks
benchmark: up
	@echo "Running performance benchmarks..."
	docker-compose -f docker-compose.test.yml run --rm test-runner \
		sh -c "go test -v -timeout=30m -bench=. ./test/mq/integration/..."

# Check test environment health
health:
	@echo "Checking test environment health..."
	@echo "Masters:"
	@curl -s http://localhost:19333/cluster/status || echo "Master 0 not accessible"
	@curl -s http://localhost:19334/cluster/status || echo "Master 1 not accessible"
	@curl -s http://localhost:19335/cluster/status || echo "Master 2 not accessible"
	@echo ""
	@echo "Filers:"
	@curl -s http://localhost:18888/ || echo "Filer 1 not accessible"
	@curl -s http://localhost:18889/ || echo "Filer 2 not accessible"
	@echo ""
	@echo "Brokers:"
	@nc -z localhost 17777 && echo "Broker 1 accessible" || echo "Broker 1 not accessible"
	@nc -z localhost 17778 && echo "Broker 2 accessible" || echo "Broker 2 not accessible"  
	@nc -z localhost 17779 && echo "Broker 3 accessible" || echo "Broker 3 not accessible"

# Generate test reports
report:
	@echo "Generating test reports..."
	docker-compose -f docker-compose.test.yml run --rm test-runner \
		sh -c "go test -v -timeout=30m ./test/mq/integration/... -json > /test-results/test-report.json"

# Load testing
load-test: up
	@echo "Running load tests..."
	docker-compose -f docker-compose.test.yml run --rm test-runner \
		sh -c "go test -v -timeout=45m ./test/mq/integration/ -run TestLoad"

# View monitoring dashboards
monitoring:
	@echo "Starting monitoring stack..."
	docker-compose -f docker-compose.test.yml up -d prometheus grafana
	@echo "Prometheus: http://localhost:19090"
	@echo "Grafana: http://localhost:13000 (admin/admin)" 