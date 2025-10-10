# Kafka Client Load Test Makefile
# Provides convenient targets for running load tests against SeaweedFS Kafka Gateway

.PHONY: help build start stop restart clean test quick-test stress-test endurance-test monitor logs status

# Configuration
DOCKER_COMPOSE := docker compose
PROJECT_NAME := kafka-client-loadtest
CONFIG_FILE := config/loadtest.yaml

# Build configuration
GOARCH ?= arm64
GOOS ?= linux

# Default test parameters
TEST_MODE ?= comprehensive
TEST_DURATION ?= 300s
PRODUCER_COUNT ?= 10
CONSUMER_COUNT ?= 5
MESSAGE_RATE ?= 1000
MESSAGE_SIZE ?= 1024

# Colors for output
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m

help: ## Show this help message
	@echo "Kafka Client Load Test Makefile"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(BLUE)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Environment variables:"
	@echo "  TEST_MODE       Test mode: producer, consumer, comprehensive (default: comprehensive)"
	@echo "  TEST_DURATION   Test duration (default: 300s)"
	@echo "  PRODUCER_COUNT  Number of producers (default: 10)"
	@echo "  CONSUMER_COUNT  Number of consumers (default: 5)"
	@echo "  MESSAGE_RATE    Messages per second per producer (default: 1000)"
	@echo "  MESSAGE_SIZE    Message size in bytes (default: 1024)"
	@echo ""
	@echo "Examples:"
	@echo "  make test                              # Run default comprehensive test"
	@echo "  make test TEST_DURATION=10m           # Run 10-minute test"
	@echo "  make quick-test                        # Run quick smoke test (rebuilds gateway)"
	@echo "  make stress-test                       # Run high-load stress test"
	@echo "  make test TEST_MODE=producer           # Producer-only test"
	@echo "  make schema-test                       # Run schema integration test with Schema Registry"
	@echo "  make schema-quick-test                 # Run quick schema test (30s timeout)"
	@echo "  make schema-loadtest                   # Run load test with schemas enabled"
	@echo "  make build-binary                      # Build SeaweedFS binary locally for Linux"
	@echo "  make build-gateway                     # Build Kafka Gateway (builds binary + Docker image)"
	@echo "  make build-gateway-clean               # Build Kafka Gateway with no cache (fresh build)"

build: ## Build the load test application
	@echo "$(BLUE)Building load test application...$(NC)"
	$(DOCKER_COMPOSE) build kafka-client-loadtest
	@echo "$(GREEN)Build completed$(NC)"

build-binary: ## Build the SeaweedFS binary locally for Linux
	@echo "$(BLUE)Building SeaweedFS binary locally for $(GOOS) $(GOARCH)...$(NC)"
	cd ../../.. && \
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build \
		-ldflags="-s -w" \
		-tags "5BytesOffset" \
		-o test/kafka/kafka-client-loadtest/weed-$(GOOS)-$(GOARCH) \
		weed/weed.go
	@echo "$(GREEN)Binary build completed: weed-$(GOOS)-$(GOARCH)$(NC)"

build-gateway: build-binary ## Build the Kafka Gateway with latest changes
	@echo "$(BLUE)Building Kafka Gateway Docker image...$(NC)"
	CACHE_BUST=$$(date +%s) $(DOCKER_COMPOSE) build kafka-gateway
	@echo "$(GREEN)Kafka Gateway build completed$(NC)"

build-gateway-clean: build-binary ## Build the Kafka Gateway with no cache (force fresh build)
	@echo "$(BLUE)Building Kafka Gateway Docker image with no cache...$(NC)"
	$(DOCKER_COMPOSE) build --no-cache kafka-gateway
	@echo "$(GREEN)Kafka Gateway clean build completed$(NC)"

setup: ## Set up monitoring and configuration
	@echo "$(BLUE)Setting up monitoring configuration...$(NC)"
	./scripts/setup-monitoring.sh
	@echo "$(GREEN)Setup completed$(NC)"

start: build-gateway ## Start the infrastructure services (without load test)
	@echo "$(BLUE)Starting SeaweedFS infrastructure...$(NC)"
	$(DOCKER_COMPOSE) up -d \
		seaweedfs-master \
		seaweedfs-volume \
		seaweedfs-filer \
		seaweedfs-mq-broker \
		kafka-gateway \
		schema-registry-init \
		schema-registry
	@echo "$(GREEN)Infrastructure started$(NC)"
	@echo "Waiting for services to be ready..."
	./scripts/wait-for-services.sh wait
	@echo "$(GREEN)All services are ready!$(NC)"

stop: ## Stop all services
	@echo "$(BLUE)Stopping all services...$(NC)"
	$(DOCKER_COMPOSE) --profile loadtest --profile monitoring down
	@echo "$(GREEN)Services stopped$(NC)"

restart: stop start ## Restart all services

clean: ## Clean up all resources (containers, volumes, networks, local data)
	@echo "$(YELLOW)Warning: This will remove all volumes and data!$(NC)"
	@echo "Press Ctrl+C to cancel, or wait 5 seconds to continue..."
	@sleep 5
	@echo "$(BLUE)Cleaning up all resources...$(NC)"
	$(DOCKER_COMPOSE) --profile loadtest --profile monitoring down -v --remove-orphans
	docker system prune -f
	@if [ -f "weed-linux-arm64" ]; then \
		echo "$(BLUE)Removing local binary...$(NC)"; \
		rm -f weed-linux-arm64; \
	fi
	@if [ -d "data" ]; then \
		echo "$(BLUE)Removing ALL local data directories (including offset state)...$(NC)"; \
		rm -rf data/*; \
	fi
	@echo "$(GREEN)Cleanup completed - all data removed$(NC)"

clean-binary: ## Clean up only the local binary
	@echo "$(BLUE)Removing local binary...$(NC)"
	@rm -f weed-linux-arm64
	@echo "$(GREEN)Binary cleanup completed$(NC)"

status: ## Show service status
	@echo "$(BLUE)Service Status:$(NC)"
	$(DOCKER_COMPOSE) ps

logs: ## Show logs from all services
	$(DOCKER_COMPOSE) logs -f

test: start ## Run the comprehensive load test
	@echo "$(BLUE)Running Kafka client load test...$(NC)"
	@echo "Mode: $(TEST_MODE), Duration: $(TEST_DURATION)"
	@echo "Producers: $(PRODUCER_COUNT), Consumers: $(CONSUMER_COUNT)"
	@echo "Message Rate: $(MESSAGE_RATE) msgs/sec, Size: $(MESSAGE_SIZE) bytes"
	@echo ""
	@docker rm -f kafka-client-loadtest-runner 2>/dev/null || true
	TEST_MODE=$(TEST_MODE) TEST_DURATION=$(TEST_DURATION) PRODUCER_COUNT=$(PRODUCER_COUNT) CONSUMER_COUNT=$(CONSUMER_COUNT) MESSAGE_RATE=$(MESSAGE_RATE) MESSAGE_SIZE=$(MESSAGE_SIZE) VALUE_TYPE=$(VALUE_TYPE) $(DOCKER_COMPOSE) --profile loadtest up --abort-on-container-exit kafka-client-loadtest
	@echo "$(GREEN)Load test completed!$(NC)"
	@$(MAKE) show-results

quick-test: build-gateway ## Run a quick smoke test (1 min, low load, WITH schemas)
	@echo "$(BLUE)================================================================$(NC)"
	@echo "$(BLUE)    Quick Test (Low Load, WITH Schema Registry + Avro)       $(NC)"
	@echo "$(BLUE)  - Duration: 1 minute                                        $(NC)"
	@echo "$(BLUE)  - Load: 1 producer × 10 msg/sec = 10 total msg/sec         $(NC)"
	@echo "$(BLUE)  - Message Type: Avro (with schema encoding)                $(NC)"
	@echo "$(BLUE)  - Schema-First: Registers schemas BEFORE producing         $(NC)"
	@echo "$(BLUE)================================================================$(NC)"
	@echo ""
	@$(MAKE) start
	@echo ""
	@echo "$(BLUE)=== Step 1: Registering schemas in Schema Registry ===$(NC)"
	@echo "$(YELLOW)[WARN] IMPORTANT: Schemas MUST be registered before producing Avro messages!$(NC)"
	@./scripts/register-schemas.sh full
	@echo "$(GREEN)- Schemas registered successfully$(NC)"
	@echo ""
	@echo "$(BLUE)=== Step 2: Running load test with Avro messages ===$(NC)"
	@$(MAKE) test \
		TEST_MODE=comprehensive \
		TEST_DURATION=60s \
		PRODUCER_COUNT=1 \
		CONSUMER_COUNT=1 \
		MESSAGE_RATE=10 \
		MESSAGE_SIZE=256 \
		VALUE_TYPE=avro
	@echo ""
	@echo "$(GREEN)================================================================$(NC)"
	@echo "$(GREEN)                    Quick Test Complete!                      $(NC)"
	@echo "$(GREEN)  - Schema Registration                                       $(NC)"
	@echo "$(GREEN)  - Avro Message Production                                   $(NC)"
	@echo "$(GREEN)  - Message Consumption                                       $(NC)"
	@echo "$(GREEN)================================================================$(NC)"

standard-test: ## Run a standard load test (2 min, medium load, WITH Schema Registry + Avro)
	@echo "$(BLUE)================================================================$(NC)"
	@echo "$(BLUE)      Standard Test (Medium Load, WITH Schema Registry)      $(NC)"
	@echo "$(BLUE)  - Duration: 2 minutes                                       $(NC)"
	@echo "$(BLUE)  - Load: 2 producers × 50 msg/sec = 100 total msg/sec       $(NC)"
	@echo "$(BLUE)  - Message Type: Avro (with schema encoding)                $(NC)"
	@echo "$(BLUE)  - IMPORTANT: Schemas registered FIRST in Schema Registry   $(NC)"
	@echo "$(BLUE)================================================================$(NC)"
	@echo ""
	@$(MAKE) start
	@echo ""
	@echo "$(BLUE)=== Step 1: Registering schemas in Schema Registry ===$(NC)"
	@echo "$(YELLOW)Note: Schemas MUST be registered before producing Avro messages!$(NC)"
	@./scripts/register-schemas.sh full
	@echo "$(GREEN)- Schemas registered$(NC)"
	@echo ""
	@echo "$(BLUE)=== Step 2: Running load test with Avro messages ===$(NC)"
	@$(MAKE) test \
		TEST_MODE=comprehensive \
		TEST_DURATION=2m \
		PRODUCER_COUNT=2 \
		CONSUMER_COUNT=2 \
		MESSAGE_RATE=50 \
		MESSAGE_SIZE=512 \
		VALUE_TYPE=avro
	@echo ""
	@echo "$(GREEN)================================================================$(NC)"
	@echo "$(GREEN)                  Standard Test Complete!                     $(NC)"
	@echo "$(GREEN)================================================================$(NC)"

stress-test: ## Run a stress test (10 minutes, high load) with schemas
	@echo "$(BLUE)Starting stress test with schema registration...$(NC)"
	@$(MAKE) start
	@echo "$(BLUE)Registering schemas with Schema Registry...$(NC)"
	@./scripts/register-schemas.sh full
	@echo "$(BLUE)Running stress test with registered schemas...$(NC)"
	@$(MAKE) test \
		TEST_MODE=comprehensive \
		TEST_DURATION=10m \
		PRODUCER_COUNT=20 \
		CONSUMER_COUNT=10 \
		MESSAGE_RATE=2000 \
		MESSAGE_SIZE=2048 \
		VALUE_TYPE=avro

endurance-test: ## Run an endurance test (30 minutes, sustained load) with schemas
	@echo "$(BLUE)Starting endurance test with schema registration...$(NC)"
	@$(MAKE) start
	@echo "$(BLUE)Registering schemas with Schema Registry...$(NC)"
	@./scripts/register-schemas.sh full
	@echo "$(BLUE)Running endurance test with registered schemas...$(NC)"
	@$(MAKE) test \
		TEST_MODE=comprehensive \
		TEST_DURATION=30m \
		PRODUCER_COUNT=10 \
		CONSUMER_COUNT=5 \
		MESSAGE_RATE=1000 \
		MESSAGE_SIZE=1024 \
		VALUE_TYPE=avro

producer-test: ## Run producer-only load test
	@$(MAKE) test TEST_MODE=producer

consumer-test: ## Run consumer-only load test (requires existing messages)
	@$(MAKE) test TEST_MODE=consumer

register-schemas: start ## Register schemas with Schema Registry
	@echo "$(BLUE)Registering schemas with Schema Registry...$(NC)"
	@./scripts/register-schemas.sh full
	@echo "$(GREEN)Schema registration completed!$(NC)"

verify-schemas: ## Verify schemas are registered in Schema Registry
	@echo "$(BLUE)Verifying schemas in Schema Registry...$(NC)"
	@./scripts/register-schemas.sh verify
	@echo "$(GREEN)Schema verification completed!$(NC)"

list-schemas: ## List all registered schemas in Schema Registry
	@echo "$(BLUE)Listing registered schemas...$(NC)"
	@./scripts/register-schemas.sh list

cleanup-schemas: ## Clean up test schemas from Schema Registry
	@echo "$(YELLOW)Cleaning up test schemas...$(NC)"
	@./scripts/register-schemas.sh cleanup
	@echo "$(GREEN)Schema cleanup completed!$(NC)"

schema-test: start ## Run schema integration test (with Schema Registry)
	@echo "$(BLUE)Running schema integration test...$(NC)"
	@echo "Testing Schema Registry integration with schematized topics"
	@echo ""
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o schema-test-linux test_schema_integration.go
	docker run --rm --network kafka-client-loadtest \
		-v $(PWD)/schema-test-linux:/usr/local/bin/schema-test \
		alpine:3.18 /usr/local/bin/schema-test
	@rm -f schema-test-linux
	@echo "$(GREEN)Schema integration test completed!$(NC)"

schema-quick-test: start ## Run quick schema test (lighter version)
	@echo "$(BLUE)Running quick schema test...$(NC)"
	@echo "Testing basic schema functionality"
	@echo ""
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o schema-test-linux test_schema_integration.go
	timeout 60s docker run --rm --network kafka-client-loadtest \
		-v $(PWD)/schema-test-linux:/usr/local/bin/schema-test \
		alpine:3.18 /usr/local/bin/schema-test || true
	@rm -f schema-test-linux
	@echo "$(GREEN)Quick schema test completed!$(NC)"

simple-schema-test: start ## Run simple schema test (step-by-step)
	@echo "$(BLUE)Running simple schema test...$(NC)"
	@echo "Step-by-step schema functionality test"
	@echo ""
	@mkdir -p simple-test
	@cp simple_schema_test.go simple-test/main.go
	cd simple-test && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../simple-schema-test-linux .
	docker run --rm --network kafka-client-loadtest \
		-v $(PWD)/simple-schema-test-linux:/usr/local/bin/simple-schema-test \
		alpine:3.18 /usr/local/bin/simple-schema-test
	@rm -f simple-schema-test-linux
	@rm -rf simple-test
	@echo "$(GREEN)Simple schema test completed!$(NC)"

basic-schema-test: start ## Run basic schema test (manual schema handling without Schema Registry)
	@echo "$(BLUE)Running basic schema test...$(NC)"
	@echo "Testing schema functionality without Schema Registry dependency"
	@echo ""
	@mkdir -p basic-test
	@cp basic_schema_test.go basic-test/main.go
	cd basic-test && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../basic-schema-test-linux .
	timeout 60s docker run --rm --network kafka-client-loadtest \
		-v $(PWD)/basic-schema-test-linux:/usr/local/bin/basic-schema-test \
		alpine:3.18 /usr/local/bin/basic-schema-test
	@rm -f basic-schema-test-linux
	@rm -rf basic-test
	@echo "$(GREEN)Basic schema test completed!$(NC)"

schema-loadtest: start ## Run load test with schemas enabled
	@echo "$(BLUE)Running schema-enabled load test...$(NC)"
	@echo "Mode: comprehensive with schemas, Duration: 3m"
	@echo "Producers: 3, Consumers: 2, Message Rate: 50 msgs/sec"
	@echo ""
	TEST_MODE=comprehensive \
	TEST_DURATION=3m \
	PRODUCER_COUNT=3 \
	CONSUMER_COUNT=2 \
	MESSAGE_RATE=50 \
	MESSAGE_SIZE=1024 \
	SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
	$(DOCKER_COMPOSE) --profile loadtest up --abort-on-container-exit kafka-client-loadtest
	@echo "$(GREEN)Schema load test completed!$(NC)"
	@$(MAKE) show-results

monitor: setup ## Start monitoring stack (Prometheus + Grafana)
	@echo "$(BLUE)Starting monitoring stack...$(NC)"
	$(DOCKER_COMPOSE) --profile monitoring up -d prometheus grafana
	@echo "$(GREEN)Monitoring stack started!$(NC)"
	@echo ""
	@echo "Access points:"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"

monitor-stop: ## Stop monitoring stack
	@echo "$(BLUE)Stopping monitoring stack...$(NC)"
	$(DOCKER_COMPOSE) --profile monitoring stop prometheus grafana
	@echo "$(GREEN)Monitoring stack stopped$(NC)"

test-with-monitoring: monitor start ## Run test with monitoring enabled
	@echo "$(BLUE)Running load test with monitoring...$(NC)"
	@$(MAKE) test
	@echo ""
	@echo "$(GREEN)Test completed! Check the monitoring dashboards:$(NC)"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Grafana:    http://localhost:3000 (admin/admin)"

show-results: ## Show test results
	@echo "$(BLUE)Test Results Summary:$(NC)"
	@if $(DOCKER_COMPOSE) ps -q kafka-client-loadtest-runner >/dev/null 2>&1; then \
		$(DOCKER_COMPOSE) exec -T kafka-client-loadtest-runner curl -s http://localhost:8080/stats 2>/dev/null || echo "Results not available"; \
	else \
		echo "Load test container not running"; \
	fi
	@echo ""
	@if [ -d "test-results" ]; then \
		echo "Detailed results saved to: test-results/"; \
		ls -la test-results/ 2>/dev/null || true; \
	fi

health-check: ## Check health of all services
	@echo "$(BLUE)Checking service health...$(NC)"
	./scripts/wait-for-services.sh check

validate-setup: ## Validate the test setup
	@echo "$(BLUE)Validating test setup...$(NC)"
	@echo "Checking Docker and Docker Compose..."
	@docker --version
	@docker compose version || docker-compose --version
	@echo ""
	@echo "Checking configuration file..."
	@if [ -f "$(CONFIG_FILE)" ]; then \
		echo "- Configuration file exists: $(CONFIG_FILE)"; \
	else \
		echo "x Configuration file not found: $(CONFIG_FILE)"; \
		exit 1; \
	fi
	@echo ""
	@echo "Checking scripts..."
	@for script in scripts/*.sh; do \
		if [ -x "$$script" ]; then \
			echo "- $$script is executable"; \
		else \
			echo "x $$script is not executable"; \
		fi; \
	done
	@echo "$(GREEN)Setup validation completed$(NC)"

dev-env: ## Set up development environment
	@echo "$(BLUE)Setting up development environment...$(NC)"
	@echo "Installing Go dependencies..."
	go mod download
	go mod tidy
	@echo "$(GREEN)Development environment ready$(NC)"

benchmark: ## Run comprehensive benchmarking suite
	@echo "$(BLUE)Running comprehensive benchmark suite...$(NC)"
	@echo "This will run multiple test scenarios and collect detailed metrics"
	@echo ""
	@$(MAKE) quick-test
	@sleep 10
	@$(MAKE) standard-test  
	@sleep 10
	@$(MAKE) stress-test
	@echo "$(GREEN)Benchmark suite completed!$(NC)"

# Advanced targets
debug: ## Start services in debug mode with verbose logging
	@echo "$(BLUE)Starting services in debug mode...$(NC)"
	SEAWEEDFS_LOG_LEVEL=debug \
	KAFKA_LOG_LEVEL=debug \
	$(DOCKER_COMPOSE) up \
		seaweedfs-master \
		seaweedfs-volume \
		seaweedfs-filer \
		seaweedfs-mq-broker \
		kafka-gateway \
		schema-registry

attach-loadtest: ## Attach to running load test container
	$(DOCKER_COMPOSE) exec kafka-client-loadtest-runner /bin/sh

exec-master: ## Execute shell in SeaweedFS master container
	$(DOCKER_COMPOSE) exec seaweedfs-master /bin/sh

exec-filer: ## Execute shell in SeaweedFS filer container
	$(DOCKER_COMPOSE) exec seaweedfs-filer /bin/sh

exec-gateway: ## Execute shell in Kafka gateway container
	$(DOCKER_COMPOSE) exec kafka-gateway /bin/sh

# Utility targets
ps: status ## Alias for status

up: start ## Alias for start

down: stop ## Alias for stop

# Help is the default target
.DEFAULT_GOAL := help
