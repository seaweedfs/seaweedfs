# Kafka Integration Testing Makefile

# Configuration
DOCKER_COMPOSE ?= docker-compose
TEST_TIMEOUT ?= 10m
KAFKA_BOOTSTRAP_SERVERS ?= localhost:9092
KAFKA_GATEWAY_URL ?= localhost:9093
SCHEMA_REGISTRY_URL ?= http://localhost:8081

# Colors for output
BLUE := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
NC := \033[0m # No Color

.PHONY: help setup test test-unit test-integration test-e2e clean logs status

help: ## Show this help message
	@echo "$(BLUE)SeaweedFS Kafka Integration Testing$(NC)"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Set up test environment (Kafka + Schema Registry + SeaweedFS)
	@echo "$(YELLOW)Setting up Kafka integration test environment...$(NC)"
	@$(DOCKER_COMPOSE) up -d zookeeper kafka schema-registry
	@echo "$(BLUE)Waiting for Kafka ecosystem to be ready...$(NC)"
	@sleep 30
	@$(DOCKER_COMPOSE) up -d seaweedfs-master seaweedfs-volume seaweedfs-filer
	@echo "$(BLUE)Waiting for SeaweedFS to be ready...$(NC)"
	@sleep 20
	@$(DOCKER_COMPOSE) up -d seaweedfs-mq-broker seaweedfs-mq-agent
	@echo "$(BLUE)Waiting for SeaweedFS MQ to be ready...$(NC)"
	@sleep 15
	@$(DOCKER_COMPOSE) up -d kafka-gateway
	@echo "$(BLUE)Waiting for Kafka Gateway to be ready...$(NC)"
	@sleep 10
	@echo "$(GREEN)✅ Test environment ready!$(NC)"

setup-schemas: setup ## Set up test environment and register schemas
	@echo "$(YELLOW)Registering test schemas...$(NC)"
	@$(DOCKER_COMPOSE) --profile setup run --rm test-setup
	@echo "$(GREEN)✅ Schemas registered!$(NC)"

test: setup-schemas test-unit test-integration ## Run all tests

test-unit: ## Run unit tests for Kafka components
	@echo "$(YELLOW)Running Kafka component unit tests...$(NC)"
	@cd ../../ && go test -v -timeout=$(TEST_TIMEOUT) ./weed/mq/kafka/...

test-integration: ## Run integration tests with real Kafka and Schema Registry
	@echo "$(YELLOW)Running Kafka integration tests...$(NC)"
	@cd ../../ && KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
		KAFKA_GATEWAY_URL=$(KAFKA_GATEWAY_URL) \
		SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
		go test -v -timeout=$(TEST_TIMEOUT) ./test/kafka/ -run Integration

test-schema: ## Run schema registry integration tests
	@echo "$(YELLOW)Running schema registry integration tests...$(NC)"
	@cd ../../ && SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
		go test -v -timeout=$(TEST_TIMEOUT) ./test/kafka/ -run Schema

test-e2e: setup-schemas ## Run end-to-end tests with complete setup
	@echo "$(YELLOW)Running end-to-end Kafka tests...$(NC)"
	@$(DOCKER_COMPOSE) --profile producer run --rm kafka-producer
	@sleep 5
	@cd ../../ && KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
		KAFKA_GATEWAY_URL=$(KAFKA_GATEWAY_URL) \
		SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
		go test -v -timeout=$(TEST_TIMEOUT) ./test/kafka/ -run E2E

test-performance: setup-schemas ## Run performance benchmarks
	@echo "$(YELLOW)Running Kafka performance benchmarks...$(NC)"
	@cd ../../ && KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
		KAFKA_GATEWAY_URL=$(KAFKA_GATEWAY_URL) \
		SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
		go test -v -timeout=$(TEST_TIMEOUT) -bench=. ./test/kafka/

test-sarama: setup-schemas ## Run Sarama client tests
	@echo "$(YELLOW)Running Sarama client tests...$(NC)"
	@cd ../../ && KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
		KAFKA_GATEWAY_URL=$(KAFKA_GATEWAY_URL) \
		go test -v -timeout=$(TEST_TIMEOUT) ./test/kafka/ -run Sarama

test-kafka-go: setup-schemas ## Run kafka-go client tests
	@echo "$(YELLOW)Running kafka-go client tests...$(NC)"
	@cd ../../ && KAFKA_BOOTSTRAP_SERVERS=$(KAFKA_BOOTSTRAP_SERVERS) \
		KAFKA_GATEWAY_URL=$(KAFKA_GATEWAY_URL) \
		go test -v -timeout=$(TEST_TIMEOUT) ./test/kafka/ -run KafkaGo

clean: ## Clean up test environment
	@echo "$(YELLOW)Cleaning up test environment...$(NC)"
	@$(DOCKER_COMPOSE) down -v --remove-orphans
	@docker system prune -f
	@echo "$(GREEN)✅ Environment cleaned up!$(NC)"

logs: ## Show logs from all services
	@$(DOCKER_COMPOSE) logs --tail=50 -f

logs-kafka: ## Show Kafka logs
	@$(DOCKER_COMPOSE) logs --tail=100 -f kafka

logs-schema-registry: ## Show Schema Registry logs
	@$(DOCKER_COMPOSE) logs --tail=100 -f schema-registry

logs-seaweedfs: ## Show SeaweedFS logs
	@$(DOCKER_COMPOSE) logs --tail=100 -f seaweedfs-master seaweedfs-volume seaweedfs-filer seaweedfs-mq-broker seaweedfs-mq-agent

logs-gateway: ## Show Kafka Gateway logs
	@$(DOCKER_COMPOSE) logs --tail=100 -f kafka-gateway

status: ## Show status of all services
	@echo "$(BLUE)Service Status:$(NC)"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "$(BLUE)Kafka Status:$(NC)"
	@curl -s http://localhost:9092 > /dev/null && echo "✅ Kafka accessible" || echo "❌ Kafka not accessible"
	@echo ""
	@echo "$(BLUE)Schema Registry Status:$(NC)"
	@curl -s $(SCHEMA_REGISTRY_URL)/subjects > /dev/null && echo "✅ Schema Registry accessible" || echo "❌ Schema Registry not accessible"
	@echo ""
	@echo "$(BLUE)Kafka Gateway Status:$(NC)"
	@nc -z localhost 9093 && echo "✅ Kafka Gateway accessible" || echo "❌ Kafka Gateway not accessible"

debug: ## Debug test environment
	@echo "$(BLUE)Debug Information:$(NC)"
	@echo "Kafka Bootstrap Servers: $(KAFKA_BOOTSTRAP_SERVERS)"
	@echo "Schema Registry URL: $(SCHEMA_REGISTRY_URL)"
	@echo "Kafka Gateway URL: $(KAFKA_GATEWAY_URL)"
	@echo ""
	@echo "Docker Compose Status:"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "Network connectivity:"
	@docker network ls | grep kafka-integration-test || echo "No Kafka test network found"
	@echo ""
	@echo "Schema Registry subjects:"
	@curl -s $(SCHEMA_REGISTRY_URL)/subjects 2>/dev/null || echo "Schema Registry not accessible"

# Development targets
dev-kafka: ## Start only Kafka ecosystem for development
	@$(DOCKER_COMPOSE) up -d zookeeper kafka schema-registry
	@sleep 20
	@$(DOCKER_COMPOSE) --profile setup run --rm test-setup

dev-seaweedfs: ## Start only SeaweedFS for development
	@$(DOCKER_COMPOSE) up -d seaweedfs-master seaweedfs-volume seaweedfs-filer seaweedfs-mq-broker seaweedfs-mq-agent

dev-gateway: dev-seaweedfs ## Start Kafka Gateway for development
	@$(DOCKER_COMPOSE) up -d kafka-gateway

dev-test: dev-kafka ## Quick test with just Kafka ecosystem
	@cd ../../ && SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) go test -v -timeout=30s -run TestSchema ./test/kafka/

# Utility targets
install-deps: ## Install required dependencies
	@echo "$(YELLOW)Installing test dependencies...$(NC)"
	@which docker > /dev/null || (echo "$(RED)Docker not found$(NC)" && exit 1)
	@which docker-compose > /dev/null || (echo "$(RED)Docker Compose not found$(NC)" && exit 1)
	@which curl > /dev/null || (echo "$(RED)curl not found$(NC)" && exit 1)
	@which nc > /dev/null || (echo "$(RED)netcat not found$(NC)" && exit 1)
	@echo "$(GREEN)✅ All dependencies available$(NC)"

check-env: ## Check test environment setup
	@echo "$(BLUE)Environment Check:$(NC)"
	@echo "KAFKA_BOOTSTRAP_SERVERS: $(KAFKA_BOOTSTRAP_SERVERS)"
	@echo "SCHEMA_REGISTRY_URL: $(SCHEMA_REGISTRY_URL)"
	@echo "KAFKA_GATEWAY_URL: $(KAFKA_GATEWAY_URL)"
	@echo "TEST_TIMEOUT: $(TEST_TIMEOUT)"
	@make install-deps

# CI targets
ci-test: ## Run tests in CI environment
	@echo "$(YELLOW)Running CI tests...$(NC)"
	@make setup-schemas
	@make test-unit
	@make test-integration
	@make clean

ci-e2e: ## Run end-to-end tests in CI
	@echo "$(YELLOW)Running CI end-to-end tests...$(NC)"
	@make test-e2e
	@make clean

# Interactive targets
shell-kafka: ## Open shell in Kafka container
	@$(DOCKER_COMPOSE) exec kafka bash

shell-gateway: ## Open shell in Kafka Gateway container
	@$(DOCKER_COMPOSE) exec kafka-gateway sh

topics: ## List Kafka topics
	@$(DOCKER_COMPOSE) exec kafka kafka-topics --list --bootstrap-server localhost:29092

create-topic: ## Create a test topic (usage: make create-topic TOPIC=my-topic)
	@$(DOCKER_COMPOSE) exec kafka kafka-topics --create --topic $(TOPIC) --bootstrap-server localhost:29092 --partitions 3 --replication-factor 1

produce: ## Produce test messages (usage: make produce TOPIC=my-topic)
	@$(DOCKER_COMPOSE) exec kafka kafka-console-producer --bootstrap-server localhost:29092 --topic $(TOPIC)

consume: ## Consume messages (usage: make consume TOPIC=my-topic)
	@$(DOCKER_COMPOSE) exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic $(TOPIC) --from-beginning
