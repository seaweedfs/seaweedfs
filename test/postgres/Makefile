# SeaweedFS PostgreSQL Test Suite Makefile

.PHONY: help start stop clean produce test psql logs status all dev

# Default target
help: ## Show this help message
	@echo "SeaweedFS PostgreSQL Test Suite"
	@echo "==============================="
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Quick start: make all"

start: ## Start SeaweedFS and PostgreSQL servers
	@./run-tests.sh start

stop: ## Stop all services
	@./run-tests.sh stop

clean: ## Stop services and remove all data
	@./run-tests.sh clean

produce: ## Create MQ test data
	@./run-tests.sh produce

test: ## Run PostgreSQL client tests
	@./run-tests.sh test

psql: ## Connect with interactive psql client
	@./run-tests.sh psql

logs: ## Show service logs
	@./run-tests.sh logs

status: ## Show service status
	@./run-tests.sh status

all: ## Run complete test suite (start -> produce -> test)
	@./run-tests.sh all

# Development targets
dev-start: ## Start services for development
	@echo "Starting development environment..."
	@docker compose up -d seaweedfs postgres-server || (echo "=== Container startup failed, showing logs ===" && docker compose logs && exit 1)
	@echo "Services started. Run 'make dev-logs' to watch logs."

dev-logs: ## Follow logs for development
	@docker compose logs -f seaweedfs postgres-server

dev-rebuild: ## Rebuild and restart services
	@docker compose down
	@docker compose up -d --build seaweedfs postgres-server

# Individual service targets
start-seaweedfs: ## Start only SeaweedFS
	@docker compose up -d seaweedfs

restart-postgres: ## Start only PostgreSQL server
	@docker compose down -d postgres-server
	@docker compose up -d --build seaweedfs postgres-server

# Testing targets
test-basic: ## Run basic connectivity test
	@docker run --rm --network postgres_seaweedfs-net postgres:15-alpine \
		psql -h postgres-server -p 5432 -U seaweedfs -d default -c "SELECT version();"

test-producer: ## Test data producer only
	@docker compose up --build mq-producer

test-client: ## Test client only  
	@docker compose up --build postgres-client

# Cleanup targets
clean-images: ## Remove Docker images
	@docker compose down
	@docker image prune -f

clean-all: ## Complete cleanup including images
	@docker compose down -v --rmi all
	@docker system prune -f
