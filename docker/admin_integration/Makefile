# SeaweedFS EC Worker Testing Environment Makefile
# Usage: make <target>

.PHONY: help start stop clean logs status monitor health up down restart scale docs test

# Default target
.DEFAULT_GOAL := help

# Docker compose file
COMPOSE_FILE := docker-compose-ec-test.yml

# Color codes for output
GREEN := \033[32m
YELLOW := \033[33m
BLUE := \033[34m
RED := \033[31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)🧪 SeaweedFS EC Worker Testing Environment$(NC)"
	@echo "$(BLUE)===========================================$(NC)"
	@echo ""
	@echo "$(YELLOW)Available targets:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-15s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(YELLOW)Quick start:$(NC) make start"
	@echo "$(YELLOW)Monitor:$(NC)     make monitor"
	@echo "$(YELLOW)Cleanup:$(NC)     make clean"

start: ## Start the complete EC testing environment
	@echo "$(GREEN)🚀 Starting SeaweedFS EC testing environment...$(NC)"
	@echo "$(BLUE)This will start:$(NC)"
	@echo "  • 1 Master server (port 9333)"
	@echo "  • 6 Volume servers (ports 8080-8085) with 50MB volume limit"
	@echo "  • 1 Filer (port 8888)"
	@echo "  • 1 Admin server (port 9900)"
	@echo "  • 3 EC Workers"
	@echo "  • 1 Load generator (continuous read/write)"
	@echo "  • 1 Monitor (port 9999)"
	@echo ""
	@mkdir -p monitor-data admin-config
	@chmod +x *.sh 2>/dev/null || true
	@docker-compose -f $(COMPOSE_FILE) down -v 2>/dev/null || true
	@docker-compose -f $(COMPOSE_FILE) up --build -d
	@echo ""
	@echo "$(GREEN)✅ Environment started successfully!$(NC)"
	@echo ""
	@$(MAKE) urls
	@echo ""
	@echo "$(YELLOW)⏳ Waiting for services to be ready...$(NC)"
	@sleep 10
	@$(MAKE) health

stop: ## Stop all services
	@echo "$(YELLOW)🛑 Stopping all services...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) stop
	@echo "$(GREEN)✅ All services stopped$(NC)"

down: ## Stop and remove all containers
	@echo "$(YELLOW)🛑 Stopping and removing containers...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) down
	@echo "$(GREEN)✅ Containers stopped and removed$(NC)"

clean: ## Stop and remove all containers, networks, volumes, and images
	@echo "$(RED)🧹 Cleaning up entire environment...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) down -v --rmi all 2>/dev/null || true
	@docker system prune -f
	@echo "$(GREEN)✅ Environment cleaned up$(NC)"

restart: ## Restart all services
	@echo "$(YELLOW)🔄 Restarting all services...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) restart
	@echo "$(GREEN)✅ All services restarted$(NC)"

up: start ## Alias for start

status: ## Show status of all services
	@echo "$(BLUE)📊 Service Status:$(NC)"
	@docker-compose -f $(COMPOSE_FILE) ps

logs: ## Show logs from all services
	@echo "$(BLUE)📋 Showing logs from all services (Ctrl+C to exit):$(NC)"
	@docker-compose -f $(COMPOSE_FILE) logs -f

logs-admin: ## Show admin server logs
	@echo "$(BLUE)📋 Admin Server Logs:$(NC)"
	@docker-compose -f $(COMPOSE_FILE) logs -f admin

logs-workers: ## Show all worker logs
	@echo "$(BLUE)📋 Worker Logs:$(NC)"
	@docker-compose -f $(COMPOSE_FILE) logs -f worker1 worker2 worker3

logs-worker1: ## Show worker1 logs
	@docker-compose -f $(COMPOSE_FILE) logs -f worker1

logs-worker2: ## Show worker2 logs
	@docker-compose -f $(COMPOSE_FILE) logs -f worker2

logs-worker3: ## Show worker3 logs
	@docker-compose -f $(COMPOSE_FILE) logs -f worker3

logs-load: ## Show load generator logs
	@echo "$(BLUE)📋 Load Generator Logs:$(NC)"
	@docker-compose -f $(COMPOSE_FILE) logs -f load_generator

logs-monitor: ## Show monitor logs
	@echo "$(BLUE)📋 Monitor Logs:$(NC)"
	@docker-compose -f $(COMPOSE_FILE) logs -f monitor

logs-master: ## Show master logs
	@docker-compose -f $(COMPOSE_FILE) logs -f master

logs-volumes: ## Show all volume server logs
	@echo "$(BLUE)📋 Volume Server Logs:$(NC)"
	@docker-compose -f $(COMPOSE_FILE) logs -f volume1 volume2 volume3 volume4 volume5 volume6

urls: ## Show monitoring URLs
	@echo "$(BLUE)📊 Monitoring URLs:$(NC)"
	@echo "  • Master UI:     http://localhost:9333"
	@echo "  • Filer:         http://localhost:8888"
	@echo "  • Admin Server:  http://localhost:9900/status"
	@echo "  • Monitor:       http://localhost:9999/status"
	@echo ""
	@echo "$(BLUE)📈 Volume Servers:$(NC)"
	@echo "  • Volume1:       http://localhost:8080/status"
	@echo "  • Volume2:       http://localhost:8081/status"
	@echo "  • Volume3:       http://localhost:8082/status"
	@echo "  • Volume4:       http://localhost:8083/status"
	@echo "  • Volume5:       http://localhost:8084/status"
	@echo "  • Volume6:       http://localhost:8085/status"

health: ## Check health of all services
	@echo "$(BLUE)🔍 Checking service health...$(NC)"
	@echo -n "  Master: "; \
	if curl -s http://localhost:9333/cluster/status > /dev/null 2>&1; then \
		echo "$(GREEN)✅ Healthy$(NC)"; \
	else \
		echo "$(RED)❌ Not responding$(NC)"; \
	fi
	@echo -n "  Filer: "; \
	if curl -s http://localhost:8888/ > /dev/null 2>&1; then \
		echo "$(GREEN)✅ Healthy$(NC)"; \
	else \
		echo "$(RED)❌ Not responding$(NC)"; \
	fi
	@echo -n "  Admin: "; \
	if curl -s http://localhost:9900/health > /dev/null 2>&1; then \
		echo "$(GREEN)✅ Healthy$(NC)"; \
	else \
		echo "$(RED)❌ Not responding$(NC)"; \
	fi
	@echo -n "  Monitor: "; \
	if curl -s http://localhost:9999/health > /dev/null 2>&1; then \
		echo "$(GREEN)✅ Healthy$(NC)"; \
	else \
		echo "$(RED)❌ Not responding$(NC)"; \
	fi

monitor: ## Open monitor dashboard in browser
	@echo "$(BLUE)📊 Opening monitor dashboard...$(NC)"
	@echo "Monitor URL: http://localhost:9999/status"
	@command -v open >/dev/null 2>&1 && open http://localhost:9999/status || \
	command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:9999/status || \
	echo "Please open http://localhost:9999/status in your browser"

monitor-status: ## Show current monitoring status via API
	@echo "$(BLUE)📊 Current Monitor Status:$(NC)"
	@curl -s http://localhost:9999/status | jq . 2>/dev/null || \
	curl -s http://localhost:9999/status 2>/dev/null || \
	echo "Monitor not available"

volume-status: ## Show volume status from master
	@echo "$(BLUE)💾 Volume Status:$(NC)"
	@curl -s http://localhost:9333/vol/status | jq . 2>/dev/null || \
	curl -s http://localhost:9333/vol/status 2>/dev/null || \
	echo "Master not available"

admin-status: ## Show admin server status
	@echo "$(BLUE)🏭 Admin Server Status:$(NC)"
	@curl -s http://localhost:9900/status | jq . 2>/dev/null || \
	curl -s http://localhost:9900/status 2>/dev/null || \
	echo "Admin server not available"

cluster-status: ## Show complete cluster status
	@echo "$(BLUE)🌐 Cluster Status:$(NC)"
	@curl -s http://localhost:9333/cluster/status | jq . 2>/dev/null || \
	curl -s http://localhost:9333/cluster/status 2>/dev/null || \
	echo "Master not available"

scale-workers: ## Scale workers (usage: make scale-workers WORKERS=5)
	@echo "$(YELLOW)⚖️  Scaling workers to $(or $(WORKERS),3)...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) up -d --scale worker2=$(or $(WORKERS),3)

scale-load: ## Restart load generator with higher rate (usage: make scale-load RATE=20)
	@echo "$(YELLOW)📈 Scaling load generation to $(or $(RATE),20) files/sec...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) stop load_generator
	@docker-compose -f $(COMPOSE_FILE) run -d --name temp_load_generator \
		-e WRITE_RATE=$(or $(RATE),20) -e DELETE_RATE=$(or $(shell expr $(or $(RATE),20) / 4),5) \
		load_generator
	@echo "$(GREEN)✅ Load generator restarted with higher rate$(NC)"

test-ec: ## Run a focused EC test scenario
	@echo "$(YELLOW)🧪 Running focused EC test...$(NC)"
	@$(MAKE) scale-load RATE=25
	@echo "$(BLUE)Monitoring EC detection...$(NC)"
	@echo "Watch for volumes >40MB that trigger EC conversion"
	@echo "Monitor at: http://localhost:9999/status"

shell-admin: ## Open shell in admin container
	@docker-compose -f $(COMPOSE_FILE) exec admin /bin/sh

shell-worker1: ## Open shell in worker1 container
	@docker-compose -f $(COMPOSE_FILE) exec worker1 /bin/sh

shell-master: ## Open shell in master container
	@docker-compose -f $(COMPOSE_FILE) exec master /bin/sh

docs: ## Show documentation
	@echo "$(BLUE)📖 EC Testing Documentation:$(NC)"
	@echo ""
	@cat EC-TESTING-README.md

build: ## Build all Docker images without starting
	@echo "$(YELLOW)🔨 Building all Docker images...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) build
	@echo "$(GREEN)✅ All images built$(NC)"

pull: ## Pull latest SeaweedFS image
	@echo "$(YELLOW)📥 Pulling latest SeaweedFS image...$(NC)"
	@docker pull chrislusf/seaweedfs:latest
	@echo "$(GREEN)✅ Latest image pulled$(NC)"

debug: ## Show debug information
	@echo "$(BLUE)🔍 Debug Information:$(NC)"
	@echo ""
	@echo "$(YELLOW)Docker Compose Version:$(NC)"
	@docker-compose --version
	@echo ""
	@echo "$(YELLOW)Docker Version:$(NC)"
	@docker --version
	@echo ""
	@echo "$(YELLOW)Current Directory:$(NC)"
	@pwd
	@echo ""
	@echo "$(YELLOW)Available Files:$(NC)"
	@ls -la *.yml *.sh *.md 2>/dev/null || echo "No config files found"
	@echo ""
	@echo "$(YELLOW)Running Containers:$(NC)"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Targets for development and testing
dev-start: ## Start with development settings (faster iteration)
	@echo "$(YELLOW)🛠️  Starting development environment...$(NC)"
	@mkdir -p monitor-data admin-config
	@WRITE_RATE=50 DELETE_RATE=10 docker-compose -f $(COMPOSE_FILE) up --build -d
	@echo "$(GREEN)✅ Development environment started with high load$(NC)"

dev-stop: stop ## Stop development environment

# Clean specific components
clean-volumes: ## Remove only data volumes
	@echo "$(YELLOW)🗄️  Removing data volumes...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) down -v
	@echo "$(GREEN)✅ Data volumes removed$(NC)"

clean-images: ## Remove built images
	@echo "$(YELLOW)🖼️  Removing built images...$(NC)"
	@docker-compose -f $(COMPOSE_FILE) down --rmi local
	@echo "$(GREEN)✅ Built images removed$(NC)"

# Backup and restore
backup-logs: ## Backup all service logs
	@echo "$(YELLOW)💾 Backing up service logs...$(NC)"
	@mkdir -p logs-backup
	@docker-compose -f $(COMPOSE_FILE) logs admin > logs-backup/admin.log 2>&1
	@docker-compose -f $(COMPOSE_FILE) logs worker1 > logs-backup/worker1.log 2>&1
	@docker-compose -f $(COMPOSE_FILE) logs worker2 > logs-backup/worker2.log 2>&1
	@docker-compose -f $(COMPOSE_FILE) logs worker3 > logs-backup/worker3.log 2>&1
	@docker-compose -f $(COMPOSE_FILE) logs monitor > logs-backup/monitor.log 2>&1
	@docker-compose -f $(COMPOSE_FILE) logs load_generator > logs-backup/load_generator.log 2>&1
	@echo "$(GREEN)✅ Logs backed up to logs-backup/$(NC)"

# Quick troubleshooting
troubleshoot: ## Run troubleshooting checks
	@echo "$(BLUE)🔧 Running troubleshooting checks...$(NC)"
	@echo ""
	@echo "$(YELLOW)1. Checking required ports:$(NC)"
	@for port in 9333 8888 9900 9999 8080 8081 8082 8083 8084 8085; do \
		echo -n "  Port $$port: "; \
		if lsof -i :$$port >/dev/null 2>&1; then \
			echo "$(RED)❌ In use$(NC)"; \
		else \
			echo "$(GREEN)✅ Available$(NC)"; \
		fi; \
	done
	@echo ""
	@echo "$(YELLOW)2. Docker resources:$(NC)"
	@docker system df
	@echo ""
	@echo "$(YELLOW)3. Service health:$(NC)"
	@$(MAKE) health 