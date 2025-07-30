# SeaweedFS Admin Integration Test Makefile
# Tests the admin server and worker functionality using official weed commands

.PHONY: help build build-and-restart restart-workers start stop restart logs clean status test admin-ui worker-logs master-logs admin-logs vacuum-test vacuum-demo vacuum-status vacuum-data vacuum-data-high vacuum-data-low vacuum-continuous vacuum-clean vacuum-help
.DEFAULT_GOAL := help

COMPOSE_FILE := docker-compose-ec-test.yml
PROJECT_NAME := admin_integration

build: ## Build SeaweedFS with latest changes and create Docker image
	@echo "🔨 Building SeaweedFS with latest changes..."
	@echo "1️⃣ Generating admin templates..."
	@cd ../../ && make admin-generate
	@echo "2️⃣ Building Docker image with latest changes..."
	@cd ../ && make build
	@echo "3️⃣ Copying binary for local docker-compose..."
	@cp ../weed ./weed-local
	@echo "✅ Build complete! Updated image: chrislusf/seaweedfs:local"
	@echo "💡 Run 'make restart' to apply changes to running services"

build-and-restart: build ## Build with latest changes and restart services
	@echo "🔄 Recreating services with new image..."
	@echo "1️⃣ Recreating admin server with new image..."
	@docker-compose -f $(COMPOSE_FILE) up -d admin
	@sleep 5
	@echo "2️⃣ Recreating workers to reconnect..."
	@docker-compose -f $(COMPOSE_FILE) up -d worker1 worker2 worker3
	@echo "✅ All services recreated with latest changes!"
	@echo "🌐 Admin UI: http://localhost:23646/"
	@echo "💡 Workers will reconnect to the new admin server"

restart-workers: ## Restart all workers to reconnect to admin server
	@echo "🔄 Restarting workers to reconnect to admin server..."
	@docker-compose -f $(COMPOSE_FILE) restart worker1 worker2 worker3
	@echo "✅ Workers restarted and will reconnect to admin server"

help: ## Show this help message
	@echo "SeaweedFS Admin Integration Test"
	@echo "================================"
	@echo "Tests admin server task distribution to workers using official weed commands"
	@echo ""
	@echo "🏗️  Cluster Management:"
	@grep -E '^(start|stop|restart|clean|status|build):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "🧪 Testing:"
	@grep -E '^(test|demo|validate|quick-test):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "🗑️  Vacuum Testing:"
	@grep -E '^vacuum-.*:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "📜 Monitoring:"
	@grep -E '^(logs|admin-logs|worker-logs|master-logs|admin-ui):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "🚀 Quick Start:"
	@echo "  make start           # Start cluster"
	@echo "  make vacuum-test     # Test vacuum tasks"
	@echo "  make vacuum-help     # Vacuum testing guide"
	@echo ""
	@echo "💡 For detailed vacuum testing: make vacuum-help"

start: ## Start the complete SeaweedFS cluster with admin and workers
	@echo "🚀 Starting SeaweedFS cluster with admin and workers..."
	@docker-compose -f $(COMPOSE_FILE) up -d
	@echo "✅ Cluster started!"
	@echo ""
	@echo "📊 Access points:"
	@echo "  • Admin UI:      http://localhost:23646/"
	@echo "  • Master UI:     http://localhost:9333/"
	@echo "  • Filer:         http://localhost:8888/"
	@echo ""
	@echo "📈 Services starting up..."
	@echo "  • Master server:  ✓"
	@echo "  • Volume servers: Starting (6 servers)..."
	@echo "  • Filer:          Starting..."
	@echo "  • Admin server:   Starting..."
	@echo "  • Workers:        Starting (3 workers)..."
	@echo ""
	@echo "⏳ Use 'make status' to check startup progress"
	@echo "💡 Use 'make logs' to watch the startup process"

start-staged: ## Start services in proper order with delays
	@echo "🚀 Starting SeaweedFS cluster in stages..."
	@echo ""
	@echo "Stage 1: Starting Master server..."
	@docker-compose -f $(COMPOSE_FILE) up -d master
	@sleep 10
	@echo ""
	@echo "Stage 2: Starting Volume servers..."
	@docker-compose -f $(COMPOSE_FILE) up -d volume1 volume2 volume3 volume4 volume5 volume6
	@sleep 15
	@echo ""
	@echo "Stage 3: Starting Filer..."
	@docker-compose -f $(COMPOSE_FILE) up -d filer
	@sleep 10
	@echo ""
	@echo "Stage 4: Starting Admin server..."
	@docker-compose -f $(COMPOSE_FILE) up -d admin
	@sleep 15
	@echo ""
	@echo "Stage 5: Starting Workers..."
	@docker-compose -f $(COMPOSE_FILE) up -d worker1 worker2 worker3
	@sleep 10
	@echo ""
	@echo "Stage 6: Starting Load generator and Monitor..."
	@docker-compose -f $(COMPOSE_FILE) up -d load_generator monitor
	@echo ""
	@echo "✅ All services started!"
	@echo ""
	@echo "📊 Access points:"
	@echo "  • Admin UI:      http://localhost:23646/"
	@echo "  • Master UI:     http://localhost:9333/"
	@echo "  • Filer:         http://localhost:8888/"
	@echo ""
	@echo "⏳ Services are initializing... Use 'make status' to check progress"

stop: ## Stop all services
	@echo "🛑 Stopping SeaweedFS cluster..."
	@docker-compose -f $(COMPOSE_FILE) down
	@echo "✅ Cluster stopped"

restart: stop start ## Restart the entire cluster

clean: ## Stop and remove all containers, networks, and volumes
	@echo "🧹 Cleaning up SeaweedFS test environment..."
	@docker-compose -f $(COMPOSE_FILE) down -v --remove-orphans
	@docker system prune -f
	@rm -rf data/
	@echo "✅ Environment cleaned"

status: ## Check the status of all services
	@echo "📊 SeaweedFS Cluster Status"
	@echo "=========================="
	@docker-compose -f $(COMPOSE_FILE) ps
	@echo ""
	@echo "📋 Service Health:"
	@echo "Master:"
	@curl -s http://localhost:9333/cluster/status | jq '.IsLeader' 2>/dev/null || echo "  ❌ Master not ready"
	@echo "Admin:"
	@curl -s http://localhost:23646/ | grep -q "Admin" && echo "  ✅ Admin ready" || echo "  ❌ Admin not ready"

logs: ## Show logs from all services
	@echo "📜 Following logs from all services..."
	@echo "💡 Press Ctrl+C to stop following logs"
	@docker-compose -f $(COMPOSE_FILE) logs -f

admin-logs: ## Show logs from admin server only
	@echo "📜 Admin server logs:"
	@docker-compose -f $(COMPOSE_FILE) logs -f admin

worker-logs: ## Show logs from all workers
	@echo "📜 Worker logs:"
	@docker-compose -f $(COMPOSE_FILE) logs -f worker1 worker2 worker3

master-logs: ## Show logs from master server
	@echo "📜 Master server logs:"
	@docker-compose -f $(COMPOSE_FILE) logs -f master

admin-ui: ## Open admin UI in browser (macOS)
	@echo "🌐 Opening admin UI in browser..."
	@open http://localhost:23646/ || echo "💡 Manually open: http://localhost:23646/"

test: ## Run integration test to verify task assignment and completion
	@echo "🧪 Running Admin-Worker Integration Test"
	@echo "========================================"
	@echo ""
	@echo "1️⃣ Checking cluster health..."
	@sleep 5
	@curl -s http://localhost:9333/cluster/status | jq '.IsLeader' > /dev/null && echo "✅ Master healthy" || echo "❌ Master not ready"
	@curl -s http://localhost:23646/ | grep -q "Admin" && echo "✅ Admin healthy" || echo "❌ Admin not ready"
	@echo ""
	@echo "2️⃣ Checking worker registration..."
	@sleep 10
	@echo "💡 Check admin UI for connected workers: http://localhost:23646/"
	@echo ""
	@echo "3️⃣ Generating load to trigger EC tasks..."
	@echo "📝 Creating test files to fill volumes..."
	@echo "Creating large files with random data to trigger EC (targeting ~60MB total to exceed 50MB limit)..."
	@for i in {1..12}; do \
		echo "Creating 5MB random file $$i..."; \
		docker run --rm --network admin_integration_seaweed_net -v /tmp:/tmp --entrypoint sh chrislusf/seaweedfs:local -c "dd if=/dev/urandom of=/tmp/largefile$$i.dat bs=1M count=5 2>/dev/null && weed upload -master=master:9333 /tmp/largefile$$i.dat && rm /tmp/largefile$$i.dat"; \
		sleep 3; \
	done
	@echo ""
	@echo "4️⃣ Waiting for volumes to process large files and reach 50MB limit..."
	@echo "This may take a few minutes as we're uploading 60MB of data..."
	@sleep 60
	@echo ""
	@echo "5️⃣ Checking for EC task creation and assignment..."
	@echo "💡 Monitor the admin UI to see:"
	@echo "   • Tasks being created for volumes needing EC"
	@echo "   • Workers picking up tasks"
	@echo "   • Task progress (pending → running → completed)"
	@echo "   • EC shards being distributed"
	@echo ""
	@echo "✅ Integration test setup complete!"
	@echo "📊 Monitor progress at: http://localhost:23646/"

quick-test: ## Quick verification that core services are running
	@echo "⚡ Quick Health Check"
	@echo "===================="
	@echo "Master:  $$(curl -s http://localhost:9333/cluster/status | jq -r '.IsLeader // "not ready"')"
	@echo "Admin:   $$(curl -s http://localhost:23646/ | grep -q "Admin" && echo "ready" || echo "not ready")"
	@echo "Workers: $$(docker-compose -f $(COMPOSE_FILE) ps worker1 worker2 worker3 | grep -c Up) running"

validate: ## Validate integration test configuration
	@echo "🔍 Validating Integration Test Configuration"
	@echo "==========================================="
	@chmod +x test-integration.sh
	@./test-integration.sh

demo: start ## Start cluster and run demonstration
	@echo "🎭 SeaweedFS Admin-Worker Demo"
	@echo "============================="
	@echo ""
	@echo "⏳ Waiting for services to start..."
	@sleep 45
	@echo ""
	@echo "🎯 Demo Overview:"
	@echo "  • 1 Master server (coordinates cluster)"
	@echo "  • 6 Volume servers (50MB volume limit)"
	@echo "  • 1 Admin server (task management)"
	@echo "  • 3 Workers (execute EC tasks)"
	@echo "  • Load generator (creates files continuously)"
	@echo ""
	@echo "📊 Watch the process:"
	@echo "  1. Visit: http://localhost:23646/"
	@echo "  2. Observe workers connecting"
	@echo "  3. Watch tasks being created and assigned"
	@echo "  4. See tasks progress from pending → completed"
	@echo ""
	@echo "🔄 The demo will:"
	@echo "  • Fill volumes to 50MB limit"
	@echo "  • Admin detects volumes needing EC"
	@echo "  • Workers receive and execute EC tasks"
	@echo "  • Tasks complete with shard distribution"
	@echo ""
	@echo "💡 Use 'make worker-logs' to see worker activity"
	@echo "💡 Use 'make admin-logs' to see admin task management"

# Vacuum Testing Targets
vacuum-test: ## Create test data with garbage and verify vacuum detection
	@echo "🧪 SeaweedFS Vacuum Task Testing"
	@echo "================================"
	@echo ""
	@echo "1️⃣ Checking cluster health..."
	@curl -s http://localhost:9333/cluster/status | jq '.IsLeader' > /dev/null && echo "✅ Master ready" || (echo "❌ Master not ready. Run 'make start' first." && exit 1)
	@curl -s http://localhost:23646/ | grep -q "Admin" && echo "✅ Admin ready" || (echo "❌ Admin not ready. Run 'make start' first." && exit 1)
	@echo ""
	@echo "2️⃣ Creating test data with garbage..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -files=25 -delete=0.5 -size=200
	@echo ""
	@echo "3️⃣ Configuration Instructions:"
	@echo "   Visit: http://localhost:23646/maintenance/config/vacuum"
	@echo "   Set for testing:"
	@echo "     • Enable Vacuum Tasks: ✅ Checked"
	@echo "     • Garbage Threshold: 0.20 (20%)"
	@echo "     • Scan Interval: [30] [Seconds]"
	@echo "     • Min Volume Age: [0] [Minutes]"
	@echo "     • Max Concurrent: 2"
	@echo ""
	@echo "4️⃣ Monitor vacuum tasks at: http://localhost:23646/maintenance"
	@echo ""
	@echo "💡 Use 'make vacuum-status' to check volume garbage ratios"

vacuum-demo: ## Run automated vacuum testing demonstration
	@echo "🎭 Vacuum Task Demo"
	@echo "=================="
	@echo ""
	@echo "⚠️  This demo requires user interaction for configuration"
	@echo "💡 Make sure cluster is running with 'make start'"
	@echo ""
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester sh -c "chmod +x demo_vacuum_testing.sh && ./demo_vacuum_testing.sh"

vacuum-status: ## Check current volume status and garbage ratios
	@echo "📊 Current Volume Status"
	@echo "======================="
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester sh -c "chmod +x check_volumes.sh && ./check_volumes.sh"

vacuum-data: ## Create test data with configurable parameters
	@echo "📁 Creating vacuum test data..."
	@echo "Usage: make vacuum-data [FILES=20] [DELETE=0.4] [SIZE=100]"
	@echo ""
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go \
		-files=$${FILES:-20} \
		-delete=$${DELETE:-0.4} \
		-size=$${SIZE:-100}

vacuum-data-high: ## Create high garbage ratio test data (should trigger vacuum)
	@echo "📁 Creating high garbage test data (70% garbage)..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -files=30 -delete=0.7 -size=150

vacuum-data-low: ## Create low garbage ratio test data (should NOT trigger vacuum)
	@echo "📁 Creating low garbage test data (15% garbage)..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -files=30 -delete=0.15 -size=150

vacuum-continuous: ## Generate garbage continuously for testing
	@echo "🔄 Generating continuous garbage for vacuum testing..."
	@echo "Creating 5 rounds of test data with 30-second intervals..."
	@for i in {1..5}; do \
		echo "Round $$i: Creating garbage..."; \
		docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -files=10 -delete=0.6 -size=100; \
		echo "Waiting 30 seconds..."; \
		sleep 30; \
	done
	@echo "✅ Continuous test complete. Check vacuum task activity!"

vacuum-clean: ## Clean up vacuum test data (removes all volumes!)
	@echo "🧹 Cleaning up vacuum test data..."
	@echo "⚠️  WARNING: This will delete ALL volumes!"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo "Stopping cluster..."
	@docker-compose -f $(COMPOSE_FILE) down
	@echo "Removing volume data..."
	@rm -rf data/volume*/
	@echo "Restarting cluster..."
	@docker-compose -f $(COMPOSE_FILE) up -d
	@echo "✅ Clean up complete. Fresh volumes ready for testing."

vacuum-help: ## Show vacuum testing help and examples
	@echo "🧪 Vacuum Testing Commands (Docker-based)"
	@echo "=========================================="
	@echo ""
	@echo "Quick Start:"
	@echo "  make start         # Start SeaweedFS cluster with vacuum-tester"
	@echo "  make vacuum-test   # Create test data and instructions"
	@echo "  make vacuum-status # Check volume status"
	@echo ""
	@echo "Data Generation:"
	@echo "  make vacuum-data-high       # High garbage (should trigger)"
	@echo "  make vacuum-data-low        # Low garbage (should NOT trigger)" 
	@echo "  make vacuum-continuous      # Continuous garbage generation"
	@echo ""
	@echo "Monitoring:"
	@echo "  make vacuum-status          # Quick volume status check"
	@echo "  make vacuum-demo           # Full guided demonstration"
	@echo ""
	@echo "Configuration:"
	@echo "  Visit: http://localhost:23646/maintenance/config/vacuum"
	@echo "  Monitor: http://localhost:23646/maintenance"
	@echo ""
	@echo "Custom Parameters:"
	@echo "  make vacuum-data FILES=50 DELETE=0.8 SIZE=200"
	@echo ""
	@echo "💡 All commands now run inside Docker containers"
	@echo "Documentation:"
	@echo "  See: VACUUM_TEST_README.md for complete guide" 