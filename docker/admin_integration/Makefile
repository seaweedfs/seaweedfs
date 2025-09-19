# SeaweedFS Admin Integration Test Makefile
# Tests the admin server and worker functionality using official weed commands

.PHONY: help build build-and-restart restart-workers restart-admin start stop restart logs clean status test admin-ui worker-logs master-logs admin-logs vacuum-test vacuum-demo vacuum-status vacuum-data vacuum-data-high vacuum-data-low vacuum-continuous vacuum-clean vacuum-help file-generation-test file-deletion-test all-file-tests
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

restart-admin: ## Restart admin server (useful after deadlock fixes)
	@echo "🔄 Restarting admin server..."
	@docker-compose -f $(COMPOSE_FILE) restart admin
	@echo "✅ Admin server restarted"
	@echo "🌐 Admin UI: http://localhost:23646/"

help: ## Show this help message
	@echo "SeaweedFS Admin Integration Test"
	@echo "================================"
	@echo "Tests admin server task distribution to workers using official weed commands"
	@echo ""
	@echo "🏗️  Cluster Management:"
	@grep -E '^(start|stop|restart|restart-admin|restart-workers|clean|status|build):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "🧪 Testing:"
	@grep -E '^(test|demo|validate|quick-test|file-.*-test|all-file-tests):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "🗑️  Vacuum Testing:"
	@grep -E '^vacuum-.*:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "🔧 EC Vacuum Testing:"
	@grep -E '^ec-vacuum-.*:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "📜 Monitoring:"
	@grep -E '^(logs|admin-logs|worker-logs|master-logs|admin-ui):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'
	@echo ""
	@echo "🚀 Quick Start:"
	@echo "  make start           # Start cluster"
	@echo "  make vacuum-test        # Test regular vacuum tasks"
	@echo "  make ec-vacuum-test     # Test EC vacuum tasks"
	@echo "  make file-generation-test # Test file generation (600 files)"
	@echo "  make file-deletion-test   # Test file deletion (300 files)"
	@echo "  make all-file-tests       # Run both file tests"
	@echo "  make vacuum-help     # Regular vacuum guide"
	@echo "  make ec-vacuum-help  # EC vacuum guide"
	@echo ""
	@echo "💡 For detailed testing guides:"
	@echo "  make vacuum-help     # Regular vacuum testing"
	@echo "  make ec-vacuum-help  # EC vacuum testing"

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

# EC Vacuum Testing Targets
file-generation-test: ## Run the file generation test (600 files of 100KB to volume 1)
	@echo "🧪 Running File Generation Test"
	@echo "==============================="
	@echo "1️⃣ Ensuring cluster is running..."
	@docker-compose -f $(COMPOSE_FILE) up -d
	@echo "2️⃣ Waiting for cluster to be ready..."
	@sleep 10
	@echo "3️⃣ Running file generation test..."
	@go test -v . -run TestFileGeneration
	@echo "✅ File generation test completed!"
	@echo "💡 This test generates 600 files of 100KB each to volume 1 with hardcoded cookie"

file-deletion-test: ## Run the file deletion test (delete 300 files from volume 1)
	@echo "🧪 Running File Deletion Test"
	@echo "============================="
	@echo "1️⃣ Ensuring cluster is running..."
	@docker-compose -f $(COMPOSE_FILE) up -d
	@echo "2️⃣ Waiting for cluster to be ready..."
	@sleep 10
	@echo "3️⃣ Running file deletion test..."
	@go test -v . -run TestFileDeletion
	@echo "✅ File deletion test completed!"
	@echo "💡 This test generates 600 files then deletes exactly 300 of them"

all-file-tests: ## Run both file generation and deletion tests
	@echo "🧪 Running All File Tests"
	@echo "========================="
	@echo "1️⃣ Ensuring cluster is running..."
	@docker-compose -f $(COMPOSE_FILE) up -d
	@echo "2️⃣ Waiting for cluster to be ready..."
	@sleep 10
	@echo "3️⃣ Running file generation test..."
	@go test -v . -run TestFileGeneration
	@echo "4️⃣ Running file deletion test..."
	@go test -v . -run TestFileDeletion
	@echo "✅ All file tests completed!"

ec-vacuum-go-test: ## Run the Go-based EC vacuum integration test with detailed file ID tracking (legacy)
	@echo "🧪 Running EC Vacuum Go Integration Test"
	@echo "========================================"
	@echo "1️⃣ Ensuring cluster is running..."
	@docker-compose -f $(COMPOSE_FILE) up -d
	@echo "2️⃣ Waiting for cluster to be ready..."
	@sleep 10
	@echo "3️⃣ Running Go test with file ID tracking..."
	@go test -v . -run TestECVolumeVacuum
	@echo "✅ EC Vacuum Go test completed!"
	@echo "💡 This test shows which file IDs are written and deleted"

ec-vacuum-test: ## Generate EC volumes and test EC vacuum functionality
	@echo "🧪 SeaweedFS EC Vacuum Task Testing"
	@echo "===================================="
	@echo ""
	@echo "1️⃣ Checking cluster health..."
	@curl -s http://localhost:9333/cluster/status | jq '.IsLeader' > /dev/null && echo "✅ Master ready" || (echo "❌ Master not ready. Run 'make start' first." && exit 1)
	@curl -s http://localhost:23646/ | grep -q "Admin" && echo "✅ Admin ready" || (echo "❌ Admin not ready. Run 'make start' first." && exit 1)
	@echo ""
	@echo "2️⃣ Generating data to trigger EC encoding..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=generate -files=30 -size=3000
	@echo ""
	@echo "3️⃣ Waiting for EC encoding to complete..."
	@echo "⏳ This may take 2-3 minutes..."
	@sleep 120
	@echo ""
	@echo "4️⃣ Generating deletions on EC volumes..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=delete -delete=0.4
	@echo ""
	@echo "5️⃣ Configuration Instructions:"
	@echo "   Visit: http://localhost:23646/maintenance/config/ec_vacuum"
	@echo "   Set for testing:"
	@echo "     • Enable EC Vacuum Tasks: ✅ Checked"
	@echo "     • Garbage Threshold: 0.30 (30%)"
	@echo "     • Scan Interval: [60] [Seconds]"
	@echo "     • Min Volume Age: [2] [Minutes]"
	@echo "     • Max Concurrent: 2"
	@echo ""
	@echo "6️⃣ Monitor EC vacuum tasks at: http://localhost:23646/maintenance"
	@echo ""
	@echo "💡 Use 'make ec-vacuum-status' to check EC volume garbage ratios"

ec-vacuum-generate: ## Generate large files to trigger EC encoding
	@echo "📁 Generating data to trigger EC encoding..."
	@echo "Creating large files targeting >50MB per volume..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=generate -files=25 -size=3000
	@echo ""
	@echo "⏳ Wait 2-3 minutes for EC encoding, then run 'make ec-vacuum-delete'"

ec-vacuum-delete: ## Create deletions on EC volumes to generate garbage
	@echo "🗑️ Creating deletions on EC volumes..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=delete -delete=$${DELETE:-0.4}
	@echo ""
	@echo "💡 Use 'make ec-vacuum-status' to check garbage ratios"

ec-vacuum-status: ## Check EC volume status and garbage ratios
	@echo "📊 EC Volume Status and Garbage Ratios"
	@echo "====================================="
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=status

ec-vacuum-continuous: ## Generate continuous EC garbage for testing
	@echo "🔄 Generating continuous EC garbage for vacuum testing..."
	@echo "Running 3 rounds with 60-second intervals..."
	@for i in $$(seq 1 3); do \
		echo "Round $$i: Generating large files..."; \
		docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=generate -files=15 -size=4000; \
		echo "Waiting 90 seconds for EC encoding..."; \
		sleep 90; \
		echo "Creating deletions..."; \
		docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=delete -delete=0.5; \
		echo "Waiting 60 seconds before next round..."; \
		sleep 60; \
	done
	@echo "✅ Continuous EC vacuum test complete. Monitor admin UI for ec_vacuum tasks!"

ec-vacuum-high: ## Create high garbage on EC volumes (should trigger EC vacuum)
	@echo "📁 Creating high garbage EC volumes (60% garbage)..."
	@echo "1. Generating files for EC..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=generate -files=20 -size=4000
	@echo "2. Waiting for EC encoding..."
	@sleep 120
	@echo "3. Creating high garbage ratio..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=delete -delete=0.6

ec-vacuum-low: ## Create low garbage on EC volumes (should NOT trigger EC vacuum)
	@echo "📁 Creating low garbage EC volumes (20% garbage)..."
	@echo "1. Generating files for EC..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=generate -files=20 -size=4000
	@echo "2. Waiting for EC encoding..."
	@sleep 120
	@echo "3. Creating low garbage ratio..."
	@docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=delete -delete=0.2

ec-vacuum-monitor: ## Monitor EC vacuum task activity in real-time
	@echo "📊 Monitoring EC Vacuum Task Activity"
	@echo "===================================="
	@echo "Press Ctrl+C to stop monitoring"
	@echo ""
	@while true; do \
		echo "=== $(date) ==="; \
		docker-compose -f $(COMPOSE_FILE) exec vacuum-tester go run create_vacuum_test_data.go -phase=status; \
		echo ""; \
		echo "🔍 Recent admin logs (EC vacuum activity):"; \
		docker-compose -f $(COMPOSE_FILE) logs --tail=5 admin | grep -i "ec_vacuum\|vacuum.*ec" || echo "No recent EC vacuum activity"; \
		echo ""; \
		sleep 30; \
	done

ec-vacuum-help: ## Show EC vacuum testing help and examples
	@echo "🧪 EC Vacuum Testing Commands"
	@echo "============================="
	@echo ""
	@echo "Quick Start:"
	@echo "  make start                  # Start SeaweedFS cluster"
	@echo "  make ec-vacuum-test         # Full EC vacuum test cycle"
	@echo "  make ec-vacuum-status       # Check EC volume status"
	@echo ""
	@echo "Manual Testing:"
	@echo "  make ec-vacuum-generate     # 1. Generate data → trigger EC"
	@echo "  # Wait 2-3 minutes for EC encoding to complete"
	@echo "  make ec-vacuum-delete       # 2. Create deletions → garbage"
	@echo "  make ec-vacuum-status       # 3. Check garbage ratios"
	@echo ""
	@echo "Automated Testing:"
	@echo "  make ec-vacuum-high         # High garbage (should trigger)"
	@echo "  make ec-vacuum-low          # Low garbage (should NOT trigger)"
	@echo "  make ec-vacuum-continuous   # Continuous testing cycle"
	@echo ""
	@echo "Monitoring:"
	@echo "  make ec-vacuum-status       # Quick EC volume status"
	@echo "  make ec-vacuum-monitor      # Real-time monitoring"
	@echo ""
	@echo "Configuration:"
	@echo "  Visit: http://localhost:23646/maintenance/config/ec_vacuum"
	@echo "  Monitor: http://localhost:23646/maintenance"
	@echo ""
	@echo "💡 EC volumes need time to encode after data generation"
	@echo "💡 Wait 2-3 minutes between generate and delete phases"
	@echo ""
	@echo "Understanding EC Vacuum:"
	@echo "  • Regular volumes → EC volumes (when >50MB)"
	@echo "  • EC vacuum cleans garbage from EC volumes"
	@echo "  • Requires different thresholds than regular vacuum"
	@echo "  • More complex due to shard distribution"

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