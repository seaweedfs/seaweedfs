# Makefile for EC integration testing
# Usage:
#   make start      - Start the test cluster (master + 6 volume servers + filer)
#   make stop       - Stop the test cluster
#   make populate   - Populate test data (~300MB across 7 volumes)
#   make shell      - Open weed shell connected to the test cluster
#   make clean      - Stop cluster and remove all test data
#   make setup      - Start cluster and populate data (one command)
#
# Requirements: curl, jq

WEED_BINARY := $(shell pwd)/../../weed/weed
TEST_DIR := /tmp/ec_manual_test
# Use non-standard ports to avoid conflicts with existing SeaweedFS servers
MASTER_PORT := 29333
FILER_PORT := 28888
VOLUME_BASE_PORT := 28080
NUM_VOLUME_SERVERS := 6
VOLUME_SIZE_LIMIT_MB := 30
MAX_VOLUMES_PER_SERVER := 10

# Build weed binary if it doesn't exist
$(WEED_BINARY):
	cd ../../weed && go build -o weed .

.PHONY: build
build: $(WEED_BINARY)

.PHONY: start
start: build
	@echo "=== Starting SeaweedFS test cluster ==="
	@mkdir -p $(TEST_DIR)/master $(TEST_DIR)/filer
	@for i in $$(seq 0 $$(($(NUM_VOLUME_SERVERS)-1))); do mkdir -p $(TEST_DIR)/volume$$i; done
	@# Create security.toml with JWT disabled
	@echo "# Disable JWT for testing" > $(TEST_DIR)/security.toml
	@echo '[jwt.signing]' >> $(TEST_DIR)/security.toml
	@echo 'key = ""' >> $(TEST_DIR)/security.toml
	@echo 'expires_after_seconds = 0' >> $(TEST_DIR)/security.toml
	@echo '' >> $(TEST_DIR)/security.toml
	@echo '[jwt.signing.read]' >> $(TEST_DIR)/security.toml
	@echo 'key = ""' >> $(TEST_DIR)/security.toml
	@echo 'expires_after_seconds = 0' >> $(TEST_DIR)/security.toml
	@# Create filer.toml with leveldb2
	@echo '[leveldb2]' > $(TEST_DIR)/filer.toml
	@echo 'enabled = true' >> $(TEST_DIR)/filer.toml
	@echo 'dir = "$(TEST_DIR)/filer/filerldb2"' >> $(TEST_DIR)/filer.toml
	@# Start master
	@echo "Starting master on port $(MASTER_PORT)..."
	@cd $(TEST_DIR) && $(WEED_BINARY) master \
		-port=$(MASTER_PORT) \
		-mdir=$(TEST_DIR)/master \
		-volumeSizeLimitMB=$(VOLUME_SIZE_LIMIT_MB) \
		-ip=127.0.0.1 \
		> $(TEST_DIR)/master/master.log 2>&1 & echo $$! > $(TEST_DIR)/master.pid
	@sleep 3
	@# Start volume servers (run from TEST_DIR to find security.toml)
	@for i in $$(seq 0 $$(($(NUM_VOLUME_SERVERS)-1))); do \
		port=$$(($(VOLUME_BASE_PORT) + $$i)); \
		echo "Starting volume server $$i on port $$port (rack$$i)..."; \
		cd $(TEST_DIR) && $(WEED_BINARY) volume \
			-port=$$port \
			-dir=$(TEST_DIR)/volume$$i \
			-max=$(MAX_VOLUMES_PER_SERVER) \
			-master=127.0.0.1:$(MASTER_PORT) \
			-ip=127.0.0.1 \
			-dataCenter=dc1 \
			-rack=rack$$i \
			> $(TEST_DIR)/volume$$i/volume.log 2>&1 & echo $$! > $(TEST_DIR)/volume$$i.pid; \
	done
	@sleep 3
	@# Start filer (run from TEST_DIR to find security.toml)
	@echo "Starting filer on port $(FILER_PORT)..."
	@cd $(TEST_DIR) && $(WEED_BINARY) filer \
		-port=$(FILER_PORT) \
		-master=127.0.0.1:$(MASTER_PORT) \
		-ip=127.0.0.1 \
		> $(TEST_DIR)/filer/filer.log 2>&1 & echo $$! > $(TEST_DIR)/filer.pid
	@sleep 3
	@echo ""
	@echo "=== Cluster started ==="
	@echo "Master: http://127.0.0.1:$(MASTER_PORT)"
	@echo "Filer:  http://127.0.0.1:$(FILER_PORT)"
	@echo "Volume servers: http://127.0.0.1:$(VOLUME_BASE_PORT) - http://127.0.0.1:$$(($(VOLUME_BASE_PORT) + $(NUM_VOLUME_SERVERS) - 1))"
	@echo ""
	@echo "Run 'make shell' to open weed shell"
	@echo "Run 'make populate' to add test data"

.PHONY: stop
stop:
	@echo "=== Stopping SeaweedFS test cluster ==="
	@# Stop filer by PID
	@-[ -f $(TEST_DIR)/filer.pid ] && kill $$(cat $(TEST_DIR)/filer.pid) 2>/dev/null && rm -f $(TEST_DIR)/filer.pid || true
	@# Stop volume servers by PID
	@for i in $$(seq 0 $$(($(NUM_VOLUME_SERVERS)-1))); do \
		[ -f $(TEST_DIR)/volume$$i.pid ] && kill $$(cat $(TEST_DIR)/volume$$i.pid) 2>/dev/null && rm -f $(TEST_DIR)/volume$$i.pid || true; \
	done
	@# Stop master by PID
	@-[ -f $(TEST_DIR)/master.pid ] && kill $$(cat $(TEST_DIR)/master.pid) 2>/dev/null && rm -f $(TEST_DIR)/master.pid || true
	@# Fallback: use pkill with specific patterns to ensure cleanup
	@-pkill -f "weed filer.*-master=127.0.0.1:$(MASTER_PORT)" 2>/dev/null || true
	@-pkill -f "weed volume.*-dir=$(TEST_DIR)/volume" 2>/dev/null || true
	@-pkill -f "weed master.*-mdir=$(TEST_DIR)/master" 2>/dev/null || true
	@echo "Cluster stopped."

.PHONY: clean
clean: stop
	@echo "Removing test data..."
	@rm -rf $(TEST_DIR)
	@echo "Clean complete."

.PHONY: populate
populate:
	@echo "=== Populating test data (~300MB) ==="
	@# Create a 500KB test file template using mktemp for isolation
	@tmpfile=$$(mktemp) && \
	dd if=/dev/urandom bs=1024 count=500 of=$$tmpfile 2>/dev/null && \
	uploaded=0; \
	for i in $$(seq 1 600); do \
		response=$$(curl -s "http://127.0.0.1:$(MASTER_PORT)/dir/assign?collection=ectest&replication=000"); \
		fid=$$(echo $$response | jq -r '.fid'); \
		url=$$(echo $$response | jq -r '.url'); \
		if [ "$$fid" != "null" ] && [ -n "$$fid" ]; then \
			curl -s -F "file=@$$tmpfile;filename=file_$$i.bin" "http://$$url/$$fid" > /dev/null; \
			uploaded=$$((uploaded + 1)); \
		fi; \
		if [ $$((i % 100)) -eq 0 ]; then \
			echo "Uploaded $$uploaded files..."; \
		fi; \
	done; \
	rm -f $$tmpfile; \
	echo ""; \
	echo "=== Data population complete ==="; \
	echo "Uploaded $$uploaded files (~$$((uploaded * 500 / 1024))MB)"
	@echo ""
	@echo "Volume status:"
	@curl -s "http://127.0.0.1:$(MASTER_PORT)/vol/status" | jq -r \
		'.Volumes.DataCenters.dc1 | to_entries[] | .key as $$rack | .value | to_entries[] | select(.value != null) | .key as $$server | .value[] | select(.Collection == "ectest") | "  Volume \(.Id): \(.FileCount) files, \((.Size/1048576*10|floor)/10)MB - \($$rack)"' 2>/dev/null || true

.PHONY: shell
shell: build
	@echo "Opening weed shell..."
	@echo "Commands to try:"
	@echo "  lock"
	@echo "  volume.list"
	@echo "  ec.encode -collection=ectest -quietFor=1s -force"
	@echo "  ec.balance -collection=ectest"
	@echo "  unlock"
	@echo ""
	@$(WEED_BINARY) shell -master=127.0.0.1:$(MASTER_PORT) -filer=127.0.0.1:$(FILER_PORT)

.PHONY: setup
setup: clean start
	@sleep 2
	@$(MAKE) populate

.PHONY: status
status:
	@echo "=== Cluster Status ==="
	@curl -s "http://127.0.0.1:$(MASTER_PORT)/vol/status" | jq -r \
		'.Volumes.DataCenters.dc1 | to_entries[] | .key as $$rack | .value | to_entries[] | select(.value != null) | .key as $$server | .value[] | select(.Collection == "ectest") | "Volume \(.Id): \(.FileCount) files, \((.Size/1048576*10|floor)/10)MB - \($$rack) (\($$server))"' 2>/dev/null | sort -t: -k1 -n || echo "Cluster not running"
	@echo ""
	@echo "=== EC Shards ==="
	@for i in $$(seq 0 $$(($(NUM_VOLUME_SERVERS)-1))); do \
		count=$$(ls $(TEST_DIR)/volume$$i/*.ec[0-9]* 2>/dev/null | wc -l | tr -d ' '); \
		if [ "$$count" != "0" ]; then \
			echo "  volume$$i (port $$(($(VOLUME_BASE_PORT) + $$i))): $$count EC shard files"; \
		fi; \
	done

.PHONY: help
help:
	@echo "EC Integration Test Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  make start     - Start test cluster (master + 6 volume servers + filer)"
	@echo "  make stop      - Stop test cluster"
	@echo "  make populate  - Populate ~300MB of test data"
	@echo "  make shell     - Open weed shell"
	@echo "  make setup     - Clean, start, and populate (all-in-one)"
	@echo "  make status    - Show cluster and EC shard status"
	@echo "  make clean     - Stop cluster and remove all test data"
	@echo "  make help      - Show this help"
	@echo ""
	@echo "Quick start:"
	@echo "  make setup     # Start cluster and populate data"
	@echo "  make shell     # Open shell to run EC commands"

