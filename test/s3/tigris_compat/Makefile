# Runs https://github.com/tigrisdata-community/s3-api-compat-tests against a
# local SeaweedFS S3 gateway and compares the report with expected_failures.txt.

.PHONY: all test help build-weed build-suite start-server stop-server run-suite test-with-server logs clean

SUITE_REPO := https://github.com/tigrisdata-community/s3-api-compat-tests.git
SUITE_COMMIT := e846275e489ec30b324344d462e698252c3a1f67
SUITE_DIR := s3-api-compat-tests
SUITE_STAMP := $(SUITE_DIR)/.seaweedfs-prepared

WEED_BINARY := ../../../weed/weed_binary
S3_PORT ?= 8335
MASTER_PORT ?= 9335
VOLUME_PORT ?= 8085
FILER_PORT ?= 8889
WEBDAV_PORT ?= 7335
ADMIN_PORT ?= 23647
SERVER_DIR := ./test-volume-data
S3_CONFIG := ./test_s3.json
# Succeeds while weed-server.pid names the server start-server launched: the
# process at that pid must have the start time recorded with it, so a process
# that reused the pid is left alone.
OWN_SERVER_RUNNING = [ -f weed-server.pid ] && [ -s weed-server.started ] && \
	[ "$$(ps -p "$$(cat weed-server.pid)" -o lstart= 2>/dev/null)" = "$$(cat weed-server.started)" ]

.DEFAULT_GOAL := help

all: test

test: test-with-server

help:
	@echo "s3-api-compat-tests (Tigris) against SeaweedFS"
	@echo ""
	@echo "  build-weed        - Build the SeaweedFS binary"
	@echo "  build-suite       - Fetch the suite at $(SUITE_COMMIT) and build it"
	@echo "  start-server      - Start weed mini with S3 on port $(S3_PORT)"
	@echo "  run-suite         - Run the suite and check expected_failures.txt"
	@echo "  stop-server       - Stop the test server"
	@echo "  test-with-server  - Start server, run suite, stop server"
	@echo "  logs              - Show server logs"
	@echo "  clean             - Remove test artifacts"

build-weed:
	@cd ../../../weed && go build -o weed_binary .

build-suite:
	@want="$(SUITE_COMMIT) $$(git hash-object seaweedfs-provider.patch)"; \
	if [ "$$(cat $(SUITE_STAMP) 2>/dev/null)" != "$$want" ]; then \
		rm -rf $(SUITE_DIR) && mkdir -p $(SUITE_DIR) && \
		git -C $(SUITE_DIR) init -q && \
		git -C $(SUITE_DIR) fetch -q --depth 1 $(SUITE_REPO) $(SUITE_COMMIT) && \
		git -C $(SUITE_DIR) checkout -q FETCH_HEAD && \
		git -C $(SUITE_DIR) apply ../seaweedfs-provider.patch && \
		echo "$$want" > $(SUITE_STAMP); \
	fi
	@cd $(SUITE_DIR) && go build -o ../compat-runner .

start-server: build-weed
	@if curl -sS --max-time 2 -o /dev/null http://127.0.0.1:$(S3_PORT)/ >/dev/null 2>&1; then \
		echo "port $(S3_PORT) is already served by another process; refusing to test against it"; \
		exit 1; \
	fi
	@if $(OWN_SERVER_RUNNING); then \
		echo "the server this Makefile started (pid $$(cat weed-server.pid)) is still running; run make stop-server first"; \
		exit 1; \
	fi
	@rm -rf weed-server.pid weed-server.started $(SERVER_DIR)
	@mkdir -p $(SERVER_DIR)
	@$(WEED_BINARY) mini \
		-dir=$(SERVER_DIR) \
		-ip=127.0.0.1 \
		-ip.bind=127.0.0.1 \
		-master.port=$(MASTER_PORT) \
		-volume.port=$(VOLUME_PORT) \
		-filer.port=$(FILER_PORT) \
		-webdav.port=$(WEBDAV_PORT) \
		-admin.port=$(ADMIN_PORT) \
		-admin.port.legacy=0 \
		-s3.port=$(S3_PORT) \
		-s3.config=$(S3_CONFIG) \
		-master.peers=none \
		> weed-test.log 2>&1 & echo $$! > weed-server.pid; \
	ps -p $$! -o lstart= > weed-server.started
	@for i in $$(seq 1 60); do \
		kill -0 $$(cat weed-server.pid) 2>/dev/null || { \
			echo "SeaweedFS server exited during startup"; \
			cat weed-test.log; \
			exit 1; \
		}; \
		if curl -sS --max-time 2 -o /dev/null http://127.0.0.1:$(S3_PORT)/ >/dev/null 2>&1; then \
			echo "SeaweedFS S3 server is ready on port $(S3_PORT)"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "SeaweedFS S3 server failed to start"; \
	cat weed-test.log; \
	$(MAKE) --no-print-directory stop-server; \
	exit 1

stop-server:
	@if [ -f weed-server.pid ]; then \
		if $(OWN_SERVER_RUNNING); then \
			kill $$(cat weed-server.pid) 2>/dev/null || true; \
			for i in $$(seq 1 30); do $(OWN_SERVER_RUNNING) || break; sleep 1; done; \
			if $(OWN_SERVER_RUNNING); then \
				echo "the server this Makefile started (pid $$(cat weed-server.pid)) is still running after 30s; keeping weed-server.pid"; \
				exit 1; \
			fi; \
		fi; \
		rm -f weed-server.pid weed-server.started; \
	fi

run-suite: build-suite
	SEAWEEDFS_S3_ENDPOINT=http://127.0.0.1:$(S3_PORT) \
	SEAWEEDFS_S3_ACCESS_KEY_ID=admin \
	SEAWEEDFS_S3_SECRET_ACCESS_KEY=admin \
		./compat-runner --providers=seaweedfs --output=results.json
	python3 check_results.py results.json expected_failures.txt

test-with-server:
	@$(MAKE) start-server || exit $$?; \
	$(MAKE) run-suite; TEST_RESULT=$$?; \
	$(MAKE) stop-server; STOP_RESULT=$$?; \
	if [ $$TEST_RESULT -ne 0 ]; then exit $$TEST_RESULT; fi; exit $$STOP_RESULT

logs:
	@test -f weed-test.log && tail -100 weed-test.log || echo "No log file found"

clean: stop-server
	@rm -rf $(SERVER_DIR) $(SUITE_DIR) compat-runner results.json weed-test.log weed-server.pid weed-server.started
