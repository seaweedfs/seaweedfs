# Runs the listing tools and listing strategies of
# https://github.com/varveio/s3-listing-study against a local SeaweedFS.
#
#   make test              build weed, fetch the study and its pinned tools, start
#                          weed server on free ports, run, stop
#   make run ENDPOINT=...  run against an already running endpoint
#
# Needs: go, python3, node/npm, cargo, docker (swath image), curl.
#
# The server runs with -s3.allowBackslashInKey, the AWS-compatible key mode: by
# default SeaweedFS rewrites '\' in object keys to '/', and the edge key set
# holds a key with a backslash.

STUDY_REPO ?= https://github.com/varveio/s3-listing-study.git
STUDY_REV ?= 666ec620d245e16baaaa0cf53461382f42c35917
WORK_DIR ?= $(CURDIR)/.work
STUDY_DIR ?= $(WORK_DIR)/s3-listing-study
TOOLS_DIR ?= $(WORK_DIR)/tools
VENV ?= $(WORK_DIR)/venv
WEED_BINARY ?= $(WORK_DIR)/weed
TOOLS ?= aws-cli,minio-mc,ps3,rclone,s3-fast-list,s3kor,s3p,s4cmd,s5cmd,s7cmd,swath
ACCESS_KEY ?= listing_study_access
SECRET_KEY ?= listing_study_secret
JOBS ?= 4
ONLY ?=

.PHONY: test run build-weed study venv tools start-server stop-server clean

test: build-weed tools
	@$(MAKE) --no-print-directory start-server; status=$$?; \
	if [ $$status -eq 0 ]; then \
		$(MAKE) --no-print-directory run ENDPOINT=http://127.0.0.1:$$(cat $(WORK_DIR)/s3.port); status=$$?; \
	fi; \
	$(MAKE) --no-print-directory stop-server; exit $$status

build-weed:
	mkdir -p $(WORK_DIR)
	cd ../../../weed && go build -buildvcs=false -o $(WEED_BINARY) .

study:
	@if [ "$$(git -C $(STUDY_DIR) rev-parse HEAD 2>/dev/null)" != "$(STUDY_REV)" ]; then \
		git init -q $(STUDY_DIR) && \
		git -C $(STUDY_DIR) fetch -q --depth 1 $(STUDY_REPO) $(STUDY_REV) && \
		git -C $(STUDY_DIR) checkout -q --force FETCH_HEAD; \
	fi

venv:
	@test -x $(VENV)/bin/python || python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -r requirements.txt

tools: study venv
	$(VENV)/bin/python install_tools.py --study $(STUDY_DIR) --tools-dir $(TOOLS_DIR) --tools $(TOOLS)

start-server:
	@mkdir -p $(WORK_DIR)
	@set -e; data=$$(mktemp -d $(WORK_DIR)/data.XXXXXX); ports=$$(python3 free_ports.py 5); \
	set -- $$ports; \
	$(WEED_BINARY) server -filer -s3 -ip 127.0.0.1 -ip.bind 127.0.0.1 -dir=$$data \
		-master.peers=none -master.raftHashicorp -master.electionTimeout 1s -master.volumeSizeLimitMB=1024 \
		-volume.max=50 -volume.preStopSeconds=1 \
		-master.port=$$1 -volume.port=$$2 -filer.port=$$3 -s3.port=$$4 -metricsPort=$$5 \
		-s3.port.iceberg=0 -s3.port.lance=0 -s3.allowBackslashInKey -s3.config=$(CURDIR)/s3.json \
		> $(WORK_DIR)/weed.log 2>&1 & echo $$! > $(WORK_DIR)/weed.pid; \
	echo $$4 > $(WORK_DIR)/s3.port; \
	for i in $$(seq 1 90); do \
		if curl -sf -o /dev/null http://127.0.0.1:$$3/ && curl -s -o /dev/null http://127.0.0.1:$$4/; then \
			echo "SeaweedFS S3 ready on port $$4"; exit 0; fi; \
		kill -0 $$(cat $(WORK_DIR)/weed.pid) || { tail -50 $(WORK_DIR)/weed.log; exit 1; }; \
		sleep 1; \
	done; tail -50 $(WORK_DIR)/weed.log; exit 1

stop-server:
	@if [ -f $(WORK_DIR)/weed.pid ]; then \
		pid=$$(cat $(WORK_DIR)/weed.pid); kill $$pid 2>/dev/null || true; \
		for i in $$(seq 1 20); do kill -0 $$pid 2>/dev/null || break; sleep 0.5; done; \
		rm -f $(WORK_DIR)/weed.pid; \
	fi

run: study
	$(VENV)/bin/python run_study.py --endpoint $(ENDPOINT) \
		--access-key $(ACCESS_KEY) --secret-key $(SECRET_KEY) \
		--study $(STUDY_DIR) --tools-dir $(TOOLS_DIR) --work-dir $(WORK_DIR)/run \
		--tools $(TOOLS) --jobs $(JOBS) --expected-failures expected_failures.txt \
		$(if $(ONLY),--only '$(ONLY)')

clean: stop-server
	rm -rf .work
