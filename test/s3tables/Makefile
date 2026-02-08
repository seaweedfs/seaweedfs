# Makefile for generating local S3Tables demo data via Trino and Spark.

.PHONY: help check-deps check-services ensure-work-dir ensure-table-bucket \
	populate-trino populate-spark populate clean

# Local endpoints (host perspective)
S3_ENDPOINT ?= http://localhost:8333
CATALOG_ENDPOINT ?= http://localhost:8181

# Container-visible endpoints (localhost -> host.docker.internal)
S3_ENDPOINT_DOCKER ?= $(subst localhost,host.docker.internal,$(subst 127.0.0.1,host.docker.internal,$(S3_ENDPOINT)))
CATALOG_ENDPOINT_DOCKER ?= $(subst localhost,host.docker.internal,$(subst 127.0.0.1,host.docker.internal,$(CATALOG_ENDPOINT)))

# Auth and table-bucket settings
# If credentials are not explicitly provided, try to reuse the ones from a running
# local weed mini process (often started with inline AWS_ACCESS_KEY_ID/SECRET).
WEED_RUNTIME_PID := $(shell pgrep -f "weed mini" 2>/dev/null | head -n 1)
DETECTED_AWS_ACCESS_KEY_ID := $(shell if [ -n "$(WEED_RUNTIME_PID)" ]; then ps eww -p "$(WEED_RUNTIME_PID)" 2>/dev/null | sed -n 's/.*AWS_ACCESS_KEY_ID=\([^[:space:]]*\).*/\1/p'; fi)
DETECTED_AWS_SECRET_ACCESS_KEY := $(shell if [ -n "$(WEED_RUNTIME_PID)" ]; then ps eww -p "$(WEED_RUNTIME_PID)" 2>/dev/null | sed -n 's/.*AWS_SECRET_ACCESS_KEY=\([^[:space:]]*\).*/\1/p'; fi)
AWS_ACCESS_KEY_ID ?= $(if $(DETECTED_AWS_ACCESS_KEY_ID),$(DETECTED_AWS_ACCESS_KEY_ID),admin)
AWS_SECRET_ACCESS_KEY ?= $(if $(DETECTED_AWS_SECRET_ACCESS_KEY),$(DETECTED_AWS_SECRET_ACCESS_KEY),admin)
AWS_REGION ?= us-east-1
TABLE_ACCOUNT_ID ?= admin
TABLE_BUCKET ?= iceberg-tables
WAREHOUSE ?= s3tablescatalog/$(TABLE_BUCKET)

# Weed shell config for bucket bootstrap
WEED_BIN ?= weed
MASTER_ADDR ?= localhost:9333

# Runtime images
TRINO_IMAGE ?= trinodb/trino:479
SPARK_IMAGE ?= tabulario/spark-iceberg:latest
SPARK_PACKAGES ?= org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.2,org.apache.iceberg:iceberg-aws-bundle:1.7.2

# Demo data layout
TRINO_NAMESPACE ?= ui_trino
TRINO_TABLE ?= customers
SPARK_NAMESPACE ?= ui_spark
SPARK_TABLE ?= events

# Temp workspace for generated configs/sql
WORK_DIR ?= /tmp/seaweedfs-s3tables-seed

help: ## Show available targets and key variables
	@echo "S3Tables local data generator"
	@echo ""
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Defaults:"
	@echo "  S3_ENDPOINT=$(S3_ENDPOINT)"
	@echo "  CATALOG_ENDPOINT=$(CATALOG_ENDPOINT)"
	@echo "  TABLE_BUCKET=$(TABLE_BUCKET)"
	@echo "  WAREHOUSE=$(WAREHOUSE)"

check-deps: ## Verify local dependencies
	@command -v docker >/dev/null 2>&1 || (echo "docker is required" && exit 1)
	@if command -v $(WEED_BIN) >/dev/null 2>&1; then \
		echo "Using weed binary: $(WEED_BIN)"; \
	elif [ -x "$(WEED_BIN)" ]; then \
		echo "Using weed binary path: $(WEED_BIN)"; \
	else \
		echo "weed binary not found at $(WEED_BIN)"; \
		echo "Override with WEED_BIN=/path/to/weed"; \
		exit 1; \
	fi; \
	if [ "$(origin AWS_ACCESS_KEY_ID)" = "file" ] && [ -n "$(DETECTED_AWS_ACCESS_KEY_ID)" ]; then \
		echo "Using AWS access key from running weed mini process: $(DETECTED_AWS_ACCESS_KEY_ID)"; \
	fi

check-services: check-deps ## Validate local master/S3/catalog endpoints before seeding
	@set -eu; \
	master_addr="$(MASTER_ADDR)"; \
	master_addr="$${master_addr%%,*}"; \
	master_http=$$(printf '%s' "$$master_addr" | sed -E 's/^([^:]+:[0-9]+)\.[0-9]+$$/\1/'); \
	echo "check-services: probing master status at http://$$master_http/cluster/status"; \
	status_json=$$(curl -fsS --max-time 3 "http://$$master_http/cluster/status") || { \
		echo "ERROR: cannot reach master status endpoint at http://$$master_http/cluster/status"; \
		echo "Set MASTER_ADDR to a reachable master, e.g. MASTER_ADDR=localhost:9333"; \
		exit 1; \
	}; \
	leader_addr=$$(printf '%s' "$$status_json" | sed -n 's/.*"Leader":"\([^"]*\)".*/\1/p'); \
	if [ -n "$$leader_addr" ]; then \
		leader_http=$$(printf '%s' "$$leader_addr" | sed -E 's/^([^:]+:[0-9]+)\.[0-9]+$$/\1/'); \
		echo "check-services: master reports leader $$leader_addr (http: $$leader_http)"; \
		if ! curl -fsS --max-time 3 "http://$$leader_http/cluster/status" >/dev/null; then \
			echo "ERROR: master advertises leader $$leader_addr (http: $$leader_http), but it is unreachable from this host."; \
			echo "This causes 'weed shell' (and make populate targets) to block waiting for master connection."; \
			echo "Fix master advertised address (-ip / -ip.bind) or point MASTER_ADDR to a reachable leader."; \
			exit 1; \
		fi; \
	fi; \
	echo "check-services: probing S3 endpoint $(S3_ENDPOINT)"; \
	s3_hostport=$$(printf '%s' "$(S3_ENDPOINT)" | sed -E 's#^https?://([^/]+)/?.*$$#\1#'); \
	s3_host=$${s3_hostport%%:*}; \
	s3_port=$${s3_hostport##*:}; \
	if [ "$$s3_host" = "$$s3_port" ]; then \
		s3_port=80; \
	fi; \
	if command -v nc >/dev/null 2>&1; then \
		nc -z -w 3 "$$s3_host" "$$s3_port" || { \
			echo "ERROR: S3 endpoint $(S3_ENDPOINT) is not reachable"; \
			exit 1; \
		}; \
	else \
		curl -sS --max-time 3 -o /dev/null "$(S3_ENDPOINT)" || { \
			echo "ERROR: S3 endpoint $(S3_ENDPOINT) is not reachable"; \
			exit 1; \
		}; \
	fi; \
	echo "check-services: probing Iceberg catalog endpoint $(CATALOG_ENDPOINT)/v1/config"; \
	curl -sS --max-time 3 -o /dev/null "$(CATALOG_ENDPOINT)/v1/config" || { \
		echo "ERROR: Iceberg catalog endpoint $(CATALOG_ENDPOINT)/v1/config is not responding"; \
		exit 1; \
	}; \
	echo "check-services: probing Iceberg auth with access key $(AWS_ACCESS_KEY_ID)"; \
	auth_body=$$(mktemp -t s3tables-auth.XXXXXX); \
	auth_code=$$(curl -sS --max-time 5 \
		--aws-sigv4 "aws:amz:$(AWS_REGION):s3tables" \
		--user "$(AWS_ACCESS_KEY_ID):$(AWS_SECRET_ACCESS_KEY)" \
		-o "$$auth_body" -w "%{http_code}" "$(CATALOG_ENDPOINT)/v1/namespaces" || true); \
	if [ "$$auth_code" = "403" ] && grep -q "access key ID you provided does not exist" "$$auth_body"; then \
		echo "ERROR: AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) is not recognized by the running Iceberg service."; \
		if [ -n "$(DETECTED_AWS_ACCESS_KEY_ID)" ] && [ -n "$(DETECTED_AWS_SECRET_ACCESS_KEY)" ] && [ "$(AWS_ACCESS_KEY_ID)" != "$(DETECTED_AWS_ACCESS_KEY_ID)" ]; then \
			echo "Detected running weed mini credentials via process env:"; \
			echo "  AWS_ACCESS_KEY_ID=$(DETECTED_AWS_ACCESS_KEY_ID)"; \
			echo "Retry with:"; \
			echo "  make populate AWS_ACCESS_KEY_ID=$(DETECTED_AWS_ACCESS_KEY_ID) AWS_SECRET_ACCESS_KEY=<secret>"; \
		fi; \
		rm -f "$$auth_body"; \
		exit 1; \
	fi; \
	if [ "$$auth_code" = "403" ] && grep -q "Access Denied" "$$auth_body"; then \
		echo "ERROR: provided AWS credentials are valid but do not have permission for Iceberg REST operations."; \
		rm -f "$$auth_body"; \
		exit 1; \
	fi; \
	rm -f "$$auth_body"

ensure-work-dir: ## Create temporary workspace for generated files
	@mkdir -p "$(WORK_DIR)/trino/catalog"

ensure-table-bucket: check-services ## Create table bucket if it does not exist
	@set -eu; \
	echo "ensure-table-bucket: checking table bucket $(TABLE_BUCKET) for account $(TABLE_ACCOUNT_ID)"; \
	get_output=$$(printf "s3tables.bucket -get -name $(TABLE_BUCKET) -account $(TABLE_ACCOUNT_ID)\nexit\n" | "$(WEED_BIN)" shell -master="$(MASTER_ADDR)" 2>&1 || true); \
	if printf '%s' "$$get_output" | grep -q "NoSuchBucket"; then \
		echo "Creating table bucket: $(TABLE_BUCKET)"; \
		create_output=$$(printf "s3tables.bucket -create -name $(TABLE_BUCKET) -account $(TABLE_ACCOUNT_ID)\nexit\n" | "$(WEED_BIN)" shell -master="$(MASTER_ADDR)" 2>&1 || true); \
		if ! printf '%s' "$$create_output" | grep -q "ARN: arn:aws:s3tables:"; then \
			echo "ERROR: failed to create table bucket $(TABLE_BUCKET)."; \
			printf '%s\n' "$$create_output"; \
			exit 1; \
		fi; \
		echo "Created table bucket: $(TABLE_BUCKET)"; \
	elif printf '%s' "$$get_output" | grep -q "ARN: arn:aws:s3tables:"; then \
		echo "Table bucket already exists: $(TABLE_BUCKET)"; \
	else \
		echo "ERROR: unable to verify table bucket $(TABLE_BUCKET)."; \
		printf '%s\n' "$$get_output"; \
		exit 1; \
	fi

populate-trino: ensure-table-bucket ensure-work-dir ## Populate sample data via Trino
	@echo "populate-trino: writing Trino catalog and SQL seeds"
	@printf '%s\n' \
		'connector.name=iceberg' \
		'iceberg.catalog.type=rest' \
		'iceberg.rest-catalog.uri=$(CATALOG_ENDPOINT_DOCKER)' \
		'iceberg.rest-catalog.warehouse=$(WAREHOUSE)' \
		'iceberg.file-format=PARQUET' \
		'iceberg.unique-table-location=true' \
		'fs.native-s3.enabled=true' \
		's3.endpoint=$(S3_ENDPOINT_DOCKER)' \
		's3.path-style-access=true' \
		's3.signer-type=AwsS3V4Signer' \
		's3.aws-access-key=$(AWS_ACCESS_KEY_ID)' \
		's3.aws-secret-key=$(AWS_SECRET_ACCESS_KEY)' \
		's3.region=$(AWS_REGION)' \
		'iceberg.rest-catalog.security=SIGV4' \
		'iceberg.rest-catalog.signing-name=s3tables' \
		'iceberg.rest-catalog.prefix=$(TABLE_BUCKET)' \
		> "$(WORK_DIR)/trino/catalog/iceberg.properties"
	@printf '%s\n' \
		'CREATE SCHEMA IF NOT EXISTS iceberg.$(TRINO_NAMESPACE);' \
		'CREATE TABLE IF NOT EXISTS iceberg.$(TRINO_NAMESPACE).$(TRINO_TABLE) (' \
		'  customer_id INTEGER,' \
		'  customer_name VARCHAR,' \
		'  country VARCHAR,' \
		'  signup_date DATE' \
		') WITH (format = '\''PARQUET'\'');' \
		'INSERT INTO iceberg.$(TRINO_NAMESPACE).$(TRINO_TABLE)' \
		'SELECT *' \
		'FROM (' \
		'  VALUES' \
		'    (1, '\''Amanda Olson'\'', '\''US'\'', DATE '\''2024-01-10'\''),' \
		'    (2, '\''Leonard Eads'\'', '\''US'\'', DATE '\''2024-03-22'\''),' \
		'    (3, '\''Debbie Ward'\'', '\''MX'\'', DATE '\''2025-07-15'\''),' \
		'    (4, '\''Donald Holt'\'', '\''CA'\'', DATE '\''2025-11-02'\'')' \
		') AS src (customer_id, customer_name, country, signup_date)' \
		'WHERE NOT EXISTS (' \
		'  SELECT 1' \
		'  FROM iceberg.$(TRINO_NAMESPACE).$(TRINO_TABLE) dst' \
		'  WHERE dst.customer_id = src.customer_id' \
		');' \
		'SELECT count(*) AS row_count FROM iceberg.$(TRINO_NAMESPACE).$(TRINO_TABLE);' \
	> "$(WORK_DIR)/trino_seed.sql"
	@echo "populate-trino: starting Trino docker run"
	@set -eu; \
	container_name="seaweedfs-s3tables-trino-seed-$$RANDOM-$$RANDOM"; \
	echo "populate-trino: launching Trino server container $$container_name"; \
	docker run -d --name "$$container_name" \
		--add-host host.docker.internal:host-gateway \
		-v "$(WORK_DIR)/trino/catalog:/etc/trino/catalog" \
		-v "$(WORK_DIR):/work" \
		-e AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
		-e AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
		-e AWS_REGION="$(AWS_REGION)" \
		"$(TRINO_IMAGE)" >/dev/null; \
	trap 'docker rm -f "$$container_name" >/dev/null 2>&1 || true' EXIT INT TERM; \
	echo "populate-trino: waiting for Trino server readiness"; \
	ready=0; \
	for i in $$(seq 1 90); do \
		if [ "$$(docker inspect -f '{{.State.Running}}' "$$container_name" 2>/dev/null || echo false)" != "true" ]; then \
			break; \
		fi; \
		if docker exec "$$container_name" trino --execute "SELECT 1" >/dev/null 2>&1; then \
			ready=1; \
			break; \
		fi; \
		sleep 1; \
	done; \
	if [ "$$ready" -ne 1 ]; then \
		echo "ERROR: Trino server in container $$container_name did not become ready"; \
		docker logs "$$container_name" | tail -n 200; \
		exit 1; \
	fi; \
	echo "populate-trino: running SQL seed script"; \
	if ! docker exec "$$container_name" trino --catalog iceberg --output-format CSV --file /work/trino_seed.sql; then \
		echo "ERROR: Trino seed query failed. Recent container logs:"; \
		docker logs "$$container_name" | tail -n 200; \
		exit 1; \
	fi

populate-spark: ensure-table-bucket ensure-work-dir ## Populate sample data via Spark SQL
	@printf '%s\n' \
		'CREATE NAMESPACE IF NOT EXISTS iceberg.$(SPARK_NAMESPACE);' \
		'CREATE TABLE IF NOT EXISTS iceberg.$(SPARK_NAMESPACE).$(SPARK_TABLE) (' \
		'  event_id BIGINT,' \
		'  user_id STRING,' \
		'  event_type STRING,' \
		'  event_day STRING' \
		') USING iceberg;' \
		'INSERT INTO iceberg.$(SPARK_NAMESPACE).$(SPARK_TABLE)' \
		'SELECT *' \
		'FROM (' \
		'  VALUES' \
		'    (1, '\''u001'\'', '\''page_view'\'', '\''2026-02-01'\''),' \
		'    (2, '\''u002'\'', '\''checkout'\'', '\''2026-02-02'\''),' \
		'    (3, '\''u003'\'', '\''purchase'\'', '\''2026-02-03'\''),' \
		'    (4, '\''u004'\'', '\''refund'\'', '\''2026-02-04'\'')' \
		') AS src (event_id, user_id, event_type, event_day)' \
		'WHERE NOT EXISTS (' \
		'  SELECT 1' \
		'  FROM iceberg.$(SPARK_NAMESPACE).$(SPARK_TABLE) dst' \
		'  WHERE dst.event_id = src.event_id' \
		');' \
		'SELECT count(*) AS row_count FROM iceberg.$(SPARK_NAMESPACE).$(SPARK_TABLE);' \
	> "$(WORK_DIR)/spark_seed.sql"
	@echo "populate-spark: starting Spark docker run"
	@docker run --rm \
		--entrypoint /bin/bash \
		--add-host host.docker.internal:host-gateway \
		-v "$(WORK_DIR):/work" \
		-e AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
		-e AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
		-e AWS_REGION="$(AWS_REGION)" \
		"$(SPARK_IMAGE)" \
		-lc '/opt/spark/bin/spark-sql \
			--packages "$(SPARK_PACKAGES)" \
			--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
			--conf "spark.sql.defaultCatalog=iceberg" \
			--conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" \
			--conf "spark.sql.catalog.iceberg.type=rest" \
			--conf "spark.sql.catalog.iceberg.uri=$(CATALOG_ENDPOINT_DOCKER)" \
			--conf "spark.sql.catalog.iceberg.warehouse=$(WAREHOUSE)" \
			--conf "spark.sql.catalog.iceberg.prefix=$(TABLE_BUCKET)" \
			--conf "spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
			--conf "spark.sql.catalog.iceberg.s3.endpoint=$(S3_ENDPOINT_DOCKER)" \
			--conf "spark.sql.catalog.iceberg.s3.path-style-access=true" \
			--conf "spark.sql.catalog.iceberg.s3.access-key-id=$(AWS_ACCESS_KEY_ID)" \
			--conf "spark.sql.catalog.iceberg.s3.secret-access-key=$(AWS_SECRET_ACCESS_KEY)" \
			--conf "spark.sql.catalog.iceberg.s3.region=$(AWS_REGION)" \
			--conf "spark.sql.catalog.iceberg.rest.sigv4-enabled=true" \
			--conf "spark.sql.catalog.iceberg.rest.signing-name=s3tables" \
			--conf "spark.sql.catalog.iceberg.rest.signing-region=$(AWS_REGION)" \
			--conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog" \
			--conf "spark.sql.catalog.spark_catalog.type=rest" \
			--conf "spark.sql.catalog.spark_catalog.uri=$(CATALOG_ENDPOINT_DOCKER)" \
			--conf "spark.sql.catalog.spark_catalog.warehouse=$(WAREHOUSE)" \
			--conf "spark.sql.catalog.spark_catalog.prefix=$(TABLE_BUCKET)" \
			--conf "spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
			--conf "spark.sql.catalog.spark_catalog.s3.endpoint=$(S3_ENDPOINT_DOCKER)" \
			--conf "spark.sql.catalog.spark_catalog.s3.path-style-access=true" \
			--conf "spark.sql.catalog.spark_catalog.s3.access-key-id=$(AWS_ACCESS_KEY_ID)" \
			--conf "spark.sql.catalog.spark_catalog.s3.secret-access-key=$(AWS_SECRET_ACCESS_KEY)" \
			--conf "spark.sql.catalog.spark_catalog.s3.region=$(AWS_REGION)" \
			--conf "spark.sql.catalog.spark_catalog.rest.sigv4-enabled=true" \
			--conf "spark.sql.catalog.spark_catalog.rest.signing-name=s3tables" \
			--conf "spark.sql.catalog.spark_catalog.rest.signing-region=$(AWS_REGION)" \
			-f /work/spark_seed.sql'

populate: populate-trino populate-spark ## Populate sample data through Trino and Spark

clean: ## Remove generated temporary files
	@rm -rf "$(WORK_DIR)"
