# Makefile for generating local S3Tables demo data via Trino and Spark.

.PHONY: help check-deps ensure-work-dir ensure-table-bucket \
	populate-trino populate-spark populate clean

# Local endpoints (host perspective)
S3_ENDPOINT ?= http://localhost:8333
CATALOG_ENDPOINT ?= http://localhost:8181

# Container-visible endpoints (localhost -> host.docker.internal)
S3_ENDPOINT_DOCKER ?= $(subst localhost,host.docker.internal,$(subst 127.0.0.1,host.docker.internal,$(S3_ENDPOINT)))
CATALOG_ENDPOINT_DOCKER ?= $(subst localhost,host.docker.internal,$(subst 127.0.0.1,host.docker.internal,$(CATALOG_ENDPOINT)))

# Auth and table-bucket settings
AWS_ACCESS_KEY_ID ?= admin
AWS_SECRET_ACCESS_KEY ?= admin
AWS_REGION ?= us-east-1
TABLE_ACCOUNT_ID ?= admin
TABLE_BUCKET ?= iceberg-tables
WAREHOUSE ?= s3tablescatalog/$(TABLE_BUCKET)

# Weed shell config for bucket bootstrap
WEED_BIN ?= ../../weed_bin
MASTER_ADDR ?= localhost:9333.19333

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
	@if [ ! -x "$(WEED_BIN)" ]; then \
		echo "weed binary not found at $(WEED_BIN)"; \
		echo "Override with WEED_BIN=/path/to/weed"; \
		exit 1; \
	fi

ensure-work-dir: ## Create temporary workspace for generated files
	@mkdir -p "$(WORK_DIR)/trino/catalog"

ensure-table-bucket: check-deps ## Create table bucket if it does not exist
	@set -eu; \
	if printf "s3tables.bucket -get -name $(TABLE_BUCKET) -account $(TABLE_ACCOUNT_ID)\nexit\n" | "$(WEED_BIN)" shell -master="$(MASTER_ADDR)" >/dev/null 2>&1; then \
		echo "Table bucket already exists: $(TABLE_BUCKET)"; \
	else \
		echo "Creating table bucket: $(TABLE_BUCKET)"; \
		printf "s3tables.bucket -create -name $(TABLE_BUCKET) -account $(TABLE_ACCOUNT_ID)\nexit\n" | "$(WEED_BIN)" shell -master="$(MASTER_ADDR)"; \
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
	@docker run --rm \
		--add-host host.docker.internal:host-gateway \
		-v "$(WORK_DIR)/trino/catalog:/etc/trino/catalog" \
		-v "$(WORK_DIR):/work" \
		-e AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
		-e AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
		-e AWS_REGION="$(AWS_REGION)" \
		"$(TRINO_IMAGE)" \
		trino --catalog iceberg --output-format CSV --file /work/trino_seed.sql

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
		--add-host host.docker.internal:host-gateway \
		-v "$(WORK_DIR):/work" \
		-e AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
		-e AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
		-e AWS_REGION="$(AWS_REGION)" \
		"$(SPARK_IMAGE)" \
		spark-sql \
		--packages "$(SPARK_PACKAGES)" \
		--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
		--conf "spark.sql.defaultCatalog=iceberg" \
		--conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" \
		--conf "spark.sql.catalog.iceberg.type=rest" \
		--conf "spark.sql.catalog.iceberg.uri=$(CATALOG_ENDPOINT_DOCKER)" \
		--conf "spark.sql.catalog.iceberg.warehouse=$(WAREHOUSE)" \
		--conf "spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
		--conf "spark.sql.catalog.iceberg.s3.endpoint=$(S3_ENDPOINT_DOCKER)" \
		--conf "spark.sql.catalog.iceberg.s3.path-style-access=true" \
		--conf "spark.sql.catalog.iceberg.s3.access-key-id=$(AWS_ACCESS_KEY_ID)" \
		--conf "spark.sql.catalog.iceberg.s3.secret-access-key=$(AWS_SECRET_ACCESS_KEY)" \
		--conf "spark.sql.catalog.iceberg.s3.region=$(AWS_REGION)" \
		--conf "spark.sql.catalog.iceberg.rest.sigv4-enabled=true" \
		--conf "spark.sql.catalog.iceberg.rest.signing-name=s3" \
		--conf "spark.sql.catalog.iceberg.rest.signing-region=$(AWS_REGION)" \
		-f /work/spark_seed.sql

populate: populate-trino populate-spark ## Populate sample data through Trino and Spark

clean: ## Remove generated temporary files
	@rm -rf "$(WORK_DIR)"
