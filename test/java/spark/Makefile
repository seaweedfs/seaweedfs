.PHONY: help build test test-local test-docker clean run-example docker-up docker-down

help:
	@echo "SeaweedFS Spark Integration Tests"
	@echo ""
	@echo "Available targets:"
	@echo "  build          - Build the project"
	@echo "  test           - Run integration tests (requires SeaweedFS running)"
	@echo "  test-local     - Run tests against local SeaweedFS"
	@echo "  test-docker    - Run tests in Docker with SeaweedFS"
	@echo "  run-example    - Run the example Spark application"
	@echo "  docker-up      - Start SeaweedFS in Docker"
	@echo "  docker-down    - Stop SeaweedFS Docker containers"
	@echo "  clean          - Clean build artifacts"

build:
	mvn clean package

test:
	@if [ -z "$$SEAWEEDFS_TEST_ENABLED" ]; then \
		echo "Setting SEAWEEDFS_TEST_ENABLED=true"; \
	fi
	SEAWEEDFS_TEST_ENABLED=true mvn test

test-local:
	@echo "Testing against local SeaweedFS (localhost:8888)..."
	./run-tests.sh

test-docker:
	@echo "Running tests in Docker..."
	docker compose up --build --abort-on-container-exit spark-tests
	docker compose down

docker-up:
	@echo "Starting SeaweedFS in Docker..."
	docker compose up -d seaweedfs-master seaweedfs-volume seaweedfs-filer
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "SeaweedFS is ready!"
	@echo "  Master: http://localhost:9333"
	@echo "  Filer:  http://localhost:8888"

docker-down:
	@echo "Stopping SeaweedFS Docker containers..."
	docker compose down -v

run-example:
	@echo "Running example application..."
	@if ! command -v spark-submit > /dev/null; then \
		echo "Error: spark-submit not found. Please install Apache Spark."; \
		exit 1; \
	fi
	spark-submit \
		--class seaweed.spark.SparkSeaweedFSExample \
		--master local[2] \
		--conf spark.hadoop.fs.seaweedfs.impl=seaweed.hdfs.SeaweedFileSystem \
		--conf spark.hadoop.fs.seaweed.filer.host=localhost \
		--conf spark.hadoop.fs.seaweed.filer.port=8888 \
		--conf spark.hadoop.fs.seaweed.filer.port.grpc=18888 \
		--conf spark.hadoop.fs.seaweed.replication="" \
		target/seaweedfs-spark-integration-tests-1.0-SNAPSHOT.jar \
		seaweedfs://localhost:8888/spark-example-output

clean:
	mvn clean
	@echo "Build artifacts cleaned"

verify-seaweedfs:
	@echo "Verifying SeaweedFS connection..."
	@curl -f http://localhost:8888/ > /dev/null 2>&1 && \
		echo "✓ SeaweedFS filer is accessible" || \
		(echo "✗ SeaweedFS filer is not accessible at http://localhost:8888"; exit 1)

.DEFAULT_GOAL := help

