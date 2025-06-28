.PHONY: build run clean test deps proto integration-test test-all

# Build the telemetry server
build:
	go build -o telemetry-server .

# Run the server in development mode
run:
	go run . -port=8080 -dashboard=true -cleanup=1h -max-age=24h

# Run the server in production mode
run-prod:
	./telemetry-server -port=8080 -dashboard=true -cleanup=24h -max-age=720h

# Clean build artifacts
clean:
	rm -f telemetry-server
	rm -f ../test/telemetry-server-test.log
	go clean

# Run unit tests
test:
	go test ./...

# Run integration tests
integration-test:
	@echo "🧪 Running telemetry integration tests..."
	cd ../../ && go run telemetry/test/integration.go

# Run all tests (unit + integration)
test-all: test integration-test

# Install dependencies
deps:
	go mod download
	go mod tidy

# Generate protobuf code (requires protoc)
proto:
	cd .. && protoc --go_out=. --go_opt=paths=source_relative proto/telemetry.proto

# Build Docker image
docker-build:
	docker build -t seaweedfs-telemetry .

# Run with Docker
docker-run:
	docker run -p 8080:8080 seaweedfs-telemetry -port=8080 -dashboard=true

# Development with auto-reload (requires air: go install github.com/cosmtrek/air@latest)
dev:
	air

# Check if protoc is available
check-protoc:
	@which protoc > /dev/null || (echo "protoc is required for proto generation. Install from https://grpc.io/docs/protoc-installation/" && exit 1)

# Full development setup
setup: check-protoc deps proto build

# Run a quick smoke test
smoke-test: build
	@echo "🔥 Running smoke test..."
	@timeout 10s ./telemetry-server -port=18081 > /dev/null 2>&1 & \
	SERVER_PID=$$!; \
	sleep 2; \
	if curl -s http://localhost:18081/health > /dev/null; then \
		echo "✅ Smoke test passed - server responds to health check"; \
	else \
		echo "❌ Smoke test failed - server not responding"; \
		exit 1; \
	fi; \
	kill $$SERVER_PID 2>/dev/null || true

# Continuous integration target
ci: deps proto build test integration-test
	@echo "🎉 All CI tests passed!"

# Help
help:
	@echo "Available targets:"
	@echo "  build           - Build the telemetry server binary"
	@echo "  run             - Run server in development mode"
	@echo "  run-prod        - Run server in production mode"
	@echo "  clean           - Clean build artifacts"
	@echo "  test            - Run unit tests"
	@echo "  integration-test- Run integration tests"
	@echo "  test-all        - Run all tests (unit + integration)"
	@echo "  deps            - Install Go dependencies"
	@echo "  proto           - Generate protobuf code"
	@echo "  docker-build    - Build Docker image"
	@echo "  docker-run      - Run with Docker"
	@echo "  dev             - Run with auto-reload (requires air)"
	@echo "  smoke-test      - Quick server health check"
	@echo "  setup           - Full development setup"
	@echo "  ci              - Continuous integration (all tests)"
	@echo "  help            - Show this help" 