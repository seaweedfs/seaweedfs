# Kafka Integration Testing with Docker Compose

This directory contains comprehensive integration tests for SeaweedFS Kafka Gateway using Docker Compose to set up all required dependencies.

## Overview

The Docker Compose setup provides:
- **Apache Kafka** with Zookeeper
- **Confluent Schema Registry** for schema management
- **SeaweedFS** complete stack (Master, Volume, Filer, MQ Broker, MQ Agent)
- **Kafka Gateway** that bridges Kafka protocol to SeaweedFS MQ
- **Test utilities** for schema registration and data setup

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Go 1.21+
- Make (optional, for convenience)

### Basic Usage

1. **Start the environment:**
   ```bash
   make setup-schemas
   ```
   This starts all services and registers test schemas.

2. **Run integration tests:**
   ```bash
   make test-integration
   ```

3. **Run end-to-end tests:**
   ```bash
   make test-e2e
   ```

4. **Clean up:**
   ```bash
   make clean
   ```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Kafka Client  │───▶│  Kafka Gateway   │───▶│   SeaweedFS MQ  │
│   (Sarama/      │    │  (Port 9093)     │    │   (Broker/Agent)│
│    kafka-go)    │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │ Schema Registry  │
                       │  (Port 8081)     │
                       └──────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │  Native Kafka    │
                       │  (Port 9092)     │
                       └──────────────────┘
```

## Services

### Core Services

| Service | Port | Description |
|---------|------|-------------|
| Zookeeper | 2181 | Kafka coordination |
| Kafka | 9092 | Native Kafka broker |
| Schema Registry | 8081 | Confluent Schema Registry |
| SeaweedFS Master | 9333 | SeaweedFS master server |
| SeaweedFS Volume | 8080 | SeaweedFS volume server |
| SeaweedFS Filer | 8888 | SeaweedFS filer server |
| SeaweedFS MQ Broker | 17777 | SeaweedFS message queue broker |
| SeaweedFS MQ Agent | 16777 | SeaweedFS message queue agent |
| Kafka Gateway | 9093 | Kafka protocol gateway to SeaweedFS |

### Test Services

| Service | Description |
|---------|-------------|
| test-setup | Registers schemas and sets up test data |
| kafka-producer | Creates topics and produces test messages |
| kafka-consumer | Consumes messages for verification |

## Available Tests

### Unit Tests
```bash
make test-unit
```
Tests individual Kafka components without external dependencies.

### Integration Tests
```bash
make test-integration
```
Tests with real Kafka, Schema Registry, and SeaweedFS services.

### Schema Tests
```bash
make test-schema
```
Tests schema registry integration and schema evolution.

### End-to-End Tests
```bash
make test-e2e
```
Complete workflow tests with all services running.

### Performance Tests
```bash
make test-performance
```
Benchmarks and performance measurements.

### Client-Specific Tests
```bash
make test-sarama      # IBM Sarama client tests
make test-kafka-go    # Segmentio kafka-go client tests
```

## Development Workflow

### Start Individual Components

```bash
# Start only Kafka ecosystem
make dev-kafka

# Start only SeaweedFS
make dev-seaweedfs

# Start Kafka Gateway
make dev-gateway
```

### Debugging

```bash
# Show all service logs
make logs

# Show specific service logs
make logs-kafka
make logs-schema-registry
make logs-seaweedfs
make logs-gateway

# Check service status
make status

# Debug environment
make debug
```

### Interactive Testing

```bash
# List Kafka topics
make topics

# Create a topic
make create-topic TOPIC=my-test-topic

# Produce messages interactively
make produce TOPIC=my-test-topic

# Consume messages
make consume TOPIC=my-test-topic

# Open shell in containers
make shell-kafka
make shell-gateway
```

## Test Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_GATEWAY_URL` | `localhost:9093` | Kafka Gateway address |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Schema Registry URL |
| `TEST_TIMEOUT` | `10m` | Test timeout duration |

### Running Tests with Custom Configuration

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
KAFKA_GATEWAY_URL=localhost:9093 \
SCHEMA_REGISTRY_URL=http://localhost:8081 \
go test -v ./test/kafka/ -run Integration
```

## Test Schemas

The setup automatically registers these test schemas:

### User Schema
```json
{
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": ["null", "string"], "default": null}
  ]
}
```

### User Event Schema
```json
{
  "type": "record",
  "name": "UserEvent",
  "fields": [
    {"name": "userId", "type": "int"},
    {"name": "eventType", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "data", "type": ["null", "string"], "default": null}
  ]
}
```

### Log Entry Schema
```json
{
  "type": "record",
  "name": "LogEntry",
  "fields": [
    {"name": "level", "type": "string"},
    {"name": "message", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "service", "type": "string"},
    {"name": "metadata", "type": {"type": "map", "values": "string"}}
  ]
}
```

## Troubleshooting

### Common Issues

1. **Services not starting:**
   ```bash
   make status
   make debug
   ```

2. **Port conflicts:**
   - Check if ports 2181, 8081, 9092, 9093, etc. are available
   - Modify `docker-compose.yml` to use different ports if needed

3. **Schema Registry connection issues:**
   ```bash
   curl http://localhost:8081/subjects
   make logs-schema-registry
   ```

4. **Kafka Gateway not responding:**
   ```bash
   nc -z localhost 9093
   make logs-gateway
   ```

5. **Test timeouts:**
   - Increase `TEST_TIMEOUT` environment variable
   - Check service health with `make status`

### Performance Tuning

For better performance in testing:

1. **Increase Docker resources:**
   - Memory: 4GB+
   - CPU: 2+ cores

2. **Adjust Kafka settings:**
   - Modify `docker-compose.yml` Kafka environment variables
   - Tune partition counts and replication factors

3. **SeaweedFS optimization:**
   - Adjust volume size limits
   - Configure appropriate replication settings

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Kafka Integration Tests

on: [push, pull_request]

jobs:
  kafka-integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v3
        with:
          go-version: '1.21'
      - name: Run Kafka Integration Tests
        run: |
          cd test/kafka
          make ci-test
```

### Local CI Testing

```bash
# Run full CI test suite
make ci-test

# Run end-to-end CI tests
make ci-e2e
```

## Contributing

When adding new tests:

1. **Follow naming conventions:**
   - Integration tests: `TestDockerIntegration_*`
   - Unit tests: `Test*_Unit`
   - Performance tests: `TestDockerIntegration_Performance`

2. **Use environment variables:**
   - Check for required environment variables
   - Skip tests gracefully if dependencies unavailable

3. **Clean up resources:**
   - Use unique topic names with timestamps
   - Clean up test data after tests

4. **Add documentation:**
   - Update this README for new test categories
   - Document any new environment variables or configuration

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [SeaweedFS Documentation](https://github.com/seaweedfs/seaweedfs/wiki)
- [IBM Sarama Kafka Client](https://github.com/IBM/sarama)
- [Segmentio kafka-go Client](https://github.com/segmentio/kafka-go)
