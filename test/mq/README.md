# SeaweedMQ Integration Test Suite

This directory contains a comprehensive integration test suite for SeaweedMQ, designed to validate all critical functionalities from basic pub/sub operations to advanced features like auto-scaling, failover, and performance testing.

## Overview

The integration test suite provides:

- **Automated Environment Setup**: Docker Compose based test clusters
- **Comprehensive Test Coverage**: Basic pub/sub, scaling, failover, performance
- **Monitoring & Metrics**: Prometheus and Grafana integration
- **CI/CD Ready**: Configurable for continuous integration pipelines
- **Load Testing**: Performance benchmarks and stress tests

## Architecture

The test environment consists of:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Master Cluster │    │ Volume Servers  │    │  Filer Cluster  │
│   (3 nodes)     │    │   (3 nodes)     │    │   (2 nodes)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Broker Cluster  │    │ Test Framework  │    │   Monitoring    │
│   (3 nodes)     │    │   (Go Tests)    │    │ (Prometheus +   │
└─────────────────┘    └─────────────────┘    │   Grafana)      │
                                              └─────────────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Go 1.21+
- Make
- 8GB+ RAM recommended
- 20GB+ disk space for test data

### Basic Usage

Choose one of three approaches:

#### Option 1: Quick Cluster Setup (Fastest)
Just starts a SeaweedMQ cluster - no test runner, no build required.

1. **Start Cluster**:
   ```bash
   cd test/mq
   make up-cluster
   ```

2. **Test manually** or with your own code against:
   - Masters: http://localhost:19333, http://localhost:19334, http://localhost:19335
   - Brokers: localhost:17777, localhost:17778, localhost:17779
   - Filers: http://localhost:18888, http://localhost:18889

#### Option 2: Use Production Images (Recommended for testing)
Uses official SeaweedFS images from Docker Hub with test runner.

1. **Start Test Environment**:
   ```bash
   cd test/mq
   make up-prod
   ```

2. **Run Tests**:
   ```bash
   make test-basic      # Basic pub/sub tests
   ```

#### Option 3: Build from Source (Development)
Builds SeaweedFS from your local source code to test latest changes.

1. **Start Test Environment**:
   ```bash
   cd test/mq
   make up  # This will build and start
   ```

2. **Run Tests**:
   ```bash
   make test-basic      # Basic pub/sub tests
   ```

#### Common Commands

3. **Run All Tests**:
   ```bash
   make test
   ```

4. **Run Specific Test Categories**:
   ```bash
   make test-performance # Performance tests
   make test-failover   # Failover tests
   make test-agent      # Agent tests
   ```

5. **Quick Smoke Test**:
   ```bash
   make smoke-test
   ```

6. **Check Health**:
   ```bash
   make health
   ```

7. **Clean Up**:
   ```bash
   make down
   ```

## Test Categories

### 1. Basic Functionality Tests

**File**: `integration/basic_pubsub_test.go`

- **TestBasicPublishSubscribe**: Basic message publishing and consumption
- **TestMultipleConsumers**: Load balancing across multiple consumers
- **TestMessageOrdering**: FIFO ordering within partitions
- **TestSchemaValidation**: Schema validation and complex nested structures

### 2. Partitioning and Scaling Tests

**File**: `integration/scaling_test.go` (to be implemented)

- **TestPartitionDistribution**: Message distribution across partitions
- **TestAutoSplitMerge**: Automatic partition split/merge based on load
- **TestBrokerScaling**: Adding/removing brokers during operation
- **TestLoadBalancing**: Even load distribution verification

### 3. Failover and Reliability Tests

**File**: `integration/failover_test.go` (to be implemented)

- **TestBrokerFailover**: Leader failover scenarios
- **TestBrokerRecovery**: Recovery from broker failures
- **TestMessagePersistence**: Data durability across restarts
- **TestFollowerReplication**: Leader-follower consistency

### 4. Performance Tests

**File**: `integration/performance_test.go` (to be implemented)

- **TestHighThroughputPublish**: High-volume message publishing
- **TestHighThroughputSubscribe**: High-volume message consumption
- **TestLatencyMeasurement**: End-to-end latency analysis
- **TestResourceUtilization**: CPU, memory, and disk usage

### 5. Agent Tests

**File**: `integration/agent_test.go` (to be implemented)

- **TestAgentPublishSessions**: Session management for publishers
- **TestAgentSubscribeSessions**: Session management for subscribers
- **TestAgentFailover**: Agent reconnection and failover
- **TestAgentConcurrency**: Concurrent session handling

## Configuration

### Environment Variables

The test framework supports configuration via environment variables:

```bash
# Cluster endpoints
SEAWEED_MASTERS="master0:9333,master1:9334,master2:9335"
SEAWEED_BROKERS="broker1:17777,broker2:17778,broker3:17779"
SEAWEED_FILERS="filer1:8888,filer2:8889"

# Test configuration
GO_TEST_TIMEOUT="30m"
TEST_RESULTS_DIR="/test-results"
```

### Docker Compose Override

Create `docker-compose.override.yml` to customize the test environment:

```yaml
version: '3.9'
services:
  broker1:
    environment:
      - CUSTOM_ENV_VAR=value
  test-runner:
    volumes:
      - ./custom-config:/config
```

## Monitoring and Metrics

### Prometheus Metrics

Access Prometheus at: http://localhost:19090

Key metrics to monitor:
- Message throughput: `seaweedmq_messages_published_total`
- Consumer lag: `seaweedmq_consumer_lag_seconds`
- Broker health: `seaweedmq_broker_health`
- Resource usage: `seaweedfs_disk_usage_bytes`

### Grafana Dashboards

Access Grafana at: http://localhost:13000 (admin/admin)

Pre-configured dashboards:
- **SeaweedMQ Overview**: System health and throughput
- **Performance Metrics**: Latency and resource usage
- **Error Analysis**: Error rates and failure patterns

## Development

### Writing New Tests

1. **Create Test File**:
   ```bash
   touch integration/my_new_test.go
   ```

2. **Use Test Framework**:
   ```go
   func TestMyFeature(t *testing.T) {
       suite := NewIntegrationTestSuite(t)
       require.NoError(t, suite.Setup())
       
       // Your test logic here
   }
   ```

3. **Run Specific Test**:
   ```bash
   go test -v ./integration/ -run TestMyFeature
   ```

### Test Framework Components

**IntegrationTestSuite**: Base test framework with cluster management
**MessageCollector**: Utility for collecting and verifying received messages
**TestMessage**: Standard message structure for testing
**Schema Builders**: Helpers for creating test schemas

### Local Development

Run tests against a local SeaweedMQ cluster:

```bash
make test-dev
```

This uses local binaries instead of Docker containers.

## Continuous Integration

### GitHub Actions Example

```yaml
name: Integration Tests
on: [push, pull_request]

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v3
        with:
          go-version: 1.21
      - name: Run Integration Tests
        run: |
          cd test/mq
          make test
```

### Jenkins Pipeline

```groovy
pipeline {
    agent any
    stages {
        stage('Setup') {
            steps {
                sh 'cd test/mq && make up'
            }
        }
        stage('Test') {
            steps {
                sh 'cd test/mq && make test'
            }
            post {
                always {
                    sh 'cd test/mq && make down'
                }
            }
        }
    }
}
```

## Troubleshooting

### Common Issues

1. **Port Conflicts**:
   ```bash
   # Check port usage
   netstat -tulpn | grep :19333
   
   # Kill conflicting processes
   sudo kill -9 $(lsof -t -i:19333)
   ```

2. **Docker Resource Issues**:
   ```bash
   # Increase Docker memory (8GB+)
   # Clean up Docker resources
   docker system prune -a
   ```

3. **Test Timeouts**:
   ```bash
   # Increase timeout
   GO_TEST_TIMEOUT=60m make test
   ```

### Debug Mode

Run tests with verbose logging:

```bash
docker-compose -f docker-compose.test.yml run --rm test-runner \
  sh -c "go test -v -race ./test/mq/integration/... -args -test.v"
```

### Container Logs

View real-time logs:

```bash
make logs

# Or specific service
docker-compose -f docker-compose.test.yml logs -f broker1
```

## Performance Benchmarks

### Throughput Benchmarks

```bash
make benchmark
```

Expected performance (on 8-core, 16GB RAM):
- **Publish Throughput**: 50K+ messages/second/broker
- **Subscribe Throughput**: 100K+ messages/second/broker
- **End-to-End Latency**: P95 < 100ms
- **Storage Efficiency**: < 20% overhead

### Load Testing

```bash
make load-test
```

Stress tests with:
- 1M+ messages
- 100+ concurrent producers
- 50+ concurrent consumers
- Multiple topic scenarios

## Contributing

### Test Guidelines

1. **Test Isolation**: Each test should be independent
2. **Resource Cleanup**: Always clean up resources in test teardown
3. **Timeouts**: Set appropriate timeouts for operations
4. **Error Handling**: Test both success and failure scenarios
5. **Documentation**: Document test purpose and expected behavior

### Code Style

- Follow Go testing conventions
- Use testify for assertions
- Include setup/teardown in test functions
- Use descriptive test names

## Future Enhancements

- [ ] Chaos engineering tests (network partitions, node failures)
- [ ] Multi-datacenter deployment testing
- [ ] Schema evolution compatibility tests
- [ ] Security and authentication tests
- [ ] Performance regression detection
- [ ] Automated load pattern generation

## Support

For issues and questions:
- Check existing GitHub issues
- Review SeaweedMQ documentation
- Join SeaweedFS community discussions

---

*This integration test suite ensures SeaweedMQ's reliability, performance, and functionality across all critical use cases and failure scenarios.* 