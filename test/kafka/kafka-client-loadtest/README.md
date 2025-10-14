# Kafka Client Load Test for SeaweedFS

This comprehensive load testing suite validates the SeaweedFS MQ stack using real Kafka client libraries. Unlike the existing SMQ tests, this uses actual Kafka clients (`sarama` and `confluent-kafka-go`) to test the complete integration through:

- **Kafka Clients** → **SeaweedFS Kafka Gateway** → **SeaweedFS MQ Broker** → **SeaweedFS Storage**

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Kafka Client  │    │  Kafka Gateway   │    │   SeaweedFS MQ      │
│   Load Test     │───▶│  (Port 9093)     │───▶│   Broker            │
│   - Producers   │    │                  │    │                     │
│   - Consumers   │    │  Protocol        │    │   Topic Management  │
│                 │    │  Translation     │    │   Message Storage   │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                                                             │
                                                             ▼
                                                ┌─────────────────────┐
                                                │  SeaweedFS Storage  │
                                                │  - Master           │
                                                │  - Volume Server    │
                                                │  - Filer            │
                                                └─────────────────────┘
```

## Features

### 🚀 **Multiple Test Modes**
- **Producer-only**: Pure message production testing
- **Consumer-only**: Consumption from existing topics  
- **Comprehensive**: Full producer + consumer load testing

### 📊 **Rich Metrics & Monitoring**
- Prometheus metrics collection
- Grafana dashboards
- Real-time throughput and latency tracking
- Consumer lag monitoring
- Error rate analysis

### 🔧 **Configurable Test Scenarios**
- **Quick Test**: 1-minute smoke test
- **Standard Test**: 5-minute medium load
- **Stress Test**: 10-minute high load  
- **Endurance Test**: 30-minute sustained load
- **Custom**: Fully configurable parameters

### 📈 **Message Types**
- **JSON**: Structured test messages
- **Avro**: Schema Registry integration
- **Binary**: Raw binary payloads

### 🛠 **Kafka Client Support**
- **Sarama**: Native Go Kafka client
- **Confluent**: Official Confluent Go client
- Schema Registry integration
- Consumer group management

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Make (optional, but recommended)

### 1. Run Default Test
```bash
make test
```
This runs a 5-minute comprehensive test with 10 producers and 5 consumers.

### 2. Quick Smoke Test
```bash
make quick-test
```
1-minute test with minimal load for validation.

### 3. Stress Test
```bash
make stress-test  
```
10-minute high-throughput test with 20 producers and 10 consumers.

### 4. Test with Monitoring
```bash
make test-with-monitoring
```
Includes Prometheus + Grafana dashboards for real-time monitoring.

## Detailed Usage

### Manual Control
```bash
# Start infrastructure only
make start

# Run load test against running infrastructure
make test TEST_MODE=comprehensive TEST_DURATION=10m

# Stop everything
make stop

# Clean up all resources
make clean
```

### Using Scripts Directly
```bash
# Full control with the main script
./scripts/run-loadtest.sh start -m comprehensive -d 10m --monitoring

# Check service health
./scripts/wait-for-services.sh check

# Setup monitoring configurations
./scripts/setup-monitoring.sh
```

### Environment Variables
```bash
export TEST_MODE=comprehensive        # producer, consumer, comprehensive  
export TEST_DURATION=300s            # Test duration
export PRODUCER_COUNT=10              # Number of producer instances
export CONSUMER_COUNT=5               # Number of consumer instances  
export MESSAGE_RATE=1000              # Messages/second per producer
export MESSAGE_SIZE=1024              # Message size in bytes
export TOPIC_COUNT=5                  # Number of topics to create
export PARTITIONS_PER_TOPIC=3         # Partitions per topic

make test
```

## Configuration

### Main Configuration File
Edit `config/loadtest.yaml` to customize:

- **Kafka Settings**: Bootstrap servers, security, timeouts
- **Producer Config**: Batching, compression, acknowledgments  
- **Consumer Config**: Group settings, fetch parameters
- **Message Settings**: Size, format (JSON/Avro/Binary)
- **Schema Registry**: Avro/Protobuf schema validation
- **Metrics**: Prometheus collection intervals
- **Test Scenarios**: Predefined load patterns

### Example Custom Configuration
```yaml
test_mode: "comprehensive"
duration: "600s"  # 10 minutes

producers:
  count: 15
  message_rate: 2000
  message_size: 2048
  compression_type: "snappy"
  acks: "all"

consumers:
  count: 8
  group_prefix: "high-load-group"
  max_poll_records: 1000

topics:
  count: 10
  partitions: 6
  replication_factor: 1
```

## Test Scenarios

### 1. Producer Performance Test
```bash
make producer-test TEST_DURATION=10m PRODUCER_COUNT=20 MESSAGE_RATE=3000
```
Tests maximum message production throughput.

### 2. Consumer Performance Test  
```bash
# First produce messages
make producer-test TEST_DURATION=5m

# Then test consumption
make consumer-test TEST_DURATION=10m CONSUMER_COUNT=15
```

### 3. Schema Registry Integration
```bash
# Enable schemas in config/loadtest.yaml
schemas:
  enabled: true
  
make test
```
Tests Avro message serialization through Schema Registry.

### 4. High Availability Test
```bash
# Test with container restarts during load
make test TEST_DURATION=20m &
sleep 300
docker restart kafka-gateway
```

## Monitoring & Metrics

### Real-Time Dashboards
When monitoring is enabled:
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

### Key Metrics Tracked
- **Throughput**: Messages/second, MB/second
- **Latency**: End-to-end message latency percentiles  
- **Errors**: Producer/consumer error rates
- **Consumer Lag**: Per-partition lag monitoring
- **Resource Usage**: CPU, memory, disk I/O

### Grafana Dashboards
- **Kafka Load Test**: Comprehensive test metrics
- **SeaweedFS Cluster**: Storage system health
- **Custom Dashboards**: Extensible monitoring

## Advanced Features

### Schema Registry Testing
```bash
# Test Avro message serialization
export KAFKA_VALUE_TYPE=avro
make test
```

The load test includes:
- Schema registration
- Avro message encoding/decoding  
- Schema evolution testing
- Compatibility validation

### Multi-Client Testing
The test supports both Sarama and Confluent clients:
```go
// Configure in producer/consumer code
useConfluent := true  // Switch client implementation
```

### Consumer Group Rebalancing
- Automatic consumer group management
- Partition rebalancing simulation
- Consumer failure recovery testing

### Chaos Testing
```yaml
chaos:
  enabled: true
  producer_failure_rate: 0.01
  consumer_failure_rate: 0.01
  network_partition_probability: 0.001
```

## Troubleshooting

### Common Issues

#### Services Not Starting
```bash
# Check service health
make health-check

# View detailed logs
make logs

# Debug mode
make debug
```

#### Low Throughput
- Increase `MESSAGE_RATE` and `PRODUCER_COUNT`
- Adjust `batch_size` and `linger_ms` in config
- Check consumer `max_poll_records` setting

#### High Latency
- Reduce `linger_ms` for lower latency
- Adjust `acks` setting (0, 1, or "all")
- Monitor consumer lag

#### Memory Issues  
```bash
# Reduce concurrent clients
make test PRODUCER_COUNT=5 CONSUMER_COUNT=3

# Adjust message size  
make test MESSAGE_SIZE=512
```

### Debug Commands
```bash
# Execute shell in containers
make exec-master
make exec-filer  
make exec-gateway

# Attach to load test
make attach-loadtest

# View real-time stats
curl http://localhost:8080/stats
```

## Development

### Building from Source
```bash
# Set up development environment
make dev-env

# Build load test binary
make build

# Run tests locally (requires Go 1.21+)
cd cmd/loadtest && go run main.go -config ../../config/loadtest.yaml
```

### Extending the Tests
1. **Add new message formats** in `internal/producer/`
2. **Add custom metrics** in `internal/metrics/`  
3. **Create new test scenarios** in `config/loadtest.yaml`
4. **Add monitoring panels** in `monitoring/grafana/dashboards/`

### Contributing
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `make test`
5. Submit a pull request

## Performance Benchmarks

### Expected Performance (on typical hardware)

| Scenario | Producers | Consumers | Rate (msg/s) | Latency (p95) |
|----------|-----------|-----------|--------------|---------------|
| Quick    | 2         | 2         | 200          | <10ms         |
| Standard | 5         | 3         | 2,500        | <20ms         |
| Stress   | 20        | 10        | 40,000       | <50ms         |
| Endurance| 10        | 5         | 10,000       | <30ms         |

*Results vary based on hardware, network, and SeaweedFS configuration*

### Tuning for Maximum Performance
```yaml
producers:
  batch_size: 1000
  linger_ms: 10
  compression_type: "lz4"
  acks: "1"  # Balance between speed and durability

consumers:  
  max_poll_records: 5000
  fetch_min_bytes: 1048576  # 1MB
  fetch_max_wait_ms: 100
```

## Comparison with Existing Tests

| Feature | SMQ Tests | **Kafka Client Load Test** |
|---------|-----------|----------------------------|
| Protocol | SMQ (SeaweedFS native) | **Kafka (industry standard)** |
| Clients | SMQ clients | **Real Kafka clients (Sarama, Confluent)** |
| Schema Registry | ❌ | **✅ Full Avro/Protobuf support** |
| Consumer Groups | Basic | **✅ Full Kafka consumer group features** |
| Monitoring | Basic | **✅ Prometheus + Grafana dashboards** |
| Test Scenarios | Limited | **✅ Multiple predefined scenarios** |
| Real-world | Synthetic | **✅ Production-like workloads** |

This load test provides comprehensive validation of the SeaweedFS Kafka Gateway using real-world Kafka clients and protocols.

---

## Quick Reference

```bash
# Essential Commands
make help                    # Show all available commands
make test                    # Run default comprehensive test  
make quick-test              # 1-minute smoke test
make stress-test             # High-load stress test
make test-with-monitoring    # Include Grafana dashboards
make clean                   # Clean up all resources

# Monitoring
make monitor                 # Start Prometheus + Grafana
# → http://localhost:9090 (Prometheus)
# → http://localhost:3000 (Grafana, admin/admin)

# Advanced
make benchmark               # Run full benchmark suite
make health-check            # Validate service health
make validate-setup          # Check configuration
```
