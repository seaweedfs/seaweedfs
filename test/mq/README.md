# SeaweedFS Message Queue Test Suite

This directory contains test programs for SeaweedFS Message Queue (MQ) functionality, including message producers and consumers.

## Prerequisites

1. **SeaweedFS with MQ Broker and Agent**: You need a running SeaweedFS instance with MQ broker and agent enabled
2. **Go**: Go 1.19 or later required for building the test programs

## Quick Start

### 1. Start SeaweedFS with MQ Broker and Agent

```bash
# Start SeaweedFS server with MQ broker and agent
weed server -mq.broker -mq.agent -filer -volume -master.peers=none

# Or start components separately
weed master -peers=none
weed volume -master=localhost:9333
weed filer -master=localhost:9333
weed mq.broker -filer=localhost:8888
weed mq.agent -brokers=localhost:17777
```

### 2. Build Test Programs

```bash
# Build both producer and consumer
make build

# Or build individually
make build-producer
make build-consumer
```

### 3. Run Basic Test

```bash
# Run a basic producer/consumer test
make test

# Or run producer and consumer manually
make consumer &  # Start consumer in background
make producer    # Start producer
```

## Test Programs

### Producer (`producer/main.go`)

Generates structured messages and publishes them to a SeaweedMQ topic via the MQ agent.

**Usage:**
```bash
./bin/producer [options]
```

**Options:**
- `-agent`: MQ agent address (default: localhost:16777)
- `-namespace`: Topic namespace (default: test)
- `-topic`: Topic name (default: test-topic)
- `-partitions`: Number of partitions (default: 4)
- `-messages`: Number of messages to produce (default: 100)
- `-publisher`: Publisher name (default: test-producer)
- `-size`: Message size in bytes (default: 1024)
- `-interval`: Interval between messages (default: 100ms)

**Example:**
```bash
./bin/producer -agent=localhost:16777 -namespace=test -topic=my-topic -messages=1000 -interval=50ms
```

### Consumer (`consumer/main.go`)

Consumes structured messages from a SeaweedMQ topic via the MQ agent.

**Usage:**
```bash
./bin/consumer [options]
```

**Options:**
- `-agent`: MQ agent address (default: localhost:16777)
- `-namespace`: Topic namespace (default: test)
- `-topic`: Topic name (default: test-topic)
- `-group`: Consumer group name (default: test-consumer-group)
- `-instance`: Consumer group instance ID (default: test-consumer-1)
- `-max-partitions`: Maximum number of partitions to consume (default: 10)
- `-window-size`: Sliding window size for concurrent processing (default: 100)
- `-offset`: Offset type: earliest, latest, timestamp (default: latest)
- `-offset-ts`: Offset timestamp in nanoseconds (for timestamp offset type)
- `-filter`: Message filter (default: empty)
- `-show-messages`: Show consumed messages (default: true)
- `-log-progress`: Log progress every 10 messages (default: true)

**Example:**
```bash
./bin/consumer -agent=localhost:16777 -namespace=test -topic=my-topic -group=my-group -offset=earliest
```

## Makefile Commands

### Building
- `make build`: Build both producer and consumer binaries
- `make build-producer`: Build producer only
- `make build-consumer`: Build consumer only

### Running
- `make producer`: Build and run producer
- `make consumer`: Build and run consumer
- `make run-producer`: Run producer directly with go run
- `make run-consumer`: Run consumer directly with go run

### Testing
- `make test`: Run basic producer/consumer test
- `make test-performance`: Run performance test (1000 messages, 8 partitions)
- `make test-multiple-consumers`: Run test with multiple consumers

### Cleanup
- `make clean`: Remove build artifacts

### Help
- `make help`: Show detailed help

## Configuration

Configure tests using environment variables:

```bash
export AGENT_ADDR=localhost:16777
export TOPIC_NAMESPACE=test
export TOPIC_NAME=test-topic
export PARTITION_COUNT=4
export MESSAGE_COUNT=100
export CONSUMER_GROUP=test-consumer-group
export CONSUMER_INSTANCE=test-consumer-1
```

## Example Usage Scenarios

### 1. Basic Producer/Consumer Test

```bash
# Terminal 1: Start consumer
make consumer

# Terminal 2: Run producer
make producer MESSAGE_COUNT=50
```

### 2. Performance Testing

```bash
# Test with high throughput
make test-performance
```

### 3. Multiple Consumer Groups

```bash
# Terminal 1: Consumer group 1
make consumer CONSUMER_GROUP=group1

# Terminal 2: Consumer group 2  
make consumer CONSUMER_GROUP=group2

# Terminal 3: Producer
make producer MESSAGE_COUNT=200
```

### 4. Different Offset Types

```bash
# Consume from earliest
make consumer OFFSET=earliest

# Consume from latest
make consumer OFFSET=latest

# Consume from timestamp
make consumer OFFSET=timestamp OFFSET_TS=1699000000000000000
```

## Troubleshooting

### Common Issues

1. **Connection Refused**: Make sure SeaweedFS MQ agent is running on the specified address
2. **Agent Not Found**: Ensure both MQ broker and agent are running (agent requires broker)
3. **Topic Not Found**: The producer will create the topic automatically on first publish
4. **Consumer Not Receiving Messages**: Check if consumer group offset is correct (try `earliest`)
5. **Build Failures**: Ensure you're running from the SeaweedFS root directory

### Debug Mode

Enable verbose logging:
```bash
# Run with debug logging
GLOG_v=4 make producer
GLOG_v=4 make consumer
```

### Check Broker and Agent Status

```bash
# Check if broker is running
curl http://localhost:9333/cluster/brokers

# Check if agent is running (if running as server)
curl http://localhost:9333/cluster/agents

# Or use weed shell
weed shell -master=localhost:9333
> mq.broker.list
```

## Architecture

The test setup demonstrates:

1. **Agent-Based Architecture**: Uses MQ agent as intermediary between clients and brokers
2. **Structured Messages**: Messages use schema-based RecordValue format instead of raw bytes
3. **Topic Management**: Creating and configuring topics with multiple partitions
4. **Message Production**: Publishing structured messages with keys for partitioning
5. **Message Consumption**: Consuming structured messages with consumer groups and offset management
6. **Load Balancing**: Multiple consumers in same group share partition assignments
7. **Fault Tolerance**: Graceful handling of agent and broker failures and reconnections

## Files

- `producer/main.go`: Message producer implementation
- `consumer/main.go`: Message consumer implementation
- `Makefile`: Build and test automation
- `README.md`: This documentation
- `bin/`: Built binaries (created during build)

## Next Steps

1. Modify the producer to send structured data using `RecordType`
2. Implement message filtering in the consumer
3. Add metrics collection and monitoring
4. Test with multiple broker instances
5. Implement schema evolution testing 