# SeaweedMQ Integration Test Design

## Overview

This document outlines the comprehensive integration test strategy for SeaweedMQ, covering all critical functionalities from basic pub/sub operations to advanced features like auto-scaling, failover, and performance testing.

## Architecture Under Test

SeaweedMQ consists of:
- **Masters**: Cluster coordination and metadata management
- **Volume Servers**: Storage layer for persistent messages
- **Filers**: File system interface for metadata storage
- **Brokers**: Message processing and routing (stateless)
- **Agents**: Client interface for pub/sub operations
- **Schema System**: Protobuf-based message schema management

## Test Categories

### 1. Basic Functionality Tests

#### 1.1 Basic Pub/Sub Operations
- **Test**: `TestBasicPublishSubscribe`
  - Publish messages to a topic
  - Subscribe and receive messages
  - Verify message content and ordering
  - Test with different data types (string, int, bytes, records)

- **Test**: `TestMultipleConsumers`
  - Multiple subscribers on same topic
  - Verify message distribution
  - Test consumer group functionality

- **Test**: `TestMessageOrdering`
  - Publish messages in sequence
  - Verify FIFO ordering within partitions
  - Test with different partition keys

#### 1.2 Schema Management
- **Test**: `TestSchemaValidation`
  - Publish with valid schemas
  - Reject invalid schema messages
  - Test schema evolution scenarios

- **Test**: `TestRecordTypes`
  - Nested record structures
  - List types and complex schemas
  - Schema-to-Parquet conversion

### 2. Partitioning and Scaling Tests

#### 2.1 Partition Management
- **Test**: `TestPartitionDistribution`
  - Messages distributed across partitions based on keys
  - Verify partition assignment logic
  - Test partition rebalancing

- **Test**: `TestAutoSplitMerge`
  - Simulate high load to trigger auto-split
  - Simulate low load to trigger auto-merge
  - Verify data consistency during splits/merges

#### 2.2 Broker Scaling
- **Test**: `TestBrokerAddRemove`
  - Add brokers during operation
  - Remove brokers gracefully
  - Verify partition reassignment

- **Test**: `TestLoadBalancing`
  - Verify even load distribution across brokers
  - Test with varying message sizes and rates
  - Monitor broker resource utilization

### 3. Failover and Reliability Tests

#### 3.1 Broker Failover
- **Test**: `TestBrokerFailover`
  - Kill leader broker during publishing
  - Verify seamless failover to follower
  - Test data consistency after failover

- **Test**: `TestBrokerRecovery`
  - Broker restart scenarios
  - State recovery from storage
  - Partition reassignment after recovery

#### 3.2 Data Durability
- **Test**: `TestMessagePersistence`
  - Publish messages and restart cluster
  - Verify all messages are recovered
  - Test with different replication settings

- **Test**: `TestFollowerReplication`
  - Leader-follower message replication
  - Verify consistency between replicas
  - Test follower promotion scenarios

### 4. Agent Functionality Tests

#### 4.1 Session Management
- **Test**: `TestPublishSessions`
  - Create/close publish sessions
  - Concurrent session management
  - Session cleanup after failures

- **Test**: `TestSubscribeSessions`
  - Subscribe session lifecycle
  - Consumer group management
  - Offset tracking and acknowledgments

#### 4.2 Error Handling
- **Test**: `TestConnectionFailures`
  - Network partitions between agent and broker
  - Automatic reconnection logic
  - Message buffering during outages

### 5. Performance and Load Tests

#### 5.1 Throughput Tests
- **Test**: `TestHighThroughputPublish`
  - Publish 100K+ messages/second
  - Monitor system resources
  - Verify no message loss

- **Test**: `TestHighThroughputSubscribe`
  - Multiple consumers processing high volume
  - Monitor processing latency
  - Test backpressure handling

#### 5.2 Spike Traffic Tests
- **Test**: `TestTrafficSpikes`
  - Sudden increase in message volume
  - Auto-scaling behavior verification
  - Resource utilization patterns

- **Test**: `TestLargeMessages`
  - Messages with large payloads (MB size)
  - Memory usage monitoring
  - Storage efficiency testing

### 6. End-to-End Scenarios

#### 6.1 Complete Workflow Tests
- **Test**: `TestProducerConsumerWorkflow`
  - Multi-stage data processing pipeline
  - Producer → Topic → Multiple Consumers
  - Data transformation and aggregation

- **Test**: `TestMultiTopicOperations`
  - Multiple topics with different schemas
  - Cross-topic message routing
  - Topic management operations

## Test Infrastructure

### Environment Setup

#### Docker Compose Configuration
```yaml
# test-environment.yml
version: '3.9'
services:
  master-cluster:
    # 3 master nodes for HA
  volume-cluster:
    # 3 volume servers for data storage
  filer-cluster:
    # 2 filers for metadata
  broker-cluster:
    # 3 brokers for message processing
  test-runner:
    # Container to run integration tests
```

#### Test Data Management
- Pre-defined test schemas
- Sample message datasets
- Performance benchmarking data

### Test Framework Structure

```go
// Base test framework
type IntegrationTestSuite struct {
    masters     []string
    brokers     []string
    filers      []string
    testClient  *TestClient
    cleanup     []func()
}

// Test utilities
type TestClient struct {
    publishers  map[string]*pub_client.TopicPublisher
    subscribers map[string]*sub_client.TopicSubscriber
    agents      []*agent.MessageQueueAgent
}
```

### Monitoring and Metrics

#### Health Checks
- Broker connectivity status
- Master cluster health
- Storage system availability
- Network connectivity between components

#### Performance Metrics
- Message throughput (msgs/sec)
- End-to-end latency
- Resource utilization (CPU, Memory, Disk)
- Network bandwidth usage

## Test Execution Strategy

### Parallel Test Execution
- Categorize tests by resource requirements
- Run independent tests in parallel
- Serialize tests that modify cluster state

### Continuous Integration
- Automated test runs on PR submissions
- Performance regression detection
- Multi-platform testing (Linux, macOS, Windows)

### Test Environment Management
- Docker-based isolated environments
- Automatic cleanup after test completion
- Resource monitoring and alerts

## Success Criteria

### Functional Requirements
- ✅ All messages published are received by subscribers
- ✅ Message ordering preserved within partitions
- ✅ Schema validation works correctly
- ✅ Auto-scaling triggers at expected thresholds
- ✅ Failover completes within 30 seconds
- ✅ No data loss during normal operations

### Performance Requirements
- ✅ Throughput: 50K+ messages/second/broker
- ✅ Latency: P95 < 100ms end-to-end
- ✅ Memory usage: < 1GB per broker under normal load
- ✅ Storage efficiency: < 20% overhead vs raw message size

### Reliability Requirements
- ✅ 99.9% uptime during normal operations
- ✅ Automatic recovery from single component failures
- ✅ Data consistency maintained across all scenarios
- ✅ Graceful degradation under resource constraints

## Implementation Timeline

### Phase 1: Core Functionality (Week 1-2)
- Basic pub/sub tests
- Schema validation tests
- Simple failover scenarios

### Phase 2: Advanced Features (Week 3-4)
- Auto-scaling tests
- Complex failover scenarios
- Agent functionality tests

### Phase 3: Performance & Load (Week 5-6)
- Throughput and latency tests
- Spike traffic handling
- Resource utilization monitoring

### Phase 4: End-to-End (Week 7-8)
- Complete workflow tests
- Multi-component integration
- Performance regression testing

## Maintenance and Updates

### Regular Updates
- Add tests for new features
- Update performance baselines
- Enhance error scenarios coverage

### Test Data Refresh
- Generate new test datasets quarterly
- Update schema examples
- Refresh performance benchmarks

This comprehensive test design ensures SeaweedMQ's reliability, performance, and functionality across all critical use cases and failure scenarios. 