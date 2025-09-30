# Development Plan: Fix Schema Registry Consumer Offset Management

## Problem Statement

Schema Registry successfully registers schemas (10/10 schemas registered with IDs 12-21) but cannot retrieve them (0/10 verified, all return 404). Root cause analysis indicates the consumer offset management in Kafka Gateway is not working correctly, preventing Schema Registry's background consumer from processing messages and populating its internal cache.

## Development Goals

1. Fix consumer offset persistence (OffsetCommit protocol)
2. Fix consumer offset retrieval (OffsetFetch protocol)
3. Ensure fetch responses return messages from correct offset
4. Add comprehensive integration and unit tests
5. Verify Schema Registry works end-to-end

## Phase 1: Test Infrastructure Setup

### Step 1.1: Create Integration Test Framework
**File**: `test/kafka/kafka-client-loadtest/integration/consumer_offset_test.go`

**Objectives**:
- Test consumer offset commit and fetch operations
- Test consumer group state persistence
- Test fetch from specific offset

**Tests**:
- `TestConsumerOffsetCommitAndFetch` - Verify offset commit and retrieval
- `TestConsumerGroupPersistence` - Verify group state survives restarts
- `TestFetchFromOffset` - Verify fetch returns messages from correct offset
- `TestOffsetReset` - Verify AUTO_OFFSET_RESET_CONFIG behavior

**Commit**: "test: add integration tests for consumer offset management"

### Step 1.2: Create Schema Registry Integration Test
**File**: `test/kafka/kafka-client-loadtest/integration/schema_registry_test.go`

**Objectives**:
- Test Schema Registry end-to-end workflow
- Test schema registration and retrieval
- Test consumer offset progression

**Tests**:
- `TestSchemaRegistryRegisterAndRetrieve` - Full registration and retrieval flow
- `TestSchemaRegistryConsumerProgression` - Verify consumer offsets advance
- `TestSchemaRegistryMultipleSchemas` - Verify multiple schemas work
- `TestSchemaRegistryRestart` - Verify schema persistence across restarts

**Commit**: "test: add Schema Registry integration tests"

### Step 1.3: Create Unit Tests for Offset Handlers
**File**: `weed/mq/kafka/protocol/offset_commit_test.go`
**File**: `weed/mq/kafka/protocol/offset_fetch_test.go`

**Objectives**:
- Test OffsetCommit request parsing and response generation
- Test OffsetFetch request parsing and response generation
- Test error handling

**Tests**:
- `TestOffsetCommitV0` through `TestOffsetCommitV8`
- `TestOffsetFetchV0` through `TestOffsetFetchV8`
- `TestOffsetCommitErrors`
- `TestOffsetFetchErrors`
- `TestOffsetCommitPersistence`

**Commit**: "test: add unit tests for offset commit/fetch handlers"

## Phase 2: Consumer Offset Storage Implementation

### Step 2.1: Design Offset Storage Interface
**File**: `weed/mq/kafka/consumer_offset/storage.go`

**Objectives**:
- Define interface for storing consumer offsets
- Support multiple storage backends (in-memory, filer)
- Thread-safe operations

**Components**:
```go
type OffsetStorage interface {
    CommitOffset(group, topic string, partition int32, offset int64, metadata string) error
    FetchOffset(group, topic string, partition int32) (int64, string, error)
    FetchAllOffsets(group string) (map[TopicPartition]OffsetMetadata, error)
    DeleteGroup(group string) error
}

type TopicPartition struct {
    Topic     string
    Partition int32
}

type OffsetMetadata struct {
    Offset   int64
    Metadata string
}
```

**Commit**: "feat(kafka): add consumer offset storage interface"

### Step 2.2: Implement In-Memory Offset Storage
**File**: `weed/mq/kafka/consumer_offset/memory_storage.go`
**File**: `weed/mq/kafka/consumer_offset/memory_storage_test.go`

**Objectives**:
- Implement in-memory storage with sync.RWMutex
- Fast for testing and single-node deployments
- Full test coverage

**Tests**:
- `TestMemoryStorageCommitAndFetch`
- `TestMemoryStorageConcurrency`
- `TestMemoryStorageDeleteGroup`
- `TestMemoryStorageFetchAllOffsets`

**Commit**: "feat(kafka): implement in-memory consumer offset storage"

### Step 2.3: Implement Filer-Based Offset Storage
**File**: `weed/mq/kafka/consumer_offset/filer_storage.go`
**File**: `weed/mq/kafka/consumer_offset/filer_storage_test.go`

**Objectives**:
- Store offsets in filer for persistence
- Use path: `/kafka/consumer_offsets/{group}/{topic}/{partition}`
- Support distributed deployments

**Path Structure**:
```
/kafka/consumer_offsets/
  {group}/
    {topic}/
      {partition}/
        offset     (file containing offset value)
        metadata   (file containing metadata string)
```

**Tests**:
- `TestFilerStorageCommitAndFetch`
- `TestFilerStoragePersistence`
- `TestFilerStorageMultipleGroups`

**Commit**: "feat(kafka): implement filer-based consumer offset storage"

### Step 2.4: Integrate Offset Storage with Handler
**File**: `weed/mq/kafka/protocol/handler.go`

**Objectives**:
- Add OffsetStorage to Handler struct
- Initialize storage in NewHandler
- Make storage configurable (memory vs filer)

**Changes**:
```go
type Handler struct {
    // ... existing fields ...
    offsetStorage consumer_offset.OffsetStorage
}

func NewHandler(config *Config) *Handler {
    var offsetStorage consumer_offset.OffsetStorage
    if config.UseMemoryOffsetStorage {
        offsetStorage = consumer_offset.NewMemoryStorage()
    } else {
        offsetStorage = consumer_offset.NewFilerStorage(config.FilerClient)
    }
    
    return &Handler{
        // ... existing fields ...
        offsetStorage: offsetStorage,
    }
}
```

**Commit**: "feat(kafka): integrate offset storage with protocol handler"

## Phase 3: OffsetCommit Protocol Implementation

### Step 3.1: Review and Fix OffsetCommit Handler
**File**: `weed/mq/kafka/protocol/offset_commit.go`

**Objectives**:
- Ensure all OffsetCommit versions are supported (v0-v8)
- Parse request correctly
- Store offsets using OffsetStorage
- Generate correct response

**Key Changes**:
- Extract group, topic, partition, offset from request
- Call `handler.offsetStorage.CommitOffset(group, topic, partition, offset, metadata)`
- Return success response with no errors
- Handle errors (unknown group, invalid partition, etc.)

**Commit**: "fix(kafka): implement OffsetCommit protocol handler"

### Step 3.2: Add OffsetCommit Debug Logging
**File**: `weed/mq/kafka/protocol/offset_commit.go`

**Objectives**:
- Add detailed logging for debugging
- Log group, topic, partition, offset
- Log success/failure

**Logging**:
```go
glog.V(1).Infof("OffsetCommit: group=%s topic=%s partition=%d offset=%d", 
    group, topic, partition, offset)
```

**Commit**: "feat(kafka): add debug logging to OffsetCommit handler"

### Step 3.3: Test OffsetCommit with Real Consumer
**File**: `test/kafka/kafka-client-loadtest/integration/offset_commit_integration_test.go`

**Objectives**:
- Use real Kafka consumer to commit offsets
- Verify offsets are stored
- Verify offsets persist across handler restarts

**Commit**: "test: add OffsetCommit integration tests with real consumer"

## Phase 4: OffsetFetch Protocol Implementation

### Step 4.1: Review and Fix OffsetFetch Handler
**File**: `weed/mq/kafka/protocol/offset_fetch.go`

**Objectives**:
- Ensure all OffsetFetch versions are supported (v0-v8)
- Parse request correctly
- Retrieve offsets using OffsetStorage
- Generate correct response

**Key Changes**:
- Extract group, topics, partitions from request
- Call `handler.offsetStorage.FetchOffset(group, topic, partition)`
- Return offsets in response
- Handle "offset not found" by returning -1

**Special Cases**:
- Empty topic list = fetch all offsets for group
- Non-existent group = return error or empty offsets (depending on version)

**Commit**: "fix(kafka): implement OffsetFetch protocol handler"

### Step 4.2: Add OffsetFetch Debug Logging
**File**: `weed/mq/kafka/protocol/offset_fetch.go`

**Objectives**:
- Add detailed logging for debugging
- Log group, topics, partitions requested
- Log offsets returned

**Logging**:
```go
glog.V(1).Infof("OffsetFetch: group=%s topics=%v", group, topics)
glog.V(1).Infof("OffsetFetch: returning offset=%d for %s/%d", offset, topic, partition)
```

**Commit**: "feat(kafka): add debug logging to OffsetFetch handler"

### Step 4.3: Test OffsetFetch with Real Consumer
**File**: `test/kafka/kafka-client-loadtest/integration/offset_fetch_integration_test.go`

**Objectives**:
- Use real Kafka consumer to fetch offsets
- Verify fetched offsets match committed offsets
- Test fetch after restart

**Commit**: "test: add OffsetFetch integration tests with real consumer"

## Phase 5: Fetch Protocol Offset Handling

### Step 5.1: Review Fetch Protocol Offset Logic
**File**: `weed/mq/kafka/protocol/fetch.go`

**Objectives**:
- Verify fetch starts from requested offset
- Verify messages are returned in order
- Verify high water mark is correct

**Key Areas**:
- `handleFetchV0V1` - verify offset handling
- `handleFetchV2Plus` - verify offset handling
- `getMessagesForPartition` - verify offset filtering

**Commit**: "refactor(kafka): review and document Fetch offset handling"

### Step 5.2: Add Fetch Offset Validation
**File**: `weed/mq/kafka/protocol/fetch.go`

**Objectives**:
- Validate requested offset is within valid range
- Return OffsetOutOfRange error if needed
- Log fetch offset for debugging

**Validation**:
```go
if fetchOffset < earliestOffset {
    // Return OffsetOutOfRange error
}
if fetchOffset > highWaterMark {
    // Return empty response
}
```

**Commit**: "feat(kafka): add offset validation to Fetch handler"

### Step 5.3: Add Fetch Debug Logging
**File**: `weed/mq/kafka/protocol/fetch.go`

**Objectives**:
- Log fetch requests with offset
- Log number of messages returned
- Log offset range of returned messages

**Logging**:
```go
glog.V(1).Infof("Fetch: topic=%s partition=%d offset=%d hwm=%d", 
    topic, partition, fetchOffset, highWaterMark)
glog.V(1).Infof("Fetch: returning %d messages, offsets %d-%d", 
    len(messages), firstOffset, lastOffset)
```

**Commit**: "feat(kafka): add debug logging to Fetch handler"

## Phase 6: Consumer Group Management

### Step 6.1: Review Consumer Group State Storage
**File**: `weed/mq/kafka/consumer_group/state_storage.go`

**Objectives**:
- Store consumer group metadata (members, assignments)
- Persist across restarts
- Thread-safe operations

**Components**:
```go
type ConsumerGroupStorage interface {
    SaveGroup(group *ConsumerGroup) error
    GetGroup(groupId string) (*ConsumerGroup, error)
    DeleteGroup(groupId string) error
    ListGroups() ([]*ConsumerGroup, error)
}

type ConsumerGroup struct {
    GroupId         string
    ProtocolType    string
    Protocol        string
    Members         []*Member
    Generation      int32
    Leader          string
    State           string
}
```

**Commit**: "feat(kafka): add consumer group state storage interface"

### Step 6.2: Implement Consumer Group Storage
**File**: `weed/mq/kafka/consumer_group/filer_state_storage.go`
**File**: `weed/mq/kafka/consumer_group/filer_state_storage_test.go`

**Objectives**:
- Store group metadata in filer
- Use path: `/kafka/consumer_groups/{group}/metadata`
- Full test coverage

**Commit**: "feat(kafka): implement filer-based consumer group storage"

### Step 6.3: Integrate with JoinGroup/SyncGroup
**File**: `weed/mq/kafka/protocol/join_group.go`
**File**: `weed/mq/kafka/protocol/sync_group.go`

**Objectives**:
- Save group state on JoinGroup
- Save member assignments on SyncGroup
- Restore state on coordinator restart

**Commit**: "feat(kafka): persist consumer group state in JoinGroup/SyncGroup"

## Phase 7: Integration and End-to-End Testing

### Step 7.1: Create Schema Registry E2E Test
**File**: `test/kafka/kafka-client-loadtest/e2e/schema_registry_e2e_test.go`

**Objectives**:
- Test complete Schema Registry workflow
- Register 10 schemas
- Verify all 10 schemas can be retrieved
- Restart Schema Registry and verify schemas still available

**Test Flow**:
1. Start Kafka Gateway
2. Start Schema Registry
3. Register 10 schemas via REST API
4. Verify all 10 schemas via REST API
5. Restart Schema Registry
6. Verify all 10 schemas still available

**Commit**: "test: add Schema Registry end-to-end test"

### Step 7.2: Update Java Reproducer as Test
**File**: `test/kafka/kafka-client-loadtest/debug-client/SchemaRegistryReproducer.java`

**Objectives**:
- Use as automated test
- Run in CI pipeline
- Return exit code 0 if all schemas verified

**Changes**:
- Exit with code 1 if verification fails
- Exit with code 0 if all schemas verified
- Add to test suite

**Commit**: "test: convert Java reproducer to automated test"

### Step 7.3: Add quick-test Validation
**File**: `test/kafka/kafka-client-loadtest/Makefile`

**Objectives**:
- Enhance quick-test to validate schema retrieval
- Fail if schemas cannot be retrieved
- Print detailed error messages

**Changes**:
```makefile
quick-test:
    @echo "Running schema registration..."
    ./scripts/register-schemas.sh full
    @echo "Verifying schemas..."
    ./scripts/verify-schemas.sh || (echo "FAILED: Schemas not retrievable" && exit 1)
    @echo "SUCCESS: All schemas registered and verified"
```

**Commit**: "test: add schema verification to quick-test"

## Phase 8: Documentation and Cleanup

### Step 8.1: Update Architecture Documentation
**File**: `docs/kafka_gateway_architecture.md`

**Objectives**:
- Document consumer offset storage design
- Document consumer group state management
- Add diagrams for offset flow

**Commit**: "docs: update Kafka Gateway architecture documentation"

### Step 8.2: Add Troubleshooting Guide
**File**: `docs/kafka_gateway_troubleshooting.md`

**Objectives**:
- Document common issues
- Add debugging steps
- Reference log messages

**Sections**:
- Consumer offset issues
- Schema Registry integration
- Debugging with glog levels

**Commit**: "docs: add Kafka Gateway troubleshooting guide"

### Step 8.3: Clean Up Debug Logging
**Files**: Multiple protocol handler files

**Objectives**:
- Review all debug logging added
- Ensure proper glog levels (V(0), V(1), V(2))
- Remove overly verbose logging

**Commit**: "refactor: clean up debug logging in protocol handlers"

### Step 8.4: Remove Temporary Test Files
**Objectives**:
- Remove one-off test scripts
- Keep only integration/unit tests
- Clean up debug-client directory

**Commit**: "chore: remove temporary test files"

## Phase 9: Performance and Optimization

### Step 9.1: Add Offset Storage Metrics
**File**: `weed/mq/kafka/consumer_offset/metrics.go`

**Objectives**:
- Track offset commit latency
- Track offset fetch latency
- Track storage size

**Commit**: "feat(kafka): add metrics for offset storage operations"

### Step 9.2: Optimize Offset Storage Access
**File**: `weed/mq/kafka/consumer_offset/cached_storage.go`

**Objectives**:
- Add LRU cache for frequently accessed offsets
- Reduce filer read operations
- Maintain consistency

**Commit**: "perf(kafka): add caching layer for offset storage"

### Step 9.3: Add Benchmark Tests
**File**: `weed/mq/kafka/consumer_offset/storage_bench_test.go`

**Objectives**:
- Benchmark offset commit operations
- Benchmark offset fetch operations
- Compare memory vs filer storage

**Commit**: "test: add benchmark tests for offset storage"

## Phase 10: Final Validation

### Step 10.1: Run Full Test Suite
**Objectives**:
- Run all unit tests
- Run all integration tests
- Run all E2E tests
- Verify all pass

**Commands**:
```bash
go test ./weed/mq/kafka/... -v
go test ./test/kafka/kafka-client-loadtest/integration/... -v
make quick-test
make standard-test
```

**Commit**: "test: validate all tests pass"

### Step 10.2: Performance Testing
**File**: `test/kafka/kafka-client-loadtest/performance/schema_registry_perf_test.go`

**Objectives**:
- Test with 1000 schemas
- Measure registration throughput
- Measure retrieval latency

**Commit**: "test: add Schema Registry performance tests"

### Step 10.3: Final Integration Test
**Objectives**:
- Reset storage completely
- Run quick-test
- Run standard-test
- Run Java reproducer
- Verify 100% success rate

**Commit**: "test: final validation of Schema Registry integration"

## Success Criteria

1. All unit tests pass
2. All integration tests pass
3. quick-test shows 10/10 schemas registered AND 10/10 schemas verified
4. standard-test shows 10/10 schemas registered AND 10/10 schemas verified
5. Java reproducer shows 10/10 schemas registered AND 10/10 schemas verified
6. Schema Registry logs show consumer progressing through offsets (0, 1, 2, 3, ... 11)
7. No consumer offset resets
8. Schemas persist across Schema Registry restarts

## Timeline Estimate

- Phase 1 (Test Infrastructure): 2 days
- Phase 2 (Offset Storage): 3 days
- Phase 3 (OffsetCommit): 2 days
- Phase 4 (OffsetFetch): 2 days
- Phase 5 (Fetch Protocol): 1 day
- Phase 6 (Consumer Groups): 2 days
- Phase 7 (Integration Testing): 2 days
- Phase 8 (Documentation): 1 day
- Phase 9 (Optimization): 2 days
- Phase 10 (Validation): 1 day

**Total: 18 days**

## Risk Mitigation

1. **Risk**: Tests fail on existing features
   - **Mitigation**: Run full test suite before starting, establish baseline

2. **Risk**: Offset storage design doesn't scale
   - **Mitigation**: Design interface first, can swap implementations

3. **Risk**: Breaking changes to existing Kafka Gateway features
   - **Mitigation**: Comprehensive integration tests, backward compatibility

4. **Risk**: Schema Registry still doesn't work after fixes
   - **Mitigation**: Test each phase incrementally, add detailed logging

## Notes

- Each commit should be atomic and deployable
- All tests must pass before commit
- Code review required for each phase
- Performance testing required before production deployment
