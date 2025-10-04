# Quick Test Results - Schema Registry Integration

## Test Execution Summary

**Test**: `make quick-test`  
**Date**: September 30, 2025  
**Duration**: Services started successfully, schema registration completed, verification failed

## Results

### Schema Registration: SUCCESS ✓
- **10/10 schemas registered successfully**
- Schema IDs: 1-10
- All subjects created without errors:
  - loadtest-topic-0-value (ID: 1)
  - loadtest-topic-0-key (ID: 2)
  - loadtest-topic-1-value (ID: 3)
  - loadtest-topic-1-key (ID: 4)
  - loadtest-topic-2-value (ID: 5)
  - loadtest-topic-2-key (ID: 6)
  - loadtest-topic-3-value (ID: 7)
  - loadtest-topic-3-key (ID: 8)
  - loadtest-topic-4-value (ID: 9)
  - loadtest-topic-4-key (ID: 10)

### Schema Verification: FAILED ✗
- **0/10 schemas verified**
- All schema lookups returned 404 Not Found
- Schemas were registered but not retrievable

## Root Cause Analysis

### Schema Registry Consumer Offset Issue

From Schema Registry logs, we can see the consumer offset is resetting:

```
[2025-09-30 05:52:10,303] Reached offset at 0
[2025-09-30 05:52:10,349] Wait to catch up until the offset at 2
[2025-09-30 05:52:10,349] Reached offset at 2
[2025-09-30 05:52:10,393] Wait to catch up until the offset at 0  <- RESETS!
[2025-09-30 05:52:10,393] Reached offset at 0
[2025-09-30 05:52:10,435] Wait to catch up until the offset at 3
[2025-09-30 05:52:10,435] Reached offset at 3
[2025-09-30 05:52:10,475] Wait to catch up until the offset at 0  <- RESETS AGAIN!
```

**Pattern**: The consumer offset keeps resetting from a higher value (2, 3, 4, 5) back to 0.

### Why This Happens

1. **Schema Registry's Internal Consumer**: Schema Registry uses a Kafka consumer to read the `_schemas` topic and populate its internal cache (lookupCache).

2. **Consumer Offset Not Persisting**: The consumer offsets are being committed, but when the consumer seeks/fetches, it's not retrieving the correct committed offset.

3. **Cache Not Populating**: Because the consumer keeps resetting to offset 0, it never properly processes messages at offsets 1-10 to populate the lookupCache.

4. **404 on Retrieval**: When verification tries to retrieve schemas, they're not in the cache (because the consumer never processed them), resulting in 404 errors.

## Implementation Status

### What's Working ✓
1. **Consumer Offset Storage Layer**
   - Memory storage: Fully functional
   - Filer storage: Implementation complete
   - All unit tests passing

2. **Protocol Handler Integration**
   - `consumerOffsetStorage` added to Handler
   - OffsetCommit handler updated to use new storage
   - OffsetFetch handler updated to use new storage

3. **Schema Registration**
   - Messages successfully written to `_schemas` topic
   - Schema IDs assigned correctly (1-10)
   - HTTP registration API working

### What's Not Working ✗
1. **Consumer Offset Retrieval in Fetch Protocol**
   - The consumer is not maintaining its position correctly
   - Offsets reset instead of advancing sequentially

2. **Schema Registry Cache Population**
   - Internal consumer not processing messages
   - lookupCache remains empty
   - Schemas not retrievable via GET API

## Next Steps Required

### Option 1: Debug Consumer Offset Fetch/Commit Flow
- Add detailed logging to OffsetCommit/OffsetFetch handlers
- Trace Schema Registry's consumer group: "schema-registry"
- Verify offset storage and retrieval for `_schemas` topic

### Option 2: Investigate Fetch Protocol
- Check if Fetch requests are using committed offsets correctly
- Verify high water mark calculation
- Ensure messages are returned from correct offset

### Option 3: Check Consumer Group Coordination
- Verify JoinGroup/SyncGroup for "schema-registry" group
- Check if consumer group state is persisting
- Validate generation IDs and member assignments

## Files to Investigate

1. `weed/mq/kafka/protocol/offset_management.go`
   - Line 148-157: commitOffsetToSMQ implementation
   - Line 172-End: handleOffsetFetch implementation

2. `weed/mq/kafka/protocol/fetch.go`
   - Fetch handler offset logic
   - Message retrieval from storage

3. `weed/mq/kafka/protocol/handler.go`
   - Line 2883-2926: commitOffsetToSMQ and fetchOffsetFromSMQ

## Conclusion

The consumer offset management implementation is **functionally complete** at the storage layer, but there's a **protocol-level integration issue** preventing Schema Registry's consumer from maintaining its offset correctly.

**The core problem**: Committed offsets are being stored, but when the consumer fetches data, it's not seeking to the correct committed offset, causing it to reset to 0 repeatedly.

**Impact**: Schema Registry cannot retrieve registered schemas because its internal cache never gets populated.

**Recommendation**: Add debug logging to trace the exact consumer offset flow for the "schema-registry" consumer group reading from "_schemas" topic.
