# Schema Integration Test Results Summary

## Test Plan Overview

We created a systematic test plan to isolate and debug the schema integration issues:

1. **Test 1**: Schema creation in topic.conf based on schema ID
2. **Test 2**: Producing schematized messages to Kafka Gateway  
3. **Test 3**: Consuming schematized messages from Kafka Gateway
4. **Test 4**: Full integration test

## Root Cause Analysis

### Primary Issue: Schema Registry Offset Timeout

**Problem**: Schema Registry consistently fails with:
```
StoreTimeoutException: KafkaStoreReaderThread failed to reach target offset within the timeout interval. 
targetOffset: 5, offsetReached: 4, timeout(ms): 500
```

**Root Cause**: The fetch path in SeaweedMQ is not properly handling offset-based reads for the `_schemas` topic.

### Debug Evidence

1. **Kafka Gateway Logs**:
   ```
   Fetch v7 - Topic: _schemas, partition: 0, fetchOffset: 5 (effective: 5), highWaterMark: 5, maxBytes: 1048576
   No messages available - effective fetchOffset 5 >= highWaterMark 5
   ```

2. **Schema Registry Behavior**:
   - Successfully registers first few schemas (IDs 1-4)
   - Fails when trying to register subsequent schemas
   - Times out waiting for offset 5 when high watermark is 5

3. **SeaweedFS Filer Inspection**:
   - `_schemas` topic exists with `checkpoint.offset` file
   - No actual log files containing message data
   - Suggests messages are tracked in ledger but not persisted to disk

## Technical Analysis

### Issue 1: Offset Boundary Condition
In Kafka, if high watermark is N, valid offsets are 0 to N-1. Schema Registry requesting offset N when high watermark is N should return empty, but it's timing out instead.

### Issue 2: Message Persistence Gap
- Offset ledger shows messages exist (high watermark advances)
- No corresponding log files in filer storage
- Indicates disconnect between ledger tracking and actual message storage

### Issue 3: Consumer Wire Format Handling
- Producer correctly creates Confluent Wire Format (magic byte + schema ID + Avro data)
- Consumer was not handling wire format unwrapping (fixed in our tests)
- Load test consumers were receiving empty messages (Length: 0)

## Fixes Applied

### 1. Debug Messages Added
Added comprehensive debug logging to:
- `weed/mq/kafka/integration/seaweedmq_handler.go` - GetStoredRecords function
- `weed/util/log_buffer/log_read.go` - Offset-based filtering
- Kafka Gateway fetch responses

### 2. Consumer Wire Format Fix
Updated `internal/consumer/consumer.go` to properly handle Confluent Wire Format:
```go
// Handle Confluent Wire Format when schemas are enabled
var avroData []byte
if c.config.Schemas.Enabled {
    if len(value) < 5 {
        return nil, fmt.Errorf("message too short for Confluent Wire Format: %d bytes", len(value))
    }
    // Check magic byte (should be 0)
    if value[0] != 0 {
        return nil, fmt.Errorf("invalid Confluent Wire Format magic byte: %d", value[0])
    }
    // Extract schema ID and Avro data
    schemaID := binary.BigEndian.Uint32(value[1:5])
    avroData = value[5:]
} else {
    avroData = value
}
```

### 3. Offset Filtering Fix
Modified `weed/util/log_buffer/log_read.go` to properly handle offset-based positioning:
```go
if startPosition.IsOffsetBased() {
    startOffset := startPosition.GetOffset()
    if logEntry.Offset < startOffset {
        // Skip entries before the starting offset
        pos += 4 + int(size)
        batchSize++
        // advance the start position so subsequent entries use timestamp-based flow
        startPosition = NewMessagePosition(logEntry.TsNs, batchIndex)
        continue
    }
}
```

## Current Status

### Working Components
- ✅ Schema Registry API (can register schemas when not timing out)
- ✅ Kafka Gateway protocol handling
- ✅ Producer Confluent Wire Format creation
- ✅ Consumer Confluent Wire Format parsing (after fix)

### Failing Components
- ❌ Schema Registry offset consumption (times out)
- ❌ Message persistence to disk (ledger vs storage mismatch)
- ❌ System topic (`_schemas`) fetch handling
- ❌ Full schematized message pipeline

## Next Steps

### Immediate Priority
1. **Fix the offset fetch issue**: The core problem is that Schema Registry can't read its own messages from the `_schemas` topic
2. **Investigate message persistence**: Understand why messages are tracked in ledger but not written to log files
3. **Test system topic handling**: Verify that system topics (starting with `_`) have proper fetch behavior

### Recommended Investigation
1. **Check SeaweedMQ broker logs** for message persistence errors
2. **Verify topic creation flow** when schema management is enabled
3. **Test with simpler offset patterns** to isolate the boundary condition issue
4. **Compare working vs failing offset sequences** in debug logs

### Testing Strategy
1. Run individual component tests in isolation
2. Use the debug messages to trace exact failure points
3. Verify each layer before moving to integration tests
4. Focus on the `_schemas` topic behavior specifically

## Test Files Created

- `test_1_schema_creation.go` - Schema registration and topic.conf verification
- `test_2_produce_schematized.go` - Schematized message production
- `test_3_consume_schematized.go` - Schematized message consumption
- Individual component tests for debugging

The systematic approach revealed that the issue is not in the application layer (producer/consumer) but in the core SeaweedMQ fetch implementation for system topics.

