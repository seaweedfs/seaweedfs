# Schema Registry Integration - Debugging Session Summary

## Completed Work

### 1. Consumer Offset Management (✓ COMPLETE)
- Implemented `OffsetStorage` interface with in-memory and filer-based implementations
- Integrated offset storage with Kafka Gateway Handler
- All unit tests passing (9/9 for MemoryStorage)
- Committed across 7+ commits

### 2. Root Cause Analysis (✓ COMPLETE)
**Problem**: Schema Registry schemas registered (10/10) but verification failed (0/10)

**Investigation Trail**:
1. ✓ Kafka protocol compatibility verified
2. ✓ Offset management working correctly (0→12 progression)
3. ✓ Schema Registry "Reached offset" pattern explained (normal sync behavior)
4. ✓ Identified all records returning with valueLen=0
5. ✓ **ROOT CAUSE FOUND**: Subscriber session caching

### 3. Subscriber Session Caching Bug (✓ IDENTIFIED & FIXED)
**Problem**:
- `GetOrCreateSubscriber()` cached sessions per topic-partition
- If Schema Registry requested same offset twice (e.g., offset 1)
- Returned SAME session which had already advanced past that offset
- Session returned empty/stale data
- Schema Registry never saw offsets 2-11 (the actual schemas)

**Evidence**:
```
Kafka Gateway Requests: 0, 1, 1, 1... (stuck at 1)
Broker Has Data: offset 0 (NOOP), offset 1 (NOOP), offset 2 (511 bytes), offset 3 (111 bytes)...
But: Session reused for offset 1 was already at offset 2+, returned empty
```

**Fix Implemented**:
- Created `CreateFreshSubscriber()` - creates new uncached session per fetch
- Properly closes session after read to avoid resource leaks
- Committed in: ab0e428d2

## Current Status

### 4. New Issue: Fetch Timeout (⚠ IN PROGRESS)
**Problem**: CreateFreshSubscriber causes fetch requests to hang/timeout

**Symptoms**:
- Schema Registry: "Cancelled in-flight FETCH request... request timeout: 30000ms"
- Schema Registry: "KafkaStoreReaderThread failed to reach target offset within the timeout interval"
- Kafka Gateway: No FETCH requests being processed (only connection opens/closes)
- Broker: Receiving subscribe requests, sending data successfully

**Hypothesis**:
- `CreateFreshSubscriber` might be blocking in `stream.Recv()` waiting for init response
- Or `getActualPartitionAssignment()` is failing silently
- Or there's a deadlock between subscriber creation and fetch handler

**Next Steps**:
1. Add comprehensive error logging to `CreateFreshSubscriber`
2. Check if `getActualPartitionAssignment` is succeeding
3. Verify init response is being received
4. Consider alternative approach: reuse session but track its current position

## Files Modified (22 commits total)

### Core Implementation:
- `weed/mq/kafka/consumer_offset/*.go` - Offset storage layer
- `weed/mq/kafka/protocol/handler.go` - Offset storage integration
- `weed/mq/kafka/integration/seaweedmq_handler.go` - CreateFreshSubscriber
- `weed/mq/kafka/protocol/fetch.go` - System topic handling

### Debug/Analysis:
- Multiple `.md` documentation files
- Debug logging in broker_grpc_sub.go, fetch.go, fetch_multibatch.go

## Key Insights

1. **Data IS being stored correctly** - Broker logs show 511/111 byte values
2. **Data IS being retrieved correctly** - Broker subscriber sends correct data
3. **The bug was in the CACHING layer** - Session reuse caused stale reads
4. **New challenge**: Fresh sessions per fetch may have concurrency/initialization issues

## Recommendation

The subscriber session caching fix is correct in principle, but the implementation needs refinement to avoid blocking/timeout issues. Consider:
1. Async subscriber initialization
2. Connection pooling with proper state tracking
3. Or: Fix the cached session approach to track current offset position
