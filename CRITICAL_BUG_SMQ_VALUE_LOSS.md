# CRITICAL BUG: SMQ Value Data Loss for System Topics

## Executive Summary
**SMQ is losing value data between write and read for the `_schemas` topic.**

## Evidence Trail

### 1. Write Path - Data EXISTS (Value length=511, 111 bytes)
```
🔥 BROKER DEBUG: DataMessage Key length=28, Value length=0      <- NOOP
🔥 BROKER DEBUG: DataMessage Key length=28, Value length=0      <- NOOP  
🔥 BROKER DEBUG: DataMessage Key length=77, Value length=511    <- REAL DATA ✓
🔥 BROKER DEBUG: DataMessage Key length=75, Value length=111    <- REAL DATA ✓
🔥 BROKER DEBUG: DataMessage Key length=77, Value length=511    <- REAL DATA ✓
```

**12 total records written: 2 NOOPs + 10 schemas with data (511, 111 bytes each)**

### 2. Read Path - Data LOST (All values are 0 bytes)
```
📍 SR RECORD[0]: keyLen=28 valueLen=0  <- Reading offset 0 (NOOP, correct)
📍 SR RECORD[0]: keyLen=28 valueLen=0  <- Reading offset 1 (NOOP, correct)
📍 SR RECORD[0]: keyLen=28 valueLen=0  <- Reading offset 2 (SHOULD BE 511 bytes!) ✗
📍 SR RECORD[0]: keyLen=28 valueLen=0  <- Reading offset 3 (SHOULD BE 111 bytes!) ✗
...
```

**All 12 reads return valueLen=0, even though 10 were written with data!**

## Root Cause Location

The bug is in the SMQ storage/retrieval layer, somewhere between:

1. `Handler.produceSchemaBasedRecord()` (line 1144 in produce.go)
   - Calls: `h.seaweedMQHandler.ProduceRecord(topic, partition, key, value)`
   - At this point, `value` has correct length (511, 111 bytes)

2. `MultiBatchFetcher.GetStoredRecords()` (fetch_multibatch.go)
   - Returns: `smqRecords` with all empty values
   - At this point, `GetValue()` returns 0-byte arrays

## Hypothesis

The issue is likely in one of these places:

### A. SMQ Broker's PublishRecord
- Location: `seaweedMQHandler.ProduceRecord()` implementation
- Problem: May not be storing the value field correctly for system topics

### B. Filer Storage
- Location: How messages are written to/read from the filer
- Problem: Value field might not be included in the protobuf message stored

### C. LogBuffer
- Location: In-memory buffer before filer persistence  
- Problem: Value field might be dropped when creating DataMessage

### D. GetStoredRecords
- Location: Reading from filer back to Kafka Gateway
- Problem: Value field might not be parsed/returned correctly

## Impact

- Schema Registry cannot function (cache never populates)
- Any system topic using `_` prefix will have this issue
- Consumer offsets topic (`__consumer_offsets`) likely affected too
- **BLOCKS all Schema Registry integration**

## Next Steps

1. Add logging in `seaweedMQHandler.ProduceRecord()` to verify value is received
2. Check what's written to filer (inspect actual filer entry)
3. Check what's read from filer (log GetStoredRecords input)
4. Find where value bytes are being dropped
5. Fix the data loss bug

## Test Status

- Offset management: ✓ Working
- Fetch protocol: ✓ Working  
- Schema registration (write): ✓ Working
- **Schema retrieval (read): ✗ BROKEN (this bug)**
