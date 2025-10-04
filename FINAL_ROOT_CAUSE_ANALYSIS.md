# Final Root Cause Analysis - Schema Registry Integration

## Status: Identified Likely Root Cause

### Evidence Summary

1. **Kafka Gateway Fetch Protocol: WORKING CORRECTLY** ✓
   - Clean offset progression: 0→1→2→...→12
   - No offset resets
   - Base offsets in record batches are correct
   - Returns data when HWM advances

2. **Schema Registry Writer: WORKING** ✓
   - Successfully writes schemas to `_schemas` topic
   - Logs: "Resource association log - (tenant, schemaHash, operation): (default, ..., REGISTER)"
   - All 10 schemas write successfully

3. **Schema Registry Reader Thread: CONSUMING BUT NOT PROCESSING** ✗
   - Successfully waits for offsets (logs show "Reached offset at X")
   - This means `waitUntilOffset()` succeeds
   - Which means `offsetInSchemasTopic` is being updated (line 214 in KafkaStoreReaderThread)
   - BUT: Cache remains empty (GET /subjects returns `[]`)

### The Smoking Gun

From Schema Registry logs:
```
POST /subjects/loadtest-topic-0-value/versions → 200 (writes schema ID 1)
Wait to catch up until the offset at 0
Reached offset at 0  ← Reader thread consumed offset 0!

Later:
GET /subjects → 200 2  ← Returns [] (empty array)
```

**Conclusion**: The reader thread IS consuming records (offset advances), but the records are NOT populating the cache.

## Root Cause Hypothesis

The reader thread processes records in `doWork()` (KafkaStoreReaderThread.java:196-294):

```java
for (ConsumerRecord<byte[], byte[]> record : records) {
    K messageKey = serializer.deserializeKey(record.key());  // Line 203
    
    if (!messageKey.equals(noopKey)) {
        V message = serializer.deserializeValue(record.value());  // Line 222
        
        // ... validation ...
        
        storeUpdateHandler.handleUpdate(
            messageKey, message, record.offset(), record.timestamp());  // Line 260
    }
}
```

Three possible failures (in order of likelihood):

### 1. **Deserialization Failure** (MOST LIKELY)
   - `serializer.deserializeValue(record.value())` throws exception
   - Exception is caught but logged as warning (line 226)
   - Record is skipped, cache never updated
   - **Why**: Our record value format might not match what Schema Registry expects

### 2. **Key Equality Issue**
   - Every record is matching `noopKey` check
   - All records are being treated as no-ops and skipped
   - **Why**: Our key encoding might be producing noop keys

### 3. **Validation Failure**
   - `message != null && !message.equals(oldMessage)` check fails (line 249)
   - Record is skipped
   - **Why**: Message format issues

## Next Steps to Diagnose

1. **Enable DEBUG Logging** in Schema Registry
   - Set `log4j.logger.io.confluent.kafka.schemaregistry.storage=DEBUG`
   - Look for deserialization warnings/errors

2. **Inspect Actual Record Content**
   - Use `weed sql` to query `_schemas` topic
   - Compare key/value format with real Kafka schema records
   - Check if our RecordValue encoding matches expectations

3. **Check Record Format**
   - Schema Registry expects specific Avro-encoded key/value format
   - Keys: `{magicByte}{schemaId}` for schema keys, or JSON for config keys
   - Values: Avro-encoded SchemaValue or ConfigValue

4. **Compare with Real Kafka**
   - Produce a test schema to real Kafka
   - Examine the raw bytes
   - Compare with our implementation

## The Key Question

**What format does Schema Registry's serializer expect for the `_schemas` topic records?**

Answer: `SchemaRegistrySerializer` (from schema-registry codebase) encodes records as:
- **Key**: Avro-encoded SchemaKey or ConfigKey
- **Value**: Avro-encoded SchemaValue or ConfigValue

We need to verify our Kafka Gateway returns records in this exact format, or Schema Registry's deserializer will fail silently.
