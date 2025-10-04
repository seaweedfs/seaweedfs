# ROOT CAUSE IDENTIFIED - Schema Registry Cache Issue

## The Smoking Gun

### What We're Returning to Schema Registry:
```
keyLen=28 valueLen=0 
keyHex=7b226b657974797065223a224e4f4f50222c226d
valueHex= (EMPTY!)
```

Decoded key: `{"keytype":"NOOP","m...`

### The Problem:
**ALL records in the `_schemas` topic are NOOP records with EMPTY values!**

## Why This Breaks Schema Registry

From `KafkaStoreReaderThread.java:210-218`:
```java
if (messageKey.equals(noopKey)) {
    // If it's a noop, update local offset counter and do nothing else
    offsetInSchemasTopic = record.offset();
    // NO CACHE UPDATE - just skip the record!
} else {
    // This code NEVER executes because all our records are NOOPs
    handleUpdate(messageKey, message, record.offset(), record.timestamp());
}
```

**Result**: 
- Reader thread consumes all records successfully (offsets advance 0→12) ✓
- But ALL records are NOOPs, so `handleUpdate()` is NEVER called ✗
- Cache remains empty ✗
- GET /subjects returns `[]` ✗

## The Real Issue

Schema Registry WRITES real schema data (we see "Resource association log" in SR logs), but when we READ it back via Kafka Gateway, we get:
- Keys: NOOP JSON format (wrong - should be Avro-encoded SchemaKey)
- Values: EMPTY (wrong - should be Avro-encoded SchemaValue)

## Where the Data Went Wrong

Two possibilities:

### 1. **Storage Issue** - Data stored incorrectly
   - Schema Registry produces Avro-encoded records
   - We store them in SMQ
   - But we're losing the actual value data somewhere

### 2. **Retrieval Issue** - Data retrieved incorrectly  
   - Data is stored correctly
   - But when we fetch it back, we're not decoding it properly
   - `decodeRecordValueToKafkaMessage()` might be returning empty

## Next Steps

1. Check what's actually stored in SMQ for `_schemas` topic
2. Verify Schema Registry is writing correct Avro data
3. Find where the value bytes are being lost
4. Fix the data flow to preserve Avro-encoded records

## Impact

This explains 100% of the symptoms:
- ✓ Schemas register successfully (writes work)
- ✓ Consumer advances offsets correctly (reads work)
- ✓ No deserialization errors (NOOPs are valid)
- ✗ Cache never populates (all NOOPs, no real data)
- ✗ GET returns empty (cache is empty)
