# Schema Registry "Reached offset at X" Behavior - EXPLAINED

## The Mystery Solved

The log pattern `0→0→1→0→0→2` is **NORMAL Schema Registry behavior**, not a bug!

### What "Reached offset at X" Actually Means

From `KafkaStore.java:320-324`:
```java
private void waitUntilKafkaReaderReachesOffset(long offset, int timeoutMs) throws StoreException {
    log.info("Wait to catch up until the offset at {}", offset);
    kafkaTopicReader.waitUntilOffset(offset, timeoutMs, TimeUnit.MILLISECONDS);
    log.info("Reached offset at {}", offset);  // <-- This is what we see in logs
}
```

This is **NOT** logging consumed record offsets. It's logging **synchronization points** where Schema Registry waits for its background reader thread to catch up after writes.

### Why We See Duplicates

1. **After every schema registration** (PUT operation, line 367):
   ```java
   waitUntilKafkaReaderReachesOffset(recordMetadata.offset(), timeout);
   ```
   - Writes record at offset N
   - Waits for reader to reach offset N
   - Logs: "Reached offset at N"

2. **Before many operations** (e.g., line 704):
   ```java
   kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
   ```
   - Calls `lastOffset(subject)` which returns `lastWrittenOffset` (global!)
   - If last write was at offset 0, waits for offset 0 again
   - Logs: "Reached offset at 0" (duplicate!)

### The Pattern Explained

```
[Write Schema ID=1 to offset 0]
  → Wait for reader to reach offset 0
  → "Reached offset at 0"

[Read operation before next write]
  → Wait for lastOffset (still 0!)
  → "Reached offset at 0"  ← DUPLICATE

[Write Schema ID=2 to offset 1]
  → Wait for reader to reach offset 1
  → "Reached offset at 1"

[Another operation]
  → Wait for lastOffset (now 1)
  → "Reached offset at 1"  ← DUPLICATE

[Some operation checks last offset for whole topic]
  → Wait for lastOffset (but it's 0 from some reason!)
  → "Reached offset at 0"  ← This is the confusing one
```

## The REAL Problem

The "Reached offset" logs are **red herrings**. The actual problem is elsewhere.

### Schema Registry's Reader Thread

From `KafkaStoreReaderThread.java:200`:
```java
for (ConsumerRecord<byte[], byte[]> record : records) {
    // Process each record and update offsetInSchemasTopic
    offsetInSchemasTopic = record.offset();  // Line 214
}
```

The reader thread **IS** consuming records (we know this because the Kafka Gateway logs show clean 0→1→2→...→12 progression).

### The Cache Population Issue

Schema Registry populates its `lookupCache` in `KafkaStoreMessageHandler.handleUpdate()`. If schemas aren't appearing in GET requests, either:

1. **Records have wrong format** - Schema Registry can't deserialize them
2. **Records are being consumed but not processed** - Deserialization fails silently
3. **Cache is being populated but GET requests query differently** - Different key format

## Next Investigation Steps

1. **Check Schema Registry error logs** for deserialization failures
2. **Verify record format** - Are we encoding schema records correctly?
3. **Check if `handleUpdate()` is being called** - Is the cache population code executing?
4. **Compare with real Kafka** - What's different about our record encoding?

## Conclusion

The offset "resets" in logs are NORMAL. The Kafka Gateway's Fetch protocol is working correctly (offsets advance 0→12). The issue is likely in:
- Record serialization format
- Schema value encoding
- Key encoding for schema records
