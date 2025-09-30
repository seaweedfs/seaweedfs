# Schema Registry Analysis: How It Works and What Kafka Gateway Needs

## Overview

This document explains how Confluent Schema Registry processes schema registration and verification, and what the Kafka Gateway needs to support for Schema Registry to work correctly.

## How Schema Registry Works

### 1. Initialization (`KafkaStore.init()`)

When Schema Registry starts:

```
1. Creates or verifies the _schemas topic exists
2. Creates a Kafka Producer for writing schemas
3. Starts KafkaStoreReaderThread - a background thread that:
   - Creates a Kafka Consumer with GROUP_ID
   - Subscribes to the _schemas topic
   - Sets AUTO_OFFSET_RESET_CONFIG to "earliest"
   - Seeks to checkpoint (if persistent) or beginning
   - Continuously polls for messages
4. Waits for reader to catch up to latest offset (if initWaitForReader=true)
```

### 2. Schema Registration Process

When a schema is registered via REST API:

```java
// File: KafkaSchemaRegistry.register()

1. kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs)
   - This ensures the cache is up-to-date before writing
   
2. Determines the next version number by reading all existing versions

3. Validates and canonicalizes the schema

4. Writes to Kafka topic (_schemas):
   - Key: SchemaKey (subject, version)
   - Value: SchemaValue (schema, id, deleted flag, etc.)
   - Producer with ACKS=-1 and ENABLE_IDEMPOTENCE=true

5. Returns schema ID to client (from response)
```

### 3. Background Reader Thread (`KafkaStoreReaderThread`)

The reader thread continuously processes messages:

```java
// File: KafkaStoreReaderThread.doWork()

while (running) {
    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
    
    for (ConsumerRecord<byte[], byte[]> record : records) {
        messageKey = serializer.deserializeKey(record.key());
        
        if (messageKey.equals(noopKey)) {
            // Update offset counter and signal waiting threads
            offsetInSchemasTopic = record.offset();
            offsetReachedThreshold.signalAll();
        } else {
            messageValue = serializer.deserializeValue(record.value());
            
            // Validate the message
            storeUpdateHandler.validateUpdate(messageKey, messageValue, tp, offset, timestamp);
            
            // Write to local store (in-memory cache)
            localStore.put(messageKey, messageValue);
            
            // Update lookup cache (for fast retrieval)
            storeUpdateHandler.handleUpdate(messageKey, messageValue, oldValue, tp, offset, timestamp);
        }
    }
}
```

### 4. Cache Population (`KafkaStoreMessageHandler.handleUpdate()`)

When a schema message is received:

```java
// File: KafkaStoreMessageHandler.handleSchemaUpdate()

if (schemaValue != null) {
    // Update the maximum id seen so far
    idGenerator.schemaRegistered(schemaKey, schemaValue);
    
    if (schemaValue.isDeleted()) {
        lookupCache.schemaDeleted(schemaKey, schemaValue, oldSchemaValue);
    } else {
        // THIS IS THE KEY STEP - updates the lookupCache
        lookupCache.schemaRegistered(schemaKey, schemaValue, oldSchemaValue);
    }
}
```

### 5. Schema Retrieval Process

When a schema is retrieved via REST API (`GET /schemas/ids/{id}` or `GET /subjects/{subject}/versions/latest`):

```java
// File: KafkaSchemaRegistry.get(int id, String subject)

1. getSchemaKeyUsingContexts(id, subject) - lookup in cache
2. kafkaStore.get(subjectVersionKey) - retrieve from local store
3. Return schema to client

// The lookupCache is used for fast lookups:
SchemaIdAndSubjects schemaIdAndSubjects = lookupCache.schemaIdAndSubjects(schema);
```

## Critical Insight: The Problem

### Why Schemas Fail Verification

**The lookupCache is only populated when:**
1. The `KafkaStoreReaderThread` successfully consumes messages from the `_schemas` topic
2. The messages are processed by `KafkaStoreMessageHandler.handleUpdate()`
3. The `lookupCache.schemaRegistered()` method is called

**In our case:**
- ✅ Schemas are successfully **written** to `_schemas` topic (Producer succeeds)
- ✅ Schema IDs are returned to the client
- ❌ Schemas **cannot be retrieved** (GET requests return 404)

**This means:**
- The `KafkaStoreReaderThread` is **not processing the messages** it just wrote
- OR the messages are being processed but **lookupCache.schemaRegistered() is not being called**
- OR there's a **race condition** where verification happens before the reader processes the write

### Evidence from Logs

From Schema Registry logs:
```
Wait to catch up until the offset at 0
Reached offset at 0
Wait to catch up until the offset at 4
Reached offset at 4
Wait to catch up until the offset at 5
Reached offset at 5
```

**Pattern Analysis:**
- Schema Registry repeatedly reaches offset 0, then jumps to higher offsets
- This suggests: **Consumer offset is being reset** or **messages are not being read in sequence**
- The offset jumps (0→4, 0→5) indicate potential **offset management issues**

### The waitUntilOffset Mechanism

```java
// File: KafkaStoreReaderThread.waitUntilOffset()

public void waitUntilOffset(long offset, long timeout, TimeUnit timeUnit) {
    while ((offsetInSchemasTopic < offset) && (timeoutNs > 0)) {
        timeoutNs = offsetReachedThreshold.awaitNanos(timeoutNs);
    }
    
    if (offsetInSchemasTopic < offset) {
        throw new StoreTimeoutException(
            "KafkaStoreReaderThread failed to reach target offset within the timeout interval. "
            + "targetOffset: " + offset + ", offsetReached: " + offsetInSchemasTopic
        );
    }
}
```

**This is called in register():**
```java
kafkaStore.waitUntilKafkaReaderReachesLastOffset(subject, kafkaStoreTimeoutMs);
```

## What Kafka Gateway Must Support

### 1. **Consumer Group Management** ✅

Schema Registry uses a consumer group with:
- `GROUP_ID`: Configured via `schemaregistry.group.id`
- `CLIENT_ID`: `KafkaStore-reader-{topic}`
- `AUTO_OFFSET_RESET_CONFIG`: `earliest`

**Requirements:**
- Kafka Gateway must properly manage consumer offsets for the group
- Consumer must be able to seek to beginning
- Consumer must be able to commit offsets
- **Offset commits must persist** across restarts

### 2. **Offset Sequence Integrity** ⚠️ **POTENTIAL ISSUE**

Schema Registry relies on **sequential offset processing**:
- Writer produces message at offset N
- Reader must read all messages from 0 to N in order
- `offsetInSchemasTopic` tracks the current position

**Requirements:**
- Offsets must be **strictly sequential** (0, 1, 2, 3, ...)
- No gaps in offset sequence
- Consumer must receive messages in order
- **This is where our issue likely exists**

### 3. **NOOP Message Handling** ✅

Schema Registry writes NOOP messages with a special key:
```java
if (messageKey.equals(noopKey)) {
    offsetInSchemasTopic = record.offset();
    offsetReachedThreshold.signalAll();
}
```

**Requirements:**
- NOOP messages must be delivered to consumers
- NOOP messages update the offset counter
- NOOP messages signal waiting threads

### 4. **Message Delivery Guarantee** ⚠️ **CRITICAL**

Producer configuration:
```java
props.put(ProducerConfig.ACKS_CONFIG, "-1");  // Wait for all replicas
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);  // Exactly-once
```

**Requirements:**
- Messages must be durably persisted before ack
- No message loss
- No duplicate messages (idempotence)
- **Consumer must see ALL messages that were acknowledged to producer**

### 5. **Consumer Offset Management** ⚠️ **LIKELY ROOT CAUSE**

From our test results:
- Java reproducer shows: Schemas registered at IDs 12-21
- Verification fails: GET requests return 404
- Schema Registry logs: "Reached offset at 0", "Reached offset at 4", "Reached offset at 5"

**Hypothesis:**
The consumer offset is not progressing correctly. Possible causes:

1. **Offset Reset Issue**: Consumer offset is being reset to 0 repeatedly
   - Check: Consumer group offset storage in Kafka Gateway
   - Check: OffsetCommit/OffsetFetch protocol implementation

2. **Fetch Response Issue**: Consumer is not receiving all messages
   - Check: Fetch response includes all messages from requested offset
   - Check: High water mark is correctly reported

3. **Offset Commit Issue**: Consumer commits are not being persisted
   - Check: OffsetCommit protocol handler
   - Check: Consumer group metadata persistence

### 6. **High Water Mark Accuracy** ✅ (Verified Working)

From our tests:
```
Got highWaterMark 12 for topic _schemas partition 0
```

This is **correct** - we have 12 messages (2 NOOP + 10 SCHEMA).

**Requirements:**
- High water mark must reflect the next offset to be written
- High water mark must be accurate immediately after write
- Fetch responses must include correct high water mark

## Root Cause Hypothesis

Based on the evidence:

### **Primary Hypothesis: Consumer Offset Not Progressing**

**Symptoms:**
1. Schemas write successfully (Producer returns schema IDs)
2. Schema Registry logs show "Reached offset at 0" repeatedly
3. Verification fails (lookupCache is empty)
4. Consumer appears to be stuck or resetting

**Likely Causes:**

1. **OffsetCommit Not Persisting**: 
   - Consumer commits offset, but Kafka Gateway doesn't save it
   - On next poll, consumer resets to beginning or last checkpoint
   - Messages are re-read but cache is cleared/reset

2. **Fetch Starting From Wrong Offset**:
   - Consumer requests fetch from offset N
   - Kafka Gateway returns messages from offset 0 instead
   - Consumer processes duplicates and gets confused

3. **Consumer Group State Not Maintained**:
   - Consumer joins group, gets partition assignment
   - State is not persisted
   - On reconnect, group rebalances and loses progress

### **Secondary Hypothesis: Message Format Issue**

The messages might not be in the format Schema Registry expects:
- Schema Registry deserializes key/value using its Serializer
- If format is wrong, deserial

ization fails silently
- Messages are skipped, cache is not updated

## Testing Recommendations

### 1. Verify Consumer Offset Persistence

```bash
# After schemas are registered, check consumer group offsets
kafka-consumer-groups --bootstrap-server kafka-gateway:9093 \
  --group schema-registry-{cluster-id} \
  --describe
```

**Expected**: Offset should be at 12 (or latest message offset)
**If Issue**: Offset is 0 or not saved

### 2. Monitor Consumer Fetch Requests

Add debug logging in Kafka Gateway for:
- `OffsetFetchRequest` - What offset does consumer request?
- `FetchRequest` - What offset does consumer fetch from?
- `OffsetCommitRequest` - What offset does consumer commit?

### 3. Verify Message Format

Check that messages written to `_schemas` topic match Kafka's wire format:
- RecordBatch format
- Message key/value format
- Compression (if any)

### 4. Test Consumer Offset Reset

```bash
# Force consumer to reset and observe behavior
kafka-consumer-groups --bootstrap-server kafka-gateway:9093 \
  --group schema-registry-{cluster-id} \
  --reset-offsets --to-earliest \
  --topic _schemas \
  --execute
```

## Recommended Fixes

### Priority 1: Consumer Offset Management

1. **Verify OffsetCommit Handler**:
   - Ensure offsets are persisted to `__consumer_offsets` topic
   - Verify OffsetFetch returns correct offsets
   - Check for race conditions in offset updates

2. **Debug Fetch Protocol**:
   - Log every fetch request with requested offset
   - Log every fetch response with returned offset range
   - Verify messages are returned from correct offset

3. **Check Consumer Group State**:
   - Verify JoinGroup/SyncGroup store consumer metadata
   - Verify partition assignments are stable
   - Check for unexpected rebalances

### Priority 2: Message Delivery

1. **Verify All Messages Reach Consumer**:
   - Producer writes message at offset N
   - Consumer should read message at offset N
   - No gaps, no duplicates, no out-of-order

2. **Check NOOP Message Processing**:
   - Schema Registry writes 2 NOOP messages initially
   - Consumer should process them and update offset
   - Verify NOOP key format matches expected format

## Conclusion

The **Kafka Gateway is successfully writing messages** to the `_schemas` topic, as evidenced by:
- ✅ 10/10 schemas registered successfully
- ✅ Schema IDs returned (12-21)
- ✅ SQL queries show 12 messages in topic
- ✅ High water mark is correct (12)

The **problem is in the read path**: Schema Registry's consumer is not successfully reading and processing the messages it just wrote, which prevents the `lookupCache` from being populated.

**The most likely root cause is consumer offset management** - either offsets are not being committed/persisted correctly, or the fetch responses are not returning messages from the correct offset.

**Next Steps:**
1. Add detailed logging to Kafka Gateway's OffsetCommit and Fetch handlers
2. Verify consumer group offset persistence
3. Test with a simple Kafka consumer to isolate the issue
4. Compare Kafka Gateway's behavior with a real Kafka broker using `tcpdump` or protocol inspection
