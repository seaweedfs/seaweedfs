# Schema Registry Offset Reset Analysis

## Problem Statement
Schema Registry repeatedly resets its consumer offset to 0, preventing its internal cache from populating with registered schemas.

## Root Cause Discovery

### Schema Registry's Consumer Behavior

From `/Users/chrislu/dev/schema-registry/core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaStoreReaderThread.java`:

```java
// Lines 110-122: Checkpoint file handling
if (localStore.isPersistent()) {
  try {
    String checkpointDir = config.getString(SchemaRegistryConfig.KAFKASTORE_CHECKPOINT_DIR_CONFIG);
    int checkpointVersion = config.getInt(SchemaRegistryConfig.KAFKASTORE_CHECKPOINT_VERSION_CONFIG);
    checkpointFile = new OffsetCheckpoint(checkpointDir, checkpointVersion, topic);
    checkpointFileCache.putAll(checkpointFile.read());
  } catch (IOException e) {
    throw new IllegalStateException("Failed to read checkpoints", e);
  }
  log.info("Successfully read checkpoints");
}

// Lines 125-134: Consumer configuration
consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

// Lines 166-184: Offset initialization
this.topicPartition = new TopicPartition(topic, 0);
List<TopicPartition> topicPartitions = Arrays.asList(this.topicPartition);
this.consumer.assign(topicPartitions);

if (localStore.isPersistent()) {
  for (final TopicPartition topicPartition : topicPartitions) {
    final Long checkpoint = checkpointFileCache.get(topicPartition);
    if (checkpoint != null) {
      log.info("Seeking to checkpoint {} for {}", checkpoint, topicPartition);
      consumer.seek(topicPartition, checkpoint);  // <-- Manual seek to checkpoint
    } else {
      log.info("Seeking to beginning for {}", topicPartition);
      consumer.seekToBeginning(Collections.singletonList(topicPartition));  // <-- Manual seek to beginning
    }
  }
} else {
  log.info("Seeking to beginning for all partitions");
  consumer.seekToBeginning(topicPartitions);  // <-- Manual seek to beginning
}
```

### Key Findings

1. **Manual Offset Management**: Schema Registry does NOT use Kafka's consumer group offset commit/fetch mechanism. Instead:
   - It uses a **local checkpoint file** to store offsets
   - It manually calls `consumer.seek()` to position the consumer
   - It sets `ENABLE_AUTO_COMMIT_CONFIG = false`

2. **Checkpoint File**: The checkpoint file is stored locally on disk (not in Kafka):
   - Default location: `/var/lib/schema-registry/checkpoint` (or configured via `kafkastore.checkpoint.dir`)
   - This is a **local file-based offset tracking**, not Kafka's `__consumer_offsets` topic

3. **Offset Persistence Flow**:
   ```java
   // Lines 285-293: After processing records
   if (localStore.isPersistent() && initialized.get()) {
     try {
       localStore.flush();
       Map<TopicPartition, Long> offsets = storeUpdateHandler.checkpoint(records.count());
       checkpointOffsets(offsets);  // <-- Writes to local file
     } catch (StoreException se) {
       log.warn("Failed to flush", se);
     }
   }
   
   // Lines 307-317: Writing checkpoint file
   private void checkpointOffsets(Map<TopicPartition, Long> offsets) {
     Map<TopicPartition, Long> newOffsets = offsets != null
         ? offsets
         : Collections.singletonMap(new TopicPartition(topic, 0), offsetInSchemasTopic + 1);
     checkpointFileCache.putAll(newOffsets);
     try {
       checkpointFile.write(checkpointFileCache);  // <-- Local file write
     } catch (final IOException e) {
       log.warn("Failed to write offset checkpoint file to {}: {}", checkpointFile, e);
     }
   }
   ```

## The Real Problem

**Schema Registry does NOT rely on Kafka's OffsetCommit/OffsetFetch API at all!**

The issue is NOT about consumer group offsets. The actual problem is:

1. **Manual `consumer.seek()` calls**: Schema Registry explicitly seeks to beginning or checkpoint
2. **When does it seek to beginning?**:
   - When `localStore.isPersistent()` is `true` but checkpoint file is missing/empty
   - When `localStore.isPersistent()` is `false` (in-memory mode)

3. **Why it keeps resetting to 0**:
   - Either the checkpoint file is not being created/written properly
   - OR the checkpoint read is failing/returning null
   - OR the local store is configured as non-persistent

## Kafka Gateway Implications

The Kafka Gateway's `OffsetCommit` and `OffsetFetch` implementations are **IRRELEVANT** to Schema Registry's behavior because:

1. Schema Registry uses `ENABLE_AUTO_COMMIT_CONFIG = false`
2. It never calls `consumer.commitSync()` or `consumer.commitAsync()`
3. It never relies on fetching committed offsets from Kafka

## What Actually Matters

Schema Registry's offset position depends on:

1. **Fetch API correctness**: When Schema Registry calls `consumer.poll()`, it must:
   - Start from the position set by `consumer.seek(topicPartition, offset)`
   - Return records starting from that offset
   - NOT reset or ignore the seek position

2. **Metadata API**: When Schema Registry starts and has no checkpoint:
   - It calls `consumer.seekToBeginning()`
   - This internally uses `ListOffsets` API with `timestamp = -2` (earliest)
   - The broker must correctly return the earliest available offset

## Next Steps

1. **Verify Fetch API honors seek position**: Add debug logging to see if `Fetch` requests respect the offset from `seek()`
2. **Check ListOffsets API**: Verify that `seekToBeginning()` correctly queries the earliest offset
3. **Investigate checkpoint file**: Check if Schema Registry is writing/reading its local checkpoint file correctly in the Docker environment

## Conclusion

The offset reset issue is NOT a consumer group offset management problem. It's a **Fetch protocol seek position** problem where the Kafka Gateway may not be correctly honoring the consumer's explicit seek position set via `consumer.seek()`.
