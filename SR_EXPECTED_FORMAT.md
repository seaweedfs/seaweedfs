# Schema Registry Expected Record Format

## Investigation Summary

Analyzed Schema Registry source code to understand the expected format for `_schemas` topic records.

## Key Findings

### 1. How Schema Registry Consumes `_schemas` Topic

**Source**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/KafkaStoreReaderThread.java`

```java
for (ConsumerRecord<byte[], byte[]> record : records) {
    K messageKey = null;
    try {
        messageKey = this.serializer.deserializeKey(record.key());
    } catch (SerializationException e) {
        log.error("Failed to deserialize the schema or config key at offset " + record.offset(), e);
        continue;
    }

    if (messageKey.equals(noopKey)) {
        // If it's a noop, update local offset counter and do nothing else
        offsetInSchemasTopic = record.offset();
        offsetReachedThreshold.signalAll();
    } else {
        V message = null;
        try {
            message = record.value() == null ? null
                                             : serializer.deserializeValue(messageKey, record.value());
        } catch (SerializationException e) {
            log.error("Failed to deserialize a schema or config update at offset " + record.offset(), e);
            continue;
        }
        // Process the schema/config update...
    }
}
```

### 2. Deserializer Implementation

**Source**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/serialization/SchemaRegistrySerializer.java`

```java
@Override
public SchemaRegistryKey deserializeKey(byte[] key) throws SerializationException {
    SchemaRegistryKey schemaKey = null;
    SchemaRegistryKeyType keyType = null;
    try {
        Map<Object, Object> keyObj = null;
        keyObj = JacksonMapper.INSTANCE.readValue(
            key, new TypeReference<Map<Object, Object>>() {});
        keyType = SchemaRegistryKeyType.forName((String) keyObj.get("keytype"));
        if (keyType == SchemaRegistryKeyType.CONFIG) {
            schemaKey = JacksonMapper.INSTANCE.readValue(key, ConfigKey.class);
        } else if (keyType == SchemaRegistryKeyType.MODE) {
            schemaKey = JacksonMapper.INSTANCE.readValue(key, ModeKey.class);
        } else if (keyType == SchemaRegistryKeyType.NOOP) {
            schemaKey = JacksonMapper.INSTANCE.readValue(key, NoopKey.class);
        } else if (keyType == SchemaRegistryKeyType.SCHEMA) {
            schemaKey = JacksonMapper.INSTANCE.readValue(key, SchemaKey.class);
        }
        // ...
    } catch (IOException e) {
        throw new SerializationException("Error while deserializing schema key", e);
    }
    return schemaKey;
}
```

### 3. NOOP Key Structure

**Source**: `core/src/main/java/io/confluent/kafka/schemaregistry/storage/NoopKey.java`

```java
@JsonPropertyOrder(value = {"keytype", "magic"})
public class NoopKey extends SchemaRegistryKey {
    private static final int MAGIC_BYTE = 0;

    public NoopKey() {
        super(SchemaRegistryKeyType.NOOP);
        this.magicByte = MAGIC_BYTE;
    }
}
```

**Parent class**: `SchemaRegistryKey.java`

```java
public abstract class SchemaRegistryKey implements Comparable<SchemaRegistryKey> {
    @Min(0)
    protected int magicByte;
    
    @NotEmpty
    protected SchemaRegistryKeyType keyType;
    
    @JsonProperty("magic")
    public int getMagicByte() {
        return this.magicByte;
    }
    
    @JsonProperty("keytype")
    public SchemaRegistryKeyType getKeyType() {
        return this.keyType;
    }
}
```

## Expected Format

### NOOP Records

**Key (JSON serialized)**:
```json
{"keytype":"NOOP","magic":0}
```

**Value**: `null` (empty)

### Schema Records

**Key (JSON serialized)**:
```json
{"keytype":"SCHEMA","subject":"my-topic-value","version":1,"magic":1}
```

**Value (JSON serialized)**:
```json
{"subject":"my-topic-value","version":1,"id":1,"schema":"...","deleted":false}
```

## Critical Issue

**Schema Registry expects JSON-serialized keys and values**, NOT protobuf-encoded `RecordValue` structures!

The Kafka Gateway's `decodeRecordValueToKafkaMessage` function is currently:
1. Returning raw bytes for system topics ✓ (correct)
2. BUT those raw bytes are protobuf `RecordValue` structures ✗ (wrong)
3. They should be JSON bytes as stored in the original Kafka message ✗ (missing)

## Root Cause

When Schema Registry produces to `_schemas`, it uses:
- **Producer serializer**: JSON (via Jackson)
- **Message format**: Standard Kafka wire protocol with JSON-serialized payload

When Kafka Gateway stores the message:
1. It receives JSON bytes from Schema Registry
2. It wraps them in a protobuf `RecordValue` structure
3. It stores the protobuf bytes in SMQ

When Kafka Gateway fetches the message:
1. It reads protobuf `RecordValue` bytes from SMQ
2. It tries to send them back to Schema Registry
3. Schema Registry tries to JSON-deserialize protobuf bytes → **FAILS**

## Solution

The Gateway must:
1. Store the original JSON bytes from Schema Registry in `RecordValue.value` field
2. When fetching for `_schemas` topic, extract and return the original JSON bytes
3. NOT attempt to re-encode/decode the payload

OR

1. Don't wrap `_schemas` topic messages in `RecordValue` at all
2. Store them as raw Kafka wire protocol bytes
3. Return them as-is

## Test Evidence

Current behavior:
- Schema Registry successfully writes to offset 0, 1, 2... 21
- Gateway shows offset 0 returns 1 record
- Gateway shows offset 1+ never returns (30s timeout)
- This suggests offset 0 (NOOP) might be processable, but offset 1+ (real schemas) are not

Expected: Schema Registry should successfully deserialize all records and populate its cache.

Actual: Schema Registry is stuck at offset 0, cannot parse subsequent records, times out on registration.
