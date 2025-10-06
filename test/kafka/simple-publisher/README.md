# Simple Kafka-Go Publisher for SeaweedMQ

This is a simple publisher client that demonstrates publishing raw messages to SeaweedMQ topics with "_" prefix, which bypass schema validation.

## Features

- **Schema-Free Publishing**: Topics with "_" prefix don't require schema validation
- **Raw Message Storage**: Messages are stored in a "value" field as raw bytes
- **Multiple Message Formats**: Supports JSON, binary, and empty messages
- **Kafka-Go Compatible**: Uses the popular kafka-go library

## Prerequisites

1. **SeaweedMQ Running**: Make sure SeaweedMQ is running on `localhost:17777` (default Kafka port)
2. **Go Modules**: The project uses Go modules for dependency management

## Setup and Run

```bash
# Navigate to the publisher directory
cd test/kafka/simple-publisher

# Download dependencies
go mod tidy

# Run the publisher
go run main.go
```

## Expected Output

```
Publishing messages to topic '_raw_messages' on broker 'localhost:17777'
Publishing messages...
- Published message 1: {"id":1,"message":"Hello from kafka-go client",...}
- Published message 2: {"id":2,"message":"Raw message without schema validation",...}
- Published message 3: {"id":3,"message":"Testing SMQ with underscore prefix topic",...}

Publishing different raw message formats...
- Published raw message 1: key=binary_key, value=Simple string message
- Published raw message 2: key=json_key, value={"raw_field": "raw_value", "number": 42}
- Published raw message 3: key=empty_key, value=
- Published raw message 4: key=, value=Message with no key

All test messages published to topic with '_' prefix!
These messages should be stored as raw bytes without schema validation.
```

## Topic Naming Convention

- **Schema-Required Topics**: `user-events`, `orders`, `payments` (require schema validation)
- **Schema-Free Topics**: `_raw_messages`, `_logs`, `_metrics` (bypass schema validation)

The "_" prefix tells SeaweedMQ to treat the topic as a system topic and skip schema processing entirely.

## Message Storage

For topics with "_" prefix:
- Messages are stored as raw bytes without schema validation
- No Confluent Schema Registry envelope is required
- Any binary data or text can be published
- SMQ assumes raw messages are stored in a "value" field internally

## Integration with SeaweedMQ

This client works with SeaweedMQ's existing schema bypass logic:

1. **`isSystemTopic()`** function identifies "_" prefix topics as system topics
2. **`produceSchemaBasedRecord()`** bypasses schema processing for system topics  
3. **Raw storage** via `seaweedMQHandler.ProduceRecord()` stores messages as-is

## Use Cases

- **Log ingestion**: Store application logs without predefined schema
- **Metrics collection**: Publish time-series data in various formats
- **Raw data pipelines**: Process unstructured data before applying schemas
- **Development/testing**: Quickly publish test data without schema setup
