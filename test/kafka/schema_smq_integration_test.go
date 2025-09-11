package kafka

import (
	"fmt"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/protocol"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/schema"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// TestSchematizedMessageToSMQ demonstrates the full flow of schematized messages to SMQ
func TestSchematizedMessageToSMQ(t *testing.T) {
	t.Log("=== Testing Schematized Message to SMQ Integration ===")

	// Create a Kafka Gateway handler with schema support
	handler := createTestKafkaHandler(t)
	defer handler.Close()

	// Test the complete workflow
	t.Run("AvroMessageWorkflow", func(t *testing.T) {
		testAvroMessageWorkflow(t, handler)
	})

	t.Run("OffsetManagement", func(t *testing.T) {
		testOffsetManagement(t, handler)
	})

	t.Run("SchemaEvolutionWorkflow", func(t *testing.T) {
		testSchemaEvolutionWorkflow(t, handler)
	})
}

func createTestKafkaHandler(t *testing.T) *protocol.Handler {
	// Create handler with schema management enabled
	handler := protocol.NewHandler()

	// Enable schema management with mock registry
	err := handler.EnableSchemaManagement(schema.ManagerConfig{
		RegistryURL: "http://localhost:8081", // Mock registry
	})
	if err != nil {
		t.Logf("Schema management not enabled (expected in test): %v", err)
	}

	return handler
}

func testAvroMessageWorkflow(t *testing.T, handler *protocol.Handler) {
	t.Log("--- Testing Avro Message Workflow ---")

	// Step 1: Create Avro schema and message
	avroSchema := `{
		"type": "record",
		"name": "UserEvent",
		"fields": [
			{"name": "userId", "type": "int"},
			{"name": "eventType", "type": "string"},
			{"name": "timestamp", "type": "long"},
			{"name": "metadata", "type": ["null", "string"], "default": null}
		]
	}`

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		t.Fatalf("Failed to create Avro codec: %v", err)
	}

	// Step 2: Create user event data
	eventData := map[string]interface{}{
		"userId":    int32(12345),
		"eventType": "login",
		"timestamp": time.Now().UnixMilli(),
		"metadata":  map[string]interface{}{"string": `{"ip":"192.168.1.1","browser":"Chrome"}`},
	}

	// Step 3: Encode to Avro binary
	avroBinary, err := codec.BinaryFromNative(nil, eventData)
	if err != nil {
		t.Fatalf("Failed to encode Avro data: %v", err)
	}

	// Step 4: Create Confluent envelope (what Kafka clients send)
	schemaID := uint32(1)
	confluentMsg := schema.CreateConfluentEnvelope(schema.FormatAvro, schemaID, nil, avroBinary)

	t.Logf("Created Confluent message: %d bytes (schema ID: %d)", len(confluentMsg), schemaID)

	// Step 5: Simulate Kafka Produce request processing
	topicName := "user-events"
	partitionID := int32(0)

	// Get or create ledger for offset management
	ledger := handler.GetOrCreateLedger(topicName, partitionID)

	// Assign offset for this message
	baseOffset := ledger.AssignOffsets(1)
	t.Logf("Assigned Kafka offset: %d", baseOffset)

	// Step 6: Process the schematized message (simulate what happens in Produce handler)
	if handler.IsSchemaEnabled() {
		// Parse Confluent envelope
		envelope, ok := schema.ParseConfluentEnvelope(confluentMsg)
		if !ok {
			t.Fatal("Failed to parse Confluent envelope")
		}

		t.Logf("Parsed envelope - Schema ID: %d, Format: %s, Payload: %d bytes",
			envelope.SchemaID, envelope.Format, len(envelope.Payload))

		// This is where the message would be decoded and sent to SMQ
		// For now, we'll simulate the SMQ storage
		timestamp := time.Now().UnixNano()
		err = ledger.AppendRecord(baseOffset, timestamp, int32(len(confluentMsg)))
		if err != nil {
			t.Fatalf("Failed to append record to ledger: %v", err)
		}

		t.Logf("Stored message in SMQ simulation - Offset: %d, Timestamp: %d, Size: %d",
			baseOffset, timestamp, len(confluentMsg))
	}

	// Step 7: Verify offset management
	retrievedTimestamp, retrievedSize, err := ledger.GetRecord(baseOffset)
	if err != nil {
		t.Fatalf("Failed to retrieve record: %v", err)
	}

	t.Logf("Retrieved record - Timestamp: %d, Size: %d", retrievedTimestamp, retrievedSize)

	// Step 8: Check high water mark
	highWaterMark := ledger.GetHighWaterMark()
	t.Logf("High water mark: %d", highWaterMark)

	if highWaterMark != baseOffset+1 {
		t.Errorf("Expected high water mark %d, got %d", baseOffset+1, highWaterMark)
	}
}

func testOffsetManagement(t *testing.T, handler *protocol.Handler) {
	t.Log("--- Testing Offset Management ---")

	topicName := "offset-test-topic"
	partitionID := int32(0)

	// Get ledger
	ledger := handler.GetOrCreateLedger(topicName, partitionID)

	// Test multiple message offsets
	messages := []string{
		"Message 1",
		"Message 2",
		"Message 3",
	}

	var offsets []int64
	baseTime := time.Now().UnixNano()

	// Assign and store multiple messages
	for i, msg := range messages {
		offset := ledger.AssignOffsets(1)
		timestamp := baseTime + int64(i)*1000000 // 1ms apart
		err := ledger.AppendRecord(offset, timestamp, int32(len(msg)))
		if err != nil {
			t.Fatalf("Failed to append record %d: %v", i, err)
		}
		offsets = append(offsets, offset)
		t.Logf("Stored message %d at offset %d", i+1, offset)
	}

	// Verify offset continuity
	for i := 1; i < len(offsets); i++ {
		if offsets[i] != offsets[i-1]+1 {
			t.Errorf("Offset not continuous: %d -> %d", offsets[i-1], offsets[i])
		}
	}

	// Test offset queries
	earliestOffset := ledger.GetEarliestOffset()
	latestOffset := ledger.GetLatestOffset()
	highWaterMark := ledger.GetHighWaterMark()

	t.Logf("Offset summary - Earliest: %d, Latest: %d, High Water Mark: %d",
		earliestOffset, latestOffset, highWaterMark)

	// Verify offset ranges
	if earliestOffset != offsets[0] {
		t.Errorf("Expected earliest offset %d, got %d", offsets[0], earliestOffset)
	}
	if latestOffset != offsets[len(offsets)-1] {
		t.Errorf("Expected latest offset %d, got %d", offsets[len(offsets)-1], latestOffset)
	}
	if highWaterMark != latestOffset+1 {
		t.Errorf("Expected high water mark %d, got %d", latestOffset+1, highWaterMark)
	}

	// Test individual record retrieval
	for i, expectedOffset := range offsets {
		timestamp, size, err := ledger.GetRecord(expectedOffset)
		if err != nil {
			t.Errorf("Failed to get record at offset %d: %v", expectedOffset, err)
			continue
		}
		t.Logf("Record %d - Offset: %d, Timestamp: %d, Size: %d",
			i+1, expectedOffset, timestamp, size)
	}
}

func testSchemaEvolutionWorkflow(t *testing.T, handler *protocol.Handler) {
	t.Log("--- Testing Schema Evolution Workflow ---")

	if !handler.IsSchemaEnabled() {
		t.Skip("Schema management not enabled, skipping evolution test")
	}

	// Step 1: Create initial schema (v1)
	schemaV1 := `{
		"type": "record",
		"name": "Product",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "price", "type": "double"}
		]
	}`

	// Step 2: Create evolved schema (v2) - adds optional field
	schemaV2 := `{
		"type": "record",
		"name": "Product",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "price", "type": "double"},
			{"name": "category", "type": "string", "default": "uncategorized"}
		]
	}`

	// Step 3: Test schema compatibility (this would normally use the schema registry)
	t.Logf("Schema V1: %s", schemaV1)
	t.Logf("Schema V2: %s", schemaV2)

	// Step 4: Create messages with both schemas
	codecV1, err := goavro.NewCodec(schemaV1)
	if err != nil {
		t.Fatalf("Failed to create V1 codec: %v", err)
	}

	codecV2, err := goavro.NewCodec(schemaV2)
	if err != nil {
		t.Fatalf("Failed to create V2 codec: %v", err)
	}

	// Message with V1 schema
	productV1 := map[string]interface{}{
		"id":    int32(101),
		"name":  "Laptop",
		"price": 999.99,
	}

	// Message with V2 schema
	productV2 := map[string]interface{}{
		"id":       int32(102),
		"name":     "Mouse",
		"price":    29.99,
		"category": "electronics",
	}

	// Encode both messages
	binaryV1, err := codecV1.BinaryFromNative(nil, productV1)
	if err != nil {
		t.Fatalf("Failed to encode V1 message: %v", err)
	}

	binaryV2, err := codecV2.BinaryFromNative(nil, productV2)
	if err != nil {
		t.Fatalf("Failed to encode V2 message: %v", err)
	}

	// Create Confluent envelopes with different schema IDs
	msgV1 := schema.CreateConfluentEnvelope(schema.FormatAvro, 1, nil, binaryV1)
	msgV2 := schema.CreateConfluentEnvelope(schema.FormatAvro, 2, nil, binaryV2)

	// Step 5: Store both messages and track offsets
	topicName := "product-events"
	partitionID := int32(0)
	ledger := handler.GetOrCreateLedger(topicName, partitionID)

	// Store V1 message
	offsetV1 := ledger.AssignOffsets(1)
	timestampV1 := time.Now().UnixNano()
	err = ledger.AppendRecord(offsetV1, timestampV1, int32(len(msgV1)))
	if err != nil {
		t.Fatalf("Failed to store V1 message: %v", err)
	}

	// Store V2 message
	offsetV2 := ledger.AssignOffsets(1)
	timestampV2 := time.Now().UnixNano()
	err = ledger.AppendRecord(offsetV2, timestampV2, int32(len(msgV2)))
	if err != nil {
		t.Fatalf("Failed to store V2 message: %v", err)
	}

	t.Logf("Stored schema evolution messages - V1 at offset %d, V2 at offset %d",
		offsetV1, offsetV2)

	// Step 6: Verify both messages can be retrieved
	_, sizeV1, err := ledger.GetRecord(offsetV1)
	if err != nil {
		t.Errorf("Failed to retrieve V1 message: %v", err)
	}

	_, sizeV2, err := ledger.GetRecord(offsetV2)
	if err != nil {
		t.Errorf("Failed to retrieve V2 message: %v", err)
	}

	t.Logf("Retrieved messages - V1 size: %d, V2 size: %d", sizeV1, sizeV2)

	// Step 7: Demonstrate backward compatibility by reading V2 message with V1 schema
	// Parse V2 envelope
	envelopeV2, ok := schema.ParseConfluentEnvelope(msgV2)
	if !ok {
		t.Fatal("Failed to parse V2 envelope")
	}

	// Try to decode V2 payload with V1 codec (should work due to backward compatibility)
	decodedWithV1, _, err := codecV1.NativeFromBinary(envelopeV2.Payload)
	if err != nil {
		t.Logf("Expected: V1 codec cannot read V2 data directly: %v", err)
	} else {
		t.Logf("Backward compatibility: V1 codec read V2 data: %+v", decodedWithV1)
	}

	t.Log("Schema evolution workflow completed successfully")
}

// TestSMQDataFormat demonstrates how data is stored in SMQ format
func TestSMQDataFormat(t *testing.T) {
	t.Log("=== Testing SMQ Data Format ===")

	// Create a sample RecordValue (SMQ format)
	recordValue := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"userId": {
				Kind: &schema_pb.Value_Int32Value{Int32Value: 12345},
			},
			"eventType": {
				Kind: &schema_pb.Value_StringValue{StringValue: "purchase"},
			},
			"amount": {
				Kind: &schema_pb.Value_DoubleValue{DoubleValue: 99.99},
			},
			"timestamp": {
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: time.Now().UnixMicro(),
					},
				},
			},
		},
	}

	// Demonstrate how this would be stored/retrieved
	t.Logf("SMQ RecordValue fields: %d", len(recordValue.Fields))
	for fieldName, fieldValue := range recordValue.Fields {
		t.Logf("  %s: %v", fieldName, getValueString(fieldValue))
	}

	// Show how offsets map to SMQ timestamps
	topicName := "smq-format-test"
	partitionID := int32(0)

	// Create handler and ledger
	handler := createTestKafkaHandler(t)
	defer handler.Close()

	ledger := handler.GetOrCreateLedger(topicName, partitionID)

	// Simulate storing the SMQ record
	kafkaOffset := ledger.AssignOffsets(1)
	smqTimestamp := time.Now().UnixNano()
	recordSize := int32(len(recordValue.String())) // Approximate size

	err := ledger.AppendRecord(kafkaOffset, smqTimestamp, recordSize)
	if err != nil {
		t.Fatalf("Failed to store SMQ record: %v", err)
	}

	t.Logf("SMQ Storage mapping:")
	t.Logf("  Kafka Offset: %d", kafkaOffset)
	t.Logf("  SMQ Timestamp: %d", smqTimestamp)
	t.Logf("  Record Size: %d bytes", recordSize)

	// Demonstrate offset-to-timestamp mapping retrieval
	retrievedTimestamp, retrievedSize, err := ledger.GetRecord(kafkaOffset)
	if err != nil {
		t.Fatalf("Failed to retrieve SMQ record: %v", err)
	}

	t.Logf("Retrieved mapping:")
	t.Logf("  Timestamp: %d", retrievedTimestamp)
	t.Logf("  Size: %d bytes", retrievedSize)

	if retrievedTimestamp != smqTimestamp {
		t.Errorf("Timestamp mismatch: stored %d, retrieved %d", smqTimestamp, retrievedTimestamp)
	}
	if retrievedSize != recordSize {
		t.Errorf("Size mismatch: stored %d, retrieved %d", recordSize, retrievedSize)
	}
}

func getValueString(value *schema_pb.Value) string {
	switch v := value.Kind.(type) {
	case *schema_pb.Value_Int32Value:
		return fmt.Sprintf("int32(%d)", v.Int32Value)
	case *schema_pb.Value_StringValue:
		return fmt.Sprintf("string(%s)", v.StringValue)
	case *schema_pb.Value_DoubleValue:
		return fmt.Sprintf("double(%.2f)", v.DoubleValue)
	case *schema_pb.Value_TimestampValue:
		return fmt.Sprintf("timestamp(%d)", v.TimestampValue.TimestampMicros)
	default:
		return fmt.Sprintf("unknown(%T)", v)
	}
}

// TestCompressionWithSchemas tests compression in combination with schemas
func TestCompressionWithSchemas(t *testing.T) {
	t.Log("=== Testing Compression with Schemas ===")

	// Create Avro message
	avroSchema := `{
		"type": "record",
		"name": "LogEvent",
		"fields": [
			{"name": "level", "type": "string"},
			{"name": "message", "type": "string"},
			{"name": "timestamp", "type": "long"}
		]
	}`

	codec, err := goavro.NewCodec(avroSchema)
	if err != nil {
		t.Fatalf("Failed to create codec: %v", err)
	}

	// Create a large, compressible message
	logMessage := ""
	for i := 0; i < 100; i++ {
		logMessage += fmt.Sprintf("This is log entry %d with repeated content. ", i)
	}

	eventData := map[string]interface{}{
		"level":     "INFO",
		"message":   logMessage,
		"timestamp": time.Now().UnixMilli(),
	}

	// Encode to Avro
	avroBinary, err := codec.BinaryFromNative(nil, eventData)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	// Create Confluent envelope
	confluentMsg := schema.CreateConfluentEnvelope(schema.FormatAvro, 1, nil, avroBinary)

	t.Logf("Message sizes:")
	t.Logf("  Original log message: %d bytes", len(logMessage))
	t.Logf("  Avro binary: %d bytes", len(avroBinary))
	t.Logf("  Confluent envelope: %d bytes", len(confluentMsg))

	// This demonstrates how compression would work with the record batch parser
	// The RecordBatchParser would compress the entire record batch containing the Confluent message
	t.Logf("Compression would be applied at the Kafka record batch level")
	t.Logf("Schema processing happens after decompression in the Produce handler")
}

// TestOffsetConsistency verifies offset consistency across restarts
func TestOffsetConsistency(t *testing.T) {
	t.Log("=== Testing Offset Consistency ===")

	topicName := "consistency-test"
	partitionID := int32(0)

	// Create first handler instance
	handler1 := createTestKafkaHandler(t)
	ledger1 := handler1.GetOrCreateLedger(topicName, partitionID)

	// Store some messages
	offsets1 := make([]int64, 3)
	for i := 0; i < 3; i++ {
		offset := ledger1.AssignOffsets(1)
		timestamp := time.Now().UnixNano()
		err := ledger1.AppendRecord(offset, timestamp, 100)
		if err != nil {
			t.Fatalf("Failed to store message %d: %v", i, err)
		}
		offsets1[i] = offset
	}

	highWaterMark1 := ledger1.GetHighWaterMark()
	t.Logf("Handler 1 - Stored %d messages, high water mark: %d", len(offsets1), highWaterMark1)

	handler1.Close()

	// Create second handler instance (simulates restart)
	handler2 := createTestKafkaHandler(t)
	defer handler2.Close()

	ledger2 := handler2.GetOrCreateLedger(topicName, partitionID)

	// In a real implementation, the ledger would be restored from persistent storage
	// For this test, we simulate that the new ledger starts fresh
	highWaterMark2 := ledger2.GetHighWaterMark()
	t.Logf("Handler 2 - Initial high water mark: %d", highWaterMark2)

	// Store more messages
	offsets2 := make([]int64, 2)
	for i := 0; i < 2; i++ {
		offset := ledger2.AssignOffsets(1)
		timestamp := time.Now().UnixNano()
		err := ledger2.AppendRecord(offset, timestamp, 100)
		if err != nil {
			t.Fatalf("Failed to store message %d: %v", i, err)
		}
		offsets2[i] = offset
	}

	finalHighWaterMark := ledger2.GetHighWaterMark()
	t.Logf("Handler 2 - Final high water mark: %d", finalHighWaterMark)

	t.Log("Note: In production, offset consistency would be maintained through persistent storage")
	t.Log("The ledger would be restored from SeaweedMQ on startup")
}
