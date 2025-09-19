package integration

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/test/kafka/internal/testutil"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/protobuf/proto"
)

func TestSchemaBasedMessageFlow(t *testing.T) {
	// Skip if SMQ is not available
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQRequired)
	defer gateway.Close()

	topic := "schema-test-topic"
	partition := int32(0)

	// Create topic
	err := gateway.GetHandler().GetSeaweedMQHandler().CreateTopic(topic, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	// Test message
	originalKey := []byte("schema-key")
	originalValue := []byte(`{"name":"John","age":30}`)

	// Step 1: Produce message using schema-based encoding
	handler := gateway.GetHandler()
	offset, err := handler.ProduceSchemaBasedRecord(topic, partition, originalKey, originalValue)
	testutil.AssertNoError(t, err, "Failed to produce schema-based record")

	if offset < 0 {
		t.Errorf("Expected non-negative offset, got %d", offset)
	}

	// Step 2: Verify the message was stored as RecordValue in SMQ
	smqRecords, err := handler.GetSeaweedMQHandler().GetStoredRecords(topic, partition, offset, 1)
	testutil.AssertNoError(t, err, "Failed to get stored records")

	if len(smqRecords) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(smqRecords))
	}

	// Step 3: Verify the stored record is a valid RecordValue
	storedRecord := smqRecords[0]
	recordValue := &schema_pb.RecordValue{}
	err = proto.Unmarshal(storedRecord.GetValue(), recordValue)
	testutil.AssertNoError(t, err, "Stored record should be valid RecordValue")

	// Verify RecordValue structure
	if recordValue.Fields == nil {
		t.Fatal("RecordValue.Fields is nil")
	}

	// Check key field
	keyField, exists := recordValue.Fields["key"]
	if !exists {
		t.Fatal("Missing 'key' field in RecordValue")
	}
	if keyValue, ok := keyField.Kind.(*schema_pb.Value_BytesValue); ok {
		if string(keyValue.BytesValue) != string(originalKey) {
			t.Errorf("Key mismatch: expected '%s', got '%s'", string(originalKey), string(keyValue.BytesValue))
		}
	} else {
		t.Errorf("Key field is not BytesValue: %T", keyField.Kind)
	}

	// Check value field
	valueField, exists := recordValue.Fields["value"]
	if !exists {
		t.Fatal("Missing 'value' field in RecordValue")
	}
	if valueValue, ok := valueField.Kind.(*schema_pb.Value_BytesValue); ok {
		if string(valueValue.BytesValue) != string(originalValue) {
			t.Errorf("Value mismatch: expected '%s', got '%s'", string(originalValue), string(valueValue.BytesValue))
		}
	} else {
		t.Errorf("Value field is not BytesValue: %T", valueField.Kind)
	}

	// Check timestamp field
	timestampField, exists := recordValue.Fields["timestamp"]
	if !exists {
		t.Fatal("Missing 'timestamp' field in RecordValue")
	}
	if _, ok := timestampField.Kind.(*schema_pb.Value_TimestampValue); !ok {
		t.Errorf("Timestamp field is not TimestampValue: %T", timestampField.Kind)
	}

	// Step 4: Test fetch path - decode RecordValue back to Kafka message
	decodedValue := handler.DecodeRecordValueToKafkaMessage(storedRecord.GetValue())
	if decodedValue == nil {
		t.Fatal("Decoded value is nil")
	}

	if string(decodedValue) != string(originalValue) {
		t.Errorf("Decoded value mismatch: expected '%s', got '%s'", string(originalValue), string(decodedValue))
	}
}

func TestSchemaBasedMessageFlowWithNullValues(t *testing.T) {
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQRequired)
	defer gateway.Close()

	topic := "schema-null-test-topic"
	partition := int32(0)

	// Create topic
	err := gateway.GetHandler().GetSeaweedMQHandler().CreateTopic(topic, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	// Test with null key and value
	var nullKey []byte = nil
	var nullValue []byte = nil

	handler := gateway.GetHandler()
	offset, err := handler.ProduceSchemaBasedRecord(topic, partition, nullKey, nullValue)
	testutil.AssertNoError(t, err, "Failed to produce schema-based record with null values")

	// Verify storage
	smqRecords, err := handler.GetSeaweedMQHandler().GetStoredRecords(topic, partition, offset, 1)
	testutil.AssertNoError(t, err, "Failed to get stored records")

	if len(smqRecords) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(smqRecords))
	}

	// Verify RecordValue handles null values correctly
	storedRecord := smqRecords[0]
	recordValue := &schema_pb.RecordValue{}
	err = proto.Unmarshal(storedRecord.GetValue(), recordValue)
	testutil.AssertNoError(t, err, "Stored record should be valid RecordValue")

	// Check that null values are represented correctly
	keyField := recordValue.Fields["key"]
	if keyValue, ok := keyField.Kind.(*schema_pb.Value_BytesValue); ok {
		if keyValue.BytesValue != nil && len(keyValue.BytesValue) > 0 {
			t.Errorf("Expected null key to be empty bytes, got '%s'", string(keyValue.BytesValue))
		}
	}

	valueField := recordValue.Fields["value"]
	if valueValue, ok := valueField.Kind.(*schema_pb.Value_BytesValue); ok {
		if valueValue.BytesValue != nil && len(valueValue.BytesValue) > 0 {
			t.Errorf("Expected null value to be empty bytes, got '%s'", string(valueValue.BytesValue))
		}
	}
}

func TestRecordValueRoundTripWithDifferentTypes(t *testing.T) {
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQRequired)
	defer gateway.Close()

	topic := "schema-types-test-topic"
	partition := int32(0)

	// Create topic
	err := gateway.GetHandler().GetSeaweedMQHandler().CreateTopic(topic, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	handler := gateway.GetHandler()

	testCases := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{
			name:  "JSON message",
			key:   []byte("json-key"),
			value: []byte(`{"type":"json","data":"test"}`),
		},
		{
			name:  "Plain text message",
			key:   []byte("text-key"),
			value: []byte("plain text message"),
		},
		{
			name:  "Binary data",
			key:   []byte("binary-key"),
			value: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
		},
		{
			name:  "Empty message",
			key:   []byte("empty-key"),
			value: []byte(""),
		},
		{
			name:  "Unicode message",
			key:   []byte("unicode-key"),
			value: []byte("Hello 世界 🌍"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Produce message
			offset, err := handler.ProduceSchemaBasedRecord(topic, partition, tc.key, tc.value)
			testutil.AssertNoError(t, err, "Failed to produce message for "+tc.name)

			// Retrieve and verify
			smqRecords, err := handler.GetSeaweedMQHandler().GetStoredRecords(topic, partition, offset, 1)
			testutil.AssertNoError(t, err, "Failed to get stored records for "+tc.name)

			if len(smqRecords) != 1 {
				t.Fatalf("Expected 1 record for %s, got %d", tc.name, len(smqRecords))
			}

			// Decode back to Kafka message
			decodedValue := handler.DecodeRecordValueToKafkaMessage(smqRecords[0].GetValue())

			if string(decodedValue) != string(tc.value) {
				t.Errorf("Round-trip failed for %s: expected '%s', got '%s'",
					tc.name, string(tc.value), string(decodedValue))
			}
		})
	}
}

func TestRecordValueValidationInBroker(t *testing.T) {
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQRequired)
	defer gateway.Close()

	topic := "schema-validation-test-topic"
	partition := int32(0)

	// Create topic
	err := gateway.GetHandler().GetSeaweedMQHandler().CreateTopic(topic, 1)
	testutil.AssertNoError(t, err, "Failed to create topic")

	handler := gateway.GetHandler()

	// Produce a valid message
	key := []byte("validation-key")
	value := []byte("validation-value")

	offset, err := handler.ProduceSchemaBasedRecord(topic, partition, key, value)
	testutil.AssertNoError(t, err, "Failed to produce valid message")

	// Verify the message was accepted and stored
	smqRecords, err := handler.GetSeaweedMQHandler().GetStoredRecords(topic, partition, offset, 1)
	testutil.AssertNoError(t, err, "Failed to get stored records")

	if len(smqRecords) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(smqRecords))
	}

	// The fact that we can retrieve the record means broker validation passed
	recordValue := &schema_pb.RecordValue{}
	err = proto.Unmarshal(smqRecords[0].GetValue(), recordValue)
	testutil.AssertNoError(t, err, "Stored record should be valid RecordValue")

	// Verify all required fields are present
	requiredFields := []string{"key", "value", "timestamp"}
	for _, field := range requiredFields {
		if _, exists := recordValue.Fields[field]; !exists {
			t.Errorf("Missing required field '%s' in RecordValue", field)
		}
	}
}

func TestBackwardCompatibilityWithRawMessages(t *testing.T) {
	gateway := testutil.NewGatewayTestServerWithSMQ(t, testutil.SMQRequired)
	defer gateway.Close()

	handler := gateway.GetHandler()

	// Test that the decode function handles raw bytes (non-RecordValue) correctly
	rawMessage := []byte("raw-kafka-message")
	decodedValue := handler.DecodeRecordValueToKafkaMessage(rawMessage)

	if decodedValue == nil {
		t.Fatal("Decoded value should not be nil for raw message")
	}

	if string(decodedValue) != string(rawMessage) {
		t.Errorf("Raw message should pass through unchanged: expected '%s', got '%s'",
			string(rawMessage), string(decodedValue))
	}
}
