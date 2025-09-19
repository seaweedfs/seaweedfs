package broker

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"google.golang.org/protobuf/proto"
)

func TestValidateRecordValue(t *testing.T) {
	broker := &MessageQueueBroker{}
	
	// Test valid Kafka RecordValue
	validRecord := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"key": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("test-key")},
			},
			"value": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("test-value")},
			},
			"timestamp": {
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: 1234567890,
						IsUtc:           true,
					},
				},
			},
		},
	}
	
	kafkaTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      "test-topic",
	}
	
	err := broker.validateRecordValue(validRecord, kafkaTopic)
	if err != nil {
		t.Errorf("Valid Kafka RecordValue should pass validation: %v", err)
	}
}

func TestValidateRecordValueMissingFields(t *testing.T) {
	broker := &MessageQueueBroker{}
	
	kafkaTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      "test-topic",
	}
	
	// Test missing key field
	recordMissingKey := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"value": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("test-value")},
			},
			"timestamp": {
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: 1234567890,
						IsUtc:           true,
					},
				},
			},
		},
	}
	
	err := broker.validateRecordValue(recordMissingKey, kafkaTopic)
	if err == nil {
		t.Error("RecordValue missing key field should fail validation")
	}
	if err.Error() != "Kafka RecordValue missing 'key' field" {
		t.Errorf("Expected specific error message, got: %v", err)
	}
	
	// Test missing value field
	recordMissingValue := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"key": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("test-key")},
			},
			"timestamp": {
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: 1234567890,
						IsUtc:           true,
					},
				},
			},
		},
	}
	
	err = broker.validateRecordValue(recordMissingValue, kafkaTopic)
	if err == nil {
		t.Error("RecordValue missing value field should fail validation")
	}
	if err.Error() != "Kafka RecordValue missing 'value' field" {
		t.Errorf("Expected specific error message, got: %v", err)
	}
	
	// Test missing timestamp field
	recordMissingTimestamp := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"key": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("test-key")},
			},
			"value": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("test-value")},
			},
		},
	}
	
	err = broker.validateRecordValue(recordMissingTimestamp, kafkaTopic)
	if err == nil {
		t.Error("RecordValue missing timestamp field should fail validation")
	}
	if err.Error() != "Kafka RecordValue missing 'timestamp' field" {
		t.Errorf("Expected specific error message, got: %v", err)
	}
}

func TestValidateRecordValueNonKafkaTopic(t *testing.T) {
	broker := &MessageQueueBroker{}
	
	// For non-Kafka topics, validation should be more lenient
	nonKafkaTopic := &schema_pb.Topic{
		Namespace: "custom",
		Name:      "test-topic",
	}
	
	recordWithoutKafkaFields := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"custom_field": {
				Kind: &schema_pb.Value_StringValue{StringValue: "custom-value"},
			},
		},
	}
	
	err := broker.validateRecordValue(recordWithoutKafkaFields, nonKafkaTopic)
	if err != nil {
		t.Errorf("Non-Kafka topic should allow flexible RecordValue structure: %v", err)
	}
}

func TestValidateRecordValueNilInputs(t *testing.T) {
	broker := &MessageQueueBroker{}
	
	kafkaTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      "test-topic",
	}
	
	// Test nil RecordValue
	err := broker.validateRecordValue(nil, kafkaTopic)
	if err == nil {
		t.Error("Nil RecordValue should fail validation")
	}
	if err.Error() != "RecordValue is nil" {
		t.Errorf("Expected specific error message, got: %v", err)
	}
	
	// Test RecordValue with nil Fields
	recordWithNilFields := &schema_pb.RecordValue{
		Fields: nil,
	}
	
	err = broker.validateRecordValue(recordWithNilFields, kafkaTopic)
	if err == nil {
		t.Error("RecordValue with nil Fields should fail validation")
	}
	if err.Error() != "RecordValue.Fields is nil" {
		t.Errorf("Expected specific error message, got: %v", err)
	}
}

func TestRecordValueMarshalUnmarshalIntegration(t *testing.T) {
	broker := &MessageQueueBroker{}
	
	// Create a valid RecordValue
	originalRecord := &schema_pb.RecordValue{
		Fields: map[string]*schema_pb.Value{
			"key": {
				Kind: &schema_pb.Value_BytesValue{BytesValue: []byte("integration-key")},
			},
			"value": {
				Kind: &schema_pb.Value_StringValue{StringValue: "integration-value"},
			},
			"timestamp": {
				Kind: &schema_pb.Value_TimestampValue{
					TimestampValue: &schema_pb.TimestampValue{
						TimestampMicros: 1234567890,
						IsUtc:           true,
					},
				},
			},
		},
	}
	
	// Marshal to bytes
	recordBytes, err := proto.Marshal(originalRecord)
	if err != nil {
		t.Fatalf("Failed to marshal RecordValue: %v", err)
	}
	
	// Unmarshal back
	unmarshaledRecord := &schema_pb.RecordValue{}
	err = proto.Unmarshal(recordBytes, unmarshaledRecord)
	if err != nil {
		t.Fatalf("Failed to unmarshal RecordValue: %v", err)
	}
	
	// Validate the unmarshaled record
	kafkaTopic := &schema_pb.Topic{
		Namespace: "kafka",
		Name:      "integration-topic",
	}
	
	err = broker.validateRecordValue(unmarshaledRecord, kafkaTopic)
	if err != nil {
		t.Errorf("Unmarshaled RecordValue should pass validation: %v", err)
	}
	
	// Verify field values
	keyField := unmarshaledRecord.Fields["key"]
	if keyValue, ok := keyField.Kind.(*schema_pb.Value_BytesValue); ok {
		if string(keyValue.BytesValue) != "integration-key" {
			t.Errorf("Key field mismatch: expected 'integration-key', got '%s'", string(keyValue.BytesValue))
		}
	} else {
		t.Errorf("Key field is not BytesValue: %T", keyField.Kind)
	}
	
	valueField := unmarshaledRecord.Fields["value"]
	if valueValue, ok := valueField.Kind.(*schema_pb.Value_StringValue); ok {
		if valueValue.StringValue != "integration-value" {
			t.Errorf("Value field mismatch: expected 'integration-value', got '%s'", valueValue.StringValue)
		}
	} else {
		t.Errorf("Value field is not StringValue: %T", valueField.Kind)
	}
}
