package mq_agent_pb

import (
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestPublishRecordResponseSerialization(t *testing.T) {
	// Test that PublishRecordResponse can serialize/deserialize with new offset fields
	original := &PublishRecordResponse{
		AckSequence: 123,
		Error:       "",
		BaseOffset:  1000, // New field
		LastOffset:  1005, // New field
	}

	// Test proto marshaling/unmarshaling
	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal PublishRecordResponse: %v", err)
	}

	restored := &PublishRecordResponse{}
	err = proto.Unmarshal(data, restored)
	if err != nil {
		t.Fatalf("Failed to unmarshal PublishRecordResponse: %v", err)
	}

	// Verify all fields are preserved
	if restored.AckSequence != original.AckSequence {
		t.Errorf("AckSequence = %d, want %d", restored.AckSequence, original.AckSequence)
	}
	if restored.BaseOffset != original.BaseOffset {
		t.Errorf("BaseOffset = %d, want %d", restored.BaseOffset, original.BaseOffset)
	}
	if restored.LastOffset != original.LastOffset {
		t.Errorf("LastOffset = %d, want %d", restored.LastOffset, original.LastOffset)
	}
}

func TestSubscribeRecordResponseSerialization(t *testing.T) {
	// Test that SubscribeRecordResponse can serialize/deserialize with new offset field
	original := &SubscribeRecordResponse{
		Key:           []byte("test-key"),
		TsNs:          1234567890,
		Error:         "",
		IsEndOfStream: false,
		IsEndOfTopic:  false,
		Offset:        42, // New field
	}

	// Test proto marshaling/unmarshaling
	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal SubscribeRecordResponse: %v", err)
	}

	restored := &SubscribeRecordResponse{}
	err = proto.Unmarshal(data, restored)
	if err != nil {
		t.Fatalf("Failed to unmarshal SubscribeRecordResponse: %v", err)
	}

	// Verify all fields are preserved
	if restored.TsNs != original.TsNs {
		t.Errorf("TsNs = %d, want %d", restored.TsNs, original.TsNs)
	}
	if restored.Offset != original.Offset {
		t.Errorf("Offset = %d, want %d", restored.Offset, original.Offset)
	}
	if string(restored.Key) != string(original.Key) {
		t.Errorf("Key = %s, want %s", string(restored.Key), string(original.Key))
	}
}

func TestPublishRecordResponseBackwardCompatibility(t *testing.T) {
	// Test that PublishRecordResponse without offset fields still works
	original := &PublishRecordResponse{
		AckSequence: 123,
		Error:       "",
		// BaseOffset and LastOffset not set (defaults to 0)
	}

	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal PublishRecordResponse: %v", err)
	}

	restored := &PublishRecordResponse{}
	err = proto.Unmarshal(data, restored)
	if err != nil {
		t.Fatalf("Failed to unmarshal PublishRecordResponse: %v", err)
	}

	// Offset fields should default to 0
	if restored.BaseOffset != 0 {
		t.Errorf("BaseOffset = %d, want 0", restored.BaseOffset)
	}
	if restored.LastOffset != 0 {
		t.Errorf("LastOffset = %d, want 0", restored.LastOffset)
	}
}
