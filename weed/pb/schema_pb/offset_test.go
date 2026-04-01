package schema_pb

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestOffsetTypeEnums(t *testing.T) {
	// Test that new offset-based enum values are defined
	tests := []struct {
		name     string
		value    OffsetType
		expected int32
	}{
		{"EXACT_OFFSET", OffsetType_EXACT_OFFSET, 25},
		{"RESET_TO_OFFSET", OffsetType_RESET_TO_OFFSET, 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int32(tt.value) != tt.expected {
				t.Errorf("OffsetType_%s = %d, want %d", tt.name, int32(tt.value), tt.expected)
			}
		})
	}
}

func TestPartitionOffsetSerialization(t *testing.T) {
	// Test that PartitionOffset can serialize/deserialize with new offset field
	original := &PartitionOffset{
		Partition: &Partition{
			RingSize:   1024,
			RangeStart: 0,
			RangeStop:  31,
			UnixTimeNs: 1234567890,
		},
		StartTsNs:   1234567890,
		StartOffset: 42, // New field
	}

	// Test proto marshaling/unmarshaling
	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal PartitionOffset: %v", err)
	}

	restored := &PartitionOffset{}
	err = proto.Unmarshal(data, restored)
	if err != nil {
		t.Fatalf("Failed to unmarshal PartitionOffset: %v", err)
	}

	// Verify all fields are preserved
	if restored.StartTsNs != original.StartTsNs {
		t.Errorf("StartTsNs = %d, want %d", restored.StartTsNs, original.StartTsNs)
	}
	if restored.StartOffset != original.StartOffset {
		t.Errorf("StartOffset = %d, want %d", restored.StartOffset, original.StartOffset)
	}
	if restored.Partition.RingSize != original.Partition.RingSize {
		t.Errorf("Partition.RingSize = %d, want %d", restored.Partition.RingSize, original.Partition.RingSize)
	}
}

func TestPartitionOffsetBackwardCompatibility(t *testing.T) {
	// Test that PartitionOffset without StartOffset still works
	original := &PartitionOffset{
		Partition: &Partition{
			RingSize:   1024,
			RangeStart: 0,
			RangeStop:  31,
			UnixTimeNs: 1234567890,
		},
		StartTsNs: 1234567890,
		// StartOffset not set (defaults to 0)
	}

	data, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("Failed to marshal PartitionOffset: %v", err)
	}

	restored := &PartitionOffset{}
	err = proto.Unmarshal(data, restored)
	if err != nil {
		t.Fatalf("Failed to unmarshal PartitionOffset: %v", err)
	}

	// StartOffset should default to 0
	if restored.StartOffset != 0 {
		t.Errorf("StartOffset = %d, want 0", restored.StartOffset)
	}
}
