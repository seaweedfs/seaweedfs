package offset

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestConsumerGroupPosition_JSON(t *testing.T) {
	tests := []struct {
		name     string
		position *ConsumerGroupPosition
	}{
		{
			name: "offset-based position",
			position: &ConsumerGroupPosition{
				Type:        "offset",
				Value:       12345,
				OffsetType:  schema_pb.OffsetType_EXACT_OFFSET.String(),
				CommittedAt: time.Now().UnixMilli(),
				Metadata:    "test metadata",
			},
		},
		{
			name: "timestamp-based position",
			position: &ConsumerGroupPosition{
				Type:        "timestamp",
				Value:       time.Now().UnixNano(),
				OffsetType:  schema_pb.OffsetType_EXACT_TS_NS.String(),
				CommittedAt: time.Now().UnixMilli(),
				Metadata:    "checkpoint at 2024-10-05",
			},
		},
		{
			name: "minimal position",
			position: &ConsumerGroupPosition{
				Type:  "offset",
				Value: 42,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to JSON
			jsonBytes, err := json.Marshal(tt.position)
			if err != nil {
				t.Fatalf("Failed to marshal: %v", err)
			}

			t.Logf("JSON: %s", string(jsonBytes))

			// Unmarshal from JSON
			var decoded ConsumerGroupPosition
			if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
				t.Fatalf("Failed to unmarshal: %v", err)
			}

			// Verify fields
			if decoded.Type != tt.position.Type {
				t.Errorf("Type mismatch: got %s, want %s", decoded.Type, tt.position.Type)
			}
			if decoded.Value != tt.position.Value {
				t.Errorf("Value mismatch: got %d, want %d", decoded.Value, tt.position.Value)
			}
			if decoded.OffsetType != tt.position.OffsetType {
				t.Errorf("OffsetType mismatch: got %s, want %s", decoded.OffsetType, tt.position.OffsetType)
			}
			if decoded.Metadata != tt.position.Metadata {
				t.Errorf("Metadata mismatch: got %s, want %s", decoded.Metadata, tt.position.Metadata)
			}
		})
	}
}

func TestConsumerGroupPosition_JSONExamples(t *testing.T) {
	// Test JSON format examples
	jsonExamples := []string{
		`{"type":"offset","value":12345}`,
		`{"type":"timestamp","value":1696521600000000000}`,
		`{"type":"offset","value":42,"offset_type":"EXACT_OFFSET","committed_at":1696521600000,"metadata":"test"}`,
	}

	for i, jsonStr := range jsonExamples {
		var position ConsumerGroupPosition
		if err := json.Unmarshal([]byte(jsonStr), &position); err != nil {
			t.Errorf("Example %d: Failed to parse JSON: %v", i, err)
			continue
		}

		t.Logf("Example %d: Type=%s, Value=%d", i, position.Type, position.Value)

		// Verify required fields
		if position.Type == "" {
			t.Errorf("Example %d: Type is empty", i)
		}
		if position.Value == 0 {
			t.Errorf("Example %d: Value is zero", i)
		}
	}
}

func TestConsumerGroupPosition_TypeValidation(t *testing.T) {
	validTypes := []string{"offset", "timestamp"}

	for _, typ := range validTypes {
		position := &ConsumerGroupPosition{
			Type:  typ,
			Value: 100,
		}

		jsonBytes, err := json.Marshal(position)
		if err != nil {
			t.Fatalf("Failed to marshal position with type '%s': %v", typ, err)
		}

		var decoded ConsumerGroupPosition
		if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
			t.Fatalf("Failed to unmarshal position with type '%s': %v", typ, err)
		}

		if decoded.Type != typ {
			t.Errorf("Type mismatch: got '%s', want '%s'", decoded.Type, typ)
		}
	}
}
