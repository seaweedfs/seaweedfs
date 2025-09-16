package broker

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/offset"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

func TestConvertOffsetToMessagePosition(t *testing.T) {
	broker := &MessageQueueBroker{}

	tests := []struct {
		name          string
		offsetType    schema_pb.OffsetType
		currentOffset int64
		expectedBatch int64
		expectError   bool
	}{
		{
			name:          "reset to earliest",
			offsetType:    schema_pb.OffsetType_RESET_TO_EARLIEST,
			currentOffset: 0,
			expectedBatch: -3,
			expectError:   false,
		},
		{
			name:          "reset to latest",
			offsetType:    schema_pb.OffsetType_RESET_TO_LATEST,
			currentOffset: 0,
			expectedBatch: -4,
			expectError:   false,
		},
		{
			name:          "exact offset zero",
			offsetType:    schema_pb.OffsetType_EXACT_OFFSET,
			currentOffset: 0,
			expectedBatch: -2,
			expectError:   false,
		},
		{
			name:          "exact offset non-zero",
			offsetType:    schema_pb.OffsetType_EXACT_OFFSET,
			currentOffset: 100,
			expectedBatch: -2,
			expectError:   false,
		},
		{
			name:          "exact timestamp",
			offsetType:    schema_pb.OffsetType_EXACT_TS_NS,
			currentOffset: 50,
			expectedBatch: -2,
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock subscription
			subscription := &offset.OffsetSubscription{
				ID:            "test-subscription",
				CurrentOffset: tt.currentOffset,
				OffsetType:    tt.offsetType,
				IsActive:      true,
			}

			position, err := broker.convertOffsetToMessagePosition(subscription)

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if position.BatchIndex != tt.expectedBatch {
				t.Errorf("Expected batch index %d, got %d", tt.expectedBatch, position.BatchIndex)
			}

			// Verify that the timestamp is reasonable (not zero for most cases)
			if tt.offsetType != schema_pb.OffsetType_RESET_TO_EARLIEST && position.Time.IsZero() {
				t.Error("Expected non-zero timestamp")
			}

			t.Logf("Offset %d (type %v) -> Position: time=%v, batch=%d",
				tt.currentOffset, tt.offsetType, position.Time, position.BatchIndex)
		})
	}
}

func TestConvertOffsetToMessagePosition_TimestampProgression(t *testing.T) {
	broker := &MessageQueueBroker{}

	// Test that higher offsets result in more recent timestamps (for the approximation)
	subscription1 := &offset.OffsetSubscription{
		ID:            "test-1",
		CurrentOffset: 10,
		OffsetType:    schema_pb.OffsetType_EXACT_OFFSET,
		IsActive:      true,
	}

	subscription2 := &offset.OffsetSubscription{
		ID:            "test-2",
		CurrentOffset: 100,
		OffsetType:    schema_pb.OffsetType_EXACT_OFFSET,
		IsActive:      true,
	}

	pos1, err1 := broker.convertOffsetToMessagePosition(subscription1)
	pos2, err2 := broker.convertOffsetToMessagePosition(subscription2)

	if err1 != nil || err2 != nil {
		t.Fatalf("Unexpected errors: %v, %v", err1, err2)
	}

	// For the current approximation, higher offsets should result in older timestamps
	// (since we subtract offset * millisecond from current time)
	if !pos1.Time.After(pos2.Time) {
		t.Errorf("Expected offset 10 to have more recent timestamp than offset 100")
		t.Logf("Offset 10: %v", pos1.Time)
		t.Logf("Offset 100: %v", pos2.Time)
	}
}

func TestConvertOffsetToMessagePosition_ConsistentResults(t *testing.T) {
	broker := &MessageQueueBroker{}

	subscription := &offset.OffsetSubscription{
		ID:            "consistent-test",
		CurrentOffset: 42,
		OffsetType:    schema_pb.OffsetType_EXACT_OFFSET,
		IsActive:      true,
	}

	// Call multiple times within a short period
	positions := make([]log_buffer.MessagePosition, 5)
	for i := 0; i < 5; i++ {
		pos, err := broker.convertOffsetToMessagePosition(subscription)
		if err != nil {
			t.Fatalf("Unexpected error on iteration %d: %v", i, err)
		}
		positions[i] = pos
		time.Sleep(1 * time.Millisecond) // Small delay
	}

	// All positions should have the same BatchIndex
	for i := 1; i < len(positions); i++ {
		if positions[i].BatchIndex != positions[0].BatchIndex {
			t.Errorf("Inconsistent BatchIndex: %d vs %d", positions[0].BatchIndex, positions[i].BatchIndex)
		}
	}

	// Timestamps should be very close (within a reasonable range)
	maxDiff := 100 * time.Millisecond
	for i := 1; i < len(positions); i++ {
		diff := positions[i].Time.Sub(positions[0].Time)
		if diff < 0 {
			diff = -diff
		}
		if diff > maxDiff {
			t.Errorf("Timestamp difference too large: %v", diff)
		}
	}

	t.Logf("Consistent results for offset 42: batch=%d, time range=%v",
		positions[0].BatchIndex, positions[len(positions)-1].Time.Sub(positions[0].Time))
}
