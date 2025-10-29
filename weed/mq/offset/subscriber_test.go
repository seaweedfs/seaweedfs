package offset

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestOffsetSubscriber_CreateSubscription(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Assign some offsets first
	registry.AssignOffsets("test-namespace", "test-topic", partition, 10)

	// Test EXACT_OFFSET subscription
	sub, err := subscriber.CreateSubscription("test-sub-1", "test-namespace", "test-topic", partition, schema_pb.OffsetType_EXACT_OFFSET, 5)
	if err != nil {
		t.Fatalf("Failed to create EXACT_OFFSET subscription: %v", err)
	}

	if sub.StartOffset != 5 {
		t.Errorf("Expected start offset 5, got %d", sub.StartOffset)
	}
	if sub.CurrentOffset != 5 {
		t.Errorf("Expected current offset 5, got %d", sub.CurrentOffset)
	}

	// Test RESET_TO_LATEST subscription
	sub2, err := subscriber.CreateSubscription("test-sub-2", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_LATEST, 0)
	if err != nil {
		t.Fatalf("Failed to create RESET_TO_LATEST subscription: %v", err)
	}

	if sub2.StartOffset != 10 { // Should be at high water mark
		t.Errorf("Expected start offset 10, got %d", sub2.StartOffset)
	}
}

func TestOffsetSubscriber_InvalidSubscription(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Assign some offsets
	registry.AssignOffsets("test-namespace", "test-topic", partition, 5)

	// Test invalid offset (beyond high water mark)
	_, err := subscriber.CreateSubscription("invalid-sub", "test-namespace", "test-topic", partition, schema_pb.OffsetType_EXACT_OFFSET, 10)
	if err == nil {
		t.Error("Expected error for offset beyond high water mark")
	}

	// Test negative offset
	_, err = subscriber.CreateSubscription("invalid-sub-2", "test-namespace", "test-topic", partition, schema_pb.OffsetType_EXACT_OFFSET, -1)
	if err == nil {
		t.Error("Expected error for negative offset")
	}
}

func TestOffsetSubscriber_DuplicateSubscription(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Create first subscription
	_, err := subscriber.CreateSubscription("duplicate-sub", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)
	if err != nil {
		t.Fatalf("Failed to create first subscription: %v", err)
	}

	// Try to create duplicate
	_, err = subscriber.CreateSubscription("duplicate-sub", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)
	if err == nil {
		t.Error("Expected error for duplicate subscription ID")
	}
}

func TestOffsetSubscription_SeekToOffset(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Assign offsets
	registry.AssignOffsets("test-namespace", "test-topic", partition, 20)

	// Create subscription
	sub, err := subscriber.CreateSubscription("seek-test", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Test valid seek
	err = sub.SeekToOffset(10)
	if err != nil {
		t.Fatalf("Failed to seek to offset 10: %v", err)
	}

	if sub.CurrentOffset != 10 {
		t.Errorf("Expected current offset 10, got %d", sub.CurrentOffset)
	}

	// Test invalid seek (beyond high water mark)
	err = sub.SeekToOffset(25)
	if err == nil {
		t.Error("Expected error for seek beyond high water mark")
	}

	// Test negative seek
	err = sub.SeekToOffset(-1)
	if err == nil {
		t.Error("Expected error for negative seek offset")
	}
}

func TestOffsetSubscription_AdvanceOffset(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Create subscription
	sub, err := subscriber.CreateSubscription("advance-test", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Test single advance
	initialOffset := sub.GetNextOffset()
	sub.AdvanceOffset()

	if sub.GetNextOffset() != initialOffset+1 {
		t.Errorf("Expected offset %d, got %d", initialOffset+1, sub.GetNextOffset())
	}

	// Test batch advance
	sub.AdvanceOffsetBy(5)

	if sub.GetNextOffset() != initialOffset+6 {
		t.Errorf("Expected offset %d, got %d", initialOffset+6, sub.GetNextOffset())
	}
}

func TestOffsetSubscription_GetLag(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Assign offsets
	registry.AssignOffsets("test-namespace", "test-topic", partition, 15)

	// Create subscription at offset 5
	sub, err := subscriber.CreateSubscription("lag-test", "test-namespace", "test-topic", partition, schema_pb.OffsetType_EXACT_OFFSET, 5)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Check initial lag
	lag, err := sub.GetLag()
	if err != nil {
		t.Fatalf("Failed to get lag: %v", err)
	}

	expectedLag := int64(15 - 5) // hwm - current
	if lag != expectedLag {
		t.Errorf("Expected lag %d, got %d", expectedLag, lag)
	}

	// Advance and check lag again
	sub.AdvanceOffsetBy(3)

	lag, err = sub.GetLag()
	if err != nil {
		t.Fatalf("Failed to get lag after advance: %v", err)
	}

	expectedLag = int64(15 - 8) // hwm - current
	if lag != expectedLag {
		t.Errorf("Expected lag %d after advance, got %d", expectedLag, lag)
	}
}

func TestOffsetSubscription_IsAtEnd(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Assign offsets
	registry.AssignOffsets("test-namespace", "test-topic", partition, 10)

	// Create subscription at end
	sub, err := subscriber.CreateSubscription("end-test", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_LATEST, 0)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Should be at end
	atEnd, err := sub.IsAtEnd()
	if err != nil {
		t.Fatalf("Failed to check if at end: %v", err)
	}

	if !atEnd {
		t.Error("Expected subscription to be at end")
	}

	// Seek to middle and check again
	sub.SeekToOffset(5)

	atEnd, err = sub.IsAtEnd()
	if err != nil {
		t.Fatalf("Failed to check if at end after seek: %v", err)
	}

	if atEnd {
		t.Error("Expected subscription not to be at end after seek")
	}
}

func TestOffsetSubscription_GetOffsetRange(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Assign offsets
	registry.AssignOffsets("test-namespace", "test-topic", partition, 20)

	// Create subscription
	sub, err := subscriber.CreateSubscription("range-test", "test-namespace", "test-topic", partition, schema_pb.OffsetType_EXACT_OFFSET, 5)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Test normal range
	offsetRange, err := sub.GetOffsetRange(10)
	if err != nil {
		t.Fatalf("Failed to get offset range: %v", err)
	}

	if offsetRange.StartOffset != 5 {
		t.Errorf("Expected start offset 5, got %d", offsetRange.StartOffset)
	}
	if offsetRange.EndOffset != 14 {
		t.Errorf("Expected end offset 14, got %d", offsetRange.EndOffset)
	}
	if offsetRange.Count != 10 {
		t.Errorf("Expected count 10, got %d", offsetRange.Count)
	}

	// Test range that exceeds high water mark
	sub.SeekToOffset(15)
	offsetRange, err = sub.GetOffsetRange(10)
	if err != nil {
		t.Fatalf("Failed to get offset range near end: %v", err)
	}

	if offsetRange.StartOffset != 15 {
		t.Errorf("Expected start offset 15, got %d", offsetRange.StartOffset)
	}
	if offsetRange.EndOffset != 19 { // Should be capped at hwm-1
		t.Errorf("Expected end offset 19, got %d", offsetRange.EndOffset)
	}
	if offsetRange.Count != 5 {
		t.Errorf("Expected count 5, got %d", offsetRange.Count)
	}
}

func TestOffsetSubscription_EmptyRange(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Assign offsets
	registry.AssignOffsets("test-namespace", "test-topic", partition, 10)

	// Create subscription at end
	sub, err := subscriber.CreateSubscription("empty-range-test", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_LATEST, 0)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Request range when at end
	offsetRange, err := sub.GetOffsetRange(5)
	if err != nil {
		t.Fatalf("Failed to get offset range at end: %v", err)
	}

	if offsetRange.Count != 0 {
		t.Errorf("Expected empty range (count 0), got count %d", offsetRange.Count)
	}

	if offsetRange.StartOffset != 10 {
		t.Errorf("Expected start offset 10, got %d", offsetRange.StartOffset)
	}

	if offsetRange.EndOffset != 9 { // Empty range: end < start
		t.Errorf("Expected end offset 9 (empty range), got %d", offsetRange.EndOffset)
	}
}

func TestOffsetSeeker_ValidateOffsetRange(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	seeker := NewOffsetSeeker(registry)
	partition := createTestPartition()

	// Assign offsets
	registry.AssignOffsets("test-namespace", "test-topic", partition, 15)

	// Test valid range
	err := seeker.ValidateOffsetRange("test-namespace", "test-topic", partition, 5, 10)
	if err != nil {
		t.Errorf("Valid range should not return error: %v", err)
	}

	// Test invalid ranges
	testCases := []struct {
		name        string
		startOffset int64
		endOffset   int64
		expectError bool
	}{
		{"negative start", -1, 5, true},
		{"end before start", 10, 5, true},
		{"start beyond hwm", 20, 25, true},
		{"valid range", 0, 14, false},
		{"single offset", 5, 5, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := seeker.ValidateOffsetRange("test-namespace", "test-topic", partition, tc.startOffset, tc.endOffset)
			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestOffsetSeeker_GetAvailableOffsetRange(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	seeker := NewOffsetSeeker(registry)
	partition := createTestPartition()

	// Test empty partition
	offsetRange, err := seeker.GetAvailableOffsetRange("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get available range for empty partition: %v", err)
	}

	if offsetRange.Count != 0 {
		t.Errorf("Expected empty range for empty partition, got count %d", offsetRange.Count)
	}

	// Assign offsets and test again
	registry.AssignOffsets("test-namespace", "test-topic", partition, 25)

	offsetRange, err = seeker.GetAvailableOffsetRange("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get available range: %v", err)
	}

	if offsetRange.StartOffset != 0 {
		t.Errorf("Expected start offset 0, got %d", offsetRange.StartOffset)
	}
	if offsetRange.EndOffset != 24 {
		t.Errorf("Expected end offset 24, got %d", offsetRange.EndOffset)
	}
	if offsetRange.Count != 25 {
		t.Errorf("Expected count 25, got %d", offsetRange.Count)
	}
}

func TestOffsetSubscriber_CloseSubscription(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Create subscription
	sub, err := subscriber.CreateSubscription("close-test", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Verify subscription exists
	_, err = subscriber.GetSubscription("close-test")
	if err != nil {
		t.Fatalf("Subscription should exist: %v", err)
	}

	// Close subscription
	err = subscriber.CloseSubscription("close-test")
	if err != nil {
		t.Fatalf("Failed to close subscription: %v", err)
	}

	// Verify subscription is gone
	_, err = subscriber.GetSubscription("close-test")
	if err == nil {
		t.Error("Subscription should not exist after close")
	}

	// Verify subscription is marked inactive
	if sub.IsActive {
		t.Error("Subscription should be marked inactive after close")
	}
}

func TestOffsetSubscription_InactiveOperations(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	subscriber := NewOffsetSubscriber(registry)
	partition := createTestPartition()

	// Create and close subscription
	sub, err := subscriber.CreateSubscription("inactive-test", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	subscriber.CloseSubscription("inactive-test")

	// Test operations on inactive subscription
	err = sub.SeekToOffset(5)
	if err == nil {
		t.Error("Expected error for seek on inactive subscription")
	}

	_, err = sub.GetLag()
	if err == nil {
		t.Error("Expected error for GetLag on inactive subscription")
	}

	_, err = sub.IsAtEnd()
	if err == nil {
		t.Error("Expected error for IsAtEnd on inactive subscription")
	}

	_, err = sub.GetOffsetRange(10)
	if err == nil {
		t.Error("Expected error for GetOffsetRange on inactive subscription")
	}
}
