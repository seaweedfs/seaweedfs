package offset

import (
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

// TestEndToEndOffsetFlow tests the complete offset management flow
func TestEndToEndOffsetFlow(t *testing.T) {
	// Create temporary database
	tmpFile, err := os.CreateTemp("", "e2e_offset_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp database: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	// Create database with migrations
	db, err := CreateDatabase(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	// Create SQL storage
	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	// Create SMQ offset integration
	integration := NewSMQOffsetIntegration(storage)

	// Test partition
	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	t.Run("PublishAndAssignOffsets", func(t *testing.T) {
		// Simulate publishing messages with offset assignment
		records := []PublishRecordRequest{
			{Key: []byte("user1"), Value: &schema_pb.RecordValue{}},
			{Key: []byte("user2"), Value: &schema_pb.RecordValue{}},
			{Key: []byte("user3"), Value: &schema_pb.RecordValue{}},
		}

		response, err := integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)
		if err != nil {
			t.Fatalf("Failed to publish record batch: %v", err)
		}

		if response.BaseOffset != 0 {
			t.Errorf("Expected base offset 0, got %d", response.BaseOffset)
		}

		if response.LastOffset != 2 {
			t.Errorf("Expected last offset 2, got %d", response.LastOffset)
		}

		// Verify high water mark
		hwm, err := integration.GetHighWaterMark("test-namespace", "test-topic", partition)
		if err != nil {
			t.Fatalf("Failed to get high water mark: %v", err)
		}

		if hwm != 3 {
			t.Errorf("Expected high water mark 3, got %d", hwm)
		}
	})

	t.Run("CreateAndUseSubscription", func(t *testing.T) {
		// Create subscription from earliest
		sub, err := integration.CreateSubscription(
			"e2e-test-sub",
			"test-namespace", "test-topic",
			partition,
			schema_pb.OffsetType_RESET_TO_EARLIEST,
			0,
		)
		if err != nil {
			t.Fatalf("Failed to create subscription: %v", err)
		}

		// Subscribe to records
		responses, err := integration.SubscribeRecords(sub, 2)
		if err != nil {
			t.Fatalf("Failed to subscribe to records: %v", err)
		}

		if len(responses) != 2 {
			t.Errorf("Expected 2 responses, got %d", len(responses))
		}

		// Check subscription advancement
		if sub.CurrentOffset != 2 {
			t.Errorf("Expected current offset 2, got %d", sub.CurrentOffset)
		}

		// Get subscription lag
		lag, err := sub.GetLag()
		if err != nil {
			t.Fatalf("Failed to get lag: %v", err)
		}

		if lag != 1 { // 3 (hwm) - 2 (current) = 1
			t.Errorf("Expected lag 1, got %d", lag)
		}
	})

	t.Run("OffsetSeekingAndRanges", func(t *testing.T) {
		// Create subscription at specific offset
		sub, err := integration.CreateSubscription(
			"seek-test-sub",
			"test-namespace", "test-topic",
			partition,
			schema_pb.OffsetType_EXACT_OFFSET,
			1,
		)
		if err != nil {
			t.Fatalf("Failed to create subscription at offset 1: %v", err)
		}

		// Verify starting position
		if sub.CurrentOffset != 1 {
			t.Errorf("Expected current offset 1, got %d", sub.CurrentOffset)
		}

		// Get offset range
		offsetRange, err := sub.GetOffsetRange(2)
		if err != nil {
			t.Fatalf("Failed to get offset range: %v", err)
		}

		if offsetRange.StartOffset != 1 {
			t.Errorf("Expected start offset 1, got %d", offsetRange.StartOffset)
		}

		if offsetRange.Count != 2 {
			t.Errorf("Expected count 2, got %d", offsetRange.Count)
		}

		// Seek to different offset
		err = sub.SeekToOffset(0)
		if err != nil {
			t.Fatalf("Failed to seek to offset 0: %v", err)
		}

		if sub.CurrentOffset != 0 {
			t.Errorf("Expected current offset 0 after seek, got %d", sub.CurrentOffset)
		}
	})

	t.Run("PartitionInformationAndMetrics", func(t *testing.T) {
		// Get partition offset info
		info, err := integration.GetPartitionOffsetInfo("test-namespace", "test-topic", partition)
		if err != nil {
			t.Fatalf("Failed to get partition offset info: %v", err)
		}

		if info.EarliestOffset != 0 {
			t.Errorf("Expected earliest offset 0, got %d", info.EarliestOffset)
		}

		if info.LatestOffset != 2 {
			t.Errorf("Expected latest offset 2, got %d", info.LatestOffset)
		}

		if info.HighWaterMark != 3 {
			t.Errorf("Expected high water mark 3, got %d", info.HighWaterMark)
		}

		if info.ActiveSubscriptions != 2 { // Two subscriptions created above
			t.Errorf("Expected 2 active subscriptions, got %d", info.ActiveSubscriptions)
		}

		// Get offset metrics
		metrics := integration.GetOffsetMetrics()
		if metrics.PartitionCount != 1 {
			t.Errorf("Expected 1 partition, got %d", metrics.PartitionCount)
		}

		if metrics.ActiveSubscriptions != 2 {
			t.Errorf("Expected 2 active subscriptions in metrics, got %d", metrics.ActiveSubscriptions)
		}
	})
}

// TestOffsetPersistenceAcrossRestarts tests that offsets persist across system restarts
func TestOffsetPersistenceAcrossRestarts(t *testing.T) {
	// Create temporary database
	tmpFile, err := os.CreateTemp("", "persistence_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp database: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	var lastOffset int64

	// First session: Create database and assign offsets
	{
		db, err := CreateDatabase(tmpFile.Name())
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		storage, err := NewSQLOffsetStorage(db)
		if err != nil {
			t.Fatalf("Failed to create SQL storage: %v", err)
		}

		integration := NewSMQOffsetIntegration(storage)

		// Publish some records
		records := []PublishRecordRequest{
			{Key: []byte("msg1"), Value: &schema_pb.RecordValue{}},
			{Key: []byte("msg2"), Value: &schema_pb.RecordValue{}},
			{Key: []byte("msg3"), Value: &schema_pb.RecordValue{}},
		}

		response, err := integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)
		if err != nil {
			t.Fatalf("Failed to publish records: %v", err)
		}

		lastOffset = response.LastOffset

		// Close connections - Close integration first to trigger final checkpoint
		integration.Close()
		storage.Close()
		db.Close()
	}

	// Second session: Reopen database and verify persistence
	{
		db, err := CreateDatabase(tmpFile.Name())
		if err != nil {
			t.Fatalf("Failed to reopen database: %v", err)
		}
		defer db.Close()

		storage, err := NewSQLOffsetStorage(db)
		if err != nil {
			t.Fatalf("Failed to create SQL storage: %v", err)
		}
		defer storage.Close()

		integration := NewSMQOffsetIntegration(storage)

		// Verify high water mark persisted
		hwm, err := integration.GetHighWaterMark("test-namespace", "test-topic", partition)
		if err != nil {
			t.Fatalf("Failed to get high water mark after restart: %v", err)
		}

		if hwm != lastOffset+1 {
			t.Errorf("Expected high water mark %d after restart, got %d", lastOffset+1, hwm)
		}

		// Assign new offsets and verify continuity
		newResponse, err := integration.PublishRecord("test-namespace", "test-topic", partition, []byte("msg4"), &schema_pb.RecordValue{})
		if err != nil {
			t.Fatalf("Failed to publish new record after restart: %v", err)
		}

		expectedNextOffset := lastOffset + 1
		if newResponse.BaseOffset != expectedNextOffset {
			t.Errorf("Expected next offset %d after restart, got %d", expectedNextOffset, newResponse.BaseOffset)
		}
	}
}

// TestConcurrentOffsetOperations tests concurrent offset operations
func TestConcurrentOffsetOperations(t *testing.T) {
	// Create temporary database
	tmpFile, err := os.CreateTemp("", "concurrent_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp database: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := CreateDatabase(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	integration := NewSMQOffsetIntegration(storage)

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	// Concurrent publishers
	const numPublishers = 5
	const recordsPerPublisher = 10

	done := make(chan bool, numPublishers)

	for i := 0; i < numPublishers; i++ {
		go func(publisherID int) {
			defer func() { done <- true }()

			for j := 0; j < recordsPerPublisher; j++ {
				key := fmt.Sprintf("publisher-%d-msg-%d", publisherID, j)
				_, err := integration.PublishRecord("test-namespace", "test-topic", partition, []byte(key), &schema_pb.RecordValue{})
				if err != nil {
					t.Errorf("Publisher %d failed to publish message %d: %v", publisherID, j, err)
					return
				}
			}
		}(i)
	}

	// Wait for all publishers to complete
	for i := 0; i < numPublishers; i++ {
		<-done
	}

	// Verify total records
	hwm, err := integration.GetHighWaterMark("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get high water mark: %v", err)
	}

	expectedTotal := int64(numPublishers * recordsPerPublisher)
	if hwm != expectedTotal {
		t.Errorf("Expected high water mark %d, got %d", expectedTotal, hwm)
	}

	// Verify no duplicate offsets
	info, err := integration.GetPartitionOffsetInfo("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get partition info: %v", err)
	}

	if info.RecordCount != expectedTotal {
		t.Errorf("Expected record count %d, got %d", expectedTotal, info.RecordCount)
	}
}

// TestOffsetValidationAndErrorHandling tests error conditions and validation
func TestOffsetValidationAndErrorHandling(t *testing.T) {
	// Create temporary database
	tmpFile, err := os.CreateTemp("", "validation_test_*.db")
	if err != nil {
		t.Fatalf("Failed to create temp database: %v", err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	db, err := CreateDatabase(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close()

	storage, err := NewSQLOffsetStorage(db)
	if err != nil {
		t.Fatalf("Failed to create SQL storage: %v", err)
	}
	defer storage.Close()

	integration := NewSMQOffsetIntegration(storage)

	partition := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	t.Run("InvalidOffsetSubscription", func(t *testing.T) {
		// Try to create subscription with invalid offset
		_, err := integration.CreateSubscription(
			"invalid-sub",
			"test-namespace", "test-topic",
			partition,
			schema_pb.OffsetType_EXACT_OFFSET,
			100, // Beyond any existing data
		)
		if err == nil {
			t.Error("Expected error for subscription beyond high water mark")
		}
	})

	t.Run("NegativeOffsetValidation", func(t *testing.T) {
		// Try to create subscription with negative offset
		_, err := integration.CreateSubscription(
			"negative-sub",
			"test-namespace", "test-topic",
			partition,
			schema_pb.OffsetType_EXACT_OFFSET,
			-1,
		)
		if err == nil {
			t.Error("Expected error for negative offset")
		}
	})

	t.Run("DuplicateSubscriptionID", func(t *testing.T) {
		// Create first subscription
		_, err := integration.CreateSubscription(
			"duplicate-id",
			"test-namespace", "test-topic",
			partition,
			schema_pb.OffsetType_RESET_TO_EARLIEST,
			0,
		)
		if err != nil {
			t.Fatalf("Failed to create first subscription: %v", err)
		}

		// Try to create duplicate
		_, err = integration.CreateSubscription(
			"duplicate-id",
			"test-namespace", "test-topic",
			partition,
			schema_pb.OffsetType_RESET_TO_EARLIEST,
			0,
		)
		if err == nil {
			t.Error("Expected error for duplicate subscription ID")
		}
	})

	t.Run("OffsetRangeValidation", func(t *testing.T) {
		// Add some data first
		integration.PublishRecord("test-namespace", "test-topic", partition, []byte("test"), &schema_pb.RecordValue{})

		// Test invalid range validation
		err := integration.ValidateOffsetRange("test-namespace", "test-topic", partition, 5, 10) // Beyond high water mark
		if err == nil {
			t.Error("Expected error for range beyond high water mark")
		}

		err = integration.ValidateOffsetRange("test-namespace", "test-topic", partition, 10, 5) // End before start
		if err == nil {
			t.Error("Expected error for end offset before start offset")
		}

		err = integration.ValidateOffsetRange("test-namespace", "test-topic", partition, -1, 5) // Negative start
		if err == nil {
			t.Error("Expected error for negative start offset")
		}
	})
}
