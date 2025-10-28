package broker

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/topic"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func createTestTopic() topic.Topic {
	return topic.Topic{
		Namespace: "test",
		Name:      "offset-test",
	}
}

func createTestPartition() topic.Partition {
	return topic.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}
}

func TestBrokerOffsetManager_AssignOffset(t *testing.T) {
	storage := NewInMemoryOffsetStorageForTesting()
	manager := NewBrokerOffsetManagerWithStorage(storage)
	testTopic := createTestTopic()
	testPartition := createTestPartition()

	// Test sequential offset assignment
	for i := int64(0); i < 10; i++ {
		assignedOffset, err := manager.AssignOffset(testTopic, testPartition)
		if err != nil {
			t.Fatalf("Failed to assign offset %d: %v", i, err)
		}

		if assignedOffset != i {
			t.Errorf("Expected offset %d, got %d", i, assignedOffset)
		}
	}
}

func TestBrokerOffsetManager_AssignBatchOffsets(t *testing.T) {
	storage := NewInMemoryOffsetStorageForTesting()
	manager := NewBrokerOffsetManagerWithStorage(storage)
	testTopic := createTestTopic()
	testPartition := createTestPartition()

	// Assign batch of offsets
	baseOffset, lastOffset, err := manager.AssignBatchOffsets(testTopic, testPartition, 5)
	if err != nil {
		t.Fatalf("Failed to assign batch offsets: %v", err)
	}

	if baseOffset != 0 {
		t.Errorf("Expected base offset 0, got %d", baseOffset)
	}

	if lastOffset != 4 {
		t.Errorf("Expected last offset 4, got %d", lastOffset)
	}

	// Assign another batch
	baseOffset2, lastOffset2, err := manager.AssignBatchOffsets(testTopic, testPartition, 3)
	if err != nil {
		t.Fatalf("Failed to assign second batch offsets: %v", err)
	}

	if baseOffset2 != 5 {
		t.Errorf("Expected base offset 5, got %d", baseOffset2)
	}

	if lastOffset2 != 7 {
		t.Errorf("Expected last offset 7, got %d", lastOffset2)
	}
}

func TestBrokerOffsetManager_GetHighWaterMark(t *testing.T) {
	storage := NewInMemoryOffsetStorageForTesting()
	manager := NewBrokerOffsetManagerWithStorage(storage)
	testTopic := createTestTopic()
	testPartition := createTestPartition()

	// Initially should be 0
	hwm, err := manager.GetHighWaterMark(testTopic, testPartition)
	if err != nil {
		t.Fatalf("Failed to get initial high water mark: %v", err)
	}

	if hwm != 0 {
		t.Errorf("Expected initial high water mark 0, got %d", hwm)
	}

	// Assign some offsets
	manager.AssignBatchOffsets(testTopic, testPartition, 10)

	// High water mark should be updated
	hwm, err = manager.GetHighWaterMark(testTopic, testPartition)
	if err != nil {
		t.Fatalf("Failed to get high water mark after assignment: %v", err)
	}

	if hwm != 10 {
		t.Errorf("Expected high water mark 10, got %d", hwm)
	}
}

func TestBrokerOffsetManager_CreateSubscription(t *testing.T) {
	storage := NewInMemoryOffsetStorageForTesting()
	manager := NewBrokerOffsetManagerWithStorage(storage)
	testTopic := createTestTopic()
	testPartition := createTestPartition()

	// Assign some offsets first
	manager.AssignBatchOffsets(testTopic, testPartition, 5)

	// Create subscription
	sub, err := manager.CreateSubscription(
		"test-sub",
		testTopic,
		testPartition,
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		0,
	)

	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	if sub.ID != "test-sub" {
		t.Errorf("Expected subscription ID 'test-sub', got %s", sub.ID)
	}

	if sub.StartOffset != 0 {
		t.Errorf("Expected start offset 0, got %d", sub.StartOffset)
	}
}

func TestBrokerOffsetManager_GetPartitionOffsetInfo(t *testing.T) {
	storage := NewInMemoryOffsetStorageForTesting()
	manager := NewBrokerOffsetManagerWithStorage(storage)
	testTopic := createTestTopic()
	testPartition := createTestPartition()

	// Test empty partition
	info, err := manager.GetPartitionOffsetInfo(testTopic, testPartition)
	if err != nil {
		t.Fatalf("Failed to get partition offset info: %v", err)
	}

	if info.EarliestOffset != 0 {
		t.Errorf("Expected earliest offset 0, got %d", info.EarliestOffset)
	}

	if info.LatestOffset != -1 {
		t.Errorf("Expected latest offset -1 for empty partition, got %d", info.LatestOffset)
	}

	// Assign offsets and test again
	manager.AssignBatchOffsets(testTopic, testPartition, 5)

	info, err = manager.GetPartitionOffsetInfo(testTopic, testPartition)
	if err != nil {
		t.Fatalf("Failed to get partition offset info after assignment: %v", err)
	}

	if info.LatestOffset != 4 {
		t.Errorf("Expected latest offset 4, got %d", info.LatestOffset)
	}

	if info.HighWaterMark != 5 {
		t.Errorf("Expected high water mark 5, got %d", info.HighWaterMark)
	}
}

func TestBrokerOffsetManager_MultiplePartitions(t *testing.T) {
	storage := NewInMemoryOffsetStorageForTesting()
	manager := NewBrokerOffsetManagerWithStorage(storage)
	testTopic := createTestTopic()

	// Create different partitions
	partition1 := topic.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	partition2 := topic.Partition{
		RingSize:   1024,
		RangeStart: 32,
		RangeStop:  63,
		UnixTimeNs: time.Now().UnixNano(),
	}

	// Assign offsets to different partitions
	assignedOffset1, err := manager.AssignOffset(testTopic, partition1)
	if err != nil {
		t.Fatalf("Failed to assign offset to partition1: %v", err)
	}

	assignedOffset2, err := manager.AssignOffset(testTopic, partition2)
	if err != nil {
		t.Fatalf("Failed to assign offset to partition2: %v", err)
	}

	// Both should start at 0
	if assignedOffset1 != 0 {
		t.Errorf("Expected offset 0 for partition1, got %d", assignedOffset1)
	}

	if assignedOffset2 != 0 {
		t.Errorf("Expected offset 0 for partition2, got %d", assignedOffset2)
	}

	// Assign more offsets to partition1
	assignedOffset1_2, err := manager.AssignOffset(testTopic, partition1)
	if err != nil {
		t.Fatalf("Failed to assign second offset to partition1: %v", err)
	}

	if assignedOffset1_2 != 1 {
		t.Errorf("Expected offset 1 for partition1, got %d", assignedOffset1_2)
	}

	// Partition2 should still be at 0 for next assignment
	assignedOffset2_2, err := manager.AssignOffset(testTopic, partition2)
	if err != nil {
		t.Fatalf("Failed to assign second offset to partition2: %v", err)
	}

	if assignedOffset2_2 != 1 {
		t.Errorf("Expected offset 1 for partition2, got %d", assignedOffset2_2)
	}
}

func TestOffsetAwarePublisher(t *testing.T) {
	storage := NewInMemoryOffsetStorageForTesting()
	manager := NewBrokerOffsetManagerWithStorage(storage)
	testTopic := createTestTopic()
	testPartition := createTestPartition()

	// Create a mock local partition (simplified for testing)
	localPartition := &topic.LocalPartition{}

	// Create offset assignment function
	assignOffsetFn := func() (int64, error) {
		return manager.AssignOffset(testTopic, testPartition)
	}

	// Create offset-aware publisher
	publisher := topic.NewOffsetAwarePublisher(localPartition, assignOffsetFn)

	if publisher.GetPartition() != localPartition {
		t.Error("Publisher should return the correct partition")
	}

	// Test would require more setup to actually publish messages
	// This tests the basic structure
}

func TestBrokerOffsetManager_GetOffsetMetrics(t *testing.T) {
	storage := NewInMemoryOffsetStorageForTesting()
	manager := NewBrokerOffsetManagerWithStorage(storage)
	testTopic := createTestTopic()
	testPartition := createTestPartition()

	// Initial metrics
	metrics := manager.GetOffsetMetrics()
	if metrics.TotalOffsets != 0 {
		t.Errorf("Expected 0 total offsets initially, got %d", metrics.TotalOffsets)
	}

	// Assign some offsets
	manager.AssignBatchOffsets(testTopic, testPartition, 5)

	// Create subscription
	manager.CreateSubscription("test-sub", testTopic, testPartition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)

	// Check updated metrics
	metrics = manager.GetOffsetMetrics()
	if metrics.PartitionCount != 1 {
		t.Errorf("Expected 1 partition, got %d", metrics.PartitionCount)
	}
}

func TestBrokerOffsetManager_AssignOffsetsWithResult(t *testing.T) {
	storage := NewInMemoryOffsetStorageForTesting()
	manager := NewBrokerOffsetManagerWithStorage(storage)
	testTopic := createTestTopic()
	testPartition := createTestPartition()

	// Assign offsets with result
	result := manager.AssignOffsetsWithResult(testTopic, testPartition, 3)

	if result.Error != nil {
		t.Fatalf("Expected no error, got: %v", result.Error)
	}

	if result.BaseOffset != 0 {
		t.Errorf("Expected base offset 0, got %d", result.BaseOffset)
	}

	if result.LastOffset != 2 {
		t.Errorf("Expected last offset 2, got %d", result.LastOffset)
	}

	if result.Count != 3 {
		t.Errorf("Expected count 3, got %d", result.Count)
	}

	if result.Topic != testTopic {
		t.Error("Topic mismatch in result")
	}

	if result.Partition != testPartition {
		t.Error("Partition mismatch in result")
	}

	if result.Timestamp <= 0 {
		t.Error("Timestamp should be set")
	}
}

func TestBrokerOffsetManager_Shutdown(t *testing.T) {
	storage := NewInMemoryOffsetStorageForTesting()
	manager := NewBrokerOffsetManagerWithStorage(storage)
	testTopic := createTestTopic()
	testPartition := createTestPartition()

	// Assign some offsets and create subscriptions
	manager.AssignBatchOffsets(testTopic, testPartition, 5)
	manager.CreateSubscription("test-sub", testTopic, testPartition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)

	// Shutdown should not panic
	manager.Shutdown()

	// After shutdown, operations should still work (using new managers)
	offset, err := manager.AssignOffset(testTopic, testPartition)
	if err != nil {
		t.Fatalf("Operations should still work after shutdown: %v", err)
	}

	// Should start from 0 again (new manager)
	if offset != 0 {
		t.Errorf("Expected offset 0 after shutdown, got %d", offset)
	}
}
