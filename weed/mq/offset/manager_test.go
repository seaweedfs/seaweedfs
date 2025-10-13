package offset

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func createTestPartition() *schema_pb.Partition {
	return &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}
}

func TestPartitionOffsetManager_BasicAssignment(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	partition := createTestPartition()

	manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
	if err != nil {
		t.Fatalf("Failed to create offset manager: %v", err)
	}

	// Test sequential offset assignment
	for i := int64(0); i < 10; i++ {
		offset := manager.AssignOffset()
		if offset != i {
			t.Errorf("Expected offset %d, got %d", i, offset)
		}
	}

	// Test high water mark
	hwm := manager.GetHighWaterMark()
	if hwm != 10 {
		t.Errorf("Expected high water mark 10, got %d", hwm)
	}
}

func TestPartitionOffsetManager_BatchAssignment(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	partition := createTestPartition()

	manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
	if err != nil {
		t.Fatalf("Failed to create offset manager: %v", err)
	}

	// Assign batch of 5 offsets
	baseOffset, lastOffset := manager.AssignOffsets(5)
	if baseOffset != 0 {
		t.Errorf("Expected base offset 0, got %d", baseOffset)
	}
	if lastOffset != 4 {
		t.Errorf("Expected last offset 4, got %d", lastOffset)
	}

	// Assign another batch
	baseOffset, lastOffset = manager.AssignOffsets(3)
	if baseOffset != 5 {
		t.Errorf("Expected base offset 5, got %d", baseOffset)
	}
	if lastOffset != 7 {
		t.Errorf("Expected last offset 7, got %d", lastOffset)
	}

	// Check high water mark
	hwm := manager.GetHighWaterMark()
	if hwm != 8 {
		t.Errorf("Expected high water mark 8, got %d", hwm)
	}
}

func TestPartitionOffsetManager_Recovery(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	partition := createTestPartition()

	// Create manager and assign some offsets
	manager1, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
	if err != nil {
		t.Fatalf("Failed to create offset manager: %v", err)
	}

	// Assign offsets and simulate records
	for i := 0; i < 150; i++ { // More than checkpoint interval
		offset := manager1.AssignOffset()
		storage.AddRecord("test-namespace", "test-topic", partition, offset)
	}

	// Wait for checkpoint to complete
	time.Sleep(100 * time.Millisecond)

	// Create new manager (simulates restart)
	manager2, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
	if err != nil {
		t.Fatalf("Failed to create offset manager after recovery: %v", err)
	}

	// Next offset should continue from checkpoint + 1
	// With checkpoint interval 100, checkpoint happens at offset 100
	// So recovery should start from 101, but we assigned 150 offsets (0-149)
	// The checkpoint should be at 100, so next offset should be 101
	// But since we have records up to 149, it should recover from storage scan
	nextOffset := manager2.AssignOffset()
	if nextOffset != 150 {
		t.Errorf("Expected next offset 150 after recovery, got %d", nextOffset)
	}
}

func TestPartitionOffsetManager_RecoveryFromStorage(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	partition := createTestPartition()

	// Simulate existing records in storage without checkpoint
	for i := int64(0); i < 50; i++ {
		storage.AddRecord("test-namespace", "test-topic", partition, i)
	}

	// Create manager - should recover from storage scan
	manager, err := NewPartitionOffsetManager("test-namespace", "test-topic", partition, storage)
	if err != nil {
		t.Fatalf("Failed to create offset manager: %v", err)
	}

	// Next offset should be 50
	nextOffset := manager.AssignOffset()
	if nextOffset != 50 {
		t.Errorf("Expected next offset 50 after storage recovery, got %d", nextOffset)
	}
}

func TestPartitionOffsetRegistry_MultiplePartitions(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)

	// Create different partitions
	partition1 := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: time.Now().UnixNano(),
	}

	partition2 := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 32,
		RangeStop:  63,
		UnixTimeNs: time.Now().UnixNano(),
	}

	// Assign offsets to different partitions
	offset1, err := registry.AssignOffset("test-namespace", "test-topic", partition1)
	if err != nil {
		t.Fatalf("Failed to assign offset to partition1: %v", err)
	}
	if offset1 != 0 {
		t.Errorf("Expected offset 0 for partition1, got %d", offset1)
	}

	offset2, err := registry.AssignOffset("test-namespace", "test-topic", partition2)
	if err != nil {
		t.Fatalf("Failed to assign offset to partition2: %v", err)
	}
	if offset2 != 0 {
		t.Errorf("Expected offset 0 for partition2, got %d", offset2)
	}

	// Assign more offsets to partition1
	offset1_2, err := registry.AssignOffset("test-namespace", "test-topic", partition1)
	if err != nil {
		t.Fatalf("Failed to assign second offset to partition1: %v", err)
	}
	if offset1_2 != 1 {
		t.Errorf("Expected offset 1 for partition1, got %d", offset1_2)
	}

	// Partition2 should still be at 0 for next assignment
	offset2_2, err := registry.AssignOffset("test-namespace", "test-topic", partition2)
	if err != nil {
		t.Fatalf("Failed to assign second offset to partition2: %v", err)
	}
	if offset2_2 != 1 {
		t.Errorf("Expected offset 1 for partition2, got %d", offset2_2)
	}
}

func TestPartitionOffsetRegistry_BatchAssignment(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	partition := createTestPartition()

	// Assign batch of offsets
	baseOffset, lastOffset, err := registry.AssignOffsets("test-namespace", "test-topic", partition, 10)
	if err != nil {
		t.Fatalf("Failed to assign batch offsets: %v", err)
	}

	if baseOffset != 0 {
		t.Errorf("Expected base offset 0, got %d", baseOffset)
	}
	if lastOffset != 9 {
		t.Errorf("Expected last offset 9, got %d", lastOffset)
	}

	// Get high water mark
	hwm, err := registry.GetHighWaterMark("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get high water mark: %v", err)
	}
	if hwm != 10 {
		t.Errorf("Expected high water mark 10, got %d", hwm)
	}
}

func TestOffsetAssigner_SingleAssignment(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	assigner := NewOffsetAssigner(storage)
	partition := createTestPartition()

	// Assign single offset
	result := assigner.AssignSingleOffset("test-namespace", "test-topic", partition)
	if result.Error != nil {
		t.Fatalf("Failed to assign single offset: %v", result.Error)
	}

	if result.Assignment == nil {
		t.Fatal("Assignment result is nil")
	}

	if result.Assignment.Offset != 0 {
		t.Errorf("Expected offset 0, got %d", result.Assignment.Offset)
	}

	if result.Assignment.Partition != partition {
		t.Error("Partition mismatch in assignment")
	}

	if result.Assignment.Timestamp <= 0 {
		t.Error("Timestamp should be set")
	}
}

func TestOffsetAssigner_BatchAssignment(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	assigner := NewOffsetAssigner(storage)
	partition := createTestPartition()

	// Assign batch of offsets
	result := assigner.AssignBatchOffsets("test-namespace", "test-topic", partition, 5)
	if result.Error != nil {
		t.Fatalf("Failed to assign batch offsets: %v", result.Error)
	}

	if result.Batch == nil {
		t.Fatal("Batch result is nil")
	}

	if result.Batch.BaseOffset != 0 {
		t.Errorf("Expected base offset 0, got %d", result.Batch.BaseOffset)
	}

	if result.Batch.LastOffset != 4 {
		t.Errorf("Expected last offset 4, got %d", result.Batch.LastOffset)
	}

	if result.Batch.Count != 5 {
		t.Errorf("Expected count 5, got %d", result.Batch.Count)
	}

	if result.Batch.Timestamp <= 0 {
		t.Error("Timestamp should be set")
	}
}

func TestOffsetAssigner_HighWaterMark(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	assigner := NewOffsetAssigner(storage)
	partition := createTestPartition()

	// Initially should be 0
	hwm, err := assigner.GetHighWaterMark("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get initial high water mark: %v", err)
	}
	if hwm != 0 {
		t.Errorf("Expected initial high water mark 0, got %d", hwm)
	}

	// Assign some offsets
	assigner.AssignBatchOffsets("test-namespace", "test-topic", partition, 10)

	// High water mark should be updated
	hwm, err = assigner.GetHighWaterMark("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get high water mark after assignment: %v", err)
	}
	if hwm != 10 {
		t.Errorf("Expected high water mark 10, got %d", hwm)
	}
}

func TestPartitionKey(t *testing.T) {
	partition1 := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: 1234567890,
	}

	partition2 := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 0,
		RangeStop:  31,
		UnixTimeNs: 1234567890,
	}

	partition3 := &schema_pb.Partition{
		RingSize:   1024,
		RangeStart: 32,
		RangeStop:  63,
		UnixTimeNs: 1234567890,
	}

	key1 := partitionKey(partition1)
	key2 := partitionKey(partition2)
	key3 := partitionKey(partition3)

	// Same partitions should have same key
	if key1 != key2 {
		t.Errorf("Same partitions should have same key: %s vs %s", key1, key2)
	}

	// Different partitions should have different keys
	if key1 == key3 {
		t.Errorf("Different partitions should have different keys: %s vs %s", key1, key3)
	}
}

func TestConcurrentOffsetAssignment(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	registry := NewPartitionOffsetRegistry(storage)
	partition := createTestPartition()

	const numGoroutines = 10
	const offsetsPerGoroutine = 100

	results := make(chan int64, numGoroutines*offsetsPerGoroutine)

	// Start concurrent offset assignments
	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < offsetsPerGoroutine; j++ {
				offset, err := registry.AssignOffset("test-namespace", "test-topic", partition)
				if err != nil {
					t.Errorf("Failed to assign offset: %v", err)
					return
				}
				results <- offset
			}
		}()
	}

	// Collect all results
	offsets := make(map[int64]bool)
	for i := 0; i < numGoroutines*offsetsPerGoroutine; i++ {
		offset := <-results
		if offsets[offset] {
			t.Errorf("Duplicate offset assigned: %d", offset)
		}
		offsets[offset] = true
	}

	// Verify we got all expected offsets
	expectedCount := numGoroutines * offsetsPerGoroutine
	if len(offsets) != expectedCount {
		t.Errorf("Expected %d unique offsets, got %d", expectedCount, len(offsets))
	}

	// Verify offsets are in expected range
	for offset := range offsets {
		if offset < 0 || offset >= int64(expectedCount) {
			t.Errorf("Offset %d is out of expected range [0, %d)", offset, expectedCount)
		}
	}
}
