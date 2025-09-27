package offset

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
)

func TestSMQOffsetIntegration_PublishRecord(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Publish a single record
	response, err := integration.PublishRecord(
		"test-namespace", "test-topic",
		partition,
		[]byte("test-key"),
		&schema_pb.RecordValue{},
	)

	if err != nil {
		t.Fatalf("Failed to publish record: %v", err)
	}

	if response.Error != "" {
		t.Errorf("Expected no error, got: %s", response.Error)
	}

	if response.BaseOffset != 0 {
		t.Errorf("Expected base offset 0, got %d", response.BaseOffset)
	}

	if response.LastOffset != 0 {
		t.Errorf("Expected last offset 0, got %d", response.LastOffset)
	}
}

func TestSMQOffsetIntegration_PublishRecordBatch(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Create batch of records
	records := []PublishRecordRequest{
		{Key: []byte("key1"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key2"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key3"), Value: &schema_pb.RecordValue{}},
	}

	// Publish batch
	response, err := integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)
	if err != nil {
		t.Fatalf("Failed to publish record batch: %v", err)
	}

	if response.Error != "" {
		t.Errorf("Expected no error, got: %s", response.Error)
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
}

func TestSMQOffsetIntegration_EmptyBatch(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Publish empty batch
	response, err := integration.PublishRecordBatch("test-namespace", "test-topic", partition, []PublishRecordRequest{})
	if err != nil {
		t.Fatalf("Failed to publish empty batch: %v", err)
	}

	if response.Error == "" {
		t.Error("Expected error for empty batch")
	}
}

func TestSMQOffsetIntegration_CreateSubscription(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Publish some records first
	records := []PublishRecordRequest{
		{Key: []byte("key1"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key2"), Value: &schema_pb.RecordValue{}},
	}
	integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)

	// Create subscription
	sub, err := integration.CreateSubscription(
		"test-sub",
		"test-namespace", "test-topic",
		partition,
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

func TestSMQOffsetIntegration_SubscribeRecords(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Publish some records
	records := []PublishRecordRequest{
		{Key: []byte("key1"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key2"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key3"), Value: &schema_pb.RecordValue{}},
	}
	integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)

	// Create subscription
	sub, err := integration.CreateSubscription(
		"test-sub",
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

	// Check offset progression
	if responses[0].Offset != 0 {
		t.Errorf("Expected first record offset 0, got %d", responses[0].Offset)
	}

	if responses[1].Offset != 1 {
		t.Errorf("Expected second record offset 1, got %d", responses[1].Offset)
	}

	// Check subscription advancement
	if sub.CurrentOffset != 2 {
		t.Errorf("Expected subscription current offset 2, got %d", sub.CurrentOffset)
	}
}

func TestSMQOffsetIntegration_SubscribeEmptyPartition(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Create subscription on empty partition
	sub, err := integration.CreateSubscription(
		"empty-sub",
		"test-namespace", "test-topic",
		partition,
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		0,
	)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Subscribe to records (should return empty)
	responses, err := integration.SubscribeRecords(sub, 10)
	if err != nil {
		t.Fatalf("Failed to subscribe to empty partition: %v", err)
	}

	if len(responses) != 0 {
		t.Errorf("Expected 0 responses from empty partition, got %d", len(responses))
	}
}

func TestSMQOffsetIntegration_SeekSubscription(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Publish records
	records := []PublishRecordRequest{
		{Key: []byte("key1"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key2"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key3"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key4"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key5"), Value: &schema_pb.RecordValue{}},
	}
	integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)

	// Create subscription
	sub, err := integration.CreateSubscription(
		"seek-sub",
		"test-namespace", "test-topic",
		partition,
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		0,
	)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Seek to offset 3
	err = integration.SeekSubscription("seek-sub", 3)
	if err != nil {
		t.Fatalf("Failed to seek subscription: %v", err)
	}

	if sub.CurrentOffset != 3 {
		t.Errorf("Expected current offset 3 after seek, got %d", sub.CurrentOffset)
	}

	// Subscribe from new position
	responses, err := integration.SubscribeRecords(sub, 2)
	if err != nil {
		t.Fatalf("Failed to subscribe after seek: %v", err)
	}

	if len(responses) != 2 {
		t.Errorf("Expected 2 responses after seek, got %d", len(responses))
	}

	if responses[0].Offset != 3 {
		t.Errorf("Expected first record offset 3 after seek, got %d", responses[0].Offset)
	}
}

func TestSMQOffsetIntegration_GetSubscriptionLag(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Publish records
	records := []PublishRecordRequest{
		{Key: []byte("key1"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key2"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key3"), Value: &schema_pb.RecordValue{}},
	}
	integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)

	// Create subscription at offset 1
	sub, err := integration.CreateSubscription(
		"lag-sub",
		"test-namespace", "test-topic",
		partition,
		schema_pb.OffsetType_EXACT_OFFSET,
		1,
	)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Get lag
	lag, err := integration.GetSubscriptionLag("lag-sub")
	if err != nil {
		t.Fatalf("Failed to get subscription lag: %v", err)
	}

	expectedLag := int64(3 - 1) // hwm - current
	if lag != expectedLag {
		t.Errorf("Expected lag %d, got %d", expectedLag, lag)
	}

	// Advance subscription and check lag again
	integration.SubscribeRecords(sub, 1)

	lag, err = integration.GetSubscriptionLag("lag-sub")
	if err != nil {
		t.Fatalf("Failed to get lag after advance: %v", err)
	}

	expectedLag = int64(3 - 2) // hwm - current
	if lag != expectedLag {
		t.Errorf("Expected lag %d after advance, got %d", expectedLag, lag)
	}
}

func TestSMQOffsetIntegration_CloseSubscription(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Create subscription
	_, err := integration.CreateSubscription(
		"close-sub",
		"test-namespace", "test-topic",
		partition,
		schema_pb.OffsetType_RESET_TO_EARLIEST,
		0,
	)
	if err != nil {
		t.Fatalf("Failed to create subscription: %v", err)
	}

	// Close subscription
	err = integration.CloseSubscription("close-sub")
	if err != nil {
		t.Fatalf("Failed to close subscription: %v", err)
	}

	// Try to get lag (should fail)
	_, err = integration.GetSubscriptionLag("close-sub")
	if err == nil {
		t.Error("Expected error when getting lag for closed subscription")
	}
}

func TestSMQOffsetIntegration_ValidateOffsetRange(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Publish some records
	records := []PublishRecordRequest{
		{Key: []byte("key1"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key2"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key3"), Value: &schema_pb.RecordValue{}},
	}
	integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)

	// Test valid range
	err := integration.ValidateOffsetRange("test-namespace", "test-topic", partition, 0, 2)
	if err != nil {
		t.Errorf("Valid range should not return error: %v", err)
	}

	// Test invalid range (beyond hwm)
	err = integration.ValidateOffsetRange("test-namespace", "test-topic", partition, 0, 5)
	if err == nil {
		t.Error("Expected error for range beyond high water mark")
	}
}

func TestSMQOffsetIntegration_GetAvailableOffsetRange(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Test empty partition
	offsetRange, err := integration.GetAvailableOffsetRange("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get available range for empty partition: %v", err)
	}

	if offsetRange.Count != 0 {
		t.Errorf("Expected empty range for empty partition, got count %d", offsetRange.Count)
	}

	// Publish records
	records := []PublishRecordRequest{
		{Key: []byte("key1"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key2"), Value: &schema_pb.RecordValue{}},
	}
	integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)

	// Test with data
	offsetRange, err = integration.GetAvailableOffsetRange("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get available range: %v", err)
	}

	if offsetRange.StartOffset != 0 {
		t.Errorf("Expected start offset 0, got %d", offsetRange.StartOffset)
	}

	if offsetRange.EndOffset != 1 {
		t.Errorf("Expected end offset 1, got %d", offsetRange.EndOffset)
	}

	if offsetRange.Count != 2 {
		t.Errorf("Expected count 2, got %d", offsetRange.Count)
	}
}

func TestSMQOffsetIntegration_GetOffsetMetrics(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Initial metrics
	metrics := integration.GetOffsetMetrics()
	if metrics.TotalOffsets != 0 {
		t.Errorf("Expected 0 total offsets initially, got %d", metrics.TotalOffsets)
	}

	if metrics.ActiveSubscriptions != 0 {
		t.Errorf("Expected 0 active subscriptions initially, got %d", metrics.ActiveSubscriptions)
	}

	// Publish records
	records := []PublishRecordRequest{
		{Key: []byte("key1"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key2"), Value: &schema_pb.RecordValue{}},
	}
	integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)

	// Create subscriptions
	integration.CreateSubscription("sub1", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)
	integration.CreateSubscription("sub2", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)

	// Check updated metrics
	metrics = integration.GetOffsetMetrics()
	if metrics.TotalOffsets != 2 {
		t.Errorf("Expected 2 total offsets, got %d", metrics.TotalOffsets)
	}

	if metrics.ActiveSubscriptions != 2 {
		t.Errorf("Expected 2 active subscriptions, got %d", metrics.ActiveSubscriptions)
	}

	if metrics.PartitionCount != 1 {
		t.Errorf("Expected 1 partition, got %d", metrics.PartitionCount)
	}
}

func TestSMQOffsetIntegration_GetOffsetInfo(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Test non-existent offset
	info, err := integration.GetOffsetInfo("test-namespace", "test-topic", partition, 0)
	if err != nil {
		t.Fatalf("Failed to get offset info: %v", err)
	}

	if info.Exists {
		t.Error("Offset should not exist in empty partition")
	}

	// Publish record
	integration.PublishRecord("test-namespace", "test-topic", partition, []byte("key1"), &schema_pb.RecordValue{})

	// Test existing offset
	info, err = integration.GetOffsetInfo("test-namespace", "test-topic", partition, 0)
	if err != nil {
		t.Fatalf("Failed to get offset info for existing offset: %v", err)
	}

	if !info.Exists {
		t.Error("Offset should exist after publishing")
	}

	if info.Offset != 0 {
		t.Errorf("Expected offset 0, got %d", info.Offset)
	}
}

func TestSMQOffsetIntegration_GetPartitionOffsetInfo(t *testing.T) {
	storage := NewInMemoryOffsetStorage()
	integration := NewSMQOffsetIntegration(storage)
	partition := createTestPartition()

	// Test empty partition
	info, err := integration.GetPartitionOffsetInfo("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get partition offset info: %v", err)
	}

	if info.EarliestOffset != 0 {
		t.Errorf("Expected earliest offset 0, got %d", info.EarliestOffset)
	}

	if info.LatestOffset != -1 {
		t.Errorf("Expected latest offset -1 for empty partition, got %d", info.LatestOffset)
	}

	if info.HighWaterMark != 0 {
		t.Errorf("Expected high water mark 0, got %d", info.HighWaterMark)
	}

	if info.RecordCount != 0 {
		t.Errorf("Expected record count 0, got %d", info.RecordCount)
	}

	// Publish records
	records := []PublishRecordRequest{
		{Key: []byte("key1"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key2"), Value: &schema_pb.RecordValue{}},
		{Key: []byte("key3"), Value: &schema_pb.RecordValue{}},
	}
	integration.PublishRecordBatch("test-namespace", "test-topic", partition, records)

	// Create subscription
	integration.CreateSubscription("test-sub", "test-namespace", "test-topic", partition, schema_pb.OffsetType_RESET_TO_EARLIEST, 0)

	// Test with data
	info, err = integration.GetPartitionOffsetInfo("test-namespace", "test-topic", partition)
	if err != nil {
		t.Fatalf("Failed to get partition offset info with data: %v", err)
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

	if info.RecordCount != 3 {
		t.Errorf("Expected record count 3, got %d", info.RecordCount)
	}

	if info.ActiveSubscriptions != 1 {
		t.Errorf("Expected 1 active subscription, got %d", info.ActiveSubscriptions)
	}
}
