package integration

import (
	"testing"
	"time"
)

// TestSeaweedMQHandler_Creation tests handler creation and shutdown
func TestSeaweedMQHandler_Creation(t *testing.T) {
	// Skip if no real agent available
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")
	
	handler, err := NewSeaweedMQHandler("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create SeaweedMQ handler: %v", err)
	}
	defer handler.Close()
	
	// Test basic operations
	topics := handler.ListTopics()
	if topics == nil {
		t.Errorf("ListTopics returned nil")
	}
	
	t.Logf("SeaweedMQ handler created successfully, found %d existing topics", len(topics))
}

// TestSeaweedMQHandler_TopicLifecycle tests topic creation and deletion
func TestSeaweedMQHandler_TopicLifecycle(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")
	
	handler, err := NewSeaweedMQHandler("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create SeaweedMQ handler: %v", err)
	}
	defer handler.Close()
	
	topicName := "lifecycle-test-topic"
	
	// Initially should not exist
	if handler.TopicExists(topicName) {
		t.Errorf("Topic %s should not exist initially", topicName)
	}
	
	// Create the topic
	err = handler.CreateTopic(topicName, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	
	// Now should exist
	if !handler.TopicExists(topicName) {
		t.Errorf("Topic %s should exist after creation", topicName)
	}
	
	// Get topic info
	info, exists := handler.GetTopicInfo(topicName)
	if !exists {
		t.Errorf("Topic info should exist")
	}
	
	if info.Name != topicName {
		t.Errorf("Topic name mismatch: got %s, want %s", info.Name, topicName)
	}
	
	if info.Partitions != 1 {
		t.Errorf("Partition count mismatch: got %d, want 1", info.Partitions)
	}
	
	// Try to create again (should fail)
	err = handler.CreateTopic(topicName, 1)
	if err == nil {
		t.Errorf("Creating existing topic should fail")
	}
	
	// Delete the topic
	err = handler.DeleteTopic(topicName)
	if err != nil {
		t.Fatalf("Failed to delete topic: %v", err)
	}
	
	// Should no longer exist
	if handler.TopicExists(topicName) {
		t.Errorf("Topic %s should not exist after deletion", topicName)
	}
	
	t.Logf("Topic lifecycle test completed successfully")
}

// TestSeaweedMQHandler_ProduceRecord tests message production
func TestSeaweedMQHandler_ProduceRecord(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")
	
	handler, err := NewSeaweedMQHandler("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create SeaweedMQ handler: %v", err)
	}
	defer handler.Close()
	
	topicName := "produce-test-topic"
	
	// Create topic
	err = handler.CreateTopic(topicName, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer handler.DeleteTopic(topicName)
	
	// Produce a record
	key := []byte("produce-key")
	value := []byte("produce-value")
	
	offset, err := handler.ProduceRecord(topicName, 0, key, value)
	if err != nil {
		t.Fatalf("Failed to produce record: %v", err)
	}
	
	if offset < 0 {
		t.Errorf("Invalid offset: %d", offset)
	}
	
	// Check ledger was updated
	ledger := handler.GetLedger(topicName, 0)
	if ledger == nil {
		t.Errorf("Ledger should exist after producing")
	}
	
	hwm := ledger.GetHighWaterMark()
	if hwm != offset+1 {
		t.Errorf("High water mark mismatch: got %d, want %d", hwm, offset+1)
	}
	
	t.Logf("Produced record at offset %d, HWM: %d", offset, hwm)
}

// TestSeaweedMQHandler_MultiplePartitions tests multiple partition handling
func TestSeaweedMQHandler_MultiplePartitions(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")
	
	handler, err := NewSeaweedMQHandler("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create SeaweedMQ handler: %v", err)
	}
	defer handler.Close()
	
	topicName := "multi-partition-test-topic"
	numPartitions := int32(3)
	
	// Create topic with multiple partitions
	err = handler.CreateTopic(topicName, numPartitions)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer handler.DeleteTopic(topicName)
	
	// Produce to different partitions
	for partitionID := int32(0); partitionID < numPartitions; partitionID++ {
		key := []byte("partition-key")
		value := []byte("partition-value")
		
		offset, err := handler.ProduceRecord(topicName, partitionID, key, value)
		if err != nil {
			t.Fatalf("Failed to produce to partition %d: %v", partitionID, err)
		}
		
		// Verify ledger
		ledger := handler.GetLedger(topicName, partitionID)
		if ledger == nil {
			t.Errorf("Ledger should exist for partition %d", partitionID)
		}
		
		t.Logf("Partition %d: produced at offset %d", partitionID, offset)
	}
	
	t.Logf("Multi-partition test completed successfully")
}

// TestSeaweedMQHandler_FetchRecords tests record fetching
func TestSeaweedMQHandler_FetchRecords(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")
	
	handler, err := NewSeaweedMQHandler("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create SeaweedMQ handler: %v", err)
	}
	defer handler.Close()
	
	topicName := "fetch-test-topic"
	
	// Create topic
	err = handler.CreateTopic(topicName, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer handler.DeleteTopic(topicName)
	
	// Produce some records
	numRecords := 3
	for i := 0; i < numRecords; i++ {
		key := []byte("fetch-key")
		value := []byte("fetch-value-" + string(rune(i)))
		
		_, err := handler.ProduceRecord(topicName, 0, key, value)
		if err != nil {
			t.Fatalf("Failed to produce record %d: %v", i, err)
		}
	}
	
	// Wait a bit for records to be available
	time.Sleep(100 * time.Millisecond)
	
	// Fetch records
	records, err := handler.FetchRecords(topicName, 0, 0, 1024)
	if err != nil {
		t.Fatalf("Failed to fetch records: %v", err)
	}
	
	if len(records) == 0 {
		t.Errorf("No records fetched")
	}
	
	t.Logf("Fetched %d bytes of record data", len(records))
	
	// Test fetching beyond high water mark
	ledger := handler.GetLedger(topicName, 0)
	hwm := ledger.GetHighWaterMark()
	
	emptyRecords, err := handler.FetchRecords(topicName, 0, hwm, 1024)
	if err != nil {
		t.Fatalf("Failed to fetch from HWM: %v", err)
	}
	
	if len(emptyRecords) != 0 {
		t.Errorf("Should get empty records beyond HWM, got %d bytes", len(emptyRecords))
	}
	
	t.Logf("Fetch test completed successfully")
}

// TestSeaweedMQHandler_ErrorHandling tests error conditions
func TestSeaweedMQHandler_ErrorHandling(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")
	
	handler, err := NewSeaweedMQHandler("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create SeaweedMQ handler: %v", err)
	}
	defer handler.Close()
	
	// Try to produce to non-existent topic
	_, err = handler.ProduceRecord("non-existent-topic", 0, []byte("key"), []byte("value"))
	if err == nil {
		t.Errorf("Producing to non-existent topic should fail")
	}
	
	// Try to fetch from non-existent topic
	_, err = handler.FetchRecords("non-existent-topic", 0, 0, 1024)
	if err == nil {
		t.Errorf("Fetching from non-existent topic should fail")
	}
	
	// Try to delete non-existent topic
	err = handler.DeleteTopic("non-existent-topic")
	if err == nil {
		t.Errorf("Deleting non-existent topic should fail")
	}
	
	t.Logf("Error handling test completed successfully")
}
