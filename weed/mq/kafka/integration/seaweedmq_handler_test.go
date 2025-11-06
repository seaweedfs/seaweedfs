package integration

import (
	"context"
	"testing"
	"time"
)

// Unit tests for new FetchRecords functionality

// TestSeaweedMQHandler_MapSeaweedToKafkaOffsets tests offset mapping logic
func TestSeaweedMQHandler_MapSeaweedToKafkaOffsets(t *testing.T) {
	// Note: This test is now obsolete since the ledger system has been removed
	// SMQ now uses native offsets directly, so no mapping is needed
	t.Skip("Test obsolete: ledger system removed, SMQ uses native offsets")
}

// TestSeaweedMQHandler_MapSeaweedToKafkaOffsets_EmptyRecords tests empty record handling
func TestSeaweedMQHandler_MapSeaweedToKafkaOffsets_EmptyRecords(t *testing.T) {
	// Note: This test is now obsolete since the ledger system has been removed
	t.Skip("Test obsolete: ledger system removed, SMQ uses native offsets")
}

// TestSeaweedMQHandler_ConvertSeaweedToKafkaRecordBatch tests record batch conversion
func TestSeaweedMQHandler_ConvertSeaweedToKafkaRecordBatch(t *testing.T) {
	handler := &SeaweedMQHandler{}

	// Create sample records
	seaweedRecords := []*SeaweedRecord{
		{
			Key:       []byte("batch-key1"),
			Value:     []byte("batch-value1"),
			Timestamp: 1000000000,
			Offset:    0,
		},
		{
			Key:       []byte("batch-key2"),
			Value:     []byte("batch-value2"),
			Timestamp: 1000000001,
			Offset:    1,
		},
	}

	fetchOffset := int64(0)
	maxBytes := int32(1024)

	// Test conversion
	batchData, err := handler.convertSeaweedToKafkaRecordBatch(seaweedRecords, fetchOffset, maxBytes)
	if err != nil {
		t.Fatalf("Failed to convert to record batch: %v", err)
	}

	if len(batchData) == 0 {
		t.Errorf("Record batch should not be empty")
	}

	// Basic validation of record batch structure
	if len(batchData) < 61 { // Minimum Kafka record batch header size
		t.Errorf("Record batch too small: got %d bytes", len(batchData))
	}

	// Verify magic byte (should be 2 for version 2)
	magicByte := batchData[16] // Magic byte is at offset 16
	if magicByte != 2 {
		t.Errorf("Invalid magic byte: got %d, want 2", magicByte)
	}

	t.Logf("Successfully converted %d records to %d byte batch", len(seaweedRecords), len(batchData))
}

// TestSeaweedMQHandler_ConvertSeaweedToKafkaRecordBatch_EmptyRecords tests empty batch handling
func TestSeaweedMQHandler_ConvertSeaweedToKafkaRecordBatch_EmptyRecords(t *testing.T) {
	handler := &SeaweedMQHandler{}

	batchData, err := handler.convertSeaweedToKafkaRecordBatch([]*SeaweedRecord{}, 0, 1024)
	if err != nil {
		t.Errorf("Converting empty records should not fail: %v", err)
	}

	if len(batchData) != 0 {
		t.Errorf("Empty record batch should be empty, got %d bytes", len(batchData))
	}
}

// TestSeaweedMQHandler_ConvertSingleSeaweedRecord tests individual record conversion
func TestSeaweedMQHandler_ConvertSingleSeaweedRecord(t *testing.T) {
	handler := &SeaweedMQHandler{}

	testCases := []struct {
		name   string
		record *SeaweedRecord
		index  int64
		base   int64
	}{
		{
			name: "Record with key and value",
			record: &SeaweedRecord{
				Key:       []byte("test-key"),
				Value:     []byte("test-value"),
				Timestamp: 1000000000,
				Offset:    5,
			},
			index: 0,
			base:  5,
		},
		{
			name: "Record with null key",
			record: &SeaweedRecord{
				Key:       nil,
				Value:     []byte("test-value-no-key"),
				Timestamp: 1000000001,
				Offset:    6,
			},
			index: 1,
			base:  5,
		},
		{
			name: "Record with empty value",
			record: &SeaweedRecord{
				Key:       []byte("test-key-empty-value"),
				Value:     []byte{},
				Timestamp: 1000000002,
				Offset:    7,
			},
			index: 2,
			base:  5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			recordData := handler.convertSingleSeaweedRecord(tc.record, tc.index, tc.base)

			if len(recordData) == 0 {
				t.Errorf("Record data should not be empty")
			}

			// Basic validation - should have at least attributes, timestamp delta, offset delta, key length, value length, headers count
			if len(recordData) < 6 {
				t.Errorf("Record data too small: got %d bytes", len(recordData))
			}

			// Verify record structure
			pos := 0

			// Attributes (1 byte)
			if recordData[pos] != 0 {
				t.Errorf("Expected attributes to be 0, got %d", recordData[pos])
			}
			pos++

			// Timestamp delta (1 byte simplified)
			pos++

			// Offset delta (1 byte simplified)
			if recordData[pos] != byte(tc.index) {
				t.Errorf("Expected offset delta %d, got %d", tc.index, recordData[pos])
			}
			pos++

			t.Logf("Successfully converted single record: %d bytes", len(recordData))
		})
	}
}

// Integration tests

// TestSeaweedMQHandler_Creation tests handler creation and shutdown
func TestSeaweedMQHandler_Creation(t *testing.T) {
	// Skip if no real broker available
	t.Skip("Integration test requires real SeaweedMQ Broker - run manually with broker available")

	handler, err := NewSeaweedMQBrokerHandler("localhost:9333", "default", "localhost")
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
	t.Skip("Integration test requires real SeaweedMQ Broker - run manually with broker available")

	handler, err := NewSeaweedMQBrokerHandler("localhost:9333", "default", "localhost")
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
	t.Skip("Integration test requires real SeaweedMQ Broker - run manually with broker available")

	handler, err := NewSeaweedMQBrokerHandler("localhost:9333", "default", "localhost")
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

	offset, err := handler.ProduceRecord(context.Background(), topicName, 0, key, value)
	if err != nil {
		t.Fatalf("Failed to produce record: %v", err)
	}

	if offset < 0 {
		t.Errorf("Invalid offset: %d", offset)
	}

	// Check high water mark from broker (ledgers removed - broker handles offset management)
	hwm, err := handler.GetLatestOffset(topicName, 0)
	if err != nil {
		t.Errorf("Failed to get high water mark: %v", err)
	}

	if hwm != offset+1 {
		t.Errorf("High water mark mismatch: got %d, want %d", hwm, offset+1)
	}

	t.Logf("Produced record at offset %d, HWM: %d", offset, hwm)
}

// TestSeaweedMQHandler_MultiplePartitions tests multiple partition handling
func TestSeaweedMQHandler_MultiplePartitions(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Broker - run manually with broker available")

	handler, err := NewSeaweedMQBrokerHandler("localhost:9333", "default", "localhost")
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

		offset, err := handler.ProduceRecord(context.Background(), topicName, partitionID, key, value)
		if err != nil {
			t.Fatalf("Failed to produce to partition %d: %v", partitionID, err)
		}

		// Verify offset from broker (ledgers removed - broker handles offset management)
		hwm, err := handler.GetLatestOffset(topicName, partitionID)
		if err != nil {
			t.Errorf("Failed to get high water mark for partition %d: %v", partitionID, err)
		} else if hwm <= offset {
			t.Errorf("High water mark should be greater than produced offset for partition %d: hwm=%d, offset=%d", partitionID, hwm, offset)
		}

		t.Logf("Partition %d: produced at offset %d", partitionID, offset)
	}

	t.Logf("Multi-partition test completed successfully")
}

// TestSeaweedMQHandler_FetchRecords tests record fetching with real SeaweedMQ data
func TestSeaweedMQHandler_FetchRecords(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Broker - run manually with broker available")

	handler, err := NewSeaweedMQBrokerHandler("localhost:9333", "default", "localhost")
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

	// Produce some test records with known data
	testRecords := []struct {
		key   string
		value string
	}{
		{"fetch-key-1", "fetch-value-1"},
		{"fetch-key-2", "fetch-value-2"},
		{"fetch-key-3", "fetch-value-3"},
	}

	var producedOffsets []int64
	for i, record := range testRecords {
		offset, err := handler.ProduceRecord(context.Background(), topicName, 0, []byte(record.key), []byte(record.value))
		if err != nil {
			t.Fatalf("Failed to produce record %d: %v", i, err)
		}
		producedOffsets = append(producedOffsets, offset)
		t.Logf("Produced record %d at offset %d: key=%s, value=%s", i, offset, record.key, record.value)
	}

	// Wait a bit for records to be available in SeaweedMQ
	time.Sleep(500 * time.Millisecond)

	// Test fetching from beginning
	fetchedBatch, err := handler.FetchRecords(topicName, 0, 0, 2048)
	if err != nil {
		t.Fatalf("Failed to fetch records: %v", err)
	}

	if len(fetchedBatch) == 0 {
		t.Errorf("No record data fetched - this indicates the FetchRecords implementation is not working properly")
	} else {
		t.Logf("Successfully fetched %d bytes of real record batch data", len(fetchedBatch))

		// Basic validation of Kafka record batch format
		if len(fetchedBatch) >= 61 { // Minimum Kafka record batch size
			// Check magic byte (at offset 16)
			magicByte := fetchedBatch[16]
			if magicByte == 2 {
				t.Logf("✓ Valid Kafka record batch format detected (magic byte = 2)")
			} else {
				t.Errorf("Invalid Kafka record batch magic byte: got %d, want 2", magicByte)
			}
		} else {
			t.Errorf("Fetched batch too small to be valid Kafka record batch: %d bytes", len(fetchedBatch))
		}
	}

	// Test fetching from specific offset
	if len(producedOffsets) > 1 {
		partialBatch, err := handler.FetchRecords(topicName, 0, producedOffsets[1], 1024)
		if err != nil {
			t.Fatalf("Failed to fetch from specific offset: %v", err)
		}
		t.Logf("Fetched %d bytes starting from offset %d", len(partialBatch), producedOffsets[1])
	}

	// Test fetching beyond high water mark (ledgers removed - use broker offset management)
	hwm, err := handler.GetLatestOffset(topicName, 0)
	if err != nil {
		t.Fatalf("Failed to get high water mark: %v", err)
	}

	emptyBatch, err := handler.FetchRecords(topicName, 0, hwm, 1024)
	if err != nil {
		t.Fatalf("Failed to fetch from HWM: %v", err)
	}

	if len(emptyBatch) != 0 {
		t.Errorf("Should get empty batch beyond HWM, got %d bytes", len(emptyBatch))
	}

	t.Logf("✓ Real data fetch test completed successfully - FetchRecords is now working with actual SeaweedMQ data!")
}

// TestSeaweedMQHandler_FetchRecords_ErrorHandling tests error cases for fetching
func TestSeaweedMQHandler_FetchRecords_ErrorHandling(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Broker - run manually with broker available")

	handler, err := NewSeaweedMQBrokerHandler("localhost:9333", "default", "localhost")
	if err != nil {
		t.Fatalf("Failed to create SeaweedMQ handler: %v", err)
	}
	defer handler.Close()

	// Test fetching from non-existent topic
	_, err = handler.FetchRecords("non-existent-topic", 0, 0, 1024)
	if err == nil {
		t.Errorf("Fetching from non-existent topic should fail")
	}

	// Create topic for partition tests
	topicName := "fetch-error-test-topic"
	err = handler.CreateTopic(topicName, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer handler.DeleteTopic(topicName)

	// Test fetching from non-existent partition (partition 1 when only 0 exists)
	batch, err := handler.FetchRecords(topicName, 1, 0, 1024)
	// This may or may not fail depending on implementation, but should return empty batch
	if err != nil {
		t.Logf("Expected behavior: fetching from non-existent partition failed: %v", err)
	} else if len(batch) > 0 {
		t.Errorf("Fetching from non-existent partition should return empty batch, got %d bytes", len(batch))
	}

	// Test with very small maxBytes
	_, err = handler.ProduceRecord(context.Background(), topicName, 0, []byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to produce test record: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	smallBatch, err := handler.FetchRecords(topicName, 0, 0, 1) // Very small maxBytes
	if err != nil {
		t.Errorf("Fetching with small maxBytes should not fail: %v", err)
	}
	t.Logf("Fetch with maxBytes=1 returned %d bytes", len(smallBatch))

	t.Logf("Error handling test completed successfully")
}

// TestSeaweedMQHandler_ErrorHandling tests error conditions
func TestSeaweedMQHandler_ErrorHandling(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Broker - run manually with broker available")

	handler, err := NewSeaweedMQBrokerHandler("localhost:9333", "default", "localhost")
	if err != nil {
		t.Fatalf("Failed to create SeaweedMQ handler: %v", err)
	}
	defer handler.Close()

	// Try to produce to non-existent topic
	_, err = handler.ProduceRecord(context.Background(), "non-existent-topic", 0, []byte("key"), []byte("value"))
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
