package integration

import (
	"context"
	"testing"
	"time"
)

// TestAgentClient_Creation tests agent client creation and health checks
func TestAgentClient_Creation(t *testing.T) {
	// Skip if no real agent available (would need real SeaweedMQ setup)
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")

	client, err := NewAgentClient("localhost:17777") // default agent port
	if err != nil {
		t.Fatalf("Failed to create agent client: %v", err)
	}
	defer client.Close()

	// Test health check
	err = client.HealthCheck()
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}

	t.Logf("Agent client created and health check passed")
}

// TestAgentClient_PublishRecord tests publishing records
func TestAgentClient_PublishRecord(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")

	client, err := NewAgentClient("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create agent client: %v", err)
	}
	defer client.Close()

	// Test publishing a record
	key := []byte("test-key")
	value := []byte("test-value")
	timestamp := time.Now().UnixNano()

	sequence, err := client.PublishRecord("test-topic", 0, key, value, timestamp)
	if err != nil {
		t.Fatalf("Failed to publish record: %v", err)
	}

	if sequence < 0 {
		t.Errorf("Invalid sequence: %d", sequence)
	}

	t.Logf("Published record with sequence: %d", sequence)
}

// TestAgentClient_SessionManagement tests publisher session lifecycle
func TestAgentClient_SessionManagement(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")

	client, err := NewAgentClient("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create agent client: %v", err)
	}
	defer client.Close()

	// Create publisher session
	session, err := client.GetOrCreatePublisher("session-test-topic", 0)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}

	if session.SessionID == 0 {
		t.Errorf("Invalid session ID: %d", session.SessionID)
	}

	if session.Topic != "session-test-topic" {
		t.Errorf("Topic mismatch: got %s, want session-test-topic", session.Topic)
	}

	if session.Partition != 0 {
		t.Errorf("Partition mismatch: got %d, want 0", session.Partition)
	}

	// Close the publisher
	err = client.ClosePublisher("session-test-topic", 0)
	if err != nil {
		t.Errorf("Failed to close publisher: %v", err)
	}

	t.Logf("Publisher session managed successfully")
}

// TestAgentClient_ConcurrentPublish tests concurrent publishing
func TestAgentClient_ConcurrentPublish(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")

	client, err := NewAgentClient("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create agent client: %v", err)
	}
	defer client.Close()

	// Publish multiple records concurrently
	numRecords := 10
	errors := make(chan error, numRecords)
	sequences := make(chan int64, numRecords)

	for i := 0; i < numRecords; i++ {
		go func(index int) {
			key := []byte("concurrent-key")
			value := []byte("concurrent-value-" + string(rune(index)))
			timestamp := time.Now().UnixNano()

			sequence, err := client.PublishRecord("concurrent-test-topic", 0, key, value, timestamp)
			if err != nil {
				errors <- err
				return
			}

			sequences <- sequence
			errors <- nil
		}(i)
	}

	// Collect results
	successCount := 0
	var lastSequence int64 = -1

	for i := 0; i < numRecords; i++ {
		err := <-errors
		if err != nil {
			t.Logf("Publish error: %v", err)
		} else {
			sequence := <-sequences
			if sequence > lastSequence {
				lastSequence = sequence
			}
			successCount++
		}
	}

	if successCount < numRecords {
		t.Errorf("Only %d/%d publishes succeeded", successCount, numRecords)
	}

	t.Logf("Concurrent publish test: %d/%d successful, last sequence: %d",
		successCount, numRecords, lastSequence)
}

// TestAgentClient_SubscriberSession tests subscriber session creation and management
func TestAgentClient_SubscriberSession(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")

	client, err := NewAgentClient("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create agent client: %v", err)
	}
	defer client.Close()

	topic := "subscriber-test-topic"
	partition := int32(0)
	startOffset := int64(0)

	// Create subscriber session
	session, err := client.GetOrCreateSubscriber(topic, partition, startOffset)
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	if session.Topic != topic {
		t.Errorf("Topic mismatch: got %s, want %s", session.Topic, topic)
	}

	if session.Partition != partition {
		t.Errorf("Partition mismatch: got %d, want %d", session.Partition, partition)
	}

	if session.Stream == nil {
		t.Errorf("Stream should not be nil")
	}

	if session.OffsetLedger == nil {
		t.Errorf("OffsetLedger should not be nil")
	}

	// Test getting existing session
	session2, err := client.GetOrCreateSubscriber(topic, partition, startOffset)
	if err != nil {
		t.Fatalf("Failed to get existing subscriber: %v", err)
	}

	// Should return the same session
	if session != session2 {
		t.Errorf("Should return the same subscriber session")
	}

	t.Logf("Subscriber session test completed successfully")
}

// TestAgentClient_ReadRecords tests reading records from subscriber stream
func TestAgentClient_ReadRecords(t *testing.T) {
	t.Skip("Integration test requires real SeaweedMQ Agent - run manually with agent available")

	client, err := NewAgentClient("localhost:17777")
	if err != nil {
		t.Fatalf("Failed to create agent client: %v", err)
	}
	defer client.Close()

	topic := "read-records-test-topic"
	partition := int32(0)

	// First, publish some records to have data to read
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("read-key-1"), []byte("read-value-1")},
		{[]byte("read-key-2"), []byte("read-value-2")},
		{[]byte("read-key-3"), []byte("read-value-3")},
	}

	// Publish records
	for i, data := range testData {
		timestamp := time.Now().UnixNano()
		sequence, err := client.PublishRecord(topic, partition, data.key, data.value, timestamp)
		if err != nil {
			t.Fatalf("Failed to publish record %d: %v", i, err)
		}
		t.Logf("Published record %d with sequence %d", i, sequence)
	}

	// Wait for records to be available
	time.Sleep(200 * time.Millisecond)

	// Create subscriber session
	subscriber, err := client.GetOrCreateSubscriber(topic, partition, 0)
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}

	// Try to read records
	maxRecords := len(testData)
	records, err := client.ReadRecords(subscriber, maxRecords)
	if err != nil {
		t.Fatalf("Failed to read records: %v", err)
	}

	t.Logf("Read %d records from SeaweedMQ", len(records))

	// Validate records
	for i, record := range records {
		if record == nil {
			t.Errorf("Record %d should not be nil", i)
			continue
		}

		if len(record.Value) == 0 {
			t.Errorf("Record %d should have non-empty value", i)
		}

		if record.Timestamp == 0 {
			t.Errorf("Record %d should have non-zero timestamp", i)
		}

		t.Logf("Record %d: key=%s, value=%s, timestamp=%d, sequence=%d",
			i, string(record.Key), string(record.Value), record.Timestamp, record.Sequence)
	}

	// Test reading with smaller maxRecords
	smallBatch, err := client.ReadRecords(subscriber, 1)
	if err != nil {
		t.Errorf("Failed to read small batch: %v", err)
	}
	t.Logf("Small batch read returned %d records", len(smallBatch))

	// Test reading when no records available (should not block indefinitely)
	emptyBatch, err := client.ReadRecords(subscriber, 10)
	if err != nil {
		t.Logf("Expected: reading when no records available returned error: %v", err)
	} else {
		t.Logf("Reading when no records available returned %d records", len(emptyBatch))
	}

	t.Logf("ReadRecords test completed successfully")
}

// TestAgentClient_ReadRecords_ErrorHandling tests error cases for reading records
func TestAgentClient_ReadRecords_ErrorHandling(t *testing.T) {
	// This is a unit test that can run without SeaweedMQ agent
	ctx := context.TODO()
	client := &AgentClient{
		subscribers: make(map[string]*SubscriberSession),
		ctx:         ctx,
	}

	// Test reading from nil session - this will fail safely
	records, err := client.ReadRecords(nil, 10)
	if err == nil {
		t.Errorf("Reading from nil session should fail")
	}
	if records != nil {
		t.Errorf("Records should be nil when session is nil")
	}

	// Test reading with maxRecords=0 - should return empty records quickly
	session := &SubscriberSession{
		Topic:     "test-topic",
		Partition: 0,
		Stream:    nil, // This will cause an error when trying to read
	}

	records, err = client.ReadRecords(session, 0)
	if len(records) != 0 {
		t.Errorf("Should return empty records for maxRecords=0, got %d", len(records))
	}
	// Error is expected due to nil stream, but it should return empty records before attempting to read

	t.Logf("ReadRecords error handling test completed")
}
