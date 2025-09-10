package integration

import (
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
