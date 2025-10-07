package integration

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// TestSubscriberStreamReceivesData tests that a subscriber can receive data from a topic
func TestSubscriberStreamReceivesData(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping subscriber stream test in short mode")
	}

	// This test requires actual SeaweedFS masters
	masters := "localhost:9333"

	// Create handler
	handler, err := NewSeaweedMQBrokerHandler(masters, "", "test-subscriber-client")
	if err != nil {
		t.Skipf("Skipping test - SeaweedFS not available: %v", err)
	}
	defer handler.Close()

	// Create a test topic
	topicName := "test-subscriber-stream"
	err = handler.CreateTopic(topicName, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer handler.DeleteTopic(topicName)

	// Give topic time to be created
	time.Sleep(2 * time.Second)

	// Publish some test records
	numRecords := 5
	for i := 0; i < numRecords; i++ {
		key := []byte{}
		value := []byte{byte(i)}
		offset, err := handler.ProduceRecord(topicName, 0, key, value)
		if err != nil {
			t.Fatalf("Failed to produce record %d: %v", i, err)
		}
		t.Logf("Produced record %d at offset %d", i, offset)
	}

	// Give broker time to flush
	time.Sleep(1 * time.Second)

	// Verify we can read the records back
	t.Logf("Attempting to read records from offset 0")
	records, err := handler.GetStoredRecords(topicName, 0, 0, numRecords)
	if err != nil {
		t.Fatalf("Failed to get stored records: %v", err)
	}

	if len(records) == 0 {
		t.Fatalf("Expected %d records but got 0 - subscriber stream not receiving data!", numRecords)
	}

	if len(records) != numRecords {
		t.Errorf("Expected %d records but got %d", numRecords, len(records))
	}

	// Verify record contents
	for i, record := range records {
		if record.GetOffset() != int64(i) {
			t.Errorf("Record %d: expected offset %d but got %d", i, i, record.GetOffset())
		}
		value := record.GetValue()
		if len(value) != 1 || value[0] != byte(i) {
			t.Errorf("Record %d: expected value [%d] but got %v", i, i, value)
		}
	}

	t.Logf("SUCCESS: Subscriber stream received all %d records correctly", numRecords)
}

// TestSubscriberStreamTimeout tests the timeout behavior when no data is available
func TestSubscriberStreamTimeout(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping subscriber timeout test in short mode")
	}

	// This test requires actual SeaweedFS masters
	masters := "localhost:9333"

	// Create handler
	handler, err := NewSeaweedMQBrokerHandler(masters, "", "test-timeout-client")
	if err != nil {
		t.Skipf("Skipping test - SeaweedFS not available: %v", err)
	}
	defer handler.Close()

	// Create an empty topic
	topicName := "test-subscriber-timeout"
	err = handler.CreateTopic(topicName, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer handler.DeleteTopic(topicName)

	// Give topic time to be created
	time.Sleep(2 * time.Second)

	// Try to read from empty topic - should timeout and return empty
	start := time.Now()
	records, err := handler.GetStoredRecords(topicName, 0, 0, 10)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("GetStoredRecords should not error on empty topic: %v", err)
	}

	if len(records) != 0 {
		t.Errorf("Expected 0 records from empty topic but got %d", len(records))
	}

	// Should timeout around 10 seconds (the firstRecordTimeout)
	if elapsed < 9*time.Second || elapsed > 11*time.Second {
		t.Logf("WARNING: Timeout took %v, expected ~10 seconds", elapsed)
	}

	t.Logf("SUCCESS: Empty topic correctly returned 0 records after %v", elapsed)
}

// TestSubscriberStreamMultipleReads tests reading the same data multiple times
func TestSubscriberStreamMultipleReads(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping multiple reads test in short mode")
	}

	// This test requires actual SeaweedFS masters
	masters := "localhost:9333"

	// Create handler
	handler, err := NewSeaweedMQBrokerHandler(masters, "", "test-multi-read-client")
	if err != nil {
		t.Skipf("Skipping test - SeaweedFS not available: %v", err)
	}
	defer handler.Close()

	// Create a test topic
	topicName := "test-subscriber-multi-read"
	err = handler.CreateTopic(topicName, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	defer handler.DeleteTopic(topicName)

	// Give topic time to be created
	time.Sleep(2 * time.Second)

	// Publish records
	numRecords := 3
	for i := 0; i < numRecords; i++ {
		_, err := handler.ProduceRecord(topicName, 0, []byte{}, []byte{byte(i)})
		if err != nil {
			t.Fatalf("Failed to produce record %d: %v", i, err)
		}
	}

	time.Sleep(1 * time.Second)

	// Read the same records twice
	for attempt := 0; attempt < 2; attempt++ {
		t.Logf("Read attempt %d", attempt+1)
		records, err := handler.GetStoredRecords(topicName, 0, 0, numRecords)
		if err != nil {
			t.Fatalf("Attempt %d: Failed to get stored records: %v", attempt+1, err)
		}

		if len(records) != numRecords {
			t.Fatalf("Attempt %d: Expected %d records but got %d", attempt+1, numRecords, len(records))
		}

		// Verify they're the same records
		for i, record := range records {
			if record.GetOffset() != int64(i) {
				t.Errorf("Attempt %d, Record %d: expected offset %d but got %d",
					attempt+1, i, i, record.GetOffset())
			}
		}
	}

	t.Logf("SUCCESS: Multiple reads returned consistent data")
}

func init() {
	// Enable verbose logging for tests
	glog.MaxSize = 1024 * 1024 * 10
}
