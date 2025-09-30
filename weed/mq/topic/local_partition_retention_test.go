package topic

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

// TestSystemTopicRetention tests that system topics are not shut down aggressively
func TestSystemTopicRetention(t *testing.T) {
	// Create a local partition for _schemas topic
	partition := Partition{
		RangeStart: 0,
		RangeStop:  2520,
		RingSize:   2520,
		UnixTimeNs: time.Now().UnixNano(),
	}

	localPartition := NewLocalPartition(partition,
		func(logBuffer *log_buffer.LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
			// Mock flush function
		},
		func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (log_buffer.MessagePosition, bool, error) {
			// Mock read function
			return startPosition, false, nil
		})

	// Test that _schemas topic is identified as a system topic
	if !isSystemTopic("_schemas") {
		t.Error("_schemas should be identified as a system topic")
	}

	// Test that regular topics are not identified as system topics
	if isSystemTopic("regular-topic") {
		t.Error("regular-topic should not be identified as a system topic")
	}

	// Test that system topic partitions are not shut down aggressively
	// Even with no publishers or subscribers, system topics should not shut down
	shouldShutdown := localPartition.MaybeShutdownLocalPartitionForTopic("_schemas")
	if shouldShutdown {
		t.Error("System topic _schemas should not be shut down aggressively")
	}

	// Test that regular topics can still be shut down normally
	shouldShutdown = localPartition.MaybeShutdownLocalPartitionForTopic("regular-topic")
	if !shouldShutdown {
		t.Error("Regular topics should be shut down when no publishers/subscribers")
	}

	t.Logf("✅ System topic retention test passed")
}

// TestSystemTopicIdentification tests the isSystemTopic function
func TestSystemTopicIdentification(t *testing.T) {
	testCases := []struct {
		topic    string
		expected bool
	}{
		{"_schemas", true},
		{"__consumer_offsets", true},
		{"__transaction_state", true},
		{"_internal_topic", true},
		{"__internal_topic", true},
		{"regular-topic", false},
		{"my-topic", false},
		{"user_data", false},
		{"data_topic", false},
	}

	for _, tc := range testCases {
		result := isSystemTopic(tc.topic)
		if result != tc.expected {
			t.Errorf("isSystemTopic(%s) = %v, expected %v", tc.topic, result, tc.expected)
		}
	}

	t.Logf("✅ System topic identification test passed")
}

// TestSchemaRegistryScenario tests the specific scenario where Schema Registry connects and disconnects
func TestSchemaRegistryScenario(t *testing.T) {
	// Create a local partition for _schemas topic
	partition := Partition{
		RangeStart: 0,
		RangeStop:  2520,
		RingSize:   2520,
		UnixTimeNs: time.Now().UnixNano(),
	}

	localPartition := NewLocalPartition(partition,
		func(logBuffer *log_buffer.LogBuffer, startTime, stopTime time.Time, buf []byte, minOffset, maxOffset int64) {
			// Mock flush function
		},
		func(startPosition log_buffer.MessagePosition, stopTsNs int64, eachLogEntryFn log_buffer.EachLogEntryFuncType) (log_buffer.MessagePosition, bool, error) {
			// Mock read function
			return startPosition, false, nil
		})

	// Simulate Schema Registry connecting as a publisher
	localPartition.Publishers.AddPublisher("schema-registry-publisher", NewLocalPublisher())
	
	// Verify partition has a publisher
	if localPartition.Publishers.Size() != 1 {
		t.Errorf("Expected 1 publisher, got %d", localPartition.Publishers.Size())
	}

	// Simulate Schema Registry disconnecting (publisher removed)
	localPartition.Publishers.RemovePublisher("schema-registry-publisher")
	
	// Verify no publishers remain
	if localPartition.Publishers.Size() != 0 {
		t.Errorf("Expected 0 publishers, got %d", localPartition.Publishers.Size())
	}

	// Test that _schemas partition is NOT shut down even with no publishers/subscribers
	shouldShutdown := localPartition.MaybeShutdownLocalPartitionForTopic("_schemas")
	if shouldShutdown {
		t.Error("_schemas partition should not be shut down when Schema Registry disconnects")
	}

	// Test that a regular topic WOULD be shut down in the same scenario
	shouldShutdown = localPartition.MaybeShutdownLocalPartitionForTopic("user-topic")
	if !shouldShutdown {
		t.Error("Regular topic should be shut down when no publishers/subscribers remain")
	}

	t.Logf("✅ Schema Registry scenario test passed - _schemas partition preserved")
}


