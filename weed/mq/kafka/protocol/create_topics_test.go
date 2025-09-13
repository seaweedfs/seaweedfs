package protocol

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestCreateTopicsV0_BasicParsing(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Build a CreateTopics v0 request
	request := make([]byte, 0, 256)

	// Topics array count (1 topic)
	request = append(request, 0x00, 0x00, 0x00, 0x01)

	// Topic 1: "test-topic"
	topicName := "test-topic"
	request = append(request, 0x00, byte(len(topicName))) // Topic name length
	request = append(request, []byte(topicName)...)       // Topic name

	// num_partitions = 3
	request = append(request, 0x00, 0x00, 0x00, 0x03)

	// replication_factor = 1
	request = append(request, 0x00, 0x01)

	// assignments array (empty)
	request = append(request, 0x00, 0x00, 0x00, 0x00)

	// configs array (empty)
	request = append(request, 0x00, 0x00, 0x00, 0x00)

	// timeout_ms = 5000
	request = append(request, 0x00, 0x00, 0x13, 0x88)

	// Call handler
	response, err := handler.handleCreateTopicsV0V1(12345, request)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(response) < 10 {
		t.Fatalf("Response too short: %d bytes", len(response))
	}

	// Check correlation ID
	correlationID := binary.BigEndian.Uint32(response[0:4])
	if correlationID != 12345 {
		t.Errorf("Expected correlation ID 12345, got %d", correlationID)
	}

	// Check topics array count
	topicsCount := binary.BigEndian.Uint32(response[4:8])
	if topicsCount != 1 {
		t.Errorf("Expected 1 topic in response, got %d", topicsCount)
	}

	// Verify topic was actually created
	if !handler.seaweedMQHandler.TopicExists("test-topic") {
		t.Error("Topic 'test-topic' was not created")
	}
}

func TestCreateTopicsV0_TopicAlreadyExists(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Pre-create the topic
	err := handler.seaweedMQHandler.CreateTopic("existing-topic", 2)
	if err != nil {
		t.Fatalf("Failed to pre-create topic: %v", err)
	}

	// Build request for the same topic
	request := make([]byte, 0, 256)

	// Topics array count (1 topic)
	request = append(request, 0x00, 0x00, 0x00, 0x01)

	// Topic 1: "existing-topic"
	topicName := "existing-topic"
	request = append(request, 0x00, byte(len(topicName))) // Topic name length
	request = append(request, []byte(topicName)...)       // Topic name

	// num_partitions = 1
	request = append(request, 0x00, 0x00, 0x00, 0x01)

	// replication_factor = 1
	request = append(request, 0x00, 0x01)

	// assignments array (empty)
	request = append(request, 0x00, 0x00, 0x00, 0x00)

	// configs array (empty)
	request = append(request, 0x00, 0x00, 0x00, 0x00)

	// timeout_ms = 5000
	request = append(request, 0x00, 0x00, 0x13, 0x88)

	// Call handler
	response, err := handler.handleCreateTopicsV0V1(12346, request)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Parse response to check error code
	if len(response) < 12 {
		t.Fatalf("Response too short for error code check: %d bytes", len(response))
	}

	// Skip correlation ID (4 bytes) + topics count (4 bytes) + topic name length (2 bytes) + topic name
	offset := 4 + 4 + 2 + len(topicName)
	if len(response) >= offset+2 {
		errorCode := binary.BigEndian.Uint16(response[offset : offset+2])
		if errorCode != 36 { // TOPIC_ALREADY_EXISTS
			t.Errorf("Expected error code 36 (TOPIC_ALREADY_EXISTS), got %d", errorCode)
		}
	} else {
		t.Error("Response too short to contain error code")
	}
}

func TestCreateTopicsV0_InvalidPartitions(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Build request with invalid partition count (0)
	request := make([]byte, 0, 256)

	// Topics array count (1 topic)
	request = append(request, 0x00, 0x00, 0x00, 0x01)

	// Topic 1: "invalid-topic"
	topicName := "invalid-topic"
	request = append(request, 0x00, byte(len(topicName))) // Topic name length
	request = append(request, []byte(topicName)...)       // Topic name

	// num_partitions = 0 (invalid)
	request = append(request, 0x00, 0x00, 0x00, 0x00)

	// replication_factor = 1
	request = append(request, 0x00, 0x01)

	// assignments array (empty)
	request = append(request, 0x00, 0x00, 0x00, 0x00)

	// configs array (empty)
	request = append(request, 0x00, 0x00, 0x00, 0x00)

	// timeout_ms = 5000
	request = append(request, 0x00, 0x00, 0x13, 0x88)

	// Call handler
	response, err := handler.handleCreateTopicsV0V1(12347, request)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Parse response to check error code
	if len(response) < 12 {
		t.Fatalf("Response too short for error code check: %d bytes", len(response))
	}

	// Skip correlation ID (4 bytes) + topics count (4 bytes) + topic name length (2 bytes) + topic name
	offset := 4 + 4 + 2 + len(topicName)
	if len(response) >= offset+2 {
		errorCode := binary.BigEndian.Uint16(response[offset : offset+2])
		if errorCode != 37 { // INVALID_PARTITIONS
			t.Errorf("Expected error code 37 (INVALID_PARTITIONS), got %d", errorCode)
		}
	} else {
		t.Error("Response too short to contain error code")
	}

	// Verify topic was not created
	if handler.seaweedMQHandler.TopicExists("invalid-topic") {
		t.Error("Topic with invalid partitions should not have been created")
	}
}

func TestCreateTopicsV2Plus_CompactFormat(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Build a CreateTopics v2 request (compact format)
	request := make([]byte, 0, 256)

	// Topics array count (compact: count + 1, so 1 topic = 2)
	request = append(request, 0x02)

	// Topic 1: "compact-topic"
	topicName := "compact-topic"
	request = append(request, byte(len(topicName)+1)) // Compact string length
	request = append(request, []byte(topicName)...)   // Topic name

	// num_partitions = 2
	request = append(request, 0x00, 0x00, 0x00, 0x02)

	// replication_factor = 1
	request = append(request, 0x00, 0x01)

	// configs array (compact: empty = 0)
	request = append(request, 0x00)

	// tagged fields (empty)
	request = append(request, 0x00)

	// timeout_ms = 10000
	request = append(request, 0x00, 0x00, 0x27, 0x10)

	// validate_only = false
	request = append(request, 0x00)

	// tagged fields at end
	request = append(request, 0x00)

	// Call handler
	response, err := handler.handleCreateTopicsV2Plus(12348, 2, request)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(response) < 10 {
		t.Fatalf("Response too short: %d bytes", len(response))
	}

	// Check correlation ID
	correlationID := binary.BigEndian.Uint32(response[0:4])
	if correlationID != 12348 {
		t.Errorf("Expected correlation ID 12348, got %d", correlationID)
	}

	// Verify topic was created
	if !handler.seaweedMQHandler.TopicExists("compact-topic") {
		t.Error("Topic 'compact-topic' was not created")
	}
}

func TestCreateTopicsV2Plus_MultipleTopics(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Build a CreateTopics v2 request with 2 topics
	request := make([]byte, 0, 512)

	// Topics array count (compact: 2 topics = 3)
	request = append(request, 0x03)

	// Topic 1: "topic-one"
	topicName1 := "topic-one"
	request = append(request, byte(len(topicName1)+1)) // Compact string length
	request = append(request, []byte(topicName1)...)   // Topic name

	// num_partitions = 1
	request = append(request, 0x00, 0x00, 0x00, 0x01)

	// replication_factor = 1
	request = append(request, 0x00, 0x01)

	// configs array (compact: empty = 0)
	request = append(request, 0x00)

	// tagged fields (empty)
	request = append(request, 0x00)

	// Topic 2: "topic-two"
	topicName2 := "topic-two"
	request = append(request, byte(len(topicName2)+1)) // Compact string length
	request = append(request, []byte(topicName2)...)   // Topic name

	// num_partitions = 3
	request = append(request, 0x00, 0x00, 0x00, 0x03)

	// replication_factor = 1
	request = append(request, 0x00, 0x01)

	// configs array (compact: empty = 0)
	request = append(request, 0x00)

	// tagged fields (empty)
	request = append(request, 0x00)

	// timeout_ms = 5000
	request = append(request, 0x00, 0x00, 0x13, 0x88)

	// validate_only = false
	request = append(request, 0x00)

	// tagged fields at end
	request = append(request, 0x00)

	// Call handler
	response, err := handler.handleCreateTopicsV2Plus(12349, 2, request)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(response) < 4 {
		t.Fatalf("Response too short: %d bytes", len(response))
	}

	// Verify both topics were created
	if !handler.seaweedMQHandler.TopicExists("topic-one") {
		t.Error("Topic 'topic-one' was not created")
	}

	if !handler.seaweedMQHandler.TopicExists("topic-two") {
		t.Error("Topic 'topic-two' was not created")
	}
}

// Integration test with actual Kafka-like workflow
func TestCreateTopics_Integration(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Test version routing
	testCases := []struct {
		name       string
		version    uint16
		topicName  string
		partitions int32
	}{
		{"Version0", 0, "integration-v0-topic", 2},
		{"Version1", 1, "integration-v1-topic", 3},
		{"Version2", 2, "integration-v2-topic", 1},
		{"Version3", 3, "integration-v3-topic", 4},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var request []byte

			if tc.version <= 1 {
				// Build v0/v1 format request
				request = make([]byte, 0, 256)

				// Topics array count (1 topic)
				request = append(request, 0x00, 0x00, 0x00, 0x01)

				// Topic name
				request = append(request, 0x00, byte(len(tc.topicName)))
				request = append(request, []byte(tc.topicName)...)

				// num_partitions
				partitionBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(partitionBytes, uint32(tc.partitions))
				request = append(request, partitionBytes...)

				// replication_factor = 1
				request = append(request, 0x00, 0x01)

				// assignments array (empty)
				request = append(request, 0x00, 0x00, 0x00, 0x00)

				// configs array (empty)
				request = append(request, 0x00, 0x00, 0x00, 0x00)

				// timeout_ms = 5000
				request = append(request, 0x00, 0x00, 0x13, 0x88)
			} else {
				// Build v2+ format request (compact)
				request = make([]byte, 0, 256)

				// Topics array count (compact: 1 topic = 2)
				request = append(request, 0x02)

				// Topic name (compact string)
				request = append(request, byte(len(tc.topicName)+1))
				request = append(request, []byte(tc.topicName)...)

				// num_partitions
				partitionBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(partitionBytes, uint32(tc.partitions))
				request = append(request, partitionBytes...)

				// replication_factor = 1
				request = append(request, 0x00, 0x01)

				// configs array (compact: empty = 0)
				request = append(request, 0x00)

				// tagged fields (empty)
				request = append(request, 0x00)

				// timeout_ms = 5000
				request = append(request, 0x00, 0x00, 0x13, 0x88)

				// validate_only = false
				request = append(request, 0x00)

				// tagged fields at end
				request = append(request, 0x00)
			}

			// Call the main handler (which routes to version-specific handlers)
			response, err := handler.handleCreateTopics(uint32(1000+tc.version), tc.version, request)

			if err != nil {
				t.Fatalf("CreateTopics v%d failed: %v", tc.version, err)
			}

			if len(response) == 0 {
				t.Fatalf("CreateTopics v%d returned empty response", tc.version)
			}

			// Verify topic was created with correct partition count
			if !handler.seaweedMQHandler.TopicExists(tc.topicName) {
				t.Errorf("Topic '%s' was not created in v%d", tc.topicName, tc.version)
			}

			// Check partition count (create ledgers on-demand to verify partition setup)
			for partitionID := int32(0); partitionID < tc.partitions; partitionID++ {
				ledger := handler.seaweedMQHandler.GetOrCreateLedger(tc.topicName, partitionID)
				if ledger == nil {
					t.Errorf("Failed to get/create ledger for topic '%s' partition %d", tc.topicName, partitionID)
				}
			}
		})
	}
}

// Benchmark CreateTopics performance
func BenchmarkCreateTopicsV0(b *testing.B) {
	handler := NewTestHandler()
	defer handler.Close()

	// Pre-build request
	request := make([]byte, 0, 256)
	request = append(request, 0x00, 0x00, 0x00, 0x01) // 1 topic

	topicName := "benchmark-topic"
	request = append(request, 0x00, byte(len(topicName)))
	request = append(request, []byte(topicName)...)
	request = append(request, 0x00, 0x00, 0x00, 0x01) // 1 partition
	request = append(request, 0x00, 0x01)             // replication factor 1
	request = append(request, 0x00, 0x00, 0x00, 0x00) // empty assignments
	request = append(request, 0x00, 0x00, 0x00, 0x00) // empty configs
	request = append(request, 0x00, 0x00, 0x13, 0x88) // timeout 5000ms

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create unique topic names to avoid "already exists" errors
		uniqueRequest := make([]byte, len(request))
		copy(uniqueRequest, request)

		// Modify topic name to make it unique
		topicSuffix := []byte(fmt.Sprintf("-%d", i))
		uniqueRequest = append(uniqueRequest[:10+len(topicName)], topicSuffix...)
		uniqueRequest = append(uniqueRequest, request[10+len(topicName):]...)

		// Update topic name length
		uniqueRequest[8] = byte(len(topicName) + len(topicSuffix))

		_, err := handler.handleCreateTopicsV0V1(uint32(i), uniqueRequest)
		if err != nil {
			b.Fatalf("CreateTopics failed on iteration %d: %v", i, err)
		}
	}
}
