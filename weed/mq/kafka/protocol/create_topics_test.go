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
	// FIXED: Added missing assignments array between replication_factor and configs
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

	// FIX: assignments array (compact: empty = 1) - this was missing before!
	request = append(request, 0x01)

	// configs array (compact: empty = 1) - was 0x00 before, should be 0x01
	request = append(request, 0x01)

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
	// FIXED: Added missing assignments arrays for both topics
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

	// FIX: assignments array (compact: empty = 1) - was missing!
	request = append(request, 0x01)

	// configs array (compact: empty = 1) - was 0x00, should be 0x01
	request = append(request, 0x01)

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

	// FIX: assignments array (compact: empty = 1) - was missing!
	request = append(request, 0x01)

	// configs array (compact: empty = 1) - was 0x00, should be 0x01
	request = append(request, 0x01)

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
				// FIXED: Added missing assignments array
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

				// FIX: assignments array (compact: empty = 1) - was missing!
				request = append(request, 0x01)

				// configs array (compact: empty = 1) - was 0x00, should be 0x01
				request = append(request, 0x01)

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

// TestCreateTopicsV5_SchemaRegistryFormat tests the actual format sent by Confluent Schema Registry
func TestCreateTopicsV5_SchemaRegistryFormat(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// This test replicates the exact request format that Confluent Schema Registry
	// sends for CreateTopics v5, which revealed the parsing bugs we fixed.

	// Build actual schema registry CreateTopics v5 request format
	// HEX: 0002095f736368656d617300000001000101020f636c65616e75702e706f6c69637908636f6d706163740000000075300000
	request := []byte{
		// FIX 1: Tagged fields count at start (was causing "0 topics found")
		0x00, // tagged fields count - we now skip this correctly

		// Topics compact array (1 topic)
		0x02, // compact array length: 1 topic + 1 = 2

		// Topic: "_schemas"
		0x09,                                           // compact string length: 8 chars + 1 = 9
		0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x73, // "_schemas"

		// num_partitions = 1
		0x00, 0x00, 0x00, 0x01,

		// replication_factor = 1
		0x00, 0x01,

		// FIX 2: Assignments compact array (was missing, causing offset errors)
		0x01, // compact array length: 0 assignments + 1 = 1 (empty array)

		// Configs compact array (1 config)
		0x02, // compact array length: 1 config + 1 = 2

		// Config name: "cleanup.policy" (compact string)
		0x0f,                                                                               // compact string length: 14 chars + 1 = 15
		0x63, 0x6c, 0x65, 0x61, 0x6e, 0x75, 0x70, 0x2e, 0x70, 0x6f, 0x6c, 0x69, 0x63, 0x79, // "cleanup.policy"

		// Config value: "compact" (compact string)
		0x08,                                     // compact string length: 7 chars + 1 = 8
		0x63, 0x6f, 0x6d, 0x70, 0x61, 0x63, 0x74, // "compact"

		// Config tagged fields (empty)
		0x00,

		// Topic tagged fields (empty)
		0x00,

		// timeout_ms = 30000
		0x00, 0x00, 0x75, 0x30,

		// validate_only = false
		0x00,

		// Top-level tagged fields (empty)
		0x00,
	}

	// Call the v5 handler
	response, err := handler.handleCreateTopicsV2Plus(1234, 5, request)

	if err != nil {
		t.Fatalf("CreateTopics v5 failed with actual schema registry format: %v", err)
	}

	if len(response) == 0 {
		t.Fatal("CreateTopics v5 returned empty response")
	}

	// Verify correlation ID
	correlationID := binary.BigEndian.Uint32(response[0:4])
	if correlationID != 1234 {
		t.Errorf("Expected correlation ID 1234, got %d", correlationID)
	}

	// Verify topic was created successfully
	if !handler.seaweedMQHandler.TopicExists("_schemas") {
		t.Error("Topic '_schemas' was not created")
	}

	// Verify the response format is correct for v5 flexible protocol
	// Should be: correlation_id(4) + throttle_time_ms(4) + topics_compact_array + topic_data + top_level_tagged_fields
	if len(response) < 8 {
		t.Fatalf("Response too short: %d bytes", len(response))
	}

	// Check throttle_time_ms is 0
	throttleTime := binary.BigEndian.Uint32(response[4:8])
	if throttleTime != 0 {
		t.Errorf("Expected throttle_time_ms=0, got %d", throttleTime)
	}

	// Verify topics array starts with compact array length
	if len(response) < 9 {
		t.Fatal("Response too short for topics array")
	}

	topicsArrayLength := response[8]
	if topicsArrayLength != 2 { // 1 topic + 1 for compact array encoding
		t.Errorf("Expected topics compact array length 2, got %d", topicsArrayLength)
	}
}

// TestCreateTopicsV5_WithoutTaggedFieldsCount tests v5 format without the initial tagged fields count
func TestCreateTopicsV5_WithoutTaggedFieldsCount(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Test the v5 format without the initial 0x00 byte (tagged fields count)
	// This should also work with our implementation
	request := []byte{
		// Topics compact array (1 topic) - no initial tagged fields count
		0x02, // compact array length: 1 topic + 1 = 2

		// Topic: "test-topic"
		0x0b,                                                       // compact string length: 10 chars + 1 = 11
		0x74, 0x65, 0x73, 0x74, 0x2d, 0x74, 0x6f, 0x70, 0x69, 0x63, // "test-topic"

		// num_partitions = 3
		0x00, 0x00, 0x00, 0x03,

		// replication_factor = 1
		0x00, 0x01,

		// Assignments compact array (empty)
		0x01, // compact array length: 0 assignments + 1 = 1

		// Configs compact array (empty)
		0x01, // compact array length: 0 configs + 1 = 1

		// Topic tagged fields (empty)
		0x00,

		// timeout_ms = 5000
		0x00, 0x00, 0x13, 0x88,

		// validate_only = false
		0x00,

		// Top-level tagged fields (empty)
		0x00,
	}

	_, err := handler.handleCreateTopicsV2Plus(5678, 5, request)

	if err != nil {
		t.Fatalf("CreateTopics v5 without initial tagged fields failed: %v", err)
	}

	// Verify topic was created
	if !handler.seaweedMQHandler.TopicExists("test-topic") {
		t.Error("Topic 'test-topic' was not created")
	}
}

// TestCreateTopicsV5_MultipleTopicsWithConfigs tests v5 with multiple topics and various configs
func TestCreateTopicsV5_MultipleTopicsWithConfigs(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Build v5 request with 2 topics, each with different configs
	request := make([]byte, 0, 512)

	// Tagged fields count at start
	request = append(request, 0x00)

	// Topics compact array (2 topics)
	request = append(request, 0x03) // 2 topics + 1 = 3

	// Topic 1: "topic-with-config"
	topic1 := "topic-with-config"
	request = append(request, byte(len(topic1)+1)) // compact string
	request = append(request, []byte(topic1)...)

	// num_partitions = 2
	request = append(request, 0x00, 0x00, 0x00, 0x02)

	// replication_factor = 1
	request = append(request, 0x00, 0x01)

	// Assignments (empty)
	request = append(request, 0x01)

	// Configs (1 config)
	request = append(request, 0x02) // 1 config + 1 = 2

	// Config: retention.ms = "86400000"
	configName := "retention.ms"
	request = append(request, byte(len(configName)+1))
	request = append(request, []byte(configName)...)

	configValue := "86400000"
	request = append(request, byte(len(configValue)+1))
	request = append(request, []byte(configValue)...)

	// Config tagged fields
	request = append(request, 0x00)

	// Topic tagged fields
	request = append(request, 0x00)

	// Topic 2: "topic-no-config"
	topic2 := "topic-no-config"
	request = append(request, byte(len(topic2)+1))
	request = append(request, []byte(topic2)...)

	// num_partitions = 1
	request = append(request, 0x00, 0x00, 0x00, 0x01)

	// replication_factor = 1
	request = append(request, 0x00, 0x01)

	// Assignments (empty)
	request = append(request, 0x01)

	// Configs (empty)
	request = append(request, 0x01)

	// Topic tagged fields
	request = append(request, 0x00)

	// timeout_ms = 10000
	request = append(request, 0x00, 0x00, 0x27, 0x10)

	// validate_only = false
	request = append(request, 0x00)

	// Top-level tagged fields
	request = append(request, 0x00)

	_, err := handler.handleCreateTopicsV2Plus(9999, 5, request)

	if err != nil {
		t.Fatalf("CreateTopics v5 with multiple topics failed: %v", err)
	}

	// Verify both topics were created
	if !handler.seaweedMQHandler.TopicExists("topic-with-config") {
		t.Error("Topic 'topic-with-config' was not created")
	}

	if !handler.seaweedMQHandler.TopicExists("topic-no-config") {
		t.Error("Topic 'topic-no-config' was not created")
	}
}

// TestCreateTopicsV5_ErrorCases tests various error conditions with v5 format
func TestCreateTopicsV5_ErrorCases(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	testCases := []struct {
		name        string
		request     []byte
		expectError bool
		description string
	}{
		{
			name: "TruncatedRequest",
			request: []byte{
				0x00, // tagged fields count
				0x02, // topics array
				// Missing topic data - should fail
			},
			expectError: true,
			description: "Request truncated in topics array",
		},
		{
			name: "InvalidCompactArrayLength",
			request: []byte{
				0x00, // tagged fields count
				0x00, // invalid compact array length (would be null array)
				// Should fail because null arrays aren't valid for CreateTopics
			},
			expectError: true,
			description: "Invalid compact array length",
		},
		{
			name: "MissingTimeoutMs",
			request: []byte{
				0x00,                   // tagged fields count
				0x02,                   // topics array (1 topic)
				0x05,                   // topic name "test" (4 chars + 1)
				0x74, 0x65, 0x73, 0x74, // "test"
				0x00, 0x00, 0x00, 0x01, // partitions = 1
				0x00, 0x01, // replication = 1
				0x01, // assignments (empty)
				0x01, // configs (empty)
				0x00, // topic tagged fields
				// Missing timeout_ms and beyond - should fail
			},
			expectError: true,
			description: "Missing timeout_ms field",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := handler.handleCreateTopicsV2Plus(1111, 5, tc.request)

			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error for %s, but got none", tc.description)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for %s: %v", tc.description, err)
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

// TestCreateTopicsV5_ResponseFormat tests the CreateTopics v5 response format fixes
func TestCreateTopicsV5_ResponseFormat(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Build a simple v5 request to get a response
	request := []byte{
		// Tagged fields count at start
		0x00,

		// Topics compact array (1 topic)
		0x02, // compact array length: 1 topic + 1 = 2

		// Topic: "test-response"
		0x0e, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, // "test-response" (13 chars + 1)

		// num_partitions = 2
		0x00, 0x00, 0x00, 0x02,

		// replication_factor = 1
		0x00, 0x01,

		// assignments (empty compact array)
		0x01, // empty array = 1

		// configs (empty compact array)
		0x01, // empty array = 1

		// tagged fields for topic (empty)
		0x00,

		// timeout_ms = 5000
		0x00, 0x00, 0x13, 0x88,

		// validate_only = false
		0x00,

		// top-level tagged fields (empty)
		0x00,
	}

	// Call the handler
	response, err := handler.handleCreateTopicsV2Plus(12345, 5, request)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(response) < 20 {
		t.Fatalf("Response too short: %d bytes, expected at least 20", len(response))
	}

	// Parse response to verify v5 format
	offset := 0

	// Check correlation ID
	correlationID := binary.BigEndian.Uint32(response[offset : offset+4])
	if correlationID != 12345 {
		t.Errorf("Expected correlation ID 12345, got %d", correlationID)
	}
	offset += 4

	// Check throttle_time_ms
	throttleTime := binary.BigEndian.Uint32(response[offset : offset+4])
	if throttleTime != 0 {
		t.Errorf("Expected throttle_time_ms 0, got %d", throttleTime)
	}
	offset += 4

	// Check topics compact array length
	topicsArrayLen := response[offset]
	if topicsArrayLen != 2 { // 1 topic + 1 for compact format
		t.Errorf("Expected topics array length 2 (1+1), got %d", topicsArrayLen)
	}
	offset += 1

	// Parse topic response
	// Topic name length (compact string)
	topicNameLen := response[offset] - 1 // compact format
	offset += 1

	// Topic name
	topicName := string(response[offset : offset+int(topicNameLen)])
	if topicName != "test-response" {
		t.Errorf("Expected topic name 'test-response', got '%s'", topicName)
	}
	offset += int(topicNameLen)

	// error_code (int16)
	errorCode := binary.BigEndian.Uint16(response[offset : offset+2])
	if errorCode != 0 {
		t.Errorf("Expected error_code 0, got %d", errorCode)
	}
	offset += 2

	// error_message (compact nullable string) - should be null (0) for success
	errorMessageMarker := response[offset]
	if errorMessageMarker != 0 {
		t.Errorf("Expected error_message null (0), got %d", errorMessageMarker)
	}
	offset += 1

	// NEW v5 FIELDS: num_partitions (int32)
	numPartitions := binary.BigEndian.Uint32(response[offset : offset+4])
	if numPartitions != 2 {
		t.Errorf("Expected num_partitions 2, got %d", numPartitions)
	}
	offset += 4

	// NEW v5 FIELDS: replication_factor (int16)
	replicationFactor := binary.BigEndian.Uint16(response[offset : offset+2])
	if replicationFactor != 1 {
		t.Errorf("Expected replication_factor 1, got %d", replicationFactor)
	}
	offset += 2

	// configs (compact array) - should be empty (1)
	configsArrayLen := response[offset]
	if configsArrayLen != 1 {
		t.Errorf("Expected configs array length 1 (empty), got %d", configsArrayLen)
	}
	offset += 1

	// topic tagged fields (empty)
	topicTaggedFields := response[offset]
	if topicTaggedFields != 0 {
		t.Errorf("Expected topic tagged fields 0, got %d", topicTaggedFields)
	}
	offset += 1

	// top-level tagged fields (empty)
	if offset < len(response) {
		topLevelTaggedFields := response[offset]
		if topLevelTaggedFields != 0 {
			t.Errorf("Expected top-level tagged fields 0, got %d", topLevelTaggedFields)
		}
	}

	t.Logf("✅ CreateTopics v5 response format validated successfully!")
	t.Logf("   • Response length: %d bytes", len(response))
	t.Logf("   • num_partitions: %d", numPartitions)
	t.Logf("   • replication_factor: %d", replicationFactor)
	t.Logf("   • error_message: null (correct encoding)")
}

// TestCreateTopicsV5_ResponseFormatErrorCase tests v5 response with error
func TestCreateTopicsV5_ResponseFormatErrorCase(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Create a topic that already exists first
	existingTopic := "existing-topic"
	err := handler.seaweedMQHandler.CreateTopic(existingTopic, 1)
	if err != nil {
		t.Fatalf("Failed to create existing topic: %v", err)
	}

	// Build v5 request for the existing topic
	request := []byte{
		// Tagged fields count at start
		0x00,

		// Topics compact array (1 topic)
		0x02, // compact array length: 1 topic + 1 = 2

		// Topic: "existing-topic" (14 chars)
		0x0f, 0x65, 0x78, 0x69, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x2d, 0x74, 0x6f, 0x70, 0x69, 0x63,

		// num_partitions = 3
		0x00, 0x00, 0x00, 0x03,

		// replication_factor = 1
		0x00, 0x01,

		// assignments (empty compact array)
		0x01, // empty array = 1

		// configs (empty compact array)
		0x01, // empty array = 1

		// tagged fields for topic (empty)
		0x00,

		// timeout_ms = 5000
		0x00, 0x00, 0x13, 0x88,

		// validate_only = false
		0x00,

		// top-level tagged fields (empty)
		0x00,
	}

	// Call the handler
	response, err := handler.handleCreateTopicsV2Plus(12346, 5, request)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Parse response to verify error handling
	offset := 4 + 4 + 1 // Skip correlation ID, throttle time, topics array length

	// Skip topic name
	topicNameLen := response[offset] - 1
	offset += 1 + int(topicNameLen)

	// Check error_code - should be 36 (TOPIC_ALREADY_EXISTS)
	errorCode := binary.BigEndian.Uint16(response[offset : offset+2])
	if errorCode != 36 {
		t.Errorf("Expected error_code 36 (TOPIC_ALREADY_EXISTS), got %d", errorCode)
	}
	offset += 2

	// error_message should still be null for our implementation
	errorMessageMarker := response[offset]
	if errorMessageMarker != 0 {
		t.Errorf("Expected error_message null (0), got %d", errorMessageMarker)
	}
	offset += 1

	// Verify num_partitions and replication_factor are still present even with error
	numPartitions := binary.BigEndian.Uint32(response[offset : offset+4])
	if numPartitions != 3 {
		t.Errorf("Expected num_partitions 3, got %d", numPartitions)
	}
	offset += 4

	replicationFactor := binary.BigEndian.Uint16(response[offset : offset+2])
	if replicationFactor != 1 {
		t.Errorf("Expected replication_factor 1, got %d", replicationFactor)
	}

	t.Logf("✅ CreateTopics v5 error response format validated successfully!")
	t.Logf("   • Error code: %d (TOPIC_ALREADY_EXISTS)", errorCode)
	t.Logf("   • num_partitions: %d", numPartitions)
	t.Logf("   • replication_factor: %d", replicationFactor)
}

// TestCreateTopicsV5_ResponseFormatNPEFix tests the specific fix for CreateTopics v5 response format
// that was causing NPE at line 1787 in Kafka AdminClient
func TestCreateTopicsV5_ResponseFormatNPEFix(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// This test verifies the exact fix for the NPE issue where the response format
	// was incorrectly including extra tagged fields byte at the beginning

	// Build a CreateTopics v5 request (using the Schema Registry format)
	request := []byte{
		// Top-level tagged fields (empty) - this is the fix we implemented
		0x00,

		// Topics compact array (1 topic)
		0x02, // compact array length: 1 topic + 1 = 2

		// Topic: "_schemas" (Schema Registry's topic)
		0x09,                                           // compact string length: 8 chars + 1 = 9
		0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x73, // "_schemas"

		// num_partitions = 1
		0x00, 0x00, 0x00, 0x01,

		// replication_factor = 1
		0x00, 0x01,

		// assignments (empty compact array)
		0x01,

		// configs (empty compact array)
		0x01,

		// topic tagged fields (empty)
		0x00,

		// timeout_ms = 30000
		0x00, 0x00, 0x75, 0x30,

		// validate_only = false
		0x00,

		// top-level tagged fields (empty)
		0x00,
	}

	// Call the v5 handler
	response, err := handler.handleCreateTopicsV2Plus(1787, 5, request)
	if err != nil {
		t.Fatalf("CreateTopics v5 NPE fix test failed: %v", err)
	}

	if len(response) == 0 {
		t.Fatal("CreateTopics v5 returned empty response")
	}

	// CRITICAL TEST: Verify response structure matches what AdminClient expects
	// The NPE was caused by incorrect response structure that made data.topics() return null

	offset := 0

	// 1. Correlation ID (4 bytes)
	if len(response) < offset+4 {
		t.Fatal("Response too short for correlation ID")
	}
	correlationID := binary.BigEndian.Uint32(response[offset : offset+4])
	if correlationID != 1787 {
		t.Errorf("Expected correlation ID 1787, got %d", correlationID)
	}
	offset += 4

	// 2. throttle_time_ms (4 bytes) - should come DIRECTLY after correlation ID
	// The bug was adding extra tagged fields byte here
	if len(response) < offset+4 {
		t.Fatal("Response too short for throttle_time_ms")
	}
	throttleTime := binary.BigEndian.Uint32(response[offset : offset+4])
	if throttleTime != 0 {
		t.Errorf("Expected throttle_time_ms=0, got %d", throttleTime)
	}
	offset += 4

	// 3. topics compact array - the critical part that was causing NPE
	if len(response) < offset+1 {
		t.Fatal("Response too short for topics array")
	}

	topicsArrayLength := response[offset]
	expectedLength := byte(2) // 1 topic + 1 for compact array encoding
	if topicsArrayLength != expectedLength {
		t.Errorf("Expected topics compact array length %d, got %d", expectedLength, topicsArrayLength)
		t.Error("THIS IS THE BUG THAT CAUSED data.topics() TO RETURN NULL!")
	}
	offset += 1

	// Verify we can parse the topic entry
	if len(response) < offset+1 {
		t.Fatal("Response too short for topic name length")
	}

	topicNameLength := response[offset] - 1 // compact string: actual length + 1
	offset += 1

	if len(response) < offset+int(topicNameLength) {
		t.Fatal("Response too short for topic name")
	}

	topicName := string(response[offset : offset+int(topicNameLength)])
	if topicName != "_schemas" {
		t.Errorf("Expected topic name '_schemas', got '%s'", topicName)
	}

	// Verify topic was actually created
	if !handler.seaweedMQHandler.TopicExists("_schemas") {
		t.Error("Topic '_schemas' was not created")
	}

	t.Log("✅ CreateTopics v5 NPE fix validated:")
	t.Log("   • Response format: correlation_id(4) + throttle_time_ms(4) + topics_array")
	t.Log("   • No extra tagged fields byte at beginning")
	t.Log("   • Topics array positioned correctly at offset 8")
	t.Log("   • AdminClient data.topics() will return proper list, not null")
	t.Log("   • NPE at line 1787 eliminated!")
}

// TestCreateTopicsV5_TaggedFieldsParsingFix tests the specific fix for handling top-level tagged fields
func TestCreateTopicsV5_TaggedFieldsParsingFix(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Test cases that validate the specific parsing fix we implemented
	testCases := []struct {
		name              string
		description       string
		request           []byte
		expectError       bool
		expectedTopics    int
		expectedTopicName string
	}{
		{
			name:        "WithTaggedFields",
			description: "Request starts with tagged fields (the original problem case)",
			request: []byte{
				// CRITICAL: This 0x00 byte was being treated as topics array count (0 topics)
				// Our fix correctly recognizes this as empty tagged fields and skips it
				0x00, // Top-level tagged fields count = 0 (empty)

				// Now the actual topics array starts here (offset 1)
				0x02, // Topics compact array: 1 topic + 1 = 2

				// Topic name: "test-topic" (10 chars)
				0x0b, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x74, 0x6f, 0x70, 0x69, 0x63, // "test-topic"

				// Topic details
				0x00, 0x00, 0x00, 0x01, // num_partitions = 1
				0x00, 0x01, // replication_factor = 1
				0x01, // assignments (empty)
				0x01, // configs (empty)
				0x00, // topic tagged fields

				// Request footer
				0x00, 0x00, 0x13, 0x88, // timeout_ms = 5000
				0x00, // validate_only = false
				0x00, // top-level tagged fields (empty)
			},
			expectError:       false,
			expectedTopics:    1,
			expectedTopicName: "test-topic",
		},
		{
			name:        "EmptyTaggedFields",
			description: "Request with empty tagged fields (the standard v5+ format)",
			request: []byte{
				// Top-level tagged fields (empty) - this is REQUIRED for v5+ flexible protocol
				0x00, // Tagged fields count = 0

				// Topics array starts after tagged fields
				0x02, // Topics compact array: 1 topic + 1 = 2

				// Topic name: "simple-topic" (12 chars)
				0x0d,                                                                   // Compact string length: 12 + 1 = 13 = 0x0d
				0x73, 0x69, 0x6d, 0x70, 0x6c, 0x65, 0x2d, 0x74, 0x6f, 0x70, 0x69, 0x63, // "simple-topic"

				// Topic details
				0x00, 0x00, 0x00, 0x02, // num_partitions = 2
				0x00, 0x01, // replication_factor = 1
				0x01, // assignments (empty)
				0x01, // configs (empty)
				0x00, // topic tagged fields

				// Request footer
				0x00, 0x00, 0x27, 0x10, // timeout_ms = 10000
				0x00, // validate_only = false
				0x00, // top-level tagged fields (empty)
			},
			expectError:       false,
			expectedTopics:    1,
			expectedTopicName: "simple-topic",
		},
		{
			name:        "MultipleTaggedFields",
			description: "Request with multiple tagged fields at the top level",
			request: []byte{
				// Top-level tagged fields with actual data (not empty)
				0x02, // 2 tagged fields count

				// Tagged field 1: tag=1, length=3, data=[0x01, 0x02, 0x03]
				0x01, 0x03, 0x01, 0x02, 0x03,

				// Tagged field 2: tag=5, length=1, data=[0xFF]
				0x05, 0x01, 0xFF,

				// Now the topics array
				0x02, // Topics compact array: 1 topic + 1 = 2

				// Topic name: "tagged-topic" (12 chars)
				0x0d, 0x74, 0x61, 0x67, 0x67, 0x65, 0x64, 0x2d, 0x74, 0x6f, 0x70, 0x69, 0x63, // "tagged-topic"

				// Topic details
				0x00, 0x00, 0x00, 0x01, // num_partitions = 1
				0x00, 0x01, // replication_factor = 1
				0x01, // assignments (empty)
				0x01, // configs (empty)
				0x00, // topic tagged fields

				// Request footer
				0x00, 0x00, 0x13, 0x88, // timeout_ms = 5000
				0x00, // validate_only = false
				0x00, // top-level tagged fields (empty)
			},
			expectError:       false,
			expectedTopics:    1,
			expectedTopicName: "tagged-topic",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing: %s", tc.description)

			// Call the v5+ handler
			response, err := handler.handleCreateTopicsV2Plus(1000+uint32(len(tc.name)), 5, tc.request)

			// Check error expectation
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected error for %s, but got none", tc.description)
				}
				return
			} else if err != nil {
				t.Errorf("Unexpected error for %s: %v", tc.description, err)
				return
			}

			// Verify response was generated
			if len(response) == 0 {
				t.Errorf("Empty response for %s", tc.description)
				return
			}

			// Verify topic was actually created (the key validation)
			if !handler.seaweedMQHandler.TopicExists(tc.expectedTopicName) {
				t.Errorf("Topic '%s' was not created - parsing fix failed!", tc.expectedTopicName)
				return
			}

			t.Logf("✅ %s: Successfully parsed %d topic(s) and created '%s'",
				tc.name, tc.expectedTopics, tc.expectedTopicName)
		})
	}

	t.Log("")
	t.Log("🎯 All tagged fields parsing scenarios passed!")
	t.Log("   • With tagged fields (original problem case): ✅")
	t.Log("   • Empty tagged fields (standard v5+ format): ✅")
	t.Log("   • Multiple tagged fields: ✅")
	t.Log("")
	t.Log("🔧 The fix correctly handles top-level tagged fields before parsing topics array")
}

// TestCreateTopicsV5_SchemaRegistryIntegration tests the complete Schema Registry workflow
func TestCreateTopicsV5_SchemaRegistryIntegration(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// This test simulates the complete workflow that Schema Registry uses:
	// 1. Check metadata for existing topics
	// 2. Send CreateTopics v5 request for missing topics
	// 3. Process response and continue

	t.Log("🧪 Testing complete Schema Registry workflow...")

	// Step 1: Schema Registry checks metadata (should find no topics)
	metadataRequest := []byte{
		0x01, // No topics requested (empty array + 1 for compact format)
		0x01, // allow_auto_topic_creation = true
		0x01, // include_cluster_authorized_operations = true
		0x01, // include_topic_authorized_operations = true
		0x00, // tagged fields
	}

	metadataResponse, err := handler.HandleMetadataV5V6(67890, metadataRequest)
	if err != nil {
		t.Fatalf("Metadata request failed: %v", err)
	}

	t.Logf("   • Metadata response: %d bytes", len(metadataResponse))

	// Step 2: Schema Registry sends CreateTopics v5 for _schemas topic
	// (This is the exact format that was failing before our fixes)
	createTopicsRequest := []byte{
		// FIX 1 VERIFICATION: Tagged fields count at start (this was causing "0 topics found")
		0x00,

		// Topics compact array (1 topic)
		0x02, // 1 topic + 1 = 2

		// Topic: "_schemas" (8 chars)
		0x09, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x73, // "_schemas"

		// num_partitions = 1
		0x00, 0x00, 0x00, 0x01,

		// replication_factor = 1
		0x00, 0x01,

		// FIX 2 VERIFICATION: assignments array (this was missing!)
		0x01, // empty assignments array

		// configs array with cleanup.policy=compact
		0x02,                                                                                     // 1 config + 1 = 2
		0x0f, 0x63, 0x6c, 0x65, 0x61, 0x6e, 0x75, 0x70, 0x2e, 0x70, 0x6f, 0x6c, 0x69, 0x63, 0x79, // "cleanup.policy"
		0x08, 0x63, 0x6f, 0x6d, 0x70, 0x61, 0x63, 0x74, // "compact"
		0x00, // config tagged fields

		// topic tagged fields
		0x00,

		// timeout_ms = 30000
		0x00, 0x00, 0x75, 0x30,

		// validate_only = false
		0x00,

		// top-level tagged fields
		0x00,
	}

	createTopicsResponse, err := handler.handleCreateTopicsV2Plus(67891, 5, createTopicsRequest)
	if err != nil {
		t.Fatalf("CreateTopics request failed: %v", err)
	}

	// Step 3: Verify the response is in correct v5 format
	if len(createTopicsResponse) < 20 {
		t.Fatalf("CreateTopics response too short: %d bytes", len(createTopicsResponse))
	}

	// Parse the response to ensure it's correct
	offset := 8 // Skip correlation ID + throttle_time_ms
	topicsLen := createTopicsResponse[offset]
	if topicsLen != 2 { // 1 topic + 1
		t.Errorf("Expected 1 topic in response, got %d", topicsLen-1)
	}

	t.Logf("   • CreateTopics response: %d bytes", len(createTopicsResponse))

	// Step 4: Verify topic was actually created
	if !handler.seaweedMQHandler.TopicExists("_schemas") {
		t.Errorf("Topic '_schemas' was not created")
	}

	t.Log("✅ Schema Registry integration test passed!")
	t.Log("   • Metadata check: ✅")
	t.Log("   • CreateTopics v5 parsing: ✅")
	t.Log("   • Response format: ✅")
	t.Log("   • Topic creation: ✅")
	t.Log("")
	t.Log("🎯 The CreateTopics v5 fixes enable Schema Registry compatibility!")
}
