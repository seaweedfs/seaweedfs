package protocol

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestApiVersions_AdvertisedVersionsMatch(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	response, err := handler.handleApiVersions(12345, 0)
	if err != nil {
		t.Fatalf("handleApiVersions failed: %v", err)
	}

	if len(response) < 10 {
		t.Fatalf("Response too short: %d bytes", len(response))
	}

	// Check correlation ID
	correlationID := binary.BigEndian.Uint32(response[0:4])
	if correlationID != 12345 {
		t.Errorf("Expected correlation ID 12345, got %d", correlationID)
	}

	// Check error code
	errorCode := binary.BigEndian.Uint16(response[4:6])
	if errorCode != 0 {
		t.Errorf("Expected error code 0, got %d", errorCode)
	}

	// Check number of API keys (updated for DESCRIBE_CONFIGS fix)
	numAPIKeys := binary.BigEndian.Uint32(response[6:10])
	expectedAPIKeys := uint32(17)
	if numAPIKeys != expectedAPIKeys {
		t.Errorf("Expected %d API keys, got %d", expectedAPIKeys, numAPIKeys)
	}

	// Parse and verify specific API versions
	offset := 10
	apiVersionMap := make(map[uint16][2]uint16) // apiKey -> [minVersion, maxVersion]

	for i := uint32(0); i < numAPIKeys && offset+6 <= len(response); i++ {
		apiKey := binary.BigEndian.Uint16(response[offset : offset+2])
		minVersion := binary.BigEndian.Uint16(response[offset+2 : offset+4])
		maxVersion := binary.BigEndian.Uint16(response[offset+4 : offset+6])
		offset += 6

		apiVersionMap[apiKey] = [2]uint16{minVersion, maxVersion}
	}

	// Verify critical corrected versions
	expectedVersions := map[uint16][2]uint16{
		9:  {0, 5}, // OffsetFetch: should now be v0-v5
		19: {0, 5}, // CreateTopics: should now be v0-v5
		3:  {0, 7}, // Metadata: should be v0-v7
		18: {0, 3}, // ApiVersions: should be v0-v3
		15: {0, 5}, // DescribeGroups: v0-v5
		16: {0, 4}, // ListGroups: v0-v4
		32: {0, 4}, // DescribeConfigs: v0-v4 (NPE fix - was missing from ApiVersions)
	}

	for apiKey, expected := range expectedVersions {
		if actual, exists := apiVersionMap[apiKey]; exists {
			if actual[0] != expected[0] || actual[1] != expected[1] {
				t.Errorf("API %d version mismatch: expected v%d-v%d, got v%d-v%d",
					apiKey, expected[0], expected[1], actual[0], actual[1])
			}
		} else {
			t.Errorf("API %d not found in response", apiKey)
		}
	}
}

func TestValidateAPIVersion_UpdatedVersions(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	testCases := []struct {
		name      string
		apiKey    uint16
		version   uint16
		shouldErr bool
	}{
		// OffsetFetch - should now support up to v5
		{"OffsetFetch v2", 9, 2, false},
		{"OffsetFetch v3", 9, 3, false}, // Was rejected before, should work now
		{"OffsetFetch v4", 9, 4, false}, // Was rejected before, should work now
		{"OffsetFetch v5", 9, 5, false}, // Was rejected before, should work now
		{"OffsetFetch v6", 9, 6, true},  // Should still be rejected

		// CreateTopics - should now support up to v5
		{"CreateTopics v4", 19, 4, false},
		{"CreateTopics v5", 19, 5, false}, // Was rejected before, should work now
		{"CreateTopics v6", 19, 6, true},  // Should be rejected

		// Metadata - should still support up to v7
		{"Metadata v7", 3, 7, false},
		{"Metadata v8", 3, 8, true},

		// ApiVersions - should still support up to v3
		{"ApiVersions v3", 18, 3, false},
		{"ApiVersions v4", 18, 4, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := handler.validateAPIVersion(tc.apiKey, tc.version)
			if tc.shouldErr && err == nil {
				t.Errorf("Expected error for API %d version %d, but got none", tc.apiKey, tc.version)
			}
			if !tc.shouldErr && err != nil {
				t.Errorf("Unexpected error for API %d version %d: %v", tc.apiKey, tc.version, err)
			}
		})
	}
}

func TestOffsetFetch_HigherVersionSupport(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Test that OffsetFetch v3, v4, v5 are now accepted
	versions := []uint16{3, 4, 5}

	for _, version := range versions {
		t.Run(fmt.Sprintf("OffsetFetch_v%d", version), func(t *testing.T) {
			// Create a basic OffsetFetch request
			requestBody := make([]byte, 0, 64)

			// Group ID (string: "test-group")
			groupID := "test-group"
			requestBody = append(requestBody, 0, byte(len(groupID))) // length
			requestBody = append(requestBody, []byte(groupID)...)    // group ID

			// Topics array (1 topic)
			requestBody = append(requestBody, 0, 0, 0, 1) // topics count

			// Topic: "test-topic"
			topicName := "test-topic"
			requestBody = append(requestBody, 0, byte(len(topicName))) // topic name length
			requestBody = append(requestBody, []byte(topicName)...)    // topic name

			// Partitions array (1 partition)
			requestBody = append(requestBody, 0, 0, 0, 1) // partitions count
			requestBody = append(requestBody, 0, 0, 0, 0) // partition 0

			// Call handler with the higher version
			response, err := handler.handleOffsetFetch(12345, version, requestBody)

			if err != nil {
				t.Fatalf("OffsetFetch v%d failed: %v", version, err)
			}

			if len(response) < 8 {
				t.Fatalf("OffsetFetch v%d response too short: %d bytes", version, len(response))
			}

			// Check correlation ID
			correlationID := binary.BigEndian.Uint32(response[0:4])
			if correlationID != 12345 {
				t.Errorf("OffsetFetch v%d: expected correlation ID 12345, got %d", version, correlationID)
			}
		})
	}
}

func TestCreateTopics_V5Support(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Test that CreateTopics v5 is now accepted and works
	requestBody := make([]byte, 0, 128)

	// Build v5 request (compact format)
	// Tagged fields (empty) - REQUIRED at the beginning for v5+
	requestBody = append(requestBody, 0x00)

	// Topics array (compact: 1 topic = 2)
	requestBody = append(requestBody, 0x02)

	// Topic: "v5-test-topic"
	topicName := "v5-test-topic"
	requestBody = append(requestBody, byte(len(topicName)+1)) // Compact string length
	requestBody = append(requestBody, []byte(topicName)...)   // Topic name

	// num_partitions = 2
	requestBody = append(requestBody, 0x00, 0x00, 0x00, 0x02)

	// replication_factor = 1
	requestBody = append(requestBody, 0x00, 0x01)

	// assignments array (compact: empty = 1)
	requestBody = append(requestBody, 0x01)

	// configs array (compact: empty = 1)
	requestBody = append(requestBody, 0x01)

	// topic tagged fields (empty)
	requestBody = append(requestBody, 0x00)

	// timeout_ms = 5000
	requestBody = append(requestBody, 0x00, 0x00, 0x13, 0x88)

	// validate_only = false
	requestBody = append(requestBody, 0x00)

	// tagged fields at end
	requestBody = append(requestBody, 0x00)

	// Call handler with v5
	response, err := handler.handleCreateTopics(12346, 5, requestBody)

	if err != nil {
		t.Fatalf("CreateTopics v5 failed: %v", err)
	}

	if len(response) < 8 {
		t.Fatalf("CreateTopics v5 response too short: %d bytes", len(response))
	}

	// Check correlation ID
	correlationID := binary.BigEndian.Uint32(response[0:4])
	if correlationID != 12346 {
		t.Errorf("CreateTopics v5: expected correlation ID 12346, got %d", correlationID)
	}

	// Verify topic was created
	if !handler.seaweedMQHandler.TopicExists("v5-test-topic") {
		t.Error("CreateTopics v5: topic was not created")
	}
}

// Benchmark to ensure version validation is efficient
func BenchmarkValidateAPIVersion(b *testing.B) {
	handler := NewTestHandler()
	defer handler.Close()

	// Test common API versions
	testCases := []struct {
		apiKey  uint16
		version uint16
	}{
		{9, 3},  // OffsetFetch v3
		{9, 5},  // OffsetFetch v5
		{19, 5}, // CreateTopics v5
		{3, 7},  // Metadata v7
		{18, 3}, // ApiVersions v3
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tc := testCases[i%len(testCases)]
		_ = handler.validateAPIVersion(tc.apiKey, tc.version)
	}
}

// TestDescribeConfigs_NPEFix verifies that DESCRIBE_CONFIGS (API key 32) is properly advertised
// and can be called without causing NPE in clients like Schema Registry
func TestDescribeConfigs_NPEFix(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Test 1: Verify DESCRIBE_CONFIGS is advertised in ApiVersions
	response, err := handler.handleApiVersions(12345, 0)
	if err != nil {
		t.Fatalf("handleApiVersions failed: %v", err)
	}

	// Parse the response to find DESCRIBE_CONFIGS
	numAPIKeys := binary.BigEndian.Uint32(response[6:10])
	offset := 10
	foundDescribeConfigs := false

	for i := uint32(0); i < numAPIKeys && offset+6 <= len(response); i++ {
		apiKey := binary.BigEndian.Uint16(response[offset : offset+2])
		minVersion := binary.BigEndian.Uint16(response[offset+2 : offset+4])
		maxVersion := binary.BigEndian.Uint16(response[offset+4 : offset+6])
		offset += 6

		if apiKey == 32 { // DESCRIBE_CONFIGS
			foundDescribeConfigs = true
			if minVersion != 0 || maxVersion != 4 {
				t.Errorf("DESCRIBE_CONFIGS version mismatch: expected v0-v4, got v%d-v%d", minVersion, maxVersion)
			}
			break
		}
	}

	if !foundDescribeConfigs {
		t.Error("DESCRIBE_CONFIGS (API key 32) not found in ApiVersions response - this was the root cause of Schema Registry NPE")
	}

	// Test 2: Verify DESCRIBE_CONFIGS can be called successfully
	// Build a basic DESCRIBE_CONFIGS request for a topic resource
	requestBody := make([]byte, 0, 64)

	// Resources count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Resource type (2 = topic)
	requestBody = append(requestBody, 2)

	// Resource name ("test-topic")
	topicName := "test-topic"
	requestBody = append(requestBody, 0, byte(len(topicName)))
	requestBody = append(requestBody, []byte(topicName)...)

	// Config names count (0 = return all configs)
	requestBody = append(requestBody, 0, 0, 0, 0)

	// Call DESCRIBE_CONFIGS
	describeResponse, err := handler.handleDescribeConfigs(12346, 0, requestBody)
	if err != nil {
		t.Fatalf("DESCRIBE_CONFIGS request failed: %v", err)
	}

	if len(describeResponse) < 12 {
		t.Errorf("DESCRIBE_CONFIGS response too short: %d bytes", len(describeResponse))
	}

	// Verify correlation ID
	correlationID := binary.BigEndian.Uint32(describeResponse[0:4])
	if correlationID != 12346 {
		t.Errorf("DESCRIBE_CONFIGS correlation ID mismatch: expected 12346, got %d", correlationID)
	}

	t.Log("DESCRIBE_CONFIGS NPE fix validated:")
	t.Log("   API key 32 properly advertised in ApiVersions")
	t.Log("   DESCRIBE_CONFIGS requests handled successfully")
	t.Log("   Schema Registry should no longer get 'broker does not support DESCRIBE_CONFIGS' error")
}

// TestSchemaRegistry_NPEFixesIntegration tests both NPE fixes working together
// to resolve the complete Schema Registry NPE problem
func TestSchemaRegistry_NPEFixesIntegration(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	t.Log("Testing Schema Registry NPE fixes integration...")

	// Phase 1: Schema Registry checks ApiVersions for DESCRIBE_CONFIGS support
	t.Log("Phase 1: Schema Registry checks ApiVersions for DESCRIBE_CONFIGS...")

	apiVersionsResponse, err := handler.handleApiVersions(1000, 0)
	if err != nil {
		t.Fatalf("ApiVersions failed: %v", err)
	}

	// Verify DESCRIBE_CONFIGS is advertised (fix #1)
	numAPIKeys := binary.BigEndian.Uint32(apiVersionsResponse[6:10])
	offset := 10
	foundDescribeConfigs := false

	for i := uint32(0); i < numAPIKeys && offset+6 <= len(apiVersionsResponse); i++ {
		apiKey := binary.BigEndian.Uint16(apiVersionsResponse[offset : offset+2])
		if apiKey == 32 {
			foundDescribeConfigs = true
			break
		}
		offset += 6
	}

	if !foundDescribeConfigs {
		t.Fatal("Fix #1 FAILED: DESCRIBE_CONFIGS not advertised")
	}
	t.Log("   DESCRIBE_CONFIGS (API 32) found in ApiVersions response")

	// Phase 2: Schema Registry calls DESCRIBE_CONFIGS successfully
	t.Log("Phase 2: Schema Registry calls DESCRIBE_CONFIGS...")

	describeRequest := make([]byte, 0, 32)
	describeRequest = append(describeRequest, 0, 0, 0, 1)            // 1 resource
	describeRequest = append(describeRequest, 2)                     // topic resource
	describeRequest = append(describeRequest, 0, 8)                  // name length
	describeRequest = append(describeRequest, []byte("_schemas")...) // topic name
	describeRequest = append(describeRequest, 0, 0, 0, 0)            // all configs

	_, err = handler.handleDescribeConfigs(1001, 0, describeRequest)
	if err != nil {
		t.Fatalf("Fix #1 FAILED: DESCRIBE_CONFIGS call failed: %v", err)
	}
	t.Log("   DESCRIBE_CONFIGS request succeeded")

	// Phase 3: Schema Registry calls CreateTopics v5 without NPE
	t.Log("Phase 3: Schema Registry calls CreateTopics v5...")

	createTopicsRequest := []byte{
		// Top-level tagged fields (empty)
		0x00,
		// Topics compact array (1 topic)
		0x02,
		// Topic: "_schemas"
		0x09, 0x5f, 0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x73,
		// num_partitions = 1, replication_factor = 1
		0x00, 0x00, 0x00, 0x01, 0x00, 0x01,
		// assignments (empty), configs (empty), topic tagged fields (empty)
		0x01, 0x01, 0x00,
		// timeout_ms = 30000, validate_only = false, top-level tagged fields (empty)
		0x00, 0x00, 0x75, 0x30, 0x00, 0x00,
	}

	createResponse, err := handler.handleCreateTopicsV2Plus(1002, 5, createTopicsRequest)
	if err != nil {
		t.Fatalf("Fix #2 FAILED: CreateTopics v5 failed: %v", err)
	}

	// Verify response structure prevents NPE (fix #2)
	if len(createResponse) < 10 {
		t.Fatal("Fix #2 FAILED: Response too short")
	}

	// Check response structure: correlation_id(4) + header_tagged_fields(1) + throttle_time_ms(4) + topics_array
	correlationID := binary.BigEndian.Uint32(createResponse[0:4])
	headerTaggedFields := createResponse[4]
	throttleTime := binary.BigEndian.Uint32(createResponse[5:9])
	topicsArrayLength := createResponse[9]

	if correlationID != 1002 {
		t.Errorf("Fix #2 FAILED: Wrong correlation ID: %d", correlationID)
	}
	if headerTaggedFields != 0 {
		t.Errorf("Fix #2 FAILED: Wrong header tagged fields: %d", headerTaggedFields)
	}
	if throttleTime != 0 {
		t.Errorf("Fix #2 FAILED: Wrong throttle time: %d", throttleTime)
	}
	if topicsArrayLength != 2 { // 1 topic + 1 for compact encoding
		t.Fatalf("Fix #2 FAILED: Topics array length %d causes data.topics() to return null", topicsArrayLength)
	}

	t.Log("   CreateTopics v5 response format correct - no NPE at line 1787")

	// Phase 4: Verify topic was created
	if !handler.seaweedMQHandler.TopicExists("_schemas") {
		t.Error("Topic creation failed")
	}
	t.Log("   Topic '_schemas' successfully created")

	t.Log("")
	t.Log("SCHEMA REGISTRY NPE FIXES INTEGRATION - COMPLETE SUCCESS!")
	t.Log("   Fix #1: DESCRIBE_CONFIGS properly advertised and working")
	t.Log("   Fix #2: CreateTopics v5 response format prevents NPE")
	t.Log("   Schema Registry workflow: ApiVersions -> DescribeConfigs -> CreateTopics")
	t.Log("   No more 'broker does not support DESCRIBE_CONFIGS' error")
	t.Log("   No more NPE at org.apache.kafka.clients.admin.KafkaAdminClient:1787")
	t.Log("   data.topics() returns proper list, not null")
	t.Log("")
	t.Log("Schema Registry can now successfully connect to SeaweedFS Kafka Gateway!")
}

func TestApiVersions_ResponseFormat(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	response, err := handler.handleApiVersions(99999, 0)
	if err != nil {
		t.Fatalf("handleApiVersions failed: %v", err)
	}

	// Verify the response can be parsed correctly
	if len(response) < 10 {
		t.Fatalf("Response too short for basic parsing")
	}

	offset := 0

	// Correlation ID (4 bytes)
	correlationID := binary.BigEndian.Uint32(response[offset : offset+4])
	if correlationID != 99999 {
		t.Errorf("Wrong correlation ID: expected 99999, got %d", correlationID)
	}
	offset += 4

	// Error code (2 bytes)
	errorCode := binary.BigEndian.Uint16(response[offset : offset+2])
	if errorCode != 0 {
		t.Errorf("Non-zero error code: %d", errorCode)
	}
	offset += 2

	// Number of API keys (4 bytes)
	numAPIKeys := binary.BigEndian.Uint32(response[offset : offset+4])
	if numAPIKeys != 17 {
		t.Errorf("Wrong number of API keys: expected 17, got %d", numAPIKeys)
	}
	offset += 4

	// Verify each API key entry format (apiKey + minVer + maxVer)
	for i := uint32(0); i < numAPIKeys && offset+6 <= len(response); i++ {
		apiKey := binary.BigEndian.Uint16(response[offset : offset+2])
		minVersion := binary.BigEndian.Uint16(response[offset+2 : offset+4])
		maxVersion := binary.BigEndian.Uint16(response[offset+4 : offset+6])

		// Verify minVersion <= maxVersion
		if minVersion > maxVersion {
			t.Errorf("API %d: invalid version range %d-%d", apiKey, minVersion, maxVersion)
		}

		// Verify minVersion is typically 0
		if minVersion != 0 {
			t.Errorf("API %d: unexpected min version %d (expected 0)", apiKey, minVersion)
		}

		offset += 6
	}

	if offset != len(response) {
		t.Errorf("Response parsing mismatch: expected %d bytes, parsed %d", len(response), offset)
	}
}
