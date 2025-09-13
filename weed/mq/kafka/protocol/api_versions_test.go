package protocol

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestApiVersions_AdvertisedVersionsMatch(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	response, err := handler.handleApiVersions(12345)
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

	// Check number of API keys
	numAPIKeys := binary.BigEndian.Uint32(response[6:10])
	expectedAPIKeys := uint32(14)
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

	// configs array (compact: empty = 0)
	requestBody = append(requestBody, 0x00)

	// tagged fields (empty)
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

func TestApiVersions_ResponseFormat(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	response, err := handler.handleApiVersions(99999)
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
	if numAPIKeys != 14 {
		t.Errorf("Wrong number of API keys: expected 14, got %d", numAPIKeys)
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
