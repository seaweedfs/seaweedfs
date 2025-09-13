package protocol

import (
	"encoding/binary"
	"testing"
)

func TestApiVersions_FlexibleVersionSupport(t *testing.T) {
	handler := NewTestHandler()

	testCases := []struct {
		name           string
		apiVersion     uint16
		expectFlexible bool
	}{
		{"ApiVersions v0", 0, false},
		{"ApiVersions v1", 1, false},
		{"ApiVersions v2", 2, false},
		{"ApiVersions v3", 3, true},
		{"ApiVersions v4", 4, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			correlationID := uint32(12345)

			response, err := handler.handleApiVersions(correlationID, tc.apiVersion)
			if err != nil {
				t.Fatalf("handleApiVersions failed: %v", err)
			}

			if len(response) < 4 {
				t.Fatalf("Response too short: %d bytes", len(response))
			}

			// Check correlation ID
			respCorrelationID := binary.BigEndian.Uint32(response[0:4])
			if respCorrelationID != correlationID {
				t.Errorf("Correlation ID = %d, want %d", respCorrelationID, correlationID)
			}

			// Check error code
			errorCode := binary.BigEndian.Uint16(response[4:6])
			if errorCode != 0 {
				t.Errorf("Error code = %d, want 0", errorCode)
			}

			// Parse API keys count based on version
			offset := 6
			var apiKeysCount uint32

			if tc.expectFlexible {
				// Should use compact array format
				count, consumed, err := DecodeCompactArrayLength(response[offset:])
				if err != nil {
					t.Fatalf("Failed to decode compact array length: %v", err)
				}
				apiKeysCount = count
				offset += consumed
			} else {
				// Should use regular array format
				if len(response) < offset+4 {
					t.Fatalf("Response too short for regular array length")
				}
				apiKeysCount = binary.BigEndian.Uint32(response[offset : offset+4])
				offset += 4
			}

			if apiKeysCount != 14 {
				t.Errorf("API keys count = %d, want 14", apiKeysCount)
			}

			// Verify that we have enough data for all API keys
			// Each API key entry is 6 bytes: api_key(2) + min_version(2) + max_version(2)
			expectedMinSize := offset + int(apiKeysCount*6)
			if tc.expectFlexible {
				expectedMinSize += 1 // tagged fields
			}

			if len(response) < expectedMinSize {
				t.Errorf("Response too short: got %d bytes, expected at least %d", len(response), expectedMinSize)
			}

			// Check that ApiVersions API itself is properly listed
			// API Key 18 should be the first entry
			if len(response) >= offset+6 {
				apiKey := binary.BigEndian.Uint16(response[offset : offset+2])
				minVersion := binary.BigEndian.Uint16(response[offset+2 : offset+4])
				maxVersion := binary.BigEndian.Uint16(response[offset+4 : offset+6])

				if apiKey != 18 {
					t.Errorf("First API key = %d, want 18 (ApiVersions)", apiKey)
				}
				if minVersion != 0 {
					t.Errorf("ApiVersions min version = %d, want 0", minVersion)
				}
				if maxVersion != 3 {
					t.Errorf("ApiVersions max version = %d, want 3", maxVersion)
				}
			}

			t.Logf("ApiVersions v%d response: %d bytes, flexible: %v", tc.apiVersion, len(response), tc.expectFlexible)
		})
	}
}

func TestFlexibleVersions_HeaderParsingIntegration(t *testing.T) {
	testCases := []struct {
		name           string
		apiKey         uint16
		apiVersion     uint16
		clientID       string
		expectFlexible bool
	}{
		{"Metadata v8 (regular)", 3, 8, "test-client", false},
		{"Metadata v9 (flexible)", 3, 9, "test-client", true},
		{"ApiVersions v2 (regular)", 18, 2, "test-client", false},
		{"ApiVersions v3 (flexible)", 18, 3, "test-client", true},
		{"CreateTopics v1 (regular)", 19, 1, "test-client", false},
		{"CreateTopics v2 (flexible)", 19, 2, "test-client", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Construct request header
			var headerData []byte

			// API Key (2 bytes)
			headerData = append(headerData, byte(tc.apiKey>>8), byte(tc.apiKey))

			// API Version (2 bytes)
			headerData = append(headerData, byte(tc.apiVersion>>8), byte(tc.apiVersion))

			// Correlation ID (4 bytes)
			correlationID := uint32(54321)
			corrBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(corrBytes, correlationID)
			headerData = append(headerData, corrBytes...)

			if tc.expectFlexible {
				// Flexible version: compact string for client ID
				headerData = append(headerData, FlexibleString(tc.clientID)...)
				// Empty tagged fields
				headerData = append(headerData, 0)
			} else {
				// Regular version: standard string for client ID
				clientIDBytes := []byte(tc.clientID)
				headerData = append(headerData, byte(len(clientIDBytes)>>8), byte(len(clientIDBytes)))
				headerData = append(headerData, clientIDBytes...)
			}

			// Add dummy request body
			headerData = append(headerData, 1, 2, 3, 4)

			// Parse header
			header, body, err := ParseRequestHeader(headerData)
			if err != nil {
				t.Fatalf("ParseRequestHeader failed: %v", err)
			}

			// Validate parsed header
			if header.APIKey != tc.apiKey {
				t.Errorf("APIKey = %d, want %d", header.APIKey, tc.apiKey)
			}
			if header.APIVersion != tc.apiVersion {
				t.Errorf("APIVersion = %d, want %d", header.APIVersion, tc.apiVersion)
			}
			if header.CorrelationID != correlationID {
				t.Errorf("CorrelationID = %d, want %d", header.CorrelationID, correlationID)
			}
			if header.ClientID == nil || *header.ClientID != tc.clientID {
				t.Errorf("ClientID = %v, want %s", header.ClientID, tc.clientID)
			}

			// Check tagged fields presence
			hasTaggedFields := header.TaggedFields != nil
			if hasTaggedFields != tc.expectFlexible {
				t.Errorf("Tagged fields present = %v, want %v", hasTaggedFields, tc.expectFlexible)
			}

			// Validate body
			expectedBody := []byte{1, 2, 3, 4}
			if len(body) != len(expectedBody) {
				t.Errorf("Body length = %d, want %d", len(body), len(expectedBody))
			}
			for i, b := range expectedBody {
				if i < len(body) && body[i] != b {
					t.Errorf("Body[%d] = %d, want %d", i, body[i], b)
				}
			}

			t.Logf("Header parsing for %s v%d: flexible=%v, client=%s",
				getAPIName(tc.apiKey), tc.apiVersion, tc.expectFlexible, tc.clientID)
		})
	}
}

func TestCreateTopics_FlexibleVersionConsistency(t *testing.T) {
	handler := NewTestHandler()

	// Test that CreateTopics v2+ continues to work correctly with flexible version utilities
	correlationID := uint32(99999)

	// Build CreateTopics v2 request using flexible version utilities
	var requestData []byte

	// Topics array (compact: 1 topic = 2)
	requestData = append(requestData, 2)

	// Topic name (compact string)
	topicName := "flexible-test-topic"
	requestData = append(requestData, FlexibleString(topicName)...)

	// Number of partitions (4 bytes)
	requestData = append(requestData, 0, 0, 0, 3)

	// Replication factor (2 bytes)
	requestData = append(requestData, 0, 1)

	// Configs array (compact: empty = 0)
	requestData = append(requestData, 0)

	// Tagged fields (empty)
	requestData = append(requestData, 0)

	// Timeout (4 bytes)
	requestData = append(requestData, 0, 0, 0x27, 0x10) // 10000ms

	// Validate only (1 byte)
	requestData = append(requestData, 0)

	// Tagged fields at end
	requestData = append(requestData, 0)

	// Call CreateTopics v2
	response, err := handler.handleCreateTopicsV2Plus(correlationID, 2, requestData)
	if err != nil {
		t.Fatalf("handleCreateTopicsV2Plus failed: %v", err)
	}

	if len(response) < 8 {
		t.Fatalf("Response too short: %d bytes", len(response))
	}

	// Check correlation ID
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("Correlation ID = %d, want %d", respCorrelationID, correlationID)
	}

	// Verify topic was created
	if !handler.seaweedMQHandler.TopicExists(topicName) {
		t.Errorf("Topic '%s' was not created", topicName)
	}

	t.Logf("CreateTopics v2 with flexible utilities: topic created successfully")
}

func BenchmarkFlexibleVersions_HeaderParsing(b *testing.B) {
	// Pre-construct headers for different scenarios
	scenarios := []struct {
		name string
		data []byte
	}{
		{
			name: "Regular_v1",
			data: func() []byte {
				var data []byte
				data = append(data, 0, 3, 0, 1) // Metadata v1
				corrBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(corrBytes, 12345)
				data = append(data, corrBytes...)
				data = append(data, 0, 11, 'b', 'e', 'n', 'c', 'h', '-', 'c', 'l', 'i', 'e', 'n', 't')
				data = append(data, 1, 2, 3)
				return data
			}(),
		},
		{
			name: "Flexible_v9",
			data: func() []byte {
				var data []byte
				data = append(data, 0, 3, 0, 9) // Metadata v9
				corrBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(corrBytes, 12345)
				data = append(data, corrBytes...)
				data = append(data, FlexibleString("bench-client")...)
				data = append(data, 0) // empty tagged fields
				data = append(data, 1, 2, 3)
				return data
			}(),
		},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := ParseRequestHeader(scenario.data)
				if err != nil {
					b.Fatalf("ParseRequestHeader failed: %v", err)
				}
			}
		})
	}
}
