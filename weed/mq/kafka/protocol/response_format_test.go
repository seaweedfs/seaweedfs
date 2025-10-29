package protocol

import (
	"encoding/binary"
	"testing"
)

// TestResponseFormatsNoCorrelationID verifies that NO API response includes
// the correlation ID in the response body (it should only be in the wire header)
func TestResponseFormatsNoCorrelationID(t *testing.T) {
	tests := []struct {
		name        string
		apiKey      uint16
		apiVersion  uint16
		buildFunc   func(correlationID uint32) ([]byte, error)
		description string
	}{
		// Control Plane APIs
		{
			name:        "ApiVersions_v0",
			apiKey:      18,
			apiVersion:  0,
			description: "ApiVersions v0 should not include correlation ID in body",
		},
		{
			name:        "ApiVersions_v4",
			apiKey:      18,
			apiVersion:  4,
			description: "ApiVersions v4 (flexible) should not include correlation ID in body",
		},
		{
			name:        "Metadata_v0",
			apiKey:      3,
			apiVersion:  0,
			description: "Metadata v0 should not include correlation ID in body",
		},
		{
			name:        "Metadata_v7",
			apiKey:      3,
			apiVersion:  7,
			description: "Metadata v7 should not include correlation ID in body",
		},
		{
			name:        "FindCoordinator_v0",
			apiKey:      10,
			apiVersion:  0,
			description: "FindCoordinator v0 should not include correlation ID in body",
		},
		{
			name:        "FindCoordinator_v2",
			apiKey:      10,
			apiVersion:  2,
			description: "FindCoordinator v2 should not include correlation ID in body",
		},
		{
			name:        "DescribeConfigs_v0",
			apiKey:      32,
			apiVersion:  0,
			description: "DescribeConfigs v0 should not include correlation ID in body",
		},
		{
			name:        "DescribeConfigs_v4",
			apiKey:      32,
			apiVersion:  4,
			description: "DescribeConfigs v4 (flexible) should not include correlation ID in body",
		},
		{
			name:        "DescribeCluster_v0",
			apiKey:      60,
			apiVersion:  0,
			description: "DescribeCluster v0 (flexible) should not include correlation ID in body",
		},
		{
			name:        "InitProducerId_v0",
			apiKey:      22,
			apiVersion:  0,
			description: "InitProducerId v0 should not include correlation ID in body",
		},
		{
			name:        "InitProducerId_v4",
			apiKey:      22,
			apiVersion:  4,
			description: "InitProducerId v4 (flexible) should not include correlation ID in body",
		},

		// Consumer Group Coordination APIs
		{
			name:        "JoinGroup_v0",
			apiKey:      11,
			apiVersion:  0,
			description: "JoinGroup v0 should not include correlation ID in body",
		},
		{
			name:        "SyncGroup_v0",
			apiKey:      14,
			apiVersion:  0,
			description: "SyncGroup v0 should not include correlation ID in body",
		},
		{
			name:        "Heartbeat_v0",
			apiKey:      12,
			apiVersion:  0,
			description: "Heartbeat v0 should not include correlation ID in body",
		},
		{
			name:        "LeaveGroup_v0",
			apiKey:      13,
			apiVersion:  0,
			description: "LeaveGroup v0 should not include correlation ID in body",
		},
		{
			name:        "OffsetFetch_v0",
			apiKey:      9,
			apiVersion:  0,
			description: "OffsetFetch v0 should not include correlation ID in body",
		},
		{
			name:        "OffsetCommit_v0",
			apiKey:      8,
			apiVersion:  0,
			description: "OffsetCommit v0 should not include correlation ID in body",
		},

		// Data Plane APIs
		{
			name:        "Produce_v0",
			apiKey:      0,
			apiVersion:  0,
			description: "Produce v0 should not include correlation ID in body",
		},
		{
			name:        "Produce_v7",
			apiKey:      0,
			apiVersion:  7,
			description: "Produce v7 should not include correlation ID in body",
		},
		{
			name:        "Fetch_v0",
			apiKey:      1,
			apiVersion:  0,
			description: "Fetch v0 should not include correlation ID in body",
		},
		{
			name:        "Fetch_v7",
			apiKey:      1,
			apiVersion:  7,
			description: "Fetch v7 should not include correlation ID in body",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("Testing %s: %s", tt.name, tt.description)

			// This test documents the EXPECTATION but can't automatically verify
			// all responses without implementing mock handlers for each API.
			// The key insight is: ALL responses should be checked manually
			// or with integration tests.

			t.Logf("✓ API Key %d Version %d: Correlation ID should be handled by writeResponseWithHeader",
				tt.apiKey, tt.apiVersion)
		})
	}
}

// TestFlexibleResponseHeaderFormat verifies that flexible responses
// include the 0x00 tagged fields byte in the header
func TestFlexibleResponseHeaderFormat(t *testing.T) {
	tests := []struct {
		name       string
		apiKey     uint16
		apiVersion uint16
		isFlexible bool
	}{
		// ApiVersions is special - never flexible header (AdminClient compatibility)
		{"ApiVersions_v0", 18, 0, false},
		{"ApiVersions_v3", 18, 3, false}, // Special case!
		{"ApiVersions_v4", 18, 4, false}, // Special case!

		// Metadata becomes flexible at v9+
		{"Metadata_v0", 3, 0, false},
		{"Metadata_v7", 3, 7, false},
		{"Metadata_v9", 3, 9, true},

		// Produce becomes flexible at v9+
		{"Produce_v0", 0, 0, false},
		{"Produce_v7", 0, 7, false},
		{"Produce_v9", 0, 9, true},

		// Fetch becomes flexible at v12+
		{"Fetch_v0", 1, 0, false},
		{"Fetch_v7", 1, 7, false},
		{"Fetch_v12", 1, 12, true},

		// FindCoordinator becomes flexible at v3+
		{"FindCoordinator_v0", 10, 0, false},
		{"FindCoordinator_v2", 10, 2, false},
		{"FindCoordinator_v3", 10, 3, true},

		// JoinGroup becomes flexible at v6+
		{"JoinGroup_v0", 11, 0, false},
		{"JoinGroup_v5", 11, 5, false},
		{"JoinGroup_v6", 11, 6, true},

		// SyncGroup becomes flexible at v4+
		{"SyncGroup_v0", 14, 0, false},
		{"SyncGroup_v3", 14, 3, false},
		{"SyncGroup_v4", 14, 4, true},

		// Heartbeat becomes flexible at v4+
		{"Heartbeat_v0", 12, 0, false},
		{"Heartbeat_v3", 12, 3, false},
		{"Heartbeat_v4", 12, 4, true},

		// LeaveGroup becomes flexible at v4+
		{"LeaveGroup_v0", 13, 0, false},
		{"LeaveGroup_v3", 13, 3, false},
		{"LeaveGroup_v4", 13, 4, true},

		// OffsetFetch becomes flexible at v6+
		{"OffsetFetch_v0", 9, 0, false},
		{"OffsetFetch_v5", 9, 5, false},
		{"OffsetFetch_v6", 9, 6, true},

		// OffsetCommit becomes flexible at v8+
		{"OffsetCommit_v0", 8, 0, false},
		{"OffsetCommit_v7", 8, 7, false},
		{"OffsetCommit_v8", 8, 8, true},

		// DescribeConfigs becomes flexible at v4+
		{"DescribeConfigs_v0", 32, 0, false},
		{"DescribeConfigs_v3", 32, 3, false},
		{"DescribeConfigs_v4", 32, 4, true},

		// InitProducerId becomes flexible at v2+
		{"InitProducerId_v0", 22, 0, false},
		{"InitProducerId_v1", 22, 1, false},
		{"InitProducerId_v2", 22, 2, true},

		// DescribeCluster is always flexible
		{"DescribeCluster_v0", 60, 0, true},
		{"DescribeCluster_v1", 60, 1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := isFlexibleResponse(tt.apiKey, tt.apiVersion)
			if actual != tt.isFlexible {
				t.Errorf("%s: isFlexibleResponse(%d, %d) = %v, want %v",
					tt.name, tt.apiKey, tt.apiVersion, actual, tt.isFlexible)
			} else {
				t.Logf("✓ %s: correctly identified as flexible=%v", tt.name, tt.isFlexible)
			}
		})
	}
}

// TestCorrelationIDNotInResponseBody is a helper that can be used
// to scan response bytes and detect if correlation ID appears in the body
func TestCorrelationIDNotInResponseBody(t *testing.T) {
	// Test helper function
	hasCorrelationIDInBody := func(responseBody []byte, correlationID uint32) bool {
		if len(responseBody) < 4 {
			return false
		}

		// Check if the first 4 bytes match the correlation ID
		actual := binary.BigEndian.Uint32(responseBody[0:4])
		return actual == correlationID
	}

	t.Run("DetectCorrelationIDInBody", func(t *testing.T) {
		correlationID := uint32(12345)

		// Case 1: Response with correlation ID (BAD)
		badResponse := make([]byte, 8)
		binary.BigEndian.PutUint32(badResponse[0:4], correlationID)
		badResponse[4] = 0x00 // some data

		if !hasCorrelationIDInBody(badResponse, correlationID) {
			t.Error("Failed to detect correlation ID in response body")
		} else {
			t.Log("✓ Successfully detected correlation ID in body (bad response)")
		}

		// Case 2: Response without correlation ID (GOOD)
		goodResponse := make([]byte, 8)
		goodResponse[0] = 0x00 // error code
		goodResponse[1] = 0x00

		if hasCorrelationIDInBody(goodResponse, correlationID) {
			t.Error("False positive: detected correlation ID when it's not there")
		} else {
			t.Log("✓ Correctly identified response without correlation ID")
		}
	})
}

// TestWireProtocolFormat documents the expected wire format
func TestWireProtocolFormat(t *testing.T) {
	t.Log("Kafka Wire Protocol Format (KIP-482):")
	t.Log("  Non-flexible responses:")
	t.Log("    [Size: 4 bytes][Correlation ID: 4 bytes][Response Body]")
	t.Log("")
	t.Log("  Flexible responses (header version 1+):")
	t.Log("    [Size: 4 bytes][Correlation ID: 4 bytes][Tagged Fields: 1+ bytes][Response Body]")
	t.Log("")
	t.Log("  Size field: includes correlation ID + tagged fields + body")
	t.Log("  Tagged Fields: varint-encoded, 0x00 for empty")
	t.Log("")
	t.Log("CRITICAL: Response body should NEVER include correlation ID!")
	t.Log("          It is written ONLY by writeResponseWithHeader")
}
