package protocol

import (
	"encoding/binary"
	"testing"
)

// TestHeartbeatResponseFormat_V0 verifies Heartbeat v0 response format
// v0: error_code (2 bytes) - NO throttle_time_ms
func TestHeartbeatResponseFormat_V0(t *testing.T) {
	h := &Handler{}
	response := HeartbeatResponse{
		CorrelationID: 12345,
		ErrorCode:     ErrorCodeNone,
	}

	result := h.buildHeartbeatResponseV(response, 0)

	// v0 should only have error_code (2 bytes)
	if len(result) != 2 {
		t.Errorf("Heartbeat v0 response length = %d, want 2 bytes (error_code only)", len(result))
	}

	// Verify error code
	errorCode := int16(binary.BigEndian.Uint16(result[0:2]))
	if errorCode != ErrorCodeNone {
		t.Errorf("Heartbeat v0 error_code = %d, want %d", errorCode, ErrorCodeNone)
	}
}

// TestHeartbeatResponseFormat_V1ToV3 verifies Heartbeat v1-v3 response format
// v1-v3: throttle_time_ms (4 bytes) -> error_code (2 bytes)
// CRITICAL: throttle_time_ms comes FIRST in v1+
func TestHeartbeatResponseFormat_V1ToV3(t *testing.T) {
	testCases := []struct {
		apiVersion uint16
		name       string
	}{
		{1, "v1"},
		{2, "v2"},
		{3, "v3"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{}
			response := HeartbeatResponse{
				CorrelationID: 12345,
				ErrorCode:     ErrorCodeNone,
			}

			result := h.buildHeartbeatResponseV(response, tc.apiVersion)

			// v1-v3 should have throttle_time_ms (4 bytes) + error_code (2 bytes) = 6 bytes
			if len(result) != 6 {
				t.Errorf("Heartbeat %s response length = %d, want 6 bytes", tc.name, len(result))
			}

			// CRITICAL: Verify field order - throttle_time_ms BEFORE error_code
			// Bytes 0-3: throttle_time_ms (should be 0)
			throttleTime := int32(binary.BigEndian.Uint32(result[0:4]))
			if throttleTime != 0 {
				t.Errorf("Heartbeat %s throttle_time_ms = %d, want 0", tc.name, throttleTime)
			}

			// Bytes 4-5: error_code (should be 0 = ErrorCodeNone)
			errorCode := int16(binary.BigEndian.Uint16(result[4:6]))
			if errorCode != ErrorCodeNone {
				t.Errorf("Heartbeat %s error_code = %d, want %d", tc.name, errorCode, ErrorCodeNone)
			}
		})
	}
}

// TestHeartbeatResponseFormat_V4Plus verifies Heartbeat v4+ response format (flexible)
// v4+: throttle_time_ms (4 bytes) -> error_code (2 bytes) -> tagged_fields (varint)
func TestHeartbeatResponseFormat_V4Plus(t *testing.T) {
	testCases := []struct {
		apiVersion uint16
		name       string
	}{
		{4, "v4"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{}
			response := HeartbeatResponse{
				CorrelationID: 12345,
				ErrorCode:     ErrorCodeNone,
			}

			result := h.buildHeartbeatResponseV(response, tc.apiVersion)

			// v4+ should have throttle_time_ms (4 bytes) + error_code (2 bytes) + tagged_fields (1 byte for empty) = 7 bytes
			if len(result) != 7 {
				t.Errorf("Heartbeat %s response length = %d, want 7 bytes", tc.name, len(result))
			}

			// Verify field order - throttle_time_ms BEFORE error_code
			// Bytes 0-3: throttle_time_ms (should be 0)
			throttleTime := int32(binary.BigEndian.Uint32(result[0:4]))
			if throttleTime != 0 {
				t.Errorf("Heartbeat %s throttle_time_ms = %d, want 0", tc.name, throttleTime)
			}

			// Bytes 4-5: error_code (should be 0 = ErrorCodeNone)
			errorCode := int16(binary.BigEndian.Uint16(result[4:6]))
			if errorCode != ErrorCodeNone {
				t.Errorf("Heartbeat %s error_code = %d, want %d", tc.name, errorCode, ErrorCodeNone)
			}

			// Byte 6: tagged_fields (should be 0x00 for empty)
			taggedFields := result[6]
			if taggedFields != 0x00 {
				t.Errorf("Heartbeat %s tagged_fields = 0x%02x, want 0x00", tc.name, taggedFields)
			}
		})
	}
}

// TestHeartbeatResponseFormat_ErrorCode verifies error codes are correctly encoded
func TestHeartbeatResponseFormat_ErrorCode(t *testing.T) {
	testCases := []struct {
		errorCode int16
		name      string
	}{
		{ErrorCodeNone, "None"},
		{ErrorCodeUnknownMemberID, "UnknownMemberID"},
		{ErrorCodeIllegalGeneration, "IllegalGeneration"},
		{ErrorCodeRebalanceInProgress, "RebalanceInProgress"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{}
			response := HeartbeatResponse{
				CorrelationID: 12345,
				ErrorCode:     tc.errorCode,
			}

			// Test with v3 (non-flexible)
			result := h.buildHeartbeatResponseV(response, 3)

			// Bytes 4-5: error_code
			errorCode := int16(binary.BigEndian.Uint16(result[4:6]))
			if errorCode != tc.errorCode {
				t.Errorf("Heartbeat v3 error_code = %d, want %d", errorCode, tc.errorCode)
			}
		})
	}
}

// TestHeartbeatResponseFormat_BugReproduce reproduces the original bug
// This test documents the bug where error_code was placed BEFORE throttle_time_ms in v1-v3
func TestHeartbeatResponseFormat_BugReproduce(t *testing.T) {
	t.Skip("This test documents the original bug - skip to avoid false failures")

	// Original buggy implementation would have:
	// v1-v3: error_code (2 bytes) -> throttle_time_ms (4 bytes)
	// This caused Sarama to read error_code bytes as throttle_time_ms, resulting in huge throttle values

	// Example: error_code = 0 (0x0000) would be read as throttle_time_ms = 0
	// But if there were any non-zero bytes, it would cause massive throttle times

	// But if error_code was non-zero, e.g., ErrorCodeIllegalGeneration = 22:
	buggyResponseWithError := []byte{
		0x00, 0x16, // error_code = 22 (0x0016)
		0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
	}

	// Sarama would read:
	// - Bytes 0-3 as throttle_time_ms: 0x00160000 = 1441792 ms = 24 minutes!
	throttleTimeMs := binary.BigEndian.Uint32(buggyResponseWithError[0:4])
	if throttleTimeMs != 1441792 {
		t.Errorf("Buggy format would cause throttle_time_ms = %d ms (%.1f minutes), want 1441792 ms (24 minutes)",
			throttleTimeMs, float64(throttleTimeMs)/60000)
	}

	t.Logf("Original bug: error_code=22 would be misread as throttle_time_ms=%d ms (%.1f minutes)",
		throttleTimeMs, float64(throttleTimeMs)/60000)
}
