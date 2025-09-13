package protocol

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

func TestKafkaErrorCodes(t *testing.T) {
	tests := []struct {
		name         string
		errorCode    int16
		expectedInfo ErrorInfo
	}{
		{
			name:      "No error",
			errorCode: ErrorCodeNone,
			expectedInfo: ErrorInfo{
				Code: 0, Name: "NONE", Description: "No error", Retriable: false,
			},
		},
		{
			name:      "Unknown server error",
			errorCode: ErrorCodeUnknownServerError,
			expectedInfo: ErrorInfo{
				Code: 1, Name: "UNKNOWN_SERVER_ERROR", Description: "Unknown server error", Retriable: true,
			},
		},
		{
			name:      "Topic already exists",
			errorCode: ErrorCodeTopicAlreadyExists,
			expectedInfo: ErrorInfo{
				Code: 36, Name: "TOPIC_ALREADY_EXISTS", Description: "Topic already exists", Retriable: false,
			},
		},
		{
			name:      "Invalid partitions",
			errorCode: ErrorCodeInvalidPartitions,
			expectedInfo: ErrorInfo{
				Code: 37, Name: "INVALID_PARTITIONS", Description: "Invalid number of partitions", Retriable: false,
			},
		},
		{
			name:      "Request timed out",
			errorCode: ErrorCodeRequestTimedOut,
			expectedInfo: ErrorInfo{
				Code: 7, Name: "REQUEST_TIMED_OUT", Description: "Request timed out", Retriable: true,
			},
		},
		{
			name:      "Connection timeout",
			errorCode: ErrorCodeConnectionTimeout,
			expectedInfo: ErrorInfo{
				Code: 61, Name: "CONNECTION_TIMEOUT", Description: "Connection timeout", Retriable: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := GetErrorInfo(tt.errorCode)
			if info.Code != tt.expectedInfo.Code {
				t.Errorf("GetErrorInfo().Code = %d, want %d", info.Code, tt.expectedInfo.Code)
			}
			if info.Name != tt.expectedInfo.Name {
				t.Errorf("GetErrorInfo().Name = %s, want %s", info.Name, tt.expectedInfo.Name)
			}
			if info.Description != tt.expectedInfo.Description {
				t.Errorf("GetErrorInfo().Description = %s, want %s", info.Description, tt.expectedInfo.Description)
			}
			if info.Retriable != tt.expectedInfo.Retriable {
				t.Errorf("GetErrorInfo().Retriable = %v, want %v", info.Retriable, tt.expectedInfo.Retriable)
			}
		})
	}
}

func TestIsRetriableError(t *testing.T) {
	tests := []struct {
		name      string
		errorCode int16
		retriable bool
	}{
		{"None", ErrorCodeNone, false},
		{"Unknown server error", ErrorCodeUnknownServerError, true},
		{"Topic already exists", ErrorCodeTopicAlreadyExists, false},
		{"Request timed out", ErrorCodeRequestTimedOut, true},
		{"Rebalance in progress", ErrorCodeRebalanceInProgress, true},
		{"Invalid group ID", ErrorCodeInvalidGroupID, false},
		{"Network exception", ErrorCodeNetworkException, true},
		{"Connection timeout", ErrorCodeConnectionTimeout, true},
		{"Read timeout", ErrorCodeReadTimeout, true},
		{"Write timeout", ErrorCodeWriteTimeout, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetriableError(tt.errorCode); got != tt.retriable {
				t.Errorf("IsRetriableError() = %v, want %v", got, tt.retriable)
			}
		})
	}
}

func TestBuildErrorResponse(t *testing.T) {
	tests := []struct {
		name          string
		correlationID uint32
		errorCode     int16
		expectedLen   int
	}{
		{"Basic error response", 12345, ErrorCodeUnknownServerError, 6},
		{"Topic already exists", 67890, ErrorCodeTopicAlreadyExists, 6},
		{"No error", 11111, ErrorCodeNone, 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := BuildErrorResponse(tt.correlationID, tt.errorCode)

			if len(response) != tt.expectedLen {
				t.Errorf("BuildErrorResponse() length = %d, want %d", len(response), tt.expectedLen)
			}

			// Verify correlation ID
			if len(response) >= 4 {
				correlationID := binary.BigEndian.Uint32(response[0:4])
				if correlationID != tt.correlationID {
					t.Errorf("Correlation ID = %d, want %d", correlationID, tt.correlationID)
				}
			}

			// Verify error code
			if len(response) >= 6 {
				errorCode := binary.BigEndian.Uint16(response[4:6])
				if errorCode != uint16(tt.errorCode) {
					t.Errorf("Error code = %d, want %d", errorCode, uint16(tt.errorCode))
				}
			}
		})
	}
}

func TestBuildErrorResponseWithMessage(t *testing.T) {
	tests := []struct {
		name          string
		correlationID uint32
		errorCode     int16
		message       string
		expectNullMsg bool
	}{
		{"Error with message", 12345, ErrorCodeUnknownServerError, "Test error message", false},
		{"Error with empty message", 67890, ErrorCodeTopicAlreadyExists, "", true},
		{"Error with long message", 11111, ErrorCodeInvalidPartitions, "This is a longer error message for testing", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response := BuildErrorResponseWithMessage(tt.correlationID, tt.errorCode, tt.message)

			// Should have at least correlation ID (4) + error code (2) + message length (2)
			minLen := 8
			if len(response) < minLen {
				t.Errorf("BuildErrorResponseWithMessage() length = %d, want at least %d", len(response), minLen)
			}

			// Verify correlation ID
			correlationID := binary.BigEndian.Uint32(response[0:4])
			if correlationID != tt.correlationID {
				t.Errorf("Correlation ID = %d, want %d", correlationID, tt.correlationID)
			}

			// Verify error code
			errorCode := binary.BigEndian.Uint16(response[4:6])
			if errorCode != uint16(tt.errorCode) {
				t.Errorf("Error code = %d, want %d", errorCode, uint16(tt.errorCode))
			}

			// Verify message
			if tt.expectNullMsg {
				// Should have null string marker (0xFF, 0xFF)
				if len(response) >= 8 && (response[6] != 0xFF || response[7] != 0xFF) {
					t.Errorf("Expected null string marker, got %x %x", response[6], response[7])
				}
			} else {
				// Should have message length and message
				if len(response) >= 8 {
					messageLen := binary.BigEndian.Uint16(response[6:8])
					if messageLen != uint16(len(tt.message)) {
						t.Errorf("Message length = %d, want %d", messageLen, len(tt.message))
					}

					if len(response) >= 8+len(tt.message) {
						actualMessage := string(response[8 : 8+len(tt.message)])
						if actualMessage != tt.message {
							t.Errorf("Message = %q, want %q", actualMessage, tt.message)
						}
					}
				}
			}
		})
	}
}

func TestClassifyNetworkError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected int16
	}{
		{"No error", nil, ErrorCodeNone},
		{"Generic error", errors.New("generic error"), ErrorCodeUnknownServerError},
		{"Connection refused", errors.New("connection refused"), ErrorCodeConnectionRefused},
		{"Connection timeout", errors.New("connection timeout"), ErrorCodeConnectionTimeout},
		{"Network timeout", &timeoutError{}, ErrorCodeRequestTimedOut},
		{"Network error", &networkError{}, ErrorCodeNetworkException},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassifyNetworkError(tt.err); got != tt.expected {
				t.Errorf("ClassifyNetworkError() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestHandleTimeoutError(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		operation string
		expected  int16
	}{
		{"No error", nil, "read", ErrorCodeNone},
		{"Read timeout", &timeoutError{}, "read", ErrorCodeReadTimeout},
		{"Write timeout", &timeoutError{}, "write", ErrorCodeWriteTimeout},
		{"Connect timeout", &timeoutError{}, "connect", ErrorCodeConnectionTimeout},
		{"Generic timeout", &timeoutError{}, "unknown", ErrorCodeRequestTimedOut},
		{"Non-timeout error", errors.New("other error"), "read", ErrorCodeUnknownServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HandleTimeoutError(tt.err, tt.operation); got != tt.expected {
				t.Errorf("HandleTimeoutError() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDefaultTimeoutConfig(t *testing.T) {
	config := DefaultTimeoutConfig()

	if config.ConnectionTimeout != 30*time.Second {
		t.Errorf("ConnectionTimeout = %v, want %v", config.ConnectionTimeout, 30*time.Second)
	}
	if config.ReadTimeout != 10*time.Second {
		t.Errorf("ReadTimeout = %v, want %v", config.ReadTimeout, 10*time.Second)
	}
	if config.WriteTimeout != 10*time.Second {
		t.Errorf("WriteTimeout = %v, want %v", config.WriteTimeout, 10*time.Second)
	}
	if config.RequestTimeout != 30*time.Second {
		t.Errorf("RequestTimeout = %v, want %v", config.RequestTimeout, 30*time.Second)
	}
}

func TestSafeFormatError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{"No error", nil, ""},
		{"Generic error", errors.New("test error"), "Error: test error"},
		{"Network error", &networkError{}, "Error: network error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SafeFormatError(tt.err); got != tt.expected {
				t.Errorf("SafeFormatError() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestGetErrorInfo_UnknownErrorCode(t *testing.T) {
	unknownCode := int16(9999)
	info := GetErrorInfo(unknownCode)

	if info.Code != unknownCode {
		t.Errorf("Code = %d, want %d", info.Code, unknownCode)
	}
	if info.Name != "UNKNOWN" {
		t.Errorf("Name = %s, want UNKNOWN", info.Name)
	}
	if info.Description != "Unknown error code" {
		t.Errorf("Description = %s, want 'Unknown error code'", info.Description)
	}
	if info.Retriable != false {
		t.Errorf("Retriable = %v, want false", info.Retriable)
	}
}

// Integration test for error handling in protocol context
func TestErrorHandling_Integration(t *testing.T) {
	// Test building various protocol error responses
	tests := []struct {
		name      string
		apiKey    uint16
		errorCode int16
		message   string
	}{
		{"ApiVersions error", 18, ErrorCodeUnsupportedVersion, "Version not supported"},
		{"Metadata error", 3, ErrorCodeUnknownTopicOrPartition, "Topic not found"},
		{"Produce error", 0, ErrorCodeMessageTooLarge, "Message exceeds size limit"},
		{"Fetch error", 1, ErrorCodeOffsetOutOfRange, "Offset out of range"},
		{"CreateTopics error", 19, ErrorCodeTopicAlreadyExists, "Topic already exists"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			correlationID := uint32(12345)

			// Test basic error response
			basicResponse := BuildErrorResponse(correlationID, tt.errorCode)
			if len(basicResponse) != 6 {
				t.Errorf("Basic response length = %d, want 6", len(basicResponse))
			}

			// Test error response with message
			messageResponse := BuildErrorResponseWithMessage(correlationID, tt.errorCode, tt.message)
			expectedMinLen := 8 + len(tt.message) // 4 (correlationID) + 2 (errorCode) + 2 (messageLen) + len(message)
			if len(messageResponse) < expectedMinLen {
				t.Errorf("Message response length = %d, want at least %d", len(messageResponse), expectedMinLen)
			}

			// Verify error is correctly classified
			info := GetErrorInfo(tt.errorCode)
			if info.Code != tt.errorCode {
				t.Errorf("Error info code = %d, want %d", info.Code, tt.errorCode)
			}
		})
	}
}

// Mock error types for testing
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "timeout error" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }

type networkError struct{}

func (e *networkError) Error() string   { return "network error" }
func (e *networkError) Timeout() bool   { return false }
func (e *networkError) Temporary() bool { return true }

// Test timeout detection
func TestTimeoutDetection(t *testing.T) {
	// Test with context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// Wait for context to timeout
	time.Sleep(2 * time.Millisecond)

	select {
	case <-ctx.Done():
		err := ctx.Err()
		errorCode := HandleTimeoutError(err, "context")
		if errorCode != ErrorCodeRequestTimedOut {
			t.Errorf("Context timeout error code = %v, want %v", errorCode, ErrorCodeRequestTimedOut)
		}
	default:
		t.Error("Context should have timed out")
	}
}

// Benchmark error response building
func BenchmarkBuildErrorResponse(b *testing.B) {
	correlationID := uint32(12345)
	errorCode := ErrorCodeUnknownServerError

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BuildErrorResponse(correlationID, errorCode)
	}
}

func BenchmarkBuildErrorResponseWithMessage(b *testing.B) {
	correlationID := uint32(12345)
	errorCode := ErrorCodeUnknownServerError
	message := "This is a test error message"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BuildErrorResponseWithMessage(correlationID, errorCode, message)
	}
}

func BenchmarkClassifyNetworkError(b *testing.B) {
	err := &timeoutError{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ClassifyNetworkError(err)
	}
}
