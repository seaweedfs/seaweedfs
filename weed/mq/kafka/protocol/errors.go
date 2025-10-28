package protocol

import (
	"context"
	"encoding/binary"
	"net"
	"time"
)

// Kafka Protocol Error Codes
// Based on Apache Kafka protocol specification
const (
	// Success
	ErrorCodeNone int16 = 0

	// General server errors
	ErrorCodeUnknownServerError           int16 = -1
	ErrorCodeOffsetOutOfRange             int16 = 1
	ErrorCodeCorruptMessage               int16 = 3 // Also UNKNOWN_TOPIC_OR_PARTITION
	ErrorCodeUnknownTopicOrPartition      int16 = 3
	ErrorCodeInvalidFetchSize             int16 = 4
	ErrorCodeLeaderNotAvailable           int16 = 5
	ErrorCodeNotLeaderOrFollower          int16 = 6 // Formerly NOT_LEADER_FOR_PARTITION
	ErrorCodeRequestTimedOut              int16 = 7
	ErrorCodeBrokerNotAvailable           int16 = 8
	ErrorCodeReplicaNotAvailable          int16 = 9
	ErrorCodeMessageTooLarge              int16 = 10
	ErrorCodeStaleControllerEpoch         int16 = 11
	ErrorCodeOffsetMetadataTooLarge       int16 = 12
	ErrorCodeNetworkException             int16 = 13
	ErrorCodeOffsetLoadInProgress         int16 = 14
	ErrorCodeGroupLoadInProgress          int16 = 15
	ErrorCodeNotCoordinatorForGroup       int16 = 16
	ErrorCodeNotCoordinatorForTransaction int16 = 17

	// Consumer group coordination errors
	ErrorCodeIllegalGeneration          int16 = 22
	ErrorCodeInconsistentGroupProtocol  int16 = 23
	ErrorCodeInvalidGroupID             int16 = 24
	ErrorCodeUnknownMemberID            int16 = 25
	ErrorCodeInvalidSessionTimeout      int16 = 26
	ErrorCodeRebalanceInProgress        int16 = 27
	ErrorCodeInvalidCommitOffsetSize    int16 = 28
	ErrorCodeTopicAuthorizationFailed   int16 = 29
	ErrorCodeGroupAuthorizationFailed   int16 = 30
	ErrorCodeClusterAuthorizationFailed int16 = 31
	ErrorCodeInvalidTimestamp           int16 = 32
	ErrorCodeUnsupportedSASLMechanism   int16 = 33
	ErrorCodeIllegalSASLState           int16 = 34
	ErrorCodeUnsupportedVersion         int16 = 35

	// Topic management errors
	ErrorCodeTopicAlreadyExists        int16 = 36
	ErrorCodeInvalidPartitions         int16 = 37
	ErrorCodeInvalidReplicationFactor  int16 = 38
	ErrorCodeInvalidReplicaAssignment  int16 = 39
	ErrorCodeInvalidConfig             int16 = 40
	ErrorCodeNotController             int16 = 41
	ErrorCodeInvalidRecord             int16 = 42
	ErrorCodePolicyViolation           int16 = 43
	ErrorCodeOutOfOrderSequenceNumber  int16 = 44
	ErrorCodeDuplicateSequenceNumber   int16 = 45
	ErrorCodeInvalidProducerEpoch      int16 = 46
	ErrorCodeInvalidTxnState           int16 = 47
	ErrorCodeInvalidProducerIDMapping  int16 = 48
	ErrorCodeInvalidTransactionTimeout int16 = 49
	ErrorCodeConcurrentTransactions    int16 = 50

	// Connection and timeout errors
	ErrorCodeConnectionRefused int16 = 60 // Custom for connection issues
	ErrorCodeConnectionTimeout int16 = 61 // Custom for connection timeouts
	ErrorCodeReadTimeout       int16 = 62 // Custom for read timeouts
	ErrorCodeWriteTimeout      int16 = 63 // Custom for write timeouts

	// Consumer group specific errors
	ErrorCodeMemberIDRequired     int16 = 79
	ErrorCodeFencedInstanceID     int16 = 82
	ErrorCodeGroupMaxSizeReached  int16 = 84
	ErrorCodeUnstableOffsetCommit int16 = 95
)

// ErrorInfo contains metadata about a Kafka error
type ErrorInfo struct {
	Code        int16
	Name        string
	Description string
	Retriable   bool
}

// KafkaErrors maps error codes to their metadata
var KafkaErrors = map[int16]ErrorInfo{
	ErrorCodeNone: {
		Code: ErrorCodeNone, Name: "NONE", Description: "No error", Retriable: false,
	},
	ErrorCodeUnknownServerError: {
		Code: ErrorCodeUnknownServerError, Name: "UNKNOWN_SERVER_ERROR",
		Description: "Unknown server error", Retriable: true,
	},
	ErrorCodeOffsetOutOfRange: {
		Code: ErrorCodeOffsetOutOfRange, Name: "OFFSET_OUT_OF_RANGE",
		Description: "Offset out of range", Retriable: false,
	},
	ErrorCodeUnknownTopicOrPartition: {
		Code: ErrorCodeUnknownTopicOrPartition, Name: "UNKNOWN_TOPIC_OR_PARTITION",
		Description: "Topic or partition does not exist", Retriable: false,
	},
	ErrorCodeInvalidFetchSize: {
		Code: ErrorCodeInvalidFetchSize, Name: "INVALID_FETCH_SIZE",
		Description: "Invalid fetch size", Retriable: false,
	},
	ErrorCodeLeaderNotAvailable: {
		Code: ErrorCodeLeaderNotAvailable, Name: "LEADER_NOT_AVAILABLE",
		Description: "Leader not available", Retriable: true,
	},
	ErrorCodeNotLeaderOrFollower: {
		Code: ErrorCodeNotLeaderOrFollower, Name: "NOT_LEADER_OR_FOLLOWER",
		Description: "Not leader or follower", Retriable: true,
	},
	ErrorCodeRequestTimedOut: {
		Code: ErrorCodeRequestTimedOut, Name: "REQUEST_TIMED_OUT",
		Description: "Request timed out", Retriable: true,
	},
	ErrorCodeBrokerNotAvailable: {
		Code: ErrorCodeBrokerNotAvailable, Name: "BROKER_NOT_AVAILABLE",
		Description: "Broker not available", Retriable: true,
	},
	ErrorCodeMessageTooLarge: {
		Code: ErrorCodeMessageTooLarge, Name: "MESSAGE_TOO_LARGE",
		Description: "Message size exceeds limit", Retriable: false,
	},
	ErrorCodeOffsetMetadataTooLarge: {
		Code: ErrorCodeOffsetMetadataTooLarge, Name: "OFFSET_METADATA_TOO_LARGE",
		Description: "Offset metadata too large", Retriable: false,
	},
	ErrorCodeNetworkException: {
		Code: ErrorCodeNetworkException, Name: "NETWORK_EXCEPTION",
		Description: "Network error", Retriable: true,
	},
	ErrorCodeOffsetLoadInProgress: {
		Code: ErrorCodeOffsetLoadInProgress, Name: "OFFSET_LOAD_IN_PROGRESS",
		Description: "Offset load in progress", Retriable: true,
	},
	ErrorCodeNotCoordinatorForGroup: {
		Code: ErrorCodeNotCoordinatorForGroup, Name: "NOT_COORDINATOR_FOR_GROUP",
		Description: "Not coordinator for group", Retriable: true,
	},
	ErrorCodeInvalidGroupID: {
		Code: ErrorCodeInvalidGroupID, Name: "INVALID_GROUP_ID",
		Description: "Invalid group ID", Retriable: false,
	},
	ErrorCodeUnknownMemberID: {
		Code: ErrorCodeUnknownMemberID, Name: "UNKNOWN_MEMBER_ID",
		Description: "Unknown member ID", Retriable: false,
	},
	ErrorCodeInvalidSessionTimeout: {
		Code: ErrorCodeInvalidSessionTimeout, Name: "INVALID_SESSION_TIMEOUT",
		Description: "Invalid session timeout", Retriable: false,
	},
	ErrorCodeRebalanceInProgress: {
		Code: ErrorCodeRebalanceInProgress, Name: "REBALANCE_IN_PROGRESS",
		Description: "Group rebalance in progress", Retriable: true,
	},
	ErrorCodeInvalidCommitOffsetSize: {
		Code: ErrorCodeInvalidCommitOffsetSize, Name: "INVALID_COMMIT_OFFSET_SIZE",
		Description: "Invalid commit offset size", Retriable: false,
	},
	ErrorCodeTopicAuthorizationFailed: {
		Code: ErrorCodeTopicAuthorizationFailed, Name: "TOPIC_AUTHORIZATION_FAILED",
		Description: "Topic authorization failed", Retriable: false,
	},
	ErrorCodeGroupAuthorizationFailed: {
		Code: ErrorCodeGroupAuthorizationFailed, Name: "GROUP_AUTHORIZATION_FAILED",
		Description: "Group authorization failed", Retriable: false,
	},
	ErrorCodeUnsupportedVersion: {
		Code: ErrorCodeUnsupportedVersion, Name: "UNSUPPORTED_VERSION",
		Description: "Unsupported version", Retriable: false,
	},
	ErrorCodeTopicAlreadyExists: {
		Code: ErrorCodeTopicAlreadyExists, Name: "TOPIC_ALREADY_EXISTS",
		Description: "Topic already exists", Retriable: false,
	},
	ErrorCodeInvalidPartitions: {
		Code: ErrorCodeInvalidPartitions, Name: "INVALID_PARTITIONS",
		Description: "Invalid number of partitions", Retriable: false,
	},
	ErrorCodeInvalidReplicationFactor: {
		Code: ErrorCodeInvalidReplicationFactor, Name: "INVALID_REPLICATION_FACTOR",
		Description: "Invalid replication factor", Retriable: false,
	},
	ErrorCodeInvalidRecord: {
		Code: ErrorCodeInvalidRecord, Name: "INVALID_RECORD",
		Description: "Invalid record", Retriable: false,
	},
	ErrorCodeConnectionRefused: {
		Code: ErrorCodeConnectionRefused, Name: "CONNECTION_REFUSED",
		Description: "Connection refused", Retriable: true,
	},
	ErrorCodeConnectionTimeout: {
		Code: ErrorCodeConnectionTimeout, Name: "CONNECTION_TIMEOUT",
		Description: "Connection timeout", Retriable: true,
	},
	ErrorCodeReadTimeout: {
		Code: ErrorCodeReadTimeout, Name: "READ_TIMEOUT",
		Description: "Read operation timeout", Retriable: true,
	},
	ErrorCodeWriteTimeout: {
		Code: ErrorCodeWriteTimeout, Name: "WRITE_TIMEOUT",
		Description: "Write operation timeout", Retriable: true,
	},
	ErrorCodeIllegalGeneration: {
		Code: ErrorCodeIllegalGeneration, Name: "ILLEGAL_GENERATION",
		Description: "Illegal generation", Retriable: false,
	},
	ErrorCodeInconsistentGroupProtocol: {
		Code: ErrorCodeInconsistentGroupProtocol, Name: "INCONSISTENT_GROUP_PROTOCOL",
		Description: "Inconsistent group protocol", Retriable: false,
	},
	ErrorCodeMemberIDRequired: {
		Code: ErrorCodeMemberIDRequired, Name: "MEMBER_ID_REQUIRED",
		Description: "Member ID required", Retriable: false,
	},
	ErrorCodeFencedInstanceID: {
		Code: ErrorCodeFencedInstanceID, Name: "FENCED_INSTANCE_ID",
		Description: "Instance ID fenced", Retriable: false,
	},
	ErrorCodeGroupMaxSizeReached: {
		Code: ErrorCodeGroupMaxSizeReached, Name: "GROUP_MAX_SIZE_REACHED",
		Description: "Group max size reached", Retriable: false,
	},
	ErrorCodeUnstableOffsetCommit: {
		Code: ErrorCodeUnstableOffsetCommit, Name: "UNSTABLE_OFFSET_COMMIT",
		Description: "Offset commit during rebalance", Retriable: true,
	},
}

// GetErrorInfo returns error information for the given error code
func GetErrorInfo(code int16) ErrorInfo {
	if info, exists := KafkaErrors[code]; exists {
		return info
	}
	return ErrorInfo{
		Code: code, Name: "UNKNOWN", Description: "Unknown error code", Retriable: false,
	}
}

// IsRetriableError returns true if the error is retriable
func IsRetriableError(code int16) bool {
	return GetErrorInfo(code).Retriable
}

// BuildErrorResponse builds a standard Kafka error response
func BuildErrorResponse(correlationID uint32, errorCode int16) []byte {
	response := make([]byte, 0, 8)

	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// Error code (2 bytes)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, uint16(errorCode))
	response = append(response, errorCodeBytes...)

	return response
}

// BuildErrorResponseWithMessage builds a Kafka error response with error message
func BuildErrorResponseWithMessage(correlationID uint32, errorCode int16, message string) []byte {
	response := BuildErrorResponse(correlationID, errorCode)

	// Error message (2 bytes length + message)
	if message == "" {
		response = append(response, 0xFF, 0xFF) // Null string
	} else {
		messageLen := uint16(len(message))
		messageLenBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(messageLenBytes, messageLen)
		response = append(response, messageLenBytes...)
		response = append(response, []byte(message)...)
	}

	return response
}

// ClassifyNetworkError classifies network errors into appropriate Kafka error codes
func ClassifyNetworkError(err error) int16 {
	if err == nil {
		return ErrorCodeNone
	}

	// Check for network errors
	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			return ErrorCodeRequestTimedOut
		}
		return ErrorCodeNetworkException
	}

	// Check for specific error types
	switch err.Error() {
	case "connection refused":
		return ErrorCodeConnectionRefused
	case "connection timeout":
		return ErrorCodeConnectionTimeout
	default:
		return ErrorCodeUnknownServerError
	}
}

// TimeoutConfig holds timeout configuration for connections and operations
type TimeoutConfig struct {
	ConnectionTimeout time.Duration // Timeout for establishing connections
	ReadTimeout       time.Duration // Timeout for read operations
	WriteTimeout      time.Duration // Timeout for write operations
	RequestTimeout    time.Duration // Overall request timeout
}

// DefaultTimeoutConfig returns default timeout configuration
func DefaultTimeoutConfig() TimeoutConfig {
	return TimeoutConfig{
		ConnectionTimeout: 30 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		RequestTimeout:    30 * time.Second,
	}
}

// HandleTimeoutError handles timeout errors and returns appropriate error code
func HandleTimeoutError(err error, operation string) int16 {
	if err == nil {
		return ErrorCodeNone
	}

	// Handle context timeout errors
	if err == context.DeadlineExceeded {
		switch operation {
		case "read":
			return ErrorCodeReadTimeout
		case "write":
			return ErrorCodeWriteTimeout
		case "connect":
			return ErrorCodeConnectionTimeout
		default:
			return ErrorCodeRequestTimedOut
		}
	}

	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		switch operation {
		case "read":
			return ErrorCodeReadTimeout
		case "write":
			return ErrorCodeWriteTimeout
		case "connect":
			return ErrorCodeConnectionTimeout
		default:
			return ErrorCodeRequestTimedOut
		}
	}

	return ClassifyNetworkError(err)
}
