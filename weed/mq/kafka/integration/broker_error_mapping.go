package integration

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

// Kafka Protocol Error Codes (copied from protocol package to avoid import cycle)
const (
	kafkaErrorCodeNone                    int16 = 0
	kafkaErrorCodeUnknownServerError      int16 = 1
	kafkaErrorCodeUnknownTopicOrPartition int16 = 3
	kafkaErrorCodeNotLeaderOrFollower     int16 = 6
	kafkaErrorCodeRequestTimedOut         int16 = 7
	kafkaErrorCodeBrokerNotAvailable      int16 = 8
	kafkaErrorCodeMessageTooLarge         int16 = 10
	kafkaErrorCodeNetworkException        int16 = 13
	kafkaErrorCodeOffsetLoadInProgress    int16 = 14
	kafkaErrorCodeTopicAlreadyExists      int16 = 36
	kafkaErrorCodeInvalidPartitions       int16 = 37
	kafkaErrorCodeInvalidConfig           int16 = 40
	kafkaErrorCodeInvalidRecord           int16 = 42
)

// MapBrokerErrorToKafka maps a broker error code to the corresponding Kafka protocol error code
func MapBrokerErrorToKafka(brokerErrorCode int32) int16 {
	switch brokerErrorCode {
	case 0: // BrokerErrorNone
		return kafkaErrorCodeNone
	case 1: // BrokerErrorUnknownServerError
		return kafkaErrorCodeUnknownServerError
	case 2: // BrokerErrorTopicNotFound
		return kafkaErrorCodeUnknownTopicOrPartition
	case 3: // BrokerErrorPartitionNotFound
		return kafkaErrorCodeUnknownTopicOrPartition
	case 6: // BrokerErrorNotLeaderOrFollower
		return kafkaErrorCodeNotLeaderOrFollower
	case 7: // BrokerErrorRequestTimedOut
		return kafkaErrorCodeRequestTimedOut
	case 8: // BrokerErrorBrokerNotAvailable
		return kafkaErrorCodeBrokerNotAvailable
	case 10: // BrokerErrorMessageTooLarge
		return kafkaErrorCodeMessageTooLarge
	case 13: // BrokerErrorNetworkException
		return kafkaErrorCodeNetworkException
	case 14: // BrokerErrorOffsetLoadInProgress
		return kafkaErrorCodeOffsetLoadInProgress
	case 42: // BrokerErrorInvalidRecord
		return kafkaErrorCodeInvalidRecord
	case 36: // BrokerErrorTopicAlreadyExists
		return kafkaErrorCodeTopicAlreadyExists
	case 37: // BrokerErrorInvalidPartitions
		return kafkaErrorCodeInvalidPartitions
	case 40: // BrokerErrorInvalidConfig
		return kafkaErrorCodeInvalidConfig
	case 100: // BrokerErrorPublisherNotFound
		return kafkaErrorCodeUnknownServerError
	case 101: // BrokerErrorConnectionFailed
		return kafkaErrorCodeNetworkException
	case 102: // BrokerErrorFollowerConnectionFailed
		return kafkaErrorCodeNetworkException
	default:
		// Unknown broker error code, default to unknown server error
		return kafkaErrorCodeUnknownServerError
	}
}

// HandleBrokerResponse processes a broker response and returns appropriate error information
// Returns (kafkaErrorCode, errorMessage, error) where error is non-nil for system errors
func HandleBrokerResponse(resp *mq_pb.PublishMessageResponse) (int16, string, error) {
	if resp.Error == "" && resp.ErrorCode == 0 {
		// No error
		return kafkaErrorCodeNone, "", nil
	}

	// Use structured error code if available, otherwise fall back to string parsing
	if resp.ErrorCode != 0 {
		kafkaErrorCode := MapBrokerErrorToKafka(resp.ErrorCode)
		return kafkaErrorCode, resp.Error, nil
	}

	// Fallback: parse string error for backward compatibility
	// This handles cases where older brokers might not set ErrorCode
	kafkaErrorCode := parseStringErrorToKafkaCode(resp.Error)
	return kafkaErrorCode, resp.Error, nil
}

// parseStringErrorToKafkaCode provides backward compatibility for string-based error parsing
// This is the old brittle approach that we're replacing with structured error codes
func parseStringErrorToKafkaCode(errorMsg string) int16 {
	if errorMsg == "" {
		return kafkaErrorCodeNone
	}

	// Check for common error patterns (brittle string matching)
	switch {
	case containsAny(errorMsg, "not the leader", "not leader"):
		return kafkaErrorCodeNotLeaderOrFollower
	case containsAny(errorMsg, "topic", "not found", "does not exist"):
		return kafkaErrorCodeUnknownTopicOrPartition
	case containsAny(errorMsg, "partition", "not found"):
		return kafkaErrorCodeUnknownTopicOrPartition
	case containsAny(errorMsg, "timeout", "timed out"):
		return kafkaErrorCodeRequestTimedOut
	case containsAny(errorMsg, "network", "connection"):
		return kafkaErrorCodeNetworkException
	case containsAny(errorMsg, "too large", "size"):
		return kafkaErrorCodeMessageTooLarge
	default:
		return kafkaErrorCodeUnknownServerError
	}
}

// containsAny checks if the text contains any of the given substrings (case-insensitive)
func containsAny(text string, substrings ...string) bool {
	textLower := strings.ToLower(text)
	for _, substr := range substrings {
		if strings.Contains(textLower, strings.ToLower(substr)) {
			return true
		}
	}
	return false
}
