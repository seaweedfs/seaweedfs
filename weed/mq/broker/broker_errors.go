package broker

// Broker Error Codes
// These codes are used internally by the broker and can be mapped to Kafka protocol error codes
const (
	// Success
	BrokerErrorNone int32 = 0

	// General broker errors
	BrokerErrorUnknownServerError   int32 = 1
	BrokerErrorTopicNotFound        int32 = 2
	BrokerErrorPartitionNotFound    int32 = 3
	BrokerErrorNotLeaderOrFollower  int32 = 6 // Maps to Kafka ErrorCodeNotLeaderOrFollower
	BrokerErrorRequestTimedOut      int32 = 7
	BrokerErrorBrokerNotAvailable   int32 = 8
	BrokerErrorMessageTooLarge      int32 = 10
	BrokerErrorNetworkException     int32 = 13
	BrokerErrorOffsetLoadInProgress int32 = 14
	BrokerErrorInvalidRecord        int32 = 42
	BrokerErrorTopicAlreadyExists   int32 = 36
	BrokerErrorInvalidPartitions    int32 = 37
	BrokerErrorInvalidConfig        int32 = 40

	// Publisher/connection errors
	BrokerErrorPublisherNotFound        int32 = 100
	BrokerErrorConnectionFailed         int32 = 101
	BrokerErrorFollowerConnectionFailed int32 = 102
)

// BrokerErrorInfo contains metadata about a broker error
type BrokerErrorInfo struct {
	Code        int32
	Name        string
	Description string
	KafkaCode   int16 // Corresponding Kafka protocol error code
}

// BrokerErrors maps broker error codes to their metadata and Kafka equivalents
var BrokerErrors = map[int32]BrokerErrorInfo{
	BrokerErrorNone: {
		Code: BrokerErrorNone, Name: "NONE",
		Description: "No error", KafkaCode: 0,
	},
	BrokerErrorUnknownServerError: {
		Code: BrokerErrorUnknownServerError, Name: "UNKNOWN_SERVER_ERROR",
		Description: "Unknown server error", KafkaCode: 1,
	},
	BrokerErrorTopicNotFound: {
		Code: BrokerErrorTopicNotFound, Name: "TOPIC_NOT_FOUND",
		Description: "Topic not found", KafkaCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
	},
	BrokerErrorPartitionNotFound: {
		Code: BrokerErrorPartitionNotFound, Name: "PARTITION_NOT_FOUND",
		Description: "Partition not found", KafkaCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
	},
	BrokerErrorNotLeaderOrFollower: {
		Code: BrokerErrorNotLeaderOrFollower, Name: "NOT_LEADER_OR_FOLLOWER",
		Description: "Not leader or follower for this partition", KafkaCode: 6,
	},
	BrokerErrorRequestTimedOut: {
		Code: BrokerErrorRequestTimedOut, Name: "REQUEST_TIMED_OUT",
		Description: "Request timed out", KafkaCode: 7,
	},
	BrokerErrorBrokerNotAvailable: {
		Code: BrokerErrorBrokerNotAvailable, Name: "BROKER_NOT_AVAILABLE",
		Description: "Broker not available", KafkaCode: 8,
	},
	BrokerErrorMessageTooLarge: {
		Code: BrokerErrorMessageTooLarge, Name: "MESSAGE_TOO_LARGE",
		Description: "Message size exceeds limit", KafkaCode: 10,
	},
	BrokerErrorNetworkException: {
		Code: BrokerErrorNetworkException, Name: "NETWORK_EXCEPTION",
		Description: "Network error", KafkaCode: 13,
	},
	BrokerErrorOffsetLoadInProgress: {
		Code: BrokerErrorOffsetLoadInProgress, Name: "OFFSET_LOAD_IN_PROGRESS",
		Description: "Offset loading in progress", KafkaCode: 14,
	},
	BrokerErrorInvalidRecord: {
		Code: BrokerErrorInvalidRecord, Name: "INVALID_RECORD",
		Description: "Invalid record", KafkaCode: 42,
	},
	BrokerErrorTopicAlreadyExists: {
		Code: BrokerErrorTopicAlreadyExists, Name: "TOPIC_ALREADY_EXISTS",
		Description: "Topic already exists", KafkaCode: 36,
	},
	BrokerErrorInvalidPartitions: {
		Code: BrokerErrorInvalidPartitions, Name: "INVALID_PARTITIONS",
		Description: "Invalid partition count", KafkaCode: 37,
	},
	BrokerErrorInvalidConfig: {
		Code: BrokerErrorInvalidConfig, Name: "INVALID_CONFIG",
		Description: "Invalid configuration", KafkaCode: 40,
	},
	BrokerErrorPublisherNotFound: {
		Code: BrokerErrorPublisherNotFound, Name: "PUBLISHER_NOT_FOUND",
		Description: "Publisher not found", KafkaCode: 1, // UNKNOWN_SERVER_ERROR
	},
	BrokerErrorConnectionFailed: {
		Code: BrokerErrorConnectionFailed, Name: "CONNECTION_FAILED",
		Description: "Connection failed", KafkaCode: 13, // NETWORK_EXCEPTION
	},
	BrokerErrorFollowerConnectionFailed: {
		Code: BrokerErrorFollowerConnectionFailed, Name: "FOLLOWER_CONNECTION_FAILED",
		Description: "Failed to connect to follower brokers", KafkaCode: 13, // NETWORK_EXCEPTION
	},
}

// GetBrokerErrorInfo returns error information for the given broker error code
func GetBrokerErrorInfo(code int32) BrokerErrorInfo {
	if info, exists := BrokerErrors[code]; exists {
		return info
	}
	return BrokerErrorInfo{
		Code: code, Name: "UNKNOWN", Description: "Unknown broker error code", KafkaCode: 1,
	}
}

// GetKafkaErrorCode returns the corresponding Kafka protocol error code for a broker error
func GetKafkaErrorCode(brokerErrorCode int32) int16 {
	return GetBrokerErrorInfo(brokerErrorCode).KafkaCode
}

// CreateBrokerError creates a structured broker error with both error code and message
func CreateBrokerError(code int32, message string) (int32, string) {
	info := GetBrokerErrorInfo(code)
	if message == "" {
		message = info.Description
	}
	return code, message
}
