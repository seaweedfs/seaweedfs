package integration

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
)

func TestMapBrokerErrorToKafka(t *testing.T) {
	tests := []struct {
		name            string
		brokerErrorCode int32
		expectedKafka   int16
	}{
		{"No error", 0, kafkaErrorCodeNone},
		{"Unknown server error", 1, kafkaErrorCodeUnknownServerError},
		{"Topic not found", 2, kafkaErrorCodeUnknownTopicOrPartition},
		{"Partition not found", 3, kafkaErrorCodeUnknownTopicOrPartition},
		{"Not leader or follower", 6, kafkaErrorCodeNotLeaderOrFollower},
		{"Request timed out", 7, kafkaErrorCodeRequestTimedOut},
		{"Broker not available", 8, kafkaErrorCodeBrokerNotAvailable},
		{"Message too large", 10, kafkaErrorCodeMessageTooLarge},
		{"Network exception", 13, kafkaErrorCodeNetworkException},
		{"Offset load in progress", 14, kafkaErrorCodeOffsetLoadInProgress},
		{"Invalid record", 42, kafkaErrorCodeInvalidRecord},
		{"Topic already exists", 36, kafkaErrorCodeTopicAlreadyExists},
		{"Invalid partitions", 37, kafkaErrorCodeInvalidPartitions},
		{"Invalid config", 40, kafkaErrorCodeInvalidConfig},
		{"Publisher not found", 100, kafkaErrorCodeUnknownServerError},
		{"Connection failed", 101, kafkaErrorCodeNetworkException},
		{"Follower connection failed", 102, kafkaErrorCodeNetworkException},
		{"Unknown error code", 999, kafkaErrorCodeUnknownServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MapBrokerErrorToKafka(tt.brokerErrorCode)
			if result != tt.expectedKafka {
				t.Errorf("MapBrokerErrorToKafka(%d) = %d, want %d", tt.brokerErrorCode, result, tt.expectedKafka)
			}
		})
	}
}

func TestHandleBrokerResponse(t *testing.T) {
	tests := []struct {
		name              string
		response          *mq_pb.PublishMessageResponse
		expectedKafkaCode int16
		expectedError     string
		expectSystemError bool
	}{
		{
			name: "No error",
			response: &mq_pb.PublishMessageResponse{
				AckTsNs:   123,
				Error:     "",
				ErrorCode: 0,
			},
			expectedKafkaCode: kafkaErrorCodeNone,
			expectedError:     "",
			expectSystemError: false,
		},
		{
			name: "Structured error - Not leader",
			response: &mq_pb.PublishMessageResponse{
				AckTsNs:   0,
				Error:     "not the leader for this partition, leader is: broker2:9092",
				ErrorCode: 6, // BrokerErrorNotLeaderOrFollower
			},
			expectedKafkaCode: kafkaErrorCodeNotLeaderOrFollower,
			expectedError:     "not the leader for this partition, leader is: broker2:9092",
			expectSystemError: false,
		},
		{
			name: "Structured error - Topic not found",
			response: &mq_pb.PublishMessageResponse{
				AckTsNs:   0,
				Error:     "topic test-topic not found",
				ErrorCode: 2, // BrokerErrorTopicNotFound
			},
			expectedKafkaCode: kafkaErrorCodeUnknownTopicOrPartition,
			expectedError:     "topic test-topic not found",
			expectSystemError: false,
		},
		{
			name: "Fallback string parsing - Not leader",
			response: &mq_pb.PublishMessageResponse{
				AckTsNs:   0,
				Error:     "not the leader for this partition",
				ErrorCode: 0, // No structured error code
			},
			expectedKafkaCode: kafkaErrorCodeNotLeaderOrFollower,
			expectedError:     "not the leader for this partition",
			expectSystemError: false,
		},
		{
			name: "Fallback string parsing - Topic not found",
			response: &mq_pb.PublishMessageResponse{
				AckTsNs:   0,
				Error:     "topic does not exist",
				ErrorCode: 0, // No structured error code
			},
			expectedKafkaCode: kafkaErrorCodeUnknownTopicOrPartition,
			expectedError:     "topic does not exist",
			expectSystemError: false,
		},
		{
			name: "Fallback string parsing - Unknown error",
			response: &mq_pb.PublishMessageResponse{
				AckTsNs:   0,
				Error:     "some unknown error occurred",
				ErrorCode: 0, // No structured error code
			},
			expectedKafkaCode: kafkaErrorCodeUnknownServerError,
			expectedError:     "some unknown error occurred",
			expectSystemError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kafkaCode, errorMsg, systemErr := HandleBrokerResponse(tt.response)

			if kafkaCode != tt.expectedKafkaCode {
				t.Errorf("HandleBrokerResponse() kafkaCode = %d, want %d", kafkaCode, tt.expectedKafkaCode)
			}

			if errorMsg != tt.expectedError {
				t.Errorf("HandleBrokerResponse() errorMsg = %q, want %q", errorMsg, tt.expectedError)
			}

			if (systemErr != nil) != tt.expectSystemError {
				t.Errorf("HandleBrokerResponse() systemErr = %v, expectSystemError = %v", systemErr, tt.expectSystemError)
			}
		})
	}
}

func TestParseStringErrorToKafkaCode(t *testing.T) {
	tests := []struct {
		name         string
		errorMsg     string
		expectedCode int16
	}{
		{"Empty error", "", kafkaErrorCodeNone},
		{"Not leader error", "not the leader for this partition", kafkaErrorCodeNotLeaderOrFollower},
		{"Not leader error variant", "not leader", kafkaErrorCodeNotLeaderOrFollower},
		{"Topic not found", "topic not found", kafkaErrorCodeUnknownTopicOrPartition},
		{"Topic does not exist", "topic does not exist", kafkaErrorCodeUnknownTopicOrPartition},
		{"Partition not found", "partition not found", kafkaErrorCodeUnknownTopicOrPartition},
		{"Timeout error", "request timed out", kafkaErrorCodeRequestTimedOut},
		{"Timeout error variant", "timeout occurred", kafkaErrorCodeRequestTimedOut},
		{"Network error", "network exception", kafkaErrorCodeNetworkException},
		{"Connection error", "connection failed", kafkaErrorCodeNetworkException},
		{"Message too large", "message too large", kafkaErrorCodeMessageTooLarge},
		{"Size error", "size exceeds limit", kafkaErrorCodeMessageTooLarge},
		{"Unknown error", "some random error", kafkaErrorCodeUnknownServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseStringErrorToKafkaCode(tt.errorMsg)
			if result != tt.expectedCode {
				t.Errorf("parseStringErrorToKafkaCode(%q) = %d, want %d", tt.errorMsg, result, tt.expectedCode)
			}
		})
	}
}
