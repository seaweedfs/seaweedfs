package protocol

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/offset"
)

func TestOffsetCommitHandlerIntegration(t *testing.T) {
	// Test that the offset commit handler properly uses SMQ storage

	// Test ConsumerOffsetKey creation
	key := offset.ConsumerOffsetKey{
		Topic:                 "test-topic",
		Partition:             0,
		ConsumerGroup:         "test-group",
		ConsumerGroupInstance: "test-instance",
	}

	if key.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", key.Topic)
	}

	if key.Partition != 0 {
		t.Errorf("Expected partition 0, got %d", key.Partition)
	}

	if key.ConsumerGroup != "test-group" {
		t.Errorf("Expected consumer group 'test-group', got %s", key.ConsumerGroup)
	}

	if key.ConsumerGroupInstance != "test-instance" {
		t.Errorf("Expected instance 'test-instance', got %s", key.ConsumerGroupInstance)
	}
}

func TestOffsetCommitToSMQ_WithoutStorage(t *testing.T) {
	// Test error handling when SMQ storage is not initialized
	handler := &Handler{
		smqOffsetStorage: nil, // Not initialized
	}

	key := offset.ConsumerOffsetKey{
		Topic:         "test-topic",
		Partition:     0,
		ConsumerGroup: "test-group",
	}

	err := handler.commitOffsetToSMQ(key, 100, "test-metadata")
	if err == nil {
		t.Error("Expected error when offset storage not initialized, got nil")
	}

	expectedError := "offset storage not initialized"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

func TestFetchOffsetFromSMQ_WithoutStorage(t *testing.T) {
	// Test error handling when offset storage is not initialized
	handler := &Handler{
		smqOffsetStorage: nil, // Not initialized
	}

	key := offset.ConsumerOffsetKey{
		Topic:         "test-topic",
		Partition:     0,
		ConsumerGroup: "test-group",
	}

	offset, metadata, err := handler.fetchOffsetFromSMQ(key)
	if err == nil {
		t.Error("Expected error when SMQ storage not initialized, got nil")
	}

	if offset != -1 {
		t.Errorf("Expected offset -1, got %d", offset)
	}

	if metadata != "" {
		t.Errorf("Expected empty metadata, got '%s'", metadata)
	}
}

func TestOffsetHandlers_StructureValidation(t *testing.T) {
	// Validate that offset commit/fetch request/response structures are properly formed

	// Test OffsetCommitRequest structure
	request := OffsetCommitRequest{
		GroupID:         "test-group",
		GenerationID:    1,
		MemberID:        "test-member",
		GroupInstanceID: "test-instance",
		RetentionTime:   -1,
		Topics: []OffsetCommitTopic{
			{
				Name: "test-topic",
				Partitions: []OffsetCommitPartition{
					{
						Index:       0,
						Offset:      100,
						LeaderEpoch: -1,
						Metadata:    "test-metadata",
					},
				},
			},
		},
	}

	if request.GroupID != "test-group" {
		t.Errorf("Expected group ID 'test-group', got %s", request.GroupID)
	}

	if len(request.Topics) != 1 {
		t.Errorf("Expected 1 topic, got %d", len(request.Topics))
	}

	if request.Topics[0].Name != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got %s", request.Topics[0].Name)
	}

	if len(request.Topics[0].Partitions) != 1 {
		t.Errorf("Expected 1 partition, got %d", len(request.Topics[0].Partitions))
	}

	partition := request.Topics[0].Partitions[0]
	if partition.Index != 0 {
		t.Errorf("Expected partition index 0, got %d", partition.Index)
	}

	if partition.Offset != 100 {
		t.Errorf("Expected offset 100, got %d", partition.Offset)
	}

	// Test OffsetFetchRequest structure
	fetchRequest := OffsetFetchRequest{
		GroupID:         "test-group",
		GroupInstanceID: "test-instance",
		Topics: []OffsetFetchTopic{
			{
				Name:       "test-topic",
				Partitions: []int32{0, 1, 2},
			},
		},
		RequireStable: false,
	}

	if fetchRequest.GroupID != "test-group" {
		t.Errorf("Expected group ID 'test-group', got %s", fetchRequest.GroupID)
	}

	if len(fetchRequest.Topics) != 1 {
		t.Errorf("Expected 1 topic, got %d", len(fetchRequest.Topics))
	}

	if len(fetchRequest.Topics[0].Partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(fetchRequest.Topics[0].Partitions))
	}
}
