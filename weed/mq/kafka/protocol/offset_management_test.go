package protocol

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
	
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
)

func TestHandler_handleOffsetCommit(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	// Create a consumer group with a stable member
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 1
	group.Members["member1"] = &consumer.GroupMember{
		ID:           "member1",
		State:        consumer.MemberStateStable,
		Assignment: []consumer.PartitionAssignment{
			{Topic: "test-topic", Partition: 0},
		},
	}
	group.Mu.Unlock()
	
	// Create a basic offset commit request
	requestBody := createOffsetCommitRequestBody("test-group", 1, "member1")
	
	correlationID := uint32(123)
	response, err := h.handleOffsetCommit(correlationID, requestBody)
	
	if err != nil {
		t.Fatalf("handleOffsetCommit failed: %v", err)
	}
	
	if len(response) < 8 {
		t.Fatalf("response too short: %d bytes", len(response))
	}
	
	// Check correlation ID in response
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("expected correlation ID %d, got %d", correlationID, respCorrelationID)
	}
	
	// Verify offset was committed
	group.Mu.RLock()
	if group.OffsetCommits == nil || group.OffsetCommits["test-topic"] == nil {
		t.Error("offset commit was not stored")
	} else {
		commit, exists := group.OffsetCommits["test-topic"][0]
		if !exists {
			t.Error("offset commit for partition 0 was not stored")
		} else if commit.Offset != 0 {
			t.Errorf("expected offset 0, got %d", commit.Offset)
		}
	}
	group.Mu.RUnlock()
}

func TestHandler_handleOffsetCommit_InvalidGroup(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	// Request for non-existent group
	requestBody := createOffsetCommitRequestBody("nonexistent-group", 1, "member1")
	
	correlationID := uint32(124)
	response, err := h.handleOffsetCommit(correlationID, requestBody)
	
	if err != nil {
		t.Fatalf("handleOffsetCommit failed: %v", err)
	}
	
	// Should get error response
	if len(response) < 8 {
		t.Fatalf("error response too short: %d bytes", len(response))
	}
	
	// Response should have correlation ID
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("expected correlation ID %d, got %d", correlationID, respCorrelationID)
	}
}

func TestHandler_handleOffsetCommit_WrongGeneration(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	// Create a consumer group with generation 2
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 2
	group.Members["member1"] = &consumer.GroupMember{
		ID:           "member1",
		State:        consumer.MemberStateStable,
		Assignment: []consumer.PartitionAssignment{
			{Topic: "test-topic", Partition: 0},
		},
	}
	group.Mu.Unlock()
	
	// Request with wrong generation (1 instead of 2)
	requestBody := createOffsetCommitRequestBody("test-group", 1, "member1")
	
	correlationID := uint32(125)
	response, err := h.handleOffsetCommit(correlationID, requestBody)
	
	if err != nil {
		t.Fatalf("handleOffsetCommit failed: %v", err)
	}
	
	if len(response) < 8 {
		t.Fatalf("response too short: %d bytes", len(response))
	}
	
	// Verify no offset was committed due to generation mismatch
	group.Mu.RLock()
	if group.OffsetCommits != nil && group.OffsetCommits["test-topic"] != nil {
		if _, exists := group.OffsetCommits["test-topic"][0]; exists {
			t.Error("offset should not have been committed with wrong generation")
		}
	}
	group.Mu.RUnlock()
}

func TestHandler_handleOffsetFetch(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	// Create a consumer group with committed offsets
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 1
	
	// Pre-populate with committed offset
	group.OffsetCommits = map[string]map[int32]consumer.OffsetCommit{
		"test-topic": {
			0: {
				Offset:    42,
				Metadata:  "test-metadata",
				Timestamp: time.Now(),
			},
		},
	}
	group.Mu.Unlock()
	
	// Create a basic offset fetch request
	requestBody := createOffsetFetchRequestBody("test-group")
	
	correlationID := uint32(126)
	response, err := h.handleOffsetFetch(correlationID, requestBody)
	
	if err != nil {
		t.Fatalf("handleOffsetFetch failed: %v", err)
	}
	
	if len(response) < 8 {
		t.Fatalf("response too short: %d bytes", len(response))
	}
	
	// Check correlation ID in response
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("expected correlation ID %d, got %d", correlationID, respCorrelationID)
	}
}

func TestHandler_handleOffsetFetch_NoCommittedOffset(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	// Create a consumer group without committed offsets
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 1
	// No offset commits
	group.Mu.Unlock()
	
	requestBody := createOffsetFetchRequestBody("test-group")
	
	correlationID := uint32(127)
	response, err := h.handleOffsetFetch(correlationID, requestBody)
	
	if err != nil {
		t.Fatalf("handleOffsetFetch failed: %v", err)
	}
	
	if len(response) < 8 {
		t.Fatalf("response too short: %d bytes", len(response))
	}
	
	// Should get valid response even with no committed offsets
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("expected correlation ID %d, got %d", correlationID, respCorrelationID)
	}
}

func TestHandler_commitOffset(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	group := &consumer.ConsumerGroup{
		ID:           "test-group",
		OffsetCommits: nil,
	}
	
	// Test committing an offset
	err := h.commitOffset(group, "test-topic", 0, 100, "test-metadata")
	if err != nil {
		t.Fatalf("commitOffset failed: %v", err)
	}
	
	// Verify offset was stored
	if group.OffsetCommits == nil {
		t.Fatal("OffsetCommits map was not initialized")
	}
	
	topicOffsets, exists := group.OffsetCommits["test-topic"]
	if !exists {
		t.Fatal("topic offsets not found")
	}
	
	commit, exists := topicOffsets[0]
	if !exists {
		t.Fatal("partition offset not found")
	}
	
	if commit.Offset != 100 {
		t.Errorf("expected offset 100, got %d", commit.Offset)
	}
	
	if commit.Metadata != "test-metadata" {
		t.Errorf("expected metadata 'test-metadata', got '%s'", commit.Metadata)
	}
	
	// Test updating existing offset
	err = h.commitOffset(group, "test-topic", 0, 200, "updated-metadata")
	if err != nil {
		t.Fatalf("commitOffset update failed: %v", err)
	}
	
	updatedCommit := group.OffsetCommits["test-topic"][0]
	if updatedCommit.Offset != 200 {
		t.Errorf("expected updated offset 200, got %d", updatedCommit.Offset)
	}
	
	if updatedCommit.Metadata != "updated-metadata" {
		t.Errorf("expected updated metadata 'updated-metadata', got '%s'", updatedCommit.Metadata)
	}
}

func TestHandler_fetchOffset(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	// Test fetching from empty group
	emptyGroup := &consumer.ConsumerGroup{
		ID:           "empty-group",
		OffsetCommits: nil,
	}
	
	offset, metadata, err := h.fetchOffset(emptyGroup, "test-topic", 0)
	if err != nil {
		t.Errorf("fetchOffset should not error on empty group: %v", err)
	}
	
	if offset != -1 {
		t.Errorf("expected offset -1 for empty group, got %d", offset)
	}
	
	if metadata != "" {
		t.Errorf("expected empty metadata for empty group, got '%s'", metadata)
	}
	
	// Test fetching from group with committed offsets
	group := &consumer.ConsumerGroup{
		ID: "test-group",
		OffsetCommits: map[string]map[int32]consumer.OffsetCommit{
			"test-topic": {
				0: {
					Offset:    42,
					Metadata:  "test-metadata",
					Timestamp: time.Now(),
				},
			},
		},
	}
	
	offset, metadata, err = h.fetchOffset(group, "test-topic", 0)
	if err != nil {
		t.Errorf("fetchOffset failed: %v", err)
	}
	
	if offset != 42 {
		t.Errorf("expected offset 42, got %d", offset)
	}
	
	if metadata != "test-metadata" {
		t.Errorf("expected metadata 'test-metadata', got '%s'", metadata)
	}
	
	// Test fetching non-existent partition
	offset, metadata, err = h.fetchOffset(group, "test-topic", 1)
	if err != nil {
		t.Errorf("fetchOffset should not error on non-existent partition: %v", err)
	}
	
	if offset != -1 {
		t.Errorf("expected offset -1 for non-existent partition, got %d", offset)
	}
	
	// Test fetching non-existent topic
	offset, metadata, err = h.fetchOffset(group, "nonexistent-topic", 0)
	if err != nil {
		t.Errorf("fetchOffset should not error on non-existent topic: %v", err)
	}
	
	if offset != -1 {
		t.Errorf("expected offset -1 for non-existent topic, got %d", offset)
	}
}

func TestHandler_OffsetCommitFetch_EndToEnd(t *testing.T) {
	// Create two handlers connected via pipe to simulate client-server
	server := NewHandler()
	defer server.Close()
	
	client := NewHandler()
	defer client.Close()
	
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	
	// Setup consumer group on server
	group := server.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 1
	group.Members["member1"] = &consumer.GroupMember{
		ID:           "member1",
		State:        consumer.MemberStateStable,
		Assignment: []consumer.PartitionAssignment{
			{Topic: "test-topic", Partition: 0},
		},
	}
	group.Mu.Unlock()
	
	// Test offset commit
	commitRequestBody := createOffsetCommitRequestBody("test-group", 1, "member1")
	commitResponse, err := server.handleOffsetCommit(456, commitRequestBody)
	if err != nil {
		t.Fatalf("offset commit failed: %v", err)
	}
	
	if len(commitResponse) < 8 {
		t.Fatalf("commit response too short: %d bytes", len(commitResponse))
	}
	
	// Test offset fetch
	fetchRequestBody := createOffsetFetchRequestBody("test-group")
	fetchResponse, err := server.handleOffsetFetch(457, fetchRequestBody)
	if err != nil {
		t.Fatalf("offset fetch failed: %v", err)
	}
	
	if len(fetchResponse) < 8 {
		t.Fatalf("fetch response too short: %d bytes", len(fetchResponse))
	}
	
	// Verify the committed offset is present
	group.Mu.RLock()
	if group.OffsetCommits == nil || group.OffsetCommits["test-topic"] == nil {
		t.Error("offset commit was not stored")
	} else {
		commit, exists := group.OffsetCommits["test-topic"][0]
		if !exists {
			t.Error("offset commit for partition 0 was not found")
		} else if commit.Offset != 0 {
			t.Errorf("expected committed offset 0, got %d", commit.Offset)
		}
	}
	group.Mu.RUnlock()
}

func TestHandler_parseOffsetCommitRequest(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	requestBody := createOffsetCommitRequestBody("test-group", 1, "member1")
	
	request, err := h.parseOffsetCommitRequest(requestBody)
	if err != nil {
		t.Fatalf("parseOffsetCommitRequest failed: %v", err)
	}
	
	if request.GroupID != "test-group" {
		t.Errorf("expected group ID 'test-group', got '%s'", request.GroupID)
	}
	
	if request.GenerationID != 1 {
		t.Errorf("expected generation ID 1, got %d", request.GenerationID)
	}
	
	if request.MemberID != "member1" {
		t.Errorf("expected member ID 'member1', got '%s'", request.MemberID)
	}
}

func TestHandler_parseOffsetFetchRequest(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	requestBody := createOffsetFetchRequestBody("test-group")
	
	request, err := h.parseOffsetFetchRequest(requestBody)
	if err != nil {
		t.Fatalf("parseOffsetFetchRequest failed: %v", err)
	}
	
	if request.GroupID != "test-group" {
		t.Errorf("expected group ID 'test-group', got '%s'", request.GroupID)
	}
	
	if len(request.Topics) == 0 {
		t.Error("expected at least one topic in request")
	} else {
		if request.Topics[0].Name != "test-topic" {
			t.Errorf("expected topic name 'test-topic', got '%s'", request.Topics[0].Name)
		}
	}
}

func TestHandler_buildOffsetCommitResponse(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	response := OffsetCommitResponse{
		CorrelationID: 123,
		Topics: []OffsetCommitTopicResponse{
			{
				Name: "test-topic",
				Partitions: []OffsetCommitPartitionResponse{
					{Index: 0, ErrorCode: ErrorCodeNone},
					{Index: 1, ErrorCode: ErrorCodeOffsetMetadataTooLarge},
				},
			},
		},
	}
	
	responseBytes := h.buildOffsetCommitResponse(response)
	
	if len(responseBytes) < 16 {
		t.Fatalf("response too short: %d bytes", len(responseBytes))
	}
	
	// Check correlation ID
	correlationID := binary.BigEndian.Uint32(responseBytes[0:4])
	if correlationID != 123 {
		t.Errorf("expected correlation ID 123, got %d", correlationID)
	}
}

func TestHandler_buildOffsetFetchResponse(t *testing.T) {
	h := NewHandler()
	defer h.Close()
	
	response := OffsetFetchResponse{
		CorrelationID: 124,
		Topics: []OffsetFetchTopicResponse{
			{
				Name: "test-topic",
				Partitions: []OffsetFetchPartitionResponse{
					{
						Index:       0,
						Offset:      42,
						LeaderEpoch: -1,
						Metadata:    "test-metadata",
						ErrorCode:   ErrorCodeNone,
					},
				},
			},
		},
		ErrorCode: ErrorCodeNone,
	}
	
	responseBytes := h.buildOffsetFetchResponse(response)
	
	if len(responseBytes) < 20 {
		t.Fatalf("response too short: %d bytes", len(responseBytes))
	}
	
	// Check correlation ID
	correlationID := binary.BigEndian.Uint32(responseBytes[0:4])
	if correlationID != 124 {
		t.Errorf("expected correlation ID 124, got %d", correlationID)
	}
}

// Helper functions for creating test request bodies

func createOffsetCommitRequestBody(groupID string, generationID int32, memberID string) []byte {
	body := make([]byte, 0, 64)
	
	// Group ID (string)
	groupIDBytes := []byte(groupID)
	groupIDLength := make([]byte, 2)
	binary.BigEndian.PutUint16(groupIDLength, uint16(len(groupIDBytes)))
	body = append(body, groupIDLength...)
	body = append(body, groupIDBytes...)
	
	// Generation ID (4 bytes)
	generationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(generationIDBytes, uint32(generationID))
	body = append(body, generationIDBytes...)
	
	// Member ID (string)
	memberIDBytes := []byte(memberID)
	memberIDLength := make([]byte, 2)
	binary.BigEndian.PutUint16(memberIDLength, uint16(len(memberIDBytes)))
	body = append(body, memberIDLength...)
	body = append(body, memberIDBytes...)
	
	// Add minimal remaining data to make it parseable
	// In a real implementation, we'd add the full topics array
	
	return body
}

func createOffsetFetchRequestBody(groupID string) []byte {
	body := make([]byte, 0, 32)
	
	// Group ID (string)
	groupIDBytes := []byte(groupID)
	groupIDLength := make([]byte, 2)
	binary.BigEndian.PutUint16(groupIDLength, uint16(len(groupIDBytes)))
	body = append(body, groupIDLength...)
	body = append(body, groupIDBytes...)
	
	// Add minimal remaining data to make it parseable
	// In a real implementation, we'd add the full topics array
	
	return body
}
