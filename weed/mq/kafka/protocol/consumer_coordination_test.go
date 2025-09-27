package protocol

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
)

func TestHandler_handleHeartbeat(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	// Create a consumer group with a stable member
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 1
	group.Members["member1"] = &consumer.GroupMember{
		ID:            "member1",
		State:         consumer.MemberStateStable,
		LastHeartbeat: time.Now().Add(-5 * time.Second), // 5 seconds ago
	}
	group.Mu.Unlock()

	// Create a basic heartbeat request
	requestBody := createHeartbeatRequestBody("test-group", 1, "member1")

	correlationID := uint32(123)
	response, err := h.handleHeartbeat(correlationID, 0, requestBody)

	if err != nil {
		t.Fatalf("handleHeartbeat failed: %v", err)
	}

	if len(response) < 8 {
		t.Fatalf("response too short: %d bytes", len(response))
	}

	// Check correlation ID in response
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("expected correlation ID %d, got %d", correlationID, respCorrelationID)
	}

	// Check error code (should be ErrorCodeNone for successful heartbeat)
	errorCode := int16(binary.BigEndian.Uint16(response[4:6]))
	if errorCode != ErrorCodeNone {
		t.Errorf("expected error code %d, got %d", ErrorCodeNone, errorCode)
	}

	// Verify heartbeat timestamp was updated
	group.Mu.RLock()
	member := group.Members["member1"]
	if time.Since(member.LastHeartbeat) > 1*time.Second {
		t.Error("heartbeat timestamp was not updated")
	}
	group.Mu.RUnlock()
}

func TestHandler_handleHeartbeat_RebalanceInProgress(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	// Create a consumer group in rebalancing state
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStatePreparingRebalance // Rebalancing
	group.Generation = 1
	group.Members["member1"] = &consumer.GroupMember{
		ID:            "member1",
		State:         consumer.MemberStatePending,
		LastHeartbeat: time.Now().Add(-5 * time.Second),
	}
	group.Mu.Unlock()

	requestBody := createHeartbeatRequestBody("test-group", 1, "member1")

	correlationID := uint32(124)
	response, err := h.handleHeartbeat(correlationID, 0, requestBody)

	if err != nil {
		t.Fatalf("handleHeartbeat failed: %v", err)
	}

	if len(response) < 8 {
		t.Fatalf("response too short: %d bytes", len(response))
	}

	// Should return ErrorCodeRebalanceInProgress
	errorCode := int16(binary.BigEndian.Uint16(response[4:6]))
	if errorCode != ErrorCodeRebalanceInProgress {
		t.Errorf("expected error code %d (rebalance in progress), got %d", ErrorCodeRebalanceInProgress, errorCode)
	}
}

func TestHandler_handleHeartbeat_WrongGeneration(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	// Create a consumer group with generation 2
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 2
	group.Members["member1"] = &consumer.GroupMember{
		ID:            "member1",
		State:         consumer.MemberStateStable,
		LastHeartbeat: time.Now().Add(-5 * time.Second),
	}
	group.Mu.Unlock()

	// Send heartbeat with wrong generation (1 instead of 2)
	requestBody := createHeartbeatRequestBody("test-group", 1, "member1")

	correlationID := uint32(125)
	response, err := h.handleHeartbeat(correlationID, 0, requestBody)

	if err != nil {
		t.Fatalf("handleHeartbeat failed: %v", err)
	}

	// Should return ErrorCodeIllegalGeneration
	errorCode := int16(binary.BigEndian.Uint16(response[4:6]))
	if errorCode != ErrorCodeIllegalGeneration {
		t.Errorf("expected error code %d (illegal generation), got %d", ErrorCodeIllegalGeneration, errorCode)
	}
}

func TestHandler_handleHeartbeat_UnknownMember(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	// Create a consumer group without the requested member
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 1
	// No members in group
	group.Mu.Unlock()

	requestBody := createHeartbeatRequestBody("test-group", 1, "unknown-member")

	correlationID := uint32(126)
	response, err := h.handleHeartbeat(correlationID, 0, requestBody)

	if err != nil {
		t.Fatalf("handleHeartbeat failed: %v", err)
	}

	// Should return ErrorCodeUnknownMemberID
	errorCode := int16(binary.BigEndian.Uint16(response[4:6]))
	if errorCode != ErrorCodeUnknownMemberID {
		t.Errorf("expected error code %d (unknown member), got %d", ErrorCodeUnknownMemberID, errorCode)
	}
}

func TestHandler_handleLeaveGroup(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	// Create a consumer group with multiple members
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 1
	group.Leader = "member1"
	group.Members["member1"] = &consumer.GroupMember{
		ID:           "member1",
		State:        consumer.MemberStateStable,
		Subscription: []string{"topic1"},
	}
	group.Members["member2"] = &consumer.GroupMember{
		ID:           "member2",
		State:        consumer.MemberStateStable,
		Subscription: []string{"topic1", "topic2"},
	}
	group.SubscribedTopics = map[string]bool{
		"topic1": true,
		"topic2": true,
	}
	group.Mu.Unlock()

	// Create a leave group request
	requestBody := createLeaveGroupRequestBody("test-group", "member2")

	correlationID := uint32(127)
	response, err := h.handleLeaveGroup(correlationID, 1, requestBody)

	if err != nil {
		t.Fatalf("handleLeaveGroup failed: %v", err)
	}

	if len(response) < 8 {
		t.Fatalf("response too short: %d bytes", len(response))
	}

	// Check correlation ID in response
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("expected correlation ID %d, got %d", correlationID, respCorrelationID)
	}

	// Check error code (should be ErrorCodeNone for successful leave)
	errorCode := int16(binary.BigEndian.Uint16(response[4:6]))
	if errorCode != ErrorCodeNone {
		t.Errorf("expected error code %d, got %d", ErrorCodeNone, errorCode)
	}

	// Verify member was removed and group state updated
	group.Mu.RLock()
	if _, exists := group.Members["member2"]; exists {
		t.Error("member2 should have been removed from group")
	}

	if len(group.Members) != 1 {
		t.Errorf("expected 1 remaining member, got %d", len(group.Members))
	}

	// Group should be in rebalancing state
	if group.State != consumer.GroupStatePreparingRebalance {
		t.Errorf("expected group state PreparingRebalance, got %s", group.State)
	}

	// Generation should be incremented
	if group.Generation != 2 {
		t.Errorf("expected generation 2, got %d", group.Generation)
	}

	// Subscribed topics should be updated (only topic1 remains)
	if len(group.SubscribedTopics) != 1 || !group.SubscribedTopics["topic1"] {
		t.Error("group subscribed topics were not updated correctly")
	}

	if group.SubscribedTopics["topic2"] {
		t.Error("topic2 should have been removed from subscribed topics")
	}
	group.Mu.RUnlock()
}

func TestHandler_handleLeaveGroup_LastMember(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	// Create a consumer group with only one member
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 1
	group.Leader = "member1"
	group.Members["member1"] = &consumer.GroupMember{
		ID:           "member1",
		State:        consumer.MemberStateStable,
		Subscription: []string{"topic1"},
	}
	group.Mu.Unlock()

	requestBody := createLeaveGroupRequestBody("test-group", "member1")

	correlationID := uint32(128)
	response, err := h.handleLeaveGroup(correlationID, 1, requestBody)

	if err != nil {
		t.Fatalf("handleLeaveGroup failed: %v", err)
	}

	// Check error code
	errorCode := int16(binary.BigEndian.Uint16(response[4:6]))
	if errorCode != ErrorCodeNone {
		t.Errorf("expected error code %d, got %d", ErrorCodeNone, errorCode)
	}

	// Verify group became empty
	group.Mu.RLock()
	if len(group.Members) != 0 {
		t.Errorf("expected 0 members, got %d", len(group.Members))
	}

	if group.State != consumer.GroupStateEmpty {
		t.Errorf("expected group state Empty, got %s", group.State)
	}

	if group.Leader != "" {
		t.Errorf("expected empty leader, got %s", group.Leader)
	}

	if group.Generation != 2 {
		t.Errorf("expected generation 2, got %d", group.Generation)
	}
	group.Mu.RUnlock()
}

func TestHandler_handleLeaveGroup_LeaderLeaves(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	// Create a consumer group where the leader is leaving
	group := h.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 1
	group.Leader = "leader-member"
	group.Members["leader-member"] = &consumer.GroupMember{
		ID:    "leader-member",
		State: consumer.MemberStateStable,
	}
	group.Members["other-member"] = &consumer.GroupMember{
		ID:    "other-member",
		State: consumer.MemberStateStable,
	}
	group.Mu.Unlock()

	requestBody := createLeaveGroupRequestBody("test-group", "leader-member")

	correlationID := uint32(129)
	_, err := h.handleLeaveGroup(correlationID, 1, requestBody)

	if err != nil {
		t.Fatalf("handleLeaveGroup failed: %v", err)
	}

	// Verify leader was changed
	group.Mu.RLock()
	if group.Leader == "leader-member" {
		t.Error("leader should have been changed after leader left")
	}

	if group.Leader != "other-member" {
		t.Errorf("expected new leader to be 'other-member', got '%s'", group.Leader)
	}

	if len(group.Members) != 1 {
		t.Errorf("expected 1 remaining member, got %d", len(group.Members))
	}
	group.Mu.RUnlock()
}

func TestHandler_parseHeartbeatRequest(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	requestBody := createHeartbeatRequestBody("test-group", 1, "member1")

	request, err := h.parseHeartbeatRequest(requestBody, 0)
	if err != nil {
		t.Fatalf("parseHeartbeatRequest failed: %v", err)
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

func TestHandler_parseLeaveGroupRequest(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	requestBody := createLeaveGroupRequestBody("test-group", "member1")

	request, err := h.parseLeaveGroupRequest(requestBody)
	if err != nil {
		t.Fatalf("parseLeaveGroupRequest failed: %v", err)
	}

	if request.GroupID != "test-group" {
		t.Errorf("expected group ID 'test-group', got '%s'", request.GroupID)
	}

	if request.MemberID != "member1" {
		t.Errorf("expected member ID 'member1', got '%s'", request.MemberID)
	}
}

func TestHandler_buildHeartbeatResponse(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	response := HeartbeatResponse{
		CorrelationID: 123,
		ErrorCode:     ErrorCodeRebalanceInProgress,
	}

	responseBytes := h.buildHeartbeatResponse(response)

	if len(responseBytes) != 10 { // 4 + 2 + 4 bytes
		t.Fatalf("expected response length 10, got %d", len(responseBytes))
	}

	// Check correlation ID
	correlationID := binary.BigEndian.Uint32(responseBytes[0:4])
	if correlationID != 123 {
		t.Errorf("expected correlation ID 123, got %d", correlationID)
	}

	// Check error code
	errorCode := int16(binary.BigEndian.Uint16(responseBytes[4:6]))
	if errorCode != ErrorCodeRebalanceInProgress {
		t.Errorf("expected error code %d, got %d", ErrorCodeRebalanceInProgress, errorCode)
	}
}

func TestHandler_buildLeaveGroupResponse(t *testing.T) {
	h := NewTestHandler()
	defer h.Close()

	response := LeaveGroupResponse{
		CorrelationID: 124,
		ErrorCode:     ErrorCodeNone,
		Members: []LeaveGroupMemberResponse{
			{
				MemberID:        "member1",
				GroupInstanceID: "",
				ErrorCode:       ErrorCodeNone,
			},
		},
	}

	responseBytes := h.buildLeaveGroupResponse(response, 1)

	if len(responseBytes) < 16 {
		t.Fatalf("response too short: %d bytes", len(responseBytes))
	}

	// Check correlation ID
	correlationID := binary.BigEndian.Uint32(responseBytes[0:4])
	if correlationID != 124 {
		t.Errorf("expected correlation ID 124, got %d", correlationID)
	}

	// Check error code
	errorCode := int16(binary.BigEndian.Uint16(responseBytes[4:6]))
	if errorCode != ErrorCodeNone {
		t.Errorf("expected error code %d, got %d", ErrorCodeNone, errorCode)
	}
}

func TestHandler_HeartbeatLeaveGroup_EndToEnd(t *testing.T) {
	// Create two handlers connected via pipe to simulate client-server
	server := NewTestHandler()
	defer server.Close()

	client := NewTestHandler()
	defer client.Close()

	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	// Setup consumer group on server
	group := server.groupCoordinator.GetOrCreateGroup("test-group")
	group.Mu.Lock()
	group.State = consumer.GroupStateStable
	group.Generation = 1
	group.Leader = "member1"
	group.Members["member1"] = &consumer.GroupMember{
		ID:            "member1",
		State:         consumer.MemberStateStable,
		LastHeartbeat: time.Now().Add(-10 * time.Second),
	}
	group.Mu.Unlock()

	// Test heartbeat
	heartbeatRequestBody := createHeartbeatRequestBody("test-group", 1, "member1")
	heartbeatResponse, err := server.handleHeartbeat(456, 0, heartbeatRequestBody)
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	if len(heartbeatResponse) < 8 {
		t.Fatalf("heartbeat response too short: %d bytes", len(heartbeatResponse))
	}

	// Verify heartbeat was processed
	group.Mu.RLock()
	member := group.Members["member1"]
	if time.Since(member.LastHeartbeat) > 1*time.Second {
		t.Error("heartbeat timestamp was not updated")
	}
	group.Mu.RUnlock()

	// Test leave group
	leaveGroupRequestBody := createLeaveGroupRequestBody("test-group", "member1")
	leaveGroupResponse, err := server.handleLeaveGroup(457, 1, leaveGroupRequestBody)
	if err != nil {
		t.Fatalf("leave group failed: %v", err)
	}

	if len(leaveGroupResponse) < 8 {
		t.Fatalf("leave group response too short: %d bytes", len(leaveGroupResponse))
	}

	// Verify member left and group became empty
	group.Mu.RLock()
	if len(group.Members) != 0 {
		t.Errorf("expected 0 members after leave, got %d", len(group.Members))
	}

	if group.State != consumer.GroupStateEmpty {
		t.Errorf("expected group state Empty, got %s", group.State)
	}
	group.Mu.RUnlock()
}

// Helper functions for creating test request bodies

func createHeartbeatRequestBody(groupID string, generationID int32, memberID string) []byte {
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

	return body
}

func createLeaveGroupRequestBody(groupID string, memberID string) []byte {
	body := make([]byte, 0, 32)

	// Group ID (string)
	groupIDBytes := []byte(groupID)
	groupIDLength := make([]byte, 2)
	binary.BigEndian.PutUint16(groupIDLength, uint16(len(groupIDBytes)))
	body = append(body, groupIDLength...)
	body = append(body, groupIDBytes...)

	// Member ID (string)
	memberIDBytes := []byte(memberID)
	memberIDLength := make([]byte, 2)
	binary.BigEndian.PutUint16(memberIDLength, uint16(len(memberIDBytes)))
	body = append(body, memberIDLength...)
	body = append(body, memberIDBytes...)

	return body
}
