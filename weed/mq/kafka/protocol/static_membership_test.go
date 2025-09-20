package protocol

import (
	"encoding/binary"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
)

// Helper function to build JoinGroup request body for testing
func buildJoinGroupRequestBody(req *JoinGroupRequest) []byte {
	// This is a simplified version for testing
	// In practice, you'd use the actual protocol serialization
	body := make([]byte, 0, 256)
	
	// Group ID (string)
	groupIDLen := make([]byte, 2)
	binary.BigEndian.PutUint16(groupIDLen, uint16(len(req.GroupID)))
	body = append(body, groupIDLen...)
	body = append(body, []byte(req.GroupID)...)
	
	// Session timeout (4 bytes)
	sessionTimeout := make([]byte, 4)
	binary.BigEndian.PutUint32(sessionTimeout, uint32(req.SessionTimeout))
	body = append(body, sessionTimeout...)
	
	// Rebalance timeout (4 bytes)
	rebalanceTimeout := make([]byte, 4)
	binary.BigEndian.PutUint32(rebalanceTimeout, uint32(req.RebalanceTimeout))
	body = append(body, rebalanceTimeout...)
	
	// Member ID (string)
	memberIDLen := make([]byte, 2)
	binary.BigEndian.PutUint16(memberIDLen, uint16(len(req.MemberID)))
	body = append(body, memberIDLen...)
	body = append(body, []byte(req.MemberID)...)
	
	// Group instance ID (string, v5+)
	instanceIDLen := make([]byte, 2)
	if req.GroupInstanceID == "" {
		binary.BigEndian.PutUint16(instanceIDLen, 0xFFFF) // null
	} else {
		binary.BigEndian.PutUint16(instanceIDLen, uint16(len(req.GroupInstanceID)))
	}
	body = append(body, instanceIDLen...)
	if req.GroupInstanceID != "" {
		body = append(body, []byte(req.GroupInstanceID)...)
	}
	
	// Protocol type (string)
	protocolTypeLen := make([]byte, 2)
	binary.BigEndian.PutUint16(protocolTypeLen, uint16(len(req.ProtocolType)))
	body = append(body, protocolTypeLen...)
	body = append(body, []byte(req.ProtocolType)...)
	
	// Group protocols array (simplified)
	protocolsCount := make([]byte, 4)
	binary.BigEndian.PutUint32(protocolsCount, uint32(len(req.GroupProtocols)))
	body = append(body, protocolsCount...)
	
	for _, protocol := range req.GroupProtocols {
		// Protocol name
		nameLen := make([]byte, 2)
		binary.BigEndian.PutUint16(nameLen, uint16(len(protocol.Name)))
		body = append(body, nameLen...)
		body = append(body, []byte(protocol.Name)...)
		
		// Protocol metadata
		metadataLen := make([]byte, 4)
		binary.BigEndian.PutUint32(metadataLen, uint32(len(protocol.Metadata)))
		body = append(body, metadataLen...)
		body = append(body, protocol.Metadata...)
	}
	
	return body
}

// Helper function to build LeaveGroup request body for testing
func buildLeaveGroupRequestBody(req *LeaveGroupRequest) []byte {
	body := make([]byte, 0, 128)
	
	// Group ID (string)
	groupIDLen := make([]byte, 2)
	binary.BigEndian.PutUint16(groupIDLen, uint16(len(req.GroupID)))
	body = append(body, groupIDLen...)
	body = append(body, []byte(req.GroupID)...)
	
	// Member ID (string)
	memberIDLen := make([]byte, 2)
	binary.BigEndian.PutUint16(memberIDLen, uint16(len(req.MemberID)))
	body = append(body, memberIDLen...)
	body = append(body, []byte(req.MemberID)...)
	
	// Group instance ID (string, v3+)
	instanceIDLen := make([]byte, 2)
	if req.GroupInstanceID == "" {
		binary.BigEndian.PutUint16(instanceIDLen, 0xFFFF) // null
	} else {
		binary.BigEndian.PutUint16(instanceIDLen, uint16(len(req.GroupInstanceID)))
	}
	body = append(body, instanceIDLen...)
	if req.GroupInstanceID != "" {
		body = append(body, []byte(req.GroupInstanceID)...)
	}
	
	return body
}

func TestHandler_JoinGroup_StaticMembership(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Test static member joining
	t.Run("StaticMemberJoin", func(t *testing.T) {
		request := &JoinGroupRequest{
			GroupID:          "test-group",
			SessionTimeout:   30000,
			RebalanceTimeout: 300000,
			MemberID:         "", // New member
			GroupInstanceID:  "static-instance-1",
			ProtocolType:     "consumer",
			GroupProtocols: []GroupProtocol{
				{Name: "range", Metadata: []byte{}},
			},
		}

		// Build request body (simplified for testing)
		requestBody := buildJoinGroupRequestBody(request)
		
		response, err := handler.handleJoinGroup(12345, 5, requestBody)
		if err != nil {
			t.Fatalf("JoinGroup failed: %v", err)
		}

		// Parse response to verify member was added
		if len(response) < 10 {
			t.Fatalf("Response too short: %d bytes", len(response))
		}

		// Check error code (should be 0)
		errorCode := binary.BigEndian.Uint16(response[4:6])
		if errorCode != 0 {
			t.Errorf("Expected error code 0, got %d", errorCode)
		}

		// Verify member was added to group
		group := handler.groupCoordinator.GetGroup("test-group")
		if group == nil {
			t.Fatal("Group not found")
		}

		if len(group.Members) != 1 {
			t.Errorf("Expected 1 member, got %d", len(group.Members))
		}

		// Find the member and verify static membership
		var member *consumer.GroupMember
		for _, m := range group.Members {
			member = m
			break
		}

		if member == nil {
			t.Fatal("Member not found")
		}

		if member.GroupInstanceID == nil || *member.GroupInstanceID != "static-instance-1" {
			t.Errorf("Expected GroupInstanceID 'static-instance-1', got %v", member.GroupInstanceID)
		}

		if !handler.groupCoordinator.IsStaticMember(member) {
			t.Error("Expected member to be static")
		}
	})

	// Test static member reconnection
	t.Run("StaticMemberReconnection", func(t *testing.T) {
		// First join
		request1 := &JoinGroupRequest{
			GroupID:          "test-group-2",
			SessionTimeout:   30000,
			RebalanceTimeout: 300000,
			MemberID:         "",
			GroupInstanceID:  "static-instance-2",
			ProtocolType:     "consumer",
			GroupProtocols: []GroupProtocol{
				{Name: "range", Metadata: []byte{}},
			},
		}

		requestBody1 := buildJoinGroupRequestBody(request1)
		_, err := handler.handleJoinGroup(12346, 5, requestBody1)
		if err != nil {
			t.Fatalf("First JoinGroup failed: %v", err)
		}

		group := handler.groupCoordinator.GetGroup("test-group-2")
		if len(group.Members) != 1 {
			t.Errorf("Expected 1 member after first join, got %d", len(group.Members))
		}

		// Get the first member ID
		var firstMemberID string
		for memberID := range group.Members {
			firstMemberID = memberID
			break
		}

		// Second join with same instance ID (simulating reconnection)
		request2 := &JoinGroupRequest{
			GroupID:          "test-group-2",
			SessionTimeout:   30000,
			RebalanceTimeout: 300000,
			MemberID:         "",
			GroupInstanceID:  "static-instance-2", // Same instance ID
			ProtocolType:     "consumer",
			GroupProtocols: []GroupProtocol{
				{Name: "range", Metadata: []byte{}},
			},
		}

		requestBody2 := buildJoinGroupRequestBody(request2)
		_, err = handler.handleJoinGroup(12347, 5, requestBody2)
		if err != nil {
			t.Fatalf("Second JoinGroup failed: %v", err)
		}

		// Should still have the same member (static member reused)
		if len(group.Members) != 1 {
			t.Errorf("Expected 1 member after reconnection, got %d", len(group.Members))
		}

		// Verify the member ID is the same (static member reused)
		var secondMemberID string
		for memberID := range group.Members {
			secondMemberID = memberID
			break
		}

		if firstMemberID != secondMemberID {
			t.Errorf("Expected same member ID for static member reconnection, got %s vs %s", firstMemberID, secondMemberID)
		}
	})

	// Test dynamic member (no instance ID)
	t.Run("DynamicMember", func(t *testing.T) {
		request := &JoinGroupRequest{
			GroupID:          "test-group-3",
			SessionTimeout:   30000,
			RebalanceTimeout: 300000,
			MemberID:         "",
			GroupInstanceID:  "", // No instance ID (dynamic member)
			ProtocolType:     "consumer",
			GroupProtocols: []GroupProtocol{
				{Name: "range", Metadata: []byte{}},
			},
		}

		requestBody := buildJoinGroupRequestBody(request)
		_, err := handler.handleJoinGroup(12348, 5, requestBody)
		if err != nil {
			t.Fatalf("JoinGroup failed: %v", err)
		}

		group := handler.groupCoordinator.GetGroup("test-group-3")
		if len(group.Members) != 1 {
			t.Errorf("Expected 1 member, got %d", len(group.Members))
		}

		// Find the member and verify it's dynamic
		var member *consumer.GroupMember
		for _, m := range group.Members {
			member = m
			break
		}

		if member == nil {
			t.Fatal("Member not found")
		}

		if handler.groupCoordinator.IsStaticMember(member) {
			t.Error("Expected member to be dynamic")
		}
	})
}

func TestHandler_LeaveGroup_StaticMembership(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Setup: Add a static member
	group := handler.groupCoordinator.GetOrCreateGroup("test-group")
	instanceID := "static-instance-1"
	member := &consumer.GroupMember{
		ID:              "member-1",
		ClientID:        "client-1",
		ClientHost:      "localhost",
		GroupInstanceID: &instanceID,
		SessionTimeout:  30000,
		State:           consumer.MemberStatePending,
	}
	group.Members[member.ID] = member
	handler.groupCoordinator.RegisterStaticMember(group, member)

	// Test leaving static member
	t.Run("StaticMemberLeave", func(t *testing.T) {
		request := &LeaveGroupRequest{
			GroupID:         "test-group",
			MemberID:        "member-1",
			GroupInstanceID: "static-instance-1",
		}

		requestBody := buildLeaveGroupRequestBody(request)
		response, err := handler.handleLeaveGroup(12349, 3, requestBody)
		if err != nil {
			t.Fatalf("LeaveGroup failed: %v", err)
		}

		// Check error code (should be 0)
		if len(response) < 6 {
			t.Fatalf("Response too short: %d bytes", len(response))
		}
		errorCode := binary.BigEndian.Uint16(response[4:6])
		if errorCode != 0 {
			t.Errorf("Expected error code 0, got %d", errorCode)
		}

		// Verify member was removed
		if len(group.Members) != 0 {
			t.Errorf("Expected 0 members after leave, got %d", len(group.Members))
		}

		// Verify static member was unregistered
		foundMember := handler.groupCoordinator.FindStaticMember(group, instanceID)
		if foundMember != nil {
			t.Error("Expected static member to be unregistered")
		}
	})

	// Test leaving with wrong instance ID
	t.Run("StaticMemberLeaveWrongInstanceID", func(t *testing.T) {
		// Setup: Add another static member
		instanceID2 := "static-instance-2"
		member2 := &consumer.GroupMember{
			ID:              "member-2",
			ClientID:        "client-2",
			ClientHost:      "localhost",
			GroupInstanceID: &instanceID2,
			SessionTimeout:  30000,
			State:           consumer.MemberStatePending,
		}
		group.Members[member2.ID] = member2
		handler.groupCoordinator.RegisterStaticMember(group, member2)

		request := &LeaveGroupRequest{
			GroupID:         "test-group",
			MemberID:        "member-2",
			GroupInstanceID: "wrong-instance-id", // Wrong instance ID
		}

		requestBody := buildLeaveGroupRequestBody(request)
		response, err := handler.handleLeaveGroup(12350, 3, requestBody)
		if err != nil {
			t.Fatalf("LeaveGroup failed: %v", err)
		}

		// Check error code (should be FENCED_INSTANCE_ID)
		if len(response) < 6 {
			t.Fatalf("Response too short: %d bytes", len(response))
		}
		errorCode := binary.BigEndian.Uint16(response[4:6])
		if errorCode != uint16(ErrorCodeFencedInstanceID) {
			t.Errorf("Expected error code %d (FENCED_INSTANCE_ID), got %d", ErrorCodeFencedInstanceID, errorCode)
		}

		// Verify member was NOT removed
		if len(group.Members) != 1 {
			t.Errorf("Expected 1 member after failed leave, got %d", len(group.Members))
		}
	})
}

func TestHandler_DescribeGroups_StaticMembership(t *testing.T) {
	handler := NewTestHandler()
	defer handler.Close()

	// Setup: Add static and dynamic members
	group := handler.groupCoordinator.GetOrCreateGroup("test-group")
	
	// Static member
	staticInstanceID := "static-instance-1"
	staticMember := &consumer.GroupMember{
		ID:              "static-member",
		ClientID:        "static-client",
		ClientHost:      "localhost",
		GroupInstanceID: &staticInstanceID,
		SessionTimeout:  30000,
		State:           consumer.MemberStateStable,
	}
	group.Members[staticMember.ID] = staticMember
	handler.groupCoordinator.RegisterStaticMember(group, staticMember)

	// Dynamic member
	dynamicMember := &consumer.GroupMember{
		ID:              "dynamic-member",
		ClientID:        "dynamic-client",
		ClientHost:      "localhost",
		GroupInstanceID: nil,
		SessionTimeout:  30000,
		State:           consumer.MemberStateStable,
	}
	group.Members[dynamicMember.ID] = dynamicMember

	group.State = consumer.GroupStateStable

	// Test DescribeGroups includes GroupInstanceID
	// Build a simple DescribeGroups request body
	requestBody := make([]byte, 0, 64)
	// Groups array count (4 bytes)
	groupsCount := make([]byte, 4)
	binary.BigEndian.PutUint32(groupsCount, 1)
	requestBody = append(requestBody, groupsCount...)
	// Group ID (string)
	groupIDLen := make([]byte, 2)
	binary.BigEndian.PutUint16(groupIDLen, uint16(len("test-group")))
	requestBody = append(requestBody, groupIDLen...)
	requestBody = append(requestBody, []byte("test-group")...)
	
	response, err := handler.handleDescribeGroups(12351, 5, requestBody)
	if err != nil {
		t.Fatalf("DescribeGroups failed: %v", err)
	}

	if len(response) < 10 {
		t.Fatalf("Response too short: %d bytes", len(response))
	}

	// Basic validation - response should contain group information
	// In a real test, we'd parse the full response to verify GroupInstanceID is included
	t.Logf("DescribeGroups response length: %d bytes", len(response))
}
