package protocol

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
)

// Heartbeat API (key 12) - Consumer group heartbeat
// Consumers send periodic heartbeats to stay in the group and receive rebalancing signals

// HeartbeatRequest represents a Heartbeat request from a Kafka client
type HeartbeatRequest struct {
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupInstanceID string // Optional static membership ID
}

// HeartbeatResponse represents a Heartbeat response to a Kafka client
type HeartbeatResponse struct {
	CorrelationID uint32
	ErrorCode     int16
}

// LeaveGroup API (key 13) - Consumer graceful departure
// Consumers call this when shutting down to trigger immediate rebalancing

// LeaveGroupRequest represents a LeaveGroup request from a Kafka client
type LeaveGroupRequest struct {
	GroupID         string
	MemberID        string
	GroupInstanceID string             // Optional static membership ID
	Members         []LeaveGroupMember // For newer versions, can leave multiple members
}

// LeaveGroupMember represents a member leaving the group (for batch departures)
type LeaveGroupMember struct {
	MemberID        string
	GroupInstanceID string
	Reason          string // Optional reason for leaving
}

// LeaveGroupResponse represents a LeaveGroup response to a Kafka client
type LeaveGroupResponse struct {
	CorrelationID uint32
	ErrorCode     int16
	Members       []LeaveGroupMemberResponse // Per-member responses for newer versions
}

// LeaveGroupMemberResponse represents per-member leave group response
type LeaveGroupMemberResponse struct {
	MemberID        string
	GroupInstanceID string
	ErrorCode       int16
}

// Error codes specific to consumer coordination are imported from errors.go

func (h *Handler) handleHeartbeat(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse Heartbeat request
	request, err := h.parseHeartbeatRequest(requestBody)
	if err != nil {
		return h.buildHeartbeatErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	// Validate request
	if request.GroupID == "" || request.MemberID == "" {
		return h.buildHeartbeatErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	// Get consumer group
	group := h.groupCoordinator.GetGroup(request.GroupID)
	if group == nil {
		return h.buildHeartbeatErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	group.Mu.Lock()
	defer group.Mu.Unlock()

	// Update group's last activity
	group.LastActivity = time.Now()

	// Validate member exists
	member, exists := group.Members[request.MemberID]
	if !exists {
		return h.buildHeartbeatErrorResponse(correlationID, ErrorCodeUnknownMemberID), nil
	}

	// Validate generation
	if request.GenerationID != group.Generation {
		return h.buildHeartbeatErrorResponse(correlationID, ErrorCodeIllegalGeneration), nil
	}

	// Update member's last heartbeat
	member.LastHeartbeat = time.Now()

	// Check if rebalancing is in progress
	var errorCode int16 = ErrorCodeNone
	switch group.State {
	case consumer.GroupStatePreparingRebalance, consumer.GroupStateCompletingRebalance:
		// Signal the consumer that rebalancing is happening
		errorCode = ErrorCodeRebalanceInProgress
	case consumer.GroupStateDead:
		errorCode = ErrorCodeInvalidGroupID
	case consumer.GroupStateEmpty:
		// This shouldn't happen if member exists, but handle gracefully
		errorCode = ErrorCodeUnknownMemberID
	case consumer.GroupStateStable:
		// Normal case - heartbeat accepted
		errorCode = ErrorCodeNone
	}

	// Build successful response
	response := HeartbeatResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
	}

	return h.buildHeartbeatResponse(response), nil
}

func (h *Handler) handleLeaveGroup(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// Parse LeaveGroup request
	request, err := h.parseLeaveGroupRequest(requestBody)
	if err != nil {
		return h.buildLeaveGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	// Validate request
	if request.GroupID == "" || request.MemberID == "" {
		return h.buildLeaveGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	// Get consumer group
	group := h.groupCoordinator.GetGroup(request.GroupID)
	if group == nil {
		return h.buildLeaveGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	group.Mu.Lock()
	defer group.Mu.Unlock()

	// Update group's last activity
	group.LastActivity = time.Now()

	// Validate member exists
	member, exists := group.Members[request.MemberID]
	if !exists {
		return h.buildLeaveGroupErrorResponse(correlationID, ErrorCodeUnknownMemberID, apiVersion), nil
	}

	// For static members, only remove if GroupInstanceID matches or is not provided
	if h.groupCoordinator.IsStaticMember(member) {
		if request.GroupInstanceID != "" && *member.GroupInstanceID != request.GroupInstanceID {
			return h.buildLeaveGroupErrorResponse(correlationID, ErrorCodeFencedInstanceID, apiVersion), nil
		}
		// Unregister static member
		h.groupCoordinator.UnregisterStaticMemberLocked(group, *member.GroupInstanceID)
	}

	// Remove the member from the group
	delete(group.Members, request.MemberID)

	// Update group state based on remaining members
	if len(group.Members) == 0 {
		// Group becomes empty
		group.State = consumer.GroupStateEmpty
		group.Generation++
		group.Leader = ""
	} else {
		// Trigger rebalancing for remaining members
		group.State = consumer.GroupStatePreparingRebalance
		group.Generation++

		// If the leaving member was the leader, select a new leader
		if group.Leader == request.MemberID {
			// Select first remaining member as new leader
			for memberID := range group.Members {
				group.Leader = memberID
				break
			}
		}

		// Mark remaining members as pending to trigger rebalancing
		for _, member := range group.Members {
			member.State = consumer.MemberStatePending
		}
	}

	// Update group's subscribed topics (may have changed with member leaving)
	h.updateGroupSubscriptionFromMembers(group)

	// Build successful response
	response := LeaveGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     ErrorCodeNone,
		Members: []LeaveGroupMemberResponse{
			{
				MemberID:        request.MemberID,
				GroupInstanceID: request.GroupInstanceID,
				ErrorCode:       ErrorCodeNone,
			},
		},
	}

	return h.buildLeaveGroupResponse(response, apiVersion), nil
}

func (h *Handler) parseHeartbeatRequest(data []byte) (*HeartbeatRequest, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0

	// GroupID (string)
	groupIDLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+groupIDLength > len(data) {
		return nil, fmt.Errorf("invalid group ID length")
	}
	groupID := string(data[offset : offset+groupIDLength])
	offset += groupIDLength

	// Generation ID (4 bytes)
	if offset+4 > len(data) {
		return nil, fmt.Errorf("missing generation ID")
	}
	generationID := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// MemberID (string)
	if offset+2 > len(data) {
		return nil, fmt.Errorf("missing member ID length")
	}
	memberIDLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+memberIDLength > len(data) {
		return nil, fmt.Errorf("invalid member ID length")
	}
	memberID := string(data[offset : offset+memberIDLength])
	offset += memberIDLength

	return &HeartbeatRequest{
		GroupID:         groupID,
		GenerationID:    generationID,
		MemberID:        memberID,
		GroupInstanceID: "", // Simplified - would parse from remaining data
	}, nil
}

func (h *Handler) parseLeaveGroupRequest(data []byte) (*LeaveGroupRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0

	// GroupID (string)
	groupIDLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+groupIDLength > len(data) {
		return nil, fmt.Errorf("invalid group ID length")
	}
	groupID := string(data[offset : offset+groupIDLength])
	offset += groupIDLength

	// MemberID (string)
	if offset+2 > len(data) {
		return nil, fmt.Errorf("missing member ID length")
	}
	memberIDLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+memberIDLength > len(data) {
		return nil, fmt.Errorf("invalid member ID length")
	}
	memberID := string(data[offset : offset+memberIDLength])
	offset += memberIDLength

	// GroupInstanceID (string, v3+) - optional field
	var groupInstanceID string
	if offset+2 <= len(data) {
		instanceIDLength := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if instanceIDLength != 0xFFFF && offset+instanceIDLength <= len(data) {
			groupInstanceID = string(data[offset : offset+instanceIDLength])
		}
	}

	return &LeaveGroupRequest{
		GroupID:         groupID,
		MemberID:        memberID,
		GroupInstanceID: groupInstanceID,
		Members:         []LeaveGroupMember{}, // Would parse members array for batch operations
	}, nil
}

func (h *Handler) buildHeartbeatResponse(response HeartbeatResponse) []byte {
	result := make([]byte, 0, 12)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, response.CorrelationID)
	result = append(result, correlationIDBytes...)

	// Error code (2 bytes)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
	result = append(result, errorCodeBytes...)

	// Throttle time (4 bytes, 0 = no throttling)
	result = append(result, 0, 0, 0, 0)

	return result
}

func (h *Handler) buildLeaveGroupResponse(response LeaveGroupResponse, apiVersion uint16) []byte {
	// LeaveGroup v0 only includes correlation_id and error_code (no throttle_time_ms, no members)
	if apiVersion == 0 {
		return h.buildLeaveGroupV0Response(response)
	}

	// For v1+ use the full response format
	return h.buildLeaveGroupFullResponse(response)
}

func (h *Handler) buildLeaveGroupV0Response(response LeaveGroupResponse) []byte {
	result := make([]byte, 0, 6)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, response.CorrelationID)
	result = append(result, correlationIDBytes...)

	// Error code (2 bytes) - that's it for v0!
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
	result = append(result, errorCodeBytes...)

	return result
}

func (h *Handler) buildLeaveGroupFullResponse(response LeaveGroupResponse) []byte {
	estimatedSize := 16
	for _, member := range response.Members {
		estimatedSize += len(member.MemberID) + len(member.GroupInstanceID) + 8
	}

	result := make([]byte, 0, estimatedSize)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, response.CorrelationID)
	result = append(result, correlationIDBytes...)

	// Error code (2 bytes)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
	result = append(result, errorCodeBytes...)

	// Members array length (4 bytes)
	membersLengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(membersLengthBytes, uint32(len(response.Members)))
	result = append(result, membersLengthBytes...)

	// Members
	for _, member := range response.Members {
		// Member ID length (2 bytes)
		memberIDLength := make([]byte, 2)
		binary.BigEndian.PutUint16(memberIDLength, uint16(len(member.MemberID)))
		result = append(result, memberIDLength...)

		// Member ID
		result = append(result, []byte(member.MemberID)...)

		// Group instance ID length (2 bytes)
		instanceIDLength := make([]byte, 2)
		binary.BigEndian.PutUint16(instanceIDLength, uint16(len(member.GroupInstanceID)))
		result = append(result, instanceIDLength...)

		// Group instance ID
		if len(member.GroupInstanceID) > 0 {
			result = append(result, []byte(member.GroupInstanceID)...)
		}

		// Error code (2 bytes)
		memberErrorBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(memberErrorBytes, uint16(member.ErrorCode))
		result = append(result, memberErrorBytes...)
	}

	// Throttle time (4 bytes, 0 = no throttling)
	result = append(result, 0, 0, 0, 0)

	return result
}

func (h *Handler) buildHeartbeatErrorResponse(correlationID uint32, errorCode int16) []byte {
	response := HeartbeatResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
	}

	return h.buildHeartbeatResponse(response)
}

func (h *Handler) buildLeaveGroupErrorResponse(correlationID uint32, errorCode int16, apiVersion uint16) []byte {
	response := LeaveGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
		Members:       []LeaveGroupMemberResponse{},
	}

	return h.buildLeaveGroupResponse(response, apiVersion)
}

func (h *Handler) updateGroupSubscriptionFromMembers(group *consumer.ConsumerGroup) {
	// Update group's subscribed topics from remaining members
	group.SubscribedTopics = make(map[string]bool)
	for _, member := range group.Members {
		for _, topic := range member.Subscription {
			group.SubscribedTopics[topic] = true
		}
	}
}
