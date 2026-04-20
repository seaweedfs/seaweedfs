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

func (h *Handler) handleHeartbeat(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// Parse Heartbeat request
	request, err := h.parseHeartbeatRequest(requestBody, apiVersion)
	if err != nil {
		return h.buildHeartbeatErrorResponseV(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	// Validate request
	if request.GroupID == "" || request.MemberID == "" {
		return h.buildHeartbeatErrorResponseV(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	// Get consumer group
	group := h.groupCoordinator.GetGroup(request.GroupID)
	if group == nil {
		return h.buildHeartbeatErrorResponseV(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	group.Mu.Lock()
	defer group.Mu.Unlock()

	// Update group's last activity
	group.LastActivity = time.Now()

	// Validate member exists
	member, exists := group.Members[request.MemberID]
	if !exists {
		return h.buildHeartbeatErrorResponseV(correlationID, ErrorCodeUnknownMemberID, apiVersion), nil
	}

	// Validate generation. If the group is rebalancing we want Sarama to rejoin
	// (via ErrRebalanceInProgress -> heartbeatLoop cancels session -> newSession)
	// rather than tear down on ErrIllegalGeneration, which would exit the caller's
	// Consume() entirely since Consume does not internally re-enter on that error.
	if request.GenerationID != group.Generation {
		switch group.State {
		case consumer.GroupStatePreparingRebalance, consumer.GroupStateCompletingRebalance:
			return h.buildHeartbeatErrorResponseV(correlationID, ErrorCodeRebalanceInProgress, apiVersion), nil
		default:
			return h.buildHeartbeatErrorResponseV(correlationID, ErrorCodeIllegalGeneration, apiVersion), nil
		}
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

	return h.buildHeartbeatResponseV(response, apiVersion), nil
}

func (h *Handler) handleLeaveGroup(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// Parse LeaveGroup request
	request, err := h.parseLeaveGroupRequest(requestBody, apiVersion)
	if err != nil {
		return h.buildLeaveGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	// Validate request - v3+ carries member IDs inside the Members array
	if request.GroupID == "" {
		return h.buildLeaveGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}
	if request.MemberID == "" && len(request.Members) == 0 {
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

func (h *Handler) parseHeartbeatRequest(data []byte, apiVersion uint16) (*HeartbeatRequest, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0
	isFlexible := IsFlexibleVersion(12, apiVersion) // Heartbeat API key = 12

	// ADMINCLIENT COMPATIBILITY FIX: Parse top-level tagged fields at the beginning for flexible versions
	if isFlexible {
		_, consumed, err := DecodeTaggedFields(data[offset:])
		if err == nil {
			offset += consumed
		}
	}

	// Parse GroupID
	var groupID string
	if isFlexible {
		// FLEXIBLE V4+ FIX: GroupID is a compact string
		groupIDBytes, consumed := parseCompactString(data[offset:])
		if consumed == 0 {
			return nil, fmt.Errorf("invalid group ID compact string")
		}
		if groupIDBytes != nil {
			groupID = string(groupIDBytes)
		}
		offset += consumed
	} else {
		// Non-flexible parsing (v0-v3)
		groupIDLength := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+groupIDLength > len(data) {
			return nil, fmt.Errorf("invalid group ID length")
		}
		groupID = string(data[offset : offset+groupIDLength])
		offset += groupIDLength
	}

	// Generation ID (4 bytes) - always fixed-length
	if offset+4 > len(data) {
		return nil, fmt.Errorf("missing generation ID")
	}
	generationID := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Parse MemberID
	var memberID string
	if isFlexible {
		// FLEXIBLE V4+ FIX: MemberID is a compact string
		memberIDBytes, consumed := parseCompactString(data[offset:])
		if consumed == 0 {
			return nil, fmt.Errorf("invalid member ID compact string")
		}
		if memberIDBytes != nil {
			memberID = string(memberIDBytes)
		}
		offset += consumed
	} else {
		// Non-flexible parsing (v0-v3)
		if offset+2 > len(data) {
			return nil, fmt.Errorf("missing member ID length")
		}
		memberIDLength := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+memberIDLength > len(data) {
			return nil, fmt.Errorf("invalid member ID length")
		}
		memberID = string(data[offset : offset+memberIDLength])
		offset += memberIDLength
	}

	// Parse GroupInstanceID (nullable string) - for Heartbeat v1+
	var groupInstanceID string
	if apiVersion >= 1 {
		if isFlexible {
			// FLEXIBLE V4+ FIX: GroupInstanceID is a compact nullable string
			groupInstanceIDBytes, consumed := parseCompactString(data[offset:])
			if consumed == 0 && len(data) > offset && data[offset] == 0x00 {
				groupInstanceID = "" // null
				offset += 1
			} else {
				if groupInstanceIDBytes != nil {
					groupInstanceID = string(groupInstanceIDBytes)
				}
				offset += consumed
			}
		} else {
			// Non-flexible v1-v3: regular nullable string
			if offset+2 <= len(data) {
				instanceIDLength := int16(binary.BigEndian.Uint16(data[offset:]))
				offset += 2
				if instanceIDLength == -1 {
					groupInstanceID = "" // null string
				} else if instanceIDLength >= 0 && offset+int(instanceIDLength) <= len(data) {
					groupInstanceID = string(data[offset : offset+int(instanceIDLength)])
					offset += int(instanceIDLength)
				}
			}
		}
	}

	// Parse request-level tagged fields (v4+)
	if isFlexible {
		if offset < len(data) {
			_, consumed, err := DecodeTaggedFields(data[offset:])
			if err == nil {
				offset += consumed
			}
		}
	}

	return &HeartbeatRequest{
		GroupID:         groupID,
		GenerationID:    generationID,
		MemberID:        memberID,
		GroupInstanceID: groupInstanceID,
	}, nil
}

func (h *Handler) parseLeaveGroupRequest(data []byte, apiVersion uint16) (*LeaveGroupRequest, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0
	isFlexible := IsFlexibleVersion(uint16(APIKeyLeaveGroup), apiVersion)

	if isFlexible {
		// Skip top-level tagged fields
		_, consumed, err := DecodeTaggedFields(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("LeaveGroup v%d: top-level tagged fields: %w", apiVersion, err)
		}
		offset += consumed
	}

	// GroupID
	var groupID string
	if isFlexible {
		bytes, consumed := parseCompactString(data[offset:])
		if consumed == 0 {
			return nil, fmt.Errorf("LeaveGroup v%d: invalid group ID compact string", apiVersion)
		}
		if bytes != nil {
			groupID = string(bytes)
		}
		offset += consumed
	} else {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("missing group ID length")
		}
		groupIDLength := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+groupIDLength > len(data) {
			return nil, fmt.Errorf("invalid group ID length")
		}
		groupID = string(data[offset : offset+groupIDLength])
		offset += groupIDLength
	}

	req := &LeaveGroupRequest{
		GroupID: groupID,
		Members: []LeaveGroupMember{},
	}

	// v0-v2: top-level MemberID (string). v3+: Members array.
	if apiVersion <= 2 {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("missing member ID length")
		}
		memberIDLength := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if offset+memberIDLength > len(data) {
			return nil, fmt.Errorf("invalid member ID length")
		}
		req.MemberID = string(data[offset : offset+memberIDLength])
		offset += memberIDLength
		return req, nil
	}

	// v3+: Members array [{MemberID, GroupInstanceID[, Reason]}]
	var membersCount int
	if isFlexible {
		compactLen, consumed, err := DecodeCompactArrayLength(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("LeaveGroup v%d: invalid members compact array: %w", apiVersion, err)
		}
		membersCount = int(compactLen)
		offset += consumed
	} else {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("missing members array length")
		}
		membersCount = int(int32(binary.BigEndian.Uint32(data[offset:])))
		offset += 4
	}

	if membersCount < 0 || membersCount > 100000 {
		return nil, fmt.Errorf("unreasonable members count: %d", membersCount)
	}

	for i := 0; i < membersCount; i++ {
		var m LeaveGroupMember
		// MemberID
		if isFlexible {
			bytes, consumed := parseCompactString(data[offset:])
			if consumed == 0 {
				return nil, fmt.Errorf("LeaveGroup v%d: invalid members[%d].member_id", apiVersion, i)
			}
			if bytes != nil {
				m.MemberID = string(bytes)
			}
			offset += consumed
		} else {
			if offset+2 > len(data) {
				return nil, fmt.Errorf("members[%d]: missing member_id length", i)
			}
			ml := int(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			if offset+ml > len(data) {
				return nil, fmt.Errorf("members[%d]: invalid member_id length", i)
			}
			m.MemberID = string(data[offset : offset+ml])
			offset += ml
		}
		// GroupInstanceID (nullable)
		if isFlexible {
			bytes, consumed := parseCompactString(data[offset:])
			if consumed == 0 {
				return nil, fmt.Errorf("LeaveGroup v%d: invalid members[%d].group_instance_id", apiVersion, i)
			}
			if bytes != nil {
				m.GroupInstanceID = string(bytes)
			}
			offset += consumed
		} else {
			if offset+2 > len(data) {
				return nil, fmt.Errorf("members[%d]: missing group_instance_id length", i)
			}
			gil := int16(binary.BigEndian.Uint16(data[offset:]))
			offset += 2
			if gil >= 0 {
				if offset+int(gil) > len(data) {
					return nil, fmt.Errorf("members[%d]: invalid group_instance_id length", i)
				}
				m.GroupInstanceID = string(data[offset : offset+int(gil)])
				offset += int(gil)
			}
		}
		// v5+: Reason (compact nullable string, flexible only)
		if apiVersion >= 5 && isFlexible {
			bytes, consumed := parseCompactString(data[offset:])
			if consumed > 0 {
				if bytes != nil {
					m.Reason = string(bytes)
				}
				offset += consumed
			}
		}
		// Per-member tagged fields
		if isFlexible {
			_, consumed, err := DecodeTaggedFields(data[offset:])
			if err != nil {
				return nil, fmt.Errorf("LeaveGroup v%d: members[%d] tagged fields: %w", apiVersion, i, err)
			}
			offset += consumed
		}
		req.Members = append(req.Members, m)
	}

	// Back-compat convenience: expose the first member as top-level MemberID so
	// handler code that pre-dates Members support keeps working.
	if req.MemberID == "" && len(req.Members) > 0 {
		req.MemberID = req.Members[0].MemberID
		req.GroupInstanceID = req.Members[0].GroupInstanceID
	}

	return req, nil
}

func (h *Handler) buildHeartbeatResponse(response HeartbeatResponse) []byte {
	result := make([]byte, 0, 12)

	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// Error code (2 bytes)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
	result = append(result, errorCodeBytes...)

	// Throttle time (4 bytes, 0 = no throttling)
	result = append(result, 0, 0, 0, 0)

	return result
}

func (h *Handler) buildHeartbeatResponseV(response HeartbeatResponse, apiVersion uint16) []byte {
	isFlexible := IsFlexibleVersion(12, apiVersion) // Heartbeat API key = 12
	result := make([]byte, 0, 16)

	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	if isFlexible {
		// FLEXIBLE V4+ FORMAT
		// NOTE: Response header tagged fields are handled by writeResponseWithHeader
		// Do NOT include them in the response body

		// Throttle time (4 bytes, 0 = no throttling) - comes first in flexible format
		result = append(result, 0, 0, 0, 0)

		// Error code (2 bytes)
		errorCodeBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
		result = append(result, errorCodeBytes...)

		// Response body tagged fields (varint: 0x00 = empty)
		result = append(result, 0x00)
	} else if apiVersion >= 1 {
		// NON-FLEXIBLE V1-V3 FORMAT: throttle_time_ms BEFORE error_code
		// CRITICAL FIX: Kafka protocol specifies throttle_time_ms comes FIRST in v1+

		// Throttle time (4 bytes, 0 = no throttling) - comes first in v1-v3
		result = append(result, 0, 0, 0, 0)

		// Error code (2 bytes)
		errorCodeBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
		result = append(result, errorCodeBytes...)
	} else {
		// V0 FORMAT: Only error_code, NO throttle_time_ms

		// Error code (2 bytes)
		errorCodeBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
		result = append(result, errorCodeBytes...)
	}

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

	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

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

	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

	// For LeaveGroup v1+, throttle_time_ms comes first (4 bytes)
	result = append(result, 0, 0, 0, 0)

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

	return result
}

func (h *Handler) buildHeartbeatErrorResponse(correlationID uint32, errorCode int16) []byte {
	response := HeartbeatResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
	}

	return h.buildHeartbeatResponse(response)
}

func (h *Handler) buildHeartbeatErrorResponseV(correlationID uint32, errorCode int16, apiVersion uint16) []byte {
	response := HeartbeatResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
	}

	return h.buildHeartbeatResponseV(response, apiVersion)
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
