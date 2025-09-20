package protocol

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/consumer"
)

// JoinGroup API (key 11) - Consumer group protocol
// Handles consumer joining a consumer group and initial coordination

// JoinGroupRequest represents a JoinGroup request from a Kafka client
type JoinGroupRequest struct {
	GroupID          string
	SessionTimeout   int32
	RebalanceTimeout int32
	MemberID         string // Empty for new members
	GroupInstanceID  string // Optional static membership
	ProtocolType     string // "consumer" for regular consumers
	GroupProtocols   []GroupProtocol
}

// GroupProtocol represents a supported assignment protocol
type GroupProtocol struct {
	Name     string
	Metadata []byte
}

// JoinGroupResponse represents a JoinGroup response to a Kafka client
type JoinGroupResponse struct {
	CorrelationID uint32
	ErrorCode     int16
	GenerationID  int32
	GroupProtocol string
	GroupLeader   string
	MemberID      string
	Version       uint16
	Members       []JoinGroupMember // Only populated for group leader
}

// JoinGroupMember represents member info sent to group leader
type JoinGroupMember struct {
	MemberID        string
	GroupInstanceID string
	Metadata        []byte
}

// Error codes for JoinGroup are imported from errors.go

func (h *Handler) handleJoinGroup(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// Parse JoinGroup request
	request, err := h.parseJoinGroupRequest(requestBody, apiVersion)
	if err != nil {
		return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	// Validate request
	if request.GroupID == "" {
		return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	if !h.groupCoordinator.ValidateSessionTimeout(request.SessionTimeout) {
		return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeInvalidSessionTimeout, apiVersion), nil
	}

	// Get or create consumer group
	group := h.groupCoordinator.GetOrCreateGroup(request.GroupID)

	group.Mu.Lock()
	defer group.Mu.Unlock()

	// Update group's last activity
	group.LastActivity = time.Now()

	// Handle member ID logic with static membership support
	var memberID string
	var isNewMember bool
	var existingMember *consumer.GroupMember

	// Check for static membership first
	if request.GroupInstanceID != "" {
		existingMember = h.groupCoordinator.FindStaticMemberLocked(group, request.GroupInstanceID)
		if existingMember != nil {
			memberID = existingMember.ID
			isNewMember = false
		} else {
			// New static member
			memberID = h.groupCoordinator.GenerateMemberID(request.GroupInstanceID, "static")
			isNewMember = true
		}
	} else {
		// Dynamic membership logic
		clientKey := fmt.Sprintf("%s-%d-%s", request.GroupID, request.SessionTimeout, request.ProtocolType)

		if request.MemberID == "" {
			// New member - check if we already have a member for this client
			var existingMemberID string
			for existingID, member := range group.Members {
				if member.ClientID == clientKey && !h.groupCoordinator.IsStaticMember(member) {
					existingMemberID = existingID
					break
				}
			}

			if existingMemberID != "" {
				// Reuse existing member ID for this client
				memberID = existingMemberID
				isNewMember = false
			} else {
				// Generate new deterministic member ID
				memberID = h.groupCoordinator.GenerateMemberID(clientKey, "consumer")
				isNewMember = true
			}
		} else {
			memberID = request.MemberID
			// Check if member exists
			if _, exists := group.Members[memberID]; !exists {
				// Member ID provided but doesn't exist - reject
				return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeUnknownMemberID, apiVersion), nil
			}
			isNewMember = false
		}
	}

	// Check group state
	switch group.State {
	case consumer.GroupStateEmpty, consumer.GroupStateStable:
		// Can join or trigger rebalance
		if isNewMember || len(group.Members) == 0 {
			group.State = consumer.GroupStatePreparingRebalance
			group.Generation++
		}
	case consumer.GroupStatePreparingRebalance:
		// Rebalance in progress - if this is the leader and we have members, transition to CompletingRebalance
		if len(group.Members) > 0 && memberID == group.Leader {
			group.State = consumer.GroupStateCompletingRebalance
		}
	case consumer.GroupStateCompletingRebalance:
		// Allow join but don't change generation until SyncGroup
	case consumer.GroupStateDead:
		return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	// Extract client host from connection context
	clientHost := ExtractClientHost(h.connContext)

	// Create or update member with enhanced metadata parsing
	var groupInstanceID *string
	if request.GroupInstanceID != "" {
		groupInstanceID = &request.GroupInstanceID
	}

	// Use deterministic client identifier based on group + session timeout + protocol
	clientKey := fmt.Sprintf("%s-%d-%s", request.GroupID, request.SessionTimeout, request.ProtocolType)

	member := &consumer.GroupMember{
		ID:               memberID,
		ClientID:         clientKey,  // Use deterministic client key for member identification
		ClientHost:       clientHost, // Now extracted from actual connection
		GroupInstanceID:  groupInstanceID,
		SessionTimeout:   request.SessionTimeout,
		RebalanceTimeout: request.RebalanceTimeout,
		Subscription:     h.extractSubscriptionFromProtocolsEnhanced(request.GroupProtocols),
		State:            consumer.MemberStatePending,
		LastHeartbeat:    time.Now(),
		JoinedAt:         time.Now(),
	}

	// Add or update the member in the group before computing subscriptions or leader
	if group.Members == nil {
		group.Members = make(map[string]*consumer.GroupMember)
	}
	group.Members[memberID] = member

	// Store protocol metadata for leader
	if len(request.GroupProtocols) > 0 {
		if len(request.GroupProtocols[0].Metadata) == 0 {
			// Generate subscription metadata for available topics
			availableTopics := h.getAvailableTopics()

			metadata := make([]byte, 0, 64)
			// Version (2 bytes) - use version 0
			metadata = append(metadata, 0, 0)
			// Topics count (4 bytes)
			topicsCount := make([]byte, 4)
			binary.BigEndian.PutUint32(topicsCount, uint32(len(availableTopics)))
			metadata = append(metadata, topicsCount...)
			// Topics (string array)
			for _, topic := range availableTopics {
				topicLen := make([]byte, 2)
				binary.BigEndian.PutUint16(topicLen, uint16(len(topic)))
				metadata = append(metadata, topicLen...)
				metadata = append(metadata, []byte(topic)...)
			}
			// UserData length (4 bytes) - empty
			metadata = append(metadata, 0, 0, 0, 0)
			member.Metadata = metadata
		} else {
			member.Metadata = request.GroupProtocols[0].Metadata
		}
	}

	// Add member to group
	group.Members[memberID] = member

	// Register static member if applicable
	if member.GroupInstanceID != nil && *member.GroupInstanceID != "" {
		h.groupCoordinator.RegisterStaticMemberLocked(group, member)
	}

	// Update group's subscribed topics
	h.updateGroupSubscription(group)

	// Select assignment protocol using enhanced selection logic
	// If the group already has a selected protocol, enforce compatibility with it.
	existingProtocols := make([]string, 0, 1)
	if group.Protocol != "" {
		existingProtocols = append(existingProtocols, group.Protocol)
	}

	groupProtocol := SelectBestProtocol(request.GroupProtocols, existingProtocols)

	// If a protocol is already selected for the group, reject joins that do not support it.
	if len(existingProtocols) > 0 && groupProtocol != group.Protocol {
		// Rollback member addition and static registration before returning error
		delete(group.Members, memberID)
		if member.GroupInstanceID != nil && *member.GroupInstanceID != "" {
			h.groupCoordinator.UnregisterStaticMemberLocked(group, *member.GroupInstanceID)
		}
		// Recompute group subscription without the rejected member
		h.updateGroupSubscription(group)
		return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeInconsistentGroupProtocol, apiVersion), nil
	}

	group.Protocol = groupProtocol

	// Select group leader (first member or keep existing if still present)
	if group.Leader == "" || group.Members[group.Leader] == nil {
		group.Leader = memberID
	} else {
	}

	// Build response - use the requested API version
	response := JoinGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     ErrorCodeNone,
		GenerationID:  group.Generation,
		GroupProtocol: groupProtocol,
		GroupLeader:   group.Leader,
		MemberID:      memberID,
		Version:       apiVersion,
	}

	// If this member is the leader, include all member info for assignment
	if memberID == group.Leader {
		response.Members = make([]JoinGroupMember, 0, len(group.Members))
		for mid, m := range group.Members {
			instanceID := ""
			if m.GroupInstanceID != nil {
				instanceID = *m.GroupInstanceID
			}
			response.Members = append(response.Members, JoinGroupMember{
				MemberID:        mid,
				GroupInstanceID: instanceID,
				Metadata:        m.Metadata,
			})
		}
	}

	resp := h.buildJoinGroupResponse(response)
	return resp, nil
}

func (h *Handler) parseJoinGroupRequest(data []byte, apiVersion uint16) (*JoinGroupRequest, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0

	// JoinGroup v5 body starts with GroupID according to Kafka spec

	// GroupID (string)
	if offset+2 > len(data) {
		return nil, fmt.Errorf("missing group ID length")
	}
	groupIDLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	if offset+groupIDLength > len(data) {
		return nil, fmt.Errorf("invalid group ID length")
	}
	groupID := string(data[offset : offset+groupIDLength])
	offset += groupIDLength

	// Session timeout (4 bytes)
	if offset+4 > len(data) {
		return nil, fmt.Errorf("missing session timeout")
	}
	sessionTimeout := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Rebalance timeout (4 bytes) - for v1+ versions
	rebalanceTimeout := sessionTimeout // Default to session timeout for v0
	if apiVersion >= 1 && offset+4 <= len(data) {
		rebalanceTimeout = int32(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
	}

	// MemberID (string)
	if offset+2 > len(data) {
		return nil, fmt.Errorf("missing member ID length")
	}
	memberIDLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2
	memberID := ""
	if memberIDLength > 0 {
		if offset+memberIDLength > len(data) {
			return nil, fmt.Errorf("invalid member ID length")
		}
		memberID = string(data[offset : offset+memberIDLength])
		offset += memberIDLength
	}

	// Parse Group Instance ID (nullable string) - for JoinGroup v5+
	var groupInstanceID string
	if apiVersion >= 5 {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("missing group instance ID length")
		}
		instanceIDLength := int16(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		if instanceIDLength == -1 {
			groupInstanceID = "" // null string
		} else if instanceIDLength >= 0 {
			if offset+int(instanceIDLength) > len(data) {
				return nil, fmt.Errorf("invalid group instance ID length")
			}
			groupInstanceID = string(data[offset : offset+int(instanceIDLength)])
			offset += int(instanceIDLength)
		}
	}

	// Parse Protocol Type
	if len(data) < offset+2 {
		return nil, fmt.Errorf("JoinGroup request missing protocol type")
	}
	protocolTypeLength := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	if len(data) < offset+int(protocolTypeLength) {
		return nil, fmt.Errorf("JoinGroup request protocol type too short")
	}
	protocolType := string(data[offset : offset+int(protocolTypeLength)])
	offset += int(protocolTypeLength)

	// Parse Group Protocols array
	if len(data) < offset+4 {
		return nil, fmt.Errorf("JoinGroup request missing group protocols")
	}
	protocolsCount := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	protocols := make([]GroupProtocol, 0, protocolsCount)

	for i := uint32(0); i < protocolsCount && offset < len(data); i++ {
		// Parse protocol name
		if len(data) < offset+2 {
			break
		}
		protocolNameLength := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		if len(data) < offset+int(protocolNameLength) {
			break
		}
		protocolName := string(data[offset : offset+int(protocolNameLength)])
		offset += int(protocolNameLength)

		// Parse protocol metadata
		if len(data) < offset+4 {
			break
		}
		metadataLength := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		var metadata []byte
		if metadataLength > 0 && len(data) >= offset+int(metadataLength) {
			metadata = make([]byte, metadataLength)
			copy(metadata, data[offset:offset+int(metadataLength)])
			offset += int(metadataLength)
		}

		protocols = append(protocols, GroupProtocol{
			Name:     protocolName,
			Metadata: metadata,
		})

	}

	return &JoinGroupRequest{
		GroupID:          groupID,
		SessionTimeout:   sessionTimeout,
		RebalanceTimeout: rebalanceTimeout,
		MemberID:         memberID,
		GroupInstanceID:  groupInstanceID,
		ProtocolType:     protocolType,
		GroupProtocols:   protocols,
	}, nil
}

func (h *Handler) buildJoinGroupResponse(response JoinGroupResponse) []byte {
	// Estimate response size
	estimatedSize := 0
	// CorrelationID(4) + (optional throttle 4) + error_code(2) + generation_id(4)
	if response.Version >= 2 {
		estimatedSize = 4 + 4 + 2 + 4
	} else {
		estimatedSize = 4 + 2 + 4
	}
	estimatedSize += 2 + len(response.GroupProtocol) // protocol string
	estimatedSize += 2 + len(response.GroupLeader)   // leader string
	estimatedSize += 2 + len(response.MemberID)      // member id string
	estimatedSize += 4                               // members array count
	for _, member := range response.Members {
		// MemberID string
		estimatedSize += 2 + len(member.MemberID)
		if response.Version >= 5 {
			// GroupInstanceID string
			estimatedSize += 2 + len(member.GroupInstanceID)
		}
		// Metadata bytes (4 + len)
		estimatedSize += 4 + len(member.Metadata)
	}

	result := make([]byte, 0, estimatedSize)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, response.CorrelationID)
	result = append(result, correlationIDBytes...)

	// JoinGroup v2 adds throttle_time_ms
	if response.Version >= 2 {
		throttleTimeBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(throttleTimeBytes, 0) // No throttling
		result = append(result, throttleTimeBytes...)
	}

	// Error code (2 bytes)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
	result = append(result, errorCodeBytes...)

	// Generation ID (4 bytes)
	generationBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(generationBytes, uint32(response.GenerationID))
	result = append(result, generationBytes...)

	// Group protocol (string)
	protocolLength := make([]byte, 2)
	binary.BigEndian.PutUint16(protocolLength, uint16(len(response.GroupProtocol)))
	result = append(result, protocolLength...)
	result = append(result, []byte(response.GroupProtocol)...)

	// Group leader (string)
	leaderLength := make([]byte, 2)
	binary.BigEndian.PutUint16(leaderLength, uint16(len(response.GroupLeader)))
	result = append(result, leaderLength...)
	result = append(result, []byte(response.GroupLeader)...)

	// Member ID (string)
	memberIDLength := make([]byte, 2)
	binary.BigEndian.PutUint16(memberIDLength, uint16(len(response.MemberID)))
	result = append(result, memberIDLength...)
	result = append(result, []byte(response.MemberID)...)

	// Members array (4 bytes count + members)
	memberCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(memberCountBytes, uint32(len(response.Members)))
	result = append(result, memberCountBytes...)

	for _, member := range response.Members {
		// Member ID (string)
		memberLength := make([]byte, 2)
		binary.BigEndian.PutUint16(memberLength, uint16(len(member.MemberID)))
		result = append(result, memberLength...)
		result = append(result, []byte(member.MemberID)...)

		if response.Version >= 5 {
			// Group instance ID (string) - can be empty
			instanceIDLength := make([]byte, 2)
			binary.BigEndian.PutUint16(instanceIDLength, uint16(len(member.GroupInstanceID)))
			result = append(result, instanceIDLength...)
			if len(member.GroupInstanceID) > 0 {
				result = append(result, []byte(member.GroupInstanceID)...)
			}
		}

		// Metadata (bytes)
		metadataLength := make([]byte, 4)
		binary.BigEndian.PutUint32(metadataLength, uint32(len(member.Metadata)))
		result = append(result, metadataLength...)
		result = append(result, member.Metadata...)
	}

	return result
}

func (h *Handler) buildJoinGroupErrorResponse(correlationID uint32, errorCode int16, apiVersion uint16) []byte {
	response := JoinGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
		GenerationID:  -1,
		GroupProtocol: "",
		GroupLeader:   "",
		MemberID:      "",
		Version:       apiVersion,
		Members:       []JoinGroupMember{},
	}

	return h.buildJoinGroupResponse(response)
}

// extractSubscriptionFromProtocolsEnhanced uses improved metadata parsing with better error handling
func (h *Handler) extractSubscriptionFromProtocolsEnhanced(protocols []GroupProtocol) []string {
	// Analyze protocol metadata for debugging
	debugInfo := AnalyzeProtocolMetadata(protocols)
	for _, info := range debugInfo {
		if info.ParsedOK {
		} else {
		}
	}

	// Extract topics using enhanced parsing
	topics := ExtractTopicsFromMetadata(protocols, h.getAvailableTopics())

	return topics
}

func (h *Handler) updateGroupSubscription(group *consumer.ConsumerGroup) {
	// Update group's subscribed topics from all members
	group.SubscribedTopics = make(map[string]bool)
	for _, member := range group.Members {
		for _, topic := range member.Subscription {
			group.SubscribedTopics[topic] = true
		}
	}
}

// SyncGroup API (key 14) - Consumer group coordination completion
// Called by group members after JoinGroup to get partition assignments

// SyncGroupRequest represents a SyncGroup request from a Kafka client
type SyncGroupRequest struct {
	GroupID          string
	GenerationID     int32
	MemberID         string
	GroupInstanceID  string
	GroupAssignments []GroupAssignment // Only from group leader
}

// GroupAssignment represents partition assignment for a group member
type GroupAssignment struct {
	MemberID   string
	Assignment []byte // Serialized assignment data
}

// SyncGroupResponse represents a SyncGroup response to a Kafka client
type SyncGroupResponse struct {
	CorrelationID uint32
	ErrorCode     int16
	Assignment    []byte // Serialized partition assignment for this member
}

// Additional error codes for SyncGroup
// Error codes for SyncGroup are imported from errors.go

func (h *Handler) handleSyncGroup(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// Parse SyncGroup request
	request, err := h.parseSyncGroupRequest(requestBody, apiVersion)
	if err != nil {
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	// Validate request
	if request.GroupID == "" || request.MemberID == "" {
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	// Get consumer group
	group := h.groupCoordinator.GetGroup(request.GroupID)
	if group == nil {
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	group.Mu.Lock()
	defer group.Mu.Unlock()

	// Update group's last activity
	group.LastActivity = time.Now()

	// Validate member exists
	member, exists := group.Members[request.MemberID]
	if !exists {
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeUnknownMemberID, apiVersion), nil
	}

	// Validate generation
	if request.GenerationID != group.Generation {
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeIllegalGeneration, apiVersion), nil
	}

	// Check if this is the group leader with assignments
	if request.MemberID == group.Leader && len(request.GroupAssignments) > 0 {
		// Leader is providing assignments - process and store them
		err = h.processGroupAssignments(group, request.GroupAssignments)
		if err != nil {
			return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeInconsistentGroupProtocol, apiVersion), nil
		}

		// Move group to stable state
		group.State = consumer.GroupStateStable

		// Mark all members as stable
		for _, m := range group.Members {
			m.State = consumer.MemberStateStable
		}
	} else if group.State == consumer.GroupStateCompletingRebalance {
		// Non-leader member waiting for assignments
		// Assignments should already be processed by leader
	} else {
		// Trigger partition assignment using built-in strategy
		topicPartitions := h.getTopicPartitions(group)
		group.AssignPartitions(topicPartitions)

		group.State = consumer.GroupStateStable
		for _, m := range group.Members {
			m.State = consumer.MemberStateStable
		}
	}

	// Get assignment for this member
	assignment := h.serializeMemberAssignment(member.Assignment)

	// Build response
	response := SyncGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     ErrorCodeNone,
		Assignment:    assignment,
	}

	resp := h.buildSyncGroupResponse(response, apiVersion)
	return resp, nil
}

func (h *Handler) parseSyncGroupRequest(data []byte, apiVersion uint16) (*SyncGroupRequest, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0

	// SyncGroup v3 body starts with GroupID according to Kafka spec

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

	// GroupInstanceID (nullable string) - for SyncGroup v3+
	var groupInstanceID string
	if apiVersion >= 3 {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("missing group instance ID length")
		}
		instanceIDLength := int16(binary.BigEndian.Uint16(data[offset:]))
		offset += 2

		if instanceIDLength == -1 {
			groupInstanceID = "" // null string
		} else if instanceIDLength >= 0 {
			if offset+int(instanceIDLength) > len(data) {
				return nil, fmt.Errorf("invalid group instance ID length")
			}
			groupInstanceID = string(data[offset : offset+int(instanceIDLength)])
			offset += int(instanceIDLength)
		}
	}

	// Parse assignments array if present (leader sends assignments)
	assignments := make([]GroupAssignment, 0)

	if offset+4 <= len(data) {
		assignmentsCount := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4

		// Basic sanity check to avoid very large allocations
		if assignmentsCount >= 0 && assignmentsCount < 10000 {
			for i := 0; i < assignmentsCount && offset < len(data); i++ {
				// member_id (string)
				if offset+2 > len(data) {
					break
				}
				memberLen := int(binary.BigEndian.Uint16(data[offset:]))
				offset += 2
				if memberLen < 0 || offset+memberLen > len(data) {
					break
				}
				mID := string(data[offset : offset+memberLen])
				offset += memberLen

				// assignment (bytes)
				if offset+4 > len(data) {
					break
				}
				assignLen := int(binary.BigEndian.Uint32(data[offset:]))
				offset += 4
				if assignLen < 0 || offset+assignLen > len(data) {
					break
				}
				var assign []byte
				if assignLen > 0 {
					assign = make([]byte, assignLen)
					copy(assign, data[offset:offset+assignLen])
				}
				offset += assignLen

				assignments = append(assignments, GroupAssignment{MemberID: mID, Assignment: assign})
			}
		}
	}

	return &SyncGroupRequest{
		GroupID:          groupID,
		GenerationID:     generationID,
		MemberID:         memberID,
		GroupInstanceID:  groupInstanceID,
		GroupAssignments: assignments,
	}, nil
}

func (h *Handler) buildSyncGroupResponse(response SyncGroupResponse, apiVersion uint16) []byte {
	estimatedSize := 16 + len(response.Assignment)
	result := make([]byte, 0, estimatedSize)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, response.CorrelationID)
	result = append(result, correlationIDBytes...)

	// SyncGroup v1+ has throttle_time_ms at the beginning
	// SyncGroup v0 does NOT include throttle_time_ms
	if apiVersion >= 1 {
		// Throttle time (4 bytes, 0 = no throttling)
		result = append(result, 0, 0, 0, 0)
	}

	// Error code (2 bytes)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
	result = append(result, errorCodeBytes...)

	// Assignment (bytes)
	assignmentLength := make([]byte, 4)
	binary.BigEndian.PutUint32(assignmentLength, uint32(len(response.Assignment)))
	result = append(result, assignmentLength...)
	result = append(result, response.Assignment...)

	return result
}

func (h *Handler) buildSyncGroupErrorResponse(correlationID uint32, errorCode int16, apiVersion uint16) []byte {
	response := SyncGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
		Assignment:    []byte{},
	}

	return h.buildSyncGroupResponse(response, apiVersion)
}

func (h *Handler) processGroupAssignments(group *consumer.ConsumerGroup, assignments []GroupAssignment) error {
	// Apply leader-provided assignments
	// Clear current assignments
	for _, m := range group.Members {
		m.Assignment = nil
	}

	for _, ga := range assignments {
		m, ok := group.Members[ga.MemberID]
		if !ok {
			// Skip unknown members
			continue
		}

		parsed, err := h.parseMemberAssignment(ga.Assignment)
		if err != nil {
			return err
		}
		m.Assignment = parsed
	}

	return nil
}

// parseMemberAssignment decodes ConsumerGroupMemberAssignment bytes into assignments
func (h *Handler) parseMemberAssignment(data []byte) ([]consumer.PartitionAssignment, error) {
	if len(data) < 2+4 {
		// Empty or missing; treat as no assignment
		return []consumer.PartitionAssignment{}, nil
	}

	offset := 0

	// Version (2 bytes)
	if offset+2 > len(data) {
		return nil, fmt.Errorf("assignment too short for version")
	}
	_ = int16(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2

	// Number of topics (4 bytes)
	if offset+4 > len(data) {
		return nil, fmt.Errorf("assignment too short for topics count")
	}
	topicsCount := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	if topicsCount < 0 || topicsCount > 100000 {
		return nil, fmt.Errorf("unreasonable topics count in assignment: %d", topicsCount)
	}

	result := make([]consumer.PartitionAssignment, 0)

	for i := 0; i < topicsCount && offset < len(data); i++ {
		// topic string
		if offset+2 > len(data) {
			return nil, fmt.Errorf("assignment truncated reading topic len")
		}
		tlen := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if tlen < 0 || offset+tlen > len(data) {
			return nil, fmt.Errorf("assignment truncated reading topic name")
		}
		topic := string(data[offset : offset+tlen])
		offset += tlen

		// partitions array length
		if offset+4 > len(data) {
			return nil, fmt.Errorf("assignment truncated reading partitions len")
		}
		numPartitions := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if numPartitions < 0 || numPartitions > 1000000 {
			return nil, fmt.Errorf("unreasonable partitions count: %d", numPartitions)
		}

		for p := 0; p < numPartitions; p++ {
			if offset+4 > len(data) {
				return nil, fmt.Errorf("assignment truncated reading partition id")
			}
			pid := int32(binary.BigEndian.Uint32(data[offset:]))
			offset += 4
			result = append(result, consumer.PartitionAssignment{Topic: topic, Partition: pid})
		}
	}

	// Optional UserData: bytes length + data. Safe to ignore.
	// If present but truncated, ignore silently.

	return result, nil
}

func (h *Handler) getTopicPartitions(group *consumer.ConsumerGroup) map[string][]int32 {
	topicPartitions := make(map[string][]int32)

	// Get partition info for all subscribed topics
	for topic := range group.SubscribedTopics {
		// Check if topic exists using SeaweedMQ handler
		if h.seaweedMQHandler.TopicExists(topic) {
			// For now, assume 1 partition per topic (can be extended later)
			// In a real implementation, this would query SeaweedMQ for actual partition count
			partitions := []int32{0}
			topicPartitions[topic] = partitions
		} else {
			// Default to single partition if topic not found
			topicPartitions[topic] = []int32{0}
		}
	}

	return topicPartitions
}

func (h *Handler) serializeMemberAssignment(assignments []consumer.PartitionAssignment) []byte {
	// Build ConsumerGroupMemberAssignment format exactly as Sarama expects:
	// Version(2) + Topics array + UserData bytes

	// Group assignments by topic
	topicAssignments := make(map[string][]int32)
	for _, assignment := range assignments {
		topicAssignments[assignment.Topic] = append(topicAssignments[assignment.Topic], assignment.Partition)
	}

	result := make([]byte, 0, 64)

	// Version (2 bytes) - use version 1
	result = append(result, 0, 1)

	// Number of topics (4 bytes) - array length
	numTopicsBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(numTopicsBytes, uint32(len(topicAssignments)))
	result = append(result, numTopicsBytes...)

	// Topics - each topic follows Kafka string + int32 array format
	for topic, partitions := range topicAssignments {
		// Topic name as Kafka string: length(2) + content
		topicLenBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(topicLenBytes, uint16(len(topic)))
		result = append(result, topicLenBytes...)
		result = append(result, []byte(topic)...)

		// Partitions as int32 array: length(4) + elements
		numPartitionsBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(numPartitionsBytes, uint32(len(partitions)))
		result = append(result, numPartitionsBytes...)

		// Partitions (4 bytes each)
		for _, partition := range partitions {
			partitionBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(partitionBytes, uint32(partition))
			result = append(result, partitionBytes...)
		}
	}

	// UserData as Kafka bytes: length(4) + data (empty in our case)
	// For empty user data, just put length = 0
	result = append(result, 0, 0, 0, 0)

	return result
}

// getAvailableTopics returns list of available topics for subscription metadata
func (h *Handler) getAvailableTopics() []string {
	return h.seaweedMQHandler.ListTopics()
}
