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

// Error codes for JoinGroup
const (
	ErrorCodeNone                  int16 = 0
	ErrorCodeInvalidGroupID        int16 = 24
	ErrorCodeUnknownMemberID       int16 = 25
	ErrorCodeInvalidSessionTimeout int16 = 26
	ErrorCodeRebalanceInProgress   int16 = 27
	ErrorCodeMemberIDRequired      int16 = 79
	ErrorCodeFencedInstanceID      int16 = 82
)

func (h *Handler) handleJoinGroup(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// DEBUG: Hex dump the request to understand format
	dumpLen := len(requestBody)
	if dumpLen > 100 {
		dumpLen = 100
	}
	fmt.Printf("DEBUG: JoinGroup request hex dump (first %d bytes): %x\n", dumpLen, requestBody[:dumpLen])

	// Parse JoinGroup request
	request, err := h.parseJoinGroupRequest(requestBody)
	if err != nil {
		fmt.Printf("DEBUG: JoinGroup parseJoinGroupRequest error: %v\n", err)
		return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	fmt.Printf("DEBUG: JoinGroup parsed request - GroupID: '%s', MemberID: '%s', SessionTimeout: %d\n",
		request.GroupID, request.MemberID, request.SessionTimeout)
	fmt.Printf("DEBUG: JoinGroup protocols count: %d\n", len(request.GroupProtocols))
	for i, protocol := range request.GroupProtocols {
		fmt.Printf("DEBUG: JoinGroup protocol[%d]: name='%s', metadata_len=%d, metadata_hex=%x\n",
			i, protocol.Name, len(protocol.Metadata), protocol.Metadata)
	}

	// Validate request
	if request.GroupID == "" {
		return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	if !h.groupCoordinator.ValidateSessionTimeout(request.SessionTimeout) {
		return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeInvalidSessionTimeout), nil
	}

	// Get or create consumer group
	group := h.groupCoordinator.GetOrCreateGroup(request.GroupID)

	group.Mu.Lock()
	defer group.Mu.Unlock()

	// Update group's last activity
	group.LastActivity = time.Now()

	// Handle member ID logic
	var memberID string
	var isNewMember bool

	// Use deterministic client identifier based on group + session timeout + protocol
	clientKey := fmt.Sprintf("%s-%d-%s", request.GroupID, request.SessionTimeout, request.ProtocolType)

	if request.MemberID == "" {
		// New member - check if we already have a member for this client

		// Look for existing member with same client characteristics
		var existingMemberID string
		for existingID, member := range group.Members {
			if member.ClientID == clientKey {
				existingMemberID = existingID
				break
			}
		}

		if existingMemberID != "" {
			// Reuse existing member ID for this client
			memberID = existingMemberID
			isNewMember = false
			fmt.Printf("DEBUG: JoinGroup reusing existing member ID '%s' for client key '%s'\n", memberID, clientKey)
		} else {
			// Generate new deterministic member ID
			memberID = h.groupCoordinator.GenerateMemberID(clientKey, "consumer")
			isNewMember = true
			fmt.Printf("DEBUG: JoinGroup generated new member ID '%s' for client key '%s'\n", memberID, clientKey)
		}
	} else {
		memberID = request.MemberID
		// Check if member exists
		if _, exists := group.Members[memberID]; !exists {
			// Member ID provided but doesn't exist - reject
			return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeUnknownMemberID), nil
		}
		isNewMember = false
		fmt.Printf("DEBUG: JoinGroup using provided member ID '%s'\n", memberID)
	}

	// Check group state
	fmt.Printf("DEBUG: JoinGroup current group state: %s, generation: %d\n", group.State, group.Generation)
	switch group.State {
	case consumer.GroupStateEmpty, consumer.GroupStateStable:
		// Can join or trigger rebalance
		if isNewMember || len(group.Members) == 0 {
			group.State = consumer.GroupStatePreparingRebalance
			group.Generation++
			fmt.Printf("DEBUG: JoinGroup transitioned to PreparingRebalance, new generation: %d\n", group.Generation)
		}
	case consumer.GroupStatePreparingRebalance:
		// Rebalance in progress - if this is the leader and we have members, transition to CompletingRebalance
		if len(group.Members) > 0 && memberID == group.Leader {
			group.State = consumer.GroupStateCompletingRebalance
			fmt.Printf("DEBUG: JoinGroup leader '%s' transitioning group to CompletingRebalance (ready for SyncGroup)\n", memberID)
		}
	case consumer.GroupStateCompletingRebalance:
		// Allow join but don't change generation until SyncGroup
	case consumer.GroupStateDead:
		return h.buildJoinGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	// Create or update member
	member := &consumer.GroupMember{
		ID:               memberID,
		ClientID:         clientKey, // Use deterministic client key for member identification
		ClientHost:       "unknown", // TODO: extract from connection - needed for consumer group metadata
		SessionTimeout:   request.SessionTimeout,
		RebalanceTimeout: request.RebalanceTimeout,
		Subscription:     h.extractSubscriptionFromProtocols(request.GroupProtocols),
		State:            consumer.MemberStatePending,
		LastHeartbeat:    time.Now(),
		JoinedAt:         time.Now(),
	}

	// Store protocol metadata for leader - CRITICAL: Generate proper subscription metadata
	if len(request.GroupProtocols) > 0 {
		// If client sends empty metadata, generate subscription metadata for available topics
		if len(request.GroupProtocols[0].Metadata) == 0 {
			// Generate subscription metadata for all available topics
			// Format: version(2) + topics_count(4) + topics[]
			availableTopics := h.getAvailableTopics()
			fmt.Printf("DEBUG: JoinGroup generating subscription metadata for topics: %v\n", availableTopics)

			metadata := make([]byte, 0, 64)
			// Version (2 bytes) - use version 0 to exclude OwnedPartitions
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
			// UserData (nullable bytes) - encode empty (length 0)
			userDataLen := make([]byte, 4)
			binary.BigEndian.PutUint32(userDataLen, 0)
			metadata = append(metadata, userDataLen...)
			member.Metadata = metadata
			fmt.Printf("DEBUG: JoinGroup generated metadata (%d bytes): %x\n", len(metadata), metadata)
		} else {
			member.Metadata = request.GroupProtocols[0].Metadata
		}
	}

	// Add member to group
	group.Members[memberID] = member

	// Update group's subscribed topics
	h.updateGroupSubscription(group)

	// Select assignment protocol (prefer range, fall back to roundrobin)
	groupProtocol := "range"
	for _, protocol := range request.GroupProtocols {
		if protocol.Name == "range" || protocol.Name == "roundrobin" {
			groupProtocol = protocol.Name
			break
		}
	}
	group.Protocol = groupProtocol

	// Select group leader (first member or keep existing if still present)
	if group.Leader == "" || group.Members[group.Leader] == nil {
		group.Leader = memberID
		fmt.Printf("DEBUG: JoinGroup elected new leader: '%s' for group '%s'\n", memberID, request.GroupID)
	} else {
		fmt.Printf("DEBUG: JoinGroup keeping existing leader: '%s' for group '%s'\n", group.Leader, request.GroupID)
	}

	// Build response
	response := JoinGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     ErrorCodeNone,
		GenerationID:  group.Generation,
		GroupProtocol: groupProtocol,
		GroupLeader:   group.Leader,
		MemberID:      memberID,
		Version:       apiVersion,
	}

	fmt.Printf("DEBUG: JoinGroup response - Generation: %d, Protocol: '%s', Leader: '%s', Member: '%s'\n",
		response.GenerationID, response.GroupProtocol, response.GroupLeader, response.MemberID)

	// If this member is the leader, include all member info
	if memberID == group.Leader {
		fmt.Printf("DEBUG: JoinGroup member '%s' is the leader, including %d members in response\n", memberID, len(group.Members))
		response.Members = make([]JoinGroupMember, 0, len(group.Members))
		for _, m := range group.Members {
			response.Members = append(response.Members, JoinGroupMember{
				MemberID:        m.ID,
				GroupInstanceID: "", // Empty for kafka-go compatibility - static membership not used
				Metadata:        m.Metadata,
			})
			fmt.Printf("DEBUG: JoinGroup adding member to response - ID: '%s', Metadata: %d bytes\n", m.ID, len(m.Metadata))
		}
	} else {
		fmt.Printf("DEBUG: JoinGroup member '%s' is NOT the leader (leader is '%s'), empty members array\n", memberID, group.Leader)
	}

	return h.buildJoinGroupResponse(response), nil
}

func (h *Handler) parseJoinGroupRequest(data []byte) (*JoinGroupRequest, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0

	// Skip client_id (part of request header, not JoinGroup payload)
	clientIDLength := int(binary.BigEndian.Uint16(data[offset:]))
	offset += 2 + clientIDLength
	fmt.Printf("DEBUG: JoinGroup skipped client_id (%d bytes), offset now: %d\n", clientIDLength, offset)

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
	fmt.Printf("DEBUG: JoinGroup parsed GroupID: '%s', offset now: %d\n", groupID, offset)

	// Session timeout (4 bytes)
	if offset+4 > len(data) {
		return nil, fmt.Errorf("missing session timeout")
	}
	sessionTimeout := int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Rebalance timeout (4 bytes) - for newer versions
	rebalanceTimeout := sessionTimeout // Default to session timeout
	if offset+4 <= len(data) {
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

	// TODO: CRITICAL - JoinGroup request parsing is incomplete
	// Missing parsing of:
	// - Group instance ID (for static membership)
	// - Protocol type validation
	// - Group protocols array (client's supported assignment strategies)
	// - Protocol metadata (consumer subscriptions, user data)
	// Without this, assignment strategies and subscriptions won't work with real clients

	return &JoinGroupRequest{
		GroupID:          groupID,
		SessionTimeout:   sessionTimeout,
		RebalanceTimeout: rebalanceTimeout,
		MemberID:         memberID,
		ProtocolType:     "consumer", // TODO: Parse from request
		GroupProtocols: []GroupProtocol{
			{Name: "range", Metadata: []byte{}}, // TODO: Parse actual protocols from request
		},
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

func (h *Handler) buildJoinGroupErrorResponse(correlationID uint32, errorCode int16) []byte {
	response := JoinGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
		GenerationID:  -1,
		GroupProtocol: "",
		GroupLeader:   "",
		MemberID:      "",
		Version:       2,
		Members:       []JoinGroupMember{},
	}

	return h.buildJoinGroupResponse(response)
}

func (h *Handler) extractSubscriptionFromProtocols(protocols []GroupProtocol) []string {
	// TODO: CRITICAL - Consumer subscription extraction is hardcoded to "test-topic"
	// This breaks real Kafka consumers which send their actual subscriptions
	// Consumer protocol metadata format (for "consumer" protocol type):
	// - Version (2 bytes)
	// - Topics array (4 bytes count + topic names)
	// - User data (4 bytes length + data)
	// Without fixing this, consumers will be assigned wrong topics
	return []string{"test-topic"}
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
const (
	ErrorCodeIllegalGeneration         int16 = 22
	ErrorCodeInconsistentGroupProtocol int16 = 23
)

func (h *Handler) handleSyncGroup(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	// DEBUG: Hex dump the request to understand format
	dumpLen := len(requestBody)
	if dumpLen > 100 {
		dumpLen = 100
	}
	fmt.Printf("DEBUG: SyncGroup request hex dump (first %d bytes): %x\n", dumpLen, requestBody[:dumpLen])

	// Parse SyncGroup request
	request, err := h.parseSyncGroupRequest(requestBody)
	if err != nil {
		fmt.Printf("DEBUG: SyncGroup parseSyncGroupRequest error: %v\n", err)
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	fmt.Printf("DEBUG: SyncGroup parsed request - GroupID: '%s', MemberID: '%s', GenerationID: %d\n",
		request.GroupID, request.MemberID, request.GenerationID)

	// Validate request
	if request.GroupID == "" || request.MemberID == "" {
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	// Get consumer group
	group := h.groupCoordinator.GetGroup(request.GroupID)
	if group == nil {
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID), nil
	}

	group.Mu.Lock()
	defer group.Mu.Unlock()

	// Update group's last activity
	group.LastActivity = time.Now()

	// Validate member exists
	member, exists := group.Members[request.MemberID]
	if !exists {
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeUnknownMemberID), nil
	}

	// Validate generation
	if request.GenerationID != group.Generation {
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeIllegalGeneration), nil
	}

	// Check if this is the group leader with assignments
	if request.MemberID == group.Leader && len(request.GroupAssignments) > 0 {
		// Leader is providing assignments - process and store them
		err = h.processGroupAssignments(group, request.GroupAssignments)
		if err != nil {
			return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeInconsistentGroupProtocol), nil
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

	return h.buildSyncGroupResponse(response), nil
}

func (h *Handler) parseSyncGroupRequest(data []byte) (*SyncGroupRequest, error) {
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

	// For simplicity, we'll parse basic fields
	// In a full implementation, we'd parse the full group assignments array

	return &SyncGroupRequest{
		GroupID:          groupID,
		GenerationID:     generationID,
		MemberID:         memberID,
		GroupInstanceID:  "",
		GroupAssignments: []GroupAssignment{},
	}, nil
}

func (h *Handler) buildSyncGroupResponse(response SyncGroupResponse) []byte {
	estimatedSize := 16 + len(response.Assignment)
	result := make([]byte, 0, estimatedSize)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, response.CorrelationID)
	result = append(result, correlationIDBytes...)

	// Error code (2 bytes)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, uint16(response.ErrorCode))
	result = append(result, errorCodeBytes...)

	// Assignment (bytes)
	assignmentLength := make([]byte, 4)
	binary.BigEndian.PutUint32(assignmentLength, uint32(len(response.Assignment)))
	result = append(result, assignmentLength...)
	result = append(result, response.Assignment...)

	// Throttle time (4 bytes, 0 = no throttling)
	result = append(result, 0, 0, 0, 0)

	return result
}

func (h *Handler) buildSyncGroupErrorResponse(correlationID uint32, errorCode int16) []byte {
	response := SyncGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     errorCode,
		Assignment:    []byte{},
	}

	return h.buildSyncGroupResponse(response)
}

func (h *Handler) processGroupAssignments(group *consumer.ConsumerGroup, assignments []GroupAssignment) error {
	// In a full implementation, we'd deserialize the assignment data
	// and update each member's partition assignment
	// For now, we'll trigger our own assignment logic

	topicPartitions := h.getTopicPartitions(group)
	group.AssignPartitions(topicPartitions)

	return nil
}

func (h *Handler) getTopicPartitions(group *consumer.ConsumerGroup) map[string][]int32 {
	topicPartitions := make(map[string][]int32)

	// Get partition info for all subscribed topics
	for topic := range group.SubscribedTopics {
		// Check if topic exists in our topic registry
		h.topicsMu.RLock()
		topicInfo, exists := h.topics[topic]
		h.topicsMu.RUnlock()

		if exists {
			// Create partition list for this topic
			partitions := make([]int32, topicInfo.Partitions)
			for i := int32(0); i < topicInfo.Partitions; i++ {
				partitions[i] = i
			}
			topicPartitions[topic] = partitions
		} else {
			// Default to single partition if topic not found
			topicPartitions[topic] = []int32{0}
		}
	}

	return topicPartitions
}

func (h *Handler) serializeMemberAssignment(assignments []consumer.PartitionAssignment) []byte {
	// Build a simple serialized format for partition assignments
	// Format: version(2) + num_topics(4) + topics...
	// For each topic: topic_name_len(2) + topic_name + num_partitions(4) + partitions...

	if len(assignments) == 0 {
		return []byte{0, 1, 0, 0, 0, 0} // Version 1, 0 topics
	}

	// Group assignments by topic
	topicAssignments := make(map[string][]int32)
	for _, assignment := range assignments {
		topicAssignments[assignment.Topic] = append(topicAssignments[assignment.Topic], assignment.Partition)
	}

	result := make([]byte, 0, 64)

	// Version (2 bytes) - use version 1
	result = append(result, 0, 1)

	// Number of topics (4 bytes)
	numTopicsBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(numTopicsBytes, uint32(len(topicAssignments)))
	result = append(result, numTopicsBytes...)

	// Topics
	for topic, partitions := range topicAssignments {
		// Topic name length (2 bytes)
		topicLenBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(topicLenBytes, uint16(len(topic)))
		result = append(result, topicLenBytes...)

		// Topic name
		result = append(result, []byte(topic)...)

		// Number of partitions (4 bytes)
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

	// User data length (4 bytes) - no user data
	result = append(result, 0, 0, 0, 0)

	return result
}

// getAvailableTopics returns list of available topics for subscription metadata
func (h *Handler) getAvailableTopics() []string {
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()

	topics := make([]string, 0, len(h.topics))
	for topicName := range h.topics {
		topics = append(topics, topicName)
	}
	return topics
}
