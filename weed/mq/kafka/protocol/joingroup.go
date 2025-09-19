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
	// DEBUG: Hex dump the request to understand format
	dumpLen := len(requestBody)
	if dumpLen > 100 {
		dumpLen = 100
	}
	fmt.Printf("DEBUG: JoinGroup request hex dump (first %d bytes): %x\n", dumpLen, requestBody[:dumpLen])

	// Parse JoinGroup request
	request, err := h.parseJoinGroupRequest(requestBody, apiVersion)
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

	fmt.Printf("DEBUG: JoinGroup before - group='%s' gen=%d state=%v members=%d leader='%s'\n",
		group.ID, group.Generation, group.State, len(group.Members), group.Leader)

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
			fmt.Printf("DEBUG: JoinGroup found existing static member ID '%s' for instance '%s'\n", memberID, request.GroupInstanceID)
		} else {
			// New static member
			memberID = h.groupCoordinator.GenerateMemberID(request.GroupInstanceID, "static")
			isNewMember = true
			fmt.Printf("DEBUG: JoinGroup generated new static member ID '%s' for instance '%s'\n", memberID, request.GroupInstanceID)
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
	}

	// Check group state
	fmt.Printf("DEBUG: JoinGroup current group state: %s, generation: %d (members=%d)\n", group.State, group.Generation, len(group.Members))
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

	// Extract client host from connection context
	clientHost := ExtractClientHost(h.connContext)
	fmt.Printf("DEBUG: JoinGroup extracted client host: %s\n", clientHost)

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

	// Store protocol metadata for leader
	if len(request.GroupProtocols) > 0 {
		if len(request.GroupProtocols[0].Metadata) == 0 {
			// Generate subscription metadata for available topics
			availableTopics := h.getAvailableTopics()
			fmt.Printf("DEBUG: JoinGroup generating subscription metadata for topics: %v\n", availableTopics)

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
			fmt.Printf("DEBUG: JoinGroup generated metadata (%d bytes): %x\n", len(metadata), metadata)
		} else {
			member.Metadata = request.GroupProtocols[0].Metadata
		}
	}

	// Add member to group
	group.Members[memberID] = member

	// Register static member if applicable
	if member.GroupInstanceID != nil && *member.GroupInstanceID != "" {
		h.groupCoordinator.RegisterStaticMemberLocked(group, member)
		fmt.Printf("DEBUG: JoinGroup registered static member '%s' with instance ID '%s'\n", memberID, *member.GroupInstanceID)
	}

	// Update group's subscribed topics
	h.updateGroupSubscription(group)

	// Select assignment protocol using enhanced selection logic
	existingProtocols := make([]string, 0)
	for _ = range group.Members {
		// Collect protocols from existing members (simplified - in real implementation
		// we'd track each member's supported protocols)
		existingProtocols = append(existingProtocols, "range") // placeholder
	}

	groupProtocol := SelectBestProtocol(request.GroupProtocols, existingProtocols)
	group.Protocol = groupProtocol
	fmt.Printf("DEBUG: JoinGroup selected protocol: %s (from %d client protocols)\n",
		groupProtocol, len(request.GroupProtocols))

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
		// TESTING: Try empty members array to see if that fixes the size issue
		response.Members = make([]JoinGroupMember, 0)
	} else {
	}

	return h.buildJoinGroupResponse(response), nil
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

	fmt.Printf("DEBUG: JoinGroup - GroupID: %s, SessionTimeout: %d, RebalanceTimeout: %d, MemberID: %s, ProtocolType: %s, ProtocolsCount: %d\n",
		groupID, sessionTimeout, rebalanceTimeout, memberID, protocolType, protocolsCount)

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

		fmt.Printf("DEBUG: JoinGroup - Protocol: %s, MetadataLength: %d\n", protocolName, metadataLength)
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

// buildMinimalJoinGroupResponse creates a minimal hardcoded response for testing
func (h *Handler) buildMinimalJoinGroupResponse(correlationID uint32, apiVersion uint16) []byte {
	// Create the absolute minimal JoinGroup response that should work with kafka-go
	response := make([]byte, 0, 64)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// Throttle time (4 bytes) - v2+ only
	if apiVersion >= 2 {
		response = append(response, 0, 0, 0, 0) // No throttling
	}

	// Error code (2 bytes) - 0 = success
	response = append(response, 0, 0)

	// Generation ID (4 bytes) - use 1
	response = append(response, 0, 0, 0, 1)

	// Group protocol (STRING) - "range"
	response = append(response, 0, 5) // length
	response = append(response, []byte("range")...)

	// Group leader (STRING) - "test-member"
	response = append(response, 0, 11) // length
	response = append(response, []byte("test-member")...)

	// Member ID (STRING) - "test-member" (same as leader)
	response = append(response, 0, 11) // length
	response = append(response, []byte("test-member")...)

	// Members array (4 bytes count + members)
	response = append(response, 0, 0, 0, 1) // 1 member

	// Member 0:
	// Member ID (STRING) - "test-member"
	response = append(response, 0, 11) // length
	response = append(response, []byte("test-member")...)

	// Member metadata (BYTES) - empty
	response = append(response, 0, 0, 0, 0) // 0 bytes

	fmt.Printf("DEBUG: JoinGroup minimal response (%d bytes): %x\n", len(response), response)
	return response
}

// extractSubscriptionFromProtocols - legacy method for backward compatibility
func (h *Handler) extractSubscriptionFromProtocols(protocols []GroupProtocol) []string {
	return h.extractSubscriptionFromProtocolsEnhanced(protocols)
}

// extractSubscriptionFromProtocolsEnhanced uses improved metadata parsing with better error handling
func (h *Handler) extractSubscriptionFromProtocolsEnhanced(protocols []GroupProtocol) []string {
	// Analyze protocol metadata for debugging
	debugInfo := AnalyzeProtocolMetadata(protocols)
	for _, info := range debugInfo {
		if info.ParsedOK {
			fmt.Printf("DEBUG: Protocol %s parsed successfully: version=%d, topics=%v\n",
				info.Strategy, info.Version, info.Topics)
		} else {
			fmt.Printf("DEBUG: Protocol %s parse failed: %s\n", info.Strategy, info.ParseError)
		}
	}

	// Extract topics using enhanced parsing
	topics := ExtractTopicsFromMetadata(protocols, h.getAvailableTopics())

	fmt.Printf("DEBUG: Enhanced subscription extraction result: %v\n", topics)
	return topics
}

func (h *Handler) parseConsumerProtocolMetadata(metadata []byte) []string {
	if len(metadata) < 6 { // version(2) + topics_count(4)
		return nil
	}

	offset := 0

	// Parse version (2 bytes)
	version := binary.BigEndian.Uint16(metadata[offset : offset+2])
	offset += 2

	// Parse topics array
	if len(metadata) < offset+4 {
		return nil
	}
	topicsCount := binary.BigEndian.Uint32(metadata[offset : offset+4])
	offset += 4

	fmt.Printf("DEBUG: Consumer protocol metadata - Version: %d, TopicsCount: %d\n", version, topicsCount)

	topics := make([]string, 0, topicsCount)

	for i := uint32(0); i < topicsCount && offset < len(metadata); i++ {
		// Parse topic name
		if len(metadata) < offset+2 {
			break
		}
		topicNameLength := binary.BigEndian.Uint16(metadata[offset : offset+2])
		offset += 2

		if len(metadata) < offset+int(topicNameLength) {
			break
		}
		topicName := string(metadata[offset : offset+int(topicNameLength)])
		offset += int(topicNameLength)

		topics = append(topics, topicName)
		fmt.Printf("DEBUG: Consumer subscribed to topic: %s\n", topicName)
	}

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
	// DEBUG: Hex dump the request to understand format
	dumpLen := len(requestBody)
	if dumpLen > 100 {
		dumpLen = 100
	}
	fmt.Printf("DEBUG: SyncGroup request hex dump (first %d bytes): %x\n", dumpLen, requestBody[:dumpLen])

	// Parse SyncGroup request
	request, err := h.parseSyncGroupRequest(requestBody, apiVersion)
	if err != nil {
		fmt.Printf("DEBUG: SyncGroup parseSyncGroupRequest error: %v\n", err)
		return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeInvalidGroupID, apiVersion), nil
	}

	fmt.Printf("DEBUG: SyncGroup parsed request - GroupID: '%s', MemberID: '%s', GenerationID: %d\n",
		request.GroupID, request.MemberID, request.GenerationID)

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

	return h.buildSyncGroupResponse(response, apiVersion), nil
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

	// For simplicity, we'll parse basic fields
	// In a full implementation, we'd parse the full group assignments array

	return &SyncGroupRequest{
		GroupID:          groupID,
		GenerationID:     generationID,
		MemberID:         memberID,
		GroupInstanceID:  groupInstanceID,
		GroupAssignments: []GroupAssignment{},
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

	fmt.Printf("DEBUG: Generated assignment bytes (%d): %x\n", len(result), result)
	return result
}

// getAvailableTopics returns list of available topics for subscription metadata
func (h *Handler) getAvailableTopics() []string {
	return h.seaweedMQHandler.ListTopics()
}
