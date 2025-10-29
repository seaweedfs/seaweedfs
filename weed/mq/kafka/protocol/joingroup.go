package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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
	CorrelationID  uint32
	ThrottleTimeMs int32 // versions 2+
	ErrorCode      int16
	GenerationID   int32
	ProtocolName   string // NOT nullable in v6, nullable in v7+
	Leader         string // NOT nullable
	MemberID       string
	Version        uint16
	Members        []JoinGroupMember // Only populated for group leader
}

// JoinGroupMember represents member info sent to group leader
type JoinGroupMember struct {
	MemberID        string
	GroupInstanceID string
	Metadata        []byte
}

// Error codes for JoinGroup are imported from errors.go

func (h *Handler) handleJoinGroup(connContext *ConnectionContext, correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
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

	// Use the actual ClientID from Kafka protocol header for unique member ID generation
	clientKey := connContext.ClientID
	if clientKey == "" {
		// Fallback to deterministic key if ClientID not available
		clientKey = fmt.Sprintf("%s-%d-%s", request.GroupID, request.SessionTimeout, request.ProtocolType)
		glog.Warningf("[JoinGroup] No ClientID in ConnectionContext for group %s, using fallback: %s", request.GroupID, clientKey)
	} else {
		glog.V(1).Infof("[JoinGroup] Using ClientID from ConnectionContext for group %s: %s", request.GroupID, clientKey)
	}

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
	clientHost := ExtractClientHost(connContext)

	// Create or update member with enhanced metadata parsing
	var groupInstanceID *string
	if request.GroupInstanceID != "" {
		groupInstanceID = &request.GroupInstanceID
	}

	member := &consumer.GroupMember{
		ID:               memberID,
		ClientID:         clientKey,  // Use actual Kafka ClientID for unique member identification
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

	// Store consumer group and member ID in connection context for use in fetch requests
	connContext.ConsumerGroup = request.GroupID
	connContext.MemberID = memberID

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

	// Ensure we have a valid protocol - fallback to "range" if empty
	if groupProtocol == "" {
		groupProtocol = consumer.ProtocolNameRange
	}

	// If a protocol is already selected for the group, reject joins that do not support it.
	if len(existingProtocols) > 0 && (groupProtocol == "" || groupProtocol != group.Protocol) {
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
		CorrelationID:  correlationID,
		ThrottleTimeMs: 0,
		ErrorCode:      ErrorCodeNone,
		GenerationID:   group.Generation,
		ProtocolName:   groupProtocol,
		Leader:         group.Leader,
		MemberID:       memberID,
		Version:        apiVersion,
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
	isFlexible := IsFlexibleVersion(11, apiVersion)

	// For flexible versions, skip top-level tagged fields first
	if isFlexible {
		// Skip top-level tagged fields (they come before the actual request fields)
		_, consumed, err := DecodeTaggedFields(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("JoinGroup v%d: decode top-level tagged fields: %w", apiVersion, err)
		}
		offset += consumed
	}

	// GroupID (string or compact string) - FIRST field in request
	var groupID string
	if isFlexible {
		// Flexible protocol uses compact strings
		endIdx := offset + 20
		if endIdx > len(data) {
			endIdx = len(data)
		}
		groupIDBytes, consumed := parseCompactString(data[offset:])
		if consumed == 0 {
			return nil, fmt.Errorf("invalid group ID compact string")
		}
		if groupIDBytes != nil {
			groupID = string(groupIDBytes)
		}
		offset += consumed
	} else {
		// Non-flexible protocol uses regular strings
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

	// MemberID (string or compact string)
	var memberID string
	if isFlexible {
		// Flexible protocol uses compact strings
		memberIDBytes, consumed := parseCompactString(data[offset:])
		if consumed == 0 {
			return nil, fmt.Errorf("invalid member ID compact string")
		}
		if memberIDBytes != nil {
			memberID = string(memberIDBytes)
		}
		offset += consumed
	} else {
		// Non-flexible protocol uses regular strings
		if offset+2 > len(data) {
			return nil, fmt.Errorf("missing member ID length")
		}
		memberIDLength := int(binary.BigEndian.Uint16(data[offset:]))
		offset += 2
		if memberIDLength > 0 {
			if offset+memberIDLength > len(data) {
				return nil, fmt.Errorf("invalid member ID length")
			}
			memberID = string(data[offset : offset+memberIDLength])
			offset += memberIDLength
		}
	}

	// Parse Group Instance ID (nullable string) - for JoinGroup v5+
	var groupInstanceID string
	if apiVersion >= 5 {
		if isFlexible {
			// FLEXIBLE V6+ FIX: GroupInstanceID is a compact nullable string
			groupInstanceIDBytes, consumed := parseCompactString(data[offset:])
			if consumed == 0 && len(data) > offset {
				// Check if it's a null compact string (0x00)
				if data[offset] == 0x00 {
					groupInstanceID = "" // null
					offset += 1
				} else {
					return nil, fmt.Errorf("JoinGroup v%d: invalid group instance ID compact string", apiVersion)
				}
			} else {
				if groupInstanceIDBytes != nil {
					groupInstanceID = string(groupInstanceIDBytes)
				}
				offset += consumed
			}
		} else {
			// Non-flexible v5: regular nullable string
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
	}

	// Parse Protocol Type
	var protocolType string
	if isFlexible {
		// FLEXIBLE V6+ FIX: ProtocolType is a compact string, not regular string
		endIdx := offset + 10
		if endIdx > len(data) {
			endIdx = len(data)
		}
		protocolTypeBytes, consumed := parseCompactString(data[offset:])
		if consumed == 0 {
			return nil, fmt.Errorf("JoinGroup v%d: invalid protocol type compact string", apiVersion)
		}
		if protocolTypeBytes != nil {
			protocolType = string(protocolTypeBytes)
		}
		offset += consumed
	} else {
		// Non-flexible parsing (v0-v5)
		if len(data) < offset+2 {
			return nil, fmt.Errorf("JoinGroup request missing protocol type")
		}
		protocolTypeLength := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		if len(data) < offset+int(protocolTypeLength) {
			return nil, fmt.Errorf("JoinGroup request protocol type too short")
		}
		protocolType = string(data[offset : offset+int(protocolTypeLength)])
		offset += int(protocolTypeLength)
	}

	// Parse Group Protocols array
	var protocolsCount uint32
	if isFlexible {
		// FLEXIBLE V6+ FIX: GroupProtocols is a compact array, not regular array
		compactLength, consumed, err := DecodeCompactArrayLength(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("JoinGroup v%d: invalid group protocols compact array: %w", apiVersion, err)
		}
		protocolsCount = compactLength
		offset += consumed
	} else {
		// Non-flexible parsing (v0-v5)
		if len(data) < offset+4 {
			return nil, fmt.Errorf("JoinGroup request missing group protocols")
		}
		protocolsCount = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
	}

	protocols := make([]GroupProtocol, 0, protocolsCount)

	for i := uint32(0); i < protocolsCount && offset < len(data); i++ {
		// Parse protocol name
		var protocolName string
		if isFlexible {
			// FLEXIBLE V6+ FIX: Protocol name is a compact string
			endIdx := offset + 10
			if endIdx > len(data) {
				endIdx = len(data)
			}
			protocolNameBytes, consumed := parseCompactString(data[offset:])
			if consumed == 0 {
				return nil, fmt.Errorf("JoinGroup v%d: invalid protocol name compact string", apiVersion)
			}
			if protocolNameBytes != nil {
				protocolName = string(protocolNameBytes)
			}
			offset += consumed
		} else {
			// Non-flexible parsing
			if len(data) < offset+2 {
				break
			}
			protocolNameLength := binary.BigEndian.Uint16(data[offset : offset+2])
			offset += 2

			if len(data) < offset+int(protocolNameLength) {
				break
			}
			protocolName = string(data[offset : offset+int(protocolNameLength)])
			offset += int(protocolNameLength)
		}

		// Parse protocol metadata
		var metadata []byte
		if isFlexible {
			// FLEXIBLE V6+ FIX: Protocol metadata is compact bytes
			metadataLength, consumed, err := DecodeCompactArrayLength(data[offset:])
			if err != nil {
				return nil, fmt.Errorf("JoinGroup v%d: invalid protocol metadata compact bytes: %w", apiVersion, err)
			}
			offset += consumed

			if metadataLength > 0 && len(data) >= offset+int(metadataLength) {
				metadata = make([]byte, metadataLength)
				copy(metadata, data[offset:offset+int(metadataLength)])
				offset += int(metadataLength)
			}
		} else {
			// Non-flexible parsing
			if len(data) < offset+4 {
				break
			}
			metadataLength := binary.BigEndian.Uint32(data[offset : offset+4])
			offset += 4

			if metadataLength > 0 && len(data) >= offset+int(metadataLength) {
				metadata = make([]byte, metadataLength)
				copy(metadata, data[offset:offset+int(metadataLength)])
				offset += int(metadataLength)
			}
		}

		// Parse per-protocol tagged fields (v6+)
		if isFlexible {
			_, consumed, err := DecodeTaggedFields(data[offset:])
			if err != nil {
				// Don't fail - some clients might not send tagged fields
			} else {
				offset += consumed
			}
		}

		protocols = append(protocols, GroupProtocol{
			Name:     protocolName,
			Metadata: metadata,
		})

	}

	// Parse request-level tagged fields (v6+)
	if isFlexible {
		if offset < len(data) {
			_, _, err := DecodeTaggedFields(data[offset:])
			if err != nil {
				// Don't fail - some clients might not send tagged fields
			}
		}
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
	// Flexible response for v6+
	if IsFlexibleVersion(11, response.Version) {
		out := make([]byte, 0, 256)

		// NOTE: Correlation ID and header-level tagged fields are handled by writeResponseWithHeader
		// Do NOT include them in the response body

		// throttle_time_ms (int32) - versions 2+
		if response.Version >= 2 {
			ttms := make([]byte, 4)
			binary.BigEndian.PutUint32(ttms, uint32(response.ThrottleTimeMs))
			out = append(out, ttms...)
		}

		// error_code (int16)
		eb := make([]byte, 2)
		binary.BigEndian.PutUint16(eb, uint16(response.ErrorCode))
		out = append(out, eb...)

		// generation_id (int32)
		gb := make([]byte, 4)
		binary.BigEndian.PutUint32(gb, uint32(response.GenerationID))
		out = append(out, gb...)

		// ProtocolType (v7+ nullable compact string) - NOT in v6!
		if response.Version >= 7 {
			pt := "consumer"
			out = append(out, FlexibleNullableString(&pt)...)
		}

		// ProtocolName (compact string in v6, nullable compact string in v7+)
		if response.Version >= 7 {
			// nullable compact string in v7+
			if response.ProtocolName == "" {
				out = append(out, 0) // null
			} else {
				out = append(out, FlexibleString(response.ProtocolName)...)
			}
		} else {
			// NON-nullable compact string in v6 - must not be empty!
			if response.ProtocolName == "" {
				response.ProtocolName = consumer.ProtocolNameRange // fallback to default
			}
			out = append(out, FlexibleString(response.ProtocolName)...)
		}

		// leader (compact string) - NOT nullable
		if response.Leader == "" {
			response.Leader = "unknown" // fallback for error cases
		}
		out = append(out, FlexibleString(response.Leader)...)

		// SkipAssignment (bool) v9+
		if response.Version >= 9 {
			out = append(out, 0) // false
		}

		// member_id (compact string)
		out = append(out, FlexibleString(response.MemberID)...)

		// members (compact array)
		// Compact arrays use length+1 encoding (0 = null, 1 = empty, n+1 = array of length n)
		out = append(out, EncodeUvarint(uint32(len(response.Members)+1))...)
		for _, m := range response.Members {
			// member_id (compact string)
			out = append(out, FlexibleString(m.MemberID)...)
			// group_instance_id (compact nullable string)
			if m.GroupInstanceID == "" {
				out = append(out, 0)
			} else {
				out = append(out, FlexibleString(m.GroupInstanceID)...)
			}
			// metadata (compact bytes)
			// Compact bytes use length+1 encoding (0 = null, 1 = empty, n+1 = bytes of length n)
			out = append(out, EncodeUvarint(uint32(len(m.Metadata)+1))...)
			out = append(out, m.Metadata...)
			// member tagged fields (empty)
			out = append(out, 0)
		}

		// top-level tagged fields (empty)
		out = append(out, 0)

		return out
	}

	// Legacy (non-flexible) response path
	// Estimate response size
	estimatedSize := 0
	// CorrelationID(4) + (optional throttle 4) + error_code(2) + generation_id(4)
	if response.Version >= 2 {
		estimatedSize = 4 + 4 + 2 + 4
	} else {
		estimatedSize = 4 + 2 + 4
	}
	estimatedSize += 2 + len(response.ProtocolName) // protocol string
	estimatedSize += 2 + len(response.Leader)       // leader string
	estimatedSize += 2 + len(response.MemberID)     // member id string
	estimatedSize += 4                              // members array count
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

	// NOTE: Correlation ID is handled by writeResponseWithCorrelationID
	// Do NOT include it in the response body

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
	binary.BigEndian.PutUint16(protocolLength, uint16(len(response.ProtocolName)))
	result = append(result, protocolLength...)
	result = append(result, []byte(response.ProtocolName)...)

	// Group leader (string)
	leaderLength := make([]byte, 2)
	binary.BigEndian.PutUint16(leaderLength, uint16(len(response.Leader)))
	result = append(result, leaderLength...)
	result = append(result, []byte(response.Leader)...)

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
		CorrelationID:  correlationID,
		ThrottleTimeMs: 0,
		ErrorCode:      errorCode,
		GenerationID:   -1,
		ProtocolName:   consumer.ProtocolNameRange, // Use "range" as default protocol instead of empty string
		Leader:         "unknown",                  // Use "unknown" instead of empty string for non-nullable field
		MemberID:       "unknown",                  // Use "unknown" instead of empty string for non-nullable field
		Version:        apiVersion,
		Members:        []JoinGroupMember{},
	}

	return h.buildJoinGroupResponse(response)
}

// extractSubscriptionFromProtocolsEnhanced uses improved metadata parsing with better error handling
func (h *Handler) extractSubscriptionFromProtocolsEnhanced(protocols []GroupProtocol) []string {
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
	glog.V(2).Infof("[SYNCGROUP] Member=%s Leader=%s GroupState=%s HasAssignments=%v MemberCount=%d Gen=%d",
		request.MemberID, group.Leader, group.State, len(request.GroupAssignments) > 0, len(group.Members), request.GenerationID)

	if request.MemberID == group.Leader && len(request.GroupAssignments) > 0 {
		// Leader is providing assignments - process and store them
		glog.V(2).Infof("[SYNCGROUP] Leader %s providing client-side assignments for group %s (%d assignments)",
			request.MemberID, request.GroupID, len(request.GroupAssignments))
		err = h.processGroupAssignments(group, request.GroupAssignments)
		if err != nil {
			glog.Errorf("[SYNCGROUP] ERROR processing leader assignments: %v", err)
			return h.buildSyncGroupErrorResponse(correlationID, ErrorCodeInconsistentGroupProtocol, apiVersion), nil
		}

		// Move group to stable state
		group.State = consumer.GroupStateStable

		// Mark all members as stable
		for _, m := range group.Members {
			m.State = consumer.MemberStateStable
		}
		glog.V(2).Infof("[SYNCGROUP] Leader assignments processed successfully, group now STABLE")
	} else if request.MemberID != group.Leader && len(request.GroupAssignments) == 0 {
		// Non-leader member requesting its assignment
		// CRITICAL FIX: Non-leader members should ALWAYS wait for leader's client-side assignments
		// This is the correct behavior for Sarama and other client-side assignment protocols
		glog.V(3).Infof("[SYNCGROUP] Non-leader %s waiting for/retrieving assignment in group %s (state=%s)",
			request.MemberID, request.GroupID, group.State)
		// Assignment will be retrieved from member.Assignment below
	} else {
		// Trigger partition assignment using built-in strategy (server-side assignment)
		// This should only happen for server-side assignment protocols (not Sarama's client-side)
		glog.Warningf("[SYNCGROUP] Using server-side assignment for group %s (Leader=%s State=%s) - this should not happen with Sarama!",
			request.GroupID, group.Leader, group.State)
		topicPartitions := h.getTopicPartitions(group)
		group.AssignPartitions(topicPartitions)

		group.State = consumer.GroupStateStable
		for _, m := range group.Members {
			m.State = consumer.MemberStateStable
		}
	}

	// Get assignment for this member
	// SCHEMA REGISTRY COMPATIBILITY: Check if this is a Schema Registry client
	var assignment []byte
	if request.GroupID == "schema-registry" {
		// Schema Registry expects JSON format assignment
		assignment = h.serializeSchemaRegistryAssignment(group, member.Assignment)
	} else {
		// Standard Kafka binary assignment format
		assignment = h.serializeMemberAssignment(member.Assignment)
	}

	// Log member assignment details
	glog.V(3).Infof("[SYNCGROUP] Member %s in group %s assigned %d partitions: %v",
		request.MemberID, request.GroupID, len(member.Assignment), member.Assignment)

	// Build response
	response := SyncGroupResponse{
		CorrelationID: correlationID,
		ErrorCode:     ErrorCodeNone,
		Assignment:    assignment,
	}

	assignmentPreview := assignment
	if len(assignmentPreview) > 100 {
		assignmentPreview = assignment[:100]
	}

	resp := h.buildSyncGroupResponse(response, apiVersion)
	return resp, nil
}

func (h *Handler) parseSyncGroupRequest(data []byte, apiVersion uint16) (*SyncGroupRequest, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("request too short")
	}

	offset := 0
	isFlexible := IsFlexibleVersion(14, apiVersion) // SyncGroup API key = 14

	// ADMINCLIENT COMPATIBILITY FIX: Parse top-level tagged fields at the beginning for flexible versions
	if isFlexible {
		_, consumed, err := DecodeTaggedFields(data[offset:])
		if err == nil {
			offset += consumed
		} else {
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

	// Parse GroupInstanceID (nullable string) - for SyncGroup v3+
	var groupInstanceID string
	if apiVersion >= 3 {
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
			// Non-flexible v3: regular nullable string
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
	}

	// Parse assignments array if present (leader sends assignments)
	assignments := make([]GroupAssignment, 0)

	if offset < len(data) {
		var assignmentsCount uint32
		if isFlexible {
			// FLEXIBLE V4+ FIX: Assignments is a compact array
			compactLength, consumed, err := DecodeCompactArrayLength(data[offset:])
			if err != nil {
			} else {
				assignmentsCount = compactLength
				offset += consumed
			}
		} else {
			// Non-flexible: regular array with 4-byte length
			if offset+4 <= len(data) {
				assignmentsCount = binary.BigEndian.Uint32(data[offset:])
				offset += 4
			}
		}

		// Basic sanity check to avoid very large allocations
		if assignmentsCount > 0 && assignmentsCount < 10000 {
			for i := uint32(0); i < assignmentsCount && offset < len(data); i++ {
				var mID string
				var assign []byte

				// Parse member_id
				if isFlexible {
					// FLEXIBLE V4+ FIX: member_id is a compact string
					memberIDBytes, consumed := parseCompactString(data[offset:])
					if consumed == 0 {
						break
					}
					if memberIDBytes != nil {
						mID = string(memberIDBytes)
					}
					offset += consumed
				} else {
					// Non-flexible: regular string
					if offset+2 > len(data) {
						break
					}
					memberLen := int(binary.BigEndian.Uint16(data[offset:]))
					offset += 2
					if memberLen < 0 || offset+memberLen > len(data) {
						break
					}
					mID = string(data[offset : offset+memberLen])
					offset += memberLen
				}

				// Parse assignment (bytes)
				if isFlexible {
					// FLEXIBLE V4+ FIX: assignment is compact bytes
					assignLength, consumed, err := DecodeCompactArrayLength(data[offset:])
					if err != nil {
						break
					}
					offset += consumed
					if assignLength > 0 && offset+int(assignLength) <= len(data) {
						assign = make([]byte, assignLength)
						copy(assign, data[offset:offset+int(assignLength)])
						offset += int(assignLength)
					}

					// Flexible format requires tagged fields after each assignment struct
					if offset < len(data) {
						_, taggedConsumed, tagErr := DecodeTaggedFields(data[offset:])
						if tagErr == nil {
							offset += taggedConsumed
						}
					}
				} else {
					// Non-flexible: regular bytes
					if offset+4 > len(data) {
						break
					}
					assignLen := int(binary.BigEndian.Uint32(data[offset:]))
					offset += 4
					if assignLen < 0 || offset+assignLen > len(data) {
						break
					}
					if assignLen > 0 {
						assign = make([]byte, assignLen)
						copy(assign, data[offset:offset+assignLen])
					}
					offset += assignLen
				}

				assignments = append(assignments, GroupAssignment{MemberID: mID, Assignment: assign})
			}
		}
	}

	// Parse request-level tagged fields (v4+)
	if isFlexible {
		if offset < len(data) {
			_, consumed, err := DecodeTaggedFields(data[offset:])
			if err != nil {
			} else {
				offset += consumed
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

	// NOTE: Correlation ID and header-level tagged fields are handled by writeResponseWithHeader
	// Do NOT include them in the response body

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

	// SyncGroup v5 adds protocol_type and protocol_name (compact nullable strings)
	if apiVersion >= 5 {
		// protocol_type = null (varint 0)
		result = append(result, 0x00)
		// protocol_name = null (varint 0)
		result = append(result, 0x00)
	}

	// Assignment - FLEXIBLE V4+ FIX
	if IsFlexibleVersion(14, apiVersion) {
		// FLEXIBLE FORMAT: Assignment as compact bytes
		// Use CompactStringLength for compact bytes (not CompactArrayLength)
		// Compact bytes use the same encoding as compact strings: 0 = null, 1 = empty, n+1 = length n
		assignmentLen := len(response.Assignment)
		if assignmentLen == 0 {
			// Empty compact bytes = length 0, encoded as 0x01 (0 + 1)
			result = append(result, 0x01) // Empty compact bytes
		} else {
			// Non-empty assignment: encode length + data
			// Use CompactStringLength which correctly encodes as length+1
			compactLength := CompactStringLength(assignmentLen)
			result = append(result, compactLength...)
			result = append(result, response.Assignment...)
		}
		// Add response-level tagged fields for flexible format
		result = append(result, 0x00) // Empty tagged fields (varint: 0)
	} else {
		// NON-FLEXIBLE FORMAT: Assignment as regular bytes
		assignmentLength := make([]byte, 4)
		binary.BigEndian.PutUint32(assignmentLength, uint32(len(response.Assignment)))
		result = append(result, assignmentLength...)
		result = append(result, response.Assignment...)
	}

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
	glog.V(2).Infof("[PROCESS_ASSIGNMENTS] Processing %d member assignments from leader", len(assignments))

	// Clear current assignments
	for _, m := range group.Members {
		m.Assignment = nil
	}

	for _, ga := range assignments {
		m, ok := group.Members[ga.MemberID]
		if !ok {
			// Skip unknown members
			glog.V(1).Infof("[PROCESS_ASSIGNMENTS] Skipping unknown member: %s", ga.MemberID)
			continue
		}

		parsed, err := h.parseMemberAssignment(ga.Assignment)
		if err != nil {
			glog.Errorf("[PROCESS_ASSIGNMENTS] Failed to parse assignment for member %s: %v", ga.MemberID, err)
			return err
		}
		m.Assignment = parsed
		glog.V(3).Infof("[PROCESS_ASSIGNMENTS] Member %s assigned %d partitions: %v", ga.MemberID, len(parsed), parsed)
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
		// Get actual partition count from topic info
		topicInfo, exists := h.seaweedMQHandler.GetTopicInfo(topic)
		partitionCount := h.GetDefaultPartitions() // Use configurable default
		if exists && topicInfo != nil {
			partitionCount = topicInfo.Partitions
		}

		// Create partition list: [0, 1, 2, ...]
		partitions := make([]int32, partitionCount)
		for i := int32(0); i < partitionCount; i++ {
			partitions[i] = i
		}
		topicPartitions[topic] = partitions
	}

	return topicPartitions
}

func (h *Handler) serializeSchemaRegistryAssignment(group *consumer.ConsumerGroup, assignments []consumer.PartitionAssignment) []byte {
	// Schema Registry expects a JSON assignment in the format:
	// {"error":0,"master":"member-id","master_identity":{"host":"localhost","port":8081,"master_eligibility":true,"scheme":"http","version":"7.4.0-ce"}}

	// Extract the actual leader's identity from the leader's metadata
	// to avoid localhost/hostname mismatch that causes Schema Registry to forward
	// requests to itself
	leaderMember, exists := group.Members[group.Leader]
	if !exists {
		// Leader not found - return minimal assignment with no master identity
		// Schema Registry should handle this by failing over to another instance
		glog.Warningf("Schema Registry leader member %s not found in group %s", group.Leader, group.ID)
		jsonAssignment := `{"error":0,"master":"","master_identity":{"host":"","port":0,"master_eligibility":false,"scheme":"http","version":1}}`
		return []byte(jsonAssignment)
	}

	// Parse the leader's metadata to extract the Schema Registry identity
	// The metadata is the serialized SchemaRegistryIdentity JSON
	var identity map[string]interface{}
	err := json.Unmarshal(leaderMember.Metadata, &identity)
	if err != nil {
		// Failed to parse metadata - return minimal assignment
		// Schema Registry should provide valid metadata; if not, fail gracefully
		glog.Warningf("Failed to parse Schema Registry metadata for leader %s: %v", group.Leader, err)
		jsonAssignment := fmt.Sprintf(`{"error":0,"master":"%s","master_identity":{"host":"","port":0,"master_eligibility":false,"scheme":"http","version":1}}`, group.Leader)
		return []byte(jsonAssignment)
	}

	// Extract fields from identity - use empty/zero defaults if missing
	// Schema Registry clients should provide complete metadata
	host := ""
	port := 8081
	scheme := "http"
	version := 1
	leaderEligibility := true

	if h, ok := identity["host"].(string); ok {
		host = h
	} else {
		glog.V(1).Infof("Schema Registry metadata missing 'host' field for leader %s", group.Leader)
	}
	if p, ok := identity["port"].(float64); ok {
		port = int(p)
	}
	if s, ok := identity["scheme"].(string); ok {
		scheme = s
	}
	if v, ok := identity["version"].(float64); ok {
		version = int(v)
	}
	if le, ok := identity["master_eligibility"].(bool); ok {
		leaderEligibility = le
	}

	// Build the assignment JSON with the actual leader identity
	jsonAssignment := fmt.Sprintf(`{"error":0,"master":"%s","master_identity":{"host":"%s","port":%d,"master_eligibility":%t,"scheme":"%s","version":%d}}`,
		group.Leader, host, port, leaderEligibility, scheme, version)

	return []byte(jsonAssignment)
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

	// Get sorted topic names to ensure deterministic order
	topics := make([]string, 0, len(topicAssignments))
	for topic := range topicAssignments {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	// Topics - each topic follows Kafka string + int32 array format
	for _, topic := range topics {
		partitions := topicAssignments[topic]
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
