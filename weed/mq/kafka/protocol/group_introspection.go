package protocol

import (
	"encoding/binary"
	"fmt"
)

// handleDescribeGroups handles DescribeGroups API (key 15)
func (h *Handler) handleDescribeGroups(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {

	// Parse request
	request, err := h.parseDescribeGroupsRequest(requestBody, apiVersion)
	if err != nil {
		return nil, fmt.Errorf("parse DescribeGroups request: %w", err)
	}

	// Build response
	response := DescribeGroupsResponse{
		ThrottleTimeMs: 0,
		Groups:         make([]DescribeGroupsGroup, 0, len(request.GroupIDs)),
	}

	// Get group information for each requested group
	for _, groupID := range request.GroupIDs {
		group := h.describeGroup(groupID)
		response.Groups = append(response.Groups, group)
	}

	return h.buildDescribeGroupsResponse(response, correlationID, apiVersion), nil
}

// handleListGroups handles ListGroups API (key 16)
func (h *Handler) handleListGroups(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {

	// Parse request (ListGroups has minimal request structure)
	request, err := h.parseListGroupsRequest(requestBody, apiVersion)
	if err != nil {
		return nil, fmt.Errorf("parse ListGroups request: %w", err)
	}

	// Build response
	response := ListGroupsResponse{
		ThrottleTimeMs: 0,
		ErrorCode:      0,
		Groups:         h.listAllGroups(request.StatesFilter),
	}

	return h.buildListGroupsResponse(response, correlationID, apiVersion), nil
}

// describeGroup gets detailed information about a specific group
func (h *Handler) describeGroup(groupID string) DescribeGroupsGroup {
	// Get group information from coordinator
	if h.groupCoordinator == nil {
		return DescribeGroupsGroup{
			ErrorCode: 15, // GROUP_COORDINATOR_NOT_AVAILABLE
			GroupID:   groupID,
			State:     "Dead",
		}
	}

	group := h.groupCoordinator.GetGroup(groupID)
	if group == nil {
		return DescribeGroupsGroup{
			ErrorCode:    25, // UNKNOWN_GROUP_ID
			GroupID:      groupID,
			State:        "Dead",
			ProtocolType: "",
			Protocol:     "",
			Members:      []DescribeGroupsMember{},
		}
	}

	// Convert group to response format
	members := make([]DescribeGroupsMember, 0, len(group.Members))
	for memberID, member := range group.Members {
		// Convert assignment to bytes (simplified)
		var assignmentBytes []byte
		if len(member.Assignment) > 0 {
			// In a real implementation, this would serialize the assignment properly
			assignmentBytes = []byte(fmt.Sprintf("assignment:%d", len(member.Assignment)))
		}

		members = append(members, DescribeGroupsMember{
			MemberID:         memberID,
			GroupInstanceID:  member.GroupInstanceID, // Now supports static membership
			ClientID:         member.ClientID,
			ClientHost:       member.ClientHost,
			MemberMetadata:   member.Metadata,
			MemberAssignment: assignmentBytes,
		})
	}

	// Convert group state to string
	var stateStr string
	switch group.State {
	case 0: // Assuming 0 is Empty
		stateStr = "Empty"
	case 1: // Assuming 1 is PreparingRebalance
		stateStr = "PreparingRebalance"
	case 2: // Assuming 2 is CompletingRebalance
		stateStr = "CompletingRebalance"
	case 3: // Assuming 3 is Stable
		stateStr = "Stable"
	default:
		stateStr = "Dead"
	}

	return DescribeGroupsGroup{
		ErrorCode:     0,
		GroupID:       groupID,
		State:         stateStr,
		ProtocolType:  "consumer", // Default protocol type
		Protocol:      group.Protocol,
		Members:       members,
		AuthorizedOps: []int32{}, // Empty for now
	}
}

// listAllGroups gets a list of all consumer groups
func (h *Handler) listAllGroups(statesFilter []string) []ListGroupsGroup {
	if h.groupCoordinator == nil {
		return []ListGroupsGroup{}
	}

	allGroupIDs := h.groupCoordinator.ListGroups()
	groups := make([]ListGroupsGroup, 0, len(allGroupIDs))

	for _, groupID := range allGroupIDs {
		// Get the full group details
		group := h.groupCoordinator.GetGroup(groupID)
		if group == nil {
			continue
		}

		// Convert group state to string
		var stateStr string
		switch group.State {
		case 0:
			stateStr = "Empty"
		case 1:
			stateStr = "PreparingRebalance"
		case 2:
			stateStr = "CompletingRebalance"
		case 3:
			stateStr = "Stable"
		default:
			stateStr = "Dead"
		}

		// Apply state filter if provided
		if len(statesFilter) > 0 {
			matchesFilter := false
			for _, state := range statesFilter {
				if stateStr == state {
					matchesFilter = true
					break
				}
			}
			if !matchesFilter {
				continue
			}
		}

		groups = append(groups, ListGroupsGroup{
			GroupID:      group.ID,
			ProtocolType: "consumer", // Default protocol type
			GroupState:   stateStr,
		})
	}

	return groups
}

// Request/Response structures

type DescribeGroupsRequest struct {
	GroupIDs             []string
	IncludeAuthorizedOps bool
}

type DescribeGroupsResponse struct {
	ThrottleTimeMs int32
	Groups         []DescribeGroupsGroup
}

type DescribeGroupsGroup struct {
	ErrorCode     int16
	GroupID       string
	State         string
	ProtocolType  string
	Protocol      string
	Members       []DescribeGroupsMember
	AuthorizedOps []int32
}

type DescribeGroupsMember struct {
	MemberID         string
	GroupInstanceID  *string
	ClientID         string
	ClientHost       string
	MemberMetadata   []byte
	MemberAssignment []byte
}

type ListGroupsRequest struct {
	StatesFilter []string
}

type ListGroupsResponse struct {
	ThrottleTimeMs int32
	ErrorCode      int16
	Groups         []ListGroupsGroup
}

type ListGroupsGroup struct {
	GroupID      string
	ProtocolType string
	GroupState   string
}

// Parsing functions

func (h *Handler) parseDescribeGroupsRequest(data []byte, apiVersion uint16) (*DescribeGroupsRequest, error) {
	offset := 0
	request := &DescribeGroupsRequest{}

	// Skip client_id if present (depends on version)
	if len(data) < 4 {
		return nil, fmt.Errorf("request too short")
	}

	// Group IDs array
	groupCount := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	request.GroupIDs = make([]string, groupCount)
	for i := uint32(0); i < groupCount; i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("invalid group ID at index %d", i)
		}

		groupIDLen := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		if offset+int(groupIDLen) > len(data) {
			return nil, fmt.Errorf("group ID too long at index %d", i)
		}

		request.GroupIDs[i] = string(data[offset : offset+int(groupIDLen)])
		offset += int(groupIDLen)
	}

	// Include authorized operations (v3+)
	if apiVersion >= 3 && offset < len(data) {
		request.IncludeAuthorizedOps = data[offset] != 0
	}

	return request, nil
}

func (h *Handler) parseListGroupsRequest(data []byte, apiVersion uint16) (*ListGroupsRequest, error) {
	request := &ListGroupsRequest{}

	// ListGroups v4+ includes states filter
	if apiVersion >= 4 && len(data) >= 4 {
		offset := 0
		statesCount := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		if statesCount > 0 {
			request.StatesFilter = make([]string, statesCount)
			for i := uint32(0); i < statesCount; i++ {
				if offset+2 > len(data) {
					break
				}

				stateLen := binary.BigEndian.Uint16(data[offset : offset+2])
				offset += 2

				if offset+int(stateLen) > len(data) {
					break
				}

				request.StatesFilter[i] = string(data[offset : offset+int(stateLen)])
				offset += int(stateLen)
			}
		}
	}

	return request, nil
}

// Response building functions

func (h *Handler) buildDescribeGroupsResponse(response DescribeGroupsResponse, correlationID uint32, apiVersion uint16) []byte {
	buf := make([]byte, 0, 1024)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	buf = append(buf, correlationIDBytes...)

	// Throttle time (v1+)
	if apiVersion >= 1 {
		throttleBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(throttleBytes, uint32(response.ThrottleTimeMs))
		buf = append(buf, throttleBytes...)
	}

	// Groups array
	groupCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(groupCountBytes, uint32(len(response.Groups)))
	buf = append(buf, groupCountBytes...)

	for _, group := range response.Groups {
		// Error code
		buf = append(buf, byte(group.ErrorCode>>8), byte(group.ErrorCode))

		// Group ID
		groupIDLen := uint16(len(group.GroupID))
		buf = append(buf, byte(groupIDLen>>8), byte(groupIDLen))
		buf = append(buf, []byte(group.GroupID)...)

		// State
		stateLen := uint16(len(group.State))
		buf = append(buf, byte(stateLen>>8), byte(stateLen))
		buf = append(buf, []byte(group.State)...)

		// Protocol type
		protocolTypeLen := uint16(len(group.ProtocolType))
		buf = append(buf, byte(protocolTypeLen>>8), byte(protocolTypeLen))
		buf = append(buf, []byte(group.ProtocolType)...)

		// Protocol
		protocolLen := uint16(len(group.Protocol))
		buf = append(buf, byte(protocolLen>>8), byte(protocolLen))
		buf = append(buf, []byte(group.Protocol)...)

		// Members array
		memberCountBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(memberCountBytes, uint32(len(group.Members)))
		buf = append(buf, memberCountBytes...)

		for _, member := range group.Members {
			// Member ID
			memberIDLen := uint16(len(member.MemberID))
			buf = append(buf, byte(memberIDLen>>8), byte(memberIDLen))
			buf = append(buf, []byte(member.MemberID)...)

			// Group instance ID (v4+, nullable)
			if apiVersion >= 4 {
				if member.GroupInstanceID != nil {
					instanceIDLen := uint16(len(*member.GroupInstanceID))
					buf = append(buf, byte(instanceIDLen>>8), byte(instanceIDLen))
					buf = append(buf, []byte(*member.GroupInstanceID)...)
				} else {
					buf = append(buf, 0xFF, 0xFF) // null
				}
			}

			// Client ID
			clientIDLen := uint16(len(member.ClientID))
			buf = append(buf, byte(clientIDLen>>8), byte(clientIDLen))
			buf = append(buf, []byte(member.ClientID)...)

			// Client host
			clientHostLen := uint16(len(member.ClientHost))
			buf = append(buf, byte(clientHostLen>>8), byte(clientHostLen))
			buf = append(buf, []byte(member.ClientHost)...)

			// Member metadata
			metadataLen := uint32(len(member.MemberMetadata))
			metadataLenBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(metadataLenBytes, metadataLen)
			buf = append(buf, metadataLenBytes...)
			buf = append(buf, member.MemberMetadata...)

			// Member assignment
			assignmentLen := uint32(len(member.MemberAssignment))
			assignmentLenBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(assignmentLenBytes, assignmentLen)
			buf = append(buf, assignmentLenBytes...)
			buf = append(buf, member.MemberAssignment...)
		}

		// Authorized operations (v3+)
		if apiVersion >= 3 {
			opsCountBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(opsCountBytes, uint32(len(group.AuthorizedOps)))
			buf = append(buf, opsCountBytes...)

			for _, op := range group.AuthorizedOps {
				opBytes := make([]byte, 4)
				binary.BigEndian.PutUint32(opBytes, uint32(op))
				buf = append(buf, opBytes...)
			}
		}
	}

	return buf
}

func (h *Handler) buildListGroupsResponse(response ListGroupsResponse, correlationID uint32, apiVersion uint16) []byte {
	buf := make([]byte, 0, 512)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	buf = append(buf, correlationIDBytes...)

	// Throttle time (v1+)
	if apiVersion >= 1 {
		throttleBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(throttleBytes, uint32(response.ThrottleTimeMs))
		buf = append(buf, throttleBytes...)
	}

	// Error code
	buf = append(buf, byte(response.ErrorCode>>8), byte(response.ErrorCode))

	// Groups array
	groupCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(groupCountBytes, uint32(len(response.Groups)))
	buf = append(buf, groupCountBytes...)

	for _, group := range response.Groups {
		// Group ID
		groupIDLen := uint16(len(group.GroupID))
		buf = append(buf, byte(groupIDLen>>8), byte(groupIDLen))
		buf = append(buf, []byte(group.GroupID)...)

		// Protocol type
		protocolTypeLen := uint16(len(group.ProtocolType))
		buf = append(buf, byte(protocolTypeLen>>8), byte(protocolTypeLen))
		buf = append(buf, []byte(group.ProtocolType)...)

		// Group state (v4+)
		if apiVersion >= 4 {
			groupStateLen := uint16(len(group.GroupState))
			buf = append(buf, byte(groupStateLen>>8), byte(groupStateLen))
			buf = append(buf, []byte(group.GroupState)...)
		}
	}

	return buf
}
