package protocol

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"time"
)

// CoordinatorRegistryInterface defines the interface for coordinator registry operations
type CoordinatorRegistryInterface interface {
	IsLeader() bool
	GetLeaderAddress() string
	WaitForLeader(timeout time.Duration) (string, error)
	AssignCoordinator(consumerGroup string, requestingGateway string) (*CoordinatorAssignment, error)
	GetCoordinator(consumerGroup string) (*CoordinatorAssignment, error)
}

// CoordinatorAssignment represents a consumer group coordinator assignment
type CoordinatorAssignment struct {
	ConsumerGroup     string
	CoordinatorAddr   string
	CoordinatorNodeID int32
	AssignedAt        time.Time
	LastHeartbeat     time.Time
}

func (h *Handler) handleFindCoordinator(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {
	switch apiVersion {
	case 0:
		return h.handleFindCoordinatorV0(correlationID, requestBody)
	case 1, 2:
		return h.handleFindCoordinatorV2(correlationID, requestBody)
	default:
		return nil, fmt.Errorf("FindCoordinator version %d not supported", apiVersion)
	}
}

func (h *Handler) handleFindCoordinatorV0(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse FindCoordinator v0 request: Key (STRING) only

	// DEBUG: Hex dump the request to understand format
	dumpLen := len(requestBody)
	if dumpLen > 50 {
		dumpLen = 50
	}

	if len(requestBody) < 2 { // need at least Key length
		return nil, fmt.Errorf("FindCoordinator request too short")
	}

	offset := 0

	if len(requestBody) < offset+2 { // coordinator_key_size(2)
		return nil, fmt.Errorf("FindCoordinator request missing data (need %d bytes, have %d)", offset+2, len(requestBody))
	}

	// Parse coordinator key (group ID for consumer groups)
	coordinatorKeySize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
	offset += 2

	if len(requestBody) < offset+int(coordinatorKeySize) {
		return nil, fmt.Errorf("FindCoordinator request missing coordinator key (need %d bytes, have %d)", offset+int(coordinatorKeySize), len(requestBody))
	}

	coordinatorKey := string(requestBody[offset : offset+int(coordinatorKeySize)])
	offset += int(coordinatorKeySize)

	// Parse coordinator type (v1+ only, default to 0 for consumer groups in v0)
	_ = int8(0) // Consumer group coordinator (unused in v0)

	// Find the appropriate coordinator for this group
	coordinatorHost, coordinatorPort, nodeID, err := h.findCoordinatorForGroup(coordinatorKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find coordinator for group %s: %w", coordinatorKey, err)
	}

	// Build response
	response := make([]byte, 0, 64)

	// Correlation ID (4 bytes)
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// FindCoordinator v0 Response Format (NO throttle_time_ms, NO error_message):
	// - error_code (INT16)
	// - node_id (INT32)
	// - host (STRING)
	// - port (INT32)

	// Error code (2 bytes, 0 = no error)
	response = append(response, 0, 0)

	// Coordinator node_id (4 bytes)
	nodeIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(nodeIDBytes, uint32(nodeID))
	response = append(response, nodeIDBytes...)

	// Coordinator host (string)
	hostLen := uint16(len(coordinatorHost))
	response = append(response, byte(hostLen>>8), byte(hostLen))
	response = append(response, []byte(coordinatorHost)...)

	// Coordinator port (4 bytes)
	portBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(portBytes, uint32(coordinatorPort))
	response = append(response, portBytes...)

	return response, nil
}

func (h *Handler) handleFindCoordinatorV2(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse FindCoordinator request (v0-2 non-flex): Key (STRING), v1+ adds KeyType (INT8)

	// DEBUG: Hex dump the request to understand format
	dumpLen := len(requestBody)
	if dumpLen > 50 {
		dumpLen = 50
	}

	if len(requestBody) < 2 { // need at least Key length
		return nil, fmt.Errorf("FindCoordinator request too short")
	}

	offset := 0

	if len(requestBody) < offset+2 { // coordinator_key_size(2)
		return nil, fmt.Errorf("FindCoordinator request missing data (need %d bytes, have %d)", offset+2, len(requestBody))
	}

	// Parse coordinator key (group ID for consumer groups)
	coordinatorKeySize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
	offset += 2

	if len(requestBody) < offset+int(coordinatorKeySize) {
		return nil, fmt.Errorf("FindCoordinator request missing coordinator key (need %d bytes, have %d)", offset+int(coordinatorKeySize), len(requestBody))
	}

	coordinatorKey := string(requestBody[offset : offset+int(coordinatorKeySize)])
	offset += int(coordinatorKeySize)

	// Coordinator type present in v1+ (INT8). If absent, default 0.
	var coordinatorType byte = 0
	if offset < len(requestBody) {
		coordinatorType = requestBody[offset]
		_ = coordinatorType // Used for validation but not in current logic
	}

	// Find the appropriate coordinator for this group
	coordinatorHost, coordinatorPort, nodeID, err := h.findCoordinatorForGroup(coordinatorKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find coordinator for group %s: %w", coordinatorKey, err)
	}

	response := make([]byte, 0, 64)

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)

	// FindCoordinator v2 Response Format:
	// - throttle_time_ms (INT32)
	// - error_code (INT16)
	// - error_message (STRING) - nullable
	// - node_id (INT32)
	// - host (STRING)
	// - port (INT32)

	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)

	// Error code (2 bytes, 0 = no error)
	response = append(response, 0, 0)

	// Error message (nullable string) - null for success
	response = append(response, 0xff, 0xff) // -1 length indicates null

	// Coordinator node_id (4 bytes)
	nodeIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(nodeIDBytes, uint32(nodeID))
	response = append(response, nodeIDBytes...)

	// Coordinator host (string)
	hostLen := uint16(len(coordinatorHost))
	response = append(response, byte(hostLen>>8), byte(hostLen))
	response = append(response, []byte(coordinatorHost)...)

	// Coordinator port (4 bytes)
	portBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(portBytes, uint32(coordinatorPort))
	response = append(response, portBytes...)

	return response, nil
}

// findCoordinatorForGroup determines the coordinator gateway for a consumer group
// Uses gateway leader for distributed coordinator assignment (first-come-first-serve)
func (h *Handler) findCoordinatorForGroup(groupID string) (host string, port int, nodeID int32, err error) {
	// Get the coordinator registry from the handler
	registry := h.GetCoordinatorRegistry()
	if registry == nil {
		// Fallback to current gateway if no registry available
		Debug("No coordinator registry available, using current gateway as coordinator for group %s", groupID)
		gatewayAddr := h.GetGatewayAddress()
		host, port, err := h.parseGatewayAddress(gatewayAddr)
		if err != nil {
			Debug("Failed to parse gateway address %s: %v", gatewayAddr, err)
			return "localhost", 9092, 1, nil
		}
		nodeID = 1
		return host, port, nodeID, nil
	}

	// If this gateway is the leader, handle the assignment directly
	if registry.IsLeader() {
		return h.handleCoordinatorAssignmentAsLeader(groupID, registry)
	}

	// If not the leader, contact the leader to get/assign coordinator
	// But first check if we can quickly become the leader or if there's already a leader
	if leader := registry.GetLeaderAddress(); leader != "" {
		Debug("Found existing leader %s for group %s", leader, groupID)
		// If the leader is this gateway, handle assignment directly
		if leader == h.GetGatewayAddress() {
			return h.handleCoordinatorAssignmentAsLeader(groupID, registry)
		}
	}
	return h.requestCoordinatorFromLeader(groupID, registry)
}

// handleCoordinatorAssignmentAsLeader handles coordinator assignment when this gateway is the leader
func (h *Handler) handleCoordinatorAssignmentAsLeader(groupID string, registry CoordinatorRegistryInterface) (host string, port int, nodeID int32, err error) {
	// Check if coordinator already exists
	if assignment, err := registry.GetCoordinator(groupID); err == nil && assignment != nil {
		Debug("Found existing coordinator %s (node %d) for group %s", assignment.CoordinatorAddr, assignment.CoordinatorNodeID, groupID)
		return h.parseAddress(assignment.CoordinatorAddr, assignment.CoordinatorNodeID)
	}

	// No coordinator exists, assign the requesting gateway (first-come-first-serve)
	currentGateway := h.GetGatewayAddress()
	assignment, err := registry.AssignCoordinator(groupID, currentGateway)
	if err != nil {
		Debug("Failed to assign coordinator for group %s: %v", groupID, err)
		// Fallback to current gateway
		gatewayAddr := h.GetGatewayAddress()
		host, port, err := h.parseGatewayAddress(gatewayAddr)
		if err != nil {
			Debug("Failed to parse gateway address %s: %v", gatewayAddr, err)
			return "localhost", 9092, 1, nil
		}
		nodeID = 1
		return host, port, nodeID, nil
	}

	Debug("Assigned coordinator %s (node %d) for group %s", assignment.CoordinatorAddr, assignment.CoordinatorNodeID, groupID)
	return h.parseAddress(assignment.CoordinatorAddr, assignment.CoordinatorNodeID)
}

// requestCoordinatorFromLeader requests coordinator assignment from the gateway leader
// If no leader exists, it waits for leader election to complete
func (h *Handler) requestCoordinatorFromLeader(groupID string, registry CoordinatorRegistryInterface) (host string, port int, nodeID int32, err error) {
	// Wait for leader election to complete with a longer timeout for Schema Registry compatibility
	leaderAddress, err := h.waitForLeader(registry, 10*time.Second) // 10 second timeout for enterprise clients
	if err != nil {
		Debug("Failed to wait for leader election: %v, falling back to current gateway for group %s", err, groupID)
		gatewayAddr := h.GetGatewayAddress()
		host, port, err := h.parseGatewayAddress(gatewayAddr)
		if err != nil {
			Debug("Failed to parse gateway address %s: %v", gatewayAddr, err)
			return "localhost", 9092, 1, nil
		}
		nodeID = 1
		return host, port, nodeID, nil
	}

	Debug("Gateway leader %s elected, requesting coordinator assignment for group %s", leaderAddress, groupID)

	// Since we don't have direct RPC between gateways yet, and the leader might be this gateway,
	// check if we became the leader during the wait
	if registry.IsLeader() {
		Debug("This gateway became the leader during wait, handling assignment directly for group %s", groupID)
		return h.handleCoordinatorAssignmentAsLeader(groupID, registry)
	}

	// For now, if we can't directly contact the leader (no inter-gateway RPC yet),
	// use current gateway as fallback. In a full implementation, this would make
	// an RPC call to the leader gateway.
	Debug("Using current gateway as coordinator for group %s (inter-gateway RPC not implemented)", groupID)
	gatewayAddr := h.GetGatewayAddress()
	host, port, parseErr := h.parseGatewayAddress(gatewayAddr)
	if parseErr != nil {
		Debug("Failed to parse gateway address %s: %v", gatewayAddr, parseErr)
		return "localhost", 9092, 1, nil
	}
	nodeID = 1
	return host, port, nodeID, nil
}

// waitForLeader waits for a leader to be elected, with timeout
func (h *Handler) waitForLeader(registry CoordinatorRegistryInterface, timeout time.Duration) (leaderAddress string, err error) {
	Debug("Waiting for gateway leader election to complete...")

	// Use the registry's efficient wait mechanism
	leaderAddress, err = registry.WaitForLeader(timeout)
	if err != nil {
		Debug("Failed to wait for leader: %v", err)
		return "", err
	}

	Debug("Gateway leader elected: %s", leaderAddress)
	return leaderAddress, nil
}

// parseGatewayAddress parses a gateway address string (host:port) into host and port
func (h *Handler) parseGatewayAddress(address string) (host string, port int, err error) {
	// Use net.SplitHostPort for proper IPv6 support
	hostStr, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, fmt.Errorf("invalid gateway address format: %s", address)
	}

	port, err = strconv.Atoi(portStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port in gateway address %s: %v", address, err)
	}

	return hostStr, port, nil
}

// parseAddress parses a gateway address and returns host, port, and nodeID
func (h *Handler) parseAddress(address string, nodeID int32) (host string, port int, nid int32, err error) {
	// Reuse the correct parseGatewayAddress implementation
	host, port, err = h.parseGatewayAddress(address)
	if err != nil {
		return "", 0, 0, err
	}
	nid = nodeID
	return host, port, nid, nil
}
