package protocol

import (
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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
	glog.V(2).Infof("FindCoordinator: version=%d, correlation=%d, bodyLen=%d", apiVersion, correlationID, len(requestBody))
	switch apiVersion {
	case 0:
		glog.V(4).Infof("FindCoordinator - Routing to V0 handler")
		return h.handleFindCoordinatorV0(correlationID, requestBody)
	case 1, 2:
		glog.V(4).Infof("FindCoordinator - Routing to V1-2 handler (non-flexible)")
		return h.handleFindCoordinatorV2(correlationID, requestBody)
	case 3:
		glog.V(4).Infof("FindCoordinator - Routing to V3 handler (flexible)")
		return h.handleFindCoordinatorV3(correlationID, requestBody)
	default:
		return nil, fmt.Errorf("FindCoordinator version %d not supported", apiVersion)
	}
}

func (h *Handler) handleFindCoordinatorV0(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse FindCoordinator v0 request: Key (STRING) only

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

	// Return hostname instead of IP address for client connectivity
	// Clients need to connect to the same hostname they originally connected to
	_ = coordinatorHost // originalHost
	coordinatorHost = h.getClientConnectableHost(coordinatorHost)

	// Build response
	response := make([]byte, 0, 64)

	// NOTE: Correlation ID is handled by writeResponseWithHeader
	// Do NOT include it in the response body

	// FindCoordinator v0 Response Format (NO throttle_time_ms, NO error_message):
	// - error_code (INT16)
	// - node_id (INT32)
	// - host (STRING)
	// - port (INT32)

	// Error code (2 bytes, 0 = no error)
	response = append(response, 0, 0)

	// Coordinator node_id (4 bytes) - use direct bit conversion for int32 to uint32
	nodeIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(nodeIDBytes, uint32(int32(nodeID)))
	response = append(response, nodeIDBytes...)

	// Coordinator host (string)
	hostLen := uint16(len(coordinatorHost))
	response = append(response, byte(hostLen>>8), byte(hostLen))
	response = append(response, []byte(coordinatorHost)...)

	// Coordinator port (4 bytes) - validate port range
	if coordinatorPort < 0 || coordinatorPort > 65535 {
		return nil, fmt.Errorf("invalid port number: %d", coordinatorPort)
	}
	portBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(portBytes, uint32(coordinatorPort))
	response = append(response, portBytes...)

	return response, nil
}

func (h *Handler) handleFindCoordinatorV2(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse FindCoordinator request (v0-2 non-flex): Key (STRING), v1+ adds KeyType (INT8)

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
	if offset < len(requestBody) {
		_ = requestBody[offset] // coordinatorType
		offset++                // Move past the coordinator type byte
	}

	// Find the appropriate coordinator for this group
	coordinatorHost, coordinatorPort, nodeID, err := h.findCoordinatorForGroup(coordinatorKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find coordinator for group %s: %w", coordinatorKey, err)
	}

	// Return hostname instead of IP address for client connectivity
	// Clients need to connect to the same hostname they originally connected to
	_ = coordinatorHost // originalHost
	coordinatorHost = h.getClientConnectableHost(coordinatorHost)

	response := make([]byte, 0, 64)

	// NOTE: Correlation ID is handled by writeResponseWithHeader
	// Do NOT include it in the response body

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

	// Coordinator node_id (4 bytes) - use direct bit conversion for int32 to uint32
	nodeIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(nodeIDBytes, uint32(int32(nodeID)))
	response = append(response, nodeIDBytes...)

	// Coordinator host (string)
	hostLen := uint16(len(coordinatorHost))
	response = append(response, byte(hostLen>>8), byte(hostLen))
	response = append(response, []byte(coordinatorHost)...)

	// Coordinator port (4 bytes) - validate port range
	if coordinatorPort < 0 || coordinatorPort > 65535 {
		return nil, fmt.Errorf("invalid port number: %d", coordinatorPort)
	}
	portBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(portBytes, uint32(coordinatorPort))
	response = append(response, portBytes...)

	// Debug logging (hex dump removed to reduce CPU usage)
	if glog.V(4) {
		glog.V(4).Infof("FindCoordinator v2: Built response - bodyLen=%d, host='%s' (len=%d), port=%d, nodeID=%d",
			len(response), coordinatorHost, len(coordinatorHost), coordinatorPort, nodeID)
	}

	return response, nil
}

func (h *Handler) handleFindCoordinatorV3(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse FindCoordinator v3 request (flexible version):
	// - Key (COMPACT_STRING with varint length+1)
	// - KeyType (INT8)
	// - Tagged fields (varint)

	if len(requestBody) < 2 {
		return nil, fmt.Errorf("FindCoordinator v3 request too short")
	}

	// HEX DUMP for debugging
	glog.V(4).Infof("FindCoordinator V3 request body (first 50 bytes): % x", requestBody[:min(50, len(requestBody))])
	glog.V(4).Infof("FindCoordinator V3 request body length: %d", len(requestBody))

	offset := 0

	// The first byte is the tagged fields from the REQUEST HEADER that weren't consumed
	// Skip the tagged fields count (should be 0x00 for no tagged fields)
	if len(requestBody) > 0 && requestBody[0] == 0x00 {
		glog.V(4).Infof("FindCoordinator V3: Skipping header tagged fields byte (0x00)")
		offset = 1
	}

	// Parse coordinator key (compact string: varint length+1)
	glog.V(4).Infof("FindCoordinator V3: About to decode varint from bytes: % x", requestBody[offset:min(offset+5, len(requestBody))])
	coordinatorKeyLen, bytesRead, err := DecodeUvarint(requestBody[offset:])
	if err != nil || bytesRead <= 0 {
		return nil, fmt.Errorf("failed to decode coordinator key length: %w (bytes: % x)", err, requestBody[offset:min(offset+5, len(requestBody))])
	}
	offset += bytesRead

	glog.V(4).Infof("FindCoordinator V3: coordinatorKeyLen (varint)=%d, bytesRead=%d, offset now=%d", coordinatorKeyLen, bytesRead, offset)
	glog.V(4).Infof("FindCoordinator V3: Next bytes after varint: % x", requestBody[offset:min(offset+20, len(requestBody))])

	if coordinatorKeyLen == 0 {
		return nil, fmt.Errorf("coordinator key cannot be null in v3")
	}
	// Compact strings in Kafka use length+1 encoding:
	// varint=0 means null, varint=1 means empty string, varint=n+1 means string of length n
	coordinatorKeyLen-- // Decode: actual length = varint - 1

	glog.V(4).Infof("FindCoordinator V3: actual coordinatorKeyLen after decoding: %d", coordinatorKeyLen)

	if len(requestBody) < offset+int(coordinatorKeyLen) {
		return nil, fmt.Errorf("FindCoordinator v3 request missing coordinator key")
	}

	coordinatorKey := string(requestBody[offset : offset+int(coordinatorKeyLen)])
	offset += int(coordinatorKeyLen)

	// Parse coordinator type (INT8)
	if offset < len(requestBody) {
		_ = requestBody[offset] // coordinatorType
		offset++
	}

	// Skip tagged fields (we don't need them for now)
	if offset < len(requestBody) {
		_, bytesRead, tagErr := DecodeUvarint(requestBody[offset:])
		if tagErr == nil && bytesRead > 0 {
			offset += bytesRead
			// TODO: Parse tagged fields if needed
		}
	}

	// Find the appropriate coordinator for this group
	coordinatorHost, coordinatorPort, nodeID, err := h.findCoordinatorForGroup(coordinatorKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find coordinator for group %s: %w", coordinatorKey, err)
	}

	// Return hostname instead of IP address for client connectivity
	_ = coordinatorHost // originalHost
	coordinatorHost = h.getClientConnectableHost(coordinatorHost)

	// Build response (v3 is flexible, uses compact strings and tagged fields)
	response := make([]byte, 0, 64)

	// NOTE: Correlation ID is handled by writeResponseWithHeader
	// Do NOT include it in the response body

	// FindCoordinator v3 Response Format (FLEXIBLE):
	// - throttle_time_ms (INT32)
	// - error_code (INT16)
	// - error_message (COMPACT_NULLABLE_STRING with varint length+1, 0 = null)
	// - node_id (INT32)
	// - host (COMPACT_STRING with varint length+1)
	// - port (INT32)
	// - tagged_fields (varint, 0 = no tags)

	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)

	// Error code (2 bytes, 0 = no error)
	response = append(response, 0, 0)

	// Error message (compact nullable string) - null for success
	// Compact nullable string: 0 = null, 1 = empty string, n+1 = string of length n
	response = append(response, 0) // 0 = null

	// Coordinator node_id (4 bytes) - use direct bit conversion for int32 to uint32
	nodeIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(nodeIDBytes, uint32(int32(nodeID)))
	response = append(response, nodeIDBytes...)

	// Coordinator host (compact string: varint length+1)
	hostLen := uint32(len(coordinatorHost))
	response = append(response, EncodeUvarint(hostLen+1)...) // +1 for compact string encoding
	response = append(response, []byte(coordinatorHost)...)

	// Coordinator port (4 bytes) - validate port range
	if coordinatorPort < 0 || coordinatorPort > 65535 {
		return nil, fmt.Errorf("invalid port number: %d", coordinatorPort)
	}
	portBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(portBytes, uint32(coordinatorPort))
	response = append(response, portBytes...)

	// Tagged fields (0 = no tags)
	response = append(response, 0)

	return response, nil
}

// findCoordinatorForGroup determines the coordinator gateway for a consumer group
// Uses gateway leader for distributed coordinator assignment (first-come-first-serve)
func (h *Handler) findCoordinatorForGroup(groupID string) (host string, port int, nodeID int32, err error) {
	// Get the coordinator registry from the handler
	registry := h.GetCoordinatorRegistry()
	if registry == nil {
		// Fallback to current gateway if no registry available
		gatewayAddr := h.GetGatewayAddress()
		if gatewayAddr == "" {
			return "", 0, 0, fmt.Errorf("no coordinator registry and no gateway address configured")
		}
		host, port, err := h.parseGatewayAddress(gatewayAddr)
		if err != nil {
			return "", 0, 0, fmt.Errorf("failed to parse gateway address: %w", err)
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
		return h.parseAddress(assignment.CoordinatorAddr, assignment.CoordinatorNodeID)
	}

	// No coordinator exists, assign the requesting gateway (first-come-first-serve)
	currentGateway := h.GetGatewayAddress()
	if currentGateway == "" {
		return "", 0, 0, fmt.Errorf("no gateway address configured for coordinator assignment")
	}
	assignment, err := registry.AssignCoordinator(groupID, currentGateway)
	if err != nil {
		// Fallback to current gateway on assignment error
		host, port, parseErr := h.parseGatewayAddress(currentGateway)
		if parseErr != nil {
			return "", 0, 0, fmt.Errorf("failed to parse gateway address after assignment error: %w", parseErr)
		}
		nodeID = 1
		return host, port, nodeID, nil
	}

	return h.parseAddress(assignment.CoordinatorAddr, assignment.CoordinatorNodeID)
}

// requestCoordinatorFromLeader requests coordinator assignment from the gateway leader
// If no leader exists, it waits for leader election to complete
func (h *Handler) requestCoordinatorFromLeader(groupID string, registry CoordinatorRegistryInterface) (host string, port int, nodeID int32, err error) {
	// Wait for leader election to complete with a longer timeout for Schema Registry compatibility
	_, err = h.waitForLeader(registry, 10*time.Second) // 10 second timeout for enterprise clients
	if err != nil {
		gatewayAddr := h.GetGatewayAddress()
		if gatewayAddr == "" {
			return "", 0, 0, fmt.Errorf("failed to wait for leader and no gateway address configured: %w", err)
		}
		host, port, parseErr := h.parseGatewayAddress(gatewayAddr)
		if parseErr != nil {
			return "", 0, 0, fmt.Errorf("failed to parse gateway address after leader wait timeout: %w", parseErr)
		}
		nodeID = 1
		return host, port, nodeID, nil
	}

	// Since we don't have direct RPC between gateways yet, and the leader might be this gateway,
	// check if we became the leader during the wait
	if registry.IsLeader() {
		return h.handleCoordinatorAssignmentAsLeader(groupID, registry)
	}

	// For now, if we can't directly contact the leader (no inter-gateway RPC yet),
	// use current gateway as fallback. In a full implementation, this would make
	// an RPC call to the leader gateway.
	gatewayAddr := h.GetGatewayAddress()
	if gatewayAddr == "" {
		return "", 0, 0, fmt.Errorf("no gateway address configured for fallback coordinator")
	}
	host, port, parseErr := h.parseGatewayAddress(gatewayAddr)
	if parseErr != nil {
		return "", 0, 0, fmt.Errorf("failed to parse gateway address for fallback: %w", parseErr)
	}
	nodeID = 1
	return host, port, nodeID, nil
}

// waitForLeader waits for a leader to be elected, with timeout
func (h *Handler) waitForLeader(registry CoordinatorRegistryInterface, timeout time.Duration) (leaderAddress string, err error) {

	// Use the registry's efficient wait mechanism
	leaderAddress, err = registry.WaitForLeader(timeout)
	if err != nil {
		return "", err
	}

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

// getClientConnectableHost returns the hostname that clients can connect to
// This ensures that FindCoordinator returns the same hostname the client originally connected to
func (h *Handler) getClientConnectableHost(coordinatorHost string) string {
	// If the coordinator host is an IP address, return the original gateway hostname
	// This prevents clients from switching to IP addresses which creates new connections
	if net.ParseIP(coordinatorHost) != nil {
		// It's an IP address, return the original gateway hostname
		gatewayAddr := h.GetGatewayAddress()
		if host, _, err := h.parseGatewayAddress(gatewayAddr); err == nil {
			// If the gateway address is also an IP, return the IP directly
			// This handles local/test environments where hostnames aren't resolvable
			if net.ParseIP(host) != nil {
				// Both are IPs, return the actual IP address
				return coordinatorHost
			}
			return host
		}
		// Fallback to the coordinator host IP itself
		return coordinatorHost
	}

	// It's already a hostname, return as-is
	return coordinatorHost
}
