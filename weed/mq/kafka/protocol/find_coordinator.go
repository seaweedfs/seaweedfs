package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
)

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
	fmt.Printf("DEBUG: FindCoordinator v0 request hex dump (first %d bytes): %x\n", dumpLen, requestBody[:dumpLen])

	if len(requestBody) < 2 { // need at least Key length
		return nil, fmt.Errorf("FindCoordinator request too short")
	}

	offset := 0

	if len(requestBody) < offset+2 { // coordinator_key_size(2)
		return nil, fmt.Errorf("FindCoordinator request missing data (need %d bytes, have %d)", offset+2, len(requestBody))
	}

	// Parse coordinator key (group ID for consumer groups)
	coordinatorKeySize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
	fmt.Printf("DEBUG: FindCoordinator coordinator_key_size: %d, offset: %d\n", coordinatorKeySize, offset)
	offset += 2

	if len(requestBody) < offset+int(coordinatorKeySize) {
		return nil, fmt.Errorf("FindCoordinator request missing coordinator key (need %d bytes, have %d)", offset+int(coordinatorKeySize), len(requestBody))
	}

	coordinatorKey := string(requestBody[offset : offset+int(coordinatorKeySize)])
	offset += int(coordinatorKeySize)

	// Parse coordinator type (v1+ only, default to 0 for consumer groups in v0)
	coordinatorType := int8(0) // Consumer group coordinator

	fmt.Printf("DEBUG: FindCoordinator request for key '%s' (type: %d)\n", coordinatorKey, coordinatorType)

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

	fmt.Printf("DEBUG: FindCoordinator v0 response: coordinator at %s:%d (node %d)\n", coordinatorHost, coordinatorPort, nodeID)
	fmt.Printf("DEBUG: FindCoordinator v0 response hex dump (%d bytes): %x\n", len(response), response)

	return response, nil
}

func (h *Handler) handleFindCoordinatorV2(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse FindCoordinator request (v0-2 non-flex): Key (STRING), v1+ adds KeyType (INT8)

	// DEBUG: Hex dump the request to understand format
	dumpLen := len(requestBody)
	if dumpLen > 50 {
		dumpLen = 50
	}
	fmt.Printf("DEBUG: FindCoordinator request hex dump (first %d bytes): %x\n", dumpLen, requestBody[:dumpLen])

	if len(requestBody) < 2 { // need at least Key length
		return nil, fmt.Errorf("FindCoordinator request too short")
	}

	offset := 0

	if len(requestBody) < offset+2 { // coordinator_key_size(2)
		return nil, fmt.Errorf("FindCoordinator request missing data (need %d bytes, have %d)", offset+2, len(requestBody))
	}

	// Parse coordinator key (group ID for consumer groups)
	coordinatorKeySize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
	fmt.Printf("DEBUG: FindCoordinator coordinator_key_size: %d, offset: %d\n", coordinatorKeySize, offset)
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
	}
	fmt.Printf("DEBUG: FindCoordinator request for key '%s' (type: %d)\n", coordinatorKey, coordinatorType)

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

	fmt.Printf("DEBUG: FindCoordinator v2 response: coordinator at %s:%d (node %d)\n", coordinatorHost, coordinatorPort, nodeID)
	fmt.Printf("DEBUG: FindCoordinator response hex dump (%d bytes): %x\n", len(response), response)

	return response, nil
}

// findCoordinatorForGroup determines the coordinator gateway for a consumer group
// Asks the SMQ broker leader to assign a gateway for this consumer group
func (h *Handler) findCoordinatorForGroup(groupID string) (host string, port int, nodeID int32, err error) {
	// If we don't have SeaweedMQ handler, fallback to current gateway
	if h.seaweedMQHandler == nil {
		host, port = h.GetBrokerAddress()
		return host, port, 1, nil
	}

	// Ask SMQ brokers to assign a coordinator gateway for this consumer group
	gatewayAddress, err := h.requestCoordinatorFromBroker(groupID)
	if err != nil {
		// Fallback to current gateway if broker coordination fails
		host, port = h.GetBrokerAddress()
		return host, port, 1, nil
	}

	// Parse the gateway address returned by the broker
	host, port, parseErr := h.parseBrokerAddress(gatewayAddress)
	if parseErr != nil {
		// Fallback to current gateway if parsing fails
		host, port = h.GetBrokerAddress()
		return host, port, 1, nil
	}

	// Use consistent node ID based on group ID for this gateway
	nodeID = h.generateNodeID(groupID)
	return host, port, nodeID, nil
}

// requestCoordinatorFromBroker asks the SMQ broker leader to assign a coordinator gateway
func (h *Handler) requestCoordinatorFromBroker(groupID string) (string, error) {
	// TODO: Implement proper gateway registry with SMQ brokers
	// For now, use GetAvailableBrokers as a placeholder that should return gateway addresses
	gatewayAddresses, err := h.seaweedMQHandler.GetAvailableBrokers()
	if err != nil || len(gatewayAddresses) == 0 {
		return "", fmt.Errorf("no gateway instances available: %v", err)
	}

	// Use consistent hashing to select a gateway for this consumer group
	hash := crc32.ChecksumIEEE([]byte(groupID))
	selectedIndex := int(hash) % len(gatewayAddresses)
	selectedGateway := gatewayAddresses[selectedIndex]

	return selectedGateway, nil
}

// parseBrokerAddress parses a broker address string into host and port
func (h *Handler) parseBrokerAddress(brokerAddress string) (host string, port int, err error) {
	// Split address into host:port
	parts := strings.Split(brokerAddress, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid broker address format: %s", brokerAddress)
	}

	host = parts[0]
	port, err = strconv.Atoi(parts[1])
	if err != nil {
		return "", 0, fmt.Errorf("invalid port in broker address %s: %v", brokerAddress, err)
	}

	return host, port, nil
}

// generateNodeID generates a consistent node ID for a gateway based on group ID
func (h *Handler) generateNodeID(groupID string) int32 {
	hash := crc32.ChecksumIEEE([]byte(groupID))
	// Use hash to generate a node ID between 1-1000 for consistency
	return int32(hash%1000) + 1
}
