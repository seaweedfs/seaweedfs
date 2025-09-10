package protocol

import (
	"encoding/binary"
	"fmt"
)

func (h *Handler) handleFindCoordinator(correlationID uint32, requestBody []byte) ([]byte, error) {
	// Parse FindCoordinator request
	// Request format: client_id + coordinator_key + coordinator_type(1)
	
	if len(requestBody) < 2 { // client_id_size(2)
		return nil, fmt.Errorf("FindCoordinator request too short")
	}
	
	// Skip client_id
	clientIDSize := binary.BigEndian.Uint16(requestBody[0:2])
	offset := 2 + int(clientIDSize)
	
	if len(requestBody) < offset+3 { // coordinator_key_size(2) + coordinator_type(1)
		return nil, fmt.Errorf("FindCoordinator request missing data")
	}
	
	// Parse coordinator key (group ID for consumer groups)
	coordinatorKeySize := binary.BigEndian.Uint16(requestBody[offset : offset+2])
	offset += 2
	
	if len(requestBody) < offset+int(coordinatorKeySize)+1 {
		return nil, fmt.Errorf("FindCoordinator request missing coordinator key")
	}
	
	coordinatorKey := string(requestBody[offset : offset+int(coordinatorKeySize)])
	offset += int(coordinatorKeySize)
	
	coordinatorType := requestBody[offset]
	_ = coordinatorType // 0 = group coordinator, 1 = transaction coordinator
	
	fmt.Printf("DEBUG: FindCoordinator request for key '%s' (type: %d)\n", coordinatorKey, coordinatorType)
	
	// DEBUG: Hex dump the request to understand format
	dumpLen := len(requestBody)
	if dumpLen > 50 {
		dumpLen = 50
	}
	fmt.Printf("DEBUG: FindCoordinator request hex dump (first %d bytes): %x\n", dumpLen, requestBody[:dumpLen])
	
	response := make([]byte, 0, 64)
	
	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	response = append(response, correlationIDBytes...)
	
	// Throttle time (4 bytes, 0 = no throttling)
	response = append(response, 0, 0, 0, 0)
	
	// Error code (2 bytes, 0 = no error)
	response = append(response, 0, 0)
	
	// Error message (nullable string) - null (-1 length)
	response = append(response, 0xFF, 0xFF)
	
	// Coordinator node_id (4 bytes) - use broker 0 (this gateway)
	response = append(response, 0, 0, 0, 0)
	
	// Coordinator host (string)
	host := h.brokerHost
	hostLen := uint16(len(host))
	response = append(response, byte(hostLen>>8), byte(hostLen))
	response = append(response, []byte(host)...)
	
	// Coordinator port (4 bytes)
	portBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(portBytes, uint32(h.brokerPort))
	response = append(response, portBytes...)
	
	fmt.Printf("DEBUG: FindCoordinator response: coordinator at %s:%d\n", host, h.brokerPort)
	fmt.Printf("DEBUG: FindCoordinator response hex dump (%d bytes): %x\n", len(response), response)
	
	return response, nil
}
