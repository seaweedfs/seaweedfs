package protocol

import (
	"encoding/binary"
	"fmt"
)

// handleDescribeCluster implements the DescribeCluster API (key 60, versions 0-1)
// This API is used by Java AdminClient for broker discovery (KIP-919)
// Response format (flexible, all versions):
//
//	ThrottleTimeMs(int32) + ErrorCode(int16) + ErrorMessage(compact nullable string) +
//	[v1+: EndpointType(int8)] + ClusterId(compact string) + ControllerId(int32) +
//	Brokers(compact array) + ClusterAuthorizedOperations(int32) + TaggedFields
func (h *Handler) handleDescribeCluster(correlationID uint32, apiVersion uint16, requestBody []byte) ([]byte, error) {

	// Parse request fields (all flexible format)
	offset := 0

	// IncludeClusterAuthorizedOperations (bool - 1 byte)
	if offset >= len(requestBody) {
		return nil, fmt.Errorf("incomplete DescribeCluster request")
	}
	includeAuthorizedOps := requestBody[offset] != 0
	offset++

	// EndpointType (int8, v1+)
	var endpointType int8 = 1 // Default: brokers
	if apiVersion >= 1 {
		if offset >= len(requestBody) {
			return nil, fmt.Errorf("incomplete DescribeCluster v1+ request")
		}
		endpointType = int8(requestBody[offset])
		offset++
	}

	// Tagged fields at end of request
	// (We don't parse them, just skip)

	// Build response
	response := make([]byte, 0, 256)

	// ThrottleTimeMs (int32)
	response = append(response, 0, 0, 0, 0)

	// ErrorCode (int16) - no error
	response = append(response, 0, 0)

	// ErrorMessage (compact nullable string) - null
	response = append(response, 0x00) // varint 0 = null

	// EndpointType (int8, v1+)
	if apiVersion >= 1 {
		response = append(response, byte(endpointType))
	}

	// ClusterId (compact string)
	clusterID := "seaweedfs-kafka-gateway"
	response = append(response, CompactArrayLength(uint32(len(clusterID)))...)
	response = append(response, []byte(clusterID)...)

	// ControllerId (int32) - use broker ID 1
	controllerIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(controllerIDBytes, uint32(1))
	response = append(response, controllerIDBytes...)

	// Brokers (compact array)
	// Get advertised address
	host, port := h.GetAdvertisedAddress(h.GetGatewayAddress())

	// Broker count (compact array length)
	response = append(response, CompactArrayLength(1)...) // 1 broker

	// Broker 0: BrokerId(int32) + Host(compact string) + Port(int32) + Rack(compact nullable string) + TaggedFields
	brokerIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(brokerIDBytes, uint32(1))
	response = append(response, brokerIDBytes...) // BrokerId = 1

	// Host (compact string)
	response = append(response, CompactArrayLength(uint32(len(host)))...)
	response = append(response, []byte(host)...)

	// Port (int32) - validate port range
	if port < 0 || port > 65535 {
		return nil, fmt.Errorf("invalid port number: %d", port)
	}
	portBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(portBytes, uint32(port))
	response = append(response, portBytes...)

	// Rack (compact nullable string) - null
	response = append(response, 0x00) // varint 0 = null

	// Per-broker tagged fields
	response = append(response, 0x00) // Empty tagged fields

	// ClusterAuthorizedOperations (int32) - -2147483648 (INT32_MIN) means not included
	authOpsBytes := make([]byte, 4)
	if includeAuthorizedOps {
		// For now, return 0 (no operations authorized)
		binary.BigEndian.PutUint32(authOpsBytes, 0)
	} else {
		// -2147483648 = INT32_MIN = operations not included
		binary.BigEndian.PutUint32(authOpsBytes, 0x80000000)
	}
	response = append(response, authOpsBytes...)

	// Response-level tagged fields (flexible response)
	response = append(response, 0x00) // Empty tagged fields

	return response, nil
}
