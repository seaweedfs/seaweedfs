package kafka

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestJoinGroupDebug captures the exact JoinGroup request/response to debug format issues
func TestJoinGroupDebug(t *testing.T) {
	// Start gateway server
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: ":0", // random port
	})

	go func() {
		if err := gatewayServer.Start(); err != nil {
			t.Errorf("Gateway server error: %v", err)
		}
	}()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Get the actual listening address
	host, port := gatewayServer.GetListenerAddr()
	brokerAddr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", brokerAddr)

	// Get handler and configure it
	handler := gatewayServer.GetHandler()
	handler.SetBrokerAddress(host, port)

	// Add test topic
	topicName := "joingroup-debug-topic"
	handler.AddTopicForTesting(topicName, 1)

	// Create raw TCP connection to manually send JoinGroup request
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Send ApiVersions request first
	t.Log("=== Sending ApiVersions request ===")
	apiVersionsRequest := []byte{
		// Request header: api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) + client_id
		0x00, 0x12, // api_key = 18 (ApiVersions)
		0x00, 0x00, // api_version = 0
		0x00, 0x00, 0x00, 0x01, // correlation_id = 1
		0x00, 0x0A, // client_id length = 10
		'j', 'o', 'i', 'n', '-', 'd', 'e', 'b', 'u', 'g', // client_id = "join-debug"
	}
	
	// Send request
	messageLen := make([]byte, 4)
	binary.BigEndian.PutUint32(messageLen, uint32(len(apiVersionsRequest)))
	fullRequest := append(messageLen, apiVersionsRequest...)
	
	_, err = conn.Write(fullRequest)
	if err != nil {
		t.Fatalf("Failed to send ApiVersions: %v", err)
	}

	// Read ApiVersions response
	responseLen := make([]byte, 4)
	_, err = conn.Read(responseLen)
	if err != nil {
		t.Fatalf("Failed to read ApiVersions response length: %v", err)
	}
	
	respLen := binary.BigEndian.Uint32(responseLen)
	response := make([]byte, respLen)
	_, err = conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read ApiVersions response: %v", err)
	}
	
	t.Logf("ApiVersions response (%d bytes): %x", len(response), response)

	// Now send JoinGroup v2 request (minimal)
	t.Log("=== Sending JoinGroup v2 request ===")
	joinGroupRequest := []byte{
		// Request header: api_key(2) + api_version(2) + correlation_id(4) + client_id_len(2) + client_id
		0x00, 0x0B, // api_key = 11 (JoinGroup)
		0x00, 0x02, // api_version = 2
		0x00, 0x00, 0x00, 0x02, // correlation_id = 2
		0x00, 0x0A, // client_id length = 10
		'j', 'o', 'i', 'n', '-', 'd', 'e', 'b', 'u', 'g', // client_id = "join-debug"
		
		// JoinGroup request body:
		0x00, 0x0B, // group_id length = 11
		'd', 'e', 'b', 'u', 'g', '-', 'g', 'r', 'o', 'u', 'p', // group_id = "debug-group"
		0x00, 0x00, 0x75, 0x30, // session_timeout = 30000
		0x00, 0x00, 0x75, 0x30, // rebalance_timeout = 30000 (v1+)
		0x00, 0x00, // member_id length = 0 (empty, new member)
		0x00, 0x08, // protocol_type length = 8
		'c', 'o', 'n', 's', 'u', 'm', 'e', 'r', // protocol_type = "consumer"
		0x00, 0x00, 0x00, 0x01, // group_protocols count = 1
		0x00, 0x05, // protocol_name length = 5
		'r', 'a', 'n', 'g', 'e', // protocol_name = "range"
		0x00, 0x00, 0x00, 0x00, // protocol_metadata length = 0 (empty)
	}
	
	// Send request
	messageLen = make([]byte, 4)
	binary.BigEndian.PutUint32(messageLen, uint32(len(joinGroupRequest)))
	fullRequest = append(messageLen, joinGroupRequest...)
	
	t.Logf("Sending JoinGroup v2 request (%d bytes): %x", len(fullRequest), fullRequest)
	
	_, err = conn.Write(fullRequest)
	if err != nil {
		t.Fatalf("Failed to send JoinGroup: %v", err)
	}

	// Read JoinGroup response
	responseLen = make([]byte, 4)
	_, err = conn.Read(responseLen)
	if err != nil {
		t.Fatalf("Failed to read JoinGroup response length: %v", err)
	}
	
	respLen = binary.BigEndian.Uint32(responseLen)
	response = make([]byte, respLen)
	_, err = conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read JoinGroup response: %v", err)
	}
	
	t.Logf("JoinGroup v2 response (%d bytes): %x", len(response), response)
	
	// Parse the response manually to understand the format
	t.Log("=== Parsing JoinGroup v2 response ===")
	offset := 0
	
	// Correlation ID (4 bytes)
	correlationID := binary.BigEndian.Uint32(response[offset:offset+4])
	offset += 4
	t.Logf("Correlation ID: %d", correlationID)
	
	// Throttle time (4 bytes) - v2 addition
	throttleTime := binary.BigEndian.Uint32(response[offset:offset+4])
	offset += 4
	t.Logf("Throttle time: %d", throttleTime)
	
	// Error code (2 bytes)
	errorCode := binary.BigEndian.Uint16(response[offset:offset+2])
	offset += 2
	t.Logf("Error code: %d", errorCode)
	
	// Generation ID (4 bytes)
	generationID := binary.BigEndian.Uint32(response[offset:offset+4])
	offset += 4
	t.Logf("Generation ID: %d", generationID)
	
	// Group protocol (STRING)
	protocolLen := binary.BigEndian.Uint16(response[offset:offset+2])
	offset += 2
	protocol := string(response[offset:offset+int(protocolLen)])
	offset += int(protocolLen)
	t.Logf("Group protocol: %s", protocol)
	
	// Group leader (STRING)
	leaderLen := binary.BigEndian.Uint16(response[offset:offset+2])
	offset += 2
	leader := string(response[offset:offset+int(leaderLen)])
	offset += int(leaderLen)
	t.Logf("Group leader: %s", leader)
	
	// Member ID (STRING)
	memberIDLen := binary.BigEndian.Uint16(response[offset:offset+2])
	offset += 2
	memberID := string(response[offset:offset+int(memberIDLen)])
	offset += int(memberIDLen)
	t.Logf("Member ID: %s", memberID)
	
	// Members array
	membersCount := binary.BigEndian.Uint32(response[offset:offset+4])
	offset += 4
	t.Logf("Members count: %d", membersCount)
	
	for i := uint32(0); i < membersCount && offset < len(response); i++ {
		// Member ID (STRING)
		memberLen := binary.BigEndian.Uint16(response[offset:offset+2])
		offset += 2
		memberName := string(response[offset:offset+int(memberLen)])
		offset += int(memberLen)
		t.Logf("  Member %d ID: %s", i, memberName)
		
		// Metadata (BYTES)
		metadataLen := binary.BigEndian.Uint32(response[offset:offset+4])
		offset += 4
		metadata := response[offset:offset+int(metadataLen)]
		offset += int(metadataLen)
		t.Logf("  Member %d metadata (%d bytes): %x", i, len(metadata), metadata)
	}
	
	t.Logf("Parsed %d bytes, remaining: %d", offset, len(response)-offset)
	
	if offset != len(response) {
		t.Errorf("Response parsing mismatch: parsed %d bytes, total %d bytes", offset, len(response))
		t.Logf("Remaining bytes: %x", response[offset:])
	} else {
		t.Log("✅ JoinGroup response parsed successfully!")
	}
}
