package kafka

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestNetworkCapture captures the exact bytes sent over the network
func TestNetworkCapture(t *testing.T) {
	// Start gateway
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: "127.0.0.1:0",
	})

	go gatewayServer.Start()
	defer gatewayServer.Close()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	host, port := gatewayServer.GetListenerAddr()
	addr := fmt.Sprintf("%s:%d", host, port)
	t.Logf("Gateway running on %s", addr)

	// Add test topic
	handler := gatewayServer.GetHandler()
	handler.AddTopicForTesting("capture-topic", 1)

	// Test: Capture exact network traffic
	testNetworkTraffic(addr, t)
}

func testNetworkTraffic(addr string, t *testing.T) {
	// Create raw TCP connection
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	// Send ApiVersions request first
	t.Logf("=== Sending ApiVersions Request ===")
	apiVersionsReq := buildRawApiVersionsRequest()
	t.Logf("ApiVersions request (%d bytes): %x", len(apiVersionsReq), apiVersionsReq)

	if _, err := conn.Write(apiVersionsReq); err != nil {
		t.Fatalf("Failed to send ApiVersions: %v", err)
	}

	// Read ApiVersions response
	apiVersionsResp, err := readRawResponse(conn, t)
	if err != nil {
		t.Fatalf("Failed to read ApiVersions response: %v", err)
	}
	t.Logf("ApiVersions response (%d bytes): %x", len(apiVersionsResp), apiVersionsResp)

	// Send Metadata v1 request
	t.Logf("=== Sending Metadata v1 Request ===")
	metadataReq := buildRawMetadataV1Request([]string{"capture-topic"})
	t.Logf("Metadata request (%d bytes): %x", len(metadataReq), metadataReq)

	if _, err := conn.Write(metadataReq); err != nil {
		t.Fatalf("Failed to send Metadata: %v", err)
	}

	// Read Metadata response with detailed analysis
	metadataResp, err := readRawResponse(conn, t)
	if err != nil {
		t.Fatalf("Failed to read Metadata response: %v", err)
	}
	t.Logf("Metadata response (%d bytes): %x", len(metadataResp), metadataResp)

	// Analyze the response structure
	analyzeMetadataResponse(metadataResp, t)
}

func buildRawApiVersionsRequest() []byte {
	// Build ApiVersions request manually
	clientID := "test-client"

	// Calculate payload size: API key (2) + version (2) + correlation ID (4) + client ID length (2) + client ID
	payloadSize := 2 + 2 + 4 + 2 + len(clientID)

	req := make([]byte, 4) // Start with message size

	// Message size
	binary.BigEndian.PutUint32(req[0:4], uint32(payloadSize))

	// API key (ApiVersions = 18)
	req = append(req, 0, 18)
	// Version
	req = append(req, 0, 0)
	// Correlation ID
	req = append(req, 0, 0, 0, 1)
	// Client ID length
	clientIDLen := uint16(len(clientID))
	req = append(req, byte(clientIDLen>>8), byte(clientIDLen))
	// Client ID
	req = append(req, []byte(clientID)...)

	return req
}

func buildRawMetadataV1Request(topics []string) []byte {
	clientID := "test-client"

	// Calculate payload size: API key (2) + version (2) + correlation ID (4) + client ID length (2) + client ID + topics array
	payloadSize := 2 + 2 + 4 + 2 + len(clientID) + 4 // Base size + topics array length
	for _, topic := range topics {
		payloadSize += 2 + len(topic) // topic length (2) + topic name
	}

	req := make([]byte, 4) // Start with message size

	// Message size
	binary.BigEndian.PutUint32(req[0:4], uint32(payloadSize))

	// API key (Metadata = 3)
	req = append(req, 0, 3)
	// Version
	req = append(req, 0, 1)
	// Correlation ID
	req = append(req, 0, 0, 0, 2)
	// Client ID length
	clientIDLen := uint16(len(clientID))
	req = append(req, byte(clientIDLen>>8), byte(clientIDLen))
	// Client ID
	req = append(req, []byte(clientID)...)

	// Topics array
	topicsLen := uint32(len(topics))
	req = append(req, byte(topicsLen>>24), byte(topicsLen>>16), byte(topicsLen>>8), byte(topicsLen))
	for _, topic := range topics {
		topicLen := uint16(len(topic))
		req = append(req, byte(topicLen>>8), byte(topicLen))
		req = append(req, []byte(topic)...)
	}

	return req
}

func readRawResponse(conn net.Conn, t *testing.T) ([]byte, error) {
	// Read response size first
	sizeBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return nil, fmt.Errorf("failed to read response size: %v", err)
	}

	size := binary.BigEndian.Uint32(sizeBuf)
	t.Logf("Response size header: %d bytes", size)

	// Read response data
	data := make([]byte, size)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, fmt.Errorf("failed to read response data: %v", err)
	}

	return data, nil
}

func analyzeMetadataResponse(data []byte, t *testing.T) {
	t.Logf("=== Analyzing Metadata Response ===")

	if len(data) < 4 {
		t.Errorf("Response too short: %d bytes", len(data))
		return
	}

	offset := 0

	// Read correlation ID
	correlationID := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	t.Logf("Correlation ID: %d", correlationID)

	// Read brokers count
	if offset+4 > len(data) {
		t.Errorf("Not enough data for brokers count at offset %d", offset)
		return
	}
	brokersCount := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	t.Logf("Brokers count: %d", brokersCount)

	// Read each broker
	for i := 0; i < int(brokersCount); i++ {
		t.Logf("Reading broker %d at offset %d", i, offset)

		// Node ID
		if offset+4 > len(data) {
			t.Errorf("Not enough data for broker %d node ID", i)
			return
		}
		nodeID := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		// Host
		if offset+2 > len(data) {
			t.Errorf("Not enough data for broker %d host length", i)
			return
		}
		hostLen := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		if offset+int(hostLen) > len(data) {
			t.Errorf("Not enough data for broker %d host", i)
			return
		}
		host := string(data[offset : offset+int(hostLen)])
		offset += int(hostLen)

		// Port
		if offset+4 > len(data) {
			t.Errorf("Not enough data for broker %d port", i)
			return
		}
		port := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		// Rack (v1 addition)
		if offset+2 > len(data) {
			t.Errorf("Not enough data for broker %d rack length", i)
			return
		}
		rackLen := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		rack := ""
		if rackLen > 0 {
			if offset+int(rackLen) > len(data) {
				t.Errorf("Not enough data for broker %d rack", i)
				return
			}
			rack = string(data[offset : offset+int(rackLen)])
			offset += int(rackLen)
		}

		t.Logf("Broker %d: NodeID=%d, Host=%s, Port=%d, Rack=%s", i, nodeID, host, port, rack)
	}

	// Controller ID (v1 addition)
	if offset+4 > len(data) {
		t.Errorf("Not enough data for controller ID at offset %d", offset)
		return
	}
	controllerID := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	t.Logf("Controller ID: %d", controllerID)

	// Topics count
	if offset+4 > len(data) {
		t.Errorf("Not enough data for topics count at offset %d", offset)
		return
	}
	topicsCount := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	t.Logf("Topics count: %d", topicsCount)

	// Analyze remaining bytes
	remaining := len(data) - offset
	t.Logf("Remaining bytes after topics count: %d", remaining)
	t.Logf("Remaining data: %x", data[offset:])

	if remaining == 0 {
		t.Errorf("ERROR: No data remaining for topics! This might be the issue.")
		return
	}

	// Try to read first topic
	if topicsCount > 0 {
		t.Logf("Reading first topic at offset %d", offset)

		// Error code
		if offset+2 > len(data) {
			t.Errorf("Not enough data for topic error code")
			return
		}
		errorCode := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		// Topic name
		if offset+2 > len(data) {
			t.Errorf("Not enough data for topic name length")
			return
		}
		nameLen := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		if offset+int(nameLen) > len(data) {
			t.Errorf("Not enough data for topic name")
			return
		}
		name := string(data[offset : offset+int(nameLen)])
		offset += int(nameLen)

		t.Logf("Topic: ErrorCode=%d, Name=%s", errorCode, name)

		// Check remaining structure...
		remaining = len(data) - offset
		t.Logf("Remaining bytes after first topic name: %d", remaining)
	}
}
