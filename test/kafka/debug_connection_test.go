package kafka

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestGateway_BasicConnection tests if the gateway can handle basic TCP connections
func TestGateway_BasicConnection(t *testing.T) {
	// Start the gateway server
	srv := gateway.NewServer(gateway.Options{
		Listen:       ":0",
		UseSeaweedMQ: false,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start gateway: %v", err)
	}
	defer srv.Close()

	brokerAddr := srv.Addr()
	t.Logf("Gateway running on %s", brokerAddr)

	// Test basic TCP connection
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	t.Logf("Successfully connected to gateway")
}

// TestGateway_ApiVersionsRequest tests if we can send an ApiVersions request
func TestGateway_ApiVersionsRequest(t *testing.T) {
	// Start the gateway server
	srv := gateway.NewServer(gateway.Options{
		Listen:       ":0",
		UseSeaweedMQ: false,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start gateway: %v", err)
	}
	defer srv.Close()

	brokerAddr := srv.Addr()
	t.Logf("Gateway running on %s", brokerAddr)

	// Create connection
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Set read timeout
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Build ApiVersions request (API key 18, version 0)
	// Request format: message_size(4) + api_key(2) + api_version(2) + correlation_id(4) + client_id(2+string)
	correlationID := uint32(1)
	clientID := "debug-client"

	request := make([]byte, 0, 64)

	// Build message body first (without size)
	msgBody := make([]byte, 0, 32)
	msgBody = append(msgBody, 0, 18) // API key 18 (ApiVersions)
	msgBody = append(msgBody, 0, 0)  // API version 0

	// Correlation ID
	correlationBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationBytes, correlationID)
	msgBody = append(msgBody, correlationBytes...)

	// Client ID string
	clientIDBytes := []byte(clientID)
	msgBody = append(msgBody, byte(len(clientIDBytes)>>8), byte(len(clientIDBytes)))
	msgBody = append(msgBody, clientIDBytes...)

	// Message size (4 bytes) + message body
	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(len(msgBody)))
	request = append(request, sizeBytes...)
	request = append(request, msgBody...)

	t.Logf("Sending ApiVersions request: %d bytes", len(request))

	// Send request
	_, err = conn.Write(request)
	if err != nil {
		t.Fatalf("Failed to write request: %v", err)
	}

	// Read response size
	var responseSizeBytes [4]byte
	_, err = conn.Read(responseSizeBytes[:])
	if err != nil {
		t.Fatalf("Failed to read response size: %v", err)
	}

	responseSize := binary.BigEndian.Uint32(responseSizeBytes[:])
	t.Logf("Response size: %d bytes", responseSize)

	if responseSize == 0 || responseSize > 1024*1024 {
		t.Fatalf("Invalid response size: %d", responseSize)
	}

	// Read response body
	responseBody := make([]byte, responseSize)
	totalRead := 0
	for totalRead < int(responseSize) {
		n, err := conn.Read(responseBody[totalRead:])
		if err != nil {
			t.Fatalf("Failed to read response body: %v (read %d/%d bytes)", err, totalRead, responseSize)
		}
		totalRead += n
	}

	t.Logf("Received response: %d bytes", len(responseBody))

	// Parse basic response structure
	if len(responseBody) < 4 {
		t.Fatalf("Response too short: %d bytes", len(responseBody))
	}

	responseCorrelationID := binary.BigEndian.Uint32(responseBody[0:4])
	if responseCorrelationID != correlationID {
		t.Errorf("Correlation ID mismatch: sent %d, got %d", correlationID, responseCorrelationID)
	}

	t.Logf("ApiVersions request completed successfully")
}

// TestGateway_CreateTopicsRequest tests if we can send a CreateTopics request
func TestGateway_CreateTopicsRequest(t *testing.T) {
	// Start the gateway server
	srv := gateway.NewServer(gateway.Options{
		Listen:       ":0",
		UseSeaweedMQ: false,
	})

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start gateway: %v", err)
	}
	defer srv.Close()

	brokerAddr := srv.Addr()
	t.Logf("Gateway running on %s", brokerAddr)

	// Create connection
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Set read timeout
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Build CreateTopics request (API key 19, version 0)
	correlationID := uint32(2)
	clientID := "debug-client"
	topicName := "debug-topic"

	// Build message body
	msgBody := make([]byte, 0, 128)
	msgBody = append(msgBody, 0, 19) // API key 19 (CreateTopics)
	msgBody = append(msgBody, 0, 0)  // API version 0

	// Correlation ID
	correlationBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationBytes, correlationID)
	msgBody = append(msgBody, correlationBytes...)

	// Client ID string
	clientIDBytes := []byte(clientID)
	msgBody = append(msgBody, byte(len(clientIDBytes)>>8), byte(len(clientIDBytes)))
	msgBody = append(msgBody, clientIDBytes...)

	// Topics array - count (4 bytes)
	msgBody = append(msgBody, 0, 0, 0, 1) // 1 topic

	// Topic name
	topicNameBytes := []byte(topicName)
	msgBody = append(msgBody, byte(len(topicNameBytes)>>8), byte(len(topicNameBytes)))
	msgBody = append(msgBody, topicNameBytes...)

	// Num partitions (4 bytes)
	msgBody = append(msgBody, 0, 0, 0, 1) // 1 partition

	// Replication factor (2 bytes)
	msgBody = append(msgBody, 0, 1) // replication factor 1

	// Configs count (4 bytes)
	msgBody = append(msgBody, 0, 0, 0, 0) // 0 configs

	// Timeout (4 bytes)
	msgBody = append(msgBody, 0, 0, 0x75, 0x30) // 30 seconds

	// Message size + message body
	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(len(msgBody)))
	request := append(sizeBytes, msgBody...)

	t.Logf("Sending CreateTopics request: %d bytes", len(request))

	// Send request
	_, err = conn.Write(request)
	if err != nil {
		t.Fatalf("Failed to write request: %v", err)
	}

	// Read response size
	var responseSizeBytes [4]byte
	_, err = conn.Read(responseSizeBytes[:])
	if err != nil {
		t.Fatalf("Failed to read response size: %v", err)
	}

	responseSize := binary.BigEndian.Uint32(responseSizeBytes[:])
	t.Logf("Response size: %d bytes", responseSize)

	// Read response body
	responseBody := make([]byte, responseSize)
	totalRead := 0
	for totalRead < int(responseSize) {
		n, err := conn.Read(responseBody[totalRead:])
		if err != nil {
			t.Fatalf("Failed to read response body: %v (read %d/%d bytes)", err, totalRead, responseSize)
		}
		totalRead += n
	}

	t.Logf("CreateTopics request completed successfully, received %d bytes", len(responseBody))
}
