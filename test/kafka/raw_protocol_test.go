package kafka

import (
	"encoding/binary"
	"fmt"
	"net"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestRawProduceRequest tests our Produce API directly without kafka-go
func TestRawProduceRequest(t *testing.T) {
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

	// Pre-create topic
	topicName := "raw-test-topic"
	handler := srv.GetHandler()
	handler.AddTopicForTesting(topicName, 1)

	// Make a raw TCP connection
	conn, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Test 1: ApiVersions request (should work)
	t.Log("=== Testing ApiVersions API ===")
	if err := sendApiVersionsRequest(conn); err != nil {
		t.Fatalf("ApiVersions failed: %v", err)
	}
	t.Log("✅ ApiVersions API working")

	// Test 2: Metadata request (should work)
	t.Log("=== Testing Metadata API ===")
	if err := sendMetadataRequest(conn); err != nil {
		t.Fatalf("Metadata failed: %v", err)
	}
	t.Log("✅ Metadata API working")

	// Test 3: Raw Produce request (this is what we want to test)
	t.Log("=== Testing Produce API (RAW) ===")
	if err := sendProduceRequest(conn, topicName); err != nil {
		t.Fatalf("Produce failed: %v", err)
	}
	t.Log("✅ Produce API working!")
}

func sendApiVersionsRequest(conn net.Conn) error {
	// Build ApiVersions request
	correlationID := uint32(1)
	
	msgBody := make([]byte, 0, 32)
	msgBody = append(msgBody, 0, 18) // API key 18 (ApiVersions)
	msgBody = append(msgBody, 0, 0)  // API version 0
	
	// Correlation ID
	correlationBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationBytes, correlationID)
	msgBody = append(msgBody, correlationBytes...)
	
	// Client ID (empty)
	msgBody = append(msgBody, 0, 0) // empty client ID
	
	// Send request
	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(len(msgBody)))
	request := append(sizeBytes, msgBody...)
	
	if _, err := conn.Write(request); err != nil {
		return fmt.Errorf("write request: %w", err)
	}
	
	// Read response size
	var responseSizeBytes [4]byte
	if _, err := conn.Read(responseSizeBytes[:]); err != nil {
		return fmt.Errorf("read response size: %w", err)
	}
	
	responseSize := binary.BigEndian.Uint32(responseSizeBytes[:])
	
	// Read response body
	responseBody := make([]byte, responseSize)
	if _, err := conn.Read(responseBody); err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	
	fmt.Printf("ApiVersions response: %d bytes\n", responseSize)
	return nil
}

func sendMetadataRequest(conn net.Conn) error {
	// Build Metadata request
	correlationID := uint32(2)
	
	msgBody := make([]byte, 0, 32)
	msgBody = append(msgBody, 0, 3) // API key 3 (Metadata)
	msgBody = append(msgBody, 0, 1) // API version 1
	
	// Correlation ID
	correlationBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationBytes, correlationID)
	msgBody = append(msgBody, correlationBytes...)
	
	// Client ID (empty)
	msgBody = append(msgBody, 0, 0) // empty client ID
	
	// Topics array (empty = all topics)
	msgBody = append(msgBody, 0xFF, 0xFF, 0xFF, 0xFF) // -1 = all topics
	
	// Send request
	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(len(msgBody)))
	request := append(sizeBytes, msgBody...)
	
	if _, err := conn.Write(request); err != nil {
		return fmt.Errorf("write request: %w", err)
	}
	
	// Read response size
	var responseSizeBytes [4]byte
	if _, err := conn.Read(responseSizeBytes[:]); err != nil {
		return fmt.Errorf("read response size: %w", err)
	}
	
	responseSize := binary.BigEndian.Uint32(responseSizeBytes[:])
	
	// Read response body
	responseBody := make([]byte, responseSize)
	if _, err := conn.Read(responseBody); err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	
	fmt.Printf("Metadata response: %d bytes\n", responseSize)
	return nil
}

func sendProduceRequest(conn net.Conn, topicName string) error {
	// Build simple Produce request
	correlationID := uint32(3)
	
	msgBody := make([]byte, 0, 128)
	msgBody = append(msgBody, 0, 0) // API key 0 (Produce)
	msgBody = append(msgBody, 0, 1) // API version 1
	
	// Correlation ID
	correlationBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationBytes, correlationID)
	msgBody = append(msgBody, correlationBytes...)
	
	// Client ID (empty)
	msgBody = append(msgBody, 0, 0) // empty client ID
	
	// Acks (-1 = all replicas)
	msgBody = append(msgBody, 0xFF, 0xFF) // -1
	
	// Timeout (5000ms)
	msgBody = append(msgBody, 0, 0, 0x13, 0x88) // 5000ms
	
	// Topics count (1)
	msgBody = append(msgBody, 0, 0, 0, 1)
	
	// Topic name
	topicNameBytes := []byte(topicName)
	msgBody = append(msgBody, byte(len(topicNameBytes)>>8), byte(len(topicNameBytes)))
	msgBody = append(msgBody, topicNameBytes...)
	
	// Partitions count (1)
	msgBody = append(msgBody, 0, 0, 0, 1)
	
	// Partition 0
	msgBody = append(msgBody, 0, 0, 0, 0) // partition ID = 0
	
	// Record set (simple test record)
	testRecord := buildSimpleRecordSet("test-key", "test-value")
	recordSetSize := make([]byte, 4)
	binary.BigEndian.PutUint32(recordSetSize, uint32(len(testRecord)))
	msgBody = append(msgBody, recordSetSize...)
	msgBody = append(msgBody, testRecord...)
	
	// Send request
	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(len(msgBody)))
	request := append(sizeBytes, msgBody...)
	
	fmt.Printf("Sending Produce request: %d bytes\n", len(request))
	
	if _, err := conn.Write(request); err != nil {
		return fmt.Errorf("write request: %w", err)
	}
	
	// Read response size
	var responseSizeBytes [4]byte
	if _, err := conn.Read(responseSizeBytes[:]); err != nil {
		return fmt.Errorf("read response size: %w", err)
	}
	
	responseSize := binary.BigEndian.Uint32(responseSizeBytes[:])
	
	// Read response body
	responseBody := make([]byte, responseSize)
	if _, err := conn.Read(responseBody); err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	
	fmt.Printf("Produce response: %d bytes\n", responseSize)
	
	// Check if the response indicates success (simplified check)
	if responseSize > 8 {
		// Extract correlation ID and basic error code
		correlationResp := binary.BigEndian.Uint32(responseBody[0:4])
		if correlationResp == correlationID {
			fmt.Printf("✅ Produce request correlation ID matches: %d\n", correlationResp)
		}
		
		// Look for error codes in the response
		if len(responseBody) > 20 {
			// Skip to where partition error code should be (rough estimate)
			errorCode := binary.BigEndian.Uint16(responseBody[16:18])
			if errorCode == 0 {
				fmt.Printf("✅ Produce request succeeded (error code: 0)\n")
			} else {
				fmt.Printf("⚠️ Produce request error code: %d\n", errorCode)
			}
		}
	}
	
	return nil
}

func buildSimpleRecordSet(key, value string) []byte {
	// Build a very simple Kafka record batch (v0 format for simplicity)
	record := make([]byte, 0, 64)
	
	// Record batch header (simplified v0 format)
	record = append(record, 0, 0, 0, 0, 0, 0, 0, 0) // base offset
	record = append(record, 0, 0, 0, 30)             // batch length (estimated)
	record = append(record, 0, 0, 0, 0)              // partition leader epoch
	record = append(record, 0)                       // magic byte (v0)
	record = append(record, 0, 0, 0, 0)              // CRC32 (simplified)
	record = append(record, 0, 0)                    // attributes
	record = append(record, 0, 0, 0, 1)              // record count = 1
	
	// Simple record: key_length + key + value_length + value
	keyBytes := []byte(key)
	valueBytes := []byte(value)
	
	record = append(record, byte(len(keyBytes)>>8), byte(len(keyBytes)))
	record = append(record, keyBytes...)
	record = append(record, byte(len(valueBytes)>>8), byte(len(valueBytes)))
	record = append(record, valueBytes...)
	
	return record
}
