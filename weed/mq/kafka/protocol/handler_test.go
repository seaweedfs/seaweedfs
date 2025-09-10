package protocol

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestHandler_ApiVersions(t *testing.T) {
	// Create handler
	h := NewHandler()

	// Create in-memory connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// Handle connection in background
	done := make(chan error, 1)
	go func() {
		done <- h.HandleConn(server)
	}()

	// Create ApiVersions request manually
	// Request format: api_key(2) + api_version(2) + correlation_id(4) + client_id_size(2) + client_id + body
	correlationID := uint32(12345)
	clientID := "test-client"

	message := make([]byte, 0, 64)
	message = append(message, 0, 18) // API key 18 (ApiVersions)
	message = append(message, 0, 0)  // API version 0

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	message = append(message, correlationIDBytes...)

	// Client ID length and string
	clientIDLen := uint16(len(clientID))
	message = append(message, byte(clientIDLen>>8), byte(clientIDLen))
	message = append(message, []byte(clientID)...)

	// Empty request body for ApiVersions

	// Write message size and data
	messageSize := uint32(len(message))
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, messageSize)

	if _, err := client.Write(sizeBuf); err != nil {
		t.Fatalf("write size: %v", err)
	}
	if _, err := client.Write(message); err != nil {
		t.Fatalf("write message: %v", err)
	}

	// Read response size
	var respSizeBuf [4]byte
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := client.Read(respSizeBuf[:]); err != nil {
		t.Fatalf("read response size: %v", err)
	}

	respSize := binary.BigEndian.Uint32(respSizeBuf[:])
	if respSize == 0 || respSize > 1024*1024 {
		t.Fatalf("invalid response size: %d", respSize)
	}

	// Read response data
	respBuf := make([]byte, respSize)
	if _, err := client.Read(respBuf); err != nil {
		t.Fatalf("read response: %v", err)
	}

	// Parse response: correlation_id(4) + error_code(2) + num_api_keys(4) + api_keys + throttle_time(4)
	if len(respBuf) < 14 { // minimum response size
		t.Fatalf("response too short: %d bytes", len(respBuf))
	}

	// Check correlation ID
	respCorrelationID := binary.BigEndian.Uint32(respBuf[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("correlation ID mismatch: got %d, want %d", respCorrelationID, correlationID)
	}

	// Check error code
	errorCode := binary.BigEndian.Uint16(respBuf[4:6])
	if errorCode != 0 {
		t.Errorf("expected no error, got error code: %d", errorCode)
	}

	// Check number of API keys
	numAPIKeys := binary.BigEndian.Uint32(respBuf[6:10])
	if numAPIKeys != 2 {
		t.Errorf("expected 2 API keys, got: %d", numAPIKeys)
	}
	
	// Check API key details: api_key(2) + min_version(2) + max_version(2)
	if len(respBuf) < 22 { // need space for 2 API keys
		t.Fatalf("response too short for API key data")
	}
	
	// First API key (ApiVersions)
	apiKey := binary.BigEndian.Uint16(respBuf[10:12])
	minVersion := binary.BigEndian.Uint16(respBuf[12:14])
	maxVersion := binary.BigEndian.Uint16(respBuf[14:16])
	
	if apiKey != 18 {
		t.Errorf("expected API key 18, got: %d", apiKey)
	}
	if minVersion != 0 {
		t.Errorf("expected min version 0, got: %d", minVersion)
	}
	if maxVersion != 3 {
		t.Errorf("expected max version 3, got: %d", maxVersion)
	}
	
	// Second API key (Metadata)
	apiKey2 := binary.BigEndian.Uint16(respBuf[16:18])
	minVersion2 := binary.BigEndian.Uint16(respBuf[18:20])
	maxVersion2 := binary.BigEndian.Uint16(respBuf[20:22])
	
	if apiKey2 != 3 {
		t.Errorf("expected API key 3, got: %d", apiKey2)
	}
	if minVersion2 != 0 {
		t.Errorf("expected min version 0, got: %d", minVersion2)
	}
	if maxVersion2 != 7 {
		t.Errorf("expected max version 7, got: %d", maxVersion2)
	}

	// Close client to end handler
	client.Close()

	// Wait for handler to complete
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("handler error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("handler did not complete in time")
	}
}

func TestHandler_handleApiVersions(t *testing.T) {
	h := NewHandler()
	correlationID := uint32(999)
	
	response, err := h.handleApiVersions(correlationID)
	if err != nil {
		t.Fatalf("handleApiVersions: %v", err)
	}
	
	if len(response) < 24 { // minimum expected size (now has 2 API keys)
		t.Fatalf("response too short: %d bytes", len(response))
	}
	
	// Check correlation ID
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("correlation ID: got %d, want %d", respCorrelationID, correlationID)
	}
	
	// Check error code
	errorCode := binary.BigEndian.Uint16(response[4:6])
	if errorCode != 0 {
		t.Errorf("error code: got %d, want 0", errorCode)
	}
	
	// Check number of API keys
	numAPIKeys := binary.BigEndian.Uint32(response[6:10])
	if numAPIKeys != 2 {
		t.Errorf("expected 2 API keys, got: %d", numAPIKeys)
	}
	
	// Check first API key (ApiVersions)
	apiKey := binary.BigEndian.Uint16(response[10:12])
	if apiKey != 18 {
		t.Errorf("first API key: got %d, want 18", apiKey)
	}
	
	// Check second API key (Metadata) 
	apiKey2 := binary.BigEndian.Uint16(response[16:18])
	if apiKey2 != 3 {
		t.Errorf("second API key: got %d, want 3", apiKey2)
	}
}

func TestHandler_handleMetadata(t *testing.T) {
	h := NewHandler()
	correlationID := uint32(456)
	
	// Empty request body for minimal test
	requestBody := []byte{}
	
	response, err := h.handleMetadata(correlationID, requestBody)
	if err != nil {
		t.Fatalf("handleMetadata: %v", err)
	}
	
	if len(response) < 40 { // minimum expected size
		t.Fatalf("response too short: %d bytes", len(response))
	}
	
	// Check correlation ID
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("correlation ID: got %d, want %d", respCorrelationID, correlationID)
	}
	
	// Check throttle time
	throttleTime := binary.BigEndian.Uint32(response[4:8])
	if throttleTime != 0 {
		t.Errorf("throttle time: got %d, want 0", throttleTime)
	}
	
	// Check brokers count
	brokersCount := binary.BigEndian.Uint32(response[8:12])
	if brokersCount != 1 {
		t.Errorf("brokers count: got %d, want 1", brokersCount)
	}
	
	// Check first broker node ID
	nodeID := binary.BigEndian.Uint32(response[12:16])
	if nodeID != 0 {
		t.Errorf("broker node ID: got %d, want 0", nodeID)
	}
	
	// Check host string length
	hostLen := binary.BigEndian.Uint16(response[16:18])
	expectedHost := "localhost"
	if hostLen != uint16(len(expectedHost)) {
		t.Errorf("host length: got %d, want %d", hostLen, len(expectedHost))
	}
	
	// Check host string
	if string(response[18:18+hostLen]) != expectedHost {
		t.Errorf("host: got %s, want %s", string(response[18:18+hostLen]), expectedHost)
	}
	
	// Check port
	portStart := 18 + int(hostLen)
	port := binary.BigEndian.Uint32(response[portStart:portStart+4])
	if port != 9092 {
		t.Errorf("port: got %d, want 9092", port)
	}
}

func TestHandler_Metadata_EndToEnd(t *testing.T) {
	// Create handler
	h := NewHandler()

	// Create in-memory connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// Handle connection in background
	done := make(chan error, 1)
	go func() {
		done <- h.HandleConn(server)
	}()

	// Create Metadata request
	correlationID := uint32(789)
	clientID := "metadata-test"
	
	message := make([]byte, 0, 64)
	message = append(message, 0, 3)     // API key 3 (Metadata)
	message = append(message, 0, 0)     // API version 0
	
	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	message = append(message, correlationIDBytes...)
	
	// Client ID length and string
	clientIDLen := uint16(len(clientID))
	message = append(message, byte(clientIDLen>>8), byte(clientIDLen))
	message = append(message, []byte(clientID)...)
	
	// Empty request body (all topics)
	message = append(message, 0xFF, 0xFF, 0xFF, 0xFF) // -1 = all topics
	
	// Write message size and data
	messageSize := uint32(len(message))
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, messageSize)

	if _, err := client.Write(sizeBuf); err != nil {
		t.Fatalf("write size: %v", err)
	}
	if _, err := client.Write(message); err != nil {
		t.Fatalf("write message: %v", err)
	}

	// Read response size
	var respSizeBuf [4]byte
	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := client.Read(respSizeBuf[:]); err != nil {
		t.Fatalf("read response size: %v", err)
	}

	respSize := binary.BigEndian.Uint32(respSizeBuf[:])
	if respSize == 0 || respSize > 1024*1024 {
		t.Fatalf("invalid response size: %d", respSize)
	}

	// Read response data
	respBuf := make([]byte, respSize)
	if _, err := client.Read(respBuf); err != nil {
		t.Fatalf("read response: %v", err)
	}

	// Parse response: correlation_id(4) + throttle_time(4) + brokers + cluster_id + controller_id + topics
	if len(respBuf) < 40 { // minimum response size
		t.Fatalf("response too short: %d bytes", len(respBuf))
	}
	
	// Check correlation ID
	respCorrelationID := binary.BigEndian.Uint32(respBuf[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("correlation ID mismatch: got %d, want %d", respCorrelationID, correlationID)
	}
	
	// Check brokers count
	brokersCount := binary.BigEndian.Uint32(respBuf[8:12])
	if brokersCount != 1 {
		t.Errorf("expected 1 broker, got: %d", brokersCount)
	}

	// Close client to end handler
	client.Close()

	// Wait for handler to complete
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("handler error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("handler did not complete in time")
	}
}
