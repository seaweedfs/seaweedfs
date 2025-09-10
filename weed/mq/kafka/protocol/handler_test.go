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
	if numAPIKeys != 1 {
		t.Errorf("expected 1 API key, got: %d", numAPIKeys)
	}

	// Check API key details: api_key(2) + min_version(2) + max_version(2)
	if len(respBuf) < 16 {
		t.Fatalf("response too short for API key data")
	}

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

	if len(response) < 20 { // minimum expected size
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

	// Check API key
	apiKey := binary.BigEndian.Uint16(response[10:12])
	if apiKey != 18 {
		t.Errorf("API key: got %d, want 18", apiKey)
	}
}
