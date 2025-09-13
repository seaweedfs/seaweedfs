package protocol

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestHandler_ApiVersions(t *testing.T) {
	// Create handler
	h := NewTestHandler()

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
	if numAPIKeys != 14 {
		t.Errorf("expected 14 API keys, got: %d", numAPIKeys)
	}

	// Check API key details: api_key(2) + min_version(2) + max_version(2)
	if len(respBuf) < 52 { // need space for 7 API keys
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

	// Third API key (ListOffsets)
	apiKey3 := binary.BigEndian.Uint16(respBuf[22:24])
	minVersion3 := binary.BigEndian.Uint16(respBuf[24:26])
	maxVersion3 := binary.BigEndian.Uint16(respBuf[26:28])

	if apiKey3 != 2 {
		t.Errorf("expected API key 2, got: %d", apiKey3)
	}
	if minVersion3 != 0 {
		t.Errorf("expected min version 0, got: %d", minVersion3)
	}
	if maxVersion3 != 2 {
		t.Errorf("expected max version 2, got: %d", maxVersion3)
	}

	// Fourth API key (CreateTopics)
	apiKey4 := binary.BigEndian.Uint16(respBuf[28:30])
	minVersion4 := binary.BigEndian.Uint16(respBuf[30:32])
	maxVersion4 := binary.BigEndian.Uint16(respBuf[32:34])

	if apiKey4 != 19 {
		t.Errorf("expected API key 19, got: %d", apiKey4)
	}
	if minVersion4 != 0 {
		t.Errorf("expected min version 0, got: %d", minVersion4)
	}
	if maxVersion4 != 4 {
		t.Errorf("expected max version 4, got: %d", maxVersion4)
	}

	// Fifth API key (DeleteTopics)
	apiKey5 := binary.BigEndian.Uint16(respBuf[34:36])
	minVersion5 := binary.BigEndian.Uint16(respBuf[36:38])
	maxVersion5 := binary.BigEndian.Uint16(respBuf[38:40])

	if apiKey5 != 20 {
		t.Errorf("expected API key 20, got: %d", apiKey5)
	}
	if minVersion5 != 0 {
		t.Errorf("expected min version 0, got: %d", minVersion5)
	}
	if maxVersion5 != 4 {
		t.Errorf("expected max version 4, got: %d", maxVersion5)
	}

	// Sixth API key (Produce)
	apiKey6 := binary.BigEndian.Uint16(respBuf[40:42])
	minVersion6 := binary.BigEndian.Uint16(respBuf[42:44])
	maxVersion6 := binary.BigEndian.Uint16(respBuf[44:46])

	if apiKey6 != 0 {
		t.Errorf("expected API key 0, got: %d", apiKey6)
	}
	if minVersion6 != 0 {
		t.Errorf("expected min version 0, got: %d", minVersion6)
	}
	if maxVersion6 != 7 {
		t.Errorf("expected max version 7, got: %d", maxVersion6)
	}

	// Seventh API key (Fetch)
	apiKey7 := binary.BigEndian.Uint16(respBuf[46:48])
	minVersion7 := binary.BigEndian.Uint16(respBuf[48:50])
	maxVersion7 := binary.BigEndian.Uint16(respBuf[50:52])

	if apiKey7 != 1 {
		t.Errorf("expected API key 1, got: %d", apiKey7)
	}
	if minVersion7 != 0 {
		t.Errorf("expected min version 0, got: %d", minVersion7)
	}
	if maxVersion7 != 7 {
		t.Errorf("expected max version 7, got: %d", maxVersion7)
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
	h := NewTestHandler()
	correlationID := uint32(999)

	response, err := h.handleApiVersions(correlationID)
	if err != nil {
		t.Fatalf("handleApiVersions: %v", err)
	}

	if len(response) < 90 { // minimum expected size (now has 13 API keys)
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
	if numAPIKeys != 14 {
		t.Errorf("expected 14 API keys, got: %d", numAPIKeys)
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

	// Check third API key (ListOffsets)
	apiKey3 := binary.BigEndian.Uint16(response[22:24])
	if apiKey3 != 2 {
		t.Errorf("third API key: got %d, want 2", apiKey3)
	}
}

func TestHandler_handleMetadata(t *testing.T) {
	h := NewTestHandler()
	correlationID := uint32(456)

	// Empty request body for minimal test
	requestBody := []byte{}

	response, err := h.handleMetadata(correlationID, 0, requestBody)
	if err != nil {
		t.Fatalf("handleMetadata: %v", err)
	}

	if len(response) < 31 { // minimum expected size for v0 (calculated)
		t.Fatalf("response too short: %d bytes", len(response))
	}

	// Check correlation ID
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("correlation ID: got %d, want %d", respCorrelationID, correlationID)
	}

	// Check brokers count
	brokersCount := binary.BigEndian.Uint32(response[4:8])
	if brokersCount != 1 {
		t.Errorf("brokers count: got %d, want 1", brokersCount)
	}
}

func TestHandler_handleListOffsets(t *testing.T) {
	h := NewTestHandler()
	correlationID := uint32(123)

	// Build a simple ListOffsets v0 request body (header stripped): topics
	// topics_count(4) + topic + partitions
	topic := "test-topic"

	requestBody := make([]byte, 0, 64)

	// Topics count (1)
	requestBody = append(requestBody, 0, 0, 0, 1)

	// Topic name
	requestBody = append(requestBody, 0, byte(len(topic)))
	requestBody = append(requestBody, []byte(topic)...)

	// Partitions count (2 partitions)
	requestBody = append(requestBody, 0, 0, 0, 2)

	// Partition 0: partition_id(4) + timestamp(8) - earliest
	requestBody = append(requestBody, 0, 0, 0, 0)                                     // partition 0
	requestBody = append(requestBody, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE) // -2 (earliest)

	// Partition 1: partition_id(4) + timestamp(8) - latest
	requestBody = append(requestBody, 0, 0, 0, 1)                                     // partition 1
	requestBody = append(requestBody, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF) // -1 (latest)

	response, err := h.handleListOffsets(correlationID, 0, requestBody)
	if err != nil {
		t.Fatalf("handleListOffsets: %v", err)
	}

	if len(response) < 20 { // minimum expected size
		t.Fatalf("response too short: %d bytes", len(response))
	}

	// Check correlation ID
	respCorrelationID := binary.BigEndian.Uint32(response[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("correlation ID: got %d, want %d", respCorrelationID, correlationID)
	}

	// For v0, throttle time is not present; topics count is next
	topicsCount := binary.BigEndian.Uint32(response[4:8])
	if topicsCount != 1 {
		t.Errorf("topics count: got %d, want 1", topicsCount)
	}
}

func TestHandler_ListOffsets_EndToEnd(t *testing.T) {
	// Create handler
	h := NewTestHandler()

	// Create in-memory connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// Handle connection in background
	done := make(chan error, 1)
	go func() {
		done <- h.HandleConn(server)
	}()

	// Create ListOffsets request
	correlationID := uint32(555)
	clientID := "listoffsets-test"
	topic := "my-topic"

	message := make([]byte, 0, 128)
	message = append(message, 0, 2) // API key 2 (ListOffsets)
	message = append(message, 0, 0) // API version 0

	// Correlation ID
	correlationIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(correlationIDBytes, correlationID)
	message = append(message, correlationIDBytes...)

	// Client ID length and string
	clientIDLen := uint16(len(clientID))
	message = append(message, byte(clientIDLen>>8), byte(clientIDLen))
	message = append(message, []byte(clientID)...)

	// Topics count (1)
	message = append(message, 0, 0, 0, 1)

	// Topic name
	topicLen := uint16(len(topic))
	message = append(message, byte(topicLen>>8), byte(topicLen))
	message = append(message, []byte(topic)...)

	// Partitions count (1)
	message = append(message, 0, 0, 0, 1)

	// Partition 0 requesting earliest offset
	message = append(message, 0, 0, 0, 0)                                     // partition 0
	message = append(message, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE) // -2 (earliest)

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

	// Parse response: correlation_id(4) + topics
	if len(respBuf) < 20 { // minimum response size
		t.Fatalf("response too short: %d bytes", len(respBuf))
	}

	// Check correlation ID
	respCorrelationID := binary.BigEndian.Uint32(respBuf[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("correlation ID mismatch: got %d, want %d", respCorrelationID, correlationID)
	}

	// Check topics count for v0 (no throttle time in v0)
	topicsCount := binary.BigEndian.Uint32(respBuf[4:8])
	if topicsCount != 1 {
		t.Errorf("expected 1 topic, got: %d", topicsCount)
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

func TestHandler_Metadata_EndToEnd(t *testing.T) {
	// Create handler
	h := NewTestHandler()

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
	message = append(message, 0, 3) // API key 3 (Metadata)
	message = append(message, 0, 0) // API version 0

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

	// Parse response: correlation_id(4) + brokers + topics (v0 has no throttle time)
	if len(respBuf) < 31 { // minimum response size for v0
		t.Fatalf("response too short: %d bytes", len(respBuf))
	}

	// Check correlation ID
	respCorrelationID := binary.BigEndian.Uint32(respBuf[0:4])
	if respCorrelationID != correlationID {
		t.Errorf("correlation ID mismatch: got %d, want %d", respCorrelationID, correlationID)
	}

	// Check brokers count (immediately after correlation ID in v0)
	brokersCount := binary.BigEndian.Uint32(respBuf[4:8])
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
