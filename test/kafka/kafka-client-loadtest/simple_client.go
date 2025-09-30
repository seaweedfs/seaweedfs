package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func main() {
	fmt.Println("=== Simple ApiVersions Test ===")
	
	conn, err := net.Dial("tcp", "localhost:9093")
	if err != nil {
		fmt.Printf("Connection failed: %v\n", err)
		return
	}
	defer conn.Close()
	
	// Build a simple ApiVersions v3 request
	// Message format: length(4) + api_key(2) + api_version(2) + correlation_id(4) + client_id + client_version + tagged_fields
	
	correlationId := uint32(12345)
	clientId := "test"
	clientVersion := "1.0"
	
	// Build request body
	body := make([]byte, 0, 64)
	
	// API Key (2 bytes)
	body = append(body, 0, 18) // ApiVersions = 18
	
	// API Version (2 bytes) 
	body = append(body, 0, 3) // Version 3
	
	// Correlation ID (4 bytes)
	corrBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(corrBytes, correlationId)
	body = append(body, corrBytes...)
	
	// Client ID (compact string for v3+)
	body = append(body, byte(len(clientId)+1)) // compact string length
	body = append(body, []byte(clientId)...)
	
	// Client Version (compact string for v3+)
	body = append(body, byte(len(clientVersion)+1)) // compact string length  
	body = append(body, []byte(clientVersion)...)
	
	// Tagged fields (empty)
	body = append(body, 0)
	
	// Add message length prefix
	message := make([]byte, 4)
	binary.BigEndian.PutUint32(message, uint32(len(body)))
	message = append(message, body...)
	
	fmt.Printf("Sending %d bytes: API Key 18, Version 3\n", len(message))
	
	// Send request
	_, err = conn.Write(message)
	if err != nil {
		fmt.Printf("Write failed: %v\n", err)
		return
	}
	
	// Read response
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	
	// Read response length
	lengthBuf := make([]byte, 4)
	n, err := conn.Read(lengthBuf)
	if err != nil {
		fmt.Printf("Read length failed: %v\n", err)
		return
	}
	
	if n != 4 {
		fmt.Printf("Expected 4 length bytes, got %d\n", n)
		return
	}
	
	responseLength := binary.BigEndian.Uint32(lengthBuf)
	fmt.Printf("Response length: %d bytes\n", responseLength)
	
	// Read response body
	responseBody := make([]byte, responseLength)
	totalRead := uint32(0)
	for totalRead < responseLength {
		n, err := conn.Read(responseBody[totalRead:])
		if err != nil {
			fmt.Printf("Read failed after %d bytes: %v\n", totalRead, err)
			return
		}
		totalRead += uint32(n)
	}
	
	fmt.Printf("✅ Successfully received %d-byte response!\n", len(responseBody))
	
	// Basic response analysis
	if len(responseBody) >= 4 {
		respCorrelationId := binary.BigEndian.Uint32(responseBody[0:4])
		fmt.Printf("Response correlation ID: %d (matches: %t)\n", respCorrelationId, respCorrelationId == correlationId)
	}
	
	if len(responseBody) >= 5 {
		fmt.Printf("Response header tagged fields: 0x%02x\n", responseBody[4])
	}
	
	if len(responseBody) >= 7 {
		errorCode := binary.BigEndian.Uint16(responseBody[5:7])
		fmt.Printf("Error code: %d\n", errorCode)
	}
	
	if len(responseBody) >= 8 {
		compactArrayLen := responseBody[7]
		fmt.Printf("Compact array length: %d (actual APIs: %d)\n", compactArrayLen, compactArrayLen-1)
	}
}
