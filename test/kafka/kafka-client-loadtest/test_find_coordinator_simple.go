package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

func main() {
	fmt.Println("Testing FindCoordinator with simple request...")

	// Connect to Kafka Gateway
	conn, err := net.Dial("tcp", "kafka-gateway:9093")
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Set timeout
	conn.SetDeadline(time.Now().Add(15 * time.Second))

	// Send FindCoordinator request
	fmt.Println("Sending FindCoordinator request...")
	request := buildFindCoordinatorRequest("test-group")
	_, err = conn.Write(request)
	if err != nil {
		fmt.Printf("Failed to send FindCoordinator: %v\n", err)
		return
	}

	// Read response
	fmt.Println("Reading FindCoordinator response...")
	response := make([]byte, 1024)
	n, err := conn.Read(response)
	if err != nil {
		fmt.Printf("Failed to read response: %v\n", err)
		return
	}

	fmt.Printf("Received %d bytes response\n", n)

	// Parse response
	if n >= 4 {
		correlationID := binary.BigEndian.Uint32(response[0:4])
		fmt.Printf("Correlation ID: %d\n", correlationID)
	}
	if n >= 6 {
		errorCode := binary.BigEndian.Uint16(response[4:6])
		fmt.Printf("Error Code: %d\n", errorCode)
	}
	if n >= 10 {
		nodeID := binary.BigEndian.Uint32(response[6:10])
		fmt.Printf("Node ID: %d\n", nodeID)
	}
	if n >= 12 {
		hostLen := binary.BigEndian.Uint16(response[10:12])
		fmt.Printf("Host Length: %d\n", hostLen)
		if n >= int(12+hostLen+4) {
			host := string(response[12 : 12+hostLen])
			port := binary.BigEndian.Uint32(response[12+hostLen : 12+hostLen+4])
			fmt.Printf("Coordinator: %s:%d\n", host, port)
		}
	}

	// Hex dump of response
	fmt.Printf("Response hex: %x\n", response[:n])
}

func buildFindCoordinatorRequest(groupID string) []byte {
	// Kafka request header
	request := make([]byte, 0, 20)

	// API Key (2 bytes) - FindCoordinator = 10
	request = append(request, 0, 10)

	// API Version (2 bytes) - v0
	request = append(request, 0, 0)

	// Correlation ID (4 bytes)
	request = append(request, 0, 0, 0, 1)

	// Client ID (2 bytes + string)
	clientID := "test-client"
	request = append(request, byte(len(clientID)>>8), byte(len(clientID)))
	request = append(request, []byte(clientID)...)

	// Request body
	// Coordinator key (2 bytes + string)
	request = append(request, byte(len(groupID)>>8), byte(len(groupID)))
	request = append(request, []byte(groupID)...)

	// V0 doesn't have coordinator type field

	// Message size (4 bytes) - total length minus 4
	messageSize := len(request) - 4
	messageSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(messageSizeBytes, uint32(messageSize))

	// Prepend message size
	finalRequest := append(messageSizeBytes, request...)

	return finalRequest
}
