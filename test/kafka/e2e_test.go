package kafka_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestKafkaGateway_E2E tests the complete Kafka workflow using the gateway
func TestKafkaGateway_E2E(t *testing.T) {
	// Start the gateway server
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: ":0", // use random port
	})
	
	err := gatewayServer.Start()
	if err != nil {
		t.Fatalf("Failed to start gateway: %v", err)
	}
	defer gatewayServer.Close()
	
	// Get the actual listening address
	addr := gatewayServer.Addr()
	t.Logf("Gateway started on %s", addr)
	
	// Wait a moment for the server to be fully ready
	time.Sleep(100 * time.Millisecond)
	
	// For now, we'll do basic connectivity testing
	// In a full implementation, this would use kafka-go client
	t.Run("BasicConnectivity", func(t *testing.T) {
		testBasicConnectivity(t, addr)
	})
	
	t.Run("ApiVersionsHandshake", func(t *testing.T) {
		testApiVersionsHandshake(t, addr)
	})
	
	t.Run("TopicOperations", func(t *testing.T) {
		testTopicOperations(t, addr)
	})
}

// testBasicConnectivity verifies we can connect to the gateway
func testBasicConnectivity(t *testing.T, addr string) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to gateway: %v", err)
	}
	defer conn.Close()
	
	// Send a simple ApiVersions request
	apiVersionsReq := buildApiVersionsRequest()
	
	_, err = conn.Write(apiVersionsReq)
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	
	// Read response
	response := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := conn.Read(response)
	if err != nil {
		t.Fatalf("Failed to read response: %v", err)
	}
	
	if n < 10 {
		t.Fatalf("Response too short: %d bytes", n)
	}
	
	t.Logf("Received response of %d bytes", n)
}

// testApiVersionsHandshake tests the API versions handshake
func testApiVersionsHandshake(t *testing.T, addr string) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	
	// Send ApiVersions request
	req := buildApiVersionsRequest()
	_, err = conn.Write(req)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	
	// Read response size
	sizeBytes := make([]byte, 4)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Read(sizeBytes)
	if err != nil {
		t.Fatalf("Read size failed: %v", err)
	}
	
	responseSize := uint32(sizeBytes[0])<<24 | uint32(sizeBytes[1])<<16 | uint32(sizeBytes[2])<<8 | uint32(sizeBytes[3])
	if responseSize == 0 || responseSize > 10000 {
		t.Fatalf("Invalid response size: %d", responseSize)
	}
	
	// Read response body
	responseBody := make([]byte, responseSize)
	_, err = conn.Read(responseBody)
	if err != nil {
		t.Fatalf("Read body failed: %v", err)
	}
	
	// Verify correlation ID matches
	correlationID := uint32(responseBody[0])<<24 | uint32(responseBody[1])<<16 | uint32(responseBody[2])<<8 | uint32(responseBody[3])
	if correlationID != 12345 {
		t.Errorf("Correlation ID mismatch: got %d, want 12345", correlationID)
	}
	
	// Check that we have API keys advertised
	if len(responseBody) < 20 {
		t.Fatalf("Response too short for API key data")
	}
	
	// Error code should be 0
	errorCode := uint16(responseBody[4])<<8 | uint16(responseBody[5])
	if errorCode != 0 {
		t.Errorf("Error code: got %d, want 0", errorCode)
	}
	
	// API keys count should be > 0
	apiKeyCount := uint32(responseBody[6])<<24 | uint32(responseBody[7])<<16 | uint32(responseBody[8])<<8 | uint32(responseBody[9])
	if apiKeyCount == 0 {
		t.Errorf("No API keys advertised")
	}
	
	t.Logf("Gateway advertises %d API keys", apiKeyCount)
}

// testTopicOperations tests creating and listing topics
func testTopicOperations(t *testing.T, addr string) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	
	// Test Metadata request (should return empty topic list initially)
	metadataReq := buildMetadataRequest()
	_, err = conn.Write(metadataReq)
	if err != nil {
		t.Fatalf("Metadata request failed: %v", err)
	}
	
	// Read metadata response
	sizeBytes := make([]byte, 4)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Read(sizeBytes)
	if err != nil {
		t.Fatalf("Read metadata size failed: %v", err)
	}
	
	responseSize := uint32(sizeBytes[0])<<24 | uint32(sizeBytes[1])<<16 | uint32(sizeBytes[2])<<8 | uint32(sizeBytes[3])
	responseBody := make([]byte, responseSize)
	_, err = conn.Read(responseBody)
	if err != nil {
		t.Fatalf("Read metadata body failed: %v", err)
	}
	
	// Verify basic metadata response structure
	if len(responseBody) < 20 {
		t.Fatalf("Metadata response too short")
	}
	
	t.Logf("Metadata response received: %d bytes", len(responseBody))
	
	// Test CreateTopics request
	createTopicsReq := buildCreateTopicsRequest()
	_, err = conn.Write(createTopicsReq)
	if err != nil {
		t.Fatalf("CreateTopics request failed: %v", err)
	}
	
	// Read CreateTopics response
	_, err = conn.Read(sizeBytes)
	if err != nil {
		t.Fatalf("Read CreateTopics size failed: %v", err)
	}
	
	responseSize = uint32(sizeBytes[0])<<24 | uint32(sizeBytes[1])<<16 | uint32(sizeBytes[2])<<8 | uint32(sizeBytes[3])
	responseBody = make([]byte, responseSize)
	_, err = conn.Read(responseBody)
	if err != nil {
		t.Fatalf("Read CreateTopics body failed: %v", err)
	}
	
	t.Logf("CreateTopics response received: %d bytes", len(responseBody))
	
	// TODO: Parse response to verify topic creation success
	// For Phase 1, we're just verifying the protocol works
}

// buildApiVersionsRequest creates a Kafka ApiVersions request
func buildApiVersionsRequest() []byte {
	// Message size (4) + API key (2) + API version (2) + correlation ID (4) + client ID
	clientID := "test-e2e-client"
	messageSize := 2 + 2 + 4 + 2 + len(clientID) // API key + version + correlation + client_id_len + client_id
	
	request := make([]byte, 0, messageSize+4)
	
	// Message size
	request = append(request, byte(messageSize>>24), byte(messageSize>>16), byte(messageSize>>8), byte(messageSize))
	
	// API key (ApiVersions = 18)
	request = append(request, 0, 18)
	
	// API version
	request = append(request, 0, 3)
	
	// Correlation ID
	request = append(request, 0, 0, 0x30, 0x39) // 12345
	
	// Client ID
	request = append(request, 0, byte(len(clientID)))
	request = append(request, []byte(clientID)...)
	
	return request
}

// buildMetadataRequest creates a Kafka Metadata request
func buildMetadataRequest() []byte {
	clientID := "test-e2e-client"
	messageSize := 2 + 2 + 4 + 2 + len(clientID) + 4 // + topics_count(4) for empty topic list
	
	request := make([]byte, 0, messageSize+4)
	
	// Message size
	request = append(request, byte(messageSize>>24), byte(messageSize>>16), byte(messageSize>>8), byte(messageSize))
	
	// API key (Metadata = 3)
	request = append(request, 0, 3)
	
	// API version
	request = append(request, 0, 7)
	
	// Correlation ID
	request = append(request, 0, 0, 0x30, 0x40) // 12352
	
	// Client ID
	request = append(request, 0, byte(len(clientID)))
	request = append(request, []byte(clientID)...)
	
	// Topics count (0 = get all topics)
	request = append(request, 0, 0, 0, 0)
	
	return request
}

// buildCreateTopicsRequest creates a Kafka CreateTopics request
func buildCreateTopicsRequest() []byte {
	clientID := "test-e2e-client"
	topicName := "test-e2e-topic"
	
	// Approximate message size
	messageSize := 2 + 2 + 4 + 2 + len(clientID) + 4 + 4 + 2 + len(topicName) + 4 + 2 + 4 + 4
	
	request := make([]byte, 0, messageSize+4)
	
	// Message size placeholder
	sizePos := len(request)
	request = append(request, 0, 0, 0, 0)
	
	// API key (CreateTopics = 19)
	request = append(request, 0, 19)
	
	// API version
	request = append(request, 0, 4)
	
	// Correlation ID
	request = append(request, 0, 0, 0x30, 0x41) // 12353
	
	// Client ID
	request = append(request, 0, byte(len(clientID)))
	request = append(request, []byte(clientID)...)
	
	// Timeout (5000ms)
	request = append(request, 0, 0, 0x13, 0x88)
	
	// Topics count (1)
	request = append(request, 0, 0, 0, 1)
	
	// Topic name
	request = append(request, 0, byte(len(topicName)))
	request = append(request, []byte(topicName)...)
	
	// Num partitions (1)
	request = append(request, 0, 0, 0, 1)
	
	// Replication factor (1)
	request = append(request, 0, 1)
	
	// Configs count (0)
	request = append(request, 0, 0, 0, 0)
	
	// Topic timeout (5000ms)
	request = append(request, 0, 0, 0x13, 0x88)
	
	// Fix message size
	actualSize := len(request) - 4
	request[sizePos] = byte(actualSize >> 24)
	request[sizePos+1] = byte(actualSize >> 16)
	request[sizePos+2] = byte(actualSize >> 8)
	request[sizePos+3] = byte(actualSize)
	
	return request
}

// TestKafkaGateway_ProduceConsume tests the produce/consume workflow
// Note: This is a placeholder for future implementation with kafka-go client
func TestKafkaGateway_ProduceConsume(t *testing.T) {
	t.Skip("TODO: Implement with kafka-go client library for full E2E test")
	
	// This test would:
	// 1. Start gateway
	// 2. Create kafka-go producer and consumer
	// 3. Create topic
	// 4. Produce messages
	// 5. Consume messages
	// 6. Verify message content matches
}

// TestKafkaGateway_MultipleClients tests concurrent client connections
func TestKafkaGateway_MultipleClients(t *testing.T) {
	// Start the gateway server
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: ":0", // use random port
	})
	
	err := gatewayServer.Start()
	if err != nil {
		t.Fatalf("Failed to start gateway: %v", err)
	}
	defer gatewayServer.Close()
	
	addr := gatewayServer.Addr()
	
	// Test multiple concurrent connections
	numClients := 5
	done := make(chan error, numClients)
	
	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				done <- fmt.Errorf("client %d connect failed: %v", clientID, err)
				return
			}
			defer conn.Close()
			
			// Each client sends ApiVersions request
			req := buildApiVersionsRequest()
			_, err = conn.Write(req)
			if err != nil {
				done <- fmt.Errorf("client %d write failed: %v", clientID, err)
				return
			}
			
			// Read response
			sizeBytes := make([]byte, 4)
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, err = conn.Read(sizeBytes)
			if err != nil {
				done <- fmt.Errorf("client %d read failed: %v", clientID, err)
				return
			}
			
			done <- nil
		}(i)
	}
	
	// Wait for all clients to complete
	for i := 0; i < numClients; i++ {
		err := <-done
		if err != nil {
			t.Errorf("Client error: %v", err)
		}
	}
}

// TestKafkaGateway_StressTest performs basic stress testing
func TestKafkaGateway_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	
	// Start the gateway server
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: ":0",
	})
	
	err := gatewayServer.Start()
	if err != nil {
		t.Fatalf("Failed to start gateway: %v", err)
	}
	defer gatewayServer.Close()
	
	addr := gatewayServer.Addr()
	
	// Send many requests rapidly
	numRequests := 100
	errors := make(chan error, numRequests)
	
	for i := 0; i < numRequests; i++ {
		go func(reqID int) {
			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				errors <- err
				return
			}
			defer conn.Close()
			
			// Send multiple requests on same connection
			for j := 0; j < 3; j++ {
				req := buildApiVersionsRequest()
				_, err = conn.Write(req)
				if err != nil {
					errors <- err
					return
				}
				
				// Read response
				sizeBytes := make([]byte, 4)
				conn.SetReadDeadline(time.Now().Add(1 * time.Second))
				_, err = conn.Read(sizeBytes)
				if err != nil {
					errors <- err
					return
				}
				
				// Skip reading full response for speed
			}
			
			errors <- nil
		}(i)
	}
	
	// Collect results
	successCount := 0
	for i := 0; i < numRequests; i++ {
		err := <-errors
		if err == nil {
			successCount++
		} else {
			t.Logf("Request error: %v", err)
		}
	}
	
	successRate := float64(successCount) / float64(numRequests)
	if successRate < 0.95 { // Allow some failures under stress
		t.Errorf("Success rate too low: %.2f%% (expected >= 95%%)", successRate*100)
	}
	
	t.Logf("Stress test completed: %d/%d successful (%.2f%%)", successCount, numRequests, successRate*100)
}
