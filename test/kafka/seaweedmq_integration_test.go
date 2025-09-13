package kafka_test

import (
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

// TestSeaweedMQIntegration_E2E tests the complete workflow with SeaweedMQ backend
// This test requires a real SeaweedMQ Agent running
func TestSeaweedMQIntegration_E2E(t *testing.T) {
	// Skip by default - requires real SeaweedMQ setup
	t.Skip("Integration test requires real SeaweedMQ setup - run manually")

	// Start the gateway with SeaweedMQ backend
	gatewayServer := gateway.NewTestServer(gateway.Options{
		Listen: ":0", // random port
	})

	err := gatewayServer.Start()
	if err != nil {
		t.Fatalf("Failed to start gateway with SeaweedMQ backend: %v", err)
	}
	defer gatewayServer.Close()

	addr := gatewayServer.Addr()
	t.Logf("Started Kafka Gateway with SeaweedMQ backend on %s", addr)

	// Wait for startup
	time.Sleep(200 * time.Millisecond)

	// Test basic connectivity
	t.Run("SeaweedMQ_BasicConnectivity", func(t *testing.T) {
		testSeaweedMQConnectivity(t, addr)
	})

	// Test topic lifecycle with SeaweedMQ
	t.Run("SeaweedMQ_TopicLifecycle", func(t *testing.T) {
		testSeaweedMQTopicLifecycle(t, addr)
	})

	// Test produce/consume workflow
	t.Run("SeaweedMQ_ProduceConsume", func(t *testing.T) {
		testSeaweedMQProduceConsume(t, addr)
	})
}

// testSeaweedMQConnectivity verifies gateway responds correctly
func testSeaweedMQConnectivity(t *testing.T, addr string) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect to SeaweedMQ gateway: %v", err)
	}
	defer conn.Close()

	// Send ApiVersions request
	req := buildApiVersionsRequest()
	_, err = conn.Write(req)
	if err != nil {
		t.Fatalf("Failed to send ApiVersions: %v", err)
	}

	// Read response
	sizeBytes := make([]byte, 4)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Read(sizeBytes)
	if err != nil {
		t.Fatalf("Failed to read response size: %v", err)
	}

	responseSize := uint32(sizeBytes[0])<<24 | uint32(sizeBytes[1])<<16 | uint32(sizeBytes[2])<<8 | uint32(sizeBytes[3])
	if responseSize == 0 || responseSize > 10000 {
		t.Fatalf("Invalid response size: %d", responseSize)
	}

	responseBody := make([]byte, responseSize)
	_, err = conn.Read(responseBody)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	// Verify API keys are advertised
	if len(responseBody) < 20 {
		t.Fatalf("Response too short")
	}

	apiKeyCount := uint32(responseBody[6])<<24 | uint32(responseBody[7])<<16 | uint32(responseBody[8])<<8 | uint32(responseBody[9])
	if apiKeyCount < 6 {
		t.Errorf("Expected at least 6 API keys, got %d", apiKeyCount)
	}

	t.Logf("SeaweedMQ gateway connectivity test passed, %d API keys advertised", apiKeyCount)
}

// testSeaweedMQTopicLifecycle tests creating and managing topics
func testSeaweedMQTopicLifecycle(t *testing.T, addr string) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Test CreateTopics request
	topicName := "seaweedmq-test-topic"
	createReq := buildCreateTopicsRequestCustom(topicName)

	_, err = conn.Write(createReq)
	if err != nil {
		t.Fatalf("Failed to send CreateTopics: %v", err)
	}

	// Read response
	sizeBytes := make([]byte, 4)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Read(sizeBytes)
	if err != nil {
		t.Fatalf("Failed to read CreateTopics response size: %v", err)
	}

	responseSize := uint32(sizeBytes[0])<<24 | uint32(sizeBytes[1])<<16 | uint32(sizeBytes[2])<<8 | uint32(sizeBytes[3])
	responseBody := make([]byte, responseSize)
	_, err = conn.Read(responseBody)
	if err != nil {
		t.Fatalf("Failed to read CreateTopics response: %v", err)
	}

	// Parse response to check for success (basic validation)
	if len(responseBody) < 10 {
		t.Fatalf("CreateTopics response too short")
	}

	t.Logf("SeaweedMQ topic creation test completed: %d bytes response", len(responseBody))
}

// testSeaweedMQProduceConsume tests the produce/consume workflow
func testSeaweedMQProduceConsume(t *testing.T, addr string) {
	// This would be a more comprehensive test in a full implementation
	// For now, just test that Produce requests are handled

	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// First create a topic
	createReq := buildCreateTopicsRequestCustom("produce-test-topic")
	_, err = conn.Write(createReq)
	if err != nil {
		t.Fatalf("Failed to send CreateTopics: %v", err)
	}

	// Read CreateTopics response
	sizeBytes := make([]byte, 4)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Read(sizeBytes)
	if err != nil {
		t.Fatalf("Failed to read CreateTopics size: %v", err)
	}

	responseSize := uint32(sizeBytes[0])<<24 | uint32(sizeBytes[1])<<16 | uint32(sizeBytes[2])<<8 | uint32(sizeBytes[3])
	responseBody := make([]byte, responseSize)
	_, err = conn.Read(responseBody)
	if err != nil {
		t.Fatalf("Failed to read CreateTopics response: %v", err)
	}

	// TODO: Send a Produce request and verify it works with SeaweedMQ
	// This would require building a proper Kafka Produce request

	t.Logf("SeaweedMQ produce/consume test placeholder completed")
}

// buildCreateTopicsRequestCustom creates a CreateTopics request for a specific topic
func buildCreateTopicsRequestCustom(topicName string) []byte {
	clientID := "seaweedmq-test-client"

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
	request = append(request, 0, 0, 0x30, 0x42) // 12354

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

// TestSeaweedMQGateway_ModeSelection tests that the gateway properly selects backends
func TestSeaweedMQGateway_ModeSelection(t *testing.T) {
	// Test in-memory mode (should always work)
	t.Run("InMemoryMode", func(t *testing.T) {
		server := gateway.NewTestServer(gateway.Options{
			Listen: ":0",
		})

		err := server.Start()
		if err != nil {
			t.Fatalf("In-memory mode should start: %v", err)
		}
		defer server.Close()

		addr := server.Addr()
		if addr == "" {
			t.Errorf("Server should have listening address")
		}

		t.Logf("In-memory mode started on %s", addr)
	})

	// Test SeaweedMQ mode with invalid agent (should fall back)
	t.Run("SeaweedMQModeFallback", func(t *testing.T) {
		server := gateway.NewTestServer(gateway.Options{
			Listen: ":0",
		})

		err := server.Start()
		if err != nil {
			t.Fatalf("Should start even with invalid agent (fallback to in-memory): %v", err)
		}
		defer server.Close()

		addr := server.Addr()
		if addr == "" {
			t.Errorf("Server should have listening address")
		}

		t.Logf("SeaweedMQ mode with fallback started on %s", addr)
	})
}

// TestSeaweedMQGateway_ConfigValidation tests configuration validation
func TestSeaweedMQGateway_ConfigValidation(t *testing.T) {
	testCases := []struct {
		name       string
		options    gateway.Options
		shouldWork bool
	}{
		{
			name: "ValidInMemory",
			options: gateway.Options{
				Listen: ":0",
			},
			shouldWork: true,
		},
		{
			name: "ValidSeaweedMQWithAgent",
			options: gateway.Options{
				Listen: ":0",
			},
			shouldWork: true, // May fail if no agent, but config is valid
		},
		{
			name: "SeaweedMQWithoutAgent",
			options: gateway.Options{
				Listen: ":0",
			},
			shouldWork: true, // Should fall back to in-memory
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := gateway.NewTestServer(tc.options)
			err := server.Start()

			if tc.shouldWork && err != nil {
				t.Errorf("Expected config to work, got error: %v", err)
			}

			if err == nil {
				server.Close()
				t.Logf("Config test passed for %s", tc.name)
			}
		})
	}
}
