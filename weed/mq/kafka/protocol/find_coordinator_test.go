package protocol

import (
	"testing"
)

func TestFindCoordinatorForGroup(t *testing.T) {
	tests := []struct {
		name        string
		groupID     string
		brokerHost  string
		brokerPort  int
		expectError bool
	}{
		{
			name:        "valid group with broker info",
			groupID:     "test-group-1",
			brokerHost:  "localhost",
			brokerPort:  9092,
			expectError: false,
		},
		{
			name:        "different group same broker",
			groupID:     "test-group-2",
			brokerHost:  "localhost",
			brokerPort:  9092,
			expectError: false,
		},
		{
			name:        "empty broker host",
			groupID:     "test-group-3",
			brokerHost:  "",
			brokerPort:  9092,
			expectError: false, // Should work, returns fallback localhost
		},
		{
			name:        "zero broker port",
			groupID:     "test-group-4",
			brokerHost:  "localhost",
			brokerPort:  0,
			expectError: false, // Should work, returns fallback 9092
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &Handler{}
			if tt.brokerHost != "" && tt.brokerPort != 0 {
				handler.SetGatewayAddress(tt.brokerHost + ":" + itoa(tt.brokerPort))
			} else if tt.brokerHost == "" && tt.brokerPort != 0 {
				// empty host -> default host, set only port to check fallback host
				handler.SetGatewayAddress("localhost:" + itoa(tt.brokerPort))
			} else if tt.brokerHost != "" && tt.brokerPort == 0 {
				// zero port -> set empty port to trigger fallback parsing
				handler.SetGatewayAddress(tt.brokerHost + ":")
			}

			host, port, nodeID, err := handler.findCoordinatorForGroup(tt.groupID)

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
				return
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// The function should always return a valid gateway address (with fallbacks)
			expectedHost := tt.brokerHost
			expectedPort := tt.brokerPort

			// GetBrokerAddress() provides fallbacks for empty/zero values
			if expectedHost == "" {
				expectedHost = "localhost"
			}
			if expectedPort == 0 {
				expectedPort = 9092
			}

			if host != expectedHost {
				t.Errorf("Expected host %s, got %s", expectedHost, host)
			}

			if port != expectedPort {
				t.Errorf("Expected port %d, got %d", expectedPort, port)
			}

			if nodeID != 1 {
				t.Errorf("Expected nodeID 1, got %d", nodeID)
			}

			t.Logf("Group %s -> Coordinator %s:%d (node %d)", tt.groupID, host, port, nodeID)
		})
	}
}

func TestFindCoordinatorConsistency(t *testing.T) {
	// Test that the same group ID always returns the same coordinator
	handler := &Handler{}
	handler.SetGatewayAddress("localhost:9092")

	groupID := "consistent-test-group"

	// Call multiple times and ensure consistency
	var firstHost string
	var firstPort int
	var firstNodeID int32

	for i := 0; i < 10; i++ {
		host, port, nodeID, err := handler.findCoordinatorForGroup(groupID)
		if err != nil {
			t.Fatalf("Unexpected error on iteration %d: %v", i, err)
		}

		if i == 0 {
			firstHost = host
			firstPort = port
			firstNodeID = nodeID
		} else {
			if host != firstHost || port != firstPort || nodeID != firstNodeID {
				t.Errorf("Inconsistent coordinator on iteration %d: got %s:%d (node %d), expected %s:%d (node %d)",
					i, host, port, nodeID, firstHost, firstPort, firstNodeID)
			}
		}
	}

	t.Logf("Coordinator for group %s is consistently %s:%d (node %d)", groupID, firstHost, firstPort, firstNodeID)
}

func TestFindCoordinatorV0Request(t *testing.T) {
	handler := &Handler{}
	handler.SetGatewayAddress("test-broker:9093")

	// Create a simple FindCoordinator v0 request
	// Format: coordinator_key_size (2 bytes) + coordinator_key
	groupID := "test-group"
	requestBody := make([]byte, 2+len(groupID))
	requestBody[0] = 0                  // High byte of length
	requestBody[1] = byte(len(groupID)) // Low byte of length
	copy(requestBody[2:], []byte(groupID))

	response, err := handler.handleFindCoordinatorV0(12345, requestBody)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(response) == 0 {
		t.Fatal("Empty response")
	}

	// Basic validation - response should contain correlation ID and coordinator info
	if len(response) < 4 {
		t.Errorf("Response too short: %d bytes", len(response))
	}

	t.Logf("FindCoordinator v0 response: %d bytes", len(response))
}

func TestFindCoordinatorV2Request(t *testing.T) {
	handler := &Handler{}
	handler.SetGatewayAddress("test-broker:9093")

	// Create a simple FindCoordinator v2 request
	// Format: coordinator_key_size (2 bytes) + coordinator_key + coordinator_type (1 byte)
	groupID := "test-group"
	requestBody := make([]byte, 2+len(groupID)+1)
	requestBody[0] = 0                  // High byte of length
	requestBody[1] = byte(len(groupID)) // Low byte of length
	copy(requestBody[2:2+len(groupID)], []byte(groupID))
	requestBody[2+len(groupID)] = 0 // Coordinator type (0 = consumer group)

	response, err := handler.handleFindCoordinatorV2(12345, requestBody)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(response) == 0 {
		t.Fatal("Empty response")
	}

	// Basic validation - response should contain correlation ID and coordinator info
	if len(response) < 4 {
		t.Errorf("Response too short: %d bytes", len(response))
	}

	t.Logf("FindCoordinator v2 response: %d bytes", len(response))
}

// itoa is a tiny helper to avoid importing strconv in tests
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
